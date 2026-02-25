/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2023 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "crud_component.hxx"

#include "collections_component.hxx"
#include "core/crud_options.hxx"
#include "core/error_context/key_value_status_code.hxx"
#include "core/logger/logger.hxx"
#include "core/mcbp/buffer_writer.hxx"
#include "core/pending_operation.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/datatype.hxx"
#include "core/protocol/magic.hxx"
#include "core/protocol/status.hxx"
#include "core/range_scan_options.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/unsigned_leb128.hxx"
#include "mcbp/big_endian.hxx"
#include "mcbp/durability_level_frame.hxx"
#include "mcbp/durability_timeout_frame.hxx"
#include "mcbp/preserve_expiry_frame.hxx"
#include "mcbp/queue_request.hxx"
#include "mcbp/queue_response.hxx"
#include "platform/base64.h"
#include "timeout_defaults.hxx"
#include "utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <gsl/span>
#include <gsl/span_ext>
#include <gsl/util>
#include <snappy.h>
#include <spdlog/fmt/bundled/core.h>
#include <tao/json/value.hpp>
#include <tl/expected.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

namespace couchbase::core
{
namespace
{
auto
serialize_range_scan_create_options(const range_scan_create_options& options)
  -> std::pair<std::vector<std::byte>, std::error_code>
{
  tao::json::value body{};
  if (options.ids_only) {
    body["key_only"] = true;
  }

  if (options.collection_id != 0) {
    body["collection"] = fmt::format("{:x}", options.collection_id);
  }

  if (std::holds_alternative<range_scan>(options.scan_type) ||
      std::holds_alternative<prefix_scan>(options.scan_type)) {
    const auto& range = (std::holds_alternative<range_scan>(options.scan_type))
                          ? std::get<range_scan>(options.scan_type)
                          : std::get<prefix_scan>(options.scan_type).to_range_scan();

    const auto& from = range.from.value_or(scan_term{ "" });
    const auto& to = range.to.value_or(scan_term{ "\xf4\x8f\xfb\xfb" });

    body["range"] = {
      { from.exclusive ? "excl_start" : "start", base64::encode(from.term) },
      { to.exclusive ? "excl_end" : "end", base64::encode(to.term) },
    };
  } else if (std::holds_alternative<sampling_scan>(options.scan_type)) {
    const auto& sampling = std::get<sampling_scan>(options.scan_type);

    // The limit in sampling scan is required to be greater than 0
    if (sampling.limit <= 0) {
      return { {}, errc::common::invalid_argument };
    }

    std::uint64_t seed{};
    if (sampling.seed.has_value()) {
      seed = sampling.seed.value();
    } else {
      // Generate random uint64 as seed
      std::random_device rd;
      std::mt19937_64 gen(rd());
      std::uniform_int_distribution<std::uint64_t> dis;
      seed = dis(gen);
    }

    body["sampling"] = {
      { "samples", sampling.limit },
      { "seed", seed },
    };
  } else {
    return { {}, errc::common::invalid_argument };
  }

  if (options.snapshot_requirements) {
    const auto& snapshot = options.snapshot_requirements.value();
    tao::json::value requirements = {
      { "vb_uuid", std::to_string(snapshot.vbucket_uuid) },
      { "seqno", snapshot.sequence_number },
      { "timeout_ms",
        (options.timeout == std::chrono::milliseconds::zero())
          ? timeout_defaults::key_value_scan_timeout.count()
          : options.timeout.count() },
    };
    if (snapshot.sequence_number_exists) {
      requirements["seqno_exists"] = true;
    }
    body["snapshot_requirements"] = requirements;
  }

  return { utils::json::generate_binary(body), {} };
}

auto
parse_range_scan_keys(gsl::span<std::byte> data,
                      const std::shared_ptr<mcbp::queue_request>& request,
                      range_scan_item_callback&& item_callback) -> std::error_code
{
  do {
    if (data.empty() || request->is_cancelled()) {
      return {};
    }
    auto [key_length, remaining] =
      utils::decode_unsigned_leb128<std::size_t>(data, core::utils::leb_128_no_throw{});
    if (remaining.size() < key_length) {
      return errc::network::protocol_error;
    }
    item_callback(
      range_scan_item{ { reinterpret_cast<const char*>(remaining.data()), key_length } });
    if (remaining.size() == key_length) {
      return {};
    }
    data = gsl::make_span(remaining.data() + key_length, remaining.size() - key_length);
  } while (!data.empty());
  return {};
}

auto
parse_range_scan_documents(gsl::span<std::byte> data,
                           const std::shared_ptr<mcbp::queue_request>& request,
                           range_scan_item_callback&& item_callback) -> std::error_code
{
  do {
    if (data.empty() || request->is_cancelled()) {
      return {};
    }

    range_scan_item_body body{};
    static constexpr std::size_t header_offset = sizeof(body.flags) + sizeof(body.expiry) +
                                                 sizeof(body.sequence_number) + sizeof(body.cas) +
                                                 sizeof(body.datatype);

    if (data.size() < header_offset) {
      return errc::network::protocol_error;
    }

    body.flags = mcbp::big_endian::read_uint32(data, 0);
    body.expiry = mcbp::big_endian::read_uint32(data, 4);
    body.sequence_number = mcbp::big_endian::read_uint64(data, 8);
    body.cas = couchbase::cas{ mcbp::big_endian::read_uint64(data, 16) };
    body.datatype = data[24];
    data = gsl::make_span(data.data() + header_offset, data.size() - header_offset);

    std::string key{};
    {
      auto [key_length, remaining] =
        utils::decode_unsigned_leb128<std::size_t>(data, core::utils::leb_128_no_throw{});
      if (remaining.size() < key_length) {
        return errc::network::protocol_error;
      }
      key = { reinterpret_cast<const char*>(remaining.data()), key_length };
      data = gsl::make_span(remaining.data() + key_length, remaining.size() - key_length);
    }

    {
      auto [value_length, remaining] =
        utils::decode_unsigned_leb128<std::size_t>(data, core::utils::leb_128_no_throw{});
      if (remaining.size() < value_length) {
        return errc::network::protocol_error;
      }
      body.value = { remaining.begin(),
                     remaining.begin() + static_cast<std::ptrdiff_t>(value_length) };
      if ((body.datatype & static_cast<std::byte>(protocol::datatype::snappy)) != std::byte{ 0 }) {
        std::string uncompressed;
        if (snappy::Uncompress(
              reinterpret_cast<const char*>(body.value.data()), body.value.size(), &uncompressed)) {
          body.value = core::utils::to_binary(uncompressed);
          body.datatype &= ~static_cast<std::byte>(protocol::datatype::snappy);
        }
      }
      data = gsl::make_span(remaining.data() + value_length, remaining.size() - value_length);
    }

    item_callback(range_scan_item{ std::move(key), std::move(body) });
  } while (!data.empty());
  return {};
}

auto
parse_range_scan_data(gsl::span<std::byte> payload,
                      const std::shared_ptr<mcbp::queue_request>& request,
                      range_scan_item_callback&& items,
                      bool keys_only) -> std::error_code
{
  if (keys_only) {
    return parse_range_scan_keys(payload, request, std::move(items));
  }
  return parse_range_scan_documents(payload, request, std::move(items));
}

auto
extract_mutation_token(const std::shared_ptr<mcbp::queue_response>& response,
                       std::uint16_t vbucket,
                       const std::string& bucket_name) -> mutation_token
{
  if (response && response->extras_.size() >= 16) {
    return mutation_token{ mcbp::big_endian::read_uint64(response->extras_, 0),
                           mcbp::big_endian::read_uint64(response->extras_, 8),
                           vbucket,
                           bucket_name };
  }
  return mutation_token{ 0, 0, vbucket, bucket_name };
}
} // namespace

class crud_component_impl
{
public:
  crud_component_impl(asio::io_context& io,
                      std::string bucket_name,
                      collections_component collections,
                      std::shared_ptr<retry_strategy> default_retry_strategy)
    : io_{ io }
    , bucket_name_{ std::move(bucket_name) }
    , collections_{ std::move(collections) }
    , default_retry_strategy_{ std::move(default_retry_strategy) }
  {
    (void)io_;
  }

private:
  template<typename Options>
  auto dispatch_and_set_timeout(std::shared_ptr<mcbp::queue_request> req, const Options& options)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  template<typename Result, typename Options, typename Callback>
  auto store(std::string scope_name,
             std::string collection_name,
             std::vector<std::byte> key,
             std::vector<std::byte> value,
             const Options& options,
             protocol::client_opcode opcode,
             std::optional<couchbase::cas> cas,
             bool preserve_expiry,
             Callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        Result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      Result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, opcode, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);
    req->value_ = std::move(value);
    req->datatype_ = static_cast<std::byte>(options.data_type);

    mcbp::buffer_writer extra_buf(8);
    extra_buf.write_uint32(options.flags);
    extra_buf.write_uint32(options.expiry);
    req->extras_ = std::move(extra_buf.store_);

    if (cas.has_value()) {
      req->cas_ = cas.value().value();
    }

    if (options.durability_level != couchbase::durability_level::none) {
      req->durability_level_frame_ = mcbp::durability_level_frame{
        static_cast<mcbp::durability_level>(options.durability_level)
      };
      if (options.durability_level_timeout.count() > 0) {
        req->durability_timeout_frame_ =
          mcbp::durability_timeout_frame{ options.durability_level_timeout };
      }
    }

    if (preserve_expiry) {
      req->preserve_expiry_frame_ = mcbp::preserve_expiry_frame{};
    }

    return dispatch_and_set_timeout(req, options);
  }

  template<typename Result, typename Options, typename Callback>
  auto adjoin(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const Options& options,
              protocol::client_opcode opcode,
              Callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        Result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      Result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, opcode, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);
    req->value_ = std::move(value);
    req->cas_ = options.cas.value();

    if (options.durability_level != couchbase::durability_level::none) {
      req->durability_level_frame_ = mcbp::durability_level_frame{
        static_cast<mcbp::durability_level>(options.durability_level)
      };
      if (options.durability_level_timeout.count() > 0) {
        req->durability_timeout_frame_ =
          mcbp::durability_timeout_frame{ options.durability_level_timeout };
      }
    }

    return dispatch_and_set_timeout(req, options);
  }

  template<typename Result, typename Options, typename Callback>
  auto counter(std::string scope_name,
               std::string collection_name,
               std::vector<std::byte> key,
               const Options& options,
               protocol::client_opcode opcode,
               Callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        Result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      Result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      if (response->value_.size() == 8) {
        res.value = mcbp::big_endian::read_uint64(response->value_, 0);
      }
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, opcode, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    mcbp::buffer_writer extra_buf(20);
    extra_buf.write_uint64(options.delta);
    extra_buf.write_uint64(options.initial_value);
    extra_buf.write_uint32(options.expiry);
    req->extras_ = std::move(extra_buf.store_);

    if (options.durability_level != couchbase::durability_level::none) {
      req->durability_level_frame_ = mcbp::durability_level_frame{
        static_cast<mcbp::durability_level>(options.durability_level)
      };
      if (options.durability_level_timeout.count() > 0) {
        req->durability_timeout_frame_ =
          mcbp::durability_timeout_frame{ options.durability_level_timeout };
      }
    }

    return dispatch_and_set_timeout(req, options);
  }

public:
  auto get(std::string scope_name,
           std::string collection_name,
           std::vector<std::byte> key,
           const get_options& options,
           get_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback)](std::shared_ptr<mcbp::queue_response> response,
                                              std::shared_ptr<mcbp::queue_request> request,
                                              std::error_code error) mutable {
      if (error) {
        get_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      if (response->extras_.size() != 4) {
        return cb({}, errc::network::protocol_error);
      }
      get_result res{};
      res.value = response->value_;
      res.flags = mcbp::big_endian::read_uint32(response->extras_, 0);
      res.cas = couchbase::cas{ response->cas_ };
      res.data_type = static_cast<std::uint8_t>(response->datatype_);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::get, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    return dispatch_and_set_timeout(req, options);
  }

  auto insert(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const insert_options& options,
              insert_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return store<insert_result>(std::move(scope_name),
                                std::move(collection_name),
                                std::move(key),
                                std::move(value),
                                options,
                                protocol::client_opcode::insert,
                                {},
                                false,
                                std::move(callback));
  }

  auto upsert(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const upsert_options& options,
              upsert_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return store<upsert_result>(std::move(scope_name),
                                std::move(collection_name),
                                std::move(key),
                                std::move(value),
                                options,
                                protocol::client_opcode::upsert,
                                {},
                                options.preserve_expiry,
                                std::move(callback));
  }

  auto replace(std::string scope_name,
               std::string collection_name,
               std::vector<std::byte> key,
               std::vector<std::byte> value,
               const replace_options& options,
               replace_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return store<replace_result>(std::move(scope_name),
                                 std::move(collection_name),
                                 std::move(key),
                                 std::move(value),
                                 options,
                                 protocol::client_opcode::replace,
                                 options.cas,
                                 options.preserve_expiry,
                                 std::move(callback));
  }

  auto remove(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              const remove_options& options,
              remove_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        remove_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      remove_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::remove, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);
    req->cas_ = options.cas.value();

    if (options.durability_level != couchbase::durability_level::none) {
      req->durability_level_frame_ = mcbp::durability_level_frame{
        static_cast<mcbp::durability_level>(options.durability_level)
      };
      if (options.durability_level_timeout.count() > 0) {
        req->durability_timeout_frame_ =
          mcbp::durability_timeout_frame{ options.durability_level_timeout };
      }
    }

    return dispatch_and_set_timeout(req, options);
  }

  auto touch(std::string scope_name,
             std::string collection_name,
             std::vector<std::byte> key,
             const touch_options& options,
             touch_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        touch_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      touch_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::touch, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    mcbp::buffer_writer extra_buf(4);
    extra_buf.write_uint32(options.expiry);
    req->extras_ = std::move(extra_buf.store_);

    return dispatch_and_set_timeout(req, options);
  }

  auto get_and_touch(std::string scope_name,
                     std::string collection_name,
                     std::vector<std::byte> key,
                     const get_and_touch_options& options,
                     get_and_touch_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback)](std::shared_ptr<mcbp::queue_response> response,
                                              std::shared_ptr<mcbp::queue_request> request,
                                              std::error_code error) {
      get_and_touch_result res{};
      if (request) {
        res.internal.retry_attempts = request->retry_attempts();
        res.internal.retry_reasons = request->retry_reasons();
      }
      if (error) {
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb(std::move(res), errc::network::protocol_error);
      }
      res.value = response->value_;
      res.flags = mcbp::big_endian::read_uint32(response->extras_, 0);
      res.cas = couchbase::cas{ response->cas_ };
      res.data_type = static_cast<std::uint8_t>(response->datatype_);
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::get_and_touch, std::move(handler));

    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    mcbp::buffer_writer extra_buf(4);
    extra_buf.write_uint32(options.expiry);
    req->extras_ = std::move(extra_buf.store_);

    return dispatch_and_set_timeout(req, options);
  }

  auto get_and_lock(std::string scope_name,
                    std::string collection_name,
                    std::vector<std::byte> key,
                    const get_and_lock_options& options,
                    get_and_lock_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback)](std::shared_ptr<mcbp::queue_response> response,
                                              std::shared_ptr<mcbp::queue_request> request,
                                              std::error_code error) {
      if (error) {
        get_and_lock_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      get_and_lock_result res{};
      res.value = response->value_;
      res.flags = mcbp::big_endian::read_uint32(response->extras_, 0);
      res.cas = couchbase::cas{ response->cas_ };
      res.data_type = static_cast<std::uint8_t>(response->datatype_);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::get_and_lock, std::move(handler));

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    mcbp::buffer_writer extra_buf(4);
    extra_buf.write_uint32(options.lock_time);
    req->extras_ = std::move(extra_buf.store_);

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  auto unlock(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              const unlock_options& options,
              unlock_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      if (error) {
        unlock_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      unlock_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::unlock, std::move(handler));

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);
    req->cas_ = options.cas.value();

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  auto get_with_meta(std::string scope_name,
                     std::string collection_name,
                     std::vector<std::byte> key,
                     const get_with_meta_options& options,
                     get_with_meta_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback)](std::shared_ptr<mcbp::queue_response> response,
                                              std::shared_ptr<mcbp::queue_request> request,
                                              std::error_code error) {
      if (error) {
        get_with_meta_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      get_with_meta_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      if (response->extras_.size() >= 12) {
        res.deleted = mcbp::big_endian::read_uint32(response->extras_, 0);
        res.flags = mcbp::big_endian::read_uint32(response->extras_, 4);
        res.expiry = mcbp::big_endian::read_uint32(response->extras_, 8);
      }
      if (response->extras_.size() >= 20) {
        res.sequence_number = mcbp::big_endian::read_uint64(response->extras_, 12);
      }
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      return cb(std::move(res), {});
    };

    auto req = std::make_shared<mcbp::queue_request>(
      protocol::magic::client_request, protocol::client_opcode::get_meta, std::move(handler));

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  auto append(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const adjoin_options& options,
              adjoin_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return adjoin<adjoin_result>(std::move(scope_name),
                                 std::move(collection_name),
                                 std::move(key),
                                 std::move(value),
                                 options,
                                 protocol::client_opcode::append,
                                 std::move(callback));
  }

  auto prepend(std::string scope_name,
               std::string collection_name,
               std::vector<std::byte> key,
               std::vector<std::byte> value,
               const adjoin_options& options,
               adjoin_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return adjoin<adjoin_result>(std::move(scope_name),
                                 std::move(collection_name),
                                 std::move(key),
                                 std::move(value),
                                 options,
                                 protocol::client_opcode::prepend,
                                 std::move(callback));
  }

  auto increment(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const counter_options& options,
                 counter_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return counter<counter_result>(std::move(scope_name),
                                   std::move(collection_name),
                                   std::move(key),
                                   options,
                                   protocol::client_opcode::increment,
                                   std::move(callback));
  }

  auto decrement(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const counter_options& options,
                 counter_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    return counter<counter_result>(std::move(scope_name),
                                   std::move(collection_name),
                                   std::move(key),
                                   options,
                                   protocol::client_opcode::decrement,
                                   std::move(callback));
  }

  auto lookup_in(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const lookup_in_options& options,
                 lookup_in_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback)](std::shared_ptr<mcbp::queue_response> response,
                                              std::shared_ptr<mcbp::queue_request> request,
                                              std::error_code error) {
      if (error &&
          static_cast<key_value_status_code>(response ? response->status_ : 0) !=
            key_value_status_code::subdoc_multi_path_failure &&
          static_cast<key_value_status_code>(response ? response->status_ : 0) !=
            key_value_status_code::subdoc_multi_path_failure_deleted) {
        lookup_in_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      std::error_code ec = error;
      if (static_cast<key_value_status_code>(response ? response->status_ : 0) ==
            key_value_status_code::subdoc_multi_path_failure ||
          static_cast<key_value_status_code>(response ? response->status_ : 0) ==
            key_value_status_code::subdoc_multi_path_failure_deleted) {
        ec = {};
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      lookup_in_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      res.internal.is_deleted = (static_cast<key_value_status_code>(response->status_) ==
                                   key_value_status_code::subdoc_success_deleted ||
                                 static_cast<key_value_status_code>(response->status_) ==
                                   key_value_status_code::subdoc_multi_path_failure_deleted);

      if (!response->value_.empty()) {
        std::size_t offset = 0;
        std::size_t res_idx = 0;
        while (offset + 6 <= response->value_.size()) {
          subdoc_result sub_res{};
          sub_res.index = res_idx++;
          std::uint16_t entry_status = mcbp::big_endian::read_uint16(response->value_, offset);
          offset += 2;
          sub_res.status = static_cast<key_value_status_code>(entry_status);
          sub_res.error = protocol::map_status_code(request->command_, entry_status);

          std::uint32_t entry_size = mcbp::big_endian::read_uint32(response->value_, offset);
          offset += 4;

          if (offset + entry_size <= response->value_.size()) {
            sub_res.value.resize(entry_size);
            std::memcpy(sub_res.value.data(), response->value_.data() + offset, entry_size);
            offset += entry_size;
          }

          res.results.emplace_back(std::move(sub_res));
        }
      }
      return cb(std::move(res), ec);
    };

    auto req = std::make_shared<mcbp::queue_request>(protocol::magic::client_request,
                                                     protocol::client_opcode::subdoc_multi_lookup,
                                                     std::move(handler));

    CB_LOG_TRACE("dispatching lookup_in request: opcode={}, ops={}",
                 static_cast<std::uint8_t>(protocol::client_opcode::subdoc_multi_lookup),
                 options.operations.size());

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);

    if (options.flags != 0) {
      req->extras_ = { static_cast<std::byte>(options.flags) };
    }

    std::size_t value_size = 0;
    for (const auto& op : options.operations) {
      value_size += 4 + op.path.size();
    }
    mcbp::buffer_writer writer(value_size);
    for (const auto& op : options.operations) {
      writer.write_byte(static_cast<std::byte>(op.opcode));
      writer.write_byte(static_cast<std::byte>(op.flags));
      writer.write_uint16(static_cast<std::uint16_t>(op.path.size()));
      writer.write(utils::to_binary(op.path));
    }
    req->value_ = std::move(writer.store_);

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  auto mutate_in(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const mutate_in_options& options,
                 mutate_in_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    bucket_name = bucket_name_](std::shared_ptr<mcbp::queue_response> response,
                                                std::shared_ptr<mcbp::queue_request> request,
                                                std::error_code error) {
      CB_LOG_TRACE("mutate_in response: error={}, status={}",
                   error.message(),
                   static_cast<std::uint16_t>(response ? response->status_ : 0));
      if (error &&
          static_cast<key_value_status_code>(response ? response->status_ : 0) !=
            key_value_status_code::subdoc_multi_path_failure &&
          static_cast<key_value_status_code>(response ? response->status_ : 0) !=
            key_value_status_code::subdoc_multi_path_failure_deleted) {
        mutate_in_result res{};
        if (request) {
          res.internal.retry_attempts = request->retry_attempts();
          res.internal.retry_reasons = request->retry_reasons();
        }
        return cb(std::move(res), error);
      }
      std::error_code ec = error;
      if (static_cast<key_value_status_code>(response ? response->status_ : 0) ==
            key_value_status_code::subdoc_multi_path_failure ||
          static_cast<key_value_status_code>(response ? response->status_ : 0) ==
            key_value_status_code::subdoc_multi_path_failure_deleted) {
        ec = {};
      }
      if (!response || !request) {
        return cb({}, errc::network::protocol_error);
      }
      mutate_in_result res{};
      res.cas = couchbase::cas{ response->cas_ };
      if (static_cast<key_value_status_code>(response->status_) == key_value_status_code::success ||
          static_cast<key_value_status_code>(response->status_) ==
            key_value_status_code::subdoc_success_deleted) {
        res.token = extract_mutation_token(response, request->vbucket_, bucket_name);
      }
      res.internal.retry_attempts = request->retry_attempts();
      res.internal.retry_reasons = request->retry_reasons();
      res.internal.is_deleted = (static_cast<key_value_status_code>(response->status_) ==
                                   key_value_status_code::subdoc_success_deleted ||
                                 static_cast<key_value_status_code>(response->status_) ==
                                   key_value_status_code::subdoc_multi_path_failure_deleted);

      std::size_t offset = 0;
      while (offset + 3 <= response->value_.size()) {
        subdoc_result sub_res{};
        sub_res.index = std::to_integer<std::uint8_t>(response->value_[offset]);
        offset += 1;

        std::uint16_t entry_status = mcbp::big_endian::read_uint16(response->value_, offset);
        offset += 2;
        sub_res.status = static_cast<key_value_status_code>(entry_status);
        sub_res.error = protocol::map_status_code(request->command_, entry_status);

        if (sub_res.error == std::error_code{}) {
          if (offset + 4 <= response->value_.size()) {
            std::uint32_t entry_size = mcbp::big_endian::read_uint32(response->value_, offset);
            offset += 4;

            if (offset + entry_size <= response->value_.size()) {
              sub_res.value.resize(entry_size);
              std::memcpy(sub_res.value.data(), response->value_.data() + offset, entry_size);
              offset += entry_size;
            }
          }
        }

        res.results.emplace_back(std::move(sub_res));
      }
      return cb(std::move(res), ec);
    };

    auto req = std::make_shared<mcbp::queue_request>(protocol::magic::client_request,
                                                     protocol::client_opcode::subdoc_multi_mutation,
                                                     std::move(handler));

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->scope_name_ = std::move(scope_name);
    req->collection_name_ = std::move(collection_name);
    req->key_ = std::move(key);
    req->cas_ = options.cas.value();

    std::size_t extra_size = 0;
    if (options.expiry != 0) {
      extra_size += 4;
    }
    if (options.flags != 0) {
      extra_size += 1;
    }
    mcbp::buffer_writer extra_writer(extra_size);
    if (options.expiry != 0) {
      extra_writer.write_uint32(options.expiry);
    }
    if (options.flags != 0) {
      extra_writer.write_byte(static_cast<std::byte>(options.flags));
    }
    req->extras_ = std::move(extra_writer.store_);

    if (options.durability_level != couchbase::durability_level::none) {
      req->durability_level_frame_ = mcbp::durability_level_frame{
        static_cast<mcbp::durability_level>(options.durability_level)
      };
      if (options.durability_level_timeout.count() > 0) {
        req->durability_timeout_frame_ =
          mcbp::durability_timeout_frame{ options.durability_level_timeout };
      }
    }

    std::size_t value_size = 0;
    for (const auto& op : options.operations) {
      value_size += 8 + op.path.size() + op.value.size();
    }
    mcbp::buffer_writer writer(value_size);
    for (const auto& op : options.operations) {
      writer.write_byte(static_cast<std::byte>(op.opcode));
      writer.write_byte(static_cast<std::byte>(op.flags));
      writer.write_uint16(static_cast<std::uint16_t>(op.path.size()));
      writer.write_uint32(static_cast<std::uint32_t>(op.value.size()));
      writer.write(utils::to_binary(op.path));
      writer.write(op.value);
    }
    req->value_ = std::move(writer.store_);

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }
    return op;
  }

  auto range_scan_create(std::uint16_t vbucket_id,
                         const range_scan_create_options& options,
                         range_scan_create_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    auto handler = [cb = std::move(callback),
                    options](std::shared_ptr<mcbp::queue_response> response,
                             std::shared_ptr<mcbp::queue_request> /* request */,
                             std::error_code error) {
      if (error) {
        return cb({}, error);
      }
      return cb(range_scan_create_result{ response->value_, options.ids_only }, {});
    };

    auto req = std::make_shared<mcbp::queue_request>(protocol::magic::client_request,
                                                     protocol::client_opcode::range_scan_create,
                                                     std::move(handler));

    req->retry_strategy_ =
      options.retry_strategy ? options.retry_strategy : default_retry_strategy_;
    req->parent_span_ = options.parent_span;
    req->datatype_ = static_cast<std::byte>(protocol::datatype::json);
    req->vbucket_ = vbucket_id;
    req->scope_name_ = options.scope_name;
    req->collection_name_ = options.collection_name;
    if (auto [value, ec] = serialize_range_scan_create_options(options); !ec) {
      req->value_ = std::move(value);
    } else {
      return tl::unexpected(ec);
    }

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }

    return op;
  }

  auto range_scan_continue(const std::vector<std::byte>& scan_uuid,
                           std::uint16_t vbucket_id,
                           const range_scan_continue_options& options,
                           range_scan_item_callback&& item_callback,
                           range_scan_continue_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    if (scan_uuid.size() != 16) {
      return tl::unexpected(errc::common::invalid_argument);
    }
    auto handler = [item_cb = std::move(item_callback), cb = std::move(callback), options](
                     const std::shared_ptr<mcbp::queue_response>& response,
                     const std::shared_ptr<mcbp::queue_request>& request,
                     std::error_code error) mutable {
      if (error) {
        // in case or error code, the request will be automatically canceled
        return cb({}, error);
      }
      if (response->extras_.size() != 4) {
        if (request->internal_cancel()) {
          cb({}, errc::network::protocol_error);
        }
        return;
      }
      const bool ids_only = mcbp::big_endian::read_uint32(response->extras_, 0) == 0;

      if (auto ec = parse_range_scan_data(response->value_, request, std::move(item_cb), ids_only);
          ec) {
        if (request->internal_cancel()) {
          cb({}, ec);
        }
        return;
      }

      const range_scan_continue_result res{
        response->status_code_ == key_value_status_code::range_scan_more,
        response->status_code_ == key_value_status_code::range_scan_complete,
        ids_only,
      };

      if ((res.more || res.complete) && request->internal_cancel()) {
        cb(res, {});
      }
    };

    auto req = std::make_shared<mcbp::queue_request>(protocol::magic::client_request,
                                                     protocol::client_opcode::range_scan_continue,
                                                     std::move(handler));

    req->persistent_ = true;
    req->vbucket_ = vbucket_id;

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }

    mcbp::buffer_writer buf{ scan_uuid.size() + (sizeof(std::uint32_t) * 3) };
    buf.write(scan_uuid);
    buf.write_uint32(options.batch_item_limit);
    buf.write_uint32(gsl::narrow_cast<std::uint32_t>(options.batch_time_limit.count()));
    buf.write_uint32(options.batch_byte_limit);
    req->extras_ = std::move(buf.store_);

    return collections_.dispatch(req);
  }

  auto range_scan_cancel(std::vector<std::byte> scan_uuid,
                         std::uint16_t vbucket_id,
                         const range_scan_cancel_options& options,
                         range_scan_cancel_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    if (scan_uuid.size() != 16) {
      return tl::unexpected(errc::common::invalid_argument);
    }
    auto handler = [cb = std::move(callback),
                    options](const std::shared_ptr<mcbp::queue_response>& /* response */,
                             const std::shared_ptr<mcbp::queue_request>& /* request */,
                             std::error_code error) mutable {
      cb({}, error);
    };

    auto req = std::make_shared<mcbp::queue_request>(protocol::magic::client_request,
                                                     protocol::client_opcode::range_scan_cancel,
                                                     std::move(handler));

    req->vbucket_ = vbucket_id;
    req->extras_ = std::move(scan_uuid);

    auto op = collections_.dispatch(req);
    if (!op) {
      return op;
    }

    if (options.timeout != std::chrono::milliseconds::zero()) {
      auto timer = std::make_shared<asio::steady_timer>(io_);
      timer->expires_after(options.timeout);
      timer->async_wait([req](auto error) {
        if (error == asio::error::operation_aborted) {
          return;
        }
        req->cancel(req->idempotent() ? couchbase::errc::common::unambiguous_timeout
                                      : couchbase::errc::common::ambiguous_timeout);
      });
      req->set_deadline(timer);
    }

    return op;
  }

private:
  asio::io_context& io_;
  std::string bucket_name_;
  collections_component collections_;
  std::shared_ptr<retry_strategy> default_retry_strategy_;
};

crud_component::crud_component(asio::io_context& io,
                               std::string bucket_name,
                               collections_component collections,
                               std::shared_ptr<retry_strategy> default_retry_strategy)
  : impl_{ std::make_shared<crud_component_impl>(io,
                                                 std::move(bucket_name),
                                                 std::move(collections),
                                                 std::move(default_retry_strategy)) }
{
}

auto
crud_component::range_scan_create(std::uint16_t vbucket_id,
                                  const range_scan_create_options& options,
                                  range_scan_create_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->range_scan_create(vbucket_id, options, std::move(callback));
}

auto
crud_component::range_scan_continue(const std::vector<std::byte>& scan_uuid,
                                    std::uint16_t vbucket_id,
                                    const range_scan_continue_options& options,
                                    range_scan_item_callback&& item_callback,
                                    range_scan_continue_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->range_scan_continue(
    scan_uuid, vbucket_id, options, std::move(item_callback), std::move(callback));
}

auto
crud_component::range_scan_cancel(std::vector<std::byte> scan_uuid,
                                  std::uint16_t vbucket_id,
                                  const range_scan_cancel_options& options,
                                  range_scan_cancel_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->range_scan_cancel(std::move(scan_uuid), vbucket_id, options, std::move(callback));
}

auto
crud_component::get(std::string scope_name,
                    std::string collection_name,
                    std::vector<std::byte> key,
                    const get_options& options,
                    get_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->get(std::move(scope_name),
                    std::move(collection_name),
                    std::move(key),
                    options,
                    std::move(callback));
}
auto
crud_component::insert(std::string scope_name,
                       std::string collection_name,
                       std::vector<std::byte> key,
                       std::vector<std::byte> value,
                       const insert_options& options,
                       insert_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->insert(std::move(scope_name),
                       std::move(collection_name),
                       std::move(key),
                       std::move(value),
                       options,
                       std::move(callback));
}

auto
crud_component::upsert(std::string scope_name,
                       std::string collection_name,
                       std::vector<std::byte> key,
                       std::vector<std::byte> value,
                       const upsert_options& options,
                       upsert_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->upsert(std::move(scope_name),
                       std::move(collection_name),
                       std::move(key),
                       std::move(value),
                       options,
                       std::move(callback));
}

auto
crud_component::replace(std::string scope_name,
                        std::string collection_name,
                        std::vector<std::byte> key,
                        std::vector<std::byte> value,
                        const replace_options& options,
                        replace_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->replace(std::move(scope_name),
                        std::move(collection_name),
                        std::move(key),
                        std::move(value),
                        options,
                        std::move(callback));
}

auto
crud_component::remove(std::string scope_name,
                       std::string collection_name,
                       std::vector<std::byte> key,
                       const remove_options& options,
                       remove_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->remove(std::move(scope_name),
                       std::move(collection_name),
                       std::move(key),
                       options,
                       std::move(callback));
}

auto
crud_component::touch(std::string scope_name,
                      std::string collection_name,
                      std::vector<std::byte> key,
                      const touch_options& options,
                      touch_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->touch(std::move(scope_name),
                      std::move(collection_name),
                      std::move(key),
                      options,
                      std::move(callback));
}

auto
crud_component::get_and_touch(std::string scope_name,
                              std::string collection_name,
                              std::vector<std::byte> key,
                              const get_and_touch_options& options,
                              get_and_touch_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->get_and_touch(std::move(scope_name),
                              std::move(collection_name),
                              std::move(key),
                              options,
                              std::move(callback));
}

auto
crud_component::get_and_lock(std::string scope_name,
                             std::string collection_name,
                             std::vector<std::byte> key,
                             const get_and_lock_options& options,
                             get_and_lock_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->get_and_lock(std::move(scope_name),
                             std::move(collection_name),
                             std::move(key),
                             options,
                             std::move(callback));
}

auto
crud_component::unlock(std::string scope_name,
                       std::string collection_name,
                       std::vector<std::byte> key,
                       const unlock_options& options,
                       unlock_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->unlock(std::move(scope_name),
                       std::move(collection_name),
                       std::move(key),
                       options,
                       std::move(callback));
}

auto
crud_component::get_with_meta(std::string scope_name,
                              std::string collection_name,
                              std::vector<std::byte> key,
                              const get_with_meta_options& options,
                              get_with_meta_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->get_with_meta(std::move(scope_name),
                              std::move(collection_name),
                              std::move(key),
                              options,
                              std::move(callback));
}

auto
crud_component::append(std::string scope_name,
                       std::string collection_name,
                       std::vector<std::byte> key,
                       std::vector<std::byte> value,
                       const adjoin_options& options,
                       adjoin_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->append(std::move(scope_name),
                       std::move(collection_name),
                       std::move(key),
                       std::move(value),
                       options,
                       std::move(callback));
}

auto
crud_component::prepend(std::string scope_name,
                        std::string collection_name,
                        std::vector<std::byte> key,
                        std::vector<std::byte> value,
                        const adjoin_options& options,
                        adjoin_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->prepend(std::move(scope_name),
                        std::move(collection_name),
                        std::move(key),
                        std::move(value),
                        options,
                        std::move(callback));
}

auto
crud_component::increment(std::string scope_name,
                          std::string collection_name,
                          std::vector<std::byte> key,
                          const counter_options& options,
                          counter_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->increment(std::move(scope_name),
                          std::move(collection_name),
                          std::move(key),
                          options,
                          std::move(callback));
}

auto
crud_component::decrement(std::string scope_name,
                          std::string collection_name,
                          std::vector<std::byte> key,
                          const counter_options& options,
                          counter_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->decrement(std::move(scope_name),
                          std::move(collection_name),
                          std::move(key),
                          options,
                          std::move(callback));
}

auto
crud_component::lookup_in(std::string scope_name,
                          std::string collection_name,
                          std::vector<std::byte> key,
                          const lookup_in_options& options,
                          lookup_in_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->lookup_in(std::move(scope_name),
                          std::move(collection_name),
                          std::move(key),
                          options,
                          std::move(callback));
}

auto
crud_component::mutate_in(std::string scope_name,
                          std::string collection_name,
                          std::vector<std::byte> key,
                          const mutate_in_options& options,
                          mutate_in_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->mutate_in(std::move(scope_name),
                          std::move(collection_name),
                          std::move(key),
                          options,
                          std::move(callback));
}

} // namespace couchbase::core
