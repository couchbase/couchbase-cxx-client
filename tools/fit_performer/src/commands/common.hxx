/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#pragma once

#include "../exceptions.hxx"
#include "run.top_level.pb.h"
#include "shared.basic.pb.h"
#include "shared.content.pb.h"

#include <core/utils/json.hxx>

#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/json_transcoder.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/raw_json_transcoder.hxx>
#include <couchbase/codec/raw_string_transcoder.hxx>
#include <couchbase/codec/serializer_traits.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/error.hxx>
#include <couchbase/mutation_state.hxx>
#include <couchbase/persist_to.hxx>
#include <couchbase/replicate_to.hxx>

#include <google/protobuf/timestamp.pb.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ostr.h>

#include <functional>
#include <string>
#include <system_error>
#include <variant>
#include <vector>

namespace fit_cxx::commands::common
{
template<class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};

template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

auto
create_new_result() -> protocol::run::Result;

class custom_json_serializer
{
public:
  using document_type = tao::json::value;

  template<typename Document>
  static auto serialize(Document document) -> couchbase::codec::binary
  {
    auto json = couchbase::codec::tao_json_serializer::deserialize<tao::json::value>(
      couchbase::codec::tao_json_serializer::serialize(document));
    json["Serialized"] = true;
    return couchbase::codec::tao_json_serializer::serialize(json);
  }

  template<typename Document>
  static auto deserialize(const couchbase::codec::binary& data) -> Document
  {
    auto json = couchbase::codec::tao_json_serializer::deserialize<tao::json::value>(data);
    json["Serialized"] = false;
    return couchbase::codec::tao_json_serializer::deserialize<Document>(
      couchbase::codec::tao_json_serializer::serialize(json));
  }
};

using content = std::variant<std::nullptr_t, tao::json::value, std::string, std::vector<std::byte>>;

using transcoder = std::variant<std::monostate,
                                couchbase::codec::raw_json_transcoder,
                                couchbase::codec::raw_string_transcoder,
                                couchbase::codec::default_json_transcoder,
                                couchbase::codec::raw_binary_transcoder>;

using serializer = std::variant<couchbase::codec::tao_json_serializer, custom_json_serializer>;

void
convert_error_code(std::error_code ec, protocol::shared::Exception* exception);

auto
to_durability_level(protocol::shared::Durability level) -> couchbase::durability_level;

auto
to_persist_to(protocol::shared::ObserveBased observe) -> couchbase::persist_to;

auto
to_replicate_to(protocol::shared::ObserveBased observe) -> couchbase::replicate_to;

auto
from_durability_level(couchbase::durability_level level) -> protocol::shared::Durability;

auto
to_content(const protocol::shared::Content& proto_content) -> content;

auto
to_binary(const std::string& value) -> std::vector<std::byte>;

template<typename Cmd>
auto
to_transcoder(const Cmd& cmd) -> transcoder
{
  if (!cmd.has_options() || !cmd.options().has_transcoder()) {
    return std::monostate{};
  }
  protocol::shared::Transcoder proto_transcoder = cmd.options().transcoder();
  switch (proto_transcoder.transcoder_case()) {
    case protocol::shared::Transcoder::kJson:
      return couchbase::codec::default_json_transcoder{};
    case protocol::shared::Transcoder::kRawBinary:
      return couchbase::codec::raw_binary_transcoder{};
    case protocol::shared::Transcoder::kRawJson:
      return couchbase::codec::raw_json_transcoder{};
    case protocol::shared::Transcoder::kRawString:
      return couchbase::codec::raw_string_transcoder{};
    default:
      throw performer_exception::unimplemented(
        fmt::format("transcoder not supported {}", proto_transcoder.DebugString()));
  }
}

template<typename Spec>
auto
to_spec_transcoder(const Spec& spec) -> transcoder
{
  if (!spec.has_transcoder()) {
    return std::monostate{};
  }
  protocol::shared::Transcoder proto_transcoder = spec.transcoder();
  switch (proto_transcoder.transcoder_case()) {
    case protocol::shared::Transcoder::kJson:
      return couchbase::codec::default_json_transcoder{};
    case protocol::shared::Transcoder::kRawBinary:
      return couchbase::codec::raw_binary_transcoder{};
    case protocol::shared::Transcoder::kRawJson:
      return couchbase::codec::raw_json_transcoder{};
    case protocol::shared::Transcoder::kRawString:
      return couchbase::codec::raw_string_transcoder{};
    default:
      throw performer_exception::unimplemented(
        fmt::format("transcoder not supported {}", proto_transcoder.DebugString()));
  }
}

template<typename Result>
void
result_to_content(const Result& result,
                  const transcoder& transcoder,
                  const protocol::shared::ContentAs& proto_content_as,
                  protocol::shared::ContentTypes* proto_res_content)
{
  std::visit(
    overloaded{
      [&](std::monostate) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(result.template content_as<std::string>());
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>>();
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          case protocol::shared::ContentAs::kAsJsonObject:
          case protocol::shared::ContentAs::kAsJsonArray:
            proto_res_content->set_content_as_bytes(couchbase::core::utils::json::generate(
              result.template content_as<tao::json::value>()));
            break;
          case protocol::shared::ContentAs::kAsBoolean:
            proto_res_content->set_content_as_bool(result.template content_as<bool>());
            break;
          case protocol::shared::ContentAs::kAsInteger:
            proto_res_content->set_content_as_int64(result.template content_as<std::int64_t>());
            break;
          case protocol::shared::ContentAs::kAsFloatingPoint:
            proto_res_content->set_content_as_double(result.template content_as<double>());
            break;
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::default_json_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>());
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>();
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          case protocol::shared::ContentAs::kAsJsonObject:
          case protocol::shared::ContentAs::kAsJsonArray:
            proto_res_content->set_content_as_bytes(couchbase::core::utils::json::generate(
              result.template content_as<tao::json::value>()));
            break;
          case protocol::shared::ContentAs::kAsBoolean: {
            proto_res_content->set_content_as_bool(result.template content_as<bool, decltype(t)>());
            break;
          }
          case protocol::shared::ContentAs::kAsInteger: {
            proto_res_content->set_content_as_int64(
              result.template content_as<std::int64_t, decltype(t)>());
            break;
          }
          case protocol::shared::ContentAs::kAsFloatingPoint: {
            proto_res_content->set_content_as_double(
              result.template content_as<double, decltype(t)>());
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_binary_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>();
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_json_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>());
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>();
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_string_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>());
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](auto /* t */) {
        throw performer_exception::unimplemented(
          "unsupported transcoder - content type combination");
      } },
    transcoder);
}

template<typename Result>
void
multi_result_to_content(const Result& result,
                        std::size_t idx,
                        const transcoder& transcoder,
                        const protocol::shared::ContentAs& proto_content_as,
                        protocol::shared::ContentTypes* proto_res_content)
{
  std::visit(
    overloaded{
      [&](std::monostate) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(result.template content_as<std::string>(idx));
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>>(idx);
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          case protocol::shared::ContentAs::kAsJsonObject:
          case protocol::shared::ContentAs::kAsJsonArray:
            proto_res_content->set_content_as_bytes(couchbase::core::utils::json::generate(
              result.template content_as<tao::json::value>(idx)));
            break;
          case protocol::shared::ContentAs::kAsBoolean:
            proto_res_content->set_content_as_bool(result.template content_as<bool>(idx));
            break;
          case protocol::shared::ContentAs::kAsInteger:
            proto_res_content->set_content_as_int64(result.template content_as<std::int64_t>(idx));
            break;
          case protocol::shared::ContentAs::kAsFloatingPoint:
            proto_res_content->set_content_as_double(result.template content_as<double>(idx));
            break;
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::default_json_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>(idx));
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>(idx);
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          case protocol::shared::ContentAs::kAsJsonObject:
          case protocol::shared::ContentAs::kAsJsonArray:
            proto_res_content->set_content_as_bytes(couchbase::core::utils::json::generate(
              result.template content_as<tao::json::value>(idx)));
            break;
          case protocol::shared::ContentAs::kAsBoolean: {
            proto_res_content->set_content_as_bool(
              result.template content_as<bool, decltype(t)>(idx));
            break;
          }
          case protocol::shared::ContentAs::kAsInteger: {
            proto_res_content->set_content_as_int64(
              result.template content_as<std::int64_t, decltype(t)>(idx));
            break;
          }
          case protocol::shared::ContentAs::kAsFloatingPoint: {
            proto_res_content->set_content_as_double(
              result.template content_as<double, decltype(t)>(idx));
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_binary_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>(idx);
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_json_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>(idx));
            break;
          }
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.template content_as<std::vector<std::byte>, decltype(t)>(idx);
            proto_res_content->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](couchbase::codec::raw_string_transcoder t) {
        switch (proto_content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString: {
            proto_res_content->set_content_as_string(
              result.template content_as<std::string, decltype(t)>(idx));
            break;
          }
          default:
            throw performer_exception::unimplemented(
              "unsupported transcoder - content type combination");
        }
      },
      [&](auto /* t */) {
        throw performer_exception::unimplemented(
          "unsupported transcoder - content type combination");
      } },
    transcoder);
}

template<typename Result>
void
subdoc_result_to_content(const Result& result,
                         const std::size_t idx,
                         const protocol::shared::ContentAs& proto_content_as,
                         protocol::shared::ContentTypes* proto_res_content)
{
  if (proto_content_as.has_as_string()) {
    try {
      proto_res_content->set_content_as_string(result.template content_as<std::string>(idx));
    } catch (const std::logic_error&) {
      auto content =
        couchbase::core::utils::json::generate(result.template content_as<tao::json::value>(idx));
      proto_res_content->set_content_as_string(content);
    }
  } else if (proto_content_as.has_as_byte_array()) {
    try {
      auto content = result.template content_as<tao::binary>(idx);
      proto_res_content->set_content_as_bytes(reinterpret_cast<const char*>(content.data()),
                                              content.size());
    } catch (const std::logic_error&) {
      auto content =
        couchbase::core::utils::json::generate(result.template content_as<tao::json::value>(idx));
      proto_res_content->set_content_as_bytes(content);
    }
  } else if (proto_content_as.has_as_boolean()) {
    proto_res_content->set_content_as_bool(result.template content_as<bool>(idx));
  } else if (proto_content_as.has_as_floating_point()) {
    proto_res_content->set_content_as_double(result.template content_as<double>(idx));
  } else if (proto_content_as.has_as_integer()) {
    proto_res_content->set_content_as_int64(result.template content_as<std::int64_t>(idx));
  } else if (proto_content_as.has_as_json_array() || proto_content_as.has_as_json_object()) {
    auto content =
      couchbase::core::utils::json::generate(result.template content_as<tao::json::value>(idx));
    proto_res_content->set_content_as_bytes(content);
  }
}

void
validate_content_as(const protocol::shared::ContentAsPerformerValidation& validation,
                    const std::function<protocol::shared::ContentTypes()>& content_fn);

void
validate_content(const protocol::shared::ContentTypes& content,
                 const protocol::shared::ContentAs& content_as,
                 const std::vector<std::byte>& expected_content_bytes);

void
convert_error(const couchbase::error& err, protocol::shared::Exception* exception);

auto
to_mutation_state(const protocol::shared::MutationState& state) -> couchbase::mutation_state;

auto
to_time_point(const google::protobuf::Timestamp& timestamp)
  -> std::chrono::system_clock::time_point;
} // namespace fit_cxx::commands::common

namespace couchbase::codec
{
template<>
struct is_serializer<fit_cxx::commands::common::custom_json_serializer> : public std::true_type {
};
} // namespace couchbase::codec
