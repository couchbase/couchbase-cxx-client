/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "bench_registry.hxx"

#include "core/cluster_label_listener.hxx"
#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/io/mcbp_command_id.hxx"
#include "core/io/retry_context.hxx"
#include "core/metrics/logging_meter.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/topology/configuration.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/cas.hxx>

#include <asio/io_context.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

namespace couchbase::test::bench
{
namespace
{
auto
configured_label_listener() -> std::shared_ptr<core::cluster_label_listener>
{
  auto listener = std::make_shared<core::cluster_label_listener>();
  core::topology::configuration cfg{};
  cfg.cluster_name = "bench-cluster";
  cfg.cluster_uuid = "00000000-0000-4000-8000-000000000001";
  listener->update_config(std::move(cfg));
  return listener;
}

template<typename Wrapper>
class stop_wrapper_on_exit
{
public:
  explicit stop_wrapper_on_exit(Wrapper& wrapper)
    : wrapper_{ wrapper }
  {
    wrapper_.start();
  }

  stop_wrapper_on_exit(const stop_wrapper_on_exit&) = delete;
  stop_wrapper_on_exit(stop_wrapper_on_exit&&) = delete;
  auto operator=(const stop_wrapper_on_exit&) -> stop_wrapper_on_exit& = delete;
  auto operator=(stop_wrapper_on_exit&&) -> stop_wrapper_on_exit& = delete;

  ~stop_wrapper_on_exit()
  {
    wrapper_.stop();
  }

private:
  Wrapper& wrapper_;
};

void
meter_wrapper_logging(state& st)
{
  asio::io_context io{};
  auto listener = configured_label_listener();
  auto meter =
    std::make_shared<core::metrics::logging_meter>(io, core::metrics::logging_meter_options{});
  core::metrics::meter_wrapper wrapper{ meter, listener };
  const stop_wrapper_on_exit stop{ wrapper };

  const core::metrics::metric_attributes attrs{
    "kv", "get", {}, "default", "_default", "_default",
  };
  constexpr std::chrono::microseconds operation_latency{ 100 };
  // Warm the recorder cache before measuring steady-state recording.
  wrapper.record_value(attrs, std::chrono::steady_clock::now() - operation_latency);

  for (const std::size_t threads : { std::size_t{ 1 }, std::size_t{ 8 } }) {
    st.batched("t" + std::to_string(threads), threads, [&] {
      wrapper.record_value(attrs, std::chrono::steady_clock::now() - operation_latency);
    });
  }
}

void
meter_wrapper_noop(state& st)
{
  auto listener = configured_label_listener();
  auto meter = std::make_shared<core::metrics::noop_meter>();
  core::metrics::meter_wrapper wrapper{ meter, listener };
  const stop_wrapper_on_exit stop{ wrapper };

  const core::metrics::metric_attributes attrs{
    "kv", "get", {}, "default", "_default", "_default",
  };
  constexpr std::chrono::microseconds operation_latency{ 100 };

  st.batched("t1", 1, [&] {
    wrapper.record_value(attrs, std::chrono::steady_clock::now() - operation_latency);
  });
}

void
tracer_wrapper_threshold(state& st)
{
  asio::io_context io{};
  auto listener = configured_label_listener();
  auto tracer = std::make_shared<core::tracing::threshold_logging_tracer>(
    io, core::tracing::threshold_logging_options{});
  core::tracing::tracer_wrapper wrapper{ tracer, listener };
  const stop_wrapper_on_exit stop{ wrapper };

  for (const std::size_t threads : { std::size_t{ 1 }, std::size_t{ 8 } }) {
    st.batched("t" + std::to_string(threads), threads, [&] {
      auto span = wrapper.create_span(core::tracing::operation::mcbp_get, {});
      span->add_tag(core::tracing::attributes::op::service, core::tracing::service::key_value);
      span->end();
      do_not_optimize(span);
    });
  }
}

struct stub_session {
  [[nodiscard]] auto decode_error_code(std::uint16_t /* code */) const
    -> std::optional<core::key_value_error_map_info>
  {
    return std::nullopt;
  }

  [[nodiscard]] auto node_uuid() const -> const std::string&
  {
    return uuid_;
  }

  [[nodiscard]] auto canonical_hostname() const -> const std::string&
  {
    return hostname_;
  }

  [[nodiscard]] auto canonical_port_number() const -> std::uint16_t
  {
    return 11210;
  }

  std::string uuid_{ "00000000-0000-4000-8000-000000000001" };
  std::string hostname_{ "127.0.0.1" };
};

struct stub_command {
  struct request_type {
    core::document_id id{ "default", "_default", "_default", "bench::0000000000000001::42" };
    std::uint16_t partition{ 7 };
    std::uint32_t opaque{ 42U };
    core::io::retry_context<true> retries{};
  };

  request_type request{};
  std::optional<stub_session> session_{ stub_session{} };
  std::optional<std::string> last_dispatched_to_{ "127.0.0.1:11210" };
  std::optional<std::string> last_dispatched_from_{ "127.0.0.1:54321" };
  // A completion that observed a published attempt.
  bool attempt_readable_{ true };
  mutable std::string id_{};

  [[nodiscard]] auto id() const -> const std::string&
  {
    if (id_.empty()) {
      id_ = core::operations::make_command_id(core::protocol::client_opcode::get);
    }
    return id_;
  }

  [[nodiscard]] auto built_id() const -> std::string
  {
    return id_;
  }
};

struct stub_response {
  [[nodiscard]] auto opcode() const -> core::protocol::client_opcode
  {
    return core::protocol::client_opcode::get;
  }

  [[nodiscard]] auto opaque() const -> std::uint32_t
  {
    return 42U;
  }

  [[nodiscard]] auto status() const -> core::key_value_status_code
  {
    return core::key_value_status_code::success;
  }

  [[nodiscard]] auto cas() const -> couchbase::cas
  {
    return couchbase::cas{ 100U };
  }

  [[nodiscard]] auto error_info() const -> std::optional<core::key_value_extended_error_info>
  {
    return std::nullopt;
  }
};

void
kv_error_context_success(state& st)
{
  stub_command cmd{};
  const stub_response resp{};

  st.batched("t1", 1, [&] {
    cmd.id_.clear();
    cmd.last_dispatched_to_ = "127.0.0.1:11210";
    cmd.last_dispatched_from_ = "127.0.0.1:54321";
    auto ctx =
      core::make_key_value_error_context(std::error_code{}, std::uint16_t{ 0 }, &cmd, resp);
    do_not_optimize(ctx);
  });
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return {
    suite_name,
    {
      { CASE(meter_wrapper_logging),
        {},
        "Records a cached KV metric through meter_wrapper and logging_meter on 1 and 8 threads." },
      { CASE(meter_wrapper_noop),
        {},
        "Records a KV metric through meter_wrapper when metrics use noop_meter." },
      { CASE(tracer_wrapper_threshold),
        {},
        "Creates and ends a KV span through tracer_wrapper and threshold_logging_tracer on 1 and 8 "
        "threads." },
      { CASE(kv_error_context_success),
        {},
        "Builds key_value_error_context for a successful KV response." },
    },
  };
}
} // namespace couchbase::test::bench
