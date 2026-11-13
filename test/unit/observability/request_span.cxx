/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "framework/test_registry.hxx"

#include "core/tracing/constants.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"

#include <couchbase/tracing/request_span.hxx>

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <optional>
#include <string>
#include <thread>

namespace
{
// Sanitizer builds intercept allocation and provide their own operator new/delete, so this counter
// and the overrides below are compiled out under sanitizers (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED),
// along with the two allocation-count cases; the functional capture behaviour is covered elsewhere.
//
// Per thread, not process-wide: the framework runs each case on its own worker thread while other
// threads -- the runner, and any worker detached by an earlier case that blew its budget -- are
// free to allocate. A shared counter would attribute those to the case and fail an assertion that
// nothing was allocated. Constant-initialised, so reading it inside operator new allocates nothing.
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
thread_local long g_alloc_count{ 0 };
#endif
} // namespace

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void*
operator new(std::size_t n)
{
  ++g_alloc_count;
  void* p = std::malloc(n != 0 ? n : 1);
  if (p == nullptr) {
    throw std::bad_alloc{};
  }
  return p;
}

void
operator delete(void* p) noexcept
{
  std::free(p);
}

void
operator delete(void* p, std::size_t) noexcept
{
  std::free(p);
}
#endif

namespace couchbase::test
{
namespace
{
// A span that implements only the required interface — it does not opt into typed dispatch capture,
// so it must behave like any external tracer's span.
class minimal_span : public couchbase::tracing::request_span
{
public:
  void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
  {
  }
  void add_tag(const std::string& /* name */, const std::string& /* value */) override
  {
  }
  void end() override
  {
  }
};

// A span that opts into typed dispatch capture and records the captured values so the case can
// assert the SDK routed them through the typed path.
class capturing_span : public minimal_span
{
public:
  std::optional<std::uint32_t> captured_operation_id{};
  std::optional<std::string> captured_local_id{};
  std::optional<std::uint64_t> captured_server_duration_us{};
  std::optional<std::string> captured_peer_address{};
  std::optional<std::uint16_t> captured_peer_port{};

  auto try_set_dispatch_operation_id(std::uint32_t opaque) -> bool override
  {
    captured_operation_id = opaque;
    return true;
  }

  auto try_set_dispatch_local_id(const std::string& local_id) -> bool override
  {
    captured_local_id = local_id;
    return true;
  }

  auto try_set_dispatch_result(std::uint64_t server_duration_us,
                               const std::string& peer_address,
                               std::uint16_t peer_port) -> bool override
  {
    captured_server_duration_us = server_duration_us;
    captured_peer_address = peer_address;
    captured_peer_port = peer_port;
    return true;
  }
};

// A span whose typed setters record nothing but a flag — used to isolate the cost of the call
// itself (the tag-name temporary) from any value copy the capture would otherwise perform.
class flag_only_span : public minimal_span
{
public:
  bool touched{ false };

  auto try_set_dispatch_local_id(const std::string& /* local_id */) -> bool override
  {
    touched = true;
    return true;
  }

  auto try_set_dispatch_result(std::uint64_t /* server_duration_us */,
                               const std::string& /* peer_address */,
                               std::uint16_t /* peer_port */) -> bool override
  {
    touched = true;
    return true;
  }
};

void
a_span_does_not_capture_the_typed_dispatch_operation_id_by_default([[maybe_unused]] context& ctx)
{
  minimal_span span;
  // External tracers rely on this default being false so the SDK records the operation id as a
  // string tag instead.
  assert_false(span.try_set_dispatch_operation_id(0x1a), "the default declines the typed capture");
}

void
a_span_may_opt_into_capturing_the_typed_dispatch_operation_id([[maybe_unused]] context& ctx)
{
  capturing_span span;
  assert_true(span.try_set_dispatch_operation_id(0x1a), "an opted-in span accepts the capture");
  assert_true(span.captured_operation_id == 0x1a, "the raw opaque reaches the span");
}

void
a_captured_dispatch_operation_id_formats_to_the_reported_string([[maybe_unused]] context& ctx)
{
  using couchbase::core::tracing::format_dispatch_operation_id;
  // The reporting path turns the captured raw opaque into exactly the "0x<hex>" string that used to
  // be built eagerly as a tag. Cover the boundaries, including the widest value (10 chars) that
  // must still fit the fixed formatting buffer.
  assert_eq(format_dispatch_operation_id(0x1a), "0x1a", "a typical opaque");
  assert_eq(format_dispatch_operation_id(0), "0x0", "the smallest opaque");
  assert_eq(format_dispatch_operation_id(0xffffffff), "0xffffffff", "the widest opaque");
}

void
a_span_does_not_capture_typed_dispatch_metadata_by_default([[maybe_unused]] context& ctx)
{
  minimal_span span;
  // External tracers rely on these defaults being false so the SDK falls back to string tags.
  assert_false(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"),
               "the default declines the local id");
  assert_false(span.try_set_dispatch_result(120, "192.168.1.5", 11210),
               "the default declines the dispatch result");
}

void
a_span_may_opt_into_capturing_typed_dispatch_metadata([[maybe_unused]] context& ctx)
{
  capturing_span span;

  assert_true(span.try_set_dispatch_local_id("66388CF5BFCF7522/18CC8791579B567C"),
              "an opted-in span accepts the local id");
  assert_true(span.captured_local_id == "66388CF5BFCF7522/18CC8791579B567C",
              "the local id reaches the span");

  assert_true(span.try_set_dispatch_result(120, "192.168.1.5", 11210),
              "an opted-in span accepts the dispatch result");
  assert_true(span.captured_server_duration_us == 120, "the server duration reaches the span");
  assert_true(span.captured_peer_address == "192.168.1.5", "the peer address reaches the span");
  assert_true(span.captured_peer_port == 11210, "the peer port reaches the span");
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void
the_typed_local_id_setter_avoids_the_tag_name_allocation([[maybe_unused]] context& ctx)
{
  flag_only_span span;

  // The string tag path materializes a std::string for the tag name from the const char* constant,
  // which may heap-allocate; the typed setter takes the value directly and never constructs a
  // tag-name temporary. Assert the invariant -- the typed path allocates nothing and is never worse
  // than the tag path -- rather than an absolute tag-path count: whether "couchbase.local_id"
  // heap-allocates depends on the standard library's SSO capacity (libc++'s larger SSO keeps it
  // inline) and on allocation instrumentation being active, so a fixed ">= 1" is not portable.
  const long before_tag = g_alloc_count;
  span.add_tag(couchbase::core::tracing::attributes::dispatch::local_id, "x");
  const long tag_allocs = g_alloc_count - before_tag;

  const long before_typed = g_alloc_count;
  const bool captured = span.try_set_dispatch_local_id("x");
  const long typed_allocs = g_alloc_count - before_typed;

  assert_true(captured, "the typed setter is the path taken");
  assert_eq(typed_allocs, 0L, "the typed local id setter allocates nothing");
  assert_true(typed_allocs <= tag_allocs, "the typed path is never worse than the tag path");
}

void
the_typed_result_setter_avoids_the_tag_name_allocations([[maybe_unused]] context& ctx)
{
  flag_only_span span;

  // Each of the three string-tag names (server_duration/peer_address/peer_port) may allocate a
  // temporary std::string for the name; the single typed call captures all three with no tag-name
  // temporaries. As above, assert the invariant (typed path allocates nothing and is never worse
  // than the tag path) rather than an absolute count, since how many of the three names exceed the
  // SSO limit depends on the standard library and instrumentation.
  const long before_tags = g_alloc_count;
  span.add_tag(couchbase::core::tracing::attributes::dispatch::server_duration,
               static_cast<std::uint64_t>(120));
  span.add_tag(couchbase::core::tracing::attributes::dispatch::peer_address, "p");
  span.add_tag(couchbase::core::tracing::attributes::dispatch::peer_port,
               static_cast<std::uint64_t>(11210));
  const long tag_allocs = g_alloc_count - before_tags;

  const long before_typed = g_alloc_count;
  const bool captured = span.try_set_dispatch_result(120, "p", 11210);
  const long typed_allocs = g_alloc_count - before_typed;

  assert_true(captured, "the typed setter is the path taken");
  assert_eq(typed_allocs, 0L, "the typed result setter allocates nothing");
  assert_true(typed_allocs <= tag_allocs, "the typed path is never worse than the tag path");
}
#endif

void
wait_for_nonzero_system_clock_us()
{
  const auto before = std::chrono::system_clock::now();
  while (
    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - before)
      .count() < 2) {
    std::this_thread::yield();
  }
}

void
threshold_tracer_rolls_up_dispatch_attributes_and_ignores_late_children(
  [[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds{ 0 };
  opts.threshold_sample_size = 16;
  auto tracer = std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);

  auto op_span = tracer->start_span("upsert", nullptr);
  op_span->add_tag(couchbase::core::tracing::attributes::op::service,
                   couchbase::core::tracing::service::key_value);

  auto first = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  first->add_tag(couchbase::core::tracing::attributes::dispatch::operation_id, "0x10");
  assert_true(first->try_set_dispatch_local_id("conn-first"), "captures first local id");
  assert_true(first->try_set_dispatch_result(120, "node-a.example", 11210),
              "captures first result");
  first->end();

  auto second = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  assert_true(second->try_set_dispatch_operation_id(0x2b), "captures second typed opaque");
  assert_true(second->try_set_dispatch_local_id("conn-second"), "captures second local id");
  assert_true(second->try_set_dispatch_result(80, "node-b.example", 11210),
              "captures second result");
  second->end();

  auto late = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  assert_true(late->try_set_dispatch_operation_id(0xdead), "captures late opaque");
  assert_true(late->try_set_dispatch_local_id("conn-late"), "captures late local id");
  assert_true(late->try_set_dispatch_result(500, "node-late.example", 11210),
              "captures late result");

  wait_for_nonzero_system_clock_us();
  op_span->end();
  late->end();
  op_span->end();

  const auto report = tracer->flush_and_create_output().value_or(std::string{});
  assert_contains(report, R"("count":1)", "operation span is reported once");
  assert_contains(report, R"("last_operation_id":"0x2b")", "typed opaque supersedes string tag");
  assert_contains(report, R"("last_local_id":"conn-second")", "reports second local id");
  assert_contains(
    report, R"("last_remote_socket":"node-b.example:11210")", "reports second socket");
  assert_contains(report, R"("last_server_duration_us":80)", "reports second server duration");
  assert_contains(report, R"("total_server_duration_us":200)", "sums both completed dispatches");
}

void
threshold_tracer_handles_concurrent_child_and_parent_end_without_a_race(
  [[maybe_unused]] context& ctx)
{
  constexpr int rounds = 300;
  asio::io_context io{};
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds{ 0 };
  opts.threshold_sample_size = static_cast<std::size_t>(rounds) * 2;
  auto tracer = std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);

  for (int round = 0; round < rounds; ++round) {
    auto op_span = tracer->start_span("get_any_replica", nullptr);
    op_span->add_tag(couchbase::core::tracing::attributes::op::service,
                     couchbase::core::tracing::service::key_value);

    auto child_a = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
    (void)child_a->try_set_dispatch_operation_id(0x100U + static_cast<std::uint32_t>(round));
    (void)child_a->try_set_dispatch_local_id("replica-connection-alpha-long-identifier");
    (void)child_a->try_set_dispatch_result(95, "replica-alpha.example.internal", 11210);

    auto child_b = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
    (void)child_b->try_set_dispatch_operation_id(0x200U + static_cast<std::uint32_t>(round));
    (void)child_b->try_set_dispatch_local_id("replica-connection-bravo-long-identifier");
    (void)child_b->try_set_dispatch_result(140, "replica-bravo.example.internal", 11210);

    wait_for_nonzero_system_clock_us();

    std::atomic<int> ready{ 0 };
    auto wait_all = [&ready] {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (ready.load(std::memory_order_acquire) < 3) {
        std::this_thread::yield();
      }
    };

    std::thread one([&child_a, &wait_all] {
      wait_all();
      child_a->end();
    });
    std::thread two([&child_b, &wait_all] {
      wait_all();
      child_b->end();
    });
    wait_all();
    op_span->end();

    one.join();
    two.join();
  }

  const auto report = tracer->flush_and_create_output().value_or(std::string{});
  assert_contains(report, R"("count":)" + std::to_string(rounds), "every parent span reports once");
}

void
threshold_tracer_preserves_all_concurrent_dispatch_children([[maybe_unused]] context& ctx)
{
  constexpr int rounds = 200;
  asio::io_context io{};
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds{ 0 };
  opts.threshold_sample_size = 16;
  auto tracer = std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);

  for (int round = 0; round < rounds; ++round) {
    auto op_span = tracer->start_span("get_all_replicas", nullptr);
    op_span->add_tag(couchbase::core::tracing::attributes::op::service,
                     couchbase::core::tracing::service::key_value);

    auto child_a = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
    (void)child_a->try_set_dispatch_operation_id(0x101U);
    (void)child_a->try_set_dispatch_local_id("conn-alpha");
    (void)child_a->try_set_dispatch_result(95, "alpha.example.internal", 11210);

    auto child_b = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
    (void)child_b->try_set_dispatch_operation_id(0x202U);
    (void)child_b->try_set_dispatch_local_id("conn-bravo");
    (void)child_b->try_set_dispatch_result(140, "bravo.example.internal", 11210);

    wait_for_nonzero_system_clock_us();

    std::atomic<int> ready{ 0 };
    auto wait_both = [&ready] {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
      }
    };

    std::thread one([&child_a, &wait_both] {
      wait_both();
      child_a->end();
    });
    std::thread two([&child_b, &wait_both] {
      wait_both();
      child_b->end();
    });
    one.join();
    two.join();

    op_span->end();

    const auto report = tracer->flush_and_create_output().value_or(std::string{});
    assert_contains(
      report, R"("total_server_duration_us":235)", "sums both concurrent dispatch children");
    const bool alpha_last =
      report.find(R"("last_server_duration_us":95)") != std::string::npos &&
      report.find(R"("last_operation_id":"0x101")") != std::string::npos &&
      report.find(R"("last_local_id":"conn-alpha")") != std::string::npos &&
      report.find(R"("last_remote_socket":"alpha.example.internal:11210")") != std::string::npos;
    const bool bravo_last =
      report.find(R"("last_server_duration_us":140)") != std::string::npos &&
      report.find(R"("last_operation_id":"0x202")") != std::string::npos &&
      report.find(R"("last_local_id":"conn-bravo")") != std::string::npos &&
      report.find(R"("last_remote_socket":"bravo.example.internal:11210")") != std::string::npos;
    assert_true(alpha_last != bravo_last, "reports one coherent last child without dropping it");
  }
}

void
threshold_tracer_releases_completed_dispatch_children_before_parent_ends(
  [[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  couchbase::core::tracing::threshold_logging_options opts{};
  opts.key_value_threshold = std::chrono::milliseconds{ 0 };
  opts.threshold_sample_size = 16;
  auto tracer = std::make_shared<couchbase::core::tracing::threshold_logging_tracer>(io, opts);

  auto op_span = tracer->start_span("upsert", nullptr);
  op_span->add_tag(couchbase::core::tracing::attributes::op::service,
                   couchbase::core::tracing::service::key_value);

  constexpr std::uint32_t retry_attempts = 32;
  for (std::uint32_t i = 1; i <= retry_attempts; ++i) {
    auto attempt = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
    std::weak_ptr<couchbase::tracing::request_span> weak_attempt = attempt;
    (void)attempt->try_set_dispatch_operation_id(0x100U + i);
    (void)attempt->try_set_dispatch_local_id("conn-retry-" + std::to_string(i));
    (void)attempt->try_set_dispatch_result(10, "node-retry.example", 11210);
    attempt->end();
    attempt.reset();
    assert_true(weak_attempt.expired(), "ended retry child is released before parent ends");
  }

  auto cancelled = tracer->start_span(couchbase::core::tracing::operation::step_dispatch, op_span);
  std::weak_ptr<couchbase::tracing::request_span> weak_cancelled = cancelled;
  (void)cancelled->try_set_dispatch_operation_id(0x3ffU);
  (void)cancelled->try_set_dispatch_local_id("conn-cancelled");
  cancelled->end();
  cancelled.reset();
  assert_true(weak_cancelled.expired(), "ended cancelled child is released before parent ends");

  wait_for_nonzero_system_clock_us();
  op_span->end();

  const auto report = tracer->flush_and_create_output().value_or(std::string{});
  assert_contains(report, R"("count":1)", "operation span is reported once");
  assert_contains(report, R"("total_server_duration_us":320)", "sums every retry server duration");
  assert_contains(report, R"("last_server_duration_us":10)", "keeps last answered server duration");
  assert_contains(
    report, R"("last_remote_socket":"node-retry.example:11210")", "keeps last answered socket");
  assert_contains(report, R"("last_operation_id":"0x3ff")", "keeps latest attempt opaque");
  assert_contains(report, R"("last_local_id":"conn-cancelled")", "keeps latest attempt local id");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_span_does_not_capture_the_typed_dispatch_operation_id_by_default) },
      { CASE(a_span_may_opt_into_capturing_the_typed_dispatch_operation_id) },
      { CASE(a_captured_dispatch_operation_id_formats_to_the_reported_string) },
      { CASE(a_span_does_not_capture_typed_dispatch_metadata_by_default) },
      { CASE(a_span_may_opt_into_capturing_typed_dispatch_metadata) },
      { CASE(threshold_tracer_rolls_up_dispatch_attributes_and_ignores_late_children) },
      { CASE(threshold_tracer_handles_concurrent_child_and_parent_end_without_a_race) },
      { CASE(threshold_tracer_preserves_all_concurrent_dispatch_children) },
      { CASE(threshold_tracer_releases_completed_dispatch_children_before_parent_ends) },
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
      { CASE(the_typed_local_id_setter_avoids_the_tag_name_allocation) },
      { CASE(the_typed_result_setter_avoids_the_tag_name_allocations) },
#endif
    },
  };
}

} // namespace couchbase::test
