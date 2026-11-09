/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "benchmark/bench_runner.hxx"
#include "benchmark/report.hxx"
#include "benchmark/statistics.hxx"

#include "framework/test_registry.hxx"

#include <tao/json.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace bench;

void
histogram_percentiles_within_one_percent([[maybe_unused]] context& ctx)
{
  histogram h;
  for (std::uint64_t v = 1; v <= 100'000; ++v) {
    h.record(v);
  }
  assert_eq(h.count(), 100'000U);
  assert_true(h.percentile(50) >= 49'500 && h.percentile(50) <= 50'500, "p50");
  assert_true(h.percentile(99) >= 98'010 && h.percentile(99) <= 99'990, "p99");
  assert_true(h.max() >= 99'000 && h.max() <= 101'000, "max");
  assert_eq(h.min(), 1U);
}

void
histogram_merge_equals_single([[maybe_unused]] context& ctx)
{
  histogram all;
  histogram a;
  histogram b;
  for (std::uint64_t v = 1; v <= 10'000; ++v) {
    all.record(v * 7);
    (v <= 5'000 ? a : b).record(v * 7);
  }
  a.merge(b);
  assert_eq(a.count(), all.count());
  assert_eq(a.percentile(90), all.percentile(90));
  assert_eq(a.max(), all.max());
}

void
histogram_clamps_above_range([[maybe_unused]] context& ctx)
{
  histogram h;
  h.record(120'000'000'000ULL);
  assert_eq(h.count(), 1U);
  assert_true(h.max() >= 60'000'000'000ULL, "max reaches the clamp");
}

void
running_stats_matches_two_pass([[maybe_unused]] context& ctx)
{
  running_stats s;
  for (double x : { 2, 4, 4, 4, 5, 5, 7, 9 }) {
    s.add(x);
  }
  assert_near(s.mean(), 5.0, 1e-12);
  assert_near(s.stddev(), 2.138, 0.001);
}

void
running_stats_merge_matches_single([[maybe_unused]] context& ctx)
{
  running_stats all;
  running_stats a;
  running_stats b;
  for (double x : { 2, 4, 4, 4, 5, 5, 7, 9, 11 }) {
    all.add(x);
    (x < 5 ? a : b).add(x);
  }
  a.merge(b);
  assert_eq(a.count(), all.count());
  assert_near(a.mean(), all.mean(), 1e-12);
  assert_near(a.stddev(), all.stddev(), 1e-12);
}

void
histogram_percentile_exact_rank([[maybe_unused]] context& ctx)
{
  histogram h;
  for (std::uint64_t v = 1; v <= 1000; ++v) {
    h.record(v);
  }
  // p99.9 of 1..1000 is the sample at zero-based rank 998, the value 999. 1000 is in the next
  // bucket, so a ceil() off by one rank would return the maximum.
  assert_eq(h.percentile(99.9), h.value_at_rank(998));
  assert_true(h.percentile(99.9) < h.max(), "p99.9 is not the maximum");
}

void
histogram_percentile_exact_rank_large_count([[maybe_unused]] context& ctx)
{
  // 99.9% of 10,000,000 computes as 9990000.000000002. Its rank is 9,989,999, the last 1 ns
  // sample. A fixed tolerance returns the 1000 ns tail instead.
  histogram h;
  for (int i = 0; i < 9'990'000; ++i) {
    h.record(1);
  }
  for (int i = 0; i < 10'000; ++i) {
    h.record(1000);
  }
  assert_eq(h.percentile(99.9), std::uint64_t{ 1 });
}

void
median_ci_order_statistics([[maybe_unused]] context& ctx)
{
  std::vector<double> v;
  for (int i = 1; i <= 100; ++i) {
    v.push_back(i);
  }
  const auto ci = median_with_ci(v);
  assert_near(ci.median, 50.5, 1e-12);
  // Zero-based ranks 39 and 60: 2 * P(Bin(100, 1/2) <= 39) is 0.035, and <= 40 is 0.057.
  assert_near(ci.lower, 40, 1e-12);
  assert_near(ci.upper, 61, 1e-12);
}

void
median_ci_exact_ranks([[maybe_unused]] context& ctx)
{
  const auto ranks = [](std::size_t n) {
    std::vector<double> v;
    for (std::size_t i = 0; i < n; ++i) {
      v.push_back(static_cast<double>(i));
    }
    const auto ci = median_with_ci(v);
    return std::make_pair(ci.lower, ci.upper);
  };
  assert_true(ranks(6) == std::make_pair(0.0, 5.0), "n=6 spans the range");
  assert_true(ranks(7) == std::make_pair(0.0, 6.0), "n=7 spans the range");
  assert_true(ranks(8) == std::make_pair(0.0, 7.0), "n=8 spans the range");
  assert_true(ranks(9) == std::make_pair(1.0, 7.0), "n=9 drops one sample at each end");
  assert_true(ranks(100000) == std::make_pair(49689.0, 50310.0), "n=100000");
}

void
median_ci_single_sample([[maybe_unused]] context& ctx)
{
  const auto ci = median_with_ci(std::vector<double>{ 7 });
  assert_near(ci.median, 7, 1e-12);
  assert_true(std::isinf(ci.lower) && ci.lower < 0, "the lower bound is -infinity");
  assert_true(std::isinf(ci.upper) && ci.upper > 0, "the upper bound is +infinity");
}

void
median_ci_five_samples_have_no_interval([[maybe_unused]] context& ctx)
{
  const auto ci = median_with_ci(std::vector<double>{ 5, 1, 4, 2, 3 });
  assert_near(ci.median, 3, 1e-12);
  assert_true(std::isinf(ci.lower) && std::isinf(ci.upper), "five samples have no 95% interval");
}

void
histogram_median_ci_agrees_with_samples([[maybe_unused]] context& ctx)
{
  histogram h;
  for (std::uint64_t v = 1; v <= 100; ++v) {
    h.record(v);
  }
  const auto ci = median_with_ci(h);
  assert_near(ci.median, 50.5, 1e-12);
  assert_near(ci.lower, 40, 1e-12);
  assert_near(ci.upper, 61, 1e-12);
}

void
histogram_relative_error_holds_across_the_range([[maybe_unused]] context& ctx)
{
  for (std::uint64_t v = 1; v <= histogram::max_trackable; v = v + v / 97 + 1) {
    histogram h;
    h.record(v);
    h.record(v);
    // min/max clamp would hide bucket error, so probe a second, distant value.
    h.record(histogram::max_trackable);
    h.record(1);
    const auto got = static_cast<double>(h.value_at_rank(1));
    assert_true(std::abs(got - static_cast<double>(v)) <= 0.01 * static_cast<double>(v) + 1e-9,
                "bucket error exceeds 1%");
  }
}
// ── Runner ────────────────────────────────────────────────────────────────────

using namespace std::chrono_literals;

const options quick{ 10ms, 50ms, 200ms, 5.0 };

auto
run_suite(const bench_suite& suite,
          context& ctx,
          const std::set<std::string>& filter = {},
          const options& opts = quick) -> std::vector<case_result>
{
  std::ostringstream progress;
  return run(suite, filter, ctx, opts, progress);
}

auto
find(const std::vector<case_result>& results, const std::string& key) -> case_result
{
  for (const auto& r : results) {
    if (r.key == key) {
      return r;
    }
  }
  fail("no result for key " + key);
}

class fixed_requirement : public requirement
{
public:
  fixed_requirement(std::string what, check_result result)
    : what_{ std::move(what) }
    , result_{ std::move(result) }
  {
  }
  [[nodiscard]] auto describe() const -> std::string override
  {
    return what_;
  }
  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
    return result_;
  }

private:
  std::string what_;
  check_result result_;
};

void
ok_op(state& s)
{
  s.per_op("t1", 1, [](std::size_t) {
    return true;
  });
}

void
skips_unsatisfied_requirement(context& ctx)
{
  auto req = std::make_shared<fixed_requirement>("a unicorn", check_result::missing("none here"));
  const auto results = run_suite({ "s", { { CASE(ok_op), { req } } } }, ctx);
  assert_eq(results.size(), 1U);
  assert_true(results[0].status == outcome::skipped, "skipped");
  assert_eq(results[0].detail, req->describe());
}

void
fails_undetermined_requirement(context& ctx)
{
  auto req = std::make_shared<fixed_requirement>("a unicorn", check_result::unknown("no idea"));
  const auto results = run_suite({ "s", { { CASE(ok_op), { req } } } }, ctx);
  assert_eq(results.size(), 1U);
  assert_true(results[0].status == outcome::failed, "failed");
}

class throwing_requirement : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "a requirement that throws";
  }
  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
    throw std::runtime_error("check broke");
  }
};

class skipping_requirement : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "a requirement that calls skip()";
  }
  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
    skip("not here");
  }
};

void
throwing_requirement_fails_only_that_case(context& ctx)
{
  const auto results = run_suite(
    { "s", { { CASE(ok_op), { std::make_shared<throwing_requirement>() } }, { "next", &ok_op } } },
    ctx);
  assert_true(find(results, "ok_op").status == outcome::failed, "the throwing check fails");
  assert_contains(find(results, "ok_op").detail, "check broke");
  assert_true(find(results, "next/t1").status == outcome::measured, "the next case runs");
}

void
skip_in_requirement_skips(context& ctx)
{
  const auto req = std::make_shared<skipping_requirement>();
  const auto results = run_suite({ "s", { { CASE(ok_op), { req } } } }, ctx);
  assert_eq(results.size(), 1U);
  assert_true(results[0].status == outcome::skipped, "skipped");
  assert_eq(results[0].detail, req->describe());
}

void
throws_after_one_variant(state& s)
{
  ok_op(s);
  throw std::runtime_error("boom");
}

void
exception_fails_only_that_case(context& ctx)
{
  const auto results =
    run_suite({ "s", { { CASE(throws_after_one_variant) }, { CASE(ok_op) } } }, ctx);
  assert_true(find(results, "throws_after_one_variant/t1").status == outcome::measured,
              "the variant measured before the throw is kept");
  assert_true(find(results, "throws_after_one_variant").status == outcome::failed,
              "the throwing case fails");
  assert_contains(find(results, "throws_after_one_variant").detail, "boom");
  assert_true(find(results, "ok_op/t1").status == outcome::measured, "the next case runs");
}

void
always_fails(state& s)
{
  s.per_op("t1", 2, [](std::size_t) {
    return false;
  });
}

void
all_failed_ops_is_failed(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(always_fails) } } }, ctx);
  const auto r = find(results, "always_fails/t1");
  assert_true(r.status == outcome::failed, "failed");
  assert_true(r.errors > 0 && r.errors == r.operations, "every operation is an error");
  std::ostringstream bmf;
  write_bmf(bmf, results);
  assert_true(tao::json::from_string(bmf.str()).get_object().empty(), "no BMF entry");
}

void
every_third_fails(state& s)
{
  std::atomic<std::uint64_t> n{ 0 };
  s.per_op("t1", 1, [&n](std::size_t) {
    return n++ % 3 != 0;
  });
}

void
some_failed_ops_counts_errors(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(every_third_fails) } } }, ctx);
  const auto r = find(results, "every_third_fails/t1");
  assert_true(r.status == outcome::measured, "measured");
  assert_true(r.operations > 3, "operations ran");
  assert_true((r.errors * 3) + 3 >= r.operations && r.errors * 3 <= r.operations + 3,
              "a third of the operations are errors");
}

void
empty_batch(state& s)
{
  s.batched("t1", 1, [] {
  });
}

void
empty_batch_is_suspect(context& ctx)
{
#if !defined(__OPTIMIZE__) && !(defined(_MSC_VER) && defined(NDEBUG))
  skip("an unoptimised build keeps the empty loop, so there is nothing for the check to catch");
#endif
  const auto results = run_suite({ "s", { { CASE(empty_batch) } } }, ctx);
  const auto r = find(results, "empty_batch/t1");
  assert_true(r.status == outcome::suspect, "suspect");
  assert_true(r.latency_ns.median < 0.5, "median below the threshold");
}

// Calls made by counted(), including those of batches that were not recorded.
std::uint64_t counted_calls{ 0 };

void
counted(state& s)
{
  s.batched("t1", 1, [] {
    ++counted_calls;
    do_not_optimize(counted_calls);
  });
}

void
under_floor_batches_are_not_recorded(context& ctx)
{
  counted_calls = 0;
  const auto results =
    run_suite({ "s", { { CASE(counted) } } }, ctx, {}, { 0ms, 50ms, 200ms, 5.0 });
  const auto r = find(results, "counted/t1");
  // With no warmup, the batches of 1, 2, 4 ... 512 calls run during measurement and each takes far
  // under the 1 ms floor. Recording them would make operations equal every call made.
  assert_true(counted_calls >= r.operations + 1023,
              "calls " + std::to_string(counted_calls) + ", recorded " +
                std::to_string(r.operations));
}

// A window too short for any batch to reach the floor leaves no sample, which must not read as a
// zero median and be reported suspect.
void
no_recorded_batch_fails(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(counted) } } }, ctx, {}, { 0ms, 0ms, 0ms, 5.0 });
  const auto r = find(results, "counted/t1");
  assert_true(r.status == outcome::failed, "failed");
  assert_contains(r.detail, "raise --max-time");
  assert_true(exit_code(results) == 1, "exit code 1");
}

void
duplicate_key(state& s)
{
  ok_op(s);
  ok_op(s);
}

void
duplicate_key_fails(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(duplicate_key) } } }, ctx);
  bool failed = false;
  for (const auto& r : results) {
    failed = failed || r.status == outcome::failed;
  }
  assert_true(failed, "the case fails");
}

void
declares_no_key(state&)
{
}

void
case_without_a_key_fails(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(declares_no_key) } } }, ctx);
  assert_eq(results.size(), 1U);
  assert_eq(results[0].key, std::string{ "declares_no_key" });
  assert_true(results[0].status == outcome::failed, "failed");
  assert_eq(results[0].detail, std::string{ "the case measured no key" });
}

void
invalid_variant(state& s)
{
  s.per_op("t 1", 1, [](std::size_t) {
    return true;
  });
}

void
invalid_variant_fails(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(invalid_variant) } } }, ctx);
  assert_eq(results.size(), 1U);
  assert_true(results[0].status == outcome::failed, "failed");
  assert_contains(results[0].detail, "t 1");
}

void
bmf_shape(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(ok_op) } } }, ctx);
  std::ostringstream out;
  write_bmf(out, results);
  const auto bmf = tao::json::from_string(out.str());
  assert_eq(bmf.get_object().size(), 1U);
  const auto& measures = bmf.at("ok_op/t1").get_object();
  std::set<std::string> names;
  for (const auto& [name, value] : measures) {
    names.insert(name);
  }
  assert_true(
    names == std::set<std::string>{ "latency", "latency-p99", "throughput", "cpu-time", "max-rss" },
    "exactly latency, latency-p99, throughput, cpu-time and max-rss");
  const auto& latency = bmf.at("ok_op/t1").at("latency");
  const auto value = latency.at("value").as<double>();
  assert_true(latency.at("lower_value").as<double>() <= value, "lower <= value");
  assert_true(value <= latency.at("upper_value").as<double>(), "value <= upper");
}

// POSIX and Windows both provide CPU time and peak RSS; context switches are POSIX only.
void
resource_usage_per_key(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(ok_op) }, { CASE(counted) } } }, ctx);
  for (const auto* key : { "ok_op/t1", "counted/t1" }) {
    const auto r = find(results, key);
    assert_true(r.cpu_ns_per_op.has_value() && *r.cpu_ns_per_op > 0,
                std::string{ key } + " has a positive CPU time per operation");
    assert_true(r.max_rss_bytes.has_value() && *r.max_rss_bytes > 0,
                std::string{ key } + " has a peak RSS");
#if defined(_WIN32)
    assert_true(!r.context_switches_per_op.has_value(),
                std::string{ key } + " has no context switches on Windows");
#else
    assert_true(r.context_switches_per_op.has_value(),
                std::string{ key } + " has context switches");
#endif
  }
  std::ostringstream out;
  write_result_json(out, "s", results, ctx);
  const auto doc = tao::json::from_string(out.str());
  const auto& measured = doc.at("cases").get_array().at(0);
  assert_true(measured.at("cpu_ns_per_op").as<double>() > 0, "cpu_ns_per_op in the result JSON");
  assert_true(measured.at("max_rss_bytes").as<std::uint64_t>() > 0,
              "max_rss_bytes in the result JSON");
}

void
legal_key(state& s)
{
  s.per_op("Fake_2/t8", 1, [](std::size_t) {
    return true;
  });
}

void
bmf_escapes_names(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(legal_key) } } }, ctx);
  std::ostringstream out;
  write_bmf(out, results);
  const auto bmf = tao::json::from_string(out.str());
  assert_eq(bmf.get_object().size(), 1U);
  assert_eq(bmf.get_object().begin()->first, std::string{ "legal_key/Fake_2/t8" });
}

void
two_variants(state& s)
{
  for (const auto* v : { "t1", "t2" }) {
    s.per_op(v, 1, [](std::size_t) {
      return true;
    });
  }
}

void
filter_matches_case_or_key(context& ctx)
{
  const bench_suite suite{ "s", { { "a", &two_variants }, { CASE(ok_op) } } };
  auto results = run_suite(suite, ctx, { "a" });
  assert_eq(results.size(), 2U);
  assert_eq(results[0].key, std::string{ "a/t1" });
  assert_eq(results[1].key, std::string{ "a/t2" });

  results = run_suite(suite, ctx, { "a/t2" });
  assert_eq(results.size(), 1U);
  assert_eq(results[0].key, std::string{ "a/t2" });

  results = run_suite(suite, ctx, { "a/t3" });
  assert_eq(results.size(), 1U);
  assert_true(results[0].status == outcome::failed, "a filter matching nothing fails");
}

void
result_json_shape(context& ctx)
{
  const auto results = run_suite({ "s", { { CASE(ok_op) }, { CASE(empty_batch) } } }, ctx);
  std::ostringstream out;
  write_result_json(out, "suite \"q\"", results, ctx);
  const auto doc = tao::json::from_string(out.str());
  assert_eq(doc.at("schema").as<std::int64_t>(), std::int64_t{ 1 });
  assert_eq(doc.at("suite").get_string(), std::string{ "suite \"q\"" });
  assert_true(doc.find("server") == nullptr, "no server without a probe backend");
  assert_true(doc.find("descriptions") == nullptr, "no descriptions without described cases");

  std::ostringstream described;
  write_result_json(
    described, "s", results, ctx, { { CASE(ok_op), {}, "Says \"ok\"." }, { CASE(empty_batch) } });
  const auto descriptions = tao::json::from_string(described.str()).at("descriptions");
  assert_eq(descriptions.get_object().size(), 1U);
  assert_eq(descriptions.at("ok_op").get_string(), std::string{ "Says \"ok\"." });

  const auto& cases = doc.at("cases").get_array();
  assert_eq(cases.size(), 2U);
  assert_eq(cases[0].at("key").get_string(), std::string{ "ok_op/t1" });
  assert_eq(cases[0].at("status").get_string(), std::string{ "measured" });
  for (const auto* field :
       { "p50_ns", "p90_ns", "p99_ns", "p999_ns", "max_ns", "ops_per_second" }) {
    assert_true(cases[0].find(field) != nullptr, std::string{ "per_op has " } + field);
    assert_true(cases[1].find(field) == nullptr, std::string{ "batched lacks " } + field);
  }
  for (const auto* field :
       { "detail", "latency_ns", "mean_ns", "stddev_ns", "operations", "errors", "measured_ms" }) {
    assert_true(cases[1].find(field) != nullptr, std::string{ "batched has " } + field);
  }
  assert_true(cases[0].at("latency_ns").at("lower").as<double>() <=
                cases[0].at("latency_ns").at("median").as<double>(),
              "lower <= median");
}

void
console_groups_skips(context& ctx)
{
  auto req = std::make_shared<fixed_requirement>("a unicorn", check_result::missing("none"));
  const auto results =
    run_suite({ "s", { { CASE(ok_op), { req } }, { CASE(two_variants), { req } } } }, ctx);
  std::ostringstream out;
  write_console(out, results);
  assert_contains(out.str(), "Skipped for want of:");
  assert_contains(out.str(), "a unicorn — 2 case(s)");
}

// A range spanning two units shows each end in its own. The median's interval is printed as its
// bounds, in the median's unit, since it need not be symmetric.
void
console_range_units(context&)
{
  case_result r;
  r.key = "k";
  r.latency_ns = { 29900, 29800, 30100 };
  r.p99_ns = 100000;
  r.min_ns = 20000;
  r.max_ns = 1260000;
  case_result few = r;
  few.key = "few";
  few.latency_ns = { 29900,
                     -std::numeric_limits<double>::infinity(),
                     std::numeric_limits<double>::infinity() };
  std::ostringstream out;
  write_console(out, { r, few });
  assert_contains(out.str(), "p50 29.9 µs [29.8, 30.1]");
  assert_contains(out.str(), "p50 29.9 µs [n/a]");
  assert_contains(out.str(), "min 20.0 µs … max 1.26 ms");
  assert_true(out.str().find("0.02") == std::string::npos, "min is not printed in max's unit");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(histogram_percentiles_within_one_percent) },
      { CASE(histogram_merge_equals_single) },
      { CASE(histogram_clamps_above_range) },
      { CASE(running_stats_matches_two_pass) },
      { CASE(running_stats_merge_matches_single) },
      { CASE(histogram_percentile_exact_rank) },
      { CASE(histogram_percentile_exact_rank_large_count) },
      { CASE(median_ci_order_statistics) },
      { CASE(median_ci_single_sample) },
      { CASE(median_ci_five_samples_have_no_interval) },
      { CASE(median_ci_exact_ranks) },
      { CASE(histogram_median_ci_agrees_with_samples) },
      { CASE(histogram_relative_error_holds_across_the_range) },
      { CASE(skips_unsatisfied_requirement) },
      { CASE(fails_undetermined_requirement) },
      { CASE(throwing_requirement_fails_only_that_case) },
      { CASE(skip_in_requirement_skips) },
      { CASE(exception_fails_only_that_case) },
      { CASE(all_failed_ops_is_failed) },
      { CASE(some_failed_ops_counts_errors) },
      { CASE(empty_batch_is_suspect) },
      { CASE(under_floor_batches_are_not_recorded) },
      { CASE(no_recorded_batch_fails) },
      { CASE(duplicate_key_fails) },
      { CASE(case_without_a_key_fails) },
      { CASE(invalid_variant_fails) },
      { CASE(bmf_shape) },
      { CASE(bmf_escapes_names) },
      { CASE(resource_usage_per_key) },
      { CASE(filter_matches_case_or_key) },
      { CASE(result_json_shape) },
      { CASE(console_groups_skips) },
      { CASE(console_range_units) },
    },
  };
}
} // namespace couchbase::test
