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

// Self-test for the test framework. It exercises the runner's pass / fail / skip / timeout /
// requirement / filter behaviour by driving run() with in-memory sub-suites and asserting on the
// returned run_result. Because it asserts on the runner's *return value* (rather than letting an
// inner failure propagate), every case here passes and this binary exits 0.

#include "test_runner.hxx"

#include <chrono>
#include <cstddef>
#include <ios>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
// ── stand-ins the runner is driven with ──────────────────────────────────────

void
body_pass([[maybe_unused]] context& ctx)
{
  assert_true(true);
}
void
body_fail([[maybe_unused]] context& ctx)
{
  assert_true(false, "intentional failure");
}
void
body_skip([[maybe_unused]] context& ctx)
{
  skip("intentional skip");
}
void
body_sleep([[maybe_unused]] context& ctx)
{
  std::this_thread::sleep_for(std::chrono::milliseconds{ 200 });
}

// A backend that answers from fixed values and counts the calls, so the claim that probes are
// cached can be checked with a number instead of by reading context.cxx.
class counting_probes : public probe_backend
{
public:
  std::size_t version_calls{ 0 };
  std::size_t service_calls{ 0 };

  [[nodiscard]] auto server_version() -> couchbase::test::server_version override
  {
    ++version_calls;
    return { 7, 6, 2, false, server_edition::enterprise, deployment_type::on_prem };
  }
  [[nodiscard]] auto has_service(const std::string& name) -> bool override
  {
    ++service_calls;
    return name == "kv";
  }
  [[nodiscard]] auto has_bucket_capability(const std::string& capability) -> bool override
  {
    return capability == "durableWrite";
  }
  [[nodiscard]] auto number_of_replicas() -> std::size_t override
  {
    return 1;
  }
  [[nodiscard]] auto number_of_nodes() -> std::size_t override
  {
    return 3;
  }
  [[nodiscard]] auto server_groups() -> std::vector<std::string> override
  {
    return { "group_1" };
  }
  [[nodiscard]] auto storage_backend() -> std::string override
  {
    return "couchstore";
  }
};

// A backend that cannot answer, standing in for a configured endpoint that does not respond.
class unreachable_probes : public probe_backend
{
public:
  std::size_t calls{ 0 };

  [[nodiscard]] auto server_version() -> couchbase::test::server_version override
  {
    ++calls;
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto has_service(const std::string& /* name */) -> bool override
  {
    ++calls;
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto has_bucket_capability(const std::string& /* capability */) -> bool override
  {
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto number_of_replicas() -> std::size_t override
  {
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto number_of_nodes() -> std::size_t override
  {
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto server_groups() -> std::vector<std::string> override
  {
    throw probe_failure("connection refused");
  }
  [[nodiscard]] auto storage_backend() -> std::string override
  {
    throw probe_failure("connection refused");
  }
};

// A file-local requirement: what a translation unit writes when it needs something the built-in
// vocabulary does not cover. Kept here because "a custom requirement is short" is a claim about
// the interface, and the only honest way to hold it is to write one.
class needs_even_replicas : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "an even number of replicas";
  }
  [[nodiscard]] auto check(context& ctx) const -> check_result override
  {
    return ctx.number_of_replicas() % 2 == 0 ? check_result::ok()
                                             : check_result::missing("the count is odd");
  }
};

// A requirement that never answers, and one that answers with an exception of its own.
class blocking : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "something that never answers";
  }
  [[nodiscard]] auto check(context& /* ctx */) const -> check_result override
  {
    std::this_thread::sleep_for(std::chrono::seconds{ 30 });
    return check_result::ok();
  }
};

class skipping : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "something that gives up";
  }
  [[nodiscard]] auto check(context& /* ctx */) const -> check_result override
  {
    skip("the environment cannot provide it");
    return check_result::ok();
  }
};

class throwing : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "something that throws";
  }
  [[nodiscard]] auto check(context& /* ctx */) const -> check_result override
  {
    throw std::runtime_error("the requirement itself is broken");
  }
};

auto
no_cluster() -> configuration
{
  return {}; // cluster_configured defaults to false
}

auto
with_cluster() -> configuration
{
  configuration config;
  config.connection_string = "couchbase://localhost";
  config.cluster_configured = true;
  return config;
}

// A sink for the runner's progress output so the self-test stays quiet.
auto
run_quiet(const test_suite& suite, const std::set<std::string>& filter, context& ctx) -> run_result
{
  std::ostringstream sink;
  return run(suite, filter, ctx, sink);
}

// The common case: no cluster, no requirements, nothing to probe.
auto
run_bare(const test_suite& suite, const std::set<std::string>& filter = {}) -> run_result
{
  context ctx{ no_cluster(), std::make_unique<counting_probes>() };
  return run_quiet(suite, filter, ctx);
}

// ── the self-test cases ──────────────────────────────────────────────────────

void
runner_reports_a_passing_case([[maybe_unused]] context& ctx)
{
  const test_suite s{ "inner", { { "p", body_pass } } };
  const auto r = run_bare(s);
  assert_eq(r.passed, std::size_t{ 1 }, "one pass counted");
  assert_eq(r.failed, std::size_t{ 0 }, "no failures");
  assert_eq(exit_code(r), 0, "exit code 0 on success");
}

void
runner_detects_a_failing_case([[maybe_unused]] context& ctx)
{
  const test_suite s{ "inner", { { "f", body_fail } } };
  const auto r = run_bare(s);
  assert_eq(r.failed, std::size_t{ 1 }, "one failure counted");
  assert_eq(exit_code(r), 1, "exit code 1 on failure");
}

void
runner_reports_a_skipped_case([[maybe_unused]] context& ctx)
{
  const test_suite s{ "inner", { { "s", body_skip } } };
  const auto r = run_bare(s);
  assert_eq(r.skipped, std::size_t{ 1 }, "one skip counted");
  assert_eq(r.failed, std::size_t{ 0 }, "skip is not a failure");
  // The other half of the contract couchbase_add_test() wires up as SKIP_RETURN_CODE: a binary that
  // ran nothing but skips must report ctest Skipped, not success.
  assert_eq(exit_code(r), 77, "an all-skipped binary reports 77");
}

void
runner_detects_a_timeout([[maybe_unused]] context& ctx)
{
  // 200ms body against a 20ms budget must be reported as a failure.
  const test_suite s{ "inner", { { "t", body_sleep, {}, std::chrono::milliseconds{ 20 } } } };
  const auto r = run_bare(s);
  assert_eq(r.failed, std::size_t{ 1 }, "timeout counted as a failure");
}

void
case_output_is_flushed_as_it_is_written(context& ctx)
{
  // ctest gives these binaries a pipe for stdout, where the stream is block-buffered, and a case
  // that aborts the process discards whatever is still buffered -- so the log names neither the
  // aborting case nor any that passed before it. The cases that crash are the ones whose output is
  // most needed, so the runner must not leave its stream block-buffered.
  std::ostringstream sink;
  const test_suite s{ "inner", { { "p", body_pass } } };
  static_cast<void>(run(s, {}, ctx, sink));
  assert_true((sink.flags() & std::ios::unitbuf) != 0,
              "the runner flushes its stream after every write");
}

void
an_unconfigured_cluster_skips_and_an_unreachable_one_fails([[maybe_unused]] context& ctx)
{
  // The distinction the whole three-valued scheme exists for. A boolean predicate has to answer
  // both of these the same way, and the answer it picks is the one that does not fail the build --
  // which is how an unreachable endpoint turns a whole leg green.
  const test_suite s{ "inner", { { "c", body_pass, { needs::service("kv") } } } };

  context unconfigured{ no_cluster(), std::make_unique<unreachable_probes>() };
  const auto skipped = run_quiet(s, {}, unconfigured);
  assert_eq(skipped.skipped, std::size_t{ 1 }, "no cluster configured is a skip");
  assert_eq(skipped.failed, std::size_t{ 0 }, "and not a failure");
  assert_eq(skipped.skipped_by_requirement.count("the kv service"),
            std::size_t{ 1 },
            "the requirement that turned it away is named");

  context unreachable{ with_cluster(), std::make_unique<unreachable_probes>() };
  const auto failed = run_quiet(s, {}, unreachable);
  assert_eq(
    failed.failed, std::size_t{ 1 }, "a configured cluster that cannot answer is a failure");
  assert_eq(failed.skipped, std::size_t{ 0 }, "and never a skip");
  assert_eq(failed.passed, std::size_t{ 0 }, "and the body did not run");
}

void
a_satisfied_requirement_lets_the_case_run([[maybe_unused]] context& ctx)
{
  const test_suite s{
    "inner",
    { { "kv", body_pass, { needs::service("kv"), needs::cluster_version(v7_0, v8_0) } },
      { "n1ql", body_pass, { needs::service("n1ql") } } },
  };
  context probed{ with_cluster(), std::make_unique<counting_probes>() };
  const auto r = run_quiet(s, {}, probed);
  assert_eq(r.passed, std::size_t{ 1 }, "the case whose requirements hold runs");
  assert_eq(r.skipped, std::size_t{ 1 }, "the one whose service is absent is skipped");
  assert_eq(r.failed, std::size_t{ 0 }, "neither is a failure");
}

void
a_version_range_is_half_open([[maybe_unused]] context& ctx)
{
  // counting_probes reports 7.6.2.
  const test_suite s{
    "inner",
    { { "in_range", body_pass, { needs::cluster_version(v7_6, v8_0) } },
      { "below", body_pass, { needs::cluster_version(v8_0) } },
      { "at_upper_bound", body_pass, { needs::cluster_version(v7_0, v7_6) } } },
  };
  context probed{ with_cluster(), std::make_unique<counting_probes>() };
  const auto r = run_quiet(s, {}, probed);
  assert_eq(r.passed, std::size_t{ 1 }, "only the case whose range contains 7.6.2 runs");
  assert_eq(
    r.skipped, std::size_t{ 2 }, "below the floor and at the exclusive ceiling are skipped");
}

void
probes_are_cached_across_every_case_in_the_binary([[maybe_unused]] context& ctx)
{
  // 20 cases, each asking the same two questions. Reconnecting or re-asking per case is what turns
  // a suite into a denial-of-service against its own cluster.
  auto probes = std::make_unique<counting_probes>();
  auto* counters = probes.get();
  context probed{ with_cluster(), std::move(probes) };

  std::vector<test_case> cases;
  cases.reserve(20);
  for (std::size_t i = 0; i < 20; ++i) {
    cases.push_back({ fmt::format("case_{}", i),
                      body_pass,
                      { needs::service("kv"), needs::cluster_version(v7_0) } });
  }
  const test_suite s{ "inner", cases };
  const auto r = run_quiet(s, {}, probed);

  assert_eq(r.passed, std::size_t{ 20 }, "every case ran");
  assert_eq(counters->service_calls, std::size_t{ 1 }, "the service was asked about once");
  assert_eq(counters->version_calls, std::size_t{ 1 }, "and so was the version");
  assert_eq(probed.backends_created(), std::size_t{ 1 }, "one backend for the whole binary");
}

void
a_failed_probe_is_not_retried_per_case([[maybe_unused]] context& ctx)
{
  // The failure has to be cached too: an unreachable cluster otherwise costs one connection
  // attempt per case, which is how a broken endpoint turns a fast leg into a timeout.
  auto probes = std::make_unique<unreachable_probes>();
  auto* counters = probes.get();
  context probed{ with_cluster(), std::move(probes) };

  const test_suite s{ "inner",
                      { { "a", body_pass, { needs::cluster_version(v7_0) } },
                        { "b", body_pass, { needs::cluster_version(v7_0) } },
                        { "c", body_pass, { needs::cluster_version(v7_0) } } } };
  const auto r = run_quiet(s, {}, probed);
  assert_eq(r.failed, std::size_t{ 3 }, "each case fails on its own account");
  assert_eq(counters->calls, std::size_t{ 1 }, "but the cluster was asked exactly once");
}

void
a_requirement_that_blocks_fails_its_case_rather_than_the_run([[maybe_unused]] context& ctx)
{
  // A requirement runs before every case, so one that hangs would stall the leg rather than report
  // anything. The budget belongs to the framework, not to the requirement, which is why it is on
  // the configuration and can be shortened here.
  const test_suite s{ "inner",
                      { { "blocked", body_pass, { std::make_shared<const blocking>() } } } };

  auto config = no_cluster();
  config.requirement_budget = std::chrono::milliseconds{ 50 };
  context bounded{ config, std::make_unique<counting_probes>() };

  const auto r = run_quiet(s, {}, bounded);
  assert_eq(r.failed, std::size_t{ 1 }, "the case fails");
  assert_eq(r.passed, std::size_t{ 0 }, "the body never ran");
  assert_eq(r.skipped, std::size_t{ 0 }, "and it is not reported as inapplicable");
  assert_eq(r.timed_out,
            std::size_t{ 1 },
            "flagged as a timeout, because the abandoned worker dictates how the process exits");
}

void
a_requirement_that_throws_fails_its_case([[maybe_unused]] context& ctx)
{
  // probe_failure is turned into `undetermined` by design; anything else escaping check() is a bug
  // in the requirement, and must still land on the case that declared it rather than on the run.
  const test_suite s{ "inner",
                      { { "broken", body_pass, { std::make_shared<const throwing>() } } } };
  const auto r = run_bare(s);
  assert_eq(r.failed, std::size_t{ 1 }, "a requirement that throws fails its case");
  assert_eq(r.passed, std::size_t{ 0 }, "and the body did not run");
  assert_eq(r.skipped, std::size_t{ 0 }, "and it is not reported as inapplicable");
}

void
a_requirement_that_skips_does_not_run_its_case([[maybe_unused]] context& ctx)
{
  // skip() from inside check() is the fourth thing a requirement can do, alongside the three
  // check_result statuses. Handling only the statuses left this one falling through to a default
  // gate that said "run", so the body executed and the case reported passed behind a precondition
  // that had just declared itself unmet -- a pass with nothing verified, and no output line.
  const test_suite s{ "inner", { { "gated", body_fail, { std::make_shared<const skipping>() } } } };
  const auto r = run_bare(s);
  assert_eq(r.skipped, std::size_t{ 1 }, "a requirement that skips skips its case");
  assert_eq(r.passed, std::size_t{ 0 }, "the body did not run");
  // body_fail would have failed had it run, so this pins that the body was never entered rather
  // than merely that the tally came out even.
  assert_eq(r.failed, std::size_t{ 0 }, "and it is not a failure");
}

void
a_file_local_requirement_needs_no_framework_change([[maybe_unused]] context& ctx)
{
  // counting_probes reports one replica, so an even-replica requirement turns the case away.
  const test_suite s{
    "inner", { { "even", body_pass, { std::make_shared<const needs_even_replicas>() } } }
  };
  context probed{ with_cluster(), std::make_unique<counting_probes>() };
  const auto r = run_quiet(s, {}, probed);
  assert_eq(r.skipped, std::size_t{ 1 }, "the custom requirement gates the case");
  assert_eq(r.skipped_by_requirement.count("an even number of replicas"),
            std::size_t{ 1 },
            "and its own description is what the report groups by");
}

void
filter_selects_cases_by_name([[maybe_unused]] context& ctx)
{
  const test_suite s{ "inner", { { "a", body_pass }, { "b", body_pass } } };
  const auto r = run_bare(s, { "a" });
  assert_eq(r.passed, std::size_t{ 1 }, "only the filtered case runs");
}

void
a_failing_case_does_not_suppress_later_cases([[maybe_unused]] context& ctx)
{
  // Abandoning the suite on the first failure would hide every later regression in the same
  // binary until the first one is fixed.
  const test_suite s{ "inner", { { "f", body_fail }, { "p", body_pass }, { "s", body_skip } } };
  const auto r = run_bare(s);
  assert_eq(r.failed, std::size_t{ 1 }, "the failure is counted");
  assert_eq(r.passed, std::size_t{ 1 }, "a later case still runs after a failure");
  assert_eq(r.skipped, std::size_t{ 1 }, "a later skip is still reported after a failure");
}

void
slow_cases_run_after_a_failure([[maybe_unused]] context& ctx)
{
  test_suite s{ "inner", { { "f", body_fail } } };
  s.slow_test_cases = { { "p", body_pass } };
  const auto r = run_bare(s);
  assert_eq(r.passed, std::size_t{ 1 }, "slow cases are not abandoned by an earlier failure");
}

void
a_filter_name_matching_nothing_fails([[maybe_unused]] context& ctx)
{
  const test_suite s{ "inner", { { "a", body_pass } } };
  const auto r = run_bare(s, { "typo" });
  assert_eq(r.passed, std::size_t{ 0 }, "nothing ran");
  assert_eq(r.failed, std::size_t{ 1 }, "an unmatched filter name is a failure");
  assert_eq(exit_code(r), 1, "exit code 1, not a silent pass");
}

void
a_suite_that_runs_nothing_does_not_pass([[maybe_unused]] context& ctx)
{
  const test_suite empty{ "inner", {} };
  const auto r = run_bare(empty);
  assert_eq(r.passed, std::size_t{ 0 }, "nothing ran");
  assert_eq(r.skipped, std::size_t{ 0 }, "nothing skipped");
  assert_eq(exit_code(r), 1, "an empty suite must not report success");
}

void
a_timeout_is_reported_as_such([[maybe_unused]] context& ctx)
{
  // main() needs to distinguish a timeout from an ordinary failure: only a timeout leaves a
  // detached worker thread, which dictates how the process exits.
  const test_suite s{ "inner", { { "t", body_sleep, {}, std::chrono::milliseconds{ 20 } } } };
  const auto r = run_bare(s);
  assert_eq(r.timed_out, std::size_t{ 1 }, "the timeout is flagged separately");
  assert_eq(r.failed, std::size_t{ 1 }, "and still counted as a failure");
}

void
case_names_covers_slow_cases_and_ignores_the_environment([[maybe_unused]] context& ctx)
{
  const test_suite suite{
    "listing",
    { { "regular", body_pass, { needs::real_cluster() }, timeout::instant } },
    { { "deliberately_slow", body_pass, {}, timeout::slow } },
  };

  const auto names = case_names(suite);
  assert_eq(names.size(), std::size_t{ 2 }, "both lists are enumerated");
  assert_eq(names[0], std::string{ "regular" }, "in registration order");
  assert_eq(names[1], std::string{ "deliberately_slow" }, "slow cases come last");
}

void
list_output_carries_the_requirements_after_a_tab([[maybe_unused]] context& ctx)
{
  // cmake/TestFrameworkAddTests.cmake registers the part before the tab, so the same output has to
  // serve the build and a person asking why a case never runs.
  const test_suite suite{
    "listing",
    { { "gated", body_pass, { needs::real_cluster(), needs::service("n1ql") } },
      { "ungated", body_pass } },
  };

  const auto lines = describe_cases(suite);
  assert_eq(lines.size(), std::size_t{ 2 }, "one line per case");
  assert_eq(lines[0],
            std::string{ "gated\ta real cluster; the n1ql service" },
            "requirements follow the name, separated by a tab");
  assert_eq(lines[1], std::string{ "ungated\tno requirements" }, "and a case with none says so");
}

void
timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number([[maybe_unused]] context& ctx)
{
  assert_eq(timeout_multiplier(std::nullopt), 1.0, "unset means unscaled");
  assert_eq(timeout_multiplier(std::optional<std::string>{ "10" }), 10.0, "an integer is read");
  assert_eq(timeout_multiplier(std::optional<std::string>{ "2.5" }), 2.5, "so is a fraction");

  // A trailing suffix must not be read as a bare number: silently running with a budget nobody
  // asked for is how a leg goes green on timing rather than on behaviour.
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "10x" });
    },
    "a trailing suffix is rejected");
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "0" });
    },
    "zero would kill every case immediately");
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "-1" });
    },
    "a negative factor is rejected");
  // "inf" parses, and satisfies a bare positivity test. Accepting it makes every scaled budget
  // undefined at the narrowing cast in scale_timeouts.
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "inf" });
    },
    "a non-finite factor is rejected");
}

void
scale_timeouts_multiplies_every_budget([[maybe_unused]] context& ctx)
{
  test_suite suite{
    "budgets",
    { { "fast", body_pass, {}, std::chrono::milliseconds{ 100 } } },
    { { "slow", body_pass, {}, std::chrono::milliseconds{ 1'000 } } },
  };

  scale_timeouts(suite, 10.0);
  assert_eq(suite.test_cases[0].timeout.count(), 1'000, "the regular list is scaled");
  assert_eq(suite.slow_test_cases[0].timeout.count(), 10'000, "and so is the slow list");

  // Ceiling, not nearest and not truncation. Only a product whose fraction falls below .5 tells
  // the three apart: 100 x 1.004 is 101 under ceil and 100 under both floor and llround.
  test_suite fractional{ "fractional",
                         { { "case", body_pass, {}, std::chrono::milliseconds{ 100 } } } };
  scale_timeouts(fractional, 1.004);
  assert_eq(fractional.test_cases[0].timeout.count(), 101, "a fraction below .5 still rounds up");

  // Rounding must not produce a zero budget, which would fail every case before it started.
  scale_timeouts(suite, 0.0001);
  assert_true(suite.test_cases[0].timeout.count() >= 1, "a budget never rounds down to nothing");
}

void
a_scaled_budget_lets_a_case_outlive_its_original_one([[maybe_unused]] context& ctx)
{
  // body_sleep runs for 200ms; 50ms is not enough and 50ms x 10 is.
  test_suite suite{ "scaled", { { "sleeper", body_sleep, {}, std::chrono::milliseconds{ 50 } } } };

  const auto unscaled = run_bare(suite);
  assert_eq(unscaled.timed_out, std::size_t{ 1 }, "the original budget kills the case");

  scale_timeouts(suite, 10.0);
  const auto scaled = run_bare(suite);
  assert_eq(scaled.timed_out, std::size_t{ 0 }, "the scaled budget does not");
  assert_eq(scaled.passed, std::size_t{ 1 }, "and the case reports its own outcome");
}

void
the_configuration_reads_the_environment_it_documents(context& ctx)
{
  // The context the runner handed this case is the one main() built from the environment, so this
  // is also the check that a case reaches its own configuration without calling getenv.
  const auto& config = ctx.config();
  assert_eq(config.cluster_configured,
            ctx.env("TEST_CONNECTION_STRING").has_value(),
            "cluster_configured tracks TEST_CONNECTION_STRING and nothing else");
  assert_true(!config.bucket.empty(), "a bucket name is always resolved");
  assert_true(!config.username.empty(), "and so are credentials");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "framework_selftest",
    {
      { "runner_reports_a_passing_case", runner_reports_a_passing_case },
      { "runner_detects_a_failing_case", runner_detects_a_failing_case },
      { "runner_reports_a_skipped_case", runner_reports_a_skipped_case },
      { "runner_detects_a_timeout", runner_detects_a_timeout, {}, timeout::fast },
      { "case_output_is_flushed_as_it_is_written", case_output_is_flushed_as_it_is_written },
      { "an_unconfigured_cluster_skips_and_an_unreachable_one_fails",
        an_unconfigured_cluster_skips_and_an_unreachable_one_fails },
      { "a_satisfied_requirement_lets_the_case_run", a_satisfied_requirement_lets_the_case_run },
      { "a_version_range_is_half_open", a_version_range_is_half_open },
      { "probes_are_cached_across_every_case_in_the_binary",
        probes_are_cached_across_every_case_in_the_binary },
      { "a_failed_probe_is_not_retried_per_case", a_failed_probe_is_not_retried_per_case },
      { "a_requirement_that_blocks_fails_its_case_rather_than_the_run",
        a_requirement_that_blocks_fails_its_case_rather_than_the_run,
        {},
        timeout::fast },
      { "a_requirement_that_throws_fails_its_case", a_requirement_that_throws_fails_its_case },
      { "a_requirement_that_skips_does_not_run_its_case",
        a_requirement_that_skips_does_not_run_its_case },
      { "a_file_local_requirement_needs_no_framework_change",
        a_file_local_requirement_needs_no_framework_change },
      { "filter_selects_cases_by_name", filter_selects_cases_by_name },
      { "a_failing_case_does_not_suppress_later_cases",
        a_failing_case_does_not_suppress_later_cases },
      { "slow_cases_run_after_a_failure", slow_cases_run_after_a_failure },
      { "a_filter_name_matching_nothing_fails", a_filter_name_matching_nothing_fails },
      { "a_suite_that_runs_nothing_does_not_pass", a_suite_that_runs_nothing_does_not_pass },
      { "a_timeout_is_reported_as_such", a_timeout_is_reported_as_such, {}, timeout::fast },
      { "case_names_covers_slow_cases_and_ignores_the_environment",
        case_names_covers_slow_cases_and_ignores_the_environment },
      { "list_output_carries_the_requirements_after_a_tab",
        list_output_carries_the_requirements_after_a_tab },
      { "timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number",
        timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number },
      { "scale_timeouts_multiplies_every_budget", scale_timeouts_multiplies_every_budget },
      { "a_scaled_budget_lets_a_case_outlive_its_original_one",
        a_scaled_budget_lets_a_case_outlive_its_original_one,
        {},
        timeout::fast },
      { "the_configuration_reads_the_environment_it_documents",
        the_configuration_reads_the_environment_it_documents },
    },
  };
}

} // namespace couchbase::test
