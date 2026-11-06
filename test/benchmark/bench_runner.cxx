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

#include "bench_runner.hxx"

#include "framework/test_framework.hxx"

#include <algorithm>
#include <atomic>
#include <exception>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>

namespace couchbase::test::bench
{
namespace detail
{
// One case's run: what state needs to name, select, measure and record its keys.
struct case_run {
  context& ctx;
  const options& opts;
  const bench_case& bc;
  // Every key of the case is selected, rather than only keys the filter names in full.
  bool whole_case;
  const std::set<std::string>& filter;
  // Binary-wide, so a key repeated in another case is caught too.
  std::set<std::string>& keys_seen;
  std::vector<case_result>& results;
  std::ostream& progress;
  // The keys this case has declared, selected or not.
  std::size_t keys_declared{ 0 };
  // The key being measured, so an exception escaping it is reported against it.
  std::string current_key{};
};

thread_local const volatile char* volatile escaped{ nullptr };

void
escape(const volatile char* p) noexcept
{
  escaped = p;
}
} // namespace detail

namespace
{
using clock = std::chrono::steady_clock;

constexpr std::chrono::milliseconds check_interval{ 100 };
constexpr std::chrono::milliseconds min_batch_duration{ 1 };
// Bounds the doubling for a loop the compiler emptied, whose batches never reach a millisecond.
constexpr std::uint64_t max_batch_iterations{ std::uint64_t{ 1 } << 30 };
constexpr double suspect_below_ns{ 0.5 };

auto
make_result(std::string key, outcome status, std::string detail = {}) -> case_result
{
  case_result r;
  r.key = std::move(key);
  r.status = status;
  r.detail = std::move(detail);
  return r;
}

auto
is_identifier(std::string_view s) -> bool
{
  const auto word = [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
  };
  return !s.empty() && word(s.front()) && std::all_of(s.begin() + 1, s.end(), [&](char c) {
    return word(c) || (c >= '0' && c <= '9');
  });
}

auto
is_variant(std::string_view v) -> bool
{
  for (;;) {
    const auto slash = v.find('/');
    if (!is_identifier(v.substr(0, slash))) {
      return false;
    }
    if (slash == std::string_view::npos) {
      return true;
    }
    v.remove_prefix(slash + 1);
  }
}

// Validates and claims the key. False when the filter does not select it.
auto
begin_key(detail::case_run& run, std::string_view variant, std::size_t threads) -> bool
{
  if (!variant.empty() && !is_variant(variant)) {
    throw std::invalid_argument("variant \"" + std::string{ variant } +
                                "\" is not '/'-separated [A-Za-z_][A-Za-z0-9_]* segments");
  }
  auto key = run.bc.name;
  if (!variant.empty()) {
    key += '/';
    key.append(variant);
  }
  if (threads == 0) {
    throw std::invalid_argument("key \"" + key + "\" asks for zero threads");
  }
  if (!run.keys_seen.insert(key).second) {
    throw std::logic_error("key \"" + key + "\" is measured twice in this binary");
  }
  ++run.keys_declared;
  if (!run.whole_case && run.filter.count(key) == 0) {
    return false;
  }
  run.progress << "measuring " << key << '\n' << std::flush;
  run.current_key = std::move(key);
  return true;
}

// Runs body(thread_index) on `threads` threads and joins them all. An exception sets `abort`, so
// the other threads stop after their current operation, and the first one is rethrown. A thread
// that cannot be started sets `abort` too. The started ones are joined before its error propagates,
// and their own errors are discarded.
void
run_threads(std::size_t threads,
            std::atomic<bool>& abort,
            const std::function<void(std::size_t)>& body)
{
  std::vector<std::exception_ptr> errors(threads);
  std::vector<std::thread> pool;
  pool.reserve(threads);
  try {
    for (std::size_t i = 0; i < threads; ++i) {
      pool.emplace_back([&, i] {
        try {
          body(i);
        } catch (...) {
          errors[i] = std::current_exception();
          abort.store(true);
        }
      });
    }
  } catch (...) {
    abort.store(true);
    for (auto& t : pool) {
      t.join();
    }
    throw;
  }
  for (auto& t : pool) {
    t.join();
  }
  for (const auto& e : errors) {
    if (e) {
      std::rethrow_exception(e);
    }
  }
}

auto
converged(const median_ci& ci, double target_percent) -> bool
{
  return ci.median > 0 && (ci.upper - ci.lower) / 2 / ci.median * 100 <= target_percent;
}

// Warmup rounds, then measurement rounds until the stopping rule holds. round(deadline,
// measuring) runs every thread until its first operation that ends at or after `deadline`.
// Returns how long measurement ran.
auto
drive(const options& opts,
      const std::function<void(clock::time_point, bool)>& round,
      const std::function<median_ci()>& current) -> clock::duration
{
  auto start = clock::now();
  while (clock::now() - start < opts.warmup) {
    round(std::min(clock::now() + check_interval, start + opts.warmup), false);
  }
  start = clock::now();
  for (;;) {
    round(std::min(clock::now() + check_interval, start + opts.max_time), true);
    const auto elapsed = clock::now() - start;
    if (elapsed >= opts.max_time ||
        (elapsed >= opts.min_time && converged(current(), opts.ci_target_percent))) {
      return elapsed;
    }
  }
}

auto
nanoseconds(clock::duration d) -> std::uint64_t
{
  return static_cast<std::uint64_t>(
    std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
}

void
finish_key(detail::case_run& run, case_result r, clock::duration measured)
{
  r.key = std::move(run.current_key);
  run.current_key.clear();
  r.measured_for = std::chrono::duration_cast<std::chrono::milliseconds>(measured);
  run.results.push_back(std::move(r));
}

// The first requirement that does not hold, as a skipped or failed result.
auto
check_requirements(const bench_case& bc, context& ctx) -> std::optional<case_result>
{
  for (const auto& req : bc.requirements) {
    if (req == nullptr) {
      return make_result(bc.name, outcome::failed, "a null requirement was registered");
    }
    check_result checked;
    try {
      checked = req->check(ctx);
    } catch (const probe_failure& e) {
      checked = check_result::unknown(e.what());
    } catch (const test_skip_exception&) {
      // skip() states that the case does not apply, which is what not_satisfied means.
      checked = check_result::missing({});
    } catch (const std::exception& e) {
      return make_result(bc.name, outcome::failed, req->describe() + ": " + e.what());
    } catch (...) {
      return make_result(bc.name,
                         outcome::failed,
                         req->describe() + ": an exception not derived from std::exception");
    }
    switch (checked.value) {
      case check_result::status::satisfied:
        break;
      case check_result::status::not_satisfied:
        return make_result(bc.name, outcome::skipped, req->describe());
      case check_result::status::undetermined:
        return make_result(bc.name, outcome::failed, req->describe() + ": " + checked.detail);
    }
  }
  return std::nullopt;
}
} // namespace

state::state(detail::case_run& run)
  : run_{ &run }
{
}

auto
state::ctx() -> context&
{
  return run_->ctx;
}

void
state::per_op(std::string_view variant,
              std::size_t threads,
              const std::function<bool(std::size_t)>& fn)
{
  if (!begin_key(*run_, variant, threads)) {
    return;
  }
  struct lane {
    histogram latencies{};
    running_stats stats{};
    std::uint64_t operations{ 0 };
    std::uint64_t errors{ 0 };
  };
  std::vector<lane> lanes(threads);
  std::atomic<bool> abort{ false };

  // The hot loop writes only thread-local objects, allocated before it starts, and folds them into
  // the lane once per round.
  const auto round = [&](clock::time_point deadline, bool measuring) {
    run_threads(threads, abort, [&](std::size_t i) {
      histogram latencies;
      running_stats stats;
      std::uint64_t operations{ 0 };
      std::uint64_t errors{ 0 };
      while (!abort.load(std::memory_order_relaxed)) {
        const auto t0 = clock::now();
        const bool ok = fn(i);
        const auto t1 = clock::now();
        ++operations;
        if (ok) {
          const auto ns = nanoseconds(t1 - t0);
          latencies.record(ns);
          stats.add(static_cast<double>(ns));
        } else {
          ++errors;
        }
        if (t1 >= deadline) {
          break;
        }
      }
      if (measuring) {
        auto& l = lanes[i];
        l.latencies.merge(latencies);
        l.stats.merge(stats);
        l.operations += operations;
        l.errors += errors;
      }
    });
  };
  const auto merged = [&lanes] {
    histogram all;
    for (const auto& l : lanes) {
      all.merge(l.latencies);
    }
    return all;
  };
  const auto measured = drive(run_->opts, round, [&merged] {
    return median_with_ci(merged());
  });

  case_result r;
  running_stats stats;
  for (const auto& l : lanes) {
    stats.merge(l.stats);
    r.operations += l.operations;
    r.errors += l.errors;
  }
  const auto all = merged();
  if (all.count() == 0) {
    r.status = outcome::failed;
    r.detail = "all " + std::to_string(r.operations) + " operations failed";
  } else {
    r.latency_ns = median_with_ci(all);
    r.min_ns = all.min();
    r.p50_ns = all.percentile(50);
    r.p90_ns = all.percentile(90);
    r.p99_ns = all.percentile(99);
    r.p999_ns = all.percentile(99.9);
    r.max_ns = all.max();
    r.ops_per_second =
      static_cast<double>(all.count()) / std::chrono::duration<double>(measured).count();
    r.mean_ns = stats.mean();
    r.stddev_ns = stats.stddev();
  }
  finish_key(*run_, std::move(r), measured);
}

void
state::run_batched(std::string_view variant,
                   std::size_t threads,
                   void (*loop)(void*, std::uint64_t iterations),
                   void* fn)
{
  if (!begin_key(*run_, variant, threads)) {
    return;
  }
  struct lane {
    std::uint64_t batch{ 1 };
    std::uint64_t iterations{ 0 };
    std::vector<double> samples{};
  };
  std::vector<lane> lanes(threads);
  std::atomic<bool> abort{ false };

  const auto round = [&](clock::time_point deadline, bool measuring) {
    run_threads(threads, abort, [&](std::size_t i) {
      auto& l = lanes[i];
      while (!abort.load(std::memory_order_relaxed)) {
        const auto t0 = clock::now();
        loop(fn, l.batch);
        const auto t1 = clock::now();
        // A batch under the floor measures the clock as much as fn, so it only sizes the next one.
        // A capped batch is kept: it is how a loop the compiler emptied reaches `suspect`.
        if (t1 - t0 < min_batch_duration && l.batch < max_batch_iterations) {
          l.batch *= 2;
        } else if (measuring) {
          l.samples.push_back(static_cast<double>(nanoseconds(t1 - t0)) /
                              static_cast<double>(l.batch));
          l.iterations += l.batch;
        }
        if (t1 >= deadline) {
          break;
        }
      }
    });
  };
  const auto samples = [&lanes] {
    std::vector<double> all;
    for (const auto& l : lanes) {
      all.insert(all.end(), l.samples.begin(), l.samples.end());
    }
    return all;
  };
  const auto measured = drive(run_->opts, round, [&samples] {
    return median_with_ci(samples());
  });

  case_result r;
  running_stats stats;
  for (const auto x : samples()) {
    stats.add(x);
  }
  for (const auto& l : lanes) {
    r.operations += l.iterations;
  }
  r.latency_ns = median_with_ci(samples());
  r.mean_ns = stats.mean();
  r.stddev_ns = stats.stddev();
  if (r.operations == 0) {
    r.status = outcome::failed;
    r.detail = "no batch reached the 1 ms floor before --max-time; raise --max-time";
  } else if (r.latency_ns.median < suspect_below_ns) {
    r.status = outcome::suspect;
    r.detail = "median under 0.5 ns per iteration: the compiler may have removed the work";
  }
  finish_key(*run_, std::move(r), measured);
}

auto
run(const bench_suite& suite,
    const std::set<std::string>& filter,
    context& ctx,
    const options& opts,
    std::ostream& progress) -> std::vector<case_result>
{
  std::vector<case_result> results;
  std::set<std::string> keys_seen;
  // Filter entries that selected something even though no key carries their name: a case name, or
  // a key of a case that stopped before it could reach it.
  std::set<std::string> matched;

  for (const auto& bc : suite.cases) {
    const auto prefix = bc.name + '/';
    const bool whole_case = filter.empty() || filter.count(bc.name) > 0;
    const bool some_key = std::any_of(filter.begin(), filter.end(), [&prefix](const auto& f) {
      return f.compare(0, prefix.size(), prefix) == 0;
    });
    if (!whole_case && !some_key) {
      continue;
    }
    matched.insert(bc.name);
    const auto stopped_early = [&](case_result r) {
      results.push_back(std::move(r));
      for (const auto& f : filter) {
        if (f.compare(0, prefix.size(), prefix) == 0) {
          matched.insert(f);
        }
      }
    };

    if (auto gate = check_requirements(bc, ctx); gate) {
      stopped_early(std::move(*gate));
      continue;
    }

    detail::case_run cr{ ctx, opts, bc, whole_case, filter, keys_seen, results, progress };
    state s{ cr };
    try {
      bc.func(s);
      if (cr.keys_declared == 0) {
        results.push_back(make_result(bc.name, outcome::failed, "the case measured no key"));
      }
    } catch (const std::exception& e) {
      stopped_early(
        make_result(cr.current_key.empty() ? bc.name : cr.current_key, outcome::failed, e.what()));
    } catch (...) {
      stopped_early(make_result(cr.current_key.empty() ? bc.name : cr.current_key,
                                outcome::failed,
                                "an exception not derived from std::exception"));
    }
  }

  for (const auto& f : filter) {
    if (matched.count(f) == 0 && keys_seen.count(f) == 0) {
      results.push_back(make_result(f, outcome::failed, "the filter matches no case or key"));
    }
  }
  return results;
}

auto
exit_code(const std::vector<case_result>& results) -> int
{
  if (std::any_of(results.begin(), results.end(), [](const auto& r) {
        return r.status == outcome::failed;
      })) {
    return 1;
  }
  if (!results.empty() && std::all_of(results.begin(), results.end(), [](const auto& r) {
        return r.status == outcome::skipped;
      })) {
    return 77;
  }
  return 0;
}
} // namespace couchbase::test::bench
