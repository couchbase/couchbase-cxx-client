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

#include "report.hxx"

#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string_view>
#include <utility>

namespace couchbase::test::bench
{
namespace
{
struct unit {
  double divisor;
  const char* name;
};

auto
unit_for(double ns) -> unit
{
  using namespace std::chrono_literals;
  using nanoseconds = std::chrono::duration<double, std::nano>;
  const nanoseconds d{ ns };
  if (d < 1us) {
    return { 1, "ns" };
  }
  if (d < 1ms) {
    return { nanoseconds{ 1us }.count(), "µs" };
  }
  if (d < 1s) {
    return { nanoseconds{ 1ms }.count(), "ms" };
  }
  return { nanoseconds{ 1s }.count(), "s" };
}

// Decimals that give `v` three significant digits.
auto
decimals(double v) -> int
{
  const auto a = std::abs(v);
  if (a < 10) {
    return 2;
  }
  return a < 100 ? 1 : 0;
}

auto
fixed(double v, int places) -> std::string
{
  return fmt::format("{:.{}f}", v, places);
}

// "18.4 µs [18.2, 18.7]": the median and the bounds of its 95% interval, all in the median's unit.
// The interval is not symmetric about the median, so it is not shown as a half-width. "[n/a]" when
// there are too few samples for one.
auto
with_interval(const median_ci& ci) -> std::string
{
  const auto u = unit_for(ci.median);
  const auto m = ci.median / u.divisor;
  const auto places = decimals(m);
  if (!std::isfinite(ci.lower) || !std::isfinite(ci.upper)) {
    return fmt::format("{} {} [n/a]", fixed(m, places), u.name);
  }
  return fmt::format("{} {} [{}, {}]",
                     fixed(m, places),
                     u.name,
                     fixed(ci.lower / u.divisor, places),
                     fixed(ci.upper / u.divisor, places));
}

auto
duration(double ns) -> std::string
{
  const auto u = unit_for(ns);
  return fmt::format("{} {}", fixed(ns / u.divisor, decimals(ns / u.divisor)), u.name);
}

auto
si(double v) -> std::string
{
  constexpr double kilo = 1'000;
  constexpr double mega = 1'000'000;
  if (v >= mega) {
    return fixed(v / mega, decimals(v / mega)) + "M";
  }
  if (v >= kilo) {
    return fixed(v / kilo, decimals(v / kilo)) + "k";
  }
  return fixed(v, decimals(v));
}

auto
row(const case_result& r) -> std::string
{
  switch (r.status) {
    case outcome::skipped:
      return "skipped: " + r.detail;
    case outcome::failed:
      return "FAILED: " + r.detail;
    case outcome::measured:
    case outcome::suspect:
      break;
  }
  std::string text;
  if (r.p99_ns && r.min_ns && r.max_ns) {
    text = fmt::format("p50 {}   p99 {}   min {} … max {}   {} ops/s   {} errors",
                       with_interval(r.latency_ns),
                       duration(static_cast<double>(*r.p99_ns)),
                       duration(static_cast<double>(*r.min_ns)),
                       duration(static_cast<double>(*r.max_ns)),
                       si(r.ops_per_second.value_or(0)),
                       r.errors);
  } else {
    text = fmt::format("median {}   mean {} σ {}   {} iterations",
                       with_interval(r.latency_ns),
                       duration(r.mean_ns),
                       duration(r.stddev_ns),
                       r.operations);
  }
  if (r.status == outcome::suspect) {
    text += "   SUSPECT: " + r.detail;
  }
  return text;
}

void
quote(std::ostream& out, std::string_view s)
{
  out << '"';
  for (const char c : s) {
    switch (c) {
      case '"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          out << fmt::format("\\u{:04x}", static_cast<unsigned>(static_cast<unsigned char>(c)));
        } else {
          out << c;
        }
    }
  }
  out << '"';
}

void
number(std::ostream& out, double v)
{
  if (std::isfinite(v)) {
    out << fmt::format("{}", v);
  } else {
    out << "null";
  }
}

auto
status_name(outcome o) -> const char*
{
  switch (o) {
    case outcome::measured:
      return "measured";
    case outcome::suspect:
      return "suspect";
    case outcome::failed:
      return "failed";
    case outcome::skipped:
      return "skipped";
  }
  return "failed";
}
} // namespace

void
write_console(std::ostream& out, const std::vector<case_result>& results)
{
  std::size_t width = 0;
  for (const auto& r : results) {
    width = std::max(width, r.key.size());
  }
  std::map<outcome, std::size_t> counts;
  std::map<std::string, std::size_t> skipped_for;
  for (const auto& r : results) {
    out << fmt::format("{:<{}}   {}\n", r.key, width, row(r));
    ++counts[r.status];
    if (r.status == outcome::skipped) {
      ++skipped_for[r.detail];
    }
  }
  // Grouped, because a count alone hides a requirement that is never met.
  if (!skipped_for.empty()) {
    out << "\nSkipped for want of:\n";
    for (const auto& [requirement, count] : skipped_for) {
      out << fmt::format("  {} — {} case(s)\n", requirement, count);
    }
  }
  out << fmt::format("\n{} measured, {} suspect, {} FAILED, {} skipped\n",
                     counts[outcome::measured],
                     counts[outcome::suspect],
                     counts[outcome::failed],
                     counts[outcome::skipped]);
}

void
write_bmf(std::ostream& out, const std::vector<case_result>& results)
{
  out << '{';
  bool first = true;
  for (const auto& r : results) {
    if (r.status != outcome::measured) {
      continue;
    }
    out << (first ? "\n  " : ",\n  ");
    first = false;
    quote(out, r.key);
    out << R"(: {"latency": {"value": )";
    number(out, r.latency_ns.median);
    // A median without a 95% interval has infinite bounds, which BMF cannot carry.
    if (std::isfinite(r.latency_ns.lower)) {
      out << R"(, "lower_value": )";
      number(out, r.latency_ns.lower);
      out << R"(, "upper_value": )";
      number(out, r.latency_ns.upper);
    }
    out << '}';
    if (r.p99_ns) {
      out << R"(, "latency-p99": {"value": )" << *r.p99_ns << '}';
    }
    if (r.ops_per_second) {
      out << R"(, "throughput": {"value": )";
      number(out, *r.ops_per_second);
      out << '}';
    }
    out << '}';
  }
  out << "\n}\n";
}

void
write_result_json(std::ostream& out,
                  const std::string& suite,
                  const std::vector<case_result>& results,
                  context& ctx)
{
  out << "{\n  " << R"("schema": 1,)" << "\n  " << R"("suite": )";
  quote(out, suite);
  if (ctx.backends_created() > 0) {
    out << ",\n  " << R"("server": {"version": )";
    try {
      quote(out, ctx.server_version().to_string());
    } catch (const probe_failure&) {
      out << "null";
    }
    out << R"(, "nodes": )";
    try {
      out << ctx.number_of_nodes();
    } catch (const probe_failure&) {
      out << "null";
    }
    out << '}';
  }
  out << ",\n  " << R"("cases": [)";
  bool first = true;
  for (const auto& r : results) {
    out << (first ? "\n    {" : ",\n    {");
    first = false;
    out << R"("key": )";
    quote(out, r.key);
    out << R"(, "status": ")" << status_name(r.status) << R"(", "detail": )";
    quote(out, r.detail);
    out << R"(, "latency_ns": {"median": )";
    number(out, r.latency_ns.median);
    out << R"(, "lower": )";
    number(out, r.latency_ns.lower);
    out << R"(, "upper": )";
    number(out, r.latency_ns.upper);
    out << R"(}, "mean_ns": )";
    number(out, r.mean_ns);
    out << R"(, "stddev_ns": )";
    number(out, r.stddev_ns);
    out << R"(, "operations": )" << r.operations << R"(, "errors": )" << r.errors
        << R"(, "measured_ms": )" << r.measured_for.count();
    const std::array<std::pair<const char*, const std::optional<std::uint64_t>*>, 6> percentiles{ {
      { "min_ns", &r.min_ns },
      { "p50_ns", &r.p50_ns },
      { "p90_ns", &r.p90_ns },
      { "p99_ns", &r.p99_ns },
      { "p999_ns", &r.p999_ns },
      { "max_ns", &r.max_ns },
    } };
    for (const auto& [name, value] : percentiles) {
      if (value->has_value()) {
        out << R"(, ")" << name << R"(": )" << value->value_or(0);
      }
    }
    if (r.ops_per_second) {
      out << R"(, "ops_per_second": )";
      number(out, r.ops_per_second.value_or(0));
    }
    out << '}';
  }
  out << "\n  ]\n}\n";
}
} // namespace couchbase::test::bench
