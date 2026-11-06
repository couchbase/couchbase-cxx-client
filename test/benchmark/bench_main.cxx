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

// Shared entry point for every benchmark executable. A benchmark file provides benchmarks().
//
//   --list            print each case name and what it requires, then exit
//   --warmup MS, --min-time MS, --max-time MS, --ci-target PCT
//                     set the options (bench_framework.hxx)
//   --bmf FILE        write Bencher Metric Format
//   --json FILE       write the result JSON (report.hxx)
//   NAME | KEY        run only these cases or keys; one that matches nothing fails the run
//
// Exit code: 1 when any result failed or the arguments or an output file were invalid, 77 when
// every result was skipped, 0 otherwise.

#include "bench_runner.hxx"
#include "report.hxx"

#include <chrono>
#include <cmath>
#include <cstddef>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>

namespace
{
// The whole of `value` as a non-negative number.
auto
non_negative(const std::string& flag, const std::string& value) -> double
{
  std::size_t used = 0;
  double v = -1;
  try {
    v = std::stod(value, &used);
  } catch (const std::logic_error&) {
    used = 0;
  }
  if (used == 0 || used != value.size() || !(v >= 0) || !std::isfinite(v)) {
    throw std::invalid_argument(flag + " takes a non-negative number, got \"" + value + "\"");
  }
  return v;
}

// Upper bound for the timing options: one day, in milliseconds. It keeps the cast below and the
// runner's clock::now() + duration sums representable.
constexpr double max_milliseconds = 24.0 * 60 * 60 * 1000;

auto
milliseconds(const std::string& flag, const std::string& value) -> std::chrono::milliseconds
{
  const auto v = non_negative(flag, value);
  if (v > max_milliseconds) {
    throw std::invalid_argument(flag + " takes at most a day in milliseconds, got \"" + value +
                                "\"");
  }
  return std::chrono::milliseconds{ static_cast<std::chrono::milliseconds::rep>(v) };
}

void
write_file(const std::string& path, const std::function<void(std::ostream&)>& write)
{
  std::ofstream out(path);
  write(out);
  out.close();
  if (!out) {
    throw std::runtime_error("cannot write " + path);
  }
}
} // namespace

auto
main(int argc, char* argv[]) -> int
{
  using namespace couchbase::test;
  using namespace couchbase::test::bench;

  const auto suite = benchmarks();
  try {
    options opts;
    std::set<std::string> filter;
    std::string bmf_path;
    std::string json_path;
    for (int i = 1; i < argc; ++i) {
      const std::string arg{ argv[i] };
      const auto value = [&]() -> std::string {
        if (i + 1 >= argc) {
          throw std::invalid_argument(arg + " needs a value");
        }
        return argv[++i];
      };
      if (arg == "--list") {
        for (const auto& bc : suite.cases) {
          std::string requirements;
          for (const auto& req : bc.requirements) {
            requirements += requirements.empty() ? "" : "; ";
            requirements += req == nullptr ? "(null)" : req->describe();
          }
          std::cout << bc.name << '\t' << (requirements.empty() ? "no requirements" : requirements)
                    << '\n';
        }
        return 0;
      }
      if (arg == "--warmup") {
        opts.warmup = milliseconds(arg, value());
      } else if (arg == "--min-time") {
        opts.min_time = milliseconds(arg, value());
      } else if (arg == "--max-time") {
        opts.max_time = milliseconds(arg, value());
      } else if (arg == "--ci-target") {
        opts.ci_target_percent = non_negative(arg, value());
      } else if (arg == "--bmf") {
        bmf_path = value();
      } else if (arg == "--json") {
        json_path = value();
      } else if (arg.rfind("--", 0) == 0) {
        throw std::invalid_argument("unknown option " + arg);
      } else {
        filter.insert(arg);
      }
    }

    context ctx{ configuration::from_environment() };
    const auto results = run(suite, filter, ctx, opts, std::cerr);
    write_console(std::cout, results);
    if (!bmf_path.empty()) {
      write_file(bmf_path, [&](std::ostream& out) {
        write_bmf(out, results);
      });
    }
    if (!json_path.empty()) {
      write_file(json_path, [&](std::ostream& out) {
        write_result_json(out, suite.name, results, ctx);
      });
    }
    return exit_code(results);
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
