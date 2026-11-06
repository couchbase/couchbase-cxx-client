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

#pragma once

#include "bench_runner.hxx"

#include <ostream>
#include <string>
#include <vector>

namespace couchbase::test::bench
{
// One row per result, then the skip reasons grouped by requirement and the run summary.
void
write_console(std::ostream& out, const std::vector<case_result>& results);

// Bencher Metric Format: one benchmark per measured key, with these measures:
// - latency: the median in ns, with lower_value/upper_value when the median has a CI;
// - latency-p99 (ns) and throughput (ops/s), per_op keys only.
// Suspect, failed and skipped results are left out, so no threshold is fed a number that does not
// measure the case.
void
write_bmf(std::ostream& out, const std::vector<case_result>& results);

// The result JSON that bin/check-perf reads. Schema 1:
//
//   {
//     "schema": 1,
//     "suite": "<benchmark target name>",
//     "server": { "version": "<major.minor.micro>", "nodes": <n> },
//     "cases": [
//       {
//         "key": "<case>[/<variant>]",
//         "status": "measured" | "suspect" | "failed" | "skipped",
//         "detail": "<skip reason, failure message or suspect note; empty when measured>",
//         "latency_ns": { "median": <ns>, "lower": <ns>, "upper": <ns> },
//         "mean_ns": <ns>, "stddev_ns": <ns>,
//         "operations": <n>, "errors": <n>, "measured_ms": <ms>,
//         // per_op keys only:
//         "min_ns": <ns>, "p50_ns": <ns>, "p90_ns": <ns>, "p99_ns": <ns>, "p999_ns": <ns>,
//         "max_ns": <ns>, "ops_per_second": <n>
//       }
//     ]
//   }
//
// "server" is present only when the run created a probe backend, and a field its probe could not
// answer is null. Latencies are per operation, or per iteration for batched keys. A non-finite
// number is written as null.
void
write_result_json(std::ostream& out,
                  const std::string& suite,
                  const std::vector<case_result>& results,
                  context& ctx);
} // namespace couchbase::test::bench
