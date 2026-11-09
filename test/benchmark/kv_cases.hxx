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

// The measured part of the KV cases, shared by the fake-server and cluster binaries so that their
// rows are comparable: the same documents, key sequence and operations.

#include "bench_framework.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace couchbase::test::bench::kv
{
// The bench_case::description of each case, which both binaries register.
constexpr const char* get_description =
  "Gets one of 1,000 small JSON documents per operation, blocking on 1 or 8 threads, or 64 "
  "in flight.";
constexpr const char* upsert_description =
  "Upserts one of 1,000 small JSON documents per operation, blocking on 1 or 8 threads, or "
  "64 in flight.";
constexpr const char* upsert_get_description =
  "Upserts, then gets, one of 100,000 512-byte JSON documents per operation on 20 "
  "threads.";

constexpr std::size_t document_count = 1000;
constexpr std::size_t thread_stride = 7919;

inline auto
document_key(std::size_t i) -> std::string
{
  return "bench::" + std::to_string(i);
}

// Four members, one a string of payload_size characters.
inline auto
document_body(std::size_t payload_size = 48) -> tao::json::value
{
  return {
    { "name", "benchmark document" },
    { "kind", "kv" },
    { "payload", std::string(payload_size, 'x') },
    { "flag", true },
  };
}

// Stores every document. Throws when a store fails, which fails the case.
inline void
seed(const collection& c)
{
  const auto body = document_body();
  for (std::size_t i = 0; i < document_count; ++i) {
    if (auto [e, r] = c.upsert(document_key(i), body).get(); e) {
      throw std::runtime_error("seeding failed: " + e.message());
    }
  }
}

enum class profile : std::uint8_t {
  sdk_default,
  bare,
};

// The options both binaries connect with. Every setting below is set in both profiles, the
// compression thresholds included. The default profile sets the SDK's defaults explicitly, so a
// change of an SDK default does not move these numbers until connect_options() follows it. bare
// turns off metrics, tracing, application telemetry and both transaction cleanups.
inline auto
connect_options(const std::string& username, const std::string& password, profile p)
  -> cluster_options
{
  const bool on = p == profile::sdk_default;
  cluster_options options(username, password);
  options.metrics().enable(on);
  options.tracing().enable(on);
  options.application_telemetry().enable(on);
  options.compression().enabled(true).min_size(32).min_ratio(0.83);
  options.transactions().cleanup_config().cleanup_lost_attempts(on);
  options.transactions().cleanup_config().cleanup_client_attempts(on);
  return options;
}

constexpr std::size_t pipeline_depth = 64;

// Variant <prefix>/q64, where prefix is <target> or <target>/bare: one thread keeps pipeline_depth
// operations in flight. An iteration waits for the oldest operation and starts the next. Once round
// trips overlap, the time per iteration is the time per operation. start(n) starts operation n. The
// first iteration fills the window, so a key the filter does not select starts nothing. A failed
// operation throws, which fails the case. After the measurement every operation still in flight is
// consumed. No measured iteration waited for any of them, so the key keeps the samples it recorded,
// and the first failure among them throws once all are done, which fails the case. When the
// measurement throws, they are waited for and the exception is rethrown.
template<typename Start>
void
measure_pipelined(state& st, std::string_view prefix, Start start)
{
  std::deque<decltype(start(std::size_t{}))> window;
  const auto drain = [&window] {
    for (auto& op : window) {
      // The front of the window has been consumed by get() when its operation failed.
      if (op.valid()) {
        op.wait();
      }
    }
  };
  try {
    st.batched(std::string{ prefix } + "/q" + std::to_string(pipeline_depth),
               1,
               [&window, &start, n = std::size_t{ 0 }]() mutable {
                 if (window.empty()) {
                   while (window.size() < pipeline_depth) {
                     window.push_back(start(n++));
                   }
                 }
                 if (auto err = window.front().get().first; err) {
                   throw std::runtime_error("operation failed: " + err.message());
                 }
                 window.pop_front();
                 window.push_back(start(n++));
               });
  } catch (...) {
    drain();
    throw;
  }
  std::string failure;
  for (auto& op : window) {
    if (auto err = op.get().first; err && failure.empty()) {
      failure = "operation failed: " + err.message();
    }
  }
  if (!failure.empty()) {
    throw std::runtime_error(failure);
  }
}

// Variants <target>/t1, <target>/t8 and <target>/q64 for the default profile, <target>/bare/t1 and
// <target>/bare/q64 for bare. In t1 and t8, thread t operates on key
// (t * thread_stride + n) % document_count, n being that thread's operation count. In q64,
// operation n uses key n % document_count. In every variant a failed operation throws, which fails
// the case.
inline void
measure(state& st, std::string_view target, profile p, const collection& c, bool write)
{
  const auto body = document_body();
  const auto prefix = std::string{ target } + (p == profile::bare ? "/bare" : "");
  const std::vector<std::size_t> thread_counts =
    p == profile::bare ? std::vector<std::size_t>{ 1 } : std::vector<std::size_t>{ 1, 8 };
  for (const std::size_t threads : thread_counts) {
    // Thread t counts in counters[t][0]. The counters are 64 bytes apart, a cache line, so no two
    // threads write the same line. alignas would pad the type, which MSVC reports as C4324.
    std::vector<std::array<std::size_t, 64 / sizeof(std::size_t)>> counters(threads);
    st.per_op(prefix + "/t" + std::to_string(threads), threads, [&](std::size_t t) {
      const auto key = document_key(((t * thread_stride) + counters[t][0]++) % document_count);
      const auto err = write ? c.upsert(key, body).get().first : c.get(key).get().first;
      if (err) {
        throw std::runtime_error("operation failed: " + err.message());
      }
      return true;
    });
  }
  if (write) {
    measure_pipelined(st, prefix, [&](std::size_t n) {
      return c.upsert(document_key(n % document_count), body);
    });
  } else {
    measure_pipelined(st, prefix, [&](std::size_t n) {
      return c.get(document_key(n % document_count));
    });
  }
}
// Connects in profile p, stores the documents and measures the variants of p. The connection is
// closed however the measurement ends. Each profile connects, and seeds, whether or not the filter
// selects one of its keys.
inline void
connect_and_measure(state& st,
                    std::string_view target,
                    profile p,
                    const std::string& connection_string,
                    const std::string& username,
                    const std::string& password,
                    const std::string& bucket,
                    bool write)
{
  auto [err, c] = cluster::connect(connection_string, connect_options(username, password, p)).get();
  if (err) {
    throw std::runtime_error("connect failed: " + err.message());
  }
  try {
    const auto coll = c.bucket(bucket).default_collection();
    seed(coll);
    measure(st, target, p, coll, write);
  } catch (...) {
    c.close().get();
    throw;
  }
  c.close().get();
}

constexpr std::size_t upsert_get_document_count = 100000;
constexpr std::size_t upsert_get_threads = 20;
constexpr std::size_t upsert_get_document_size = 512;
// The payload that makes the JSON of document_body() upsert_get_document_size bytes.
constexpr std::size_t upsert_get_payload_size = 446;

// Stores value at keys 0 to upsert_get_document_count - 1 through raw_json_transcoder, with
// pipeline_depth upserts in flight. After a failure no further upsert starts. The first failure
// throws once the started upserts finish.
inline void
seed_upsert_get(const collection& c, const std::string& value)
{
  std::deque<decltype(c.upsert<codec::raw_json_transcoder>(std::string{}, value))> window;
  std::string failure;
  std::size_t next = 0;
  while (!window.empty() || (failure.empty() && next < upsert_get_document_count)) {
    if (failure.empty() && next < upsert_get_document_count && window.size() < pipeline_depth) {
      window.push_back(c.upsert<codec::raw_json_transcoder>(document_key(next++), value));
      continue;
    }
    if (auto err = window.front().get().first; err && failure.empty()) {
      failure = "seeding failed: " + err.message();
    }
    window.pop_front();
  }
  if (!failure.empty()) {
    throw std::runtime_error(failure);
  }
}

inline void
close_clusters(std::vector<cluster>& clusters)
{
  for (auto& c : clusters) {
    c.close().get();
  }
}

// Connects n clusters concurrently in the default profile and passes them to body. They are closed
// however body ends. When a connection fails, the others are closed and it throws.
template<typename Body>
void
with_clusters(std::size_t n,
              const std::string& connection_string,
              const std::string& username,
              const std::string& password,
              Body body)
{
  const auto options = connect_options(username, password, profile::sdk_default);
  std::vector<std::future<std::pair<error, cluster>>> pending;
  for (std::size_t i = 0; i < n; ++i) {
    pending.push_back(cluster::connect(connection_string, options));
  }
  std::vector<cluster> clusters;
  std::string failure;
  for (auto& f : pending) {
    auto [err, c] = f.get();
    if (!err) {
      clusters.push_back(std::move(c));
    } else if (failure.empty()) {
      failure = "connect failed: " + err.message();
    }
  }
  try {
    if (!failure.empty()) {
      throw std::runtime_error(failure);
    }
    body(clusters);
  } catch (...) {
    close_clusters(clusters);
    throw;
  }
  close_clusters(clusters);
}

// Variants <target>/t20, <target>/t20/raw and <target>/c20, all in the default profile. One
// operation is upsert(key, value) and then get(key), each waited for. Thread w, on its n-th
// operation, uses key (n * upsert_get_threads + w) % upsert_get_document_count.
//
// - t20: upsert_get_threads threads share one cluster. The value is
//   document_body(upsert_get_payload_size), built on every upsert and stored through the default
//   transcoder; the get decodes it into a tao::json::value.
// - t20/raw: as t20, but the value is that document serialised once, stored through
//   raw_json_transcoder, and the get reads it back through raw_json_transcoder without parsing.
// - c20: as t20, but each thread has a cluster of its own, connected before and closed after the
//   measurement.
//
// A failed upsert or get throws, which fails the case. The documents are stored and the c20
// clusters are connected even when the filter selects none of these keys.
inline void
measure_upsert_get(state& st,
                   std::string_view target,
                   const std::string& connection_string,
                   const std::string& username,
                   const std::string& password,
                   const std::string& bucket)
{
  const auto raw = tao::json::to_string(document_body(upsert_get_payload_size));
  if (raw.size() != upsert_get_document_size) {
    throw std::logic_error("the upsert_get document is " + std::to_string(raw.size()) + " bytes");
  }
  const auto run = [&](std::string_view variant,
                       const std::vector<collection>& colls,
                       bool raw_path) {
    // Thread w counts in counters[w][0], a cache line apart from the other threads.
    std::vector<std::array<std::size_t, 64 / sizeof(std::size_t)>> counters(upsert_get_threads);
    st.per_op(
      std::string{ target } + "/" + std::string{ variant }, upsert_get_threads, [&](std::size_t w) {
        const auto& c = colls[w % colls.size()];
        const auto key =
          document_key(((counters[w][0]++ * upsert_get_threads) + w) % upsert_get_document_count);
        const auto err = raw_path
                           ? c.upsert<codec::raw_json_transcoder>(key, raw).get().first
                           : c.upsert(key, document_body(upsert_get_payload_size)).get().first;
        if (err) {
          throw std::runtime_error("upsert failed: " + err.message());
        }
        auto [get_err, r] = c.get(key).get();
        if (get_err) {
          throw std::runtime_error("get failed: " + get_err.message());
        }
        if (raw_path) {
          do_not_optimize(r.content_as<std::string, codec::raw_json_transcoder>());
        } else {
          do_not_optimize(r.content_as<tao::json::value>());
        }
        return true;
      });
  };
  with_clusters(1, connection_string, username, password, [&](std::vector<cluster>& clusters) {
    const auto c = clusters.front().bucket(bucket).default_collection();
    seed_upsert_get(c, raw);
    run("t20", { c }, false);
    run("t20/raw", { c }, true);
  });
  with_clusters(
    upsert_get_threads, connection_string, username, password, [&](std::vector<cluster>& clusters) {
      std::vector<collection> colls;
      for (auto& c : clusters) {
        colls.push_back(c.bucket(bucket).default_collection());
      }
      run("c20", colls, false);
    });
}
} // namespace couchbase::test::bench::kv
