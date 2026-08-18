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

#include "test_helper_integration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/fmt/analytics_status.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/query_status.hxx>

#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>

//! [streaming-product]
// A document type the streaming rows are decoded into. Any type with a
// tao::json::traits specialization works with query_row::content_as() /
// analytics_row::content_as().
#include <string>

struct product {
  std::string id{};
  std::string name{};
  double price{};
};

template<>
struct tao::json::traits<product> {
  template<template<typename...> class Traits>
  static auto as(const tao::json::basic_value<Traits>& v) -> product
  {
    const auto& object = v.get_object();
    return {
      object.at("id").template as<std::string>(),
      object.at("name").template as<std::string>(),
      object.at("price").template as<double>(),
    };
  }
};
//! [streaming-product]

namespace
{
// Shared, non-example test scaffolding: put the documents the streaming examples query into
// the bucket's default collection, and make the keyspace queryable.
void
seed_products(const couchbase::cluster& cluster, const std::string& bucket_name)
{
  // Created through the index manager rather than a `CREATE PRIMARY INDEX IF NOT EXISTS`
  // statement: that syntax is only accepted from Couchbase Server 7.1, while ignore_if_exists is
  // handled by the SDK and so works on every supported server.
  auto index_err =
    cluster.query_indexes()
      .create_primary_index(bucket_name,
                            couchbase::create_primary_query_index_options{}.ignore_if_exists(true))
      .get();
  REQUIRE_SUCCESS(index_err.ec());

  auto collection = cluster.bucket(bucket_name).default_collection();
  for (std::size_t i = 0; i < 10; ++i) {
    auto [err, res] = collection
                        .upsert(fmt::format("streaming-example-{}", i),
                                tao::json::value{
                                  { "type", "streaming-example" },
                                  { "name", fmt::format("widget-{}", i) },
                                  { "price", 10.0 + static_cast<double>(i) },
                                })
                        .get();
    REQUIRE_SUCCESS(err.ec());
  }
}
} // namespace

namespace example_query_stream
{
//! [example-query-stream]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/format.h>
#include <tao/json.hpp>

// After the fmt headers: these specialize fmt::formatter, so fmt has to be declared first.
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/query_status.hxx>

#include <chrono>
#include <string>

int
main(int argc, const char* argv[])
{
  if (argc != 5) {
    fmt::print("USAGE: ./query_stream couchbase://127.0.0.1 Administrator password default\n");
    return 1;
  }

  const std::string connection_string{ argv[1] }; // "couchbase://127.0.0.1"
  const std::string username{ argv[2] };          // "Administrator"
  const std::string password{ argv[3] };          // "password"
  const std::string bucket_name{ argv[4] };       // "default"

  auto [connect_err, cluster] =
    couchbase::cluster::connect(connection_string, couchbase::cluster_options(username, password))
      .get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  const auto statement = fmt::format(R"(
      SELECT META(d).id AS id, d.name AS name, d.price AS price
      FROM `{}` AS d
      WHERE d.type = $1
      ORDER BY d.price DESC
  )",
                                     bucket_name);
  const auto options = couchbase::query_options{}
                         .positional_parameters(std::string{ "streaming-example" })
                         .scan_consistency(couchbase::query_scan_consistency::request_plus)
                         .metrics(true); // off by default; the server then omits meta.metrics()

  // [1] Start the stream. The future resolves as soon as the response preamble has been parsed --
  //     long before the whole result has been transferred. Rows are then pulled on demand, and the
  //     SDK pauses reading the socket whenever the rows it has already buffered exceed an internal
  //     high-water mark, so the memory it holds does not grow with the size of the result.
  auto [err, result] = cluster.query_stream(statement, options).get();
  if (err) {
    fmt::print("unable to start the streaming query: {}\n", err);
    return 1;
  }

  // [2] The row signature is part of that preamble, so unlike the metadata it is available
  //     immediately, without draining the stream first.
  if (const auto signature = result.signature(); signature) {
    // codec::binary is a byte vector; reinterpret to chars to print it as the JSON text it is.
    fmt::println(
      "signature: {}",
      std::string{ reinterpret_cast<const char*>(signature->data()), signature->size() });
  }

  // [3] next() is a three-state contract:
  //       (falsy error, engaged row) -- a data row
  //       (falsy error, empty)       -- clean end of stream
  //       (truthy error, empty)      -- the stream terminated with an error
  //     Only decode the row in the first case; the two terminal states carry an empty row.
  //     Only one next() may be outstanding at a time.
  fmt::println("{:<24} {:<12} {:>8}", "ID", "NAME", "PRICE");
  while (true) {
    auto [row_err, row] = result.next().get();
    if (row_err) {
      fmt::print("streaming query failed mid-stream: {}\n", row_err);
      return 1;
    }
    if (!row) {
      break; // clean end of stream
    }
    const auto p = row->content_as<couchbase::codec::tao_json_serializer, product>();
    fmt::println("{:<24} {:<12} {:>8.2f}", p.id, p.name, p.price);
  }

  // [4] The metadata resolves only once the stream has been drained (or cancelled), because the
  //     server sends it after the last row. It may be requested more than once.
  auto [meta_err, meta] = result.meta_data().get();
  if (meta_err) {
    fmt::print("unable to retrieve the query metadata: {}\n", meta_err);
    return 1;
  }
  fmt::println("status={}, request_id={}", meta.status(), meta.request_id());
  if (const auto& metrics = meta.metrics(); metrics) {
    fmt::println("rows={}, elapsed={}",
                 metrics->result_count(),
                 std::chrono::duration_cast<std::chrono::milliseconds>(metrics->elapsed_time()));
  }

  cluster.close().get();
  return 0;
}

/*

$ ./query_stream couchbase://127.0.0.1 Administrator password default
signature: {"id":"json","name":"json","price":"json"}
ID                       NAME            PRICE
streaming-example-9      widget-9        19.00
streaming-example-8      widget-8        18.00
streaming-example-7      widget-7        17.00
streaming-example-6      widget-6        16.00
streaming-example-5      widget-5        15.00
streaming-example-4      widget-4        14.00
streaming-example-3      widget-3        13.00
streaming-example-2      widget-2        12.00
streaming-example-1      widget-1        11.00
streaming-example-0      widget-0        10.00
status=success, request_id=2b6619e1-b34e-47d9-b1f7-3fa1d5fbe4b4
rows=10, elapsed=2ms

 */
//! [example-query-stream]

} // namespace example_query_stream

namespace example_query_stream_iterator
{
//! [example-query-stream-iterator]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/format.h>
#include <tao/json.hpp>

// After the fmt headers: this specializes fmt::formatter, so fmt has to be declared first.
#include <couchbase/fmt/error.hxx>

#include <chrono>
#include <cstddef>
#include <string>

int
main(int argc, const char* argv[])
{
  if (argc != 5) {
    fmt::print(
      "USAGE: ./query_stream_iterator couchbase://127.0.0.1 Administrator password default\n");
    return 1;
  }

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };
  const std::string bucket_name{ argv[4] };

  auto [connect_err, cluster] =
    couchbase::cluster::connect(connection_string, couchbase::cluster_options(username, password))
      .get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // A scope-level streaming query resolves unqualified keyspaces against that scope, so the
  // statement names the collection (`_default`) rather than the bucket.
  auto scope = cluster.bucket(bucket_name).scope("_default");

  { // [1] Range-based for over the stream. Each element is std::pair<error, query_row>: for a data
    //     row the error is falsy. If the stream ends with an error, the iterator visits exactly one
    //     final element carrying that error (with an empty row) before comparing equal to end(), so
    //     the loop can report the failure instead of silently stopping short. A clean end of stream
    //     produces no such element.
    const auto statement = R"(
        SELECT META(d).id AS id, d.name AS name, d.price AS price
        FROM _default AS d
        WHERE d.type = "streaming-example"
        ORDER BY d.price DESC
    )";
    // request_plus, because the documents were written moments ago: with the default
    // (not_bounded) the query runs against whatever the index has caught up with, so a freshly
    // written document may or may not be visible and the row count would vary between runs.
    const auto options =
      couchbase::query_options{}.scan_consistency(couchbase::query_scan_consistency::request_plus);
    auto [err, result] = scope.query_stream(statement, options).get();
    if (err) {
      fmt::print("unable to start the streaming query: {}\n", err);
      return 1;
    }

    for (auto [row_err, row] : result) {
      if (row_err) {
        fmt::print("streaming query failed mid-stream: {}\n", row_err);
        return 1;
      }
      const auto p = row.content_as<couchbase::codec::tao_json_serializer, product>();
      fmt::println("{:<24} {:<12} {:>8.2f}", p.id, p.name, p.price);
    }
  }

  { // [2] Consuming only a prefix is cheap, and safe. Rows are read from the socket on demand, so
    //     the rows that are never pulled are never transferred and never allocated: the cost of a
    //     prefix read is proportional to the prefix, not to the size of the full result. A consumer
    //     that stops pulling is not penalised either -- the streaming deadline is an inter-read
    //     idle timeout that is armed only while a socket read is in flight, so a slow or partial
    //     consumer produces no socket traffic and is never timed out.
    //
    //     The statement below yields 15000 rows of ~5.2 KB each. Compare the buffered path, which
    //     has to receive and materialise all of them before it resolves, against reading three rows
    //     from the stream and cancelling.
    const auto bulky = R"(SELECT REPEAT("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 200) AS padding
                          FROM ARRAY_RANGE(0, 15000) AS i)";

    const auto buffered_started = std::chrono::steady_clock::now();
    auto [buffered_err, buffered] = scope.query(bulky).get();
    const auto buffered_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - buffered_started);
    if (buffered_err) {
      fmt::print("unable to perform the buffered query: {}\n", buffered_err);
      return 1;
    }
    std::size_t buffered_bytes{ 0 };
    for (const auto& row : buffered.rows_as_binary()) {
      buffered_bytes += row.size();
    }
    fmt::println("query():        buffered all {} rows ({:.1f} MiB held at once) in {}",
                 buffered.rows_as_binary().size(),
                 static_cast<double>(buffered_bytes) / (1024.0 * 1024.0),
                 buffered_elapsed);

    const auto stream_started = std::chrono::steady_clock::now();
    auto [err, result] = scope.query_stream(bulky).get();
    if (err) {
      fmt::print("unable to start the streaming query: {}\n", err);
      return 1;
    }

    std::size_t seen{ 0 };
    for (auto [row_err, row] : result) {
      if (row_err) {
        fmt::print("streaming query failed mid-stream: {}\n", row_err);
        return 1;
      }
      if (++seen == 3) {
        break;
      }
    }
    // Release the connection and its timers promptly. Dropping the last handle tears the stream
    // down too, but not until any in-flight pull settles.
    result.cancel();
    const auto stream_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - stream_started);
    fmt::println("query_stream(): read {} rows and cancelled in {} -- the rest was never fetched",
                 seen,
                 stream_elapsed);
    // The streaming figure covers the same statement, run second, so it is if anything pessimistic
    // with respect to server-side caching.
  }

  cluster.close().get();
  return 0;
}

/*

$ ./query_stream_iterator couchbase://127.0.0.1 Administrator password default
streaming-example-9      widget-9        19.00
streaming-example-8      widget-8        18.00
streaming-example-7      widget-7        17.00
streaming-example-6      widget-6        16.00
streaming-example-5      widget-5        15.00
streaming-example-4      widget-4        14.00
streaming-example-3      widget-3        13.00
streaming-example-2      widget-2        12.00
streaming-example-1      widget-1        11.00
streaming-example-0      widget-0        10.00
query():        buffered all 15000 rows (74.6 MiB held at once) in 621ms
query_stream(): read 3 rows and cancelled in 12ms -- the rest was never fetched

 */
//! [example-query-stream-iterator]

} // namespace example_query_stream_iterator

namespace example_query_stream_async
{
//! [example-query-stream-async]
#include <couchbase/cluster.hxx>

#include <spdlog/fmt/bundled/format.h>

// After the fmt header: this specializes fmt::formatter, so fmt has to be declared first.
#include <couchbase/fmt/error.hxx>

#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>

// A drain that never blocks a thread: each next() completion issues the following pull from
// inside the completion handler, so the whole result is consumed on the library's I/O threads.
//
// The blocking overloads -- next().get(), meta_data().get(), and the eager iterator -- must NOT
// be used from a completion handler: they park the very thread that has to deliver the row, and
// the stream can never advance. From a handler, always use the callback next() overload.
//
// Chaining the next pull from inside the handler does not grow the stack: a row is delivered from
// the I/O event loop rather than synchronously from within next(), so the pulls do not nest.
class row_counter : public std::enable_shared_from_this<row_counter>
{
public:
  using result_type = std::pair<couchbase::error, std::uint64_t>;

  row_counter(couchbase::query_stream_result result,
              std::shared_ptr<std::promise<result_type>> done)
    : result_{ std::move(result) }
    , done_{ std::move(done) }
  {
  }

  void pull()
  {
    // shared_from_this() keeps the counter (and the stream handle it owns) alive for exactly as
    // long as pulls are outstanding.
    result_.next(
      [self = shared_from_this()](couchbase::error err, std::optional<couchbase::query_row> row) {
        if (err) {
          self->done_->set_value({ std::move(err), self->rows_ });
          return;
        }
        if (!row) {
          self->done_->set_value({ {}, self->rows_ }); // clean end of stream
          return;
        }
        ++self->rows_;
        self->pull();
      });
  }

private:
  couchbase::query_stream_result result_;
  std::shared_ptr<std::promise<result_type>> done_;
  std::uint64_t rows_{ 0 };
};

int
main(int argc, const char* argv[])
{
  if (argc != 4) {
    fmt::print("USAGE: ./query_stream_async couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto [connect_err, cluster] =
    couchbase::cluster::connect(connection_string, couchbase::cluster_options(username, password))
      .get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // However many rows the statement returns, the SDK holds only a bounded window of them: it stops
  // reading the socket once the rows it has buffered exceed a high-water mark and resumes once the
  // consumer drains back below the low-water mark. The consumer sets the pace.
  auto done = std::make_shared<std::promise<row_counter::result_type>>();
  auto finished = done->get_future();

  cluster.query_stream("SELECT n FROM ARRAY_RANGE(0, 5000) AS n",
                       couchbase::query_options{},
                       [done](couchbase::error err, couchbase::query_stream_result result) {
                         if (err) {
                           done->set_value({ std::move(err), 0 });
                           return;
                         }
                         std::make_shared<row_counter>(std::move(result), done)->pull();
                       });

  // The pump reports the row count alongside the error, because a stream can fail *after* rows
  // have already been delivered: those rows were valid and any work done on them stands. See the
  // error-handling example for the full set of failure channels.
  auto [stream_err, rows] = finished.get();
  if (stream_err) {
    fmt::print("streaming query failed after {} row(s): {}\n", rows, stream_err);
    return 1;
  }
  fmt::println("counted {} rows without blocking a thread", rows);

  cluster.close().get();
  return 0;
}

/*

$ ./query_stream_async couchbase://127.0.0.1 Administrator password
counted 5000 rows without blocking a thread

 */
//! [example-query-stream-async]

} // namespace example_query_stream_async

namespace example_analytics_stream
{
//! [example-analytics-stream]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <spdlog/fmt/bundled/format.h>
#include <tao/json.hpp>

// After the fmt header: these specialize fmt::formatter, so fmt has to be declared first.
#include <couchbase/fmt/analytics_status.hxx>
#include <couchbase/fmt/error.hxx>

#include <cstdint>
#include <string>

int
main(int argc, const char* argv[])
{
  if (argc != 4) {
    fmt::print("USAGE: ./analytics_stream couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto [connect_err, cluster] =
    couchbase::cluster::connect(connection_string, couchbase::cluster_options(username, password))
      .get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // Analytics streaming mirrors the query API: the same three-state next(), the same iterator,
  // the same end-of-stream metadata.
  auto [err, result] =
    cluster.analytics_query_stream("SELECT i AS n FROM array_range(0, 2000) AS i ORDER BY i").get();
  if (err) {
    fmt::print("unable to start the streaming analytics query: {}\n", err);
    return 1;
  }

  // The row signature comes from the response preamble, so it is available before the stream has
  // been drained (unlike the metadata).
  if (const auto signature = result.signature(); signature) {
    // codec::binary is a byte vector; reinterpret to chars to print it as the JSON text it is.
    fmt::println(
      "signature: {}",
      std::string{ reinterpret_cast<const char*>(signature->data()), signature->size() });
  }

  std::uint64_t sum{ 0 };
  while (true) {
    auto [row_err, row] = result.next().get();
    if (row_err) {
      fmt::print("streaming analytics query failed mid-stream: {}\n", row_err);
      return 1;
    }
    if (!row) {
      break; // clean end of stream
    }
    const auto value = row->content_as<couchbase::codec::tao_json_serializer, tao::json::value>();
    sum += value.at("n").as<std::uint64_t>();
  }
  fmt::println("sum of the streamed rows: {}", sum);

  auto [meta_err, meta] = result.meta_data().get();
  if (meta_err) {
    fmt::print("unable to retrieve the analytics metadata: {}\n", meta_err);
    return 1;
  }
  fmt::println("status={}, rows={}", meta.status(), meta.metrics().result_count());

  cluster.close().get();
  return 0;
}

/*

$ ./analytics_stream couchbase://127.0.0.1 Administrator password
signature: {"*":"*"}
sum of the streamed rows: 1999000
status=success, rows=2000

 */
//! [example-analytics-stream]

} // namespace example_analytics_stream

namespace example_query_stream_errors
{
//! [example-query-stream-errors]
#include <couchbase/cluster.hxx>
#include <couchbase/error_codes.hxx>

#include <spdlog/fmt/bundled/format.h>

// After the fmt header: this specializes fmt::formatter, so fmt has to be declared first.
#include <couchbase/fmt/error.hxx>

#include <chrono>
#include <cstdint>
#include <string>

namespace
{
// How far a drain got. A streaming query is not all-or-nothing the way a buffered query() is: it
// can hand over rows and only then fail, and those rows were valid. An application that mutates
// state per row has to decide whether that partial work is acceptable, so the row count travels
// with the error rather than being discarded.
struct drain_report {
  couchbase::error error{};
  std::uint64_t rows{ 0 };
};

auto
drain(const couchbase::query_stream_result& result) -> drain_report
{
  drain_report report{};
  while (true) {
    auto [row_err, row] = result.next().get();
    if (row_err) {
      report.error = std::move(row_err); // terminal error: no further row will arrive
      return report;
    }
    if (!row) {
      return report; // clean end of stream
    }
    ++report.rows;
  }
}
} // namespace

int
main(int argc, const char* argv[])
{
  if (argc != 4) {
    fmt::print("USAGE: ./query_stream_errors couchbase://127.0.0.1 Administrator password\n");
    return 1;
  }

  const std::string connection_string{ argv[1] };
  const std::string username{ argv[2] };
  const std::string password{ argv[3] };

  auto [connect_err, cluster] =
    couchbase::cluster::connect(connection_string, couchbase::cluster_options(username, password))
      .get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // A syntactically valid statement over a keyspace that does not exist. Where the failure
  // surfaces is not fixed: the query service may reject the request while the SDK is still
  // reading the response preamble, or accept it and then terminate the stream. Both channels
  // have to be handled -- a caller that only checks one of them will miss the failure.
  const auto invalid_statement = R"(SELECT * FROM `nonexistent_keyspace_xyz` LIMIT 1)";
  bool saw_error{ false };

  { // [1] Both channels for one failing statement. The error is either carried by the
    //     query_stream() future (the request never started, and the returned handle is not a
    //     usable stream), or delivered as the stream's terminal by next(). The row count that
    //     travels with the terminal is the part of the result that was consumed successfully.
    // Formatting an error renders its context too: the code and message the service itself
    // reported, the statement and encoded parameters, and the node that answered. That is what to
    // log. couchbase::error::ctx().to_json() returns the same context on its own, for a structured
    // log line.
    auto [err, result] = cluster.query_stream(invalid_statement).get();
    if (err) {
      saw_error = true;
      fmt::println("[1] rejected before streaming began: {}", err);
    } else {
      auto report = drain(result);
      if (report.error) {
        saw_error = true;
        fmt::println("[1] stream failed after {} row(s): {}", report.rows, report.error);
      } else {
        fmt::println("[1] stream ended cleanly with {} row(s)", report.rows);
      }
    }
  }

  { // [2] The iterator reports a terminal error as one final element (truthy error, empty row)
    //     before it compares equal to end(), so a range-based for loop surfaces the failure
    //     instead of quietly stopping short of the result. Ignoring the error half of the pair is
    //     how a truncated result gets mistaken for a complete one.
    auto [err, result] = cluster.query_stream(invalid_statement).get();
    if (err) {
      saw_error = true;
      fmt::println("[2] rejected before streaming began: {}", err);
    } else {
      std::uint64_t rows{ 0 };
      for (auto [row_err, row] : result) {
        if (row_err) {
          saw_error = true;
          fmt::println("[2] iterator saw the terminal error after {} row(s): {}", rows, row_err);
          break;
        }
        ++rows;
      }
    }
  }

  { // [3] Deliberate teardown is a terminal too, and has to be told apart from a real failure:
    //     after cancel(), next() reports errc::common::request_canceled. The rows consumed before
    //     the cancel are unaffected -- this is the partial-success case that a buffered query()
    //     cannot produce.
    auto [err, result] = cluster.query_stream("SELECT n FROM ARRAY_RANGE(0, 15000) AS n").get();
    if (err) {
      fmt::print("unable to start the streaming query: {}\n", err);
      return 1;
    }

    std::uint64_t rows{ 0 };
    couchbase::error before_cancel{};
    while (rows < 3) {
      auto [row_err, row] = result.next().get();
      if (row_err) {
        before_cancel = std::move(row_err);
        break;
      }
      if (!row) {
        break; // the result was shorter than expected
      }
      ++rows;
    }
    result.cancel();

    const auto after_cancel = drain(result);
    if (before_cancel) {
      fmt::println("[3] the stream failed before it could be cancelled: {}", before_cancel);
    } else if (after_cancel.error.ec() == couchbase::errc::common::request_canceled) {
      fmt::println("[3] stream cancelled after {} row(s); those rows are still valid", rows);
    } else {
      fmt::println("[3] unexpected terminal after cancel(): \"{}\"",
                   after_cancel.error.ec().message());
    }

    // [4] The terminal is sticky and idempotent: further next() calls re-deliver it rather than
    //     blocking on a drained stream, so a drain loop can never hang on a dead stream.
    auto [again_err, again_row] = result.next().get();
    fmt::println("[4] re-reading past the terminal: error=\"{}\", row_present={}",
                 again_err.ec().message(),
                 again_row.has_value());

    // [5] meta_data() resolves with the failure instead of parking forever waiting for a trailer
    //     that will never arrive.
    auto [meta_err, meta] = result.meta_data().get();
    fmt::println("[5] meta_data() after a torn-down stream: \"{}\"", meta_err.ec().message());
  }

  { // [6] Timeouts are classified by whether the request could have applied a mutation:
    //     errc::common::unambiguous_timeout when the request is read-only (it definitely did not
    //     apply, so retrying is safe) and errc::common::ambiguous_timeout otherwise (it may
    //     already have been applied -- do not blindly retry). The classification follows the
    //     request's read-only flag, which defaults to false: the SDK does not infer read-only-ness
    //     from the statement text, so mark read-only queries explicitly to get the retryable
    //     classification.
    //
    //     The deadline applies as a whole-request timeout until the response headers arrive, and
    //     from then on as an *inter-read idle* timeout that is armed only while a socket read is in
    //     flight. So a slow consumer, which generates no socket traffic, is never timed out; a
    //     mid-stream fire means the server stalled mid-body. Either way the classification is the
    //     same, and the code below handles the deadline firing on either side of the preamble.
    const auto options = couchbase::query_options{}.readonly(true).timeout(
      std::chrono::milliseconds{ 1 }); // far too short, on purpose
    auto [err, result] =
      cluster.query_stream("SELECT n FROM ARRAY_RANGE(0, 15000) AS n", options).get();
    auto ec = err.ec();
    std::uint64_t rows{ 0 };
    if (!ec) {
      const auto report = drain(result);
      ec = report.error.ec();
      rows = report.rows;
    }
    if (ec == couchbase::errc::common::unambiguous_timeout) {
      fmt::println("[6] read-only statement timed out after {} row(s); retrying it is safe", rows);
    } else if (ec == couchbase::errc::common::ambiguous_timeout) {
      fmt::println("[6] statement timed out ambiguously after {} row(s); do not blindly retry",
                   rows);
    } else {
      fmt::println("[6] statement did not time out ({} row(s), \"{}\")", rows, ec.message());
    }
  }

  if (!saw_error) {
    fmt::print("expected the invalid statement to fail, but it did not\n");
    return 1;
  }

  cluster.close().get();
  return 0;
}

/*

$ ./query_stream_errors couchbase://127.0.0.1 Administrator password
[1] rejected before streaming began: index_failure (202) | {"client_context_id":"1f2170-81d8-224f-
    45e4-6df76260782b22","first_error_code":12003,"first_error_message":"Keyspace not found in CB
    datastore: default:nonexistent_keyspace_xyz (near line 1, column 15) - cause: No bucket named
    nonexistent_keyspace_xyz","hostname":"172.18.0.5", ... ,"statement":"SELECT * FROM
    `nonexistent_keyspace_xyz` LIMIT 1"}
    (one line in reality; wrapped here, and the remaining context fields elided, for readability)
[2] rejected before streaming began: index_failure (202) | { ... same context ... }
[3] stream cancelled after 3 row(s); those rows are still valid
[4] re-reading past the terminal: error="request_canceled (2)", row_present=false
[5] meta_data() after a torn-down stream: "request_canceled (2)"
[6] read-only statement timed out after 0 row(s); retrying it is safe

 */
//! [example-query-stream-errors]

} // namespace example_query_stream_errors

TEST_CASE("example: streaming query with next()", "[integration]")
{
  couchbase::test::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }

  const auto env = couchbase::test::test_context::load_from_environment();
  seed_products(integration.public_cluster(), env.bucket);

  const char* argv[] = {
    "query_stream", // name of the "executable"
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
    env.bucket.c_str(),
  };
  REQUIRE(example_query_stream::main(5, argv) == 0);
}

TEST_CASE("example: streaming query with the iterator", "[integration]")
{
  couchbase::test::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  const auto env = couchbase::test::test_context::load_from_environment();
  seed_products(integration.public_cluster(), env.bucket);

  const char* argv[] = {
    "query_stream_iterator", env.connection_string.c_str(),
    env.username.c_str(),    env.password.c_str(),
    env.bucket.c_str(),
  };
  REQUIRE(example_query_stream_iterator::main(5, argv) == 0);
}

TEST_CASE("example: streaming query with a non-blocking pull loop", "[integration]")
{
  couchbase::test::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }

  const auto env = couchbase::test::test_context::load_from_environment();
  const char* argv[] = {
    "query_stream_async",
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };
  REQUIRE(example_query_stream_async::main(4, argv) == 0);
}

TEST_CASE("example: handling errors while streaming", "[integration]")
{
  couchbase::test::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }

  const auto env = couchbase::test::test_context::load_from_environment();
  const char* argv[] = {
    "query_stream_errors",
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };
  REQUIRE(example_query_stream_errors::main(4, argv) == 0);
}

TEST_CASE("example: streaming analytics query", "[integration]")
{
  couchbase::test::integration_test_guard integration;
  if (integration.ctx.deployment == couchbase::test::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  const auto env = couchbase::test::test_context::load_from_environment();
  const char* argv[] = {
    "analytics_stream",
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };
  REQUIRE(example_analytics_stream::main(4, argv) == 0);
}
