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

#include "test_helper_integration.hxx"

#include "core/columnar/agent.hxx"
#include "core/columnar/error_codes.hxx"
#include "core/free_form_http_request.hxx"
#include "core/row_streamer.hxx"

#include <future>
#include <memory>
#include <string>
#include <variant>

auto
get_next_item(couchbase::core::columnar::query_result& result)
  -> std::pair<std::variant<std::monostate,
                            couchbase::core::columnar::query_result_row,
                            couchbase::core::columnar::query_result_end>,
               couchbase::core::columnar::error>
{
  auto barrier = std::make_shared<
    std::promise<std::pair<std::variant<std::monostate,
                                        couchbase::core::columnar::query_result_row,
                                        couchbase::core::columnar::query_result_end>,
                           couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  result.next_row([barrier](auto item, auto err) mutable {
    barrier->set_value({ std::move(item), err });
  });
  return f.get();
}

auto
buffer_rows(couchbase::core::columnar::query_result& result)
  -> std::pair<std::vector<couchbase::core::columnar::query_result_row>,
               couchbase::core::columnar::error>
{
  std::vector<couchbase::core::columnar::query_result_row> rows{};
  while (true) {
    auto [item, err] = get_next_item(result);
    if (err) {
      return { rows, err };
    }
    REQUIRE(!std::holds_alternative<std::monostate>(item));

    if (std::holds_alternative<couchbase::core::columnar::query_result_end>(item)) {
      break;
    }

    REQUIRE(std::holds_alternative<couchbase::core::columnar::query_result_row>(item));
    rows.emplace_back(std::get<couchbase::core::columnar::query_result_row>(item));
  }
  return { rows, {} };
}

TEST_CASE("integration: columnar http component simple request", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  auto agent = couchbase::core::columnar::agent(integration.io, { { integration.cluster } });

  tao::json::value body{ { "statement", "FROM RANGE(0, 100) AS i SELECT *" } };

  auto req = couchbase::core::http_request{
    couchbase::core::service_type::analytics,     "POST", {}, "/analytics/service", {}, {},
    couchbase::core::utils::json::generate(body),
  };

  req.timeout = std::chrono::seconds(10);
  req.headers["content-type"] = "application/json";

  couchbase::core::http_response resp;
  {
    auto barrier = std::make_shared<
      std::promise<tl::expected<couchbase::core::http_response, couchbase::core::error_union>>>();
    auto f = barrier->get_future();
    auto op =
      agent.free_form_http_request(req, [barrier](auto resp, couchbase::core::error_union err) {
        if (!std::holds_alternative<std::monostate>(err)) {
          barrier->set_value(tl::unexpected(err));
          return;
        }
        barrier->set_value(std::move(resp));
      });
    REQUIRE(op.has_value());
    auto r = f.get();
    REQUIRE(r.has_value());
    resp = r.value();
  }

  auto code = resp.status_code();
  REQUIRE(code == 200);

  auto resp_body = resp.body();
  std::string buffered_body{};
  while (true) {
    auto barrier = std::make_shared<std::promise<std::pair<std::string, std::error_code>>>();
    auto f = barrier->get_future();
    resp_body.next([barrier](auto s, auto ec) {
      barrier->set_value({ std::move(s), ec });
    });
    auto [s, ec] = f.get();
    REQUIRE_SUCCESS(ec);
    if (s.empty()) {
      break;
    }
    buffered_body.append(s);
  }

  auto body_json = couchbase::core::utils::json::parse(buffered_body);
  REQUIRE(body_json.find("results") != nullptr);

  auto result_array = body_json["results"].get_array();
  REQUIRE(result_array.size() == 101);
  for (std::size_t i = 0; i <= 100; i++) {
    REQUIRE(result_array.at(i) == tao::json::value{ { "i", i } });
  }

  resp_body.cancel();
}

TEST_CASE("integration: columnar http component simple request buffered", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  auto agent = couchbase::core::columnar::agent(integration.io, { { integration.cluster } });

  tao::json::value body{ { "statement", "FROM RANGE(0, 100) AS i SELECT *" } };

  auto req = couchbase::core::http_request{
    couchbase::core::service_type::analytics,     "POST", {}, "/analytics/service", {}, {},
    couchbase::core::utils::json::generate(body),
  };

  req.timeout = std::chrono::seconds(10);
  req.headers["content-type"] = "application/json";

  couchbase::core::buffered_http_response resp;
  {
    auto barrier = std::make_shared<
      std::promise<tl::expected<couchbase::core::buffered_http_response, std::error_code>>>();
    auto f = barrier->get_future();
    auto op = agent.free_form_http_request_buffered(std::move(req), [barrier](auto resp, auto ec) {
      if (ec) {
        barrier->set_value(tl::unexpected(ec));
        return;
      }
      barrier->set_value(std::move(resp));
    });
    REQUIRE(op.has_value());
    auto r = f.get();
    REQUIRE(r.has_value());
    resp = r.value();
  }

  auto code = resp.status_code();
  REQUIRE(code == 200);

  tao::json::value body_json =
    couchbase::core::utils::json::parse(static_cast<std::string_view>(resp.body()));
  REQUIRE(body_json.find("results") != nullptr);

  auto result_array = body_json["results"].get_array();
  REQUIRE(result_array.size() == 101);
  for (std::size_t i = 0; i <= 100; i++) {
    REQUIRE(result_array.at(i) == tao::json::value{ { "i", i } });
  }
}

TEST_CASE("integration: columnar query component simple request", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 4999) AS i SELECT *" };

  couchbase::core::columnar::query_result result;
  {
    auto barrier = std::make_shared<std::promise<
      std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
      barrier->set_value({ std::move(res), err });
    });
    auto [res, err] = f.get();
    REQUIRE(resp.has_value());
    REQUIRE_SUCCESS(err.ec);
    REQUIRE_FALSE(res.metadata().has_value());
    result = std::move(res);
  }

  std::size_t row_count{ 0 };
  while (true) {
    auto [item, err] = get_next_item(result);
    REQUIRE_SUCCESS(err.ec);
    REQUIRE(!std::holds_alternative<std::monostate>(item));

    if (std::holds_alternative<couchbase::core::columnar::query_result_end>(item)) {
      break;
    }

    REQUIRE(std::holds_alternative<couchbase::core::columnar::query_result_row>(item));
    auto row = std::get<couchbase::core::columnar::query_result_row>(item);
    auto row_json = couchbase::core::utils::json::parse(row.content);
    REQUIRE(row_json == tao::json::value{ { "i", row_count } });

    row_count++;
  }
  REQUIRE(result.metadata().has_value());
  REQUIRE(result.metadata()->warnings.empty());
  REQUIRE(result.metadata()->metrics.result_count == 5000);
  REQUIRE(row_count == 5000);
}

TEST_CASE("integration: columnar query component simple request - single row response",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT \"bar\" AS foo" };

  couchbase::core::columnar::query_result result;
  {
    auto barrier = std::make_shared<std::promise<
      std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
      barrier->set_value({ std::move(res), err });
    });
    auto [res, err] = f.get();
    REQUIRE(resp.has_value());
    REQUIRE_SUCCESS(err.ec);
    REQUIRE_FALSE(res.metadata().has_value());
    result = std::move(res);
  }

  std::size_t row_count{ 0 };
  while (true) {
    auto [item, err] = get_next_item(result);
    REQUIRE_SUCCESS(err.ec);
    REQUIRE(!std::holds_alternative<std::monostate>(item));

    if (std::holds_alternative<couchbase::core::columnar::query_result_end>(item)) {
      break;
    }

    REQUIRE(std::holds_alternative<couchbase::core::columnar::query_result_row>(item));
    auto row = std::get<couchbase::core::columnar::query_result_row>(item);
    auto row_json = couchbase::core::utils::json::parse(row.content);
    REQUIRE(row_json == tao::json::value{ { "foo", "bar" } });

    row_count++;
  }
  REQUIRE(result.metadata().has_value());
  REQUIRE(result.metadata()->warnings.empty());
  REQUIRE(result.metadata()->metrics.result_count == 1);
  REQUIRE(row_count == 1);
}

TEST_CASE("integration: columnar query component request with database & scope names",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT * FROM airline LIMIT 100" };
  options.database_name = "travel-sample";
  options.scope_name = "inventory";

  couchbase::core::columnar::query_result result;
  {
    auto barrier = std::make_shared<std::promise<
      std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
      barrier->set_value({ std::move(res), err });
    });
    auto [res, err] = f.get();
    REQUIRE(resp.has_value());
    REQUIRE_SUCCESS(err.ec);
    REQUIRE_FALSE(res.metadata().has_value());
    result = std::move(res);
  }

  std::size_t row_count{ 0 };
  while (true) {
    auto [item, err] = get_next_item(result);
    REQUIRE_SUCCESS(err.ec);
    REQUIRE(!std::holds_alternative<std::monostate>(item));

    if (std::holds_alternative<couchbase::core::columnar::query_result_end>(item)) {
      break;
    }

    REQUIRE(std::holds_alternative<couchbase::core::columnar::query_result_row>(item));
    auto row = std::get<couchbase::core::columnar::query_result_row>(item);
    REQUIRE(!row.content.empty());

    row_count++;
  }
  REQUIRE(result.metadata().has_value());
  REQUIRE(result.metadata()->warnings.empty());
  REQUIRE(result.metadata()->metrics.result_count == 100);
  REQUIRE(row_count == 100);
}

TEST_CASE("integration: columnar query read some rows and cancel")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 100) AS i SELECT *" };

  couchbase::core::columnar::query_result result;
  {
    auto barrier = std::make_shared<std::promise<
      std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
      barrier->set_value({ std::move(res), err });
    });
    auto [res, err] = f.get();
    REQUIRE(resp.has_value());
    REQUIRE_SUCCESS(err.ec);
    REQUIRE_FALSE(res.metadata().has_value());
    result = std::move(res);
  }

  std::vector<std::string> buffered_rows{};
  for (std::size_t i = 0; i < 2; i++) {
    auto [item, err] = get_next_item(result);
    REQUIRE_SUCCESS(err.ec);
    REQUIRE(!std::holds_alternative<std::monostate>(item));

    if (std::holds_alternative<couchbase::core::columnar::query_result_end>(item)) {
      break;
    }
    buffered_rows.emplace_back(std::get<couchbase::core::columnar::query_result_row>(item).content);
  }
  REQUIRE_FALSE(result.metadata().has_value());
  REQUIRE(buffered_rows.size() == 2);

  result.cancel();

  for (std::size_t i = 0; i < 2; i++) {
    auto [item, err] = get_next_item(result);
    REQUIRE(err.ec == couchbase::core::columnar::client_errc::canceled);
    REQUIRE(std::holds_alternative<std::monostate>(item));
  }

  REQUIRE_FALSE(result.metadata().has_value());
}

TEST_CASE("integration: columnar query cancel operation")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 10000000) AS i SELECT *" };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  resp.value()->cancel();
  auto [res, err] = f.get();
  REQUIRE(err.ec == couchbase::core::columnar::client_errc::canceled);
}

TEST_CASE("integration: columnar query operation timeout")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 10000000) AS i SELECT *" };
  options.read_only = true;
  options.timeout = std::chrono::seconds(1);

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  auto [res, err] = f.get();
  REQUIRE(err.ec == couchbase::core::columnar::errc::timeout);
}

TEST_CASE("integration: columnar query global timeout")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::timeout_config timeouts{};
  timeouts.query_timeout = std::chrono::milliseconds(1);
  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster }, timeouts } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 200) AS i SELECT *" };
  options.read_only = true;

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  auto [res, err] = f.get();
  REQUIRE(err.ec == couchbase::core::columnar::errc::timeout);
}

TEST_CASE("integration: columnar query collection does not exist")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT * FROM `does-not-exist`" };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  auto [res, err] = f.get();
  REQUIRE(err.ec == couchbase::core::columnar::errc::query_error);

  REQUIRE(
    std::holds_alternative<couchbase::core::columnar::query_error_properties>(err.properties));
  REQUIRE(std::get<couchbase::core::columnar::query_error_properties>(err.properties).code ==
          24045);
  REQUIRE(std::get<couchbase::core::columnar::query_error_properties>(err.properties)
            .server_message.find("does-not-exist") != std::string::npos);

  REQUIRE(err.ctx.find("errors") != nullptr);
  REQUIRE(err.ctx["errors"].get_array().size() == 1);
  REQUIRE(err.ctx["errors"].get_array().at(0).get_object().at("code").get_signed() == 24045);
  REQUIRE(!err.ctx["last_dispatched_to"].get_string().empty());
  REQUIRE(!err.ctx["last_dispatched_from"].get_string().empty());
  REQUIRE(err.message_with_ctx().find("\"code\":24045") != std::string::npos);
}

TEST_CASE("integration: columnar query positional parameters")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT $1 AS foo" };
  options.positional_parameters = { { "\"bar\"" } };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  auto [res, err] = f.get();
  REQUIRE_SUCCESS(err.ec);
  auto [rows, rows_err] = buffer_rows(res);
  REQUIRE_SUCCESS(rows_err.ec);
  REQUIRE(rows.size() == 1);
  REQUIRE(couchbase::core::utils::json::parse(rows.at(0).content) ==
          tao::json::value{ { "foo", "bar" } });
}

TEST_CASE("integration: columnar query named parameters")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT $val AS foo" };
  options.named_parameters["val"] = couchbase::core::json_string{ "\"bar\"" };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());
  auto [res, err] = f.get();
  REQUIRE_SUCCESS(err.ec);
  auto [rows, rows_err] = buffer_rows(res);
  REQUIRE_SUCCESS(rows_err.ec);
  REQUIRE(rows.size() == 1);
  REQUIRE(couchbase::core::utils::json::parse(rows.at(0).content) ==
          tao::json::value{ { "foo", "bar" } });
}

TEST_CASE("integration: closing cluster before columnar query returns")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 9999) AS i SELECT *" };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());

  auto close_barrier = std::make_shared<std::promise<bool>>();
  auto close_fut = close_barrier->get_future();

  integration.cluster.close([close_barrier]() {
    close_barrier->set_value(true);
  });
  close_fut.get();

  auto [res, err] = f.get();
  REQUIRE(err.ec == couchbase::core::columnar::client_errc::canceled);
}

TEST_CASE("integration: closing cluster while reading columnar query rows")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "FROM RANGE(0, 9999) AS i SELECT *" };

  auto barrier = std::make_shared<std::promise<
    std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
  auto f = barrier->get_future();
  auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
    barrier->set_value({ std::move(res), err });
  });
  REQUIRE(resp.has_value());

  auto [res, err] = f.get();
  REQUIRE_SUCCESS(err.ec);

  auto close_barrier = std::make_shared<std::promise<bool>>();
  auto close_fut = close_barrier->get_future();

  integration.cluster.close([close_barrier]() {
    close_barrier->set_value(true);
  });
  close_fut.get();

  auto [rows, rows_err] = buffer_rows(res);
  REQUIRE(rows_err.ec == couchbase::core::columnar::client_errc::canceled);
}

TEST_CASE("integration: columnar query component timeout in raw", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  couchbase::core::columnar::agent agent{ integration.io, { { integration.cluster } } };

  couchbase::core::columnar::query_options options{ "SELECT SLEEP(1,10000);" };
  options.timeout = std::chrono::seconds(1);

  // The first request will be sent with this timeout. The server tells us to retry the server
  // timeout. This means that once we eventually time out in the client, the "server timeout" will
  // be reported in the error context.
  options.raw = { { "timeout", couchbase::core::json_string{ "\"1ms\"" } } };

  couchbase::core::columnar::query_result result;
  {
    auto barrier = std::make_shared<std::promise<
      std::pair<couchbase::core::columnar::query_result, couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto resp = agent.execute_query(options, [barrier](auto res, auto err) mutable {
      barrier->set_value({ std::move(res), err });
    });
    auto [res, err] = f.get();
    REQUIRE(resp.has_value());
    REQUIRE(err.ec == couchbase::core::columnar::errc::timeout);
    REQUIRE(err.ctx["last_errors"].get_array()[0].get_object()["code"].get_signed() == 21002);
    REQUIRE(err.ctx["last_errors"].get_array()[0].get_object()["msg"].get_string() ==
            "Request timed out and will be cancelled");
  }
}
