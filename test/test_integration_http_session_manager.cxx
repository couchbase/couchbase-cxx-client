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

#include "core/io/http_session_manager.hxx"

#include <couchbase/build_config.hxx>

TEST_CASE("integration: random node selection with analytics service", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_analytics_service()) {
    SKIP("Requires analytics service");
  }

  auto [mgr_ec, session_mgr] = integration.cluster.http_session_manager();
  REQUIRE_SUCCESS(mgr_ec);

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  auto barrier = std::make_shared<std::promise<bool>>();
  session_mgr->add_to_deferred_queue([barrier](couchbase::core::error_union err) mutable {
    REQUIRE(std::holds_alternative<std::monostate>(err));
    barrier->set_value(true);
  });
  auto fut = barrier->get_future();
  fut.get();
#endif

  auto [origin_ec, origin] = integration.cluster.origin();
  REQUIRE_SUCCESS(origin_ec);

  auto [session_ec, session] =
    session_mgr->check_out(couchbase::core::service_type::analytics, origin.credentials(), "");
  REQUIRE_SUCCESS(session_ec);

  auto last_addr = fmt::format("{}:{}", session->hostname(), session->port());

  session_mgr->check_in(couchbase::core::service_type::analytics, session);
  auto [session2_ec, session2] = session_mgr->check_out(
    couchbase::core::service_type::analytics, origin.credentials(), "", last_addr);
  REQUIRE_SUCCESS(session2_ec);

  auto new_addr = fmt::format("{}:{}", session2->hostname(), session2->port());

  if (integration.number_of_analytics_nodes() > 1) {
    REQUIRE(new_addr != last_addr);
  } else {
    REQUIRE(new_addr == last_addr);
  }
}
