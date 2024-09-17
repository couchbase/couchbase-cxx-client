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

TEST_CASE("integration: columnar database management", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().is_columnar()) {
    SKIP("Requires a columnar cluster");
  }

  auto agent = couchbase::core::columnar::agent(integration.io, { { integration.cluster } });

  SECTION("Creating database")
  {
    const couchbase::core::columnar::create_database_options options{ "test-database" };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_create(options, [barrier](auto err) {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE_SUCCESS(err.ec);
  }

  SECTION("Creating database that already exists")
  {
    const couchbase::core::columnar::create_database_options options{ "test-database" };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_create(options, [barrier](auto err) mutable {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE(err.ec == couchbase::core::columnar::errc::generic);
  }

  SECTION("Create database with ignore if exists")
  {
    const couchbase::core::columnar::create_database_options options{ "test-database", true };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_create(options, [barrier](auto err) mutable {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE_SUCCESS(err.ec);
  }

  SECTION("Fetch all databases")
  {
    const couchbase::core::columnar::fetch_all_databases_options options{};

    auto barrier = std::make_shared<
      std::promise<std::pair<std::vector<couchbase::core::columnar::database_metadata>,
                             couchbase::core::columnar::error>>>();
    auto f = barrier->get_future();
    auto op = agent.database_fetch_all(options, [barrier](auto databases, auto err) mutable {
      barrier->set_value({ std::move(databases), std::move(err) });
    });
    auto [databases, err] = f.get();

    REQUIRE_SUCCESS(err.ec);

    bool found{ false };
    for (const auto& db : databases) {
      if (db.name == "test-database") {
        found = true;
        REQUIRE(!db.is_system_database);
      }
    }
    REQUIRE(found);
  }

  SECTION("Drop database")
  {
    const couchbase::core::columnar::drop_database_options options{ "test-database" };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_drop(options, [barrier](auto err) mutable {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE_SUCCESS(err.ec);
  }

  SECTION("Drop database that does not exist")
  {
    const couchbase::core::columnar::drop_database_options options{ "test-database" };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_drop(options, [barrier](auto err) mutable {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE(err.ec == couchbase::core::columnar::errc::generic);
  }

  SECTION("Drop database with ignore if not exists")
  {
    const couchbase::core::columnar::drop_database_options options{ "test-database", true };

    auto barrier = std::make_shared<std::promise<couchbase::core::columnar::error>>();
    auto f = barrier->get_future();
    auto op = agent.database_drop(options, [barrier](auto err) mutable {
      barrier->set_value(std::move(err));
    });

    REQUIRE(op.has_value());
    auto err = f.get();
    REQUIRE_SUCCESS(err.ec);
  }
}
