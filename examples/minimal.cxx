/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include "core/logger/logger.hxx"

#include <couchbase/cluster.hxx>

#include <asio.hpp>

#include <iostream>
#include <system_error>

static constexpr auto connection_string{ "couchbase://127.0.0.1" };
static constexpr auto username{ "Administrator" };
static constexpr auto password{ "password" };
static constexpr auto bucket_name{ "default" };
static constexpr auto scope_name{ couchbase::scope::default_name };
static constexpr auto collection_name{ couchbase::collection::default_name };

int
main()
{
    couchbase::core::logger::create_console_logger();
    couchbase::core::logger::set_log_levels(couchbase::core::logger::level::trace);

    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    options.apply_profile("wan_development");
    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    if (ec) {
        std::cout << "Unable to connect to the cluster. ec: " << ec.message() << "\n";
    } else {
        auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

        const std::string document_id{ "minimal_example" };
        const tao::json::value basic_doc{
            { "a", 1.0 },
            { "b", 2.0 },
        };

        auto [ctx, resp] = collection.upsert(document_id, basic_doc, {}).get();
        std::cout << "ec: " << ctx.ec().message() << ", id: " << document_id << ", CAS: " << resp.cas().value() << "\n";
    }

    cluster.close();
    guard.reset();

    io_thread.join();

    return 0;
}
