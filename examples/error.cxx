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

static constexpr auto connection_string{ "couchbase://192.168.106.130" };
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

        // Classic key_value_error_context returned
        auto [ctx, resp] = collection.upsert(document_id, basic_doc, {}).get();
        std::cout << "ec: " << ctx.ec().message() << ", id: " << document_id << ", CAS: " << resp.cas().value() << "\n";

        // new couchbase::error returned with document_not_found
        auto [err1, doc_not_found_resp] = collection.get_with_error("does-not-exist").get();
        if (err1) { // Checks for err1.ec() != 0;
            std::cout << err1.ec().message() << "\n"<< err1.ctx().to_json_pretty() << "\n" << err1.ctx().to_json() << "\n";
        }

        // shouldn't error
        auto [err2, get_resp] = collection.get_with_error(document_id).get();
        std::cout << "ec: " << err2.message() << ", id: " << document_id << ", CAS: " << get_resp.cas().value() << "\n";

        // Errors with bucket not found
        auto err3 = cluster.query_indexes().create_primary_index_with_error("does-not-exist", {}).get();
        std::cout << err3.message() << "\n"<< err3.ctx().to_json_pretty() << "\n" << err3.ctx().to_json() << "\n";

        // shouldn't error
        auto err4 = cluster.query_indexes().create_primary_index_with_error(bucket_name, {}).get();
        std::cout << err4.message() << "\n"<< err4.ctx().to_json_pretty() << "\n" << err4.ctx().to_json() << "\n";

        // shouldn't error
        auto [err6, query_resp2] = cluster.query_with_error(fmt::format("SELECT * FROM {}", bucket_name), {}).get();
        std::cout << err6.message() << "\n"<< err6.ctx().to_json_pretty() << "\n" << err6.ctx().to_json() << "\n";
    }

    cluster.close();
    guard.reset();

    io_thread.join();

    return 0;
}
