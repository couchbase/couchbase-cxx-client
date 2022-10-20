/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "core/transactions/uid_generator.hxx"

#include "core/cluster.hxx"
#include "core/operations.hxx"
#include "core/operations/management/bucket.hxx"

#include <couchbase/transactions/transaction_keyspace.hxx>

#include "core/transactions.hxx"
#include "core/transactions/internal/utils.hxx"
#include "core/transactions/result.hxx"

#include <tao/json.hpp>

#include <spdlog/details/os.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>

// hack, until I get gtest working a bit better and can execute
// tests through make with proper working directory.
#define CONFIG_FILE_NAME "../tests/config.json"

static const uint32_t DEFAULT_IO_COMPLETION_THREADS = 4;
static const size_t MAX_PINGS = 10;
static const auto PING_INTERVAL = std::chrono::milliseconds(100);

namespace tx = couchbase::core::transactions;

struct test_config {
    std::string connection_string{ "couchbase://127.0.0.1" };
    std::string username{ "Administrator" };
    std::string password{ "password" };
    std::size_t io_threads{ 4U };
    std::string bucket{ "default" };
    std::string extra_bucket{ "secBucket" };
};

template<>
struct tao::json::traits<test_config> {
    template<template<typename...> class Traits>
    static void to(const tao::json::basic_value<Traits>& v, test_config& config)
    {
        if (const auto& val = v.template optional<std::string>("connection_string"); val) {
            config.connection_string = val.value();
        }
        if (const auto& val = v.template optional<std::string>("username"); val) {
            config.username = val.value();
        }
        if (const auto& val = v.template optional<std::string>("password"); val) {
            config.password = val.value();
        }
        if (const auto& val = v.template optional<std::size_t>("io_threads"); val) {
            config.io_threads = val.value();
        }
        if (const auto& val = v.template optional<std::string>("bucket"); val) {
            config.bucket = val.value();
        }
        if (const auto& val = v.template optional<std::string>("extra_bucket"); val) {
            config.extra_bucket = val.value();
        }
    }
};

struct conn {
    asio::io_context io;
    std::list<std::thread> io_threads;
    std::shared_ptr<couchbase::core::cluster> c;

    conn(const test_config& conf)
      : io(static_cast<int>(conf.io_threads))
      , c(couchbase::core::cluster::create(io))
    {
        // for tests, really chatty logs may be useful.
        if (!couchbase::core::logger::is_initialized()) {
            couchbase::core::logger::create_console_logger();
        }
        couchbase::core::logger::set_log_levels(couchbase::core::logger::level::trace);
        tx::set_transactions_log_level(couchbase::core::logger::level::trace);
        tx::txn_log->trace("using {} io completion threads", conf.io_threads);
        for (std::size_t i = 0; i < conf.io_threads; i++) {
            io_threads.emplace_back([this]() { io.run(); });
        }
        connect(conf);
    }

    ~conn()
    {
        // close connection
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        c->close([barrier]() { barrier->set_value(); });
        f.get();
        io.stop();
        for (auto& t : io_threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    void connect(const test_config& conf)
    {
        couchbase::core::cluster_credentials auth{};
        {
            if (auto env_val = spdlog::details::os::getenv("TEST_LOG_LEVEL"); !env_val.empty()) {
                couchbase::core::logger::set_log_levels(couchbase::core::logger::level_from_str(env_val));
            }
            auto connstr = couchbase::core::utils::parse_connection_string(conf.connection_string);
            auth.username = conf.username;
            auth.password = conf.password;
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            couchbase::core::origin orig(auth, connstr);
            orig.options().transactions.expiration_time = std::chrono::seconds(1);
            c->open(orig, [barrier](std::error_code ec) { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening cluster: " << rc.message() << std::endl;
                exit(-1);
            }
            spdlog::trace("successfully opened connection to {}", connstr.bootstrap_nodes.front().address);
        }
        // now, open the `default` bucket
        {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            c->open_bucket(conf.bucket, [barrier](std::error_code ec) { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening bucket `" << conf.bucket << "`: " << rc.message() << std::endl;
                exit(-1);
            }
        }
        // now open the extra bucket
        {
            if (!conf.extra_bucket.empty()) {
                auto barrier = std::make_shared<std::promise<std::error_code>>();

                auto f = barrier->get_future();
                c->open_bucket(conf.extra_bucket, [barrier](std::error_code ec) { barrier->set_value(ec); });
                auto rc = f.get();
                if (rc) {
                    std::cout << "ERROR opening extra bucket `" << conf.extra_bucket << "`: " << rc.message() << std::endl;
                    exit(-1);
                }
            }
        }

        // do a ping
        {
            bool ok = false;
            size_t num_pings = 0;
            auto sleep_time = PING_INTERVAL;
            // TEMPORARILY: because of CCXCBC-94, we can only sleep for some arbitrary time before pinging,
            // in hopes that query is up by then.
            spdlog::info("sleeping for 10 seconds before pinging (CXXCBC-94 workaround/hack)");
            std::this_thread::sleep_for(std::chrono::seconds(10));
            while (!ok && num_pings++ < MAX_PINGS) {
                spdlog::info("sleeping {}ms before pinging...", sleep_time.count());
                std::this_thread::sleep_for(sleep_time);
                // TEMPORARILY only ping key_value.   See CCXCBC-94 for details -- ping not returning any service
                // except KV.
                std::set<couchbase::core::service_type> services{ couchbase::core::service_type::key_value };
                auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
                auto f = barrier->get_future();
                c->ping("tests_startup", "default", services, [barrier](couchbase::core::diag::ping_result result) {
                    barrier->set_value(result);
                });
                auto result = f.get();
                ok = true;
                for (auto& svc : services) {
                    if (result.services.find(svc) != result.services.end()) {
                        if (result.services[svc].size() > 0) {
                            ok = ok && std::all_of(result.services[svc].begin(),
                                                   result.services[svc].end(),
                                                   [&](const couchbase::core::diag::endpoint_ping_info& info) {
                                                       return (!info.error && info.state == couchbase::core::diag::ping_state::ok);
                                                   });
                        } else {
                            ok = false;
                        }
                    } else {
                        ok = false;
                    }
                }
                spdlog::info("ping after connect {}", ok ? "successful" : "unsuccessful");
            }
            if (!ok) {
                exit(-1);
            }
        }
    }
};

struct passthrough_transcoder {
    using document_type = couchbase::codec::encoded_value;

    static auto decode(const couchbase::codec::encoded_value& data) -> document_type
    {
        return data;
    }
};

template<>
struct couchbase::codec::is_transcoder<passthrough_transcoder> : public std::true_type {
};

class TransactionsTestEnvironment
{
  public:
    static bool supports_query()
    {
        return spdlog::details::os::getenv("SUPPORTS_QUERY").empty();
    }

    static const test_config& get_conf()
    {
        static test_config global_config{};
        static std::once_flag default_config_initialized;

        std::call_once(default_config_initialized, []() {
            if (std::ifstream in(CONFIG_FILE_NAME, std::ifstream::in); in.is_open()) {
                spdlog::info("reading config file {}", CONFIG_FILE_NAME);
                tao::json::from_stream(in, CONFIG_FILE_NAME).to(global_config);
            }
            if (auto var = spdlog::details::os::getenv("TEST_CONNECTION_STRING"); !var.empty()) {
                global_config.connection_string = var;
            }
            if (auto var = spdlog::details::os::getenv("TEST_USERNAME"); !var.empty()) {
                global_config.username = var;
            }
            if (auto var = spdlog::details::os::getenv("TEST_PASSWORD"); !var.empty()) {
                global_config.password = var;
            }
            spdlog::info(R"(connection_string: "{}", username: "{}", bucket: "{}", extra_bucket: "{}", io_threads: {})",
                         global_config.connection_string,
                         global_config.username,
                         global_config.bucket,
                         global_config.extra_bucket,
                         global_config.io_threads);
        });

        return global_config;
    }

    template<typename Content>
    static bool upsert_doc(const couchbase::core::document_id& id, const Content& content)
    {
        auto c = couchbase::cluster(get_cluster()).bucket(id.bucket()).scope(id.scope()).collection(id.collection());

        auto [ctx, resp] = c.upsert(id.key(), content, {}).get();
        if (ctx.ec()) {
            spdlog::error("upsert doc failed with {}", ctx.ec().message());
        }
        return !ctx.ec();
    }

    template<typename Content>
    static bool insert_doc(const couchbase::core::document_id& id, const Content& content)
    {
        auto c = couchbase::cluster(get_cluster()).bucket(id.bucket()).scope(id.scope()).collection(id.collection());

        auto [ctx, resp] = c.insert(id.key(), content, {}).get();
        if (ctx.ec()) {
            spdlog::error("insert doc failed with {}", ctx.ec().message());
        }
        return !ctx.ec();
    }

    static tx::result get_doc(const couchbase::core::document_id& id)
    {
        auto c = couchbase::cluster(get_cluster()).bucket(id.bucket()).scope(id.scope()).collection(id.collection());

        auto barrier = std::make_shared<std::promise<tx::result>>();
        auto f = barrier->get_future();
        c.get(id.key(), {}, [barrier](auto ctx, couchbase::get_result resp) {
            tx::result res{};
            res.ec = ctx.ec();
            res.key = ctx.id();
            res.cas = resp.cas().value();
            auto encoded = resp.template content_as<passthrough_transcoder>();
            res.flags = encoded.flags;
            res.raw_value = encoded.data;
            barrier->set_value(res);
        });
        return tx::wrap_operation_future(f);
    }

    static std::shared_ptr<couchbase::core::cluster> get_cluster()
    {
        static conn connection(get_conf());
        return connection.c;
    }

    static couchbase::core::document_id get_document_id(const std::string& id = {})
    {
        std::string key = (id.empty() ? tx::uid_generator::next() : id);
        return { get_conf().bucket, couchbase::scope::default_name, couchbase::collection::default_name, key };
    }

    static tx::transactions get_transactions(std::shared_ptr<couchbase::core::cluster> c = get_cluster(),
                                             bool cleanup_client_attempts = true,
                                             bool cleanup_lost_txns = true)
    {
        couchbase::transactions::transactions_config cfg;
        cfg.cleanup_config().cleanup_client_attempts(cleanup_client_attempts);
        cfg.cleanup_config().cleanup_lost_attempts(cleanup_lost_txns);
        cfg.expiration_time(std::chrono::seconds(5));
        return { c, cfg };
    }
};
