/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "integration_test_guard.hxx"

#include "core/operations/management/freeform.hxx"
#include "core/utils/json.hxx"
#include "logger.hxx"

namespace test::utils
{
static void
set_thread_name(const char* name)
{
#if defined(__APPLE__)
    ::pthread_setname_np(name);
#elif defined(__FreeBSD__)
    ::pthread_set_name_np(::pthread_self(), name);
#elif defined(__linux__)
    ::pthread_setname_np(::pthread_self(), name);
#elif defined(__NetBSD__)
    ::pthread_setname_np(::pthread_self(), "%s", name);
#else
    (void)name;
#endif
}

static auto
spawn_io_threads(asio::io_context& io, std::size_t number_of_threads) -> std::vector<std::thread>
{
    std::vector<std::thread> threads;
    threads.reserve(number_of_threads);
    for (std::size_t i = 0; i < number_of_threads; i++) {
        threads.emplace_back([&io, i]() mutable {
            set_thread_name(fmt::format("cxx_io_{}", i).c_str());
            io.run();
        });
    }
    return threads;
}

static auto
build_origin(const test_context& ctx,
             const couchbase::core::cluster_credentials& auth,
             const couchbase::core::utils::connection_string& connstr) -> couchbase::core::origin
{
    couchbase::core::origin origin(auth, connstr);
    origin.options().dns_config = couchbase::core::io::dns::dns_config{
        ctx.dns_nameserver.value_or(couchbase::core::io::dns::dns_config::default_nameserver),
        ctx.dns_port.value_or(couchbase::core::io::dns::dns_config::default_port),
    };
    if (ctx.deployment == deployment_type::capella || ctx.deployment == test::utils::deployment_type::elixir) {
        origin.options().apply_profile("wan_development");
    }
    return origin;
}

integration_test_guard::integration_test_guard()
  : ctx(test_context::load_from_environment())
  , io(static_cast<int>(ctx.number_of_io_threads))
  , cluster(couchbase::core::cluster::create(io))
{
    init_logger();
    auto connstr = couchbase::core::utils::parse_connection_string(ctx.connection_string);
    couchbase::core::cluster_credentials auth{};
    if (!ctx.certificate_path.empty()) {
        auth.certificate_path = ctx.certificate_path;
        auth.key_path = ctx.key_path;
    } else {
        auth.username = ctx.username;
        auth.password = ctx.password;
    }
    io_threads = spawn_io_threads(io, ctx.number_of_io_threads);
    origin = build_origin(ctx, auth, connstr);
    open_cluster(cluster, origin);
}

integration_test_guard::integration_test_guard(const couchbase::core::cluster_options& opts)
  : ctx(test_context::load_from_environment())
  , io(static_cast<int>(ctx.number_of_io_threads))
  , cluster(couchbase::core::cluster::create(io))
{
    init_logger();
    auto auth = ctx.build_auth();
    auto connstr = couchbase::core::utils::parse_connection_string(ctx.connection_string);
    // for now, lets _only_ add a tracer or meter from the incoming options
    connstr.options.meter = opts.meter;
    connstr.options.tracer = opts.tracer;
    connstr.options.enable_mutation_tokens = opts.enable_mutation_tokens;
    origin = build_origin(ctx, auth, connstr);
    io_threads = spawn_io_threads(io, ctx.number_of_io_threads);
    open_cluster(cluster, origin);
}

integration_test_guard::~integration_test_guard()
{
    close_cluster(cluster);
    io.stop();
    for (auto& thread : io_threads) {
        thread.join();
    }
}

const couchbase::core::operations::management::bucket_describe_response::bucket_info&
integration_test_guard::load_bucket_info(const std::string& bucket_name, bool refresh)
{
    if (info.count(bucket_name) > 0 && !refresh) {
        return info[bucket_name];
    }

    auto resp = execute(cluster, couchbase::core::operations::management::bucket_describe_request{ bucket_name });
    if (resp.ctx.ec == couchbase::errc::common::service_not_available) {
        open_bucket(cluster, ctx.bucket);
        resp = execute(cluster, couchbase::core::operations::management::bucket_describe_request{ bucket_name });
    }
    if (resp.ctx.ec) {
        CB_LOG_CRITICAL("unable to load info for bucket \"{}\": {}", bucket_name, resp.ctx.ec.message());
        throw std::system_error(resp.ctx.ec);
    }

    info[bucket_name] = resp.info;
    return info[bucket_name];
}

const couchbase::core::operations::management::cluster_describe_response::cluster_info&
integration_test_guard::load_cluster_info(bool refresh)
{
    if (cluster_info && !refresh) {
        return cluster_info.value();
    }

    auto resp = execute(cluster, couchbase::core::operations::management::cluster_describe_request{});
    if (resp.ctx.ec == couchbase::errc::common::service_not_available) {
        open_bucket(cluster, ctx.bucket);
        resp = execute(cluster, couchbase::core::operations::management::cluster_describe_request{});
    }
    if (resp.ctx.ec) {
        CB_LOG_CRITICAL("unable to load info for cluster: {}", resp.ctx.ec.message());
        throw std::system_error(resp.ctx.ec);
    }

    cluster_info.emplace(std::move(resp.info));

    return cluster_info.value();
}

pools_response
integration_test_guard::load_pools_info(bool refresh)
{
    if (pools_info && !refresh) {
        return pools_info.value();
    }

    couchbase::core::operations::management::freeform_request req{};
    req.type = couchbase::core::service_type::management;
    req.method = "GET";
    req.path = "/pools";
    auto resp = execute(cluster, req);
    if (resp.ctx.ec || resp.status != 200) {
        CB_LOG_CRITICAL("unable to load pools info for cluster: {}", resp.ctx.ec.message());
        throw std::system_error(resp.ctx.ec);
    }

    auto result = couchbase::core::utils::json::parse(resp.body);

    pools_response p_response{};
    if (auto* is_developer_preview = result.find("isDeveloperPreview");
        is_developer_preview != nullptr && is_developer_preview->is_boolean()) {
        p_response.is_developer_preview = is_developer_preview->get_boolean();
    }
    if (auto* config_profile = result.find("configProfile"); config_profile != nullptr && config_profile->is_string()) {
        auto& config_profile_string = config_profile->get_string();
        if (config_profile_string == "serverless") {
            p_response.config_profile = server_config_profile::serverless;
        }
    }

    return p_response;
}

server_version
integration_test_guard::cluster_version()
{
    load_cluster_info();
    auto runtime_pools_info = load_pools_info();
    std::string runtime_version{};
    for (const auto& node : cluster_info->nodes) {
        if (runtime_version.empty()) {
            runtime_version = node.version;
        } else if (runtime_version != node.version) {
            /* mixed version cluster, ignore it and fall back to version from test context */
            runtime_version.clear();
            break;
        }
    }
    if (runtime_version.empty()) {
        return ctx.version;
    }
    auto parsed_version = server_version::parse(runtime_version, ctx.deployment);
    parsed_version.profile = runtime_pools_info.config_profile;
    parsed_version.developer_preview = runtime_pools_info.is_developer_preview;
    parsed_version.use_gocaves = ctx.version.use_gocaves;
    if (parsed_version.major == 0) {
        /* the build does not specify version properly */
        return ctx.version;
    }
    return parsed_version;
}

} // namespace test::utils
