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

#pragma once

#include <couchbase/cluster.hxx>
#include <couchbase/execute.hxx>

namespace test::utils
{
// all requests that don't define 'encoded_request_type' member type, considered "high-level" and submitted using couchbase::execute()
template<class, class = void>
struct is_low_level_request : std::false_type {
};

// all requests setup more boilerplate, including 'encoded_request_type' member type
template<class T>
struct is_low_level_request<T, std::void_t<typename T::encoded_request_type>> : std::true_type {
};

template<typename T>
inline constexpr bool is_low_level_request_v = is_low_level_request<T>::value;

template<class Request>
auto
execute(std::shared_ptr<couchbase::cluster> cluster, Request request)
{
    if constexpr (is_low_level_request_v<Request>) {
        using response_type = typename Request::response_type;
        auto barrier = std::make_shared<std::promise<response_type>>();
        auto f = barrier->get_future();
        cluster->execute(request, [barrier](response_type resp) { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        return resp;
    } else {
        using context_type = typename Request::element_type::context_type;
        using response_type = typename Request::element_type::response_type;
        auto barrier = std::make_shared<std::promise<std::pair<context_type, response_type>>>();
        auto f = barrier->get_future();
        couchbase::execute(cluster, request, [barrier](context_type ctx, response_type resp) {
            barrier->set_value(std::make_pair(std::move(ctx), std::move(resp)));
        });
        auto resp = f.get();
        return resp;
    }
}

void
open_cluster(std::shared_ptr<couchbase::cluster> cluster, const couchbase::origin& origin);

void
close_cluster(std::shared_ptr<couchbase::cluster> cluster);

void
open_bucket(std::shared_ptr<couchbase::cluster> cluster, const std::string& bucket_name);

void
close_bucket(std::shared_ptr<couchbase::cluster> cluster, const std::string& bucket_name);
} // namespace test::utils