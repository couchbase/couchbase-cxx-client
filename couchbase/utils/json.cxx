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

#include <couchbase/utils/json.hxx>

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

namespace couchbase::utils::json
{
/**
 *
 * This transformer is necessary to handle invalid JSON sent by the server.
 *
 * 1) For some reason "projector" field gets duplicated in the configuration JSON
 *
 * 2) CXXCBC-13, ns_server sends response to list buckets request with duplicated keys.
 */
template<typename Consumer>
struct last_key_wins : Consumer {
    using Consumer::Consumer;

    using Consumer::keys_;
    using Consumer::stack_;
    using Consumer::value;

    void member()
    {
        Consumer::stack_.back().prepare_object()[Consumer::keys_.back()] = std::move(Consumer::value);
        Consumer::keys_.pop_back();
    }
};

tao::json::value
parse(const std::string& input)
{
    return tao::json::from_string<utils::json::last_key_wins>(input);
}

tao::json::value
parse(const json_string& input)
{
    return parse(input.str());
}

tao::json::value
parse(const char* input, std::size_t size)
{
    return tao::json::from_string<utils::json::last_key_wins>(input, size);
}

std::string
generate(const tao::json::value& object)
{
    return tao::json::to_string(object);
}
} // namespace couchbase::utils::json
