/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include <core/utils/duration_parser.hxx>
#include <couchbase/cluster_options.hxx>
#include <couchbase/codec/transcoder_traits.hxx>

#include <docopt.h>
#include <fmt/chrono.h>

#include <string>
#include <vector>

namespace cbc
{
auto
parse_options(const std::string& doc, const std::vector<std::string>& argv, bool options_first = false) -> docopt::Options;

void
fill_cluster_options(const docopt::Options& options, couchbase::cluster_options& cluster_options, std::string& connection_string);

auto
default_cluster_options() -> const couchbase::cluster_options&;

auto
default_connection_string() -> const std::string&;

auto
usage_block_for_cluster_options() -> std::string;

auto
default_log_level() -> const std::string&;

auto
usage_block_for_logger() -> std::string;

void
apply_logger_options(const docopt::Options& options);

struct keyspace_with_id {
    std::string bucket_name;
    std::string scope_name;
    std::string collection_name;
    std::string id;
};

auto
extract_inlined_keyspace(const std::string& id) -> std::optional<keyspace_with_id>;

struct passthrough_transcoder {
    using document_type = std::pair<std::vector<std::byte>, std::uint32_t>;

    static auto decode(const couchbase::codec::encoded_value& encoded) -> document_type
    {
        return { encoded.data, encoded.flags };
    }
};

[[nodiscard]] bool
get_bool_option(const docopt::Options& options, const std::string& name);

[[nodiscard]] double
get_double_option(const docopt::Options& options, const std::string& name);
} // namespace cbc

template<>
struct couchbase::codec::is_transcoder<cbc::passthrough_transcoder> : public std::true_type {
};

#define parse_duration_option(setter, option_name)                                                                                         \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            auto value = options.at(option_name).asString();                                                                               \
            try {                                                                                                                          \
                setter(std::chrono::duration_cast<std::chrono::milliseconds>(couchbase::core::utils::parse_duration(value)));              \
            } catch (const couchbase::core::utils::duration_parse_error&) {                                                                \
                try {                                                                                                                      \
                    setter(std::chrono::milliseconds(std::stoull(value, nullptr, 10)));                                                    \
                } catch (const std::invalid_argument&) {                                                                                   \
                    throw docopt::DocoptArgumentError(                                                                                     \
                      fmt::format("cannot parse '{}' as duration in " option_name ": not a number", value));                               \
                } catch (const std::out_of_range&) {                                                                                       \
                    throw docopt::DocoptArgumentError(                                                                                     \
                      fmt::format("cannot parse '{}' as duration in " option_name ": out of range", value));                               \
                }                                                                                                                          \
            }                                                                                                                              \
        }                                                                                                                                  \
    } while (0)

#define parse_string_option(setter, option_name)                                                                                           \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            setter(options.at(option_name).asString());                                                                                    \
        }                                                                                                                                  \
    } while (0)

#define parse_disable_option(setter, option_name)                                                                                          \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            setter(!options.at(option_name).asBool());                                                                                     \
        }                                                                                                                                  \
    } while (0)

#define parse_enable_option(setter, option_name)                                                                                           \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            setter(options.at(option_name).asBool());                                                                                      \
        }                                                                                                                                  \
    } while (0)

#define parse_integer_option(setter, option_name)                                                                                          \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            auto value = options.at(option_name).asString();                                                                               \
            try {                                                                                                                          \
                setter(std::stoull(value, nullptr, 10));                                                                                   \
            } catch (const std::invalid_argument&) {                                                                                       \
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as integer in " option_name ": not a number", value));    \
            } catch (const std::out_of_range&) {                                                                                           \
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as integer in " option_name ": out of range", value));    \
            }                                                                                                                              \
        }                                                                                                                                  \
    } while (0)

#define parse_float_option(setter, option_name)                                                                                            \
    do {                                                                                                                                   \
        if (options.find(option_name) != options.end() && options.at(option_name)) {                                                       \
            auto value = options.at(option_name).asString();                                                                               \
            try {                                                                                                                          \
                setter(std::stod(value, nullptr));                                                                                         \
            } catch (const std::invalid_argument&) {                                                                                       \
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as float in " option_name ": not a number", value));      \
            } catch (const std::out_of_range&) {                                                                                           \
                throw docopt::DocoptArgumentError(fmt::format("cannot parse '{}' as float in " option_name ": out of range", value));      \
            }                                                                                                                              \
        }                                                                                                                                  \
    } while (0)
