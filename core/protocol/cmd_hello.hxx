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

#include "client_opcode.hxx"
#include "cmd_info.hxx"
#include "core/io/mcbp_message.hxx"
#include "hello_feature.hxx"
#include "status.hxx"

namespace couchbase::core::protocol
{

class hello_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::hello;

  private:
    std::vector<hello_feature> supported_features_;

  public:
    [[nodiscard]] const std::vector<hello_feature>& supported_features() const
    {
        return supported_features_;
    }

    bool parse(key_value_status_code status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<std::byte>& body,
               const cmd_info& info);
};

class hello_request_body
{
  public:
    using response_body_type = hello_response_body;
    static const inline client_opcode opcode = client_opcode::hello;

  private:
    std::vector<std::byte> key_;
    std::vector<hello_feature> features_{
        hello_feature::tcp_nodelay,
        hello_feature::xattr,
        hello_feature::xerror,
        hello_feature::select_bucket,
        hello_feature::json,
        hello_feature::duplex,
        hello_feature::alt_request_support,
        hello_feature::tracing,
        hello_feature::sync_replication,
        hello_feature::vattr,
        hello_feature::collections,
        hello_feature::subdoc_create_as_deleted,
        hello_feature::preserve_ttl,
    };
    std::vector<std::byte> value_;

  public:
    void user_agent(std::string_view val);

    void enable_unordered_execution()
    {
        features_.emplace_back(hello_feature::unordered_execution);
    }

    void enable_clustermap_change_notification()
    {
        features_.emplace_back(hello_feature::clustermap_change_notification);
    }

    void enable_compression()
    {
        features_.emplace_back(hello_feature::snappy);
    }

    void enable_mutation_tokens()
    {
        features_.emplace_back(hello_feature::mutation_seqno);
    }

    [[nodiscard]] const std::vector<hello_feature>& features() const
    {
        return features_;
    }

    [[nodiscard]] const auto& key() const
    {
        return key_;
    }

    [[nodiscard]] const auto& framing_extras() const
    {
        return empty_buffer;
    }

    [[nodiscard]] const auto& extras() const
    {
        return empty_buffer;
    }

    [[nodiscard]] const auto& value()
    {
        if (value_.empty()) {
            fill_body();
        }
        return value_;
    }

    [[nodiscard]] std::size_t size()
    {
        if (value_.empty()) {
            fill_body();
        }
        return key_.size() + value_.size();
    }

  private:
    void fill_body();
};

} // namespace couchbase::core::protocol
