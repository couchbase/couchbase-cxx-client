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

#include <couchbase/document_id.hxx>
#include <couchbase/io/mcbp_message.hxx>
#include <couchbase/mutation_token.hxx>
#include <couchbase/protocol/client_opcode.hxx>
#include <couchbase/protocol/cmd_info.hxx>
#include <couchbase/protocol/durability_level.hxx>
#include <couchbase/protocol/status.hxx>

namespace couchbase::protocol
{

class replace_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::replace;

  private:
    mutation_token token_;

  public:
    mutation_token& token()
    {
        return token_;
    }

    [[nodiscard]] const mutation_token& token() const
    {
        return token_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info& info);
};

class replace_request_body
{
  public:
    using response_body_type = replace_response_body;
    static const inline client_opcode opcode = client_opcode::replace;

  private:
    std::string key_{};
    std::vector<std::uint8_t> extras_{};
    std::vector<std::uint8_t> content_{};
    std::uint32_t flags_{};
    std::uint32_t expiry_{};
    std::vector<std::uint8_t> framing_extras_{};

  public:
    void id(const document_id& id);

    void durability(protocol::durability_level level, std::optional<std::uint16_t> timeout);

    void preserve_expiry();

    void content(const std::string_view& content)
    {
        content_ = { content.begin(), content.end() };
    }

    void flags(uint32_t flags)
    {
        flags_ = flags;
    }

    void expiry(uint32_t value)
    {
        expiry_ = value;
    }

    [[nodiscard]] const std::string& key() const
    {
        return key_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& framing_extras() const
    {
        return framing_extras_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& extras()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        return extras_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& value() const
    {
        return content_;
    }

    [[nodiscard]] std::size_t size()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        return framing_extras_.size() + extras_.size() + key_.size() + content_.size();
    }

  private:
    void fill_extras();
};

} // namespace couchbase::protocol
