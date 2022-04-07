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

#include <couchbase/cas.hxx>
#include <couchbase/document_id.hxx>
#include <couchbase/io/mcbp_message.hxx>
#include <couchbase/protocol/client_opcode.hxx>
#include <couchbase/protocol/cmd_info.hxx>
#include <couchbase/protocol/status.hxx>

namespace couchbase::protocol
{

class get_meta_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::get_meta;

  private:
    std::uint32_t deleted_;
    std::uint32_t flags_;
    std::uint32_t expiry_;
    std::uint64_t sequence_number_;
    std::uint8_t datatype_;

  public:
    [[nodiscard]] bool is_deleted() const
    {
        return deleted_ != 0;
    }

    [[nodiscard]] std::uint32_t flags() const
    {
        return flags_;
    }

    [[nodiscard]] std::uint32_t expiry() const
    {
        return expiry_;
    }

    [[nodiscard]] std::uint64_t sequence_number() const
    {
        return sequence_number_;
    }

    [[nodiscard]] std::uint8_t datatype() const
    {
        return datatype_;
    }

    bool parse(protocol::status status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<uint8_t>& body,
               const cmd_info& info);
};

class get_meta_request_body
{
  public:
    using response_body_type = get_meta_response_body;
    static const inline client_opcode opcode = client_opcode::get_meta;

  private:
    std::string key_;
    std::vector<std::uint8_t> extras_{ 0x02 /* format version, supported since Couchbase Server 5.0, includes datatype into response */ };

  public:
    void id(const document_id& id);

    [[nodiscard]] const std::string& key() const
    {
        return key_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& framing_extras() const
    {
        return empty_buffer;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& extras() const
    {
        return extras_;
    }

    [[nodiscard]] const std::vector<std::uint8_t>& value()
    {
        return empty_buffer;
    }

    [[nodiscard]] std::size_t size()
    {
        return extras_.size() + key_.size();
    }

  private:
    void fill_body();
};

} // namespace couchbase::protocol
