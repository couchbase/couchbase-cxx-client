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
#include "core/document_id.hxx"
#include "core/io/mcbp_message.hxx"
#include "status.hxx"

#include <gsl/assert>

namespace couchbase::core::protocol
{

class lookup_in_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::subdoc_multi_lookup;

    struct lookup_in_field {
        key_value_status_code status{};
        std::string value;
    };

  private:
    std::vector<lookup_in_field> fields_;

  public:
    [[nodiscard]] const std::vector<lookup_in_field>& fields() const
    {
        return fields_;
    }

    bool parse(key_value_status_code status,
               const header_buffer& header,
               std::uint8_t framing_extras_size,
               std::uint16_t key_size,
               std::uint8_t extras_size,
               const std::vector<std::byte>& body,
               const cmd_info& info);
};

class lookup_in_request_body
{
  public:
    using response_body_type = lookup_in_response_body;
    static const inline client_opcode opcode = client_opcode::subdoc_multi_lookup;

    /**
     * Allow access to XATTRs for deleted documents (instead of returning KEY_ENOENT).
     */
    static const inline std::uint8_t doc_flag_access_deleted = 0b0000'0100;

    struct lookup_in_specs {
        /**
         * If set, the path refers to an Extended Attribute (XATTR).
         * If clear, the path refers to a path inside the document body.
         */
        static const inline std::uint8_t path_flag_xattr = 0b0000'0100;

        struct entry {
            std::uint8_t opcode;
            std::uint8_t flags;
            std::string path;
            std::size_t original_index{};
        };
        std::vector<entry> entries;

        void add_spec(subdoc_opcode operation, bool xattr, const std::string& path)
        {
            add_spec(static_cast<std::uint8_t>(operation), xattr ? path_flag_xattr : 0, path);
        }

        void add_spec(std::uint8_t operation, std::uint8_t flags, const std::string& path)
        {
            Expects(is_valid_subdoc_opcode(operation));
            entries.emplace_back(entry{ operation, flags, path });
        }
    };

  private:
    std::vector<std::byte> key_;
    std::vector<std::byte> extras_{};
    std::vector<std::byte> value_{};

    std::uint8_t flags_{ 0 };
    lookup_in_specs specs_;

  public:
    void id(const document_id& id);

    void access_deleted(bool value)
    {
        if (value) {
            flags_ = doc_flag_access_deleted;
        } else {
            flags_ = 0;
        }
    }

    void specs(const lookup_in_specs& specs)
    {
        specs_ = specs;
    }

    [[nodiscard]] const auto& key() const
    {
        return key_;
    }

    [[nodiscard]] const auto& framing_extras() const
    {
        return empty_buffer;
    }

    [[nodiscard]] const auto& extras()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        return extras_;
    }

    [[nodiscard]] const auto& value()
    {
        if (value_.empty()) {
            fill_value();
        }
        return value_;
    }

    [[nodiscard]] std::size_t size()
    {
        if (extras_.empty()) {
            fill_extras();
        }
        if (value_.empty()) {
            fill_value();
        }
        return key_.size() + extras_.size() + value_.size();
    }

  private:
    void fill_extras();

    void fill_value();
};

} // namespace couchbase::core::protocol
