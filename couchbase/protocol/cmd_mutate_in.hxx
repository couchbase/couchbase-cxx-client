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

#include <gsl/assert>

namespace couchbase::protocol
{

class mutate_in_response_body
{
  public:
    static const inline client_opcode opcode = client_opcode::subdoc_multi_mutation;

    struct mutate_in_field {
        std::uint8_t index{};
        protocol::status status{};
        std::string value{};
    };

  private:
    std::vector<mutate_in_field> fields_;
    mutation_token token_;

  public:
    [[nodiscard]] const std::vector<mutate_in_field>& fields() const
    {
        return fields_;
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
               const cmd_info& /* info */);
};

class mutate_in_request_body
{
  public:
    using response_body_type = mutate_in_response_body;
    static const inline client_opcode opcode = client_opcode::subdoc_multi_mutation;

    enum class store_semantics_type {
        /**
         * Replace the document, fail if it does not exist. This is the default.
         */
        replace,

        /**
         * Replace the document or create it if it does not exist.
         */
        upsert,

        /**
         * Replace the document or create it if it does not exist.
         */
        insert,
    };

    /**
     * Create the document if it does not exist. Implies `path_flag_create_parents`.
     * and `upsert` mutation semantics. Not valid with `insert`.
     */
    static const inline uint8_t doc_flag_mkdoc = 0b0000'0001;

    /**
     * Add the document only if it does not exist. Implies `path_flag_create_parents`.
     * Not valid with `doc_flag_mkdoc`.
     */
    static const inline uint8_t doc_flag_add = 0b0000'0010;

    /**
     * Allow access to XATTRs for deleted documents (instead of returning KEY_ENOENT).
     */
    static const inline uint8_t doc_flag_access_deleted = 0b0000'0100;

    /**
     * Used with `doc_flag_mkdoc` / `doc_flag_add`; if the document does not exist then create
     * it in the "Deleted" state, instead of the normal "Alive" state.
     * Not valid unless `doc_flag_mkdoc` or `doc_flag_add` specified.
     */
    static const inline uint8_t doc_flag_create_as_deleted = 0b0000'1000;

    /**
     * If the document exists and isn't deleted the operation will fail with .
     * If the input document _is_ deleted the result of the operation will store the
     * document as a "live" document instead of a deleted document.
     */
    static const inline uint8_t doc_flag_revive_document = 0b0001'0000;

    struct mutate_in_specs {
        /**
         * Should non-existent intermediate paths be created
         */
        static const inline uint8_t path_flag_create_parents = 0b0000'0001;

        /**
         * If set, the path refers to an Extended Attribute (XATTR).
         * If clear, the path refers to a path inside the document body.
         */
        static const inline uint8_t path_flag_xattr = 0b0000'0100;

        /**
         * Expand macro values inside extended attributes. The request is
         * invalid if this flag is set without `path_flag_create_parents` being set.
         */
        static const inline uint8_t path_flag_expand_macros = 0b0001'0000;

        struct entry {
            std::uint8_t opcode;
            std::uint8_t flags;
            std::string path;
            std::string param;
            std::size_t original_index{};
        };
        std::vector<entry> entries;

        static inline uint8_t build_path_flags(bool xattr, bool create_parents, bool expand_macros)
        {
            uint8_t flags = 0;
            if (xattr) {
                flags |= path_flag_xattr;
            }
            if (create_parents) {
                flags |= path_flag_create_parents;
            }
            if (expand_macros) {
                flags |= path_flag_expand_macros;
            }
            return flags;
        }

        void add_spec(subdoc_opcode operation,
                      bool xattr,
                      bool create_parents,
                      bool expand_macros,
                      const std::string& path,
                      const std::string& param)
        {
            if (operation == protocol::subdoc_opcode::replace && path.empty()) {
                operation = protocol::subdoc_opcode::set_doc;
            }
            add_spec(static_cast<std::uint8_t>(operation), build_path_flags(xattr, create_parents, expand_macros), path, param);
        }

        void add_spec(subdoc_opcode operation,
                      bool xattr,
                      bool create_parents,
                      bool expand_macros,
                      const std::string& path,
                      const std::int64_t increment)
        {
            Expects(operation == protocol::subdoc_opcode::counter);
            add_spec(static_cast<std::uint8_t>(operation),
                     build_path_flags(xattr, create_parents, expand_macros),
                     path,
                     std::to_string(increment));
        }

        void add_spec(subdoc_opcode operation, bool xattr, const std::string& path)
        {
            Expects(operation == protocol::subdoc_opcode::remove || operation == protocol::subdoc_opcode::remove_doc);
            if (operation == protocol::subdoc_opcode::remove && path.empty()) {
                operation = protocol::subdoc_opcode::remove_doc;
            }
            add_spec(static_cast<std::uint8_t>(operation), build_path_flags(xattr, false, false), path, "");
        }

        void add_spec(uint8_t operation, uint8_t flags, const std::string& path, const std::string& param)
        {
            Expects(is_valid_subdoc_opcode(operation));
            entries.emplace_back(entry{ operation, flags, path, param });
        }
    };

  private:
    std::string key_;
    std::vector<std::uint8_t> extras_{};
    std::vector<std::uint8_t> value_{};

    std::uint32_t expiry_{ 0 };
    std::uint8_t flags_{ 0 };
    mutate_in_specs specs_;
    std::vector<std::uint8_t> framing_extras_{};

  public:
    void id(const document_id& id);

    void expiry(uint32_t value)
    {
        expiry_ = value;
    }

    void access_deleted(bool value)
    {
        if (value) {
            flags_ |= doc_flag_access_deleted;
        } else {
            flags_ &= static_cast<std::uint8_t>(~doc_flag_access_deleted);
        }
    }

    void create_as_deleted(bool value)
    {
        if (value) {
            flags_ |= doc_flag_create_as_deleted;
        } else {
            flags_ &= static_cast<std::uint8_t>(~doc_flag_create_as_deleted);
        }
    }

    void store_semantics(store_semantics_type semantics)
    {
        flags_ &= 0b1111'1100; /* reset first two bits */
        switch (semantics) {
            case store_semantics_type::replace:
                /* leave bits as zeros */
                break;
            case store_semantics_type::upsert:
                flags_ |= doc_flag_mkdoc;
                break;
            case store_semantics_type::insert:
                flags_ |= doc_flag_add;
                break;
        }
    }

    void specs(const mutate_in_specs& specs)
    {
        specs_ = specs;
    }

    void durability(protocol::durability_level level, std::optional<std::uint16_t> timeout);

    void preserve_expiry();

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

    [[nodiscard]] const std::vector<std::uint8_t>& value()
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
        return framing_extras_.size() + extras_.size() + key_.size() + value_.size();
    }

  private:
    void fill_extras();

    void fill_value();
};

} // namespace couchbase::protocol
