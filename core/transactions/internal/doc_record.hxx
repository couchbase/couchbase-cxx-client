/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include "transaction_fields.hxx"

#include "core/document_id.hxx"

#include <cstdint>
#include <string>

namespace couchbase::core::transactions
{
struct doc_record {
  public:
    doc_record(std::string bucket_name, std::string scope_name, std::string collection_name, std::string id)
      : id_(std::move(bucket_name), std::move(scope_name), std::move(collection_name), std::move(id))
    {
    }

    [[nodiscard]] const std::string& bucket_name() const
    {
        return id_.bucket();
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_.key();
    }

    [[nodiscard]] const std::string& collection_name() const
    {
        return id_.collection();
    }

    [[nodiscard]] const core::document_id& document_id() const
    {
        return id_;
    }

    static doc_record create_from(const tao::json::value& obj)
    {
        std::string bucket_name = obj.at(ATR_FIELD_PER_DOC_BUCKET).get_string();
        std::string scope_name = obj.at(ATR_FIELD_PER_DOC_SCOPE).get_string();
        std::string collection_name = obj.at(ATR_FIELD_PER_DOC_COLLECTION).get_string();
        std::string id = obj.at(ATR_FIELD_PER_DOC_ID).get_string();
        return doc_record(bucket_name, scope_name, collection_name, id);
    }

    template<typename OStream>
    friend OStream& operator<<(OStream& os, const doc_record& dr)
    {
        os << "doc_record{";
        os << "bucket: " << dr.id_.bucket() << ",";
        os << "scope: " << dr.id_.scope() << ",";
        os << "collection: " << dr.id_.collection() << ",";
        os << "key: " << dr.id_.key();
        os << "}";
        return os;
    }

  private:
    core::document_id id_;
};
} // namespace couchbase::core::transactions

template<>
struct fmt::formatter<couchbase::core::transactions::doc_record> {
  public:
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(const couchbase::core::transactions::doc_record& r, FormatContext& ctx) const
    {
        return format_to(ctx.out(),
                         "doc_record:{{ bucket: {}, scope: {}, collection: {}, key: {} }}",
                         r.document_id().bucket(),
                         r.document_id().scope(),
                         r.document_id().collection(),
                         r.document_id().key());
    }
};