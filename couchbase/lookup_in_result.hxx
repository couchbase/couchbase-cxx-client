/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/result.hxx>
#include <couchbase/subdoc/lookup_in_macro.hxx>

#include <optional>

namespace couchbase
{

/**
 * Represents result of lookup_in operations.
 *
 * @since 1.0.0
 * @committed
 */
class lookup_in_result : public result
{
  public:
    /**
     * @since 1.0.0
     * @internal
     */
    struct entry {
        std::string path;
        codec::binary value;
        std::size_t original_index;
        bool exists;
    };

    /**
     * @since 1.0.0
     * @internal
     */
    lookup_in_result() = default;

    /**
     * Constructs result for lookup_in_result operation
     *
     * @param cas
     * @param entries list of the fields returned by the server
     * @param is_deleted
     *
     * @since 1.0.0
     * @committed
     */
    lookup_in_result(couchbase::cas cas, std::vector<entry> entries, bool is_deleted)
      : result{ cas }
      , entries_{ std::move(entries) }
      , is_deleted_{ is_deleted }
    {
    }

    /**
     * Decodes field of the document into type.
     *
     * @tparam Document custom type that codec::json_transcoder should use to decode the field
     * @param index the index of the result field
     * @return decoded document content
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Document>
    [[nodiscard]] auto content_as(std::size_t index) const -> Document
    {
        for (const entry& e : entries_) {
            if (e.original_index == index) {
                return codec::tao_json_serializer::deserialize<Document>(e.value);
            }
        }
        throw std::system_error(errc::key_value::path_invalid, "invalid index for lookup_in result: {}" + std::to_string(index));
    }

    /**
     * Decodes field of the document into type.
     *
     * @tparam Document custom type that codec::json_transcoder should use to decode the field
     * @param path the path of the result field
     * @return decoded document content
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Document>
    [[nodiscard]] auto content_as(const std::string& path) const -> Document
    {
        for (const entry& e : entries_) {
            if (e.path == path) {
                return codec::tao_json_serializer::deserialize<Document>(e.value);
            }
        }
        throw std::system_error(errc::key_value::path_invalid, "invalid path for lookup_in result: " + path);
    }

    /**
     * Decodes field of the document into type.
     *
     * @tparam Document custom type that codec::json_transcoder should use to decode the field
     * @param macro the path of the result field
     * @return decoded document content
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Document>
    [[nodiscard]] auto content_as(subdoc::lookup_in_macro macro) const -> Document
    {
        const auto& macro_string = subdoc::to_string(macro);
        for (const entry& e : entries_) {
            if (e.path == macro_string) {
                return codec::tao_json_serializer::deserialize<Document>(e.value);
            }
        }
        throw std::system_error(errc::key_value::path_invalid,
                                "invalid path for lookup_in result: macro#" + std::to_string(static_cast<std::uint32_t>(macro)));
    }

    /**
     * Allows to check if a value at the given index exists.
     *
     * @param index the index at which to check.
     * @return true if a value is present at the index, false otherwise.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto exists(std::size_t index) const -> bool
    {
        for (const entry& e : entries_) {
            if (e.original_index == index) {
                return e.exists;
            }
        }
        return false;
    }

    /**
     * Allows to check if a value at the given index exists.
     *
     * @param macro the path of the result field
     * @return true if a value is present at the index, false otherwise.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto exists(subdoc::lookup_in_macro macro) const -> bool
    {
        const auto& macro_string = subdoc::to_string(macro);
        for (const entry& e : entries_) {
            if (e.path == macro_string) {
                return e.exists;
            }
        }
        return false;
    }

    /**
     * Allows to check if a value at the given index exists.
     *
     * @param path the path of the result field
     * @return true if a value is present at the index, false otherwise.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto exists(const std::string& path) const -> bool
    {
        for (const entry& e : entries_) {
            if (e.path == path) {
                return e.exists;
            }
        }
        return false;
    }

    /**
     * Returns whether this document was deleted (a tombstone).
     *
     * Will always be false unless {@link lookup_in_options#access_deleted()} has been set.
     *
     * For internal use only: applications should not require it.
     *
     * @return whether this document was a tombstone
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto is_deleted() const -> bool
    {
        return is_deleted_;
    }

    /**
     * Returns whether the field has value
     *
     * @param index the index of the result field
     * @return true if the server returned value for the field
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto has_value(std::size_t index) const -> bool
    {
        for (const entry& e : entries_) {
            if (e.original_index == index) {
                return !e.value.empty();
            }
        }
        throw std::system_error(errc::key_value::path_invalid, "invalid index for mutate_in result: " + std::to_string(index));
    }

    /**
     * Returns whether the field has value
     *
     * @param path the path of the result field
     * @return true if the server returned value for the field
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto has_value(const std::string& path) const -> bool
    {
        for (const entry& e : entries_) {
            if (e.path == path) {
                return !e.value.empty();
            }
        }
        throw std::system_error(errc::key_value::path_invalid, "invalid path for mutate_in result: " + path);
    }

  private:
    std::vector<entry> entries_{};
    bool is_deleted_{ false };
};

} // namespace couchbase
