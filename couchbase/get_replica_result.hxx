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

#include <couchbase/result.hxx>

#include <vector>

namespace couchbase
{

/**
 * Represents result of @ref collection#get_any_replica operations, also returned by @ref collection#get_all_replicas.
 *
 * @since 1.0.0
 * @committed
 */
class get_replica_result : public result
{
  public:
    /**
     * @since 1.0.0
     * @internal
     */
    get_replica_result()
      : is_replica_{ false }
      , flags_{ 0 }
    {
    }

    /**
     * Constructs result for get_any_replica operation, or an entry for get_all_replicas operation.
     *
     * @param cas
     * @param is_replica true if the document originates from replica node
     * @param value raw document contents
     * @param flags flags stored on the server, that describes document encoding
     *
     * @since 1.0.0
     * @committed
     */
    get_replica_result(couchbase::cas cas, bool is_replica, std::vector<std::byte> value, std::uint32_t flags)
      : result{ cas }
      , is_replica_{ is_replica }
      , value_{ std::move(value) }
      , flags_{ flags }
    {
    }

    /**
     *
     * @return true if the document came from replica, false for active node.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto is_replica() const -> bool
    {
        return is_replica_;
    }

    /**
     * Returns raw content of the document.
     *
     * @return
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto content() const -> const std::vector<std::byte>&
    {
        return value_;
    }

    /**
     * Returns flags associated with the document.
     *
     * @return
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto flags() const -> std::uint32_t
    {
        return flags_;
    }

    /**
     * Decodes content of the document using given transcoder.
     *
     * @tparam Transcoder type that has static function `decode` that takes `std::vector<std::byte>` with `flags` and returns
     * `value_type`
     * @tparam value_type type that `Transcoder` returns
     * @return decoded document content
     *
     * @par Get flags and value as they are stored in the result
     *  Here is an example of custom transcoder, that just extracts value and flags as they are stored in the result.
     * @snippet test_integration_read_replica.cxx smuggling-transcoder
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder, typename value_type = typename Transcoder::value_type>
    [[nodiscard]] auto content_as() const -> value_type
    {
        return Transcoder::decode(value_, flags_);
    }

  private:
    bool is_replica_;
    std::vector<std::byte> value_;
    std::uint32_t flags_;
};

} // namespace couchbase
