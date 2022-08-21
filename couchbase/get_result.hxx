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

#include <couchbase/codec/json_transcoder.hxx>
#include <couchbase/result.hxx>

#include <chrono>
#include <cinttypes>
#include <optional>
#include <vector>

namespace couchbase
{

/**
 * Represents result of @ref collection#get
 *
 * @since 1.0.0
 * @committed
 */
class get_result : public result
{
  public:
    /**
     * @since 1.0.0
     * @internal
     */
    get_result() = default;

    /**
     * Constructs result for get operation
     *
     * @param cas
     * @param value raw document contents along with flags describing its structure
     * @param expiry_time optional point in time when the document will expire
     *
     * @since 1.0.0
     * @committed
     */
    get_result(couchbase::cas cas, codec::encoded_value value, std::optional<std::chrono::system_clock::time_point> expiry_time)
      : result{ cas }
      , value_{ std::move(value) }
      , expiry_time_{ expiry_time }
    {
    }

    /**
     * Decodes content of the document using given codec.
     *
     * @tparam Document custom type that `Transcoder` returns
     * @tparam Transcoder type that has static function `decode` that takes codec::encoded_value and returns `Document`
     * @return decoded document content
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Document,
             typename Transcoder = codec::json_transcoder,
             std::enable_if_t<!codec::is_transcoder_v<Document>, bool> = true,
             std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
    [[nodiscard]] auto content_as() const -> Document
    {
        return Transcoder::template decode<Document>(value_);
    }

    /**
     * Decodes content of the document using given codec.
     *
     * @tparam Transcoder type that has static function `decode` that takes codec::encoded_value and returns `Transcoder::value_type`
     * @return decoded document content
     *
     * @par Get flags and value as they are stored in the result
     *  Here is an example of custom transcoder, that just extracts value and flags as they are stored in the result.
     * @snippet test_integration_read_replica.cxx smuggling-transcoder
     *  Usage
     * @snippet test_integration_read_replica.cxx smuggling-transcoder-usage
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Transcoder, std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
    [[nodiscard]] auto content_as() const -> typename Transcoder::document_type
    {
        return Transcoder::decode(value_);
    }

    /**
     * If the document has an expiry, returns the point in time when the loaded
     * document expires.
     *
     * @note This method always returns an empty `std::optional` unless
     * the {@link collection#get()} request was made using {@link get_options#with_expiry()}
     * set to true.
     *
     * @return expiry time if present
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto expiry_time() const -> const std::optional<std::chrono::system_clock::time_point>&
    {
        return expiry_time_;
    }

  private:
    codec::encoded_value value_{};
    std::optional<std::chrono::system_clock::time_point> expiry_time_{};
};

} // namespace couchbase
