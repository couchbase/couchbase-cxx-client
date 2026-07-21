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

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/serializer_traits.hxx>
#include <utility>

namespace couchbase
{

namespace codec
{
class tao_json_serializer;
} // namespace codec

/**
 * Represents a single row of a streaming analytics result.
 *
 * The raw JSON bytes can be decoded via @ref content_as().
 *
 * @since 1.4.0
 * @volatile
 */
class analytics_row
{
public:
  /**
   * @since 1.4.0
   * @volatile
   */
  analytics_row() = default;

  /**
   * Constructs a row from raw JSON bytes.
   *
   * @param content raw JSON bytes for this row
   *
   * @since 1.4.0
   * @volatile
   */
  explicit analytics_row(codec::binary content)
    : content_{ std::move(content) }
  {
  }

  /**
   * Decodes the row content into the requested document type using the given serializer.
   *
   * Only call this on a row obtained from a successful @ref analytics_stream_result#next() (falsy
   * error, engaged optional). The end-of-stream and error sentinels carry an empty @ref
   * analytics_row, and decoding empty/malformed content throws the serializer's parse exception —
   * gate on the three-state next() contract (check the error, then the optional) before decoding.
   *
   * @tparam Serializer the serializer to use (defaults to @ref codec::tao_json_serializer)
   * @tparam Document the document type to decode into (defaults to the serializer's document_type)
   * @return the decoded document
   * @throws the serializer's deserialization exception if the content is empty or not valid JSON
   *
   * @since 1.4.0
   * @volatile
   */
  template<typename Serializer = codec::tao_json_serializer,
           typename Document = typename Serializer::document_type,
           std::enable_if_t<codec::is_serializer_v<Serializer>, bool> = true>
  [[nodiscard]] auto content_as() const -> Document
  {
    return Serializer::template deserialize<Document>(content_);
  }

  /**
   * Returns the raw binary content of this row.
   *
   * @return raw JSON bytes
   *
   * @since 1.4.0
   * @volatile
   */
  [[nodiscard]] auto content_as_binary() const -> const codec::binary&
  {
    return content_;
  }

private:
  codec::binary content_{};
};

} // namespace couchbase
