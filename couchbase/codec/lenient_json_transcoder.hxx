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

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/codec/transcoder_traits.hxx>

namespace couchbase::codec
{
class tao_json_serializer;

/**
 * A JSON transcoder that encodes with JSON common flags like @ref json_transcoder, but does NOT
 * validate the document's common flags when decoding.
 *
 * It backs the no-transcoder `content_as<Document>()` overloads of the transactions read results
 * (@ref couchbase::transactions::transaction_get_result and the get-multi results): per the
 * ExtBinarySupport spec the SDK "ignores [the flags] when reading", so e.g. a document written with
 * binary common flags but holding valid JSON can still be read. This restores the
 * pre-ExtBinarySupport behaviour where those accessors did not reject non-JSON flags. Ordinary KV
 * reads continue to use the strict @ref json_transcoder.
 *
 * It is an implementation detail of the transactions read path and is not meant to be named
 * explicitly by applications.
 *
 * @since 1.4.0
 * @internal
 */
template<typename Serializer>
class lenient_json_transcoder
{
public:
  template<typename Document>
  static auto encode(Document document) -> encoded_value
  {
    return { Serializer::serialize(document), codec_flags::json_common_flags };
  }

  template<typename Document>
  static auto decode(const encoded_value& encoded) -> Document
  {
    return Serializer::template deserialize<Document>(encoded.data);
  }
};

using default_lenient_json_transcoder = lenient_json_transcoder<tao_json_serializer>;

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
template<>
struct is_transcoder<default_lenient_json_transcoder> : public std::true_type {
};
#endif
} // namespace couchbase::codec
