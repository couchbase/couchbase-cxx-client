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
#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace core::utils::json
{
std::vector<std::byte>
generate_binary(const tao::json::value& object);

tao::json::value
parse_binary(const std::vector<std::byte>& input);
} // namespace core::utils::json
#endif

namespace codec
{
class json_transcoder
{
  public:
    template<typename Document>
    static auto encode(Document document) -> encoded_value
    {
        return { core::utils::json::generate_binary(tao::json::value(document)), codec_flags::json_common_flags };
    }

    template<typename Document>
    static auto decode(const binary& data) -> Document
    {
        try {
            if constexpr (std::is_same_v<Document, tao::json::value>) {
                return core::utils::json::parse_binary(data);
            } else {
                return core::utils::json::parse_binary(data).as<Document>();
            }
        } catch (const tao::pegtl::parse_error& e) {
            throw std::system_error(errc::common::decoding_failure,
                                    std::string("json_transcoder cannot parse document as JSON: ").append(e.message()));
        } catch (const std::out_of_range& e) {
            throw std::system_error(errc::common::decoding_failure,
                                    std::string("json_transcoder cannot parse document: ").append(e.what()));
        }
    }

    template<typename Document>
    static auto decode(const encoded_value& encoded) -> Document
    {
        if (!codec_flags::has_common_flags(encoded.flags, codec_flags::json_common_flags)) {
            throw std::system_error(errc::common::decoding_failure,
                                    "json_transcoder excepts document to have JSON common flags, flags=" + std::to_string(encoded.flags));
        }

        return decode<Document>(encoded.data);
    }
};

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
template<>
struct is_transcoder<json_transcoder> : public std::true_type {
};
#endif
} // namespace codec
} // namespace couchbase
