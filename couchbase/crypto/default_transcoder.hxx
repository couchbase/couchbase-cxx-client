/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025. Couchbase, Inc.
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

#include <couchbase/codec/transcoder_traits.hxx>
#include <couchbase/crypto/transcoder.hxx>

namespace couchbase
{
namespace codec
{
class tao_json_serializer;
} // namespace codec

namespace crypto
{
using default_transcoder = transcoder<codec::tao_json_serializer>;
} // namespace crypto
} // namespace couchbase

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
template<>
struct couchbase::codec::is_transcoder<couchbase::crypto::default_transcoder>
  : public std::true_type {
};

template<>
struct couchbase::codec::is_crypto_transcoder<couchbase::crypto::default_transcoder>
  : public std::true_type {
};
#endif
