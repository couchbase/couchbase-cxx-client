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

#include <type_traits>

namespace couchbase::codec
{
template<typename T>
struct is_transcoder : public std::false_type {
};

template<typename T>
inline constexpr bool is_transcoder_v = is_transcoder<T>::value;

template<typename T>
struct is_crypto_transcoder : public std::false_type {
};

template<typename T>
inline constexpr bool is_crypto_transcoder_v = is_crypto_transcoder<T>::value;
} // namespace couchbase::codec
