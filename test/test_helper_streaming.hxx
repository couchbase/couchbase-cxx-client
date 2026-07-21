/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "core/free_form_http_request.hxx"

#include <asio/io_context.hpp>

#include <string>

namespace test::utils
{
inline auto
make_cached_response_body(asio::io_context& io, std::string data)
  -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory(io, std::move(data));
}

// Like make_cached_response_body, but the body is handed out in chunk_size-byte slices (one per
// next() pull) so a consumer's read-vs-consume back-pressure can be exercised deterministically.
inline auto
make_chunked_response_body(asio::io_context& io, std::string data, std::size_t chunk_size)
  -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory(io, std::move(data), chunk_size);
}
} // namespace test::utils
