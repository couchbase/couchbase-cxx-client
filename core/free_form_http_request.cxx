/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "free_form_http_request.hxx"

namespace couchbase::core
{
class http_response_impl
{
  public:
    auto endpoint() -> std::string
    {
        return {};
    }

    auto status_code() -> std::uint32_t
    {
        return {};
    }

    auto content_length() -> std::size_t
    {
        return {};
    }

    auto body() -> std::vector<std::byte>
    {
        return {};
    }

    auto close() -> std::error_code
    {
        return {};
    }
};

http_response::http_response()
  : impl_{ std::make_shared<http_response_impl>() }
{
}

auto
http_response::endpoint() -> std::string
{
    return impl_->endpoint();
}
auto
http_response::status_code() -> std::uint32_t
{
    return impl_->status_code();
}
auto
http_response::content_length() -> std::size_t
{
    return impl_->content_length();
}

auto
http_response::body() -> std::vector<std::byte>
{
    return impl_->body();
}

auto
http_response::close() -> std::error_code
{
    return impl_->close();
}
} // namespace couchbase::core
