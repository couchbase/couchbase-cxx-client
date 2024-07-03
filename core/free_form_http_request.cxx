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

#include "io/http_streaming_response.hxx"
#include "utils/movable_function.hxx"

#include <memory>

namespace couchbase::core
{
class http_response_impl
{
public:
  http_response_impl() = default;

  explicit http_response_impl(io::http_streaming_response streaming_resp)
    : streaming_resp_{ std::move(streaming_resp) }
  {
  }

  auto endpoint() const -> std::string
  {
    return {};
  }

  auto status_code() const -> std::uint32_t
  {
    return streaming_resp_.status_code();
  }

  auto content_length() const -> std::size_t
  {
    if (streaming_resp_.headers().find("content-length") == streaming_resp_.headers().end()) {
      return 0;
    }
    return std::stoul(streaming_resp_.headers().at("content-length"));
  }

  void next_body(utils::movable_function<void(std::string, std::error_code)> callback)
  {
    return streaming_resp_.body().next(std::move(callback));
  }

  void close_body()
  {
    return streaming_resp_.body().close();
  }

private:
  io::http_streaming_response streaming_resp_;
};

http_response::http_response(io::http_streaming_response resp)
  : impl_{ std::make_shared<http_response_impl>(std::move(resp)) }
{
}

auto
http_response::endpoint() const -> std::string
{
  return impl_->endpoint();
}
auto
http_response::status_code() const -> std::uint32_t
{
  return impl_->status_code();
}
auto
http_response::content_length() const -> std::size_t
{
  return impl_->content_length();
}

auto
http_response::body() const -> http_response_body
{
  return { impl_ };
}

void
http_response::close()
{
  return impl_->close_body();
}

http_response_body::http_response_body(std::shared_ptr<http_response_impl> impl)
  : impl_{ std::move(impl) }
{
}

void
http_response_body::cancel()
{
  return impl_->close_body();
}

void
http_response_body::next(utils::movable_function<void(std::string, std::error_code)> callback)
{
  return impl_->next_body(std::move(callback));
}
} // namespace couchbase::core
