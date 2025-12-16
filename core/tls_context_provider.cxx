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

#include "tls_context_provider.hxx"

#include <asio/ssl/context.hpp>

namespace couchbase::core
{

tls_context_provider::tls_context_provider()
  : ctx_(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client))
{
}

tls_context_provider::tls_context_provider(std::shared_ptr<asio::ssl::context> ctx)
  : ctx_(std::move(ctx))
{
}

auto
tls_context_provider::get_ctx() const -> std::shared_ptr<asio::ssl::context>
{
  return std::atomic_load(&ctx_);
}

void
tls_context_provider::set_ctx(std::shared_ptr<asio::ssl::context> new_ctx)
{
  std::atomic_store(&ctx_, std::move(new_ctx));
}
} // namespace couchbase::core
