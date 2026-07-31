/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "core/protostellar/dispatcher.hxx"

#include <grpcpp/create_channel.h>

#include <utility>

namespace couchbase::core::protostellar
{
auto
default_channel_arguments() -> grpc::ChannelArguments
{
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30'000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5'000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  // RFC 77 (Bootstrapping -> Maximum Message Size): couchbase2 clients must raise the receive
  // limit to 25 MiB; gRPC's 4 MB default is too small for the largest values Couchbase serves.
  args.SetMaxReceiveMessageSize(25 * 1024 * 1024);
  return args;
}

auto
make_insecure_channel(const std::string& endpoint) -> std::shared_ptr<grpc::Channel>
{
  return grpc::CreateCustomChannel(
    endpoint, grpc::InsecureChannelCredentials(), default_channel_arguments());
}

dispatcher::dispatcher(asio::io_context& io, std::shared_ptr<grpc::Channel> channel)
  : io_{ io }
  , channel_{ std::move(channel) }
{
}

auto
dispatcher::channel() const -> const std::shared_ptr<grpc::Channel>&
{
  return channel_;
}

} // namespace couchbase::core::protostellar
