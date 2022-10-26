/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "crud_component.hxx"

#include "collections_component.hxx"

#include "core/logger/logger.hxx"
#include "core/mcbp/buffer_writer.hxx"
#include "core/protocol/datatype.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/unsigned_leb128.hxx"
#include "mcbp/big_endian.hxx"
#include "mcbp/queue_request.hxx"
#include "mcbp/queue_response.hxx"
#include "platform/base64.h"
#include "snappy.h"
#include "utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

namespace couchbase::core
{
class crud_component_impl
{
  public:
    crud_component_impl(asio::io_context& io, collections_component collections, std::shared_ptr<retry_strategy> default_retry_strategy)
      : io_{ io }
      , collections_{ std::move(collections) }
      , default_retry_strategy_{ std::move(default_retry_strategy) }
    {
        (void)io_;
    }

  private:
    asio::io_context& io_;
    collections_component collections_;
    std::shared_ptr<retry_strategy> default_retry_strategy_;
};

crud_component::crud_component(asio::io_context& io,
                               collections_component collections,
                               std::shared_ptr<retry_strategy> default_retry_strategy)
  : impl_{ std::make_shared<crud_component_impl>(io, std::move(collections), std::move(default_retry_strategy)) }
{
}

} // namespace couchbase::core
