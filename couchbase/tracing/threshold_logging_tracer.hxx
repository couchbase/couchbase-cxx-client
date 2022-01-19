/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include <couchbase/tracing/request_tracer.hxx>
#include <couchbase/tracing/threshold_logging_options.hxx>

#include <asio/io_context.hpp>
#include <memory>
#include <string>

namespace couchbase::tracing
{
class threshold_logging_span;
class threshold_logging_tracer_impl;

class threshold_logging_tracer
  : public request_tracer
  , public std::enable_shared_from_this<threshold_logging_tracer>
{
  public:
    threshold_logging_tracer(asio::io_context& ctx, threshold_logging_options options);

    void start();

    std::shared_ptr<request_span> start_span(std::string name, std::shared_ptr<request_span> parent) override;
    void report(std::shared_ptr<threshold_logging_span> span);

  private:
    threshold_logging_options options_;
    std::shared_ptr<threshold_logging_tracer_impl> impl_{};
};

} // namespace couchbase::tracing
