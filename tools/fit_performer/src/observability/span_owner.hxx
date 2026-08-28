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

#include "observability.top.pb.h"

#include <couchbase/tracing/request_span.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace fit_cxx::observability
{
class span_owner
{
public:
  span_owner() = default;

  auto get_span(const std::string& id) -> std::shared_ptr<couchbase::tracing::request_span>;

  void create_span(const std::shared_ptr<couchbase::tracing::request_tracer>& tracer,
                   const protocol::observability::SpanCreateRequest* req);

  void finish_span(const protocol::observability::SpanFinishRequest* req);

  template<typename Cmd>
  auto get_parent_span(const Cmd& cmd) -> std::shared_ptr<couchbase::tracing::request_span>
  {
    if (!cmd.has_options() || !cmd.options().has_parent_span_id()) {
      return {};
    }
    return get_span(cmd.options().parent_span_id());
  }

private:
  void store_span(const std::string& id, std::shared_ptr<couchbase::tracing::request_span> span);
  auto extract_span(const std::string& id) -> std::shared_ptr<couchbase::tracing::request_span>;

  std::map<std::string, std::shared_ptr<couchbase::tracing::request_span>> spans_{};
  std::mutex mutex_{};
};
} // namespace fit_cxx::observability
