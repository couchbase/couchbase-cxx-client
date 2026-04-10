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

#include "span_owner.hxx"

#include "../exceptions.hxx"

#include <couchbase/tracing/request_span.hxx>

#include <spdlog/fmt/fmt.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace fit_cxx::observability
{
auto
span_owner::get_span(const std::string& id) -> std::shared_ptr<couchbase::tracing::request_span>
{
  const std::scoped_lock lock{ mutex_ };
  try {
    return spans_.at(id);
  } catch (const std::out_of_range&) {
    throw performer_exception::not_found(fmt::format("Span with id {} not found", id));
  }
}

void
span_owner::create_span(const std::shared_ptr<couchbase::tracing::request_tracer>& tracer,
                        const protocol::observability::SpanCreateRequest* req)
{
  std::shared_ptr<couchbase::tracing::request_span> parent_span{};
  if (req->has_parent_span_id()) {
    parent_span = get_span(req->parent_span_id());
  }

  auto span = tracer->start_span(req->name(), std::move(parent_span));

  if (req->attributes_size() > 0 && !span->uses_tags()) {
    throw performer_exception::failed_precondition("Span does not use tags");
  }

  for (const auto& [key, val] : req->attributes()) {
    switch (val.value_case()) {
      case protocol::observability::Attribute::ValueCase::kValueLong:
        span->add_tag(key, static_cast<std::uint64_t>(val.value_long()));
        break;
      case protocol::observability::Attribute::ValueCase::kValueString:
        span->add_tag(key, val.value_string());
        break;
      case protocol::observability::Attribute::ValueCase::kValueBoolean:
        throw performer_exception::unimplemented(
          "The C++ SDK does not currently support boolean attribute values");
      default:
        throw performer_exception::invalid_argument(
          fmt::format("Unexpected span attribute Value type: {}", val.DebugString()));
    }
  }

  store_span(req->id(), std::move(span));
}

void
span_owner::finish_span(const protocol::observability::SpanFinishRequest* req)
{
  auto span = extract_span(req->id());
  span->end();
}

void
span_owner::store_span(const std::string& id,
                       std::shared_ptr<couchbase::tracing::request_span> span)
{
  const std::scoped_lock lock{ mutex_ };
  spans_[id] = std::move(span);
}

auto
span_owner::extract_span(const std::string& id) -> std::shared_ptr<couchbase::tracing::request_span>
{
  const std::scoped_lock lock{ mutex_ };
  try {
    return spans_.extract(id).mapped();
  } catch (const std::out_of_range&) {
    throw performer_exception::not_found(fmt::format("Span with id {} not found", id));
  }
}

} // namespace fit_cxx::observability
