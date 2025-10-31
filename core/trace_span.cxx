/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Current Couchbase, Inc.
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

#include "trace_span.hxx"

#include "chrono_utils.hxx"

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

namespace tao::json
{
template<>
struct traits<couchbase::core::signal_attribute> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::signal_attribute& attr)
  {
    v = {
      { "name", attr.name },
      { "value", attr.value },
    };
  }
};

template<>
struct traits<couchbase::core::trace_event> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::trace_event& event)
  {
    v = {
      { "name", event.name },
      { "timestamp", couchbase::core::to_iso8601_utc(event.timestamp) },
      { "attributes", event.attributes },
    };
  }
};

template<>
struct traits<couchbase::core::trace_span> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::trace_span& span)
  {
    v = {
      { "name", span.name },
      {
        "context",
        {
          { "trace_id", span.context.trace_id },
          { "span_id", span.context.span_id },
        },
      },
      { "parent_id", span.parent_id },
      { "start_time", couchbase::core::to_iso8601_utc(span.start_time) },
      { "end_time", couchbase::core::to_iso8601_utc(span.end_time) },
      { "attributes", span.attributes },
      { "events", span.events },
    };
  }
};
} // namespace tao::json

namespace couchbase::core
{

auto
to_string(const trace_span& data) -> std::string
{
  tao::json::value json = data;
  return tao::json::to_string(json);
}
} // namespace couchbase::core
