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

#include "log_entry.hxx"

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
struct traits<couchbase::core::log_entry> {
  template<template<typename...> class Traits>
  static void assign(basic_value<Traits>& v, const couchbase::core::log_entry& entry)
  {
    v = {
      { "timestamp", entry.timestamp },
      { "severity", entry.severity },
      { "message", entry.message },
      {
        "context",
        {
          { "trace_id", entry.context.trace_id },
          { "span_id", entry.context.span_id },
        },
      },
      { "attributes", entry.attributes },
    };
  }
};
} // namespace tao::json

namespace couchbase::core
{

auto
to_string(const log_entry& data) -> std::string
{
  tao::json::value json = data;
  return tao::json::to_string(json);
}
} // namespace couchbase::core
