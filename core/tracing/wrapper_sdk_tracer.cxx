/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025. Couchbase, Inc.
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

#include "wrapper_sdk_tracer.hxx"

#include "core/logger/logger.hxx"

namespace couchbase::core::tracing
{
auto
wrapper_sdk_tracer::start_span(std::string name,
                               std::shared_ptr<couchbase::tracing::request_span> parent)
  -> std::shared_ptr<couchbase::tracing::request_span>
{
  const auto parent_wrapper_span = std::dynamic_pointer_cast<wrapper_sdk_span>(parent);
  if (!parent_wrapper_span) {
    // If no parent span is provided, wrappers have no way of accessing any child spans created, so
    // there is no reason to create spans.
    return noop_instance_;
  }
  const auto span = std::make_shared<wrapper_sdk_span>(std::move(name), std::move(parent));
  parent_wrapper_span->add_child(span);
  return span;
}

wrapper_sdk_span::wrapper_sdk_span(std::string name)
  : couchbase::tracing::request_span{ std::move(name) }
{
}

wrapper_sdk_span::wrapper_sdk_span(std::string name,
                                   std::shared_ptr<couchbase::tracing::request_span> parent)
  : couchbase::tracing::request_span{ std::move(name) }
  , parent_{ parent }
{
}

void
wrapper_sdk_span::add_child(const std::shared_ptr<wrapper_sdk_span>& child)
{
  const std::scoped_lock lock(children_mutex_);
  children_.emplace_back(child);
}

void
wrapper_sdk_span::add_tag(const std::string& name, std::uint64_t value)
{
  uint_tags_.emplace(name, value);
}

void
wrapper_sdk_span::add_tag(const std::string& name, const std::string& value)
{
  string_tags_.emplace(name, value);
}

void
wrapper_sdk_span::end()
{
  end_time_ = std::chrono::system_clock::now();
}

auto
wrapper_sdk_span::uint_tags() const -> const std::map<std::string, std::uint64_t>&
{
  return uint_tags_;
}

auto
wrapper_sdk_span::string_tags() const -> const std::map<std::string, std::string>&
{
  return string_tags_;
}

auto
wrapper_sdk_span::children() -> std::vector<std::shared_ptr<wrapper_sdk_span>>
{
  const std::scoped_lock lock(children_mutex_);
  return children_;
}

auto
wrapper_sdk_span::start_time() const -> const std::chrono::system_clock::time_point&
{
  return start_time_;
}

auto
wrapper_sdk_span::end_time() const -> const std::chrono::system_clock::time_point&
{
  return end_time_;
}

auto
wrapper_sdk_span::parent() const -> std::shared_ptr<couchbase::tracing::request_span>
{
  return parent_.lock();
}

} // namespace couchbase::core::tracing
