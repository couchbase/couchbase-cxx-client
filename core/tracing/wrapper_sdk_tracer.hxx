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

#pragma once

#include <couchbase/tracing/request_tracer.hxx>

#include "core/tracing/noop_tracer.hxx"

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace couchbase::core::tracing
{
/**
 * Tracer for use by C++ SDK wrappers. It is intended for storing spans & tags in memory, so that
 * wrappers can then use their own tracing infrastructure to create them.
 */
class wrapper_sdk_tracer : public couchbase::tracing::request_tracer
{
public:
  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent)
    -> std::shared_ptr<couchbase::tracing::request_span> override;

private:
  std::shared_ptr<noop_span> noop_instance_{ std::make_shared<noop_span>() };
};

class wrapper_sdk_span : public couchbase::tracing::request_span
{
public:
  wrapper_sdk_span() = default;
  explicit wrapper_sdk_span(std::string name);
  wrapper_sdk_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent);

  void add_child(const std::shared_ptr<wrapper_sdk_span>& child);

  void add_tag(const std::string& name, std::uint64_t value) override;
  void add_tag(const std::string& name, const std::string& value) override;

  void end() override;

  [[nodiscard]] auto uint_tags() const -> const std::map<std::string, std::uint64_t>&;
  [[nodiscard]] auto string_tags() const -> const std::map<std::string, std::string>&;
  [[nodiscard]] auto children() -> std::vector<std::shared_ptr<wrapper_sdk_span>>;
  [[nodiscard]] auto start_time() const -> const std::chrono::system_clock::time_point&;
  [[nodiscard]] auto end_time() const -> const std::chrono::system_clock::time_point&;

  [[nodiscard]] auto parent() const -> std::shared_ptr<couchbase::tracing::request_span> override;

private:
  std::map<std::string, std::uint64_t> uint_tags_{};
  std::map<std::string, std::string> string_tags_{};
  std::chrono::system_clock::time_point start_time_{ std::chrono::system_clock::now() };
  std::chrono::system_clock::time_point end_time_{};

  // The only way to access spans is through their parents, so parents must hold owning references
  // to their children.
  std::vector<std::shared_ptr<wrapper_sdk_span>> children_{};
  std::mutex children_mutex_;

  // A weak pointer is used instead of the shared pointer in the parent class, to avoid circular
  // references.
  std::weak_ptr<couchbase::tracing::request_span> parent_{};
};
} // namespace couchbase::core::tracing
