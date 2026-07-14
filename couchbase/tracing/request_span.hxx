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

#include <cinttypes>
#include <memory>
#include <string>

namespace couchbase::tracing
{
class request_span
{
public:
  request_span() = default;
  request_span(const request_span& other) = default;
  request_span(request_span&& other) = default;
  auto operator=(const request_span& other) -> request_span& = default;
  auto operator=(request_span&& other) -> request_span& = default;
  virtual ~request_span() = default;

  explicit request_span(std::string name)
    : name_(std::move(name))
  {
  }
  request_span(std::string name, std::shared_ptr<request_span> parent)
    : name_(std::move(name))
    , parent_(std::move(parent))
  {
  }
  virtual void add_tag(const std::string& name, std::uint64_t value) = 0;
  virtual void add_tag(const std::string& name, const std::string& value) = 0;
  virtual void end() = 0;

  [[nodiscard]] auto name() const -> const std::string&
  {
    return name_;
  }

  [[nodiscard]] virtual auto parent() const -> std::shared_ptr<request_span>
  {
    return parent_;
  }

  [[nodiscard]] virtual auto uses_tags() const -> bool
  {
    return true;
  }

  /**
   * Efficiently record the dispatch operation identifier (the request opaque) without formatting it
   * to a string on the hot path.
   *
   * The default implementation returns false, which tells the SDK to record the identifier as a
   * regular string tag via add_tag() instead — so external tracer implementations are unaffected.
   * Built-in spans may override this to capture the raw value and defer the (rarely needed)
   * formatting until the span is actually reported.
   *
   * @return true if the span captured the identifier; false to request the string-tag fallback.
   */
  [[nodiscard]] virtual auto try_set_dispatch_operation_id(std::uint32_t /* opaque */) -> bool
  {
    return false;
  }

  /**
   * Efficiently record the local connection identifier of a dispatch span without materializing a
   * tag-name string on the hot path.
   *
   * Like try_set_dispatch_operation_id(), the default returns false so the SDK records the value as
   * a regular string tag and external tracer implementations are unaffected.
   *
   * @return true if the span captured the identifier; false to request the string-tag fallback.
   */
  [[nodiscard]] virtual auto try_set_dispatch_local_id(const std::string& /* local_id */) -> bool
  {
    return false;
  }

  /**
   * Efficiently record the result metadata of a dispatch span (server processing time and the peer
   * socket) in a single call, without materializing tag-name strings on the hot path.
   *
   * The default returns false so the SDK records the values as regular string tags and external
   * tracer implementations are unaffected.
   *
   * @return true if the span captured the metadata; false to request the string-tag fallback.
   */
  [[nodiscard]] virtual auto try_set_dispatch_result(std::uint64_t /* server_duration_us */,
                                                     const std::string& /* peer_address */,
                                                     std::uint16_t /* peer_port */) -> bool
  {
    return false;
  }

private:
  std::string name_{};
  std::shared_ptr<request_span> parent_{ nullptr };
};
} // namespace couchbase::tracing
