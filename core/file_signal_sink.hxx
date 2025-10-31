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

#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <memory>

namespace couchbase::core
{
class file_signal_sink_impl;

class file_signal_sink : public std::enable_shared_from_this<file_signal_sink>
{
public:
  explicit file_signal_sink(FILE* output);
  file_signal_sink(const file_signal_sink&) = delete;
  file_signal_sink(file_signal_sink&&) = delete;
  auto operator=(const file_signal_sink&) -> file_signal_sink& = delete;
  auto operator=(file_signal_sink&&) -> file_signal_sink& = delete;
  ~file_signal_sink() = default;

  void start();
  void stop();

  [[nodiscard]] auto tracer() -> std::shared_ptr<couchbase::tracing::request_tracer>;
  [[nodiscard]] auto meter() -> std::shared_ptr<couchbase::metrics::meter>;

private:
  std::shared_ptr<file_signal_sink_impl> impl_{};
};

} // namespace couchbase::core
