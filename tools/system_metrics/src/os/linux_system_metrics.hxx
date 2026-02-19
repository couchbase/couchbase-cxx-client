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

#include "system_metrics.hxx"

namespace system_metrics
{

class LinuxSystemMetrics : public SystemMetrics
{
public:
  [[nodiscard]] auto get_system_boot_time() const -> std::uint64_t override;
  [[nodiscard]] auto get_logical_cpu_count() const -> std::uint32_t override;
  [[nodiscard]] auto get_proc_cpu(sm_pid_t pid) const -> proc_cpu_t override;
  [[nodiscard]] auto get_proc_memory(sm_pid_t pid) const -> proc_mem_t override;
  void iterate_proc_threads(sm_pid_t pid, IterateThreadCallback callback) override;
};

} // namespace system_metrics
