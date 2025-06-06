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

#include "logger.hxx"

#include "fmt_log_record_exporter.hxx"

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#endif
#include <opentelemetry/logs/provider.h>

#include <opentelemetry/sdk/logs/logger.h>
#include <opentelemetry/sdk/logs/logger_context_factory.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h>
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif

#if defined(_WIN32)
#include <processthreadsapi.h>
#include <windows.h>
#else
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__linux__)
#include <pthread.h>
#else
#include <thread>
#endif
#endif

#include <vector>

namespace couchbase::observability
{
namespace
{
inline auto
thread_id() noexcept -> std::uint64_t
{
#ifdef _WIN32
  return static_cast<std::uint64_t>(::GetCurrentThreadId());
#elif defined(__linux__)
  return static_cast<std::uint64_t>(::syscall(SYS_gettid));
#elif __APPLE__
  uint64_t tid;
#ifdef MAC_OS_X_VERSION_MAX_ALLOWED
  {
#if (MAC_OS_X_VERSION_MAX_ALLOWED < 1060) || defined(__POWERPC__)
    tid = pthread_mach_thread_np(pthread_self());
#elif MAC_OS_X_VERSION_MIN_REQUIRED < 1060
    if (&pthread_threadid_np) {
      pthread_threadid_np(nullptr, &tid);
    } else {
      tid = pthread_mach_thread_np(pthread_self());
    }
#else
    pthread_threadid_np(nullptr, &tid);
#endif
  }
#else
  pthread_threadid_np(nullptr, &tid);
#endif
  return static_cast<std::uint64_t>(tid);
#else
  return static_cast<std::uint64_t>(std::hash<std::thread::id>()(std::this_thread::get_id()));
#endif
}

inline auto
process_id() noexcept -> int
{
#ifdef _WIN32
  return static_cast<int>(::GetCurrentProcessId());
#else
  return ::getpid();
#endif
}

inline auto
thread_name() -> std::string
{
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__linux__)
  std::string name(100, '\0');
  int res = ::pthread_getname_np(::pthread_self(), name.data(), name.size());
  if (res == 0) {
    name.resize(strnlen(name.data(), name.size()));
    return name;
  }
#elif defined(_WIN32)
  HANDLE hThread = GetCurrentThread();
  PWSTR data = nullptr;
  HRESULT hr = GetThreadDescription(hThread, &data);
  if (SUCCEEDED(hr) && data) {
    int len = WideCharToMultiByte(CP_UTF8, 0, data, -1, nullptr, 0, nullptr, nullptr);
    std::string name(len - 1, '\0');
    WideCharToMultiByte(CP_UTF8, 0, data, -1, name.data(), len, nullptr, nullptr);
    LocalFree(data);
    return name;
  }
#endif
  return {};
}

} // namespace

auto
logger() -> std::shared_ptr<opentelemetry::logs::Logger>
{
  thread_local auto logger = opentelemetry::logs::Provider::GetLoggerProvider()->GetLogger(
    "cbc_logger",
    "cxxcbc",
    couchbase::core::meta::sdk_semver(),
    "",
    {
      { "process_id", process_id() },
      { "thread_id", thread_id() },
      { "thread_name", thread_name() },
    });
  return logger;
}

void
init_logger(const logger_options& options)
{
  using namespace opentelemetry;

  if (options.use_http_logger) {
    exporter::otlp::OtlpHttpLogRecordExporterOptions logger_options;
    auto exporter = exporter::otlp::OtlpHttpLogRecordExporterFactory::Create(logger_options);
    auto processor = sdk::logs::SimpleLogRecordProcessorFactory::Create(std::move(exporter));
    auto resource = sdk::resource::Resource::Create({
      { "service.name", "cbc" },
      { "service.version", couchbase::core::meta::sdk_semver() },
    });
    std::vector<std::unique_ptr<sdk::logs::LogRecordProcessor>> processors;
    processors.emplace_back(std::move(processor));
    auto context = sdk::logs::LoggerContextFactory::Create(std::move(processors), resource);
    std::shared_ptr<logs::LoggerProvider> provider =
      sdk::logs::LoggerProviderFactory::Create(std::move(context));
    logs::Provider::SetLoggerProvider(provider);
  } else {
    auto exporter = std::make_unique<couchbase::observability::fmt_log_exporter>(stderr);
    auto processor = sdk::logs::SimpleLogRecordProcessorFactory::Create(std::move(exporter));
    std::vector<std::unique_ptr<sdk::logs::LogRecordProcessor>> processors;
    processors.emplace_back(std::move(processor));
    auto context = sdk::logs::LoggerContextFactory::Create(std::move(processors));
    std::shared_ptr<logs::LoggerProvider> provider =
      sdk::logs::LoggerProviderFactory::Create(std::move(context));
    logs::Provider::SetLoggerProvider(provider);
  }
}
} // namespace couchbase::observability
