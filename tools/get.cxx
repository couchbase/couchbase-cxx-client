/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "get.hxx"

#include "utils.hxx"

#include <core/logger/logger.hxx>
#include <core/meta/version.hxx>
#include <core/tracing/otel_tracer.hxx>

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <observability/fmt_log_record_exporter.hxx>

#include <opentelemetry/logs/provider.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span_startoptions.h>
#include <opentelemetry/trace/tracer_provider.h>

#include <opentelemetry/sdk/logs/logger.h>
#include <opentelemetry/sdk/logs/logger_context_factory.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>

#include <opentelemetry/exporters/ostream/log_record_exporter.h>
#include <opentelemetry/exporters/ostream/log_record_exporter_factory.h>

#include <opentelemetry/exporters/otlp/otlp_environment.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h>

#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/chrono.h>
#include <tao/json.hpp>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>

#include <cstdint>
#include <memory>

#include <thread>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#if defined(_WIN32)
#include <processthreadsapi.h>
#include <vector>
#include <windows.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__linux__)
#include <pthread.h>
#endif

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
  return static_cast<int>(::getpid());
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
  return {};
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
  return {};
#else
  return {};
#endif
}

} // namespace

namespace cbc
{
namespace
{
void
init_otel_tracer()
{
  opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;
  auto exporter = opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(opts);
  auto processor =
    opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
  auto resource = opentelemetry::sdk::resource::Resource::Create({
    { "service.name", "cbc" },
  });
  auto provider =
    opentelemetry::sdk::trace::TracerProviderFactory::Create(std::move(processor), resource);
  opentelemetry::trace::Provider::SetTracerProvider(
    std::shared_ptr<opentelemetry::trace::TracerProvider>{ std::move(provider) });
}

class get_app : public CLI::App
{
public:
  get_app()
    : CLI::App{ "Retrieve document from the server.", "get" }
  {
    add_option("id", ids_, "IDs of the documents to retrieve.")->required(true);
    add_flag("--verbose", verbose_, "Include more context and information where it is applicable.");
    add_option("--bucket-name", bucket_name_, "Name of the bucket.")
      ->default_val(default_bucket_name);
    add_option("--scope-name", scope_name_, "Name of the scope.")
      ->default_val(couchbase::scope::default_name);
    add_option("--collection-name", collection_name_, "Name of the collection.")
      ->default_val(couchbase::collection::default_name);
    add_flag("--inlined-keyspace",
             inlined_keyspace_,
             "Extract bucket, scope, collection and key from the IDs (captures will be done with "
             "/^(.*?):(.*?)\\.(.*?):(.*)$/).");
    add_flag("--with-expiry", with_expiry_, "Return document expiry time, if set.");
    add_option("--project",
               projections_,
               fmt::format("Return only part of the document, that corresponds given JSON-pointer "
                           "(could be used multiple times)."))
      ->allow_extra_args(false);
    add_flag("--hexdump",
             hexdump_,
             "Print value using hexdump encoding (safe for binary data on STDOUT).");
    add_flag("--pretty-json",
             pretty_json_,
             "Try to pretty-print as JSON value (prints AS-IS if the document is not a JSON).");
    add_flag("--json-lines",
             json_lines_,
             "Use JSON Lines format (https://jsonlines.org) to print results.");

    add_flag("--use-http-logger", use_http_logger_, "Use HTTP logger instead of ostream.");

    add_common_options(this, common_options_);
    allow_extras(true);
  }

  [[nodiscard]] auto get_otel_tracer() const -> std::shared_ptr<opentelemetry::trace::Tracer>
  {
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer("cbc", couchbase::core::meta::sdk_semver());
  }

  [[nodiscard]] auto get_otel_logger() const -> std::shared_ptr<opentelemetry::logs::Logger>
  {
    thread_local auto logger = opentelemetry::logs::Provider::GetLoggerProvider()->GetLogger(
      "cbc_logger",
      "cxxcbc.cbc",
      couchbase::core::meta::sdk_semver(),
      "",
      {
        { "process_id", process_id() },
        { "thread_id", thread_id() },
        { "thread_name", thread_name() },
      });
    return logger;
  }

  [[nodiscard]] auto execute() const -> int
  {
    apply_logger_options(common_options_.logger);

    auto cluster_options = build_cluster_options(common_options_);

    init_otel_tracer();
    init_otel_logger();

    couchbase::get_options common_get_options{};
    if (with_expiry_) {
      common_get_options.with_expiry(true);
    }
    if (!projections_.empty()) {
      common_get_options.project(projections_);
    }

    const auto connection_string = common_options_.connection.connection_string;

    auto logger = get_otel_logger();
    auto tracer = get_otel_tracer();

    cluster_options.tracing().tracer(couchbase::core::tracing::otel_request_tracer::wrap(tracer));

    auto [connect_err, cluster] =
      couchbase::cluster::connect(connection_string, cluster_options).get();
    if (connect_err) {
      fail(fmt::format(
        "Failed to connect to the cluster at \"{}\": {}", connection_string, connect_err));
    }

    {
      auto top_level_span = tracer->StartSpan("cbc.get-batch",
                                              {
                                                { "number_of_documents", ids_.size() },
                                              });
      for (const auto& id : ids_) {
        auto bucket_name = bucket_name_;
        auto scope_name = scope_name_;
        auto collection_name = collection_name_;
        auto document_id = id;

        if (inlined_keyspace_) {
          if (auto keyspace_with_id = extract_inlined_keyspace(id); keyspace_with_id) {
            bucket_name = keyspace_with_id->bucket_name;
            scope_name = keyspace_with_id->scope_name;
            collection_name = keyspace_with_id->collection_name;
            document_id = keyspace_with_id->id;
          }
        }

        auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

        opentelemetry::trace::StartSpanOptions span_options;
        span_options.parent = top_level_span->GetContext();
        auto span = tracer->StartSpan("cbc.get",
                                      {
                                        { "cbc.bucket", bucket_name },
                                        { "cbc.scope", scope_name },
                                        { "cbc.collection", collection_name },
                                      },
                                      span_options);
        auto get_options = common_get_options;
        get_options.parent_span(couchbase::core::tracing::otel_request_span::wrap(span));
        auto [err, resp] = collection.get(document_id, get_options).get();
        logger->Warn("");
        logger->Warn("this is the message error");
        logger->Warn("this is the message error: {msg}",
                     opentelemetry::common::MakeAttributes({
                       { "msg", err.ec().message() },
                     }));
        logger->Warn("this is the message error: {msg} and some context",
                     opentelemetry::common::MakeAttributes({
                       { "msg", err.ec().message() },
                     }),
                     span->GetContext());
        logger->Error("this is the message error: {}", err.ec().message(), span->GetContext());

        if (json_lines_) {
          print_result_json_line(bucket_name, scope_name, collection_name, document_id, err, resp);
        } else {
          print_result(bucket_name, scope_name, collection_name, document_id, err, resp);
        }
      }
    }

    cluster.close().get();

    return 0;
  }

private:
  void print_result_json_line(const std::string& bucket_name,
                              const std::string& scope_name,
                              const std::string& collection_name,
                              const std::string& document_id,
                              const couchbase::error& err,
                              const couchbase::get_result& resp) const
  {
    tao::json::value line = tao::json::empty_object;
    tao::json::value meta = {
      { "bucket_name", bucket_name },
      { "scope_name", scope_name },
      { "collection_name", collection_name },
      { "document_id", document_id },
    };
    if (err.ec()) {
      line["error"] = fmt::format("{}", err);
    } else {
      auto [value, flags] = resp.content_as<passthrough_transcoder>();
      meta["cas"] = fmt::format("0x{}", resp.cas());
      meta["flags"] = flags;
      if (const auto& expiry = resp.expiry_time(); expiry) {
        meta["expiry_time"] = fmt::format("{}", expiry.value());
      }
      try {
        line["json"] = couchbase::core::utils::json::parse_binary(value);
      } catch (const tao::pegtl::parse_error&) {
        line["base64"] = value;
      }
    }
    line["meta"] = meta;
    fmt::print(stdout, "{}\n", tao::json::to_string<tao::json::events::binary_to_base64>(line));
    (void)fflush(stdout);
  }

  void print_result(const std::string& bucket_name,
                    const std::string& scope_name,
                    const std::string& collection_name,
                    const std::string& document_id,
                    const couchbase::error& err,
                    const couchbase::get_result& resp) const
  {
    const std::string prefix = fmt::format("bucket: {}, collection: {}.{}, id: {}",
                                           bucket_name,
                                           scope_name,
                                           collection_name,
                                           document_id);
    (void)fflush(stderr);
    if (err.ec()) {
      fmt::print(stderr, "{}, error: {}\n", prefix, err.ec().message());
      if (verbose_) {
        fmt::print(stderr, "{}\n", err.ctx().to_json());
      }
    } else {
      auto [value, flags] = resp.content_as<passthrough_transcoder>();
      if (const auto& exptime = resp.expiry_time(); exptime.has_value()) {
        fmt::print(stderr,
                   "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}, expiry: {}\n",
                   prefix,
                   value.size(),
                   resp.cas(),
                   flags,
                   exptime.value());
      } else {
        fmt::print(stderr,
                   "{}, size: {}, CAS: 0x{}, flags: 0x{:08x}\n",
                   prefix,
                   value.size(),
                   resp.cas(),
                   flags);
      }
      (void)fflush(stderr);
      (void)fflush(stdout);
      if (hexdump_) {
        auto hex = fmt::format("{:a}", spdlog::to_hex(value));
        fmt::print(stdout, "{}\n", std::string_view(hex.data() + 1, hex.size() - 1));
      } else if (pretty_json_) {
        try {
          auto json = couchbase::core::utils::json::parse_binary(value);
          fmt::print(stdout, "{}\n", tao::json::to_string(json, 2));
        } catch (const tao::pegtl::parse_error&) {
          fmt::print(stdout,
                     "{}\n",
                     std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
        }
      } else {
        fmt::print(stdout,
                   "{}\n",
                   std::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
      }
      (void)fflush(stdout);
    }
  }

  void init_otel_logger() const
  {
    using namespace opentelemetry;

    if (use_http_logger_) {
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

  common_options common_options_{};

  std::string bucket_name_{ default_bucket_name };
  std::string scope_name_{ couchbase::scope::default_name };
  std::string collection_name_{ couchbase::collection::default_name };
  std::vector<std::string> projections_{};
  bool with_expiry_{ false };
  bool inlined_keyspace_{ false };
  bool hexdump_{ false };
  bool pretty_json_{ false };
  bool json_lines_{ false };
  bool verbose_{ false };

  bool use_http_logger_{ false };

  std::vector<std::string> ids_{};
};
} // namespace

auto
make_get_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<get_app>();
}

auto
execute_get_command(const CLI::App* app) -> int
{
  if (const auto* get = dynamic_cast<const get_app*>(app); get != nullptr) {
    return get->execute();
  }
  return 1;
}
} // namespace cbc
