/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Present Couchbase, Inc.
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

#include "fmt_log_record_exporter.hxx"

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-builtins"
#endif
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/logs/read_write_log_record.h>
#include <opentelemetry/sdk/logs/recordable.h>
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#elif defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <spdlog/fmt/chrono.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>

#include <spdlog/fmt/bundled/args.h>

#include <cstdio>
#include <memory>

template<>
struct fmt::formatter<opentelemetry::common::SystemTimestamp>
  : fmt::formatter<std::chrono::system_clock::time_point> {

  template<typename FormatContext>
  auto format(const opentelemetry::common::SystemTimestamp& ts, FormatContext& ctx) const
  {
    return fmt::formatter<std::chrono::system_clock::time_point>::format(
      static_cast<std::chrono::system_clock::time_point>(ts), ctx);
  }
};

namespace
{
template<typename IdType>
auto
to_hex(const IdType& id) -> std::string
{
  std::string buffer(2 * IdType::kSize, '0');
  id.ToLowerBase16(buffer);
  return buffer;
}

auto
trim_quotes(const std::string& s) -> std::string_view
{
  size_t start = 0;
  size_t end = s.size();

  if (!s.empty() && s.front() == '"') {
    ++start;
  }
  if (end > start && s.back() == '"') {
    --end;
  }
  return { s.data() + start, end - start };
}

struct log_ids {
  const opentelemetry::trace::TraceId& tid;
  const opentelemetry::trace::SpanId& sid;
};

struct log_body {
  const opentelemetry::common::AttributeValue& fmt_string;
  const std::unordered_map<std::string, opentelemetry::common::AttributeValue>& params;
};
} // namespace

template<>
struct fmt::formatter<log_ids> {
  constexpr auto parse(format_parse_context& ctx) const
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const log_ids& ids, FormatContext& ctx) const
  {
    if (ids.tid.IsValid() && ids.sid.IsValid()) {
      return fmt::format_to(
        ctx.out(), "\t[tid=\"{}\", sid=\"{}\"]", to_hex(ids.tid), to_hex(ids.sid));
    }
    return ctx.out();
  }
};

template<>
struct fmt::formatter<opentelemetry::common::AttributeValue> {
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const opentelemetry::common::AttributeValue& value, FormatContext& ctx) const
  {
    return std::visit(
      [&ctx](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::vector<bool>> ||
                      std::is_same_v<T, std::vector<int32_t>> ||
                      std::is_same_v<T, std::vector<int64_t>> ||
                      std::is_same_v<T, std::vector<uint32_t>> ||
                      std::is_same_v<T, std::vector<double>> ||
                      std::is_same_v<T, std::vector<std::string_view>> ||
                      std::is_same_v<T, std::vector<uint64_t>> ||
                      std::is_same_v<T, std::vector<uint8_t>>) {
          return fmt::format_to(ctx.out(), "[{}]", fmt::join(v, ", "));
        }
        if constexpr (std::is_same_v<T, const char*>) {
          return fmt::format_to(ctx.out(), "\"{}\"", v);
        }
        if constexpr (std::is_same_v<T, std::string_view>) {
          return fmt::format_to(ctx.out(), "\"{}\"", v);
        }
        return fmt::format_to(ctx.out(), "{}", v);
      },
      value);
  }
};

template<>
struct fmt::formatter<std::unordered_map<std::string, opentelemetry::common::AttributeValue>> {
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(
    const std::unordered_map<std::string, opentelemetry::common::AttributeValue>& attributes,
    FormatContext& ctx) const
  {
    if (attributes.empty()) {
      return ctx.out();
    }

    auto out = ctx.out();
    out = fmt::format_to(out, "\t{{");
    for (auto it = attributes.begin(); it != attributes.end(); ++it) {
      if (it != attributes.begin()) {
        out = fmt::format_to(out, ", ");
      }
      out = fmt::format_to(out, "\"{}\": {}", it->first, it->second);
    }
    out = fmt::format_to(out, "}}");
    return out;
  }
};

template<>
struct fmt::formatter<log_body> {
  constexpr auto parse(format_parse_context& ctx) const
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const log_body& body, FormatContext& ctx) const
  {
    std::string quoted_fmt_string = fmt::format("{}", body.fmt_string);
    auto fmt_string = trim_quotes(quoted_fmt_string);

    if (fmt_string.empty()) {
      return ctx.out();
    }

    fmt::dynamic_format_arg_store<fmt::format_context> store;

    for (const auto& [name, value] : body.params) {
      std::visit(
        [&](const auto& v) {
          store.push_back(fmt::arg(name.c_str(), v));
        },
        value);
    }

    auto result = fmt::vformat(fmt_string, store);
    return fmt::format_to(ctx.out(), "\t{}", result);
  }
};

template<>
struct fmt::formatter<opentelemetry::sdk::common::OwnedAttributeValue> {
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const opentelemetry::sdk::common::OwnedAttributeValue& value,
              FormatContext& ctx) const
  {
    return std::visit(
      [&ctx](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::vector<bool>> ||
                      std::is_same_v<T, std::vector<int32_t>> ||
                      std::is_same_v<T, std::vector<int64_t>> ||
                      std::is_same_v<T, std::vector<uint32_t>> ||
                      std::is_same_v<T, std::vector<double>> ||
                      std::is_same_v<T, std::vector<std::string_view>> ||
                      std::is_same_v<T, std::vector<uint64_t>> ||
                      std::is_same_v<T, std::vector<uint8_t>>) {
          return fmt::format_to(ctx.out(), "[{}]", fmt::join(v, ", "));
        }
        if constexpr (std::is_same_v<T, const char*>) {
          return fmt::format_to(ctx.out(), "\"{}\"", v);
        }
        if constexpr (std::is_same_v<T, std::string_view>) {
          return fmt::format_to(ctx.out(), "\"{}\"", v);
        }
        return fmt::format_to(ctx.out(), "{}", v);
      },
      value);
  }
};

template<>
struct fmt::formatter<opentelemetry::sdk::instrumentationscope::InstrumentationScope> {
  constexpr auto parse(format_parse_context& ctx) const
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const opentelemetry::sdk::instrumentationscope::InstrumentationScope& scope,
              FormatContext& ctx) const
  {
    if (const auto& name = scope.GetName(); !name.empty()) {
      auto out = fmt::format_to(ctx.out(), " [");

      const auto& attrs = scope.GetAttributes();
      if (auto attr = attrs.find("process_id"); attr != attrs.end()) {
        out = fmt::format_to(out, "{},", attr->second);
      }
      if (auto attr = attrs.find("thread_id"); attr != attrs.end()) {
        out = fmt::format_to(out, "{},", attr->second);
      }
      return fmt::format_to(out, "{}]", name);
    }
    return ctx.out();
  }
};

namespace couchbase::observability
{

fmt_log_exporter::fmt_log_exporter(FILE* file)
  : file_(file)
{
}

auto
fmt_log_exporter::MakeRecordable() noexcept -> std::unique_ptr<opentelemetry::sdk::logs::Recordable>
{
  return std::make_unique<opentelemetry::sdk::logs::ReadWriteLogRecord>();
}

auto
fmt_log_exporter::Export(
  const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::logs::Recordable>>&
    records) noexcept -> opentelemetry::sdk::common::ExportResult
{
  for (auto& record : records) {
    auto log_record = std::unique_ptr<opentelemetry::sdk::logs::ReadWriteLogRecord>(
      dynamic_cast<opentelemetry::sdk::logs::ReadWriteLogRecord*>(record.release()));

    if (log_record == nullptr) {
      continue;
    }

    fmt::println(file_,
                 "{:%Y-%m-%dT%H:%M:%S%z}{:>7}{}{}{}{}",
                 log_record->GetObservedTimestamp(),
                 log_record->GetSeverityText(),
                 log_record->GetInstrumentationScope(),
                 log_body{ log_record->GetBody(), log_record->GetAttributes() },
                 log_record->GetAttributes(),
                 log_ids{ log_record->GetTraceId(), log_record->GetSpanId() });
  }
  return opentelemetry::sdk::common::ExportResult::kSuccess;
}

auto
fmt_log_exporter::Shutdown(std::chrono::microseconds /* timeout */) noexcept -> bool
{
  return true;
}

auto
fmt_log_exporter::ForceFlush(std::chrono::microseconds /* timeout */) noexcept -> bool
{
  fflush(file_);
  return true;
}
} // namespace couchbase::observability
