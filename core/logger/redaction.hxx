/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

/*
 * Log redaction annotations.
 *
 * The SDK never removes or obscures anything itself. It wraps sensitive values in fixed tags so
 * that an external tool can strip or hash them after the fact. Three categories exist:
 *
 *   <ud>...</ud>  user data    document keys and values, application usernames, query statements,
 *                              email addresses, document xattrs
 *   <md>...</md>  metadata     cluster names, bucket/scope/collection names, design document,
 *                              view and index names, replication stream names
 *   <sd>...</sd>  system data  IP addresses, hostnames, ports, DNS topology
 *
 * Usage at a log site:
 *
 *   CB_LOG_INFO("connecting to {}", logger::system_data(host));
 *   CB_LOG_DEBUG("fetching {}", logger::user_data(key));
 *
 * A list of values gets one tag per entry rather than one around the whole list, so that an entry
 * still matches the same value logged on its own elsewhere. There is a list form of each category,
 * and it also renders the double quotes some of these lines already carry, because a tag has to sit
 * inside them:
 *
 *   CB_LOG_INFO("bootstrap nodes: [{}]",
 *               logger::system_data_list(addresses, logger::list_entries::quoted));
 *
 * A serialized document goes the other way: annotate the whole argument, or none of it, but never
 * a value inside it. A tag written into a JSON string value is read back as part of that value by
 * anything that parses the line, and some of what this SDK logs has a shape other tools consume --
 * SDK RFC 0067 fixes the threshold and orphan reports field by field. So the tag sits outside the
 * document, which leaves the document itself byte-identical:
 *
 *   CB_LOG_WARNING("Operations over threshold: {}",
 *                  logger::system_data(utils::json::generate(report)));
 *
 * One tag then has to carry values that may span categories, so pick the strictest category any
 * value inside the document could belong to. The whole document hashes as a single token either
 * way, so what a coarse span costs is diagnosability, not protection. "None of it" is the other
 * legitimate answer, and it is the one the configuration dumps below take.
 *
 * A tagged value must never contain a newline. The tool that consumes these tags is line-oriented,
 * so a value that spans lines leaves its opening tag and its closing tag on different lines; the
 * tool reports an unmatched tag and passes the value through in clear text, which is the one
 * failure mode this whole mechanism exists to prevent. The wrappers escape newlines and carriage
 * returns to "\n" and "\r" while redaction is enabled, so no call site has to remember: the
 * values most likely to span lines are HTTP bodies and serialized documents, which is to say
 * exactly the ones that must not leak. The exclusion markers below escape nothing, because the
 * whole point of not_redacted() is to show the bytes as they are.
 *
 * One shape stays untagged for the opposite reason. A hex dump written with the "{:a}" spec
 * renders as a multi-line grid, so escaping would keep the tag intact but flatten the dump into a
 * single line of "\n" literals, and a flattened hex dump is not a hex dump. Those sites say so and
 * use not_redacted(); the checker rejects a tag around one.
 *
 * `bin/check-log-annotations` cannot help with the general case. Whether a value contains a
 * newline is a property of the value at run time, not of the source.
 *
 * Qualify the calls as above. In particular `metadata` is also a member function on several core
 * types (query_result, row_streamer, transaction_get_result), where an unqualified call would
 * resolve to the member instead. In a file whose enclosing namespace is `couchbase` rather than
 * `couchbase::core`, write `core::logger::` instead: a plain `logger::` there finds the public
 * `couchbase::logger` from <couchbase/logger.hxx>, which has none of these functions.
 *
 * When redaction is disabled the wrappers format exactly as the underlying value, so annotating a
 * log statement never changes its output for users who have not opted in.
 *
 * These wrappers hold a reference to the value and are only valid within the enclosing
 * full-expression. That is always the case inside a CB_LOG_* macro. Do not store one:
 *
 *   auto tagged = logger::user_data(build_key());  // DANGLING - build_key() is destroyed here
 *
 * Note that CB_LOG_*_RAW bypasses fmt entirely and cannot carry annotations, and CB_LOG_PROTOCOL
 * is deliberately not redacted. The configuration dumps behind the dump_configuration option are
 * left unannotated for the same reason: both exist to show exactly what crossed the wire, and both
 * are opt-in debugging aids rather than something a deployment leaves on. Each of those sites says
 * so with logger::not_redacted(), so the choice is visible where the dump is written.
 *
 * `bin/check-log-annotations` reports log arguments that look sensitive but are not wrapped. It is
 * a heuristic over argument names, so it suggests rather than decides. It cannot see a value that
 * was tagged anywhere other than the log site, which is why the list forms above exist as named
 * wrappers rather than as a helper per file: the checker recognises the name and counts the
 * argument as annotated, instead of having to be told to look away from it.
 *
 * Two wrappers record a reviewed exclusion:
 *
 *   logger::not_sensitive(value)  the identifier suggests a category the value does not belong to
 *   logger::not_redacted(value)   sensitive, and deliberately logged untagged anyway
 *
 * Neither changes what is printed. Prefer them to the `no-redact` comment the checker also honours,
 * which silences a whole log statement and so also silences arguments added to it later.
 */

#pragma once

#include "logger.hxx"

#include <spdlog/fmt/bundled/core.h>
#include <spdlog/fmt/bundled/format.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>

namespace couchbase::core::logger
{

template<typename T>
struct redacted_user_data {
  const T& value;
};

template<typename T>
struct redacted_meta_data {
  const T& value;
};

template<typename T>
struct redacted_system_data {
  const T& value;
};

template<typename T>
struct conditionally_redacted_user_data {
  const T& value;
  bool sensitive;
};

template<typename T>
struct not_sensitive_value {
  const T& value;
};

template<typename T>
struct not_redacted_value {
  const T& value;
};

/**
 * Tag a value as user data (<ud>): document keys and values, application usernames, query
 * statements, email addresses, document xattrs.
 */
template<typename T>
auto
user_data(const T& value)
{
  return redacted_user_data<T>{ value };
}

/**
 * Tag a value as metadata (<md>): cluster, bucket, scope and collection names, design document,
 * view and index names, and other Couchbase resource names.
 */
template<typename T>
auto
metadata(const T& value)
{
  return redacted_meta_data<T>{ value };
}

/**
 * Tag a value as system data (<sd>): IP addresses, hostnames, ports and DNS topology.
 */
template<typename T>
auto
system_data(const T& value)
{
  return redacted_system_data<T>{ value };
}

/**
 * Tag a value as user data (<ud>) only when `sensitive` holds, and print it unchanged otherwise.
 *
 * For an argument that is sensitive on one branch of a log statement and a constant on another.
 * Tagging the constant hashes it to a fixed digest, which protects nothing -- the domain is one
 * value, so anyone holding the salt recovers it immediately -- and costs the reader the diagnostic
 * the constant was there to give. A body the SDK elided to "[hidden]" and a body it redacted are
 * then indistinguishable in the output.
 */
template<typename T>
auto
user_data_if(bool sensitive, const T& value)
{
  return conditionally_redacted_user_data<T>{ value, sensitive };
}

/**
 * Mark a value the annotation checker flags but that is not sensitive, because the identifier
 * suggests a category the value does not belong to. Formats exactly as the value would unwrapped,
 * so this is an assertion for the reader and for the checker rather than a change in behaviour.
 */
template<typename T>
auto
not_sensitive(const T& value)
{
  return not_sensitive_value<T>{ value };
}

/**
 * Mark a sensitive value that is deliberately logged without a tag, because wrapping it here is
 * either not possible or not wanted. Formats exactly as the value would unwrapped. Unlike
 * not_sensitive() this asserts nothing about the content, so say at the call site why the value
 * carries no tag, and reference a ticket when the answer is that it eventually should.
 */
template<typename T>
auto
not_redacted(const T& value)
{
  return not_redacted_value<T>{ value };
}

/**
 * Whether the entries of a tagged list render inside double quotes. The tag sits inside the quotes,
 * never around them: a span that swallows the punctuation hashes to something matching no other
 * line in the log.
 */
enum class list_entries {
  bare,
  quoted,
};

namespace detail
{
/*
 * Replace the characters that would split a tagged value across log lines. The consumer of these
 * tags is line-oriented: an embedded newline leaves the opening tag and the closing tag on
 * different lines, and the value is then passed through untouched instead of being redacted.
 */
template<typename OutputIt>
auto
write_escaped(OutputIt out, std::string_view value) -> OutputIt
{
  std::size_t copied = 0;
  for (std::size_t index = 0; index < value.size(); ++index) {
    std::string_view replacement{};
    switch (value[index]) {
      case '\n':
        replacement = "\\n";
        break;
      case '\r':
        replacement = "\\r";
        break;
      default:
        continue;
    }
    const auto begin = value.begin() + static_cast<std::ptrdiff_t>(copied);
    out = std::copy(begin, value.begin() + static_cast<std::ptrdiff_t>(index), out);
    out = std::copy(replacement.begin(), replacement.end(), out);
    copied = index + 1;
  }
  return std::copy(value.begin() + static_cast<std::ptrdiff_t>(copied), value.end(), out);
}

template<typename Container, typename Wrap>
auto
tagged_list(const Container& values, list_entries entries, std::string_view separator, Wrap wrap)
  -> std::string
{
  std::string result;
  auto first = true;
  for (const auto& value : values) {
    if (!first) {
      result.append(separator);
    }
    first = false;
    if (entries == list_entries::quoted) {
      result.append(fmt::format(R"("{}")", wrap(value)));
    } else {
      result.append(fmt::format("{}", wrap(value)));
    }
  }
  return result;
}
} // namespace detail

/*
 * Tag every entry of a container and join them, so a list correlates entry by entry. One tag around
 * the joined string would hash the whole list as a single token, matching nothing else in the log,
 * not even the same value logged beside it in a span of its own.
 *
 * Unlike the wrappers above, these return a std::string rather than a view of the value, because
 * there is nothing to view: the tagged entries do not exist until they are built. Call them inside
 * the CB_LOG_* argument list, so they stay as lazy as the statement that needs them.
 */

/**
 * Tag each entry of a container as user data (<ud>) and join them.
 */
template<typename Container>
auto
user_data_list(const Container& values,
               list_entries entries = list_entries::bare,
               std::string_view separator = ", ") -> std::string
{
  return detail::tagged_list(values, entries, separator, [](const auto& value) {
    return user_data(value);
  });
}

/**
 * Tag each entry of a container as metadata (<md>) and join them.
 */
template<typename Container>
auto
metadata_list(const Container& values,
              list_entries entries = list_entries::bare,
              std::string_view separator = ", ") -> std::string
{
  return detail::tagged_list(values, entries, separator, [](const auto& value) {
    return metadata(value);
  });
}

/**
 * Tag each entry of a container as system data (<sd>) and join them.
 */
template<typename Container>
auto
system_data_list(const Container& values,
                 list_entries entries = list_entries::bare,
                 std::string_view separator = ", ") -> std::string
{
  return detail::tagged_list(values, entries, separator, [](const auto& value) {
    return system_data(value);
  });
}

} // namespace couchbase::core::logger

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
/*
 * The formatters inherit parse() from the underlying formatter, so format specifications keep
 * working. Note that a specification applies to the value only, so "{:>10}" pads inside the tags.
 */
#define COUCHBASE_LOGGER_REDACTION_FORMATTER(wrapper, tag)                                         \
  template<typename T>                                                                             \
  struct fmt::formatter<couchbase::core::logger::wrapper<T>> : fmt::formatter<T> {                 \
    template<typename FormatContext>                                                               \
    auto format(const couchbase::core::logger::wrapper<T>& redacted, FormatContext& ctx) const     \
    {                                                                                              \
      if (!couchbase::core::logger::is_log_redaction_enabled()) {                                  \
        return fmt::formatter<T>::format(redacted.value, ctx);                                     \
      }                                                                                            \
      fmt::memory_buffer buffer;                                                                   \
      fmt::basic_format_context<fmt::appender, char> buffered{ fmt::appender(buffer), {} };        \
      fmt::formatter<T>::format(redacted.value, buffered);                                         \
      ctx.advance_to(fmt::format_to(ctx.out(), "<" tag ">"));                                      \
      ctx.advance_to(couchbase::core::logger::detail::write_escaped(                               \
        ctx.out(), std::string_view{ buffer.data(), buffer.size() }));                             \
      return fmt::format_to(ctx.out(), "</" tag ">");                                              \
    }                                                                                              \
  }

COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_user_data, "ud");
COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_meta_data, "md");
COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_system_data, "sd");

#undef COUCHBASE_LOGGER_REDACTION_FORMATTER

/*
 * Reuses the <ud> formatter for the tagged branch rather than repeating it, so the two cannot
 * drift apart on escaping or on what a disabled redaction level prints.
 */
template<typename T>
struct fmt::formatter<couchbase::core::logger::conditionally_redacted_user_data<T>>
  : fmt::formatter<couchbase::core::logger::redacted_user_data<T>> {
  template<typename FormatContext>
  auto format(const couchbase::core::logger::conditionally_redacted_user_data<T>& redacted,
              FormatContext& ctx) const
  {
    if (!redacted.sensitive) {
      return fmt::formatter<T>::format(redacted.value, ctx);
    }
    return fmt::formatter<couchbase::core::logger::redacted_user_data<T>>::format(
      couchbase::core::logger::redacted_user_data<T>{ redacted.value }, ctx);
  }
};

/*
 * The exclusion markers print their value and nothing else, whether or not redaction is enabled.
 */
#define COUCHBASE_LOGGER_EXCLUSION_FORMATTER(wrapper)                                              \
  template<typename T>                                                                             \
  struct fmt::formatter<couchbase::core::logger::wrapper<T>> : fmt::formatter<T> {                 \
    template<typename FormatContext>                                                               \
    auto format(const couchbase::core::logger::wrapper<T>& marked, FormatContext& ctx) const       \
    {                                                                                              \
      return fmt::formatter<T>::format(marked.value, ctx);                                         \
    }                                                                                              \
  }

COUCHBASE_LOGGER_EXCLUSION_FORMATTER(not_sensitive_value);
COUCHBASE_LOGGER_EXCLUSION_FORMATTER(not_redacted_value);

#undef COUCHBASE_LOGGER_EXCLUSION_FORMATTER
#endif
