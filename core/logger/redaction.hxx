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
 * are opt-in debugging aids rather than something a deployment leaves on.
 *
 * `bin/check-log-annotations` reports log arguments that look sensitive but are not wrapped. It is
 * a heuristic over argument names, so it suggests rather than decides. Two wrappers record a
 * reviewed exclusion:
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
      ctx.advance_to(fmt::format_to(ctx.out(), "<" tag ">"));                                      \
      ctx.advance_to(fmt::formatter<T>::format(redacted.value, ctx));                              \
      return fmt::format_to(ctx.out(), "</" tag ">");                                              \
    }                                                                                              \
  }

COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_user_data, "ud");
COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_meta_data, "md");
COUCHBASE_LOGGER_REDACTION_FORMATTER(redacted_system_data, "sd");

#undef COUCHBASE_LOGGER_REDACTION_FORMATTER

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
