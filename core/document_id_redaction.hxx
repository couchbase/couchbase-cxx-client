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
 * The logging formatter for a document id, kept out of document_id_fmt.hxx so that the plain
 * formatter stays free of the logger. That header is included by transaction internals, a subdoc
 * test and the FIT performer, none of which log a document id; only the handful of files that pass
 * one to a CB_LOG_* statement include this.
 */

#pragma once

#include "document_id_fmt.hxx"
#include "logger/redaction.hxx"

#include <spdlog/fmt/bundled/core.h>

namespace couchbase::core::logger
{
struct redacted_document_id {
  const document_id& value;
};

/**
 * Tag a document id for logging. It renders as bucket/scope.collection/key, and those parts do not
 * share a redaction category: the names are metadata and only the key is user data. Wrapping the
 * rendered form as a whole would put a bucket name inside a <ud> span, and the span would hash to
 * something that matches no other log line.
 *
 * The plain formatter in document_id_fmt.hxx is left alone on purpose, since it is also used
 * outside logging.
 */
inline auto
document(const document_id& value) -> redacted_document_id
{
  return { value };
}
} // namespace couchbase::core::logger

template<>
struct fmt::formatter<couchbase::core::logger::redacted_document_id> {
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const couchbase::core::logger::redacted_document_id& redacted,
              FormatContext& ctx) const
  {
    namespace logger = couchbase::core::logger;
    const auto& id = redacted.value;
    // An id built without a collection carries an empty collection path, and renders with an empty
    // middle component. Keep that shape rather than joining an empty scope and collection.
    if (id.collection_path().empty()) {
      return format_to(
        ctx.out(), "{}//{}", logger::metadata(id.bucket()), logger::user_data(id.key()));
    }
    return format_to(ctx.out(),
                     "{}/{}.{}/{}",
                     logger::metadata(id.bucket()),
                     logger::metadata(id.scope()),
                     logger::metadata(id.collection()),
                     logger::user_data(id.key()));
  }
};
