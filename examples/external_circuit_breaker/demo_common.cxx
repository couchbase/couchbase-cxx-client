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

#include "demo_common.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdlib>

#if defined(_WIN32)
#include <cstdio>
#endif

namespace example::demo
{

auto
is_breaker_failure(const couchbase::error& err) -> bool
{
  using couchbase::errc::common;
  if (!err) {
    return false;
  }
  if (err.ec() == common::ambiguous_timeout || err.ec() == common::unambiguous_timeout ||
      err.ec() == common::temporary_failure || err.ec() == common::service_not_available ||
      err.ec() == common::internal_server_failure || err.ec() == common::request_canceled) {
    return true;
  }
  return false;
}

auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>
{
  if (name.empty()) {
    return std::nullopt;
  }
#if defined(_WIN32)
  char* buf = nullptr;
  size_t len = 0;
  if (_dupenv_s(&buf, &len, name.c_str()) == 0 && buf != nullptr) {
    std::string value(buf);
    std::free(buf);
    if (!value.empty()) {
      return value;
    }
  }
  return std::nullopt;
#else
  if (const char* val = std::getenv(name.c_str())) { // NOLINT(concurrency-mt-unsafe)
    if (val[0] != '\0') {
      return std::string(val);
    }
  }
  return std::nullopt;
#endif
}

} // namespace example::demo
