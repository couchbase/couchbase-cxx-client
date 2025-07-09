/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-present Couchbase, Inc.
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

#include "capabilities.hxx"

#include "core/logger/logger.hxx"

namespace couchbase::core
{
auto
configuration_capabilities::supports_operational_client(const cluster_options& options) const
  -> bool
{
  if (options.allow_enterprise_analytics) {
    CB_LOG_DEBUG("Bypassing cluster prod_name check as allow_enterprise_analytics is enabled");
    return true;
  }

  if (!prod_name.has_value()) {
    return true;
  }

  if (prod_name.value().rfind("Couchbase Server", 0) == 0) {
    return true;
  }

  std::string msg = fmt::format("This {} cluster cannot be used with this SDK, which is intended "
                                "for use with operational clusters. ",
                                prod_name.value());
  if (prod_name.value().rfind("Enterprise Analytics", 0) == 0) {
    msg.append("For this cluster, an Enterprise Analytics SDK should be used.");
  }
  CB_LOG_ERROR(msg);

  return false;
}
} // namespace couchbase::core
