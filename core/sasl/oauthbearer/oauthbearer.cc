/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "oauthbearer.h"

#include "core/sasl/stringutils.h"

#include <spdlog/fmt/bundled/core.h>

namespace couchbase::core::sasl::mechanism::oauthbearer
{
auto
ClientBackend::start() -> std::pair<error, std::string_view>
{
  auto header = std::string{ "n,," };
  header.push_back(0x01);
  header.append(fmt::format("auth=Bearer {}", passwordCallback()));
  header.push_back(0x01);
  header.push_back(0x01);
  client_message = header;
  return { error::OK, client_message };
}

auto
ClientBackend::step(std::string_view /*input*/) -> std::pair<error, std::string_view>
{
  throw std::logic_error("ClientBackend::step(): OAUTHBEARER auth should not call step");
}

} // namespace couchbase::core::sasl::mechanism::oauthbearer
