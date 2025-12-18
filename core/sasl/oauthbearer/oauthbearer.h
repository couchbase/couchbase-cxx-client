/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "core/sasl/client.h"

/// Extremely simple prototype of an OAUTHBEARER mechanism as
/// desceribed in https://datatracker.ietf.org/doc/html/rfc7628 and
/// https://datatracker.ietf.org/doc/html/rfc6750
namespace couchbase::core::sasl::mechanism::oauthbearer
{

class ClientBackend : public MechanismBackend
{
public:
  ClientBackend(GetUsernameCallback& user_cb, GetPasswordCallback& password_cb, ClientContext& ctx)
    : MechanismBackend(user_cb, password_cb, ctx)
  {
  }

  [[nodiscard]] auto get_name() const -> std::string_view override
  {
    return "OAUTHBEARER";
  }

  auto start() -> std::pair<error, std::string_view> override;

  auto start() -> std::pair<error, std::string_view> override;

private:
  std::string client_message;
};
} // namespace couchbase::core::sasl::mechanism::oauthbearer
