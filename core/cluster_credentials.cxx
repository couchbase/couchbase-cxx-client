/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "cluster_credentials.hxx"

namespace couchbase::core
{
auto
cluster_credentials::uses_certificate() const -> bool
{
  return !certificate_path.empty();
}

auto
cluster_credentials::requires_tls() const -> bool
{
  return uses_certificate() || uses_jwt();
}

auto
cluster_credentials::uses_jwt() const -> bool
{
  return !jwt_token.empty();
}

auto
cluster_credentials::uses_password() const -> bool
{
  return !username.empty() && !password.empty();
}

auto
cluster_credentials::is_same_type(const cluster_credentials& other) const -> bool
{
  return uses_certificate() == other.uses_certificate() && uses_jwt() == other.uses_jwt() &&
         uses_password() == other.uses_password();
}
} // namespace couchbase::core
