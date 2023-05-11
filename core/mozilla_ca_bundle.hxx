/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <array>
#include <gsl/span>
#include <string_view>

namespace couchbase::core::default_ca
{
struct certificate {
    std::string_view authority;
    std::string_view body;
};

auto
mozilla_ca_certs() -> gsl::span<const certificate>;

auto
mozilla_ca_certs_date() -> std::string_view;

auto
mozilla_ca_certs_sha256() -> std::string_view;
} // namespace couchbase::core::default_ca
