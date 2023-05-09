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

#include "tls_verify_mode.hxx"

#include <optional>
#include <string>

namespace couchbase
{
class security_options
{
  public:
    auto enabled(bool enabled) -> security_options&
    {
        enabled_ = enabled;
        return *this;
    }

    auto tls_verify(tls_verify_mode mode) -> security_options&
    {
        tls_verify_ = mode;
        return *this;
    }

    auto trust_certificate(std::string certificate_path) -> security_options&
    {
        trust_certificate_ = certificate_path;
        return *this;
    }

    struct built {
        bool enabled;
        tls_verify_mode tls_verify;
        std::optional<std::string> trust_certificate;
        bool disable_mozilla_ca_certificates;
    };

    [[nodiscard]] auto build() const -> built
    {
        return {
            enabled_,
            tls_verify_,
            trust_certificate_,
            disable_mozilla_ca_certificates_,
        };
    }

  private:
    bool enabled_{ true };
    tls_verify_mode tls_verify_{ tls_verify_mode::peer };
    std::optional<std::string> trust_certificate_{};
    bool disable_mozilla_ca_certificates_{ false };
};
} // namespace couchbase
