/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// What a test case and its requirements are handed: the configuration the run was started with,
// the process environment, and a small set of cached probes describing the server.
//
// This header stays lean on purpose: standard library only, and not a formatting library either.
// Every probe returns a plain type, so nothing here names a core or public SDK type, and a test
// file that only needs to *describe* its requirements pays no compile cost for the client library.

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::test
{

// Thrown by a probe that could not answer. The runner turns one escaping a requirement into
// `undetermined`, so a requirement never handles errors itself.
class probe_failure : public std::exception
{
public:
  explicit probe_failure(std::string message)
    : message_{ std::move(message) }
  {
  }
  [[nodiscard]] auto what() const noexcept -> const char* override
  {
    return message_.c_str();
  }

private:
  std::string message_;
};

// Thrown by a probe whose backend could not be opened at all, as against one the cluster could not
// answer. Every probe in the process shares that outcome: the connection lives in the backend, so
// an endpoint that refused one refuses the next, and the answer caches are keyed per probe kind. A
// backend that opened and then could not answer one question throws plain probe_failure, or that
// one question would speak for the rest.
class probe_backend_unavailable : public probe_failure
{
public:
  using probe_failure::probe_failure;
};

enum class server_edition : std::uint8_t {
  unknown,
  enterprise,
  community,
  columnar,
};

enum class deployment_type : std::uint8_t {
  on_prem,
  capella,
  elixir,
};

// The lean mirror of test::utils::server_version, carrying what a requirement can ask about and
// nothing else. Ordered by (major, minor, micro) so a version range is a pair of comparisons.
struct server_version {
  std::uint32_t major{ 0 };
  std::uint32_t minor{ 0 };
  std::uint32_t micro{ 0 };
  bool developer_preview{ false };
  server_edition edition{ server_edition::unknown };
  deployment_type deployment{ deployment_type::on_prem };

  [[nodiscard]] auto to_string() const -> std::string
  {
    return std::to_string(major) + '.' + std::to_string(minor) + '.' + std::to_string(micro);
  }

  [[nodiscard]] friend auto operator<(const server_version& a, const server_version& b) -> bool
  {
    if (a.major != b.major) {
      return a.major < b.major;
    }
    if (a.minor != b.minor) {
      return a.minor < b.minor;
    }
    return a.micro < b.micro;
  }
  [[nodiscard]] friend auto operator>=(const server_version& a, const server_version& b) -> bool
  {
    return !(a < b);
  }
};

// Version constants the requirement vocabulary names, so a case says which release introduced the
// behaviour it pins rather than repeating a number nobody can attribute later.
inline constexpr server_version v6_5{ 6, 5, 0 };
inline constexpr server_version v7_0{ 7, 0, 0 };
inline constexpr server_version v7_1{ 7, 1, 0 };
inline constexpr server_version v7_2{ 7, 2, 0 };
inline constexpr server_version v7_6{ 7, 6, 0 };
inline constexpr server_version v8_0{ 8, 0, 0 };

// Everything the run was started with, resolved once before any case executes. A test body reads
// these rather than the environment, so what a case depends on is visible here instead of being
// spread over getenv calls.
struct configuration {
  std::string connection_string{};
  std::string username{ "Administrator" };
  std::string password{ "password" };
  std::string bucket{ "default" };
  std::string other_bucket{ "secBucket" };
  // Set iff TEST_CONNECTION_STRING is set and non-empty. Distinct from "a cluster answered": an
  // endpoint that is configured but unreachable is a failure, never a skip.
  bool cluster_configured{ false };
  // The gocaves mock, driven by TEST_USE_GOCAVES / CB_USE_GOCAVES the way
  // bin/run-integration-tests already sets them.
  bool mock{ false };
  // Upper bound on a case's whole requirement phase, which runs before every case in the binary.
  // Distinct from the case's own budget: this one bounds the probing, that one the work, and
  // CB_TEST_TIMEOUT_MULTIPLIER scales both. A requirement that blocks anyway costs its case this
  // much and nothing more.
  std::chrono::milliseconds requirement_budget{ std::chrono::seconds{ 60 } };

  [[nodiscard]] static auto from_environment() -> configuration;
};

// The probes, behind an interface so the lean framework does not link the client library. Exactly
// one implementation is linked into a test executable: the null one, whose every probe reports it
// has no cluster to ask, or the cluster one, which drives a real connection.
class probe_backend
{
public:
  probe_backend() = default;
  probe_backend(const probe_backend&) = delete;
  auto operator=(const probe_backend&) -> probe_backend& = delete;
  virtual ~probe_backend() = default;

  [[nodiscard]] virtual auto server_version() -> couchbase::test::server_version = 0;
  [[nodiscard]] virtual auto has_service(const std::string& name) -> bool = 0;
  [[nodiscard]] virtual auto has_bucket_capability(const std::string& capability) -> bool = 0;
  [[nodiscard]] virtual auto number_of_replicas() -> std::size_t = 0;
  [[nodiscard]] virtual auto number_of_nodes() -> std::size_t = 0;
  [[nodiscard]] virtual auto server_groups() -> std::vector<std::string> = 0;
  [[nodiscard]] virtual auto storage_backend() -> std::string = 0;
};

// Defined by whichever backend library the executable links (see cmake/TestFramework.cmake). The
// context calls it on the first probe, never at startup: a test that asks nothing of the server
// opens no connection.
[[nodiscard]] auto
make_probe_backend(const configuration& config) -> std::unique_ptr<probe_backend>;

// Portable std::getenv wrapper returning std::nullopt for unset *or* empty values, matching the
// wrappers in tools/utils.cxx and examples/external_circuit_breaker. Needed because MSVC treats
// plain getenv() as deprecated, and the test tree builds with /W4 /WX. Lives here so every test
// executable shares one implementation; context::env() is the same thing reached through a case's
// own context.
[[nodiscard]] auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>;

class context
{
public:
  explicit context(configuration config);
  // For the framework's own tests, which drive the probes with a stand-in backend.
  context(configuration config, std::unique_ptr<probe_backend> backend);
  context(const context&) = delete;
  auto operator=(const context&) -> context& = delete;
  ~context();

  [[nodiscard]] auto config() const -> const configuration&;

  // The process environment, so a case that genuinely needs a variable reads it from here and the
  // run can report what it consulted.
  [[nodiscard]] auto env(const std::string& name) const -> std::optional<std::string>;

  // Probes. Each is cached for the lifetime of the process -- a binary with 300 cases must not
  // open 300 connections -- and each throws probe_failure when it cannot answer. A backend that
  // reports it could not be opened is asked nothing further, by any of them.
  [[nodiscard]] auto server_version() -> couchbase::test::server_version;
  [[nodiscard]] auto has_service(const std::string& name) -> bool;
  [[nodiscard]] auto has_bucket_capability(const std::string& capability) -> bool;
  [[nodiscard]] auto number_of_replicas() -> std::size_t;
  [[nodiscard]] auto number_of_nodes() -> std::size_t;
  [[nodiscard]] auto server_groups() -> std::vector<std::string>;
  [[nodiscard]] auto storage_backend() -> std::string;

  // How many times a backend has been created. The caching above is what keeps a suite from
  // reconnecting per case, and a number is the only way to hold that claim to account.
  [[nodiscard]] auto backends_created() const -> std::size_t;

private:
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace couchbase::test
