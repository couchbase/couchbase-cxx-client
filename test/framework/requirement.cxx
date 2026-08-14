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

#include "requirement.hxx"

// This file formats every requirement description, so it asks for fmt itself rather than
// relying on a header it includes happening to carry it.
#include <spdlog/fmt/fmt.h>

#include <functional>
#include <optional>
#include <utility>

namespace couchbase::test::needs
{
namespace
{
// Every built-in requirement is this shape: a sentence, and a predicate over the context that
// closes over whatever the factory was given. A class per requirement would be the same thing
// spelled a hundred lines longer; a file-local requirement that wants state of its own still
// derives from `requirement` directly.
class simple_requirement : public requirement
{
public:
  simple_requirement(std::string description, std::function<check_result(context&)> check)
    : description_{ std::move(description) }
    , check_{ std::move(check) }
  {
  }

  [[nodiscard]] auto describe() const -> std::string override
  {
    return description_;
  }

  [[nodiscard]] auto check(context& ctx) const -> check_result override
  {
    return check_(ctx);
  }

private:
  std::string description_;
  std::function<check_result(context&)> check_;
};

auto
make(std::string description, std::function<check_result(context&)> check) -> requirement_ptr
{
  return std::make_shared<const simple_requirement>(std::move(description), std::move(check));
}

// A cluster has to be configured before any probe is worth attempting. Reported as not_satisfied
// (a skip) rather than undetermined: nobody asked for a cluster, so nothing is unknown.
auto
unconfigured(const context& ctx) -> std::optional<check_result>
{
  if (!ctx.config().cluster_configured) {
    return check_result::missing("no cluster is configured (TEST_CONNECTION_STRING is unset)");
  }
  return std::nullopt;
}
} // namespace

auto
real_cluster() -> requirement_ptr
{
  return make("a real cluster", [](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (ctx.config().mock) {
      return check_result::missing("the mock is in use, not a real cluster");
    }
    return check_result::ok();
  });
}

auto
mock() -> requirement_ptr
{
  return make("the gocaves mock", [](context& ctx) {
    // Configured first, as real_cluster() does. Nothing here derives an endpoint from the mock:
    // the variable selects which server the suite believes it is addressing, not where it is, so a
    // case gated on the mock has as little to run against as a real one when nothing is set.
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (!ctx.config().mock) {
      // Not "unset": either variable may be set to a value that does not enable it, and the report
      // has to send the reader to the rule rather than to one spelling of it.
      return check_result::missing(
        "the mock is not in use (neither TEST_USE_GOCAVES nor CB_USE_GOCAVES is true, yes or 1)");
    }
    return check_result::ok();
  });
}

auto
service(std::string name) -> requirement_ptr
{
  // The description is built before the capture moves `name`. As two arguments to the same call
  // their evaluation order is unspecified, and a compiler that moves first formats an empty name --
  // which then becomes the key the skip report groups by.
  auto description = fmt::format("the {} service", name);
  return make(std::move(description), [name = std::move(name)](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (!ctx.has_service(name)) {
      return check_result::missing(fmt::format("the cluster runs no {} node", name));
    }
    return check_result::ok();
  });
}

auto
cluster_version(server_version from) -> requirement_ptr
{
  return make(fmt::format("a cluster at {} or later", from.to_string()), [from](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (const auto actual = ctx.server_version(); actual < from) {
      return check_result::missing(
        fmt::format("the cluster is {}, below {}", actual.to_string(), from.to_string()));
    }
    return check_result::ok();
  });
}

auto
cluster_version(server_version from, server_version until) -> requirement_ptr
{
  return make(fmt::format("a cluster in [{}, {})", from.to_string(), until.to_string()),
              [from, until](context& ctx) {
                if (const auto missing = unconfigured(ctx); missing.has_value()) {
                  return *missing;
                }
                const auto actual = ctx.server_version();
                if (actual < from) {
                  return check_result::missing(fmt::format(
                    "the cluster is {}, below {}", actual.to_string(), from.to_string()));
                }
                if (actual >= until) {
                  return check_result::missing(fmt::format(
                    "the cluster is {}, at or above {}", actual.to_string(), until.to_string()));
                }
                return check_result::ok();
              });
}

auto
bucket_capability(std::string capability) -> requirement_ptr
{
  auto description = fmt::format("the {} bucket capability", capability);
  return make(std::move(description), [capability = std::move(capability)](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (!ctx.has_bucket_capability(capability)) {
      return check_result::missing(fmt::format("the bucket does not report {}", capability));
    }
    return check_result::ok();
  });
}

namespace
{
auto
deployment_name(deployment_type type) -> std::string
{
  switch (type) {
    case deployment_type::capella:
      return "Capella";
    case deployment_type::elixir:
      return "Elixir";
    case deployment_type::on_prem:
      break;
  }
  return "on-prem";
}
} // namespace

auto
deployment(deployment_type type) -> requirement_ptr
{
  return make(fmt::format("a {} deployment", deployment_name(type)), [type](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (ctx.server_version().deployment != type) {
      return check_result::missing(fmt::format("the deployment is not {}", deployment_name(type)));
    }
    return check_result::ok();
  });
}

auto
edition(server_edition wanted) -> requirement_ptr
{
  // Every enumerator named, and no default. This string is the requirement's description, which is
  // also the key the skip report groups by, so labelling one edition as another makes the report
  // give a reason nobody asked for. A ternary silently lumps every future edition in with the last
  // branch; the switch makes -Wswitch stop the build until the new one is named.
  std::string name{ "unknown" };
  switch (wanted) {
    case server_edition::enterprise:
      name = "enterprise";
      break;
    case server_edition::community:
      name = "community";
      break;
    case server_edition::columnar:
      name = "columnar";
      break;
    case server_edition::unknown:
      break;
  }
  return make(fmt::format("the {} edition", name), [wanted, name](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    const auto actual = ctx.server_version().edition;
    // Not a skip: the case may well be applicable, and reporting it as inapplicable would claim
    // knowledge the cluster refused to give.
    if (actual == server_edition::unknown) {
      return check_result::unknown("the cluster did not report its edition");
    }
    if (actual != wanted) {
      return check_result::missing(fmt::format("the cluster is not {}", name));
    }
    return check_result::ok();
  });
}

auto
storage_backend(std::string backend) -> requirement_ptr
{
  auto description = fmt::format("a {} bucket", backend);
  return make(std::move(description), [backend = std::move(backend)](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    const auto actual = ctx.storage_backend();
    // "unknown" is how the SDK spells "the server did not report one" -- a bucket on a release
    // predating the setting parses that way. Treated as a mismatch it would skip the case and say
    // "the bucket is unknown, not magma", naming a backend no bucket has and hiding a cluster that
    // could not answer behind a skip. Not knowing is undetermined, as it is for the edition above.
    if (actual == "unknown") {
      return check_result::unknown("the cluster did not report a storage backend");
    }
    if (actual != backend) {
      return check_result::missing(fmt::format("the bucket is {}, not {}", actual, backend));
    }
    return check_result::ok();
  });
}

auto
replicas(std::size_t at_least) -> requirement_ptr
{
  return make(fmt::format("at least {} replica(s)", at_least), [at_least](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (const auto actual = ctx.number_of_replicas(); actual < at_least) {
      return check_result::missing(
        fmt::format("the bucket has {} replica(s), fewer than {}", actual, at_least));
    }
    return check_result::ok();
  });
}

auto
nodes(std::size_t at_least) -> requirement_ptr
{
  return make(fmt::format("at least {} node(s)", at_least), [at_least](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (const auto actual = ctx.number_of_nodes(); actual < at_least) {
      return check_result::missing(
        fmt::format("the cluster has {} node(s), fewer than {}", actual, at_least));
    }
    return check_result::ok();
  });
}

auto
server_groups(std::size_t at_least) -> requirement_ptr
{
  return make(fmt::format("at least {} server group(s)", at_least), [at_least](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (const auto actual = ctx.server_groups().size(); actual < at_least) {
      return check_result::missing(
        fmt::format("the cluster has {} server group(s), fewer than {}", actual, at_least));
    }
    return check_result::ok();
  });
}

auto
developer_preview() -> requirement_ptr
{
  return make("developer preview enabled", [](context& ctx) {
    if (const auto missing = unconfigured(ctx); missing.has_value()) {
      return *missing;
    }
    if (!ctx.server_version().developer_preview) {
      return check_result::missing("the cluster is not in developer preview");
    }
    return check_result::ok();
  });
}

} // namespace couchbase::test::needs
