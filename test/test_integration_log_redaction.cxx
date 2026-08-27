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

/*
 * Covers the one link in log redaction that no unit test can reach: opening a cluster is what
 * turns the setting from an option on an origin into process-wide state. Whether the wrappers
 * themselves produce the right text is settled in test_unit_logger.cxx, and whether the
 * connection string parses is settled in test_unit_connection_string.cxx.
 */

#include "test_helper_integration.hxx"

#include "test/utils/logger.hxx"

#include "core/logger/logger.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/logger.hxx>

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

namespace
{
// Redaction is process-wide, so restore it even if an assertion fails part way through a case.
// A leak would surface as an unrelated failure in whichever case ran next.
class scoped_log_redaction
{
public:
  explicit scoped_log_redaction(bool enable)
    : previous_{ couchbase::core::logger::is_log_redaction_enabled() }
  {
    couchbase::core::logger::set_log_redaction(enable);
  }

  scoped_log_redaction(const scoped_log_redaction&) = delete;
  scoped_log_redaction(scoped_log_redaction&&) = delete;
  auto operator=(const scoped_log_redaction&) -> scoped_log_redaction& = delete;
  auto operator=(scoped_log_redaction&&) -> scoped_log_redaction& = delete;

  ~scoped_log_redaction()
  {
    couchbase::core::logger::set_log_redaction(previous_);
  }

private:
  bool previous_;
};

// The callback is global too, and it is invoked from the IO threads, so the lines need a lock.
class captured_log
{
public:
  captured_log()
  {
    couchbase::logger::register_log_callback([this](std::string_view message,
                                                    couchbase::logger::log_level,
                                                    couchbase::logger::log_location) {
      const std::lock_guard<std::mutex> lock{ mutex_ };
      lines_.emplace_back(message);
    });
  }

  captured_log(const captured_log&) = delete;
  captured_log(captured_log&&) = delete;
  auto operator=(const captured_log&) -> captured_log& = delete;
  auto operator=(captured_log&&) -> captured_log& = delete;

  ~captured_log()
  {
    couchbase::logger::unregister_log_callback();
  }

  [[nodiscard]] auto contains(const std::string& needle) const -> bool
  {
    const std::lock_guard<std::mutex> lock{ mutex_ };
    return std::any_of(lines_.begin(), lines_.end(), [&needle](const auto& line) {
      return line.find(needle) != std::string::npos;
    });
  }

private:
  mutable std::mutex mutex_{};
  std::vector<std::string> lines_{};
};

auto
with_options(const std::string& connection_string, const std::string& options) -> std::string
{
  if (options.empty()) {
    return connection_string;
  }
  const std::string separator = connection_string.find('?') == std::string::npos ? "?" : "&";
  return connection_string + separator + options;
}

auto
connect_to_cluster(const test::utils::test_context& ctx, const std::string& extra_options)
{
  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  if (ctx.use_wan_development_profile) {
    options.apply_profile("wan_development");
  }
  return couchbase::cluster::connect(with_options(ctx.connection_string, extra_options), options)
    .get();
}
} // namespace

TEST_CASE("integration: opening a cluster applies log_redaction from the connection string",
          "[integration]")
{
  test::utils::init_logger();
  const auto ctx = test::utils::test_context::load_from_environment();
  const scoped_log_redaction redaction{ false };

  auto [err, cluster] = connect_to_cluster(ctx, "log_redaction=true");
  REQUIRE_SUCCESS(err.ec());

  // The connection string only reaches core::cluster_options. Turning that into the state the
  // wrappers read happens in cluster_impl::open, which is why this cannot be a unit test.
  REQUIRE(couchbase::core::logger::is_log_redaction_enabled());

  cluster.close().get();
}

TEST_CASE("integration: opening a cluster never turns log redaction off", "[integration]")
{
  test::utils::init_logger();
  const auto ctx = test::utils::test_context::load_from_environment();
  const scoped_log_redaction redaction{ true };

  // Redaction is process-wide while it is configured per cluster, so a cluster opened without
  // the option must not disable it for another cluster, or for an application that asked for it
  // directly. Only the "on" direction is applied on open.
  auto [err, cluster] = connect_to_cluster(ctx, "");
  REQUIRE_SUCCESS(err.ec());

  REQUIRE(couchbase::core::logger::is_log_redaction_enabled());

  cluster.close().get();
}

TEST_CASE("integration: enabling log_redaction and dump_configuration together is warned about",
          "[integration]")
{
  test::utils::init_logger();
  const auto ctx = test::utils::test_context::load_from_environment();
  const scoped_log_redaction redaction{ false };

  // A configuration dump is deliberately not annotated: it exists to show exactly what crossed
  // the wire. Enabling both is therefore a combination worth saying out loud, since the dump can
  // carry values that are tagged everywhere else in the same log.
  const captured_log log{};

  auto [err, cluster] = connect_to_cluster(ctx, "log_redaction=true&dump_configuration=true");
  REQUIRE_SUCCESS(err.ec());

  REQUIRE(couchbase::core::logger::is_log_redaction_enabled());
  // Match text only the warning carries. The name of the option on its own also appears in
  // the cluster-open dump of the origin, so a looser needle would pass with the warning removed.
  REQUIRE(log.contains("log redaction is enabled, but so is dump_configuration"));

  cluster.close().get();
}
