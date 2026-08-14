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

#include "context.hxx"

#include <cstdlib>
#include <map>
#include <utility>

namespace couchbase::test
{
namespace
{
// A probe answered once and remembered, including the failure. Re-running a probe that already
// failed would turn one unreachable cluster into one round trip per case.
template<typename T>
class cached
{
public:
  template<typename Fn>
  auto get(Fn&& compute) -> T
  {
    if (!value_.has_value() && failure_.empty()) {
      try {
        value_ = std::forward<Fn>(compute)();
      } catch (const probe_failure& e) {
        failure_ = e.what();
      }
    }
    if (!failure_.empty()) {
      throw probe_failure(failure_);
    }
    return *value_;
  }

private:
  std::optional<T> value_{};
  std::string failure_{};
};
} // namespace

auto
configuration::from_environment() -> configuration
{
  configuration config;

  if (const auto value = safe_getenv("TEST_CONNECTION_STRING"); value.has_value()) {
    config.connection_string = *value;
    config.cluster_configured = true;
  }
  if (const auto value = safe_getenv("TEST_USERNAME"); value.has_value()) {
    config.username = *value;
  }
  if (const auto value = safe_getenv("TEST_PASSWORD"); value.has_value()) {
    config.password = *value;
  }
  if (const auto value = safe_getenv("TEST_BUCKET"); value.has_value()) {
    config.bucket = *value;
  }
  // OTHER_TEST_BUCKET is the spelling test/utils/test_context.cxx uses and therefore the one the
  // suite is driven with; TEST_OTHER_BUCKET is accepted after it so an existing local environment
  // keeps working.
  if (const auto value = safe_getenv("OTHER_TEST_BUCKET"); value.has_value()) {
    config.other_bucket = *value;
  } else if (const auto legacy = safe_getenv("TEST_OTHER_BUCKET"); legacy.has_value()) {
    config.other_bucket = *legacy;
  }
  // Both spellings: bin/run-integration-tests reads CB_USE_GOCAVES and exports TEST_USE_GOCAVES,
  // and a developer running a binary by hand sets whichever they remember.
  //
  // The value decides, not its presence, and the accepted words match test/utils/test_context.cxx.
  // Reading presence alone made TEST_USE_GOCAVES=false mean the real cluster to every Catch2 test
  // and the mock to this framework in the same run -- two readers in one process disagreeing about
  // which endpoint they are talking to.
  const auto enabled = [](const std::optional<std::string>& value) {
    return value.has_value() && (*value == "true" || *value == "yes" || *value == "1");
  };
  config.mock = enabled(safe_getenv("TEST_USE_GOCAVES")) || enabled(safe_getenv("CB_USE_GOCAVES"));

  return config;
}

struct context::impl {
  configuration config;
  std::unique_ptr<probe_backend> backend{};
  std::size_t backends_created{ 0 };

  cached<couchbase::test::server_version> version{};
  cached<std::size_t> replicas{};
  cached<std::size_t> nodes{};
  cached<std::vector<std::string>> groups{};
  cached<std::string> storage{};
  std::map<std::string, cached<bool>, std::less<>> services{};
  std::map<std::string, cached<bool>, std::less<>> capabilities{};

  auto probes() -> probe_backend&
  {
    if (backend == nullptr) {
      // Deliberately not at startup: a case that asks nothing of the server must not open a
      // connection, and a suite of 300 cases must open at most this one.
      backend = make_probe_backend(config);
      ++backends_created;
    }
    return *backend;
  }
};

context::context(configuration config)
  : impl_{ std::make_unique<impl>() }
{
  impl_->config = std::move(config);
}

context::context(configuration config, std::unique_ptr<probe_backend> backend)
  : impl_{ std::make_unique<impl>() }
{
  impl_->config = std::move(config);
  impl_->backend = std::move(backend);
  impl_->backends_created = 1;
}

context::~context() = default;

auto
context::config() const -> const configuration&
{
  return impl_->config;
}

auto
context::env(const std::string& name) const -> std::optional<std::string>
{
  return safe_getenv(name);
}

auto
context::server_version() -> couchbase::test::server_version
{
  return impl_->version.get([this]() {
    return impl_->probes().server_version();
  });
}

auto
context::has_service(const std::string& name) -> bool
{
  return impl_->services[name].get([this, &name]() {
    return impl_->probes().has_service(name);
  });
}

auto
context::has_bucket_capability(const std::string& capability) -> bool
{
  return impl_->capabilities[capability].get([this, &capability]() {
    return impl_->probes().has_bucket_capability(capability);
  });
}

auto
context::number_of_replicas() -> std::size_t
{
  return impl_->replicas.get([this]() {
    return impl_->probes().number_of_replicas();
  });
}

auto
context::number_of_nodes() -> std::size_t
{
  return impl_->nodes.get([this]() {
    return impl_->probes().number_of_nodes();
  });
}

auto
context::server_groups() -> std::vector<std::string>
{
  return impl_->groups.get([this]() {
    return impl_->probes().server_groups();
  });
}

auto
context::storage_backend() -> std::string
{
  return impl_->storage.get([this]() {
    return impl_->probes().storage_backend();
  });
}

auto
context::backends_created() const -> std::size_t
{
  return impl_->backends_created;
}

auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>
{
  if (name.empty()) {
    return std::nullopt;
  }

  // noexcept, and it builds a std::string: an allocation failure would otherwise terminate rather
  // than report. Every caller treats "no value" as "unset", which is the honest answer when the
  // environment could not be read.
  try {
#if defined(_WIN32)
    char* buf = nullptr;
    std::size_t len = 0;
    if (_dupenv_s(&buf, &len, name.c_str()) == 0 && buf != nullptr) {
      std::string value(buf);
      free(buf); // NOLINT(cppcoreguidelines-no-malloc) — _dupenv_s allocates with malloc
      if (!value.empty()) {
        return value;
      }
    }
    return std::nullopt;
#else
    if (const char* value = std::getenv(name.c_str()); // NOLINT(concurrency-mt-unsafe)
        value != nullptr && value[0] != '\0') {
      return std::string{ value };
    }
    return std::nullopt;
#endif
  } catch (...) {
    return std::nullopt;
  }
}

} // namespace couchbase::test
