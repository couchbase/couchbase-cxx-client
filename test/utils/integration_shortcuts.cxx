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

#include "integration_shortcuts.hxx"

#include "core/logger/logger.hxx"
#include "core/utils/join_strings.hxx"

#include <couchbase/build_config.hxx>

#include <future>

namespace test::utils
{
void
open_cluster(const couchbase::core::cluster& cluster, const couchbase::core::origin& origin)
{
  std::promise<std::error_code> promise;
  auto future = promise.get_future();
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  cluster.open_in_background(origin, [&promise](std::error_code ec) mutable {
    promise.set_value(ec);
  });
#else
  cluster.open(origin, [&promise](std::error_code ec) mutable {
    promise.set_value(ec);
  });
#endif
  auto rc = future.get();
  if (rc) {
    CB_LOG_CRITICAL("unable to connect to the cluster: {}, nodes={}",
                    rc.message(),
                    couchbase::core::utils::join_strings(origin.get_nodes(), ", "));
    throw std::system_error(rc);
  }
}

void
close_cluster(const couchbase::core::cluster& cluster)
{
  std::promise<void> promise;
  auto future = promise.get_future();
  cluster.close([&promise]() mutable {
    promise.set_value();
  });
  future.get();
}

void
open_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name)
{
  std::promise<std::error_code> promise;
  auto future = promise.get_future();
  cluster.open_bucket(bucket_name, [&promise](std::error_code ec) mutable {
    promise.set_value(ec);
  });
  auto rc = future.get();
  if (rc) {
    CB_LOG_CRITICAL("unable to open bucket: {}, name={}", rc.message(), bucket_name);
    throw std::system_error(rc);
  }
}

void
close_bucket(const couchbase::core::cluster& cluster, const std::string& bucket_name)
{
  std::promise<std::error_code> promise;
  auto future = promise.get_future();
  cluster.close_bucket(bucket_name, [&promise](std::error_code ec) mutable {
    promise.set_value(ec);
  });
  auto rc = future.get();
  if (rc) {
    CB_LOG_CRITICAL("unable to close bucket: {}, name={}", rc.message(), bucket_name);
    throw std::system_error(rc);
  }
}
} // namespace test::utils
