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

// Live KV round-trip against a real couchbase2:// (Cloud Native Gateway) endpoint. cluster_only:
// runs only when TEST_CONNECTION_STRING is set (the harness skips it otherwise). It drives the
// full transport -- channel/credentials, dispatcher, converter, component, error mapping --
// through an upsert -> get -> remove chain, so it fails loudly if any layer regresses against a
// real gateway. Extra env: TEST_CB2_USERNAME/TEST_CB2_PASSWORD (default Administrator/password),
// TEST_CB2_BUCKET (default "default"). TLS verification is disabled (dev gateway cert).

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/protostellar/component.hxx"
#include "core/protostellar/credentials.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/tls_verify_mode.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <cstdlib>
#include <string>

namespace couchbase::cng::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace ps = ::couchbase::core::protostellar;
namespace pk = ::couchbase::core::protostellar::kv;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::cluster_options;
using ::couchbase::core::document_id;
using ::couchbase::core::tls_verify_mode;
using ::couchbase::core::protostellar::component;
using ::couchbase::core::protostellar::component_config;
using ::couchbase::core::utils::parse_connection_string;
using namespace std::chrono_literals;

auto
env_or(const char* name, const char* fallback) -> std::string
{
  return safe_getenv(name).value_or(fallback);
}

void
kv_crud_round_trip_against_live_gateway()
{
  const auto connstr_opt = safe_getenv("TEST_CONNECTION_STRING");
  if (!connstr_opt.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  const auto parsed = parse_connection_string(connstr_opt.value());
  if (!parsed.uses_protostellar()) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }
  if (parsed.bootstrap_nodes.empty()) {
    skip("no nodes in TEST_CONNECTION_STRING");
  }

  const auto& node = parsed.bootstrap_nodes.front();
  const auto port = node.port > 0 ? node.port : parsed.default_port;
  const std::string endpoint = node.address + ":" + std::to_string(port);

  cluster_options options;
  options.enable_tls = true;
  options.tls_verify = tls_verify_mode::none; // dev gateway certificate is not chainable

  cluster_credentials credentials;
  credentials.username = env_or("TEST_CB2_USERNAME", "Administrator");
  credentials.password = env_or("TEST_CB2_PASSWORD", "password");

  const auto bucket = env_or("TEST_CB2_BUCKET", "default");
  const auto id = document_id{ bucket, "_default", "_default", "cng-live-kv-key" };
  const std::string document = "{\"cng\":true}";

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto channel = ps::make_channel(endpoint, options, credentials);
  // Named-field assignment rather than positional aggregate initialisation: component_config
  // gains fields as the series progresses, and a positional list silently rebinds when it does.
  component_config config;
  config.channel = channel;
  config.credentials = credentials;
  config.timeouts = { 20000ms };
  component comp{ io, config };

  std::string failure;    // non-empty => the round-trip failed at some stage
  bool completed = false; // the whole chain ran
  couchbase::cas upsert_cas;

  ops::upsert_request upsert;
  upsert.id = id;
  upsert.value = cu::to_binary(document);
  upsert.flags = 0x06U;

  comp.execute(std::move(upsert), [&](ops::upsert_response upsert_result) {
    if (upsert_result.ctx.ec()) {
      failure = "upsert: " + upsert_result.ctx.ec().message();
      work.reset();
      return;
    }
    upsert_cas = upsert_result.cas;

    ops::get_request get;
    get.id = id;
    comp.execute(std::move(get), [&](ops::get_response get_result) {
      if (get_result.ctx.ec()) {
        failure = "get: " + get_result.ctx.ec().message();
        work.reset();
        return;
      }
      if (cu::to_string(get_result.value) != document) {
        failure = "get: value mismatch, got '" + cu::to_string(get_result.value) + "'";
        work.reset();
        return;
      }
      // The upsert set these deliberately so the document is written as JSON. Checking only the
      // value bytes would pass against a converter that dropped content_flags entirely.
      if (get_result.flags != 0x06U) {
        failure = "get: flags mismatch, got " + std::to_string(get_result.flags);
        work.reset();
        return;
      }

      ops::remove_request remove;
      remove.id = id;
      comp.execute(std::move(remove), [&](ops::remove_response remove_result) {
        if (remove_result.ctx.ec()) {
          failure = "remove: " + remove_result.ctx.ec().message();
        } else {
          completed = true;
        }
        work.reset();
      });
    });
  });

  io.run();

  assert_true(failure.empty(), failure);
  assert_true(completed, "upsert -> get -> remove chain completed");
  assert_true(upsert_cas.value() != 0, "upsert returned a non-zero CAS");
}

void
insert_and_replace_round_trip_against_live_gateway()
{
  const auto connstr_opt = safe_getenv("TEST_CONNECTION_STRING");
  if (!connstr_opt.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  const auto parsed = parse_connection_string(connstr_opt.value());
  if (!parsed.uses_protostellar() || parsed.bootstrap_nodes.empty()) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }
  const auto& node = parsed.bootstrap_nodes.front();
  const auto port = node.port > 0 ? node.port : parsed.default_port;
  const std::string endpoint = node.address + ":" + std::to_string(port);

  cluster_options options;
  options.enable_tls = true;
  options.tls_verify = tls_verify_mode::none;

  cluster_credentials credentials;
  credentials.username = env_or("TEST_CB2_USERNAME", "Administrator");
  credentials.password = env_or("TEST_CB2_PASSWORD", "password");

  const auto bucket = env_or("TEST_CB2_BUCKET", "default");
  const auto id = document_id{ bucket, "_default", "_default", "cng-live-insert-replace-key" };
  const auto missing_id = document_id{ bucket, "_default", "_default", "cng-live-missing-key" };

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  auto channel = ps::make_channel(endpoint, options, credentials);
  // Named-field assignment rather than positional aggregate initialisation: component_config
  // gains fields as the series progresses, and a positional list silently rebinds when it does.
  component_config config;
  config.channel = channel;
  config.credentials = credentials;
  config.timeouts = { 20000ms };
  component comp{ io, config };

  std::string failure;
  bool completed = false;
  bool duplicate_insert_failed_correctly = false;
  bool replace_missing_failed_correctly = false;
  bool remove_missing_failed_correctly = false;

  // 1. Clean slate
  ops::remove_request pre_remove;
  pre_remove.id = id;
  comp.execute(std::move(pre_remove), [&](ops::remove_response) {
    // 2. Insert new document
    ops::insert_request insert;
    insert.id = id;
    insert.value = cu::to_binary("doc1");
    comp.execute(std::move(insert), [&](ops::insert_response insert_result) {
      if (insert_result.ctx.ec()) {
        failure = "insert: " + insert_result.ctx.ec().message();
        work.reset();
        return;
      }

      // 3. Insert existing document should fail with document_exists
      ops::insert_request dup_insert;
      dup_insert.id = id;
      dup_insert.value = cu::to_binary("doc1_dup");
      comp.execute(std::move(dup_insert), [&](ops::insert_response dup_result) {
        if (dup_result.ctx.ec() == couchbase::errc::key_value::document_exists) {
          duplicate_insert_failed_correctly = true;
        } else {
          failure = "duplicate insert did not fail with document_exists, got: " +
                    dup_result.ctx.ec().message();
          work.reset();
          return;
        }

        // 4. Replace existing document
        ops::replace_request replace;
        replace.id = id;
        replace.value = cu::to_binary("doc2");
        comp.execute(std::move(replace), [&](ops::replace_response replace_result) {
          if (replace_result.ctx.ec()) {
            failure = "replace: " + replace_result.ctx.ec().message();
            work.reset();
            return;
          }

          // 5. Replace missing document should fail with document_not_found
          ops::replace_request missing_replace;
          missing_replace.id = missing_id;
          missing_replace.value = cu::to_binary("doc_missing");
          comp.execute(std::move(missing_replace), [&](ops::replace_response missing_result) {
            if (missing_result.ctx.ec() == couchbase::errc::key_value::document_not_found) {
              replace_missing_failed_correctly = true;
            } else {
              failure = "replace missing did not fail with document_not_found, got: " +
                        missing_result.ctx.ec().message();
              work.reset();
              return;
            }

            // 6. Remove existing document
            ops::remove_request remove;
            remove.id = id;
            comp.execute(std::move(remove), [&](ops::remove_response remove_result) {
              if (remove_result.ctx.ec()) {
                failure = "remove: " + remove_result.ctx.ec().message();
                work.reset();
                return;
              }

              // 7. Remove missing document should fail with document_not_found
              ops::remove_request missing_remove;
              missing_remove.id = missing_id;
              comp.execute(
                std::move(missing_remove), [&](ops::remove_response missing_remove_result) {
                  if (missing_remove_result.ctx.ec() ==
                      couchbase::errc::key_value::document_not_found) {
                    remove_missing_failed_correctly = true;
                    completed = true;
                  } else {
                    failure = "remove missing did not fail with document_not_found, got: " +
                              missing_remove_result.ctx.ec().message();
                  }
                  work.reset();
                });
            });
          });
        });
      });
    });
  });

  io.run();

  assert_true(failure.empty(), failure);
  assert_true(duplicate_insert_failed_correctly, "duplicate insert failed with document_exists");
  assert_true(replace_missing_failed_correctly, "replace missing failed with document_not_found");
  assert_true(remove_missing_failed_correctly, "remove missing failed with document_not_found");
  assert_true(
    completed,
    "insert -> dup_insert -> replace -> missing_replace -> remove -> missing_remove completed");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_kv",
    {
      { "kv_crud_round_trip_against_live_gateway",
        kv_crud_round_trip_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "insert_and_replace_round_trip_against_live_gateway",
        insert_and_replace_round_trip_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::cng::test
