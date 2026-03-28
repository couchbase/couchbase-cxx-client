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

#include "test_helper.hxx"

#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

TEST_CASE("unit: make_key_value_error_context with ec and id only", "[unit]")
{
  couchbase::core::document_id id{ "bucket", "_default", "_default", "my-doc" };
  auto ec = couchbase::errc::key_value::document_not_found;
  auto ctx = couchbase::core::make_key_value_error_context(ec, id);

  CHECK(ctx.ec() == ec);
  CHECK(ctx.id() == "my-doc");
  CHECK(ctx.bucket() == "bucket");
  CHECK(ctx.scope() == "_default");
  CHECK(ctx.collection() == "_default");
  CHECK(ctx.retry_attempts() == 0);
  CHECK(ctx.retry_reasons().empty());
  CHECK_FALSE(ctx.last_dispatched_to().has_value());
  CHECK_FALSE(ctx.last_dispatched_from().has_value());
  CHECK_FALSE(ctx.status_code().has_value());
  CHECK_FALSE(ctx.error_map_info().has_value());
  CHECK_FALSE(ctx.extended_error_info().has_value());
}

TEST_CASE("unit: make_key_value_error_context with retry fields propagated", "[unit]")
{
  couchbase::core::document_id id{ "travel-sample", "inventory", "airline", "doc-key" };
  auto ec = couchbase::errc::key_value::document_locked;
  std::size_t retry_attempts = 3;
  std::set<couchbase::retry_reason> retry_reasons{
    couchbase::retry_reason::key_value_locked,
    couchbase::retry_reason::key_value_temporary_failure,
  };

  auto ctx = couchbase::core::make_key_value_error_context(ec, id, retry_attempts, retry_reasons);

  CHECK(ctx.ec() == ec);
  CHECK(ctx.id() == "doc-key");
  CHECK(ctx.bucket() == "travel-sample");
  CHECK(ctx.scope() == "inventory");
  CHECK(ctx.collection() == "airline");
  CHECK(ctx.retry_attempts() == retry_attempts);
  CHECK(ctx.retry_reasons() == retry_reasons);
  CHECK(ctx.retried_because_of(couchbase::retry_reason::key_value_locked));
  CHECK(ctx.retried_because_of(couchbase::retry_reason::key_value_temporary_failure));
  CHECK_FALSE(ctx.retried_because_of(couchbase::retry_reason::socket_not_available));
}

TEST_CASE("unit: make_key_value_error_context zero-retry overload delegates correctly", "[unit]")
{
  couchbase::core::document_id id{ "bucket", "scope", "collection", "key" };
  auto ec = couchbase::errc::common::authentication_failure;

  auto ctx_simple = couchbase::core::make_key_value_error_context(ec, id);
  auto ctx_explicit = couchbase::core::make_key_value_error_context(ec, id, 0, {});

  // Both overloads must produce identical observable state.
  CHECK(ctx_simple.ec() == ctx_explicit.ec());
  CHECK(ctx_simple.id() == ctx_explicit.id());
  CHECK(ctx_simple.bucket() == ctx_explicit.bucket());
  CHECK(ctx_simple.scope() == ctx_explicit.scope());
  CHECK(ctx_simple.collection() == ctx_explicit.collection());
  CHECK(ctx_simple.retry_attempts() == ctx_explicit.retry_attempts());
  CHECK(ctx_simple.retry_reasons() == ctx_explicit.retry_reasons());
}

TEST_CASE("unit: make_key_value_error_context with empty retry reasons set", "[unit]")
{
  couchbase::core::document_id id{ "bucket", "_default", "_default", "key" };
  auto ec = couchbase::errc::key_value::document_exists;

  auto ctx = couchbase::core::make_key_value_error_context(ec, id, 5, {});

  CHECK(ctx.retry_attempts() == 5);
  CHECK(ctx.retry_reasons().empty());
}
