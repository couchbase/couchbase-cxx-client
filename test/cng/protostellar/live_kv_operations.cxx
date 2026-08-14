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

// Live coverage of the KV operations beyond the CRUD chain (CXXCBC-894). cluster_only.
//
// component_kv_operations.cxx proves the client speaks the protocol correctly to a double that
// answers the way the protocol says it should. That leaves one thing unproven: whether a real
// gateway agrees. These cases check the behaviour the operations exist for -- that a lock actually
// blocks a write, that an expiry actually expires the document, that append actually appends --
// rather than that a particular field survived a round trip.
//
// The expectations mirror the reference suites (gocbcoreps impl_kv_test.go); where a case pins
// something subtle the reasoning is written next to it.
//
// Cases that need an RPC the gateway does not serve skip rather than fail: gateway builds vary in
// which of the KV surface they implement, and a skip says "not covered here" where a failure would
// wrongly say "the client is broken".

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/error_context/key_value.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/error_codes.hxx>

#include <chrono>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace cu = ::couchbase::core::utils;

// A gateway that does not implement an RPC reports feature_not_available; that is a statement about
// the deployment, not about the client, so the case steps aside instead of going red.
void
skip_if_unsupported(const std::error_code& ec, const char* what)
{
  if (ec == couchbase::errc::common::feature_not_available) {
    skip(std::string{ "gateway does not implement " } + what);
  }
}

// Puts a known document in place and hands back its cas. Every case needs one and none of them are
// about upsert, so a failure here is reported as a precondition rather than as the case's subject.
auto
seed(live_kv_fixture& fixture, const std::string& key, const std::string& value) -> couchbase::cas
{
  ops::upsert_request request;
  request.id = fixture.id(key);
  request.value = cu::to_binary(value);
  request.flags = 0x06U;
  const auto response = fixture.execute(std::move(request));
  assert_false(static_cast<bool>(response.ctx.ec()), "precondition: seeding " + key + " succeeded");
  return response.cas;
}

void
discard(live_kv_fixture& fixture, const std::string& key)
{
  ops::remove_request request;
  request.id = fixture.id(key);
  (void)fixture.execute(std::move(request));
}

[[nodiscard]] auto
read(live_kv_fixture& fixture, const std::string& key) -> ops::get_response
{
  ops::get_request request;
  request.id = fixture.id(key);
  return fixture.execute(std::move(request));
}

// How long an expiry case will wait for a document to become unreadable. Far longer than the one
// second of expiry it sets, on purpose: the number is not a prediction of how fast the server is,
// it is the point past which "the document is still here" stops being slowness and starts being a
// defect.
//
// Deliberately under the case's own timeout budget. If the two were equal the harness would kill
// the case first and report a bare timeout, hiding the assertion message that says what actually
// went wrong -- turning a precise failure into a mystery. The static_assert holds that property
// rather than leaving it to a comment: whichever of the two numbers someone changes later, the
// build stops instead of the diagnostics quietly degrading.
constexpr std::chrono::seconds expiry_deadline{ 20 };
static_assert(expiry_deadline < timeout::integration,
              "the expiry poll must give up before the harness times the case out, so that a "
              "failure reports the assertion rather than a bare timeout");

// Polls until the document is gone, or the deadline passes.
//
// A fixed sleep would be a bet that the machine is fast enough, and that bet is lost exactly when
// CI is busiest -- the classic shape of a test that is green locally and flaky in the pipeline.
// Polling inverts it: a slow host only makes the case take longer, never fail, and the case still
// fails promptly-ish when expiry genuinely does not work. Nothing here asserts on elapsed time,
// only on the state transition, so a starved host cannot produce a wrong answer.
[[nodiscard]] auto
wait_until_unreadable(live_kv_fixture& fixture, const std::string& key) -> bool
{
  const auto give_up = std::chrono::steady_clock::now() + expiry_deadline;
  for (;;) {
    if (read(fixture, key).ctx.ec() == couchbase::errc::key_value::document_not_found) {
      return true;
    }
    if (std::chrono::steady_clock::now() >= give_up) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 100 });
  }
}

// Seeds a second document that nothing expires, so a case can tell "the expiry worked" apart from
// "everything vanished". Without it, a bucket flush, a dropped collection or a broken fixture would
// make an expiry case pass for entirely the wrong reason.
struct expiry_control {
  explicit expiry_control(live_kv_fixture& fixture, std::string key)
    : fixture_{ fixture }
    , key_{ std::move(key) }
  {
    seed(fixture_, key_, R"({"cng":"control"})");
  }

  expiry_control(const expiry_control&) = delete;
  expiry_control(expiry_control&&) = delete;
  auto operator=(const expiry_control&) -> expiry_control& = delete;
  auto operator=(expiry_control&&) -> expiry_control& = delete;
  ~expiry_control() = default;

  void assert_survived() const
  {
    const auto control = read(fixture_, key_);
    assert_false(static_cast<bool>(control.ctx.ec()),
                 "control document, which was never given an expiry, is still readable -- so the "
                 "document under test disappeared because of its expiry and not for some other "
                 "reason");
  }

  void discard() const
  {
    ::couchbase::test::discard(fixture_, key_);
  }

private:
  live_kv_fixture& fixture_;
  std::string key_;
};

void
exists_distinguishes_a_present_document_from_a_missing_one()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-exists" };
  seed(fixture, key, R"({"cng":true})");

  ops::exists_request present;
  present.id = fixture.id(key);
  const auto present_result = fixture.execute(std::move(present));
  skip_if_unsupported(present_result.ctx.ec(), "Exists");
  assert_false(static_cast<bool>(present_result.ctx.ec()),
               "exists on a present document succeeded");
  assert_true(present_result.document_exists, "the document is reported as existing");

  discard(fixture, key);

  ops::exists_request absent;
  absent.id = fixture.id(key);
  const auto absent_result = fixture.execute(std::move(absent));

  // The contract the whole operation rests on: asking about a document that is not there is a
  // successful answer of "no", not an error. A gateway that returned NOT_FOUND here would make
  // every caller wrap exists in a try/catch to learn the ordinary case.
  assert_false(static_cast<bool>(absent_result.ctx.ec()),
               "exists on a removed document is still a success");
  assert_false(absent_result.document_exists, "and reports the document as absent");
}

void
touch_expires_a_document()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-touch" };
  seed(fixture, key, R"({"cng":true})");

  const expiry_control control{ fixture, "cng-live-touch-control" };

  // Before the touch the document has no expiry, so it must be readable. This is what makes the
  // disappearance below attributable to the touch rather than to the seed having silently failed.
  assert_false(static_cast<bool>(read(fixture, key).ctx.ec()),
               "the document is readable before any expiry is applied");

  ops::touch_request request;
  request.id = fixture.id(key);
  request.expiry = 1;
  const auto touched = fixture.execute(std::move(request));
  skip_if_unsupported(touched.ctx.ec(), "Touch");
  assert_false(static_cast<bool>(touched.ctx.ec()), "touch succeeded");

  // Reading the document back once the expiry has passed is what proves the expiry was applied;
  // asserting only that touch returned a cas would pass against a client that dropped the field.
  assert_true(wait_until_unreadable(fixture, key),
              "the document became unreadable within the deadline after a 1s expiry");
  control.assert_survived();
  control.discard();
}

void
get_and_lock_blocks_a_write_and_unlock_releases_it()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-lock" };
  seed(fixture, key, R"({"cng":true})");

  ops::get_and_lock_request lock;
  lock.id = fixture.id(key);
  lock.lock_time = 15;
  const auto locked = fixture.execute(std::move(lock));
  skip_if_unsupported(locked.ctx.ec(), "GetAndLock");
  assert_false(static_cast<bool>(locked.ctx.ec()), "get_and_lock succeeded");
  assert_eq(cu::to_string(locked.value),
            std::string{ R"({"cng":true})" },
            "get_and_lock returns the document body");

  // A lock that does not block a write is not a lock. This is the property the operation exists
  // for, and it cannot be observed against a test double that merely echoes a cas back.
  ops::upsert_request blocked;
  blocked.id = fixture.id(key);
  blocked.value = cu::to_binary(R"({"cng":"blocked"})");
  const auto blocked_result = fixture.execute(std::move(blocked));
  assert_true(blocked_result.ctx.ec() == couchbase::errc::key_value::document_locked,
              "a write to a locked document is refused with document_locked");

  ops::unlock_request unlock;
  unlock.id = fixture.id(key);
  unlock.cas = locked.cas;
  const auto unlocked = fixture.execute(std::move(unlock));
  assert_false(static_cast<bool>(unlocked.ctx.ec()), "unlock with the lock cas succeeded");

  ops::upsert_request allowed;
  allowed.id = fixture.id(key);
  allowed.value = cu::to_binary(R"({"cng":"allowed"})");
  const auto allowed_result = fixture.execute(std::move(allowed));
  assert_false(static_cast<bool>(allowed_result.ctx.ec()),
               "the same write succeeds once the lock is released");

  discard(fixture, key);
}

void
unlock_with_the_wrong_cas_is_refused()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-unlock-cas" };
  seed(fixture, key, R"({"cng":true})");

  ops::get_and_lock_request lock;
  lock.id = fixture.id(key);
  lock.lock_time = 5;
  const auto locked = fixture.execute(std::move(lock));
  skip_if_unsupported(locked.ctx.ec(), "GetAndLock");
  assert_false(static_cast<bool>(locked.ctx.ec()), "get_and_lock succeeded");

  ops::unlock_request wrong;
  wrong.id = fixture.id(key);
  wrong.cas = couchbase::cas{ locked.cas.value() ^ 0xffULL };
  const auto refused = fixture.execute(std::move(wrong));

  // Unlocking with a cas that is not the lock's must not succeed: the cas is what proves the caller
  // is the lock holder, so accepting any value would let an unrelated caller release someone
  // else's lock.
  assert_true(static_cast<bool>(refused.ctx.ec()),
              "unlock with a cas that is not the lock's is refused");

  ops::unlock_request right;
  right.id = fixture.id(key);
  right.cas = locked.cas;
  (void)fixture.execute(std::move(right));
  discard(fixture, key);
}

void
get_and_touch_returns_the_value_and_applies_the_expiry()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-gat" };
  seed(fixture, key, R"({"cng":true})");
  const expiry_control control{ fixture, "cng-live-gat-control" };

  ops::get_and_touch_request request;
  request.id = fixture.id(key);
  request.expiry = 1;
  const auto result = fixture.execute(std::move(request));
  skip_if_unsupported(result.ctx.ec(), "GetAndTouch");
  assert_false(static_cast<bool>(result.ctx.ec()), "get_and_touch succeeded");
  assert_eq(cu::to_string(result.value),
            std::string{ R"({"cng":true})" },
            "get_and_touch returns the document body");

  assert_true(wait_until_unreadable(fixture, key),
              "the expiry get_and_touch applied took effect within the deadline");
  control.assert_survived();
  control.discard();
}

void
counters_increment_and_decrement_a_live_value()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-counter" };
  discard(fixture, key); // a counter from an earlier run would change the arithmetic

  ops::increment_request first;
  first.id = fixture.id(key);
  first.delta = 5;
  first.initial_value = 100;
  const auto seeded = fixture.execute(std::move(first));
  skip_if_unsupported(seeded.ctx.ec(), "Increment");
  assert_false(static_cast<bool>(seeded.ctx.ec()), "increment succeeded");
  // With initial set and no document present the counter is created at the initial value; the
  // delta is not applied on top of it. Getting this backwards yields 105 and is the classic
  // counter-semantics bug.
  assert_eq(seeded.content,
            std::uint64_t{ 100 },
            "a counter that did not exist is created at the initial value");

  ops::increment_request again;
  again.id = fixture.id(key);
  again.delta = 5;
  again.initial_value = 100;
  const auto incremented = fixture.execute(std::move(again));
  assert_false(static_cast<bool>(incremented.ctx.ec()), "the second increment succeeded");
  assert_eq(incremented.content,
            std::uint64_t{ 105 },
            "an existing counter is incremented by the delta and ignores the initial value");

  ops::decrement_request down;
  down.id = fixture.id(key);
  down.delta = 3;
  const auto decremented = fixture.execute(std::move(down));
  skip_if_unsupported(decremented.ctx.ec(), "Decrement");
  assert_false(static_cast<bool>(decremented.ctx.ec()), "decrement succeeded");
  assert_eq(decremented.content, std::uint64_t{ 102 }, "decrement subtracts the delta");

  discard(fixture, key);
}

void
append_and_prepend_extend_a_live_value()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-append" };
  seed(fixture, key, "base");

  ops::append_request append;
  append.id = fixture.id(key);
  append.value = cu::to_binary("-suffix");
  const auto appended = fixture.execute(std::move(append));
  skip_if_unsupported(appended.ctx.ec(), "Append");
  assert_false(static_cast<bool>(appended.ctx.ec()), "append succeeded");

  ops::prepend_request prepend;
  prepend.id = fixture.id(key);
  prepend.value = cu::to_binary("prefix-");
  const auto prepended = fixture.execute(std::move(prepend));
  skip_if_unsupported(prepended.ctx.ec(), "Prepend");
  assert_false(static_cast<bool>(prepended.ctx.ec()), "prepend succeeded");

  // Reading the result is what distinguishes append from prepend. Two operations that both
  // succeeded and both returned a cas would look identical without this.
  const auto after = read(fixture, key);
  assert_false(static_cast<bool>(after.ctx.ec()), "the document reads back");
  assert_eq(cu::to_string(after.value),
            std::string{ "prefix-base-suffix" },
            "append went to the end and prepend to the beginning");

  discard(fixture, key);
}

void
incrementing_a_non_numeric_document_is_reported_as_delta_invalid()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-not-numeric" };
  seed(fixture, key, R"({"cng":"not a number"})");

  ops::increment_request request;
  request.id = fixture.id(key);
  request.delta = 1;
  const auto result = fixture.execute(std::move(request));
  skip_if_unsupported(result.ctx.ec(), "Increment");

  // The gateway answers this with FAILED_PRECONDITION carrying a PreconditionFailure violation of
  // type DOC_NOT_NUMERIC (NewDocNotNumericStatus in gateway/dataimpl/server_v1/errorhandler.go,
  // reachable only from Increment and Decrement). Reporting it as internal_server_failure would
  // tell the caller the cluster is broken when in fact their document is not a counter.
  assert_true(result.ctx.ec() == couchbase::errc::key_value::delta_invalid,
              "a counter operation on a non-numeric document is delta_invalid, got: " +
                result.ctx.ec().message());

  discard(fixture, key);
}

void
append_to_a_missing_document_is_reported_as_not_found()
{
  live_kv_fixture fixture;
  const std::string key{ "cng-live-append-missing" };
  discard(fixture, key);

  ops::append_request request;
  request.id = fixture.id(key);
  request.value = cu::to_binary("-suffix");
  const auto result = fixture.execute(std::move(request));
  skip_if_unsupported(result.ctx.ec(), "Append");

  // Append does not create. A gateway or client that reported this as a success would silently
  // lose the write.
  assert_true(result.ctx.ec() == couchbase::errc::key_value::document_not_found,
              "appending to a document that does not exist is document_not_found");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_kv_operations",
    {
      { "exists_distinguishes_a_present_document_from_a_missing_one",
        exists_distinguishes_a_present_document_from_a_missing_one,
        timeout::integration,
        test_env::cluster_only },
      { "touch_expires_a_document",
        touch_expires_a_document,
        timeout::integration,
        test_env::cluster_only },
      { "get_and_lock_blocks_a_write_and_unlock_releases_it",
        get_and_lock_blocks_a_write_and_unlock_releases_it,
        timeout::integration,
        test_env::cluster_only },
      { "unlock_with_the_wrong_cas_is_refused",
        unlock_with_the_wrong_cas_is_refused,
        timeout::integration,
        test_env::cluster_only },
      { "get_and_touch_returns_the_value_and_applies_the_expiry",
        get_and_touch_returns_the_value_and_applies_the_expiry,
        timeout::integration,
        test_env::cluster_only },
      { "counters_increment_and_decrement_a_live_value",
        counters_increment_and_decrement_a_live_value,
        timeout::integration,
        test_env::cluster_only },
      { "append_and_prepend_extend_a_live_value",
        append_and_prepend_extend_a_live_value,
        timeout::integration,
        test_env::cluster_only },
      { "incrementing_a_non_numeric_document_is_reported_as_delta_invalid",
        incrementing_a_non_numeric_document_is_reported_as_delta_invalid,
        timeout::integration,
        test_env::cluster_only },
      { "append_to_a_missing_document_is_reported_as_not_found",
        append_to_a_missing_document_is_reported_as_not_found,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::test
