/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "framework/test_registry.hxx"

#include "core/transactions/internal/exceptions_internal.hxx"
#include "core/transactions/internal/utils.hxx"
#include "core/transactions/transaction_get_result.hxx"

#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>

#include <chrono>
#include <cmath>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace couchbase::core::transactions;

const double min_jitter_fraction = 1.0 - RETRY_OP_JITTER;

constexpr auto one_ms = std::chrono::milliseconds(1);
constexpr auto ten_ms = std::chrono::milliseconds(10);
constexpr auto hundred_ms = std::chrono::milliseconds(100);

struct retry_state {
  std::vector<std::chrono::steady_clock::time_point> timings;

  void function()
  {
    timings.push_back(std::chrono::steady_clock::now());
    throw retry_operation("try again");
  }
  void function2()
  {
    timings.push_back(std::chrono::steady_clock::now());
  }
  auto timing_differences() -> std::vector<std::chrono::microseconds>
  {
    std::vector<std::chrono::microseconds> retval;
    auto last = timings.front();
    for (auto& t : timings) {
      retval.push_back(std::chrono::duration_cast<std::chrono::microseconds>(t - last));
      last = t;
    }
    return retval;
  }
};

void
exponential_backoff_with_a_timeout_gives_up_when_it_elapses([[maybe_unused]] context& ctx)
{
  retry_state state;
  const auto start = std::chrono::steady_clock::now();

  assert_throws<retry_operation_timeout>(
    [&state]() {
      retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] {
        state.function();
      });
    },
    "the timeout is reported rather than retried forever");

  // The helper takes its deadline on entry, so the timeout cannot be declared until at least that
  // long has passed since start. Measured against the clock rather than against the recorded
  // timings, which begin after the first call and end before the one that throws.
  assert_false(state.timings.empty(), "the operation is called before the timeout is declared");
  assert_true(std::chrono::steady_clock::now() - start >= hundred_ms,
              "the timeout is not declared before it has elapsed");
}

void
exponential_backoff_with_a_timeout_bounds_the_retry_count([[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_timeout>(
    [&state]() {
      retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] {
        state.function();
      });
    },
    "the timeout is reported rather than retried forever");

  // Minimum jitter maximises the count: the delays are 0.9, 1.8, 3.6, 7.2 ms and are then
  // clamped to ten_ms with the jitter discarded, so 13.5 ms of the hundred_ms budget buys eight
  // more full delays and a ninth truncated against the deadline. Thirteen delays, so fourteen
  // calls, and fifteen is out of reach. sleep_for may overshoot, which lowers the count, so only
  // the upper bound is checkable.
  assert_true(state.timings.size() < 15, "the delays are not shorter than the backoff schedule");
}

void
exponential_backoff_with_a_timeout_never_retries_faster_than_the_schedule(
  [[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_timeout>(
    [&state]() {
      retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] {
        state.function();
      });
    },
    "the timeout is reported rather than retried forever");

  // The minimum delays: 0 for the first call, then 0.9, 1.8, 3.6, 7.2 ms, and ten_ms exactly once
  // the clamp discards the jitter. The last delay is whatever is left of the budget, so it is
  // excluded. Only the floor is checkable -- sleep_for may overshoot by any amount, so an upper
  // bound would be a flake -- which means the rule held here is that no retry comes sooner than
  // the schedule allows, not that the delays grow. A constant ten_ms delay would satisfy it.
  std::size_t count = 0;
  const auto last = state.timings.size() - 1;
  for (auto& t : state.timing_differences()) {
    if (count == 0) {
      assert_eq(t.count(), 0, "the first call is not delayed");
    } else if (count < last) {
      const auto min = min_jitter_fraction * std::pow(2, count - 1) * 1000.0;
      if (min < 10000) {
        assert_true(static_cast<double>(t.count()) >= min,
                    "the delay before a retry doubles, less the jitter");
      } else {
        assert_true(t.count() >= 10000, "the delay has reached the maximum");
      }
    }
    count++;
  }
}

void
exponential_backoff_with_a_timeout_always_calls_the_operation([[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_timeout>(
    [&state]() {
      retry_op_exponential_backoff_timeout<void>(ten_ms, ten_ms, ten_ms, [&state] {
        state.function();
      });
    },
    "the timeout is reported rather than retried forever");

  // A budget equal to the first delay usually allows one retry, and the jitter sometimes allows a
  // second; neither may reduce the operation to never being called at all.
  assert_false(state.timings.empty(), "a budget as short as the first delay still calls once");
}

void
exponential_backoff_stops_at_the_attempt_limit([[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_retries_exhausted>(
    [&state]() {
      retry_op_exponential_backoff<void>(one_ms, 20, [&state] {
        state.function();
      });
    },
    "exhausting the attempts is reported rather than retried forever");

  assert_eq(
    state.timings.size(), std::size_t{ 21 }, "twenty retries follow the first call, and no more");
}

void
exponential_backoff_with_no_retries_still_calls_the_operation([[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_retries_exhausted>(
    [&state]() {
      retry_op_exponential_backoff<void>(one_ms, 0, [&state] {
        state.function();
      });
    },
    "exhausting the attempts is reported rather than retried forever");

  assert_eq(state.timings.size(), std::size_t{ 1 }, "a retry budget of zero still calls once");
}

void
exponential_backoff_never_retries_faster_than_the_capped_schedule([[maybe_unused]] context& ctx)
{
  retry_state state;

  assert_throws<retry_operation_retries_exhausted>(
    [&state]() {
      retry_op_exponential_backoff<void>(one_ms, 10, [&state] {
        state.function();
      });
    },
    "exhausting the attempts is reported rather than retried forever");

  // The nominal delays double from 1 ms and stop at 2^DEFAULT_RETRY_OP_EXPONENT_CAP: 1, 2, 4, 8,
  // 16, 32, 64, 128, 256 ms, and 256 thereafter. The bound below is that sequence less the jitter,
  // which this overload keeps on the capped delay. There is no deadline here, so the final
  // interval is a full delay and is simply left unasserted.
  //
  // The cap itself is not observable this way: an uncapped run would clear every floor below by a
  // wider margin, and the first interval where the two differ is the one left unasserted. What is
  // held here is the floor of the whole capped sequence.
  std::size_t count = 0;
  const auto last = state.timings.size() - 1;
  for (const auto& t : state.timing_differences()) {
    if (count == 0) {
      assert_eq(t.count(), 0, "the first call is not delayed");
    } else if (count < last) {
      const auto min = min_jitter_fraction *
                       std::pow(2, std::fmin(DEFAULT_RETRY_OP_EXPONENT_CAP, count - 1)) * 1000;
      assert_true(static_cast<double>(t.count()) >= min,
                  "the delay doubles up to the exponent cap, less the jitter");
    }
    count++;
  }
}

void
exp_delay_throws_once_its_timeout_elapses([[maybe_unused]] context& ctx)
{
  retry_state state;
  exp_delay op(one_ms, ten_ms, hundred_ms);
  const auto start = std::chrono::steady_clock::now();

  try {
    while (true) {
      op();
      state.function2();
    }
  } catch (const retry_operation_timeout&) {
    assert_true(std::chrono::steady_clock::now() - start >= hundred_ms,
                "the timeout is not declared before it elapses");
    assert_true(state.timings.size() <= 15, "the delays are not shorter than the schedule");
    return;
  }
  fail("the delay reports its timeout rather than blocking forever");
}

void
constant_delay_throws_once_its_attempts_are_exhausted([[maybe_unused]] context& ctx)
{
  retry_state state;
  auto op = constant_delay(ten_ms, 10);

  try {
    while (true) {
      op();
      state.function2();
    }
  } catch (const retry_operation_retries_exhausted&) {
    assert_eq(state.timings.size(), std::size_t{ 10 }, "the operation runs once per attempt");
    return;
  }
  fail("the delay reports its exhausted attempts rather than blocking forever");
}

struct conversion_fixture {
  const std::string bucket{ "bucket" };
  const std::string scope{ "scope" };
  const std::string collection{ "collection" };
  const std::string key{ "key" };
  const couchbase::cas cas{ 100ULL };
  const tao::json::value content{ { "some_number", 0 } };
  const couchbase::codec::encoded_value json_content{
    couchbase::codec::default_json_transcoder::encode(content)
  };
  const transaction_links links{ "atr_id",
                                 "atr_bucket",
                                 "atr_scope",
                                 "atr_collection",
                                 "txn_id",
                                 "attempt_id",
                                 "op_id",
                                 json_content,
                                 {} /* binary_content */,
                                 "cas_pre_txn",
                                 "rev_pre_txn",
                                 0,
                                 "crc",
                                 "op",
                                 tao::json::value::array({ "xxx", "yyy" }),
                                 false };
  const document_metadata metadata{ "cas", "revid", 0, "crc32" };

  [[nodiscard]] auto make_core_result() const -> transaction_get_result
  {
    return transaction_get_result{
      { bucket, scope, collection, key }, json_content, cas.value(), links, metadata
    };
  }
};

void
a_core_get_result_converts_to_the_public_one([[maybe_unused]] context& ctx)
{
  const conversion_fixture f;
  auto core_result = f.make_core_result();

  auto public_result = core_result.to_public_result();

  assert_eq(public_result.id(), f.key, "the key survives the conversion");
  assert_true(public_result.content_as<tao::json::value>() == f.content,
              "the content survives the conversion");
  assert_eq(core_result.collection(), f.collection, "the collection is unchanged by converting");
  assert_eq(core_result.bucket(), f.bucket, "the bucket is unchanged by converting");
  assert_eq(core_result.scope(), f.scope, "the scope is unchanged by converting");
  assert_true(core_result.cas() == f.cas, "the CAS is unchanged by converting");
  assert_eq(core_result.key(), f.key, "the key is unchanged by converting");
  // The content is _moved_ into the public result, so core_result.content() is left valid but
  // holding anything; nothing below may read it.
}

void
a_core_get_result_survives_a_round_trip_through_the_public_one([[maybe_unused]] context& ctx)
{
  const conversion_fixture f;
  auto core_result = f.make_core_result();

  auto public_result = core_result.to_public_result();
  const transaction_get_result final_core_result(public_result);

  assert_eq(core_result.collection(), final_core_result.collection(), "the collection round-trips");
  assert_eq(core_result.bucket(), final_core_result.bucket(), "the bucket round-trips");
  assert_eq(core_result.scope(), final_core_result.scope(), "the scope round-trips");
  assert_true(core_result.cas() == final_core_result.cas(), "the CAS round-trips");
  assert_true(final_core_result.content() == f.json_content, "the content round-trips");
  assert_eq(core_result.key(), final_core_result.key(), "the key round-trips");
  assert_true(final_core_result.cas() == f.cas,
              "the CAS is the one the core result was built with");
  assert_eq(final_core_result.bucket(), f.bucket, "the bucket is the one it was built with");
  assert_eq(final_core_result.scope(), f.scope, "the scope is the one it was built with");
  assert_eq(
    final_core_result.collection(), f.collection, "the collection is the one it was built with");
  assert_eq(final_core_result.key(), f.key, "the key is the one it was built with");
  assert_true(final_core_result.content() == f.json_content,
              "the content is the one it was built with");
  assert_eq(final_core_result.links().staged_operation_id(),
            f.links.staged_operation_id(),
            "the staged operation id round-trips");
  assert_eq(final_core_result.links().staged_attempt_id(),
            f.links.staged_attempt_id(),
            "the staged attempt id round-trips");
  assert_eq(final_core_result.links().crc32_of_staging(),
            f.links.crc32_of_staging(),
            "the staging CRC round-trips");
  assert_eq(final_core_result.links().atr_collection_name(),
            f.links.atr_collection_name(),
            "the ATR collection round-trips");
  assert_eq(final_core_result.links().atr_scope_name(),
            f.links.atr_scope_name(),
            "the ATR scope round-trips");
  assert_eq(final_core_result.links().atr_bucket_name(),
            f.links.atr_bucket_name(),
            "the ATR bucket round-trips");
  assert_eq(
    final_core_result.links().is_deleted(), f.links.is_deleted(), "the deleted flag round-trips");
  assert_true(final_core_result.links().forward_compat() == f.links.forward_compat(),
              "the forward-compatibility block round-trips");
  assert_eq(final_core_result.links().atr_id(), f.links.atr_id(), "the ATR id round-trips");
  assert_eq(final_core_result.links().cas_pre_txn(),
            f.links.cas_pre_txn(),
            "the pre-transaction CAS round-trips");
  assert_eq(final_core_result.links().exptime_pre_txn(),
            f.links.exptime_pre_txn(),
            "the pre-transaction expiry round-trips");
  assert_eq(final_core_result.links().op(), f.links.op(), "the staged operation round-trips");
  assert_eq(final_core_result.links().revid_pre_txn(),
            f.links.revid_pre_txn(),
            "the pre-transaction revision round-trips");
  assert_true(final_core_result.links().staged_content_json() == f.links.staged_content_json(),
              "the staged content round-trips");
  assert_eq(final_core_result.links().staged_transaction_id(),
            f.links.staged_transaction_id(),
            "the staged transaction id round-trips");
  assert_eq(final_core_result.metadata()->cas(), f.metadata.cas(), "the metadata CAS round-trips");
  assert_eq(
    final_core_result.metadata()->crc32(), f.metadata.crc32(), "the metadata CRC round-trips");
  assert_eq(final_core_result.metadata()->exptime(),
            f.metadata.exptime(),
            "the metadata expiry round-trips");
  assert_eq(
    final_core_result.metadata()->revid(), f.metadata.revid(), "the metadata revision round-trips");
}

void
a_default_constructed_core_get_result_converts_to_the_public_one([[maybe_unused]] context& ctx)
{
  transaction_get_result core_result{};

  auto final_public_result = core_result.to_public_result();

  assert_true(final_public_result.id().empty(), "an unpopulated core result carries no key");
}

void
a_default_constructed_public_get_result_survives_a_round_trip([[maybe_unused]] context& ctx)
{
  const couchbase::transactions::transaction_get_result public_res;

  transaction_get_result core_res(public_res);
  auto final_public_res = core_res.to_public_result();

  assert_true(final_public_res.id().empty(), "an unpopulated public result carries no key");
}

// ${Mutation.CAS} is written by kvengine as 'macroToString(htonll(info.cas))', so the eight bytes
// inside the string are reversed relative to the CAS value, and they are reversed by the server
// rather than by this host. Recovering the value therefore needs an unconditional reversal, not a
// network-to-host conversion: the two are the same on a little-endian host and differ on a
// big-endian one, where a conversion would return the string's value unchanged.
//
// 0x000058a71dd25c15 reversed is 0x155cd21da7580000, or 1539336197457313792 nanoseconds, which the
// function reports as milliseconds.
void
parse_mutation_cas_decodes_the_kvengine_cas_macro([[maybe_unused]] context& ctx)
{
  assert_eq(parse_mutation_cas("0x000058a71dd25c15"),
            1539336197457U,
            "the macro's bytes are reversed and the result reported in milliseconds");
  assert_eq(parse_mutation_cas(""), 0U, "an absent macro decodes to zero rather than raising");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(exponential_backoff_with_a_timeout_gives_up_when_it_elapses), {}, timeout::slow },
      { CASE(exponential_backoff_with_a_timeout_bounds_the_retry_count), {}, timeout::slow },
      { CASE(exponential_backoff_with_a_timeout_never_retries_faster_than_the_schedule),
        {},
        timeout::slow },
      { CASE(exponential_backoff_with_a_timeout_always_calls_the_operation), {}, timeout::slow },
      { CASE(exponential_backoff_stops_at_the_attempt_limit), {}, timeout::slow },
      { CASE(exponential_backoff_with_no_retries_still_calls_the_operation), {}, timeout::slow },
      { CASE(exponential_backoff_never_retries_faster_than_the_capped_schedule),
        {},
        timeout::slow },
      { CASE(exp_delay_throws_once_its_timeout_elapses), {}, timeout::slow },
      { CASE(constant_delay_throws_once_its_attempts_are_exhausted), {}, timeout::slow },
      { CASE(a_core_get_result_converts_to_the_public_one), {}, timeout::instant },
      { CASE(a_core_get_result_survives_a_round_trip_through_the_public_one),
        {},
        timeout::instant },
      { CASE(a_default_constructed_core_get_result_converts_to_the_public_one),
        {},
        timeout::instant },
      { CASE(a_default_constructed_public_get_result_survives_a_round_trip), {}, timeout::instant },
      { CASE(parse_mutation_cas_decodes_the_kvengine_cas_macro), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
