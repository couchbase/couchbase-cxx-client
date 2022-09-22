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

#include "test_helper.hxx"
#include "utils/transactions_env.h"

#include "core/transactions/internal/exceptions_internal.hxx"
#include "core/transactions/internal/utils.hxx"

#include <future>
#include <iostream>
#include <limits>
#include <thread>

using namespace couchbase::core::transactions;
using namespace std;

double min_jitter_fraction = 1.0 - RETRY_OP_JITTER;

struct retry_state {
    vector<chrono::steady_clock::time_point> timings;

    void reset()
    {
        timings.clear();
    }
    void function()
    {
        timings.push_back(chrono::steady_clock::now());
        throw retry_operation("try again");
    }
    void function2()
    {
        timings.push_back(chrono::steady_clock::now());
        return;
    }
    template<typename R, typename P>
    void function_with_delay(chrono::duration<R, P> delay)
    {
        this_thread::sleep_for(delay);
        function();
    }
    vector<chrono::microseconds> timing_differences()
    {
        vector<chrono::microseconds> retval;
        auto last = timings.front();
        for (auto& t : timings) {
            retval.push_back(chrono::duration_cast<chrono::microseconds>(t - last));
            last = t;
        }
        return retval;
    }
    chrono::milliseconds elapsed_ms()
    {
        return chrono::duration_cast<chrono::milliseconds>(timings.back() - timings.front());
    }
    chrono::time_point<chrono::steady_clock>& first_timing()
    {
        return timings.front();
    }
};
// convenience stuff
auto one_ms = chrono::milliseconds(1);
auto ten_ms = chrono::milliseconds(10);
auto hundred_ms = chrono::milliseconds(100);

TEST_CASE("exponential backoff with timeout: will timeout", "[unit]")
{
    retry_state state;
    auto start = chrono::steady_clock::now();
    REQUIRE_THROWS_AS(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                      retry_operation_timeout);
    // sleep_for is only guaranteed to sleep for _at_least_ the time requested.
    // so lets make sure the total elapsed time is at least what we wanted.
    // TODO: notice that timings are the times that the function is _called_.  The actual start time for the exponential backoff
    //       is _before_ that call, so we could be slightly under 100ms in this test.  A very rare
    //       fail in this tests is possible. So we kept track of the time right before we called the function
    //       and added that to the elapsed time.  Not perfect, but should prevent the occasional spurious failure.
    REQUIRE(state.timings.size() > 0);
    auto extra = chrono::duration_cast<chrono::milliseconds>(state.first_timing() - start);
    REQUIRE(state.elapsed_ms() + extra >= hundred_ms);
}

TEST_CASE("exponential backoff with timeout: retry count in range", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                      retry_operation_timeout);
    // should be 1+2+4+8+10+10+10+...
    // +/- 10% jitter RECALCULATE if jitter fraction changes!
    // Consider solving exactly if we allow user-supplied jitter fraction.
    // So retries should be less than or equal 0.9+1.8+3.6+7.2+9+9.. = 13.5 + 9+...(9 times)+ 5.5 = 14
    // and greater than or equal 1.1+2.2+4.4+8.8+11+... = 16.5 + 11+11...(7 times)+ 6.5 = 12
    // the # times it will be called is one higher than this.  Also - since sleep_for can be _longer_
    // than you ask for, we could be significantly under the 12 above.  Let's just make sure they are not
    // more frequent than the max
    REQUIRE(state.timings.size() < 15);
}

TEST_CASE("exponential backoff with timeout: retry timing reasonable", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                      retry_operation_timeout);
    // expect 0,1,2,4,8,10... +/-10% with last one being the remainder
    size_t count = 0;
    auto last = state.timings.size() - 1;
    for (auto& t : state.timing_differences()) {
        if (count == 0) {
            REQUIRE(0 == t.count());
        } else if (count < last) {
            // in microseconds
            auto min = min_jitter_fraction * pow(2, count - 1) * 1000.0;
            if (min < 10000) {
                REQUIRE(static_cast<double>(t.count()) > min);
            } else {
                REQUIRE(t.count() > 10000);
            }
        }
        count++;
    }
}

TEST_CASE("exponential backoff with timeout: always retries at least once", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff_timeout<void>(ten_ms, ten_ms, ten_ms, [&state] { state.function(); }),
                      retry_operation_timeout);
    // Usually just retries once, sometimes the jitter means a second retry
    REQUIRE(2 <= state.timings.size());
}

TEST_CASE("exponential backoff with max attempts: will stop at max", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff<void>(one_ms, 20, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // this will delay 1+2+4+8+16+32+128+128+128... = 255+128+128... = 7(to get to 255)+13*128
    REQUIRE(21 == state.timings.size());
}

TEST_CASE("exponential backoff with max attempts: zero retries", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff<void>(one_ms, 0, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // Should always be called once
    REQUIRE(1 == state.timings.size());
}

TEST_CASE("exponential backoff with max attempts: retry timing reasonable", "[unit]")
{
    retry_state state;
    REQUIRE_THROWS_AS(retry_op_exponential_backoff<void>(one_ms, 10, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // expect 0,1,2,4,8,16,32,64,128,128..... +/-10% with last one being the remainder
    size_t count = 0;
    auto last = state.timings.size() - 1;
    for (const auto& t : state.timing_differences()) {
        if (count == 0) {
            REQUIRE(0 == t.count());
        } else if (count < last) {
            auto min = min_jitter_fraction * pow(2, fmin(DEFAULT_RETRY_OP_EXPONENT_CAP, count - 1)) * 1000;
            REQUIRE(static_cast<double>(t.count()) >= min);
        }
        count++;
    }
}

TEST_CASE("exp_delay: can call till timeout", "[unit]")
{
    retry_state state;
    exp_delay op(one_ms, ten_ms, hundred_ms);
    try {
        auto lambda = [&state, &op]() {
            while (true) {
                op();
                state.function2();
            }
        };
        lambda();
        FAIL("expected exception");
    } catch (retry_operation_timeout&) {
        cout << "elapsed: " << state.elapsed_ms().count() << endl;
        REQUIRE(state.elapsed_ms() >= hundred_ms);
        REQUIRE(state.timings.size() <= 15);
    }
}

TEST_CASE("retryable op: can have constant delay", "[unit]")
{
    retry_state state;
    auto op = constant_delay(ten_ms, 10);
    try {
        auto lambda = [&state, &op]() {
            while (true) {
                op();
                state.function2();
            }
        };
        lambda();
        FAIL("expected exception");
    } catch (const retry_operation_retries_exhausted&) {
        REQUIRE(state.timings.size() == 10);
    }
}
