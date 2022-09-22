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

#include "core/transactions/waitable_op_list.hxx"

#include <future>
#include <list>
#include <thread>

static const std::string NODE{ "someipaddress" };

TEST_CASE("transactions: defaults to KV mode", "[unit]")
{
    couchbase::core::transactions::waitable_op_list op_list;
    auto mode = op_list.get_mode();
    REQUIRE(mode.query_node.empty());
    REQUIRE(mode.mode == couchbase::core::transactions::attempt_mode::modes::KV);
}

TEST_CASE("transactions: can set mode and node", "[unit]")
{
    couchbase::core::transactions::waitable_op_list op_list;
    std::atomic<bool> begin_work_called{ false };
    std::atomic<bool> do_work_called{ false };
    op_list.increment_ops();
    op_list.set_query_mode(
      [&op_list, &begin_work_called]() {
          op_list.set_query_node(NODE);
          begin_work_called = true;
      },
      [&do_work_called]() { do_work_called = true; });

    auto mode = op_list.get_mode();
    REQUIRE(mode.query_node == NODE);
    REQUIRE(mode.mode == couchbase::core::transactions::attempt_mode::modes::QUERY);
    REQUIRE(begin_work_called.load());
    REQUIRE_FALSE(do_work_called.load());
}

TEST_CASE("transactions: set mode waits on in flight ops", "[unit]")
{
    couchbase::core::transactions::waitable_op_list op_list;
    op_list.increment_ops();
    op_list.increment_ops();
    std::atomic<bool> do_work_called{ false };
    auto f = std::async(std::launch::async, [&op_list, &do_work_called] {
        op_list.set_query_mode([&op_list]() { op_list.set_query_node(NODE); }, [&do_work_called]() { do_work_called = true; });
    });
    auto f2 = std::async(std::launch::async, [&op_list] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        op_list.decrement_in_flight();
    });
    CHECK(std::future_status::timeout == f.wait_for(std::chrono::milliseconds(100)));
    f2.get();
    CHECK(std::future_status::ready == f.wait_for(std::chrono::milliseconds(100)));
    f.get();
    auto mode = op_list.get_mode();
    REQUIRE(mode.mode == couchbase::core::transactions::attempt_mode::modes::QUERY);
    REQUIRE_FALSE(do_work_called.load());
}

TEST_CASE("transactions: set mode calls appropriate callbacks", "[unit]")
{
    int NUM_FUTURES{ 10 };
    couchbase::core::transactions::waitable_op_list op_list;
    std::atomic<int> do_work_calls{ 0 };
    std::atomic<int> begin_work_calls{ 0 };
    auto call_set_query_mode = [&op_list, &begin_work_calls, &do_work_calls]() {
        op_list.increment_ops();
        op_list.set_query_mode(
          [&op_list, &begin_work_calls] {
              begin_work_calls++;
              op_list.set_query_node(NODE);
              op_list.decrement_in_flight();
              op_list.decrement_ops();
          },
          [&op_list, &do_work_calls]() {
              do_work_calls++;
              op_list.decrement_in_flight();
              op_list.decrement_ops();
          });
    };

    std::list<std::future<void>> futures;
    for (int i = 0; i < NUM_FUTURES; i++) {
        futures.emplace_back(std::async(std::launch::async, call_set_query_mode));
    }
    for (auto& f : futures) {
        f.get();
    }
    REQUIRE(do_work_calls.load() == NUM_FUTURES - 1);
    REQUIRE(begin_work_calls.load() == 1);
}

TEST_CASE("transactions: get mode waits", "[unit]")
{
    couchbase::core::transactions::waitable_op_list op_list;
    std::atomic<bool> begin_work_called{ false };
    std::atomic<bool> do_work_called{ false };
    op_list.increment_ops();
    op_list.set_query_mode([&begin_work_called]() { begin_work_called = true; }, [&do_work_called]() { do_work_called = true; });
    auto f = std::async(std::launch::async, [&op_list] {
        auto mode = op_list.get_mode();
        return (mode.query_node == NODE && mode.mode == couchbase::core::transactions::attempt_mode::modes::QUERY);
    });
    auto f2 = std::async(std::launch::async, [&op_list] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        op_list.set_query_node(NODE);
        return;
    });
    CHECK(std::future_status::timeout == f.wait_for(std::chrono::milliseconds(100)));
    f2.get();
    CHECK(f.get());
    auto mode = op_list.get_mode();
    CHECK(mode.query_node == NODE);
    CHECK(mode.mode == couchbase::core::transactions::attempt_mode::modes::QUERY);
}
