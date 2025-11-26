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

#include "core/trace_span.hxx"
#include "test_helper.hxx"

#include "core/signal_bridge.hxx"

#include <spdlog/fmt/bundled/format.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

// NOLINTBEGIN(bugprone-chained-comparison, misc-use-anonymous-namespace)

TEST_CASE("signal_bridge_options construction and getters", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  SECTION("Default construction")
  {
    signal_bridge_options opts;
    REQUIRE(opts.buffer_limit() == signal_bridge_options::default_buffer_limit);
    REQUIRE(opts.notification_threshold() == signal_bridge_options::default_notification_threshold);
  }

  SECTION("Parameterized construction")
  {
    signal_bridge_options opts(500, 0.8);
    REQUIRE(opts.buffer_limit() == 500);
    REQUIRE(opts.notification_threshold() == 0.8);
  }

  SECTION("Fluent setter interface")
  {
    signal_bridge_options opts;
    opts.buffer_limit(2000).notification_threshold(0.5);
    REQUIRE(opts.buffer_limit() == 2000);
    REQUIRE(opts.notification_threshold() == 0.5);
  }
}

TEST_CASE("signal_bridge basic operations", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  SECTION("Emplace and take_buffer")
  {
    signal_bridge_options opts(10, 0.7);
    signal_bridge bridge(opts);

    bridge.emplace(signal_data{ trace_span{ "span 1" } });
    bridge.emplace(signal_data{ trace_span{ "span 2" } });
    bridge.emplace(signal_data{ trace_span{ "span 3" } });

    auto buffer = bridge.take_buffer();
    REQUIRE(buffer.size() == 3);
    REQUIRE(buffer.front().is_trace_span());
    REQUIRE(buffer.front().as_trace_span().name == "span 1");
    buffer.pop();
    REQUIRE(buffer.front().as_trace_span().name == "span 2");
    buffer.pop();
    REQUIRE(buffer.front().as_trace_span().name == "span 3");
  }

  SECTION("Buffer limit respected - no overflow")
  {
    signal_bridge_options opts(5, 0.7);
    signal_bridge bridge(opts);

    for (int i = 0; i < 10; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
    }

    auto buffer = bridge.take_buffer();
    REQUIRE(buffer.size() == 5);
  }

  SECTION("Wait timeout returns empty optional")
  {
    signal_bridge_options opts(10, 0.7);
    signal_bridge bridge(opts);

    bridge.emplace(signal_data{ trace_span{ "span 1" } });

    auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(50));
    REQUIRE_FALSE(result.has_value());
  }

  SECTION("Empty buffer after take_buffer")
  {
    signal_bridge_options opts(10, 0.7);
    signal_bridge bridge(opts);

    bridge.emplace(signal_data{ trace_span{ "span 1" } });
    bridge.emplace(signal_data{ trace_span{ "span 2" } });

    auto buffer1 = bridge.take_buffer();
    REQUIRE(buffer1.size() == 2);

    auto buffer2 = bridge.take_buffer();
    REQUIRE(buffer2.empty());
  }
}

TEST_CASE("signal_bridge notification threshold", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  SECTION("Notification triggered at threshold")
  {
    signal_bridge_options opts(10, 0.7);
    signal_bridge bridge(opts);

    std::atomic<bool> notified{ false };

    std::thread consumer([&]() -> void {
      auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(1000));
      if (result.has_value()) {
        notified = true;
      }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Add 7 items to reach threshold (0.7 * 10 = 7)
    for (int i = 0; i < 7; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
    }

    consumer.join();
    REQUIRE(notified);
  }

  SECTION("Notification NOT triggered below threshold")
  {
    signal_bridge_options opts(10, 0.7);
    signal_bridge bridge(opts);

    std::atomic<bool> notified{ false };

    std::thread consumer([&]() -> void {
      auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(100));
      if (result.has_value()) {
        notified = true;
      }
    });

    // Add 6 items, below threshold (0.7 * 10 = 7)
    for (int i = 0; i < 6; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
    }

    consumer.join();
    REQUIRE_FALSE(notified);
  }
}

TEST_CASE("signal_bridge single producer, single consumer", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(100, 0.7);
  signal_bridge bridge(opts);

  std::atomic<std::size_t> produced_count{ 0 };
  std::atomic<std::size_t> consumed_count{ 0 };
  std::atomic<bool> producer_done{ false };

  SECTION("Producer fills buffer, consumer drains")
  {
    std::thread producer([&]() -> void {
      for (int i = 0; i < 100; ++i) {
        bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
        ++produced_count;
      }
      producer_done = true;
    });

    std::thread consumer([&]() -> void {
      while (!producer_done || consumed_count < produced_count) {
        auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(100));
        if (result.has_value()) {
          consumed_count += result->size();
        } else {
          auto buffer = bridge.take_buffer();
          consumed_count += buffer.size();
        }
      }
    });

    producer.join();
    consumer.join();

    REQUIRE(produced_count == 100);
    REQUIRE(consumed_count == 100);
  }
}

TEST_CASE("signal_bridge multiple producers, single consumer", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(500, 0.5);
  signal_bridge bridge(opts);

  constexpr std::size_t num_producers = 4;
  constexpr std::size_t items_per_producer = 100;
  std::atomic<std::size_t> consumed_count{ 0 };
  std::atomic<std::size_t> producers_done{ 0 };

  std::vector<std::thread> producers;

  producers.reserve(num_producers);
  for (std::size_t p = 0; p < num_producers; ++p) {
    producers.emplace_back([&, p]() -> void {
      for (std::size_t i = 0; i < items_per_producer; ++i) {
        bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", (p * 1000) + i) } });
      }
      ++producers_done;
    });
  }

  std::thread consumer([&]() -> void {
    while (producers_done < num_producers || consumed_count < num_producers * items_per_producer) {
      auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(50));
      if (result.has_value()) {
        consumed_count += result->size();
      } else {
        auto buffer = bridge.take_buffer();
        consumed_count += buffer.size();
      }
    }
  });

  for (auto& producer : producers) {
    producer.join();
  }
  consumer.join();

  REQUIRE(consumed_count == num_producers * items_per_producer);
}

TEST_CASE("signal_bridge single producer, multiple consumers", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(1000, 0.6);
  signal_bridge bridge(opts);

  constexpr std::size_t total_items = 500;
  constexpr std::size_t num_consumers = 3;

  std::atomic<std::size_t> produced_count{ 0 };
  std::atomic<std::size_t> consumed_count{ 0 };
  std::atomic<bool> producer_done{ false };

  std::thread producer([&]() -> void {
    for (std::size_t i = 0; i < total_items; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
      ++produced_count;
    }
    producer_done = true;
  });

  std::vector<std::thread> consumers;
  consumers.reserve(num_consumers);
  for (std::size_t c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() -> void {
      while (!producer_done || consumed_count < produced_count) {
        auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(50));
        if (result.has_value()) {
          consumed_count += result->size();
        } else if (producer_done) {
          auto buffer = bridge.take_buffer();
          consumed_count += buffer.size();
          break;
        }
      }
    });
  }

  producer.join();
  for (auto& consumer : consumers) {
    consumer.join();
  }

  REQUIRE(consumed_count >= total_items);
}

TEST_CASE("signal_bridge multiple producers, multiple consumers", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(500, 0.7);
  signal_bridge bridge(opts);

  constexpr std::size_t num_producers = 3;
  constexpr std::size_t num_consumers = 3;
  constexpr std::size_t items_per_producer = 200;

  std::atomic<std::size_t> total_produced{ 0 };
  std::atomic<std::size_t> total_consumed{ 0 };
  std::atomic<std::size_t> producers_done{ 0 };

  std::vector<std::thread> producers;
  producers.reserve(num_producers);
  for (std::size_t p = 0; p < num_producers; ++p) {
    producers.emplace_back([&, p]() -> void {
      for (std::size_t i = 0; i < items_per_producer; ++i) {
        bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", (p * 1000) + i) } });
        ++total_produced;
      }
      ++producers_done;
    });
  }

  std::vector<std::thread> consumers;
  consumers.reserve(num_consumers);
  for (std::size_t c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() -> void {
      while (producers_done < num_producers || total_consumed < total_produced) {
        auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(50));
        if (result.has_value()) {
          total_consumed += result->size();
        } else if (producers_done >= num_producers) {
          auto buffer = bridge.take_buffer();
          if (buffer.empty()) {
            break;
          }
          total_consumed += buffer.size();
        }
      }
    });
  }

  for (auto& producer : producers) {
    producer.join();
  }
  for (auto& consumer : consumers) {
    consumer.join();
  }

  REQUIRE(total_produced == num_producers * items_per_producer);
  REQUIRE(total_consumed >= num_producers * items_per_producer);
}

TEST_CASE("signal_bridge stress test with high contention", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(1000, 0.5);
  signal_bridge bridge(opts);

  constexpr std::size_t num_producers = 8;
  constexpr std::size_t items_per_producer = 1000;

  std::atomic<std::size_t> total_produced{ 0 };
  std::atomic<std::size_t> producers_done{ 0 };

  std::vector<std::thread> producers;
  producers.reserve(num_producers);
  for (std::size_t p = 0; p < num_producers; ++p) {
    producers.emplace_back([&]() -> void {
      for (std::size_t i = 0; i < items_per_producer; ++i) {
        bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
        ++total_produced;
      }
      ++producers_done;
    });
  }

  for (auto& producer : producers) {
    producer.join();
  }

  // Drain remaining buffer
  auto final_buffer = bridge.take_buffer();

  REQUIRE(producers_done == num_producers);
  REQUIRE(total_produced == num_producers * items_per_producer);
  REQUIRE(final_buffer.size() <= 1000); // Respects buffer limit
}

TEST_CASE("signal_bridge wait_for_buffer_ready returns data on notification",
          "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(10, 0.7);
  signal_bridge bridge(opts);

  std::atomic<bool> data_received{ false };
  std::atomic<size_t> buffer_size{ 0 };

  std::thread consumer([&]() -> void {
    auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(2000));
    if (result.has_value()) {
      data_received = true;
      buffer_size = result->size();
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Produce enough items to trigger notification
  for (int i = 0; i < 7; ++i) {
    bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
  }

  consumer.join();

  REQUIRE(data_received);
  REQUIRE(buffer_size == 7);
}

TEST_CASE("signal_bridge edge cases", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  SECTION("Buffer limit of 1")
  {
    signal_bridge_options opts(1, 1.0);
    signal_bridge bridge(opts);

    bridge.emplace(signal_data{ trace_span{ "span 1" } });
    bridge.emplace(signal_data{ trace_span{ "span 2" } }); // Should be discarded

    auto buffer = bridge.take_buffer();
    REQUIRE(buffer.size() == 1);
    REQUIRE(buffer.front().as_trace_span().name == "span 1");
  }

  SECTION("Threshold of 1.0 triggers immediately when full")
  {
    signal_bridge_options opts(5, 1.0);
    signal_bridge bridge(opts);

    std::atomic<bool> notified{ false };

    std::thread consumer([&]() -> void {
      auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(500));
      if (result.has_value()) {
        notified = true;
      }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    for (int i = 0; i < 5; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
    }

    consumer.join();
    REQUIRE(notified);
  }

  SECTION("Threshold of 0.0 never triggers through threshold")
  {
    signal_bridge_options opts(10, 0.0);
    signal_bridge bridge(opts);

    std::atomic<bool> notified{ false };

    std::thread consumer([&]() -> void {
      auto result = bridge.wait_for_buffer_ready(std::chrono::milliseconds(100));
      if (result.has_value()) {
        notified = true;
      }
    });

    for (int i = 0; i < 10; ++i) {
      bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
    }

    consumer.join();
    REQUIRE_FALSE(notified);
  }
}

TEST_CASE("signal_bridge buffer ownership transfer", "[unit][signal_bridge]")
{
  using namespace couchbase::core;

  signal_bridge_options opts(100, 0.5);
  signal_bridge bridge(opts);

  for (int i = 0; i < 50; ++i) {
    bridge.emplace(signal_data{ trace_span{ fmt::format("span {}", i) } });
  }

  auto buffer1 = bridge.take_buffer();
  REQUIRE(buffer1.size() == 50);

  // Buffer should be empty after move
  auto buffer2 = bridge.take_buffer();
  REQUIRE(buffer2.empty());

  // Can continue adding after buffer was taken
  bridge.emplace(signal_data{ trace_span{ "span 100" } });
  auto buffer3 = bridge.take_buffer();
  REQUIRE(buffer3.size() == 1);
}

// NOLINTEND(bugprone-chained-comparison, misc-use-anonymous-namespace)
