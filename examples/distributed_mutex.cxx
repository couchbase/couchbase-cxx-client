/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include "core/logger/logger.hxx"

#include <couchbase/cluster.hxx>

#include <couchbase/best_effort_retry_strategy.hxx>

#include <asio.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>

#include <system_error>
#include <thread>

static constexpr auto connection_string{ "couchbase://127.0.0.1" };
static constexpr auto username{ "Administrator" };
static constexpr auto password{ "password" };
static constexpr auto bucket_name{ "default" };
static constexpr auto scope_name{ couchbase::scope::default_name };
static constexpr auto collection_name{ couchbase::collection::default_name };

class lock_aware_retry_strategy : public couchbase::best_effort_retry_strategy
{
  public:
    lock_aware_retry_strategy(couchbase::backoff_calculator calculator)
      : couchbase::best_effort_retry_strategy(calculator)
      , calculator_{ calculator }
    {
    }

    auto retry_after(const couchbase::retry_request& request, couchbase::retry_reason reason) -> couchbase::retry_action override
    {
        if (reason == couchbase::retry_reason::key_value_locked) {
            // here we use the same calculator as best_effort_retry_strategy, but it could be different one
            auto backoff_duration = calculator_(request.retry_attempts());
            fmt::print("retrying in {} because of \"key_value_locked\", attempt {}\n", backoff_duration, request.retry_attempts());
            return couchbase::retry_action{ backoff_duration };
        }
        return couchbase::best_effort_retry_strategy::retry_after(request, reason);
    }

  private:
    couchbase::backoff_calculator calculator_;
};

/**
 * This is simple demo class showing how couchbase mutex could be implemented using pessimistic.
 *
 * @note this class serves for demonstration purpose. Production-ready solution should have more checks, tests and features.
 *
 * @note that the server will automatically release the lock after some time.
 *
 * Class implements `BasicLockable` requirement (but might also implement `Lockable` or even `TimedLockable`.
 * For example, if TimedLockable will be implemented, then we could tie it to lock_duration.
 *
 * @see https://en.cppreference.com/w/cpp/named_req/BasicLockable
 *
 * This mutex will give up if the lock cannot be aquired in given timeout interval, and raise an exception.
 *
 * Alternative implementation might use optimistic locking or more complex coordination mechanisms.
 *
 * @see https://docs.couchbase.com/dotnet-sdk/current/howtos/concurrent-document-mutations.html
 */
class couchbase_mutex
{
  public:
    static constexpr std::chrono::seconds default_lock_duration{ 15 };
    static constexpr std::chrono::seconds default_timeout{ 10 };

    couchbase_mutex(couchbase::collection collection,
                    std::string document_id,
                    std::chrono::seconds lock_duration = default_lock_duration,
                    std::chrono::seconds timeout = default_timeout)
      : collection_{ std::move(collection) }
      , document_id_{ std::move(document_id) }
      , lock_duration_{ lock_duration }
      , timeout_{ timeout }
    {
        auto options = couchbase::upsert_options{}.retry_strategy(retry_strategy_).timeout(timeout_);
        auto [ctx, resp] = collection_.upsert(document_id_, content_, options).get();
        if (ctx.ec()) {
            throw std::system_error(ctx.ec(),
                                    fmt::format(R"(unable to create mutex "{}" (retries: {}))", document_id_, ctx.retry_attempts()));
        }
        cas_ = resp.cas();
        fmt::print(
          "[created ] \"{}\", cas: {}, retries: {}, lock_duration: {}\n", document_id_, cas_.value(), ctx.retry_attempts(), lock_duration);
    }

    void lock()
    {
        std::scoped_lock lock(mutex_);
        auto options = couchbase::get_and_lock_options{}.retry_strategy(retry_strategy_).timeout(timeout_);
        auto [ctx, resp] = collection_.get_and_lock(document_id_, lock_duration_, options).get();
        if (ctx.ec()) {
            throw std::system_error(ctx.ec(),
                                    fmt::format(R"(unable to lock mutex "{}" (retries: {}))", document_id_, ctx.retry_attempts()));
        }
        cas_ = resp.cas();
        fmt::print("[locked  ] \"{}\", cas: {}, retries: {}\n", document_id_, cas_.value(), ctx.retry_attempts());
    }

    void unlock()
    {
        std::scoped_lock lock(mutex_);
        auto options = couchbase::unlock_options{}.timeout(timeout_);
        auto ctx = collection_.unlock(document_id_, cas_, options).get();
        if (ctx.ec()) {
            throw std::system_error(ctx.ec(),
                                    fmt::format(R"(unable to unlock mutex "{}" (retries: {}))", document_id_, ctx.retry_attempts()));
        }
        fmt::print("[unlocked] \"{}\", cas: {}, retries: {}\n", document_id_, cas_.value(), ctx.retry_attempts());
    }

  private:
    couchbase::collection collection_;
    std::string document_id_;
    std::chrono::seconds lock_duration_;
    std::chrono::seconds timeout_;
    couchbase::cas cas_;
    const std::string content_{ "__couchbase_mutex__" };
    mutable std::mutex mutex_{}; // regular mutex to protect internal state
    std::shared_ptr<lock_aware_retry_strategy> retry_strategy_{ std::make_shared<lock_aware_retry_strategy>(
      couchbase::controlled_backoff) }; // see also couchbase::exponential_backoff calculator
};

int
main()
{
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    options.apply_profile("wan_development");
    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    auto collection = cluster.bucket(bucket_name).scope(scope_name).collection(collection_name);

    // Obtain thread_id for simplicity. Could be pid_id, if it was more portable.
    auto writer_id = fmt::format("thread:{}", std::hash<std::thread::id>()(std::this_thread::get_id()));

    // Create distributed mutex to protect modification of document "order:42".
    couchbase_mutex mutex(collection, "demo_mutex");
    {
        std::scoped_lock lock(mutex);

        // while lock is kept, other process cannot modify "order:42"
        const std::string document_id{ "order:42" };
        const tao::json::value basic_doc{ { "type", "book" },
                                          { "name", "Alice in Wonderland" },
                                          { "author", "Lewis Carroll" },
                                          { "price_usd", 4.0 },
                                          { "writer_id", writer_id } };
        auto [ctx, resp] = collection.upsert(document_id, basic_doc, {}).get();
        fmt::print("[stored  ] \"{}\", ec: {}, id: \"{}\", CAS: {}, writer_id: \"{}\"\n",
                   document_id,
                   ctx.ec() ? ctx.ec().message() : "success",
                   document_id,
                   resp.cas().value(),
                   writer_id);

        fmt::print(stderr, "[wait    ] pretend to do some work for 7 seconds (distributed mutex still locked)\n");
        std::this_thread::sleep_for(std::chrono::seconds{ 7 });
    }

    cluster.close();
    guard.reset();

    io_thread.join();

    return 0;
}
