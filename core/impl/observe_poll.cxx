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

#include "observe_poll.hxx"

#include "core/cluster.hxx"
#include "core/impl/observe_seqno.hxx"

#include <memory>
#include <mutex>

namespace couchbase::core::impl
{
static constexpr bool
touches_replica(couchbase::persist_to persist_to, couchbase::replicate_to replicate_to)
{

    switch (replicate_to) {
        case replicate_to::one:
        case replicate_to::two:
        case replicate_to::three:
            return true;

        case replicate_to::none:
            break;
    }

    switch (persist_to) {
        case persist_to::one:
        case persist_to::two:
        case persist_to::three:
        case persist_to::four:
            return true;

        case persist_to::none:
        case persist_to::active:
            break;
    }
    return false;
}

static constexpr std::uint32_t
number_of_replica_nodes_required(couchbase::persist_to persist_to)
{
    switch (persist_to) {
        case persist_to::one:
            return 1U;
        case persist_to::two:
            return 2U;
        case persist_to::three:
        case persist_to::four:
            return 3U;

        case persist_to::none:
        case persist_to::active:
            break;
    }
    return 0U;
}

static constexpr std::uint32_t
number_of_replica_nodes_required(couchbase::replicate_to replicate_to)
{
    switch (replicate_to) {
        case replicate_to::one:
            return 1U;
        case replicate_to::two:
            return 2U;
        case replicate_to::three:
            return 3U;
        case replicate_to::none:
            break;
    }
    return 0U;
}

static std::pair<std::error_code, std::uint32_t>
validate_replicas(const topology::configuration& config, couchbase::persist_to persist_to, couchbase::replicate_to replicate_to)
{
    if (config.node_locator != topology::configuration::node_locator_type::vbucket) {
        return { errc::common::feature_not_available, {} };
    }

    if (touches_replica(persist_to, replicate_to)) {
        if (!config.num_replicas) {
            return { errc::key_value::durability_impossible, {} };
        }
        auto number_of_replicas = config.num_replicas.value();
        if (number_of_replica_nodes_required(persist_to) > number_of_replicas ||
            number_of_replica_nodes_required(replicate_to) > number_of_replicas) {
            return { errc::key_value::durability_impossible, {} };
        }
        return { {}, number_of_replicas };
    }
    return { {}, 0 };
}

class observe_status
{
  public:
    explicit observe_status(mutation_token token)
      : token_(std::move(token))
    {
    }

    [[nodiscard]] auto token() const -> const mutation_token&
    {
        return token_;
    }

    void reset()
    {
        std::scoped_lock lock(mutex_);
        replicated_ = 0;
        persisted_ = 0;
        persisted_on_active_ = false;
    }

    void examine(const observe_seqno_response& response)
    {
        std::scoped_lock lock(mutex_);
        bool replicated = response.current_sequence_number >= token_.sequence_number();
        bool persisted = response.last_persisted_sequence_number >= token_.sequence_number();

        replicated_ += (replicated && !response.active) ? 1 : 0;
        persisted_ += persisted ? 1 : 0;
        persisted_on_active_ |= (response.active && persisted);
    }

    [[nodiscard]] bool meets_condition(couchbase::persist_to persist_to, couchbase::replicate_to replicate_to) const
    {
        auto persistence_condition =
          (persist_to == persist_to::active && persisted_on_active_) || (persisted_ >= number_of_replica_nodes_required(persist_to));
        auto replication_condition = replicated_ >= number_of_replica_nodes_required(replicate_to);
        return persistence_condition && replication_condition;
    }

  private:
    mutation_token token_;
    std::size_t replicated_{ 0 };
    std::size_t persisted_{ 0 };
    bool persisted_on_active_{ false };
    mutable std::mutex mutex_{};
};

class observe_context;
static void
observe_poll(std::shared_ptr<couchbase::core::cluster> core, std::shared_ptr<observe_context> ctx);

class observe_context : public std::enable_shared_from_this<observe_context>
{
  public:
    observe_context(asio::io_context& io,
                    document_id id,
                    mutation_token token,
                    std::optional<std::chrono::milliseconds> timeout,
                    couchbase::persist_to persist_to,
                    couchbase::replicate_to replicate_to,
                    observe_handler&& handler)
      : poll_deadline_{ io }
      , poll_backoff_{ io }
      , id_{ std::move(id) }
      , status_{ std::move(token) }
      , timeout_{ timeout }
      , persist_to_{ persist_to }
      , replicate_to_{ replicate_to }
      , handler_{ std::move(handler) }
    {
    }

    void start()
    {
        poll_deadline_.expires_after(poll_deadline_interval_);
        poll_deadline_.async_wait([ctx = shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            ctx->finish(errc::common::ambiguous_timeout);
        });
    }

    [[nodiscard]] auto id() const -> const document_id&
    {
        return id_;
    }

    [[nodiscard]] auto bucket_name() const -> const std::string&
    {
        return id_.bucket();
    }

    [[nodiscard]] auto partition_uuid() const -> std::uint64_t
    {
        return status_.token().partition_uuid();
    }

    [[nodiscard]] auto timeout() const -> const std::optional<std::chrono::milliseconds>&
    {
        return timeout_;
    }

    [[nodiscard]] auto persist_to() const -> couchbase::persist_to
    {
        return persist_to_;
    }

    [[nodiscard]] auto replicate_to() const -> couchbase::replicate_to
    {
        return replicate_to_;
    }

    void add_request(observe_seqno_request&& request)
    {
        requests_.emplace_back(std::move(request));
    }

    void handle_response(observe_seqno_response&& response)
    {
        --expect_number_of_responses_;
        auto r = std::move(response);
        status_.examine(r);
        maybe_finish();
    }

    void finish(std::error_code ec)
    {
        poll_backoff_.cancel();
        poll_deadline_.cancel();
        observe_handler handler{};
        {
            std::scoped_lock lock(handler_mutex_);
            std::swap(handler_, handler);
        }
        if (handler) {
            handler(ec);
        }
    }

    void maybe_finish()
    {
        observe_handler handler{};
        {
            std::scoped_lock lock(handler_mutex_);
            if (!handler_) {
                return;
            }
            if (status_.meets_condition(persist_to_, replicate_to_)) {
                std::swap(handler_, handler);
            } else if (expect_number_of_responses_ == 0 && on_last_response_) {
                poll_backoff_.expires_after(poll_backoff_interval_);
                return poll_backoff_.async_wait(std::move(on_last_response_));
            }
        }
        if (handler) {
            handler({});
        }
    }

    void on_last_response(std::size_t expected_number_of_responses, std::function<void(std::error_code)> handler)
    {
        expect_number_of_responses_ = expected_number_of_responses;
        on_last_response_ = std::move(handler);
    }

    void execute(std::shared_ptr<couchbase::core::cluster> core)
    {
        auto requests = std::move(requests_);
        status_.reset();
        on_last_response(requests.size(), [core, ctx = shared_from_this()](std::error_code ec) mutable {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            observe_poll(std::move(core), std::move(ctx));
        });
        for (auto&& request : requests) {
            core->execute(std::move(request),
                          [ctx = shared_from_this()](observe_seqno_response&& response) { ctx->handle_response(std::move(response)); });
        }
    }

  private:
    asio::steady_timer poll_deadline_;
    asio::steady_timer poll_backoff_;
    const document_id id_;
    observe_status status_;
    std::optional<std::chrono::milliseconds> timeout_;
    couchbase::persist_to persist_to_;
    couchbase::replicate_to replicate_to_;
    std::vector<observe_seqno_request> requests_{};
    std::atomic_size_t expect_number_of_responses_{};
    std::mutex handler_mutex_{};
    observe_handler handler_{};
    std::function<void(std::error_code)> on_last_response_{};
    std::chrono::milliseconds poll_backoff_interval_{ 500 };
    std::chrono::milliseconds poll_deadline_interval_{ 5'000 };
};

static void
observe_poll(std::shared_ptr<couchbase::core::cluster> core, std::shared_ptr<observe_context> ctx)
{
    std::string bucket_name = ctx->bucket_name();
    core->with_bucket_configuration(
      bucket_name, [core, ctx = std::move(ctx)](std::error_code ec, const core::topology::configuration& config) mutable {
          if (ec) {
              return ctx->finish(ec);
          }
          auto [err, number_of_replicas] = validate_replicas(config, ctx->persist_to(), ctx->replicate_to());
          if (err) {
              return ctx->finish(err);
          }

          if (ctx->persist_to() != persist_to::none) {
              ctx->add_request(observe_seqno_request{ ctx->id(), true, ctx->partition_uuid(), ctx->timeout() });
          }

          if (touches_replica(ctx->persist_to(), ctx->replicate_to())) {
              for (std::uint32_t replica_index = 1; replica_index <= number_of_replicas; ++replica_index) {
                  auto replica_id = ctx->id();
                  replica_id.node_index(replica_index);
                  ctx->add_request(observe_seqno_request{ replica_id, false, ctx->partition_uuid(), ctx->timeout() });
              }
          }
          ctx->execute(core);
      });
}

void
initiate_observe_poll(std::shared_ptr<couchbase::core::cluster> core,
                      document_id id,
                      mutation_token token,
                      std::optional<std::chrono::milliseconds> timeout,
                      couchbase::persist_to persist_to,
                      couchbase::replicate_to replicate_to,
                      observe_handler&& handler)
{
    auto ctx = std::make_shared<observe_context>(
      core->io_context(), std::move(id), std::move(token), timeout, persist_to, replicate_to, std::move(handler));
    ctx->start();
    return observe_poll(std::move(core), std::move(ctx));
}
} // namespace couchbase::core::impl
