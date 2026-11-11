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

#pragma once

#include "core/app_telemetry_meter.hxx"
#include "core/document_id_redaction.hxx"
#include "core/error_context/key_value_error_map_info.hxx"
#include "core/logger/redaction.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/protocol/client_request.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_get_collection_id.hxx"
#include "core/tracing/constants.hxx"
#include "core/utils/movable_function.hxx"
#include "couchbase/metrics/meter.hxx"
#include "couchbase/tracing/request_tracer.hxx"
#include "mcbp_command_id.hxx"
#include "mcbp_session.hxx"
#include "mcbp_traits.hxx"
#include "retry_orchestrator.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/dispatch.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>
#include <spdlog/fmt/bundled/chrono.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <type_traits>
#include <utility>

namespace couchbase::core::operations
{

using mcbp_command_handler =
  utils::movable_function<void(std::error_code, std::optional<io::mcbp_message>&&)>;

template<typename Manager, typename Request>
struct mcbp_command : public std::enable_shared_from_this<mcbp_command<Manager, Request>> {
  using encoded_request_type = typename Request::encoded_request_type;
  using encoded_response_type = typename Request::encoded_response_type;
  asio::steady_timer deadline;
  asio::steady_timer retry_backoff;
  // Only cancellable operations (replica fan-out) need a strand: their dispatch
  // path can run on the caller thread while a sibling's completion cancels them
  // from an IO thread, so all access to session_/opaque_/handler_ is confined to
  // it. Non-cancellable operations get an empty placeholder instead, so they pay
  // neither the strand_impl allocation nor any access on the hot path; the strand
  // is referenced only inside `if constexpr (is_cancellable_operation_v<...>)`.
  struct unused_strand {
  };
  using command_strand = std::conditional_t<is_cancellable_operation_v<Request>,
                                            asio::strand<asio::io_context::executor_type>,
                                            unused_strand>;
  command_strand strand_;
  Request request;
  encoded_request_type encoded;
  std::optional<std::uint32_t> opaque_{};
  std::optional<io::mcbp_session> session_{};
  mcbp_command_handler handler_{};
  // The dispatch path of a non-cancellable command runs on whichever thread calls map_and_send(),
  // while the deadline runs cancel() on an I/O thread. state_ arbitrates between them:
  //
  // - state_completed: the one completion was claimed (claim_completion()); publish_dispatch()
  //   commits no write after it.
  // - state_written: the current attempt's write was committed and the server has not rejected it
  //   with unknown_collection, so the server may apply it.
  // - state_published: session_ and opaque_ hold the current attempt and stay unchanged until the
  //   next begin_dispatch(), which clears it before reassigning them. publish_dispatch() sets it
  //   with release before collection resolution and before the write, and a claim that observes it
  //   acquires them. A claim that does not observe it reads neither.
  // - state_earlier_written: an earlier attempt's write may have been applied. begin_dispatch()
  //   moves state_written here, and nothing clears it.
  //
  // The dispatcher's transitions and the claim are read-modify-writes of state_, so they are
  // totally ordered: either publish_dispatch() observes the claim and no write is committed after
  // it, or the claim observes state_written, and a non-idempotent request reports
  // ambiguous_timeout. A write committed before the claim may still reach the session after it. A
  // subscription registered after the claim is discarded by withdraw_if_completed().
  //
  // The bits above state_flags count attempts, advanced by two exchanges that fail once completed:
  // begin_dispatch(), which starts a dispatch attempt, and accept_retry(), which accepts a retry
  // for scheduling. A claim is therefore ordered against both. after_retry_decision() relies on it:
  // an unchanged count after a completion means the claim beat any acceptance or later dispatch.
  static constexpr std::uint32_t state_completed{ 1U };
  static constexpr std::uint32_t state_written{ 2U };
  static constexpr std::uint32_t state_published{ 4U };
  static constexpr std::uint32_t state_earlier_written{ 8U };
  static constexpr std::uint32_t state_in_doubt{ state_written | state_earlier_written };
  static constexpr std::uint32_t state_flags{ 0xffU };
  static constexpr std::uint32_t state_attempt{ 0x100U };
  std::atomic<std::uint32_t> state_{ 0U };
  // The dispatch span of the write in flight. Ended exactly once: by the response handler that
  // takes it, or by complete(), which also closes the slot. Closing prevents publication, not
  // creation: a send() past publish_dispatch() when the claim landed still creates its span, and
  // ends it itself when it cannot publish it, possibly after the completion callback has run.
  std::mutex dispatch_span_mutex_{};
  std::shared_ptr<couchbase::tracing::request_span> dispatch_span_{};
  bool dispatch_span_closed_{ false };
  std::shared_ptr<Manager> manager_{};
  std::chrono::milliseconds timeout_{};
  // Built lazily by id(); empty until first requested. See id().
  mutable std::string id_{};
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
  std::shared_ptr<couchbase::tracing::request_span> span_{ nullptr };
#endif
  std::shared_ptr<couchbase::tracing::request_span> parent_span_{ nullptr };
  std::optional<std::string> last_dispatched_from_{};
  std::optional<std::string> last_dispatched_to_{};
  std::chrono::time_point<std::chrono::steady_clock> started_at_{
    std::chrono::steady_clock::now()
  };
  std::chrono::time_point<std::chrono::steady_clock> deadline_expiry_{ started_at_ + timeout_ };
  std::vector<std::chrono::microseconds> server_durations_{};
  // Sum of server_durations_, kept as they are added so an orphan needs no rescan.
  std::chrono::microseconds total_server_duration_{};

  static auto make_command_strand(asio::io_context& ctx) -> command_strand
  {
    if constexpr (is_cancellable_operation_v<Request>) {
      return command_strand{ ctx.get_executor() };
    } else {
      return command_strand{};
    }
  }

  mcbp_command(asio::io_context& ctx,
               std::shared_ptr<Manager> manager,
               Request req,
               std::chrono::milliseconds default_timeout)
    : deadline(ctx)
    , retry_backoff(ctx)
    , strand_(make_command_strand(ctx))
    , request(req)
    , manager_(manager)
    , timeout_(request.timeout.value_or(default_timeout))
    , parent_span_(request.parent_span)
  {
  }

  // The diagnostic identifier ("<opcode>/<uuid>") is used only in log lines,
  // retry traces, and the returned key_value error context — never on the wire.
  // Build it lazily on first use and cache it, so it is materialized at most once
  // per command instead of eagerly in every constructor, and repeated readers -- a
  // retry log line and then the completion's error context -- share the one cached
  // value. The completion path builds the error context, so a completed operation
  // still materializes it once; the laziness only avoids the cost for commands that
  // never reach a point that needs it. The cache is logically const (it does not
  // change the command's observable value), hence the mutable member and const
  // accessor.
  [[nodiscard]] auto id() const -> const std::string&
  {
    if (id_.empty()) {
      id_ = make_command_id(encoded_request_type::body_type::opcode);
    }
    return id_;
  }

  void start(mcbp_command_handler&& handler)
  {
    started_at_ = std::chrono::steady_clock::now();
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
    span_ = manager_->tracer()->create_span(
      tracing::span_name_for_mcbp_command(encoded_request_type::body_type::opcode), parent_span_);
    if (span_->uses_tags()) {
      span_->add_tag(tracing::attributes::op::service, tracing::service::key_value);
      span_->add_tag(tracing::attributes::op::bucket_name, request.id.bucket());
    }
#endif

    handler_ = std::move(handler);
    deadline.expires_after(timeout_);
    deadline_expiry_ = deadline.expiry();
    deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->cancel(retry_reason::do_not_retry);
    });

    if constexpr (is_cancellable_operation_v<Request>) {
      request.cancel_token->setup([weak_self = this->weak_from_this()] {
        if (auto self = weak_self.lock()) {
          self->cancel(retry_reason::do_not_retry, false);
        }
      });
    }
  }

  // Whether the one completion was claimed. Safe from any thread.
  [[nodiscard]] auto is_completed() const -> bool
  {
    return (state_.load(std::memory_order_acquire) & state_completed) != 0;
  }

  void cancel(retry_reason reason, bool is_timeout = true)
  {
    if constexpr (is_cancellable_operation_v<Request>) {
      // Confine cancellation to the command strand so it cannot race the
      // dispatch path (map_and_send/send_to/send) of a replica sibling that is
      // still being set up on another thread.
      if (!strand_.running_in_this_thread()) {
        return asio::dispatch(strand_, [self = this->shared_from_this(), reason, is_timeout]() {
          self->cancel(reason, is_timeout);
        });
      }
    }
    const auto observed = claim_completion();
    if ((observed & state_completed) != 0) {
      return;
    }
    const bool published = (observed & state_published) != 0;
    if (published && session_ && opaque_) {
      // Withdraws the subscription; the response handler it runs finds the completion claimed.
      // When the session is stopping, cancel() returns false and discard() removes a late entry.
      if (!session_->cancel(*opaque_, asio::error::operation_aborted, reason) &&
          session_->is_stopped()) {
        withdraw_if_completed();
      }
    }
    if (is_timeout) {
      complete(
        request.retries.idempotent() || (observed & state_in_doubt) == 0
          ? errc::common::unambiguous_timeout // safe to retry or has not been sent to the server
          : errc::common::ambiguous_timeout,  // non-idempotent and may have been sent
        {},
        published);
    } else {
      complete(errc::common::request_canceled, {}, published);
    }
  }

  // Re-dispatch a session completion handler onto the command strand for
  // cancellable operations, so the response/timeout path (which mcbp_session
  // invokes on its own IO thread) cannot run concurrently with a strand-confined
  // cancel(). A no-op passthrough for non-cancellable operations.
  template<typename Handler>
  auto on_strand(Handler&& handler)
  {
    if constexpr (is_cancellable_operation_v<Request>) {
      return [self = this->shared_from_this(), handler = std::forward<Handler>(handler)](
               std::error_code ec,
               retry_reason reason,
               io::mcbp_message&& msg,
               std::optional<key_value_error_map_info> error_info) mutable {
        if (self->strand_.running_in_this_thread()) {
          return handler(ec, reason, std::move(msg), std::move(error_info));
        }
        asio::dispatch(self->strand_,
                       [handler = std::move(handler),
                        ec,
                        reason,
                        msg = std::move(msg),
                        error_info = std::move(error_info)]() mutable {
                         handler(ec, reason, std::move(msg), std::move(error_info));
                       });
      };
    } else {
      return std::forward<Handler>(handler);
    }
  }

  void invoke_handler(std::error_code ec,
                      std::optional<io::mcbp_message>&& msg = {},
                      std::shared_ptr<app_telemetry_value_recorder> telemetry_recorder = nullptr)
  {
    if ((claim_completion() & state_completed) != 0) {
      // A response that passed the handler's completed check and then lost the claim: the session
      // has already removed its subscription, so cancel() could not report it. Only a frame read
      // from the wire makes an orphan, as in the handler's own late branch; a retry decision
      // reaches here without the message, so the handler's scope carries that provenance.
      if (wire_response_.command == this) {
        report_wire_response_orphan();
      } else if (msg && msg->header.magic != 0) {
        report_orphan();
      }
      return;
    }
    if (wire_response_.command == this) {
      wire_response_.completed_by_answer = true;
    }
    // Callers run on the dispatch path or in a response handler, which are ordered after the writes
    // to session_.
    complete(ec, std::move(msg), true, std::move(telemetry_recorder));
  }

  void request_collection_id()
  {
    if constexpr (is_cancellable_operation_v<Request>) {
      // Reached from the unknown-collection retry timer on an arbitrary IO
      // thread; hop onto the strand before touching session_/opaque_.
      if (!strand_.running_in_this_thread()) {
        return asio::dispatch(strand_, [self = this->shared_from_this()]() {
          self->request_collection_id();
        });
      }
    }
    // The lookup is a dispatch attempt of its own: no write once completed, and its opaque goes
    // into opaque_ so that cancel() and withdraw_if_completed() can withdraw its subscription.
    if (!begin_dispatch()) {
      return;
    }
    if (session_->is_stopped()) {
      return manager_->map_and_send(this->shared_from_this());
    }
    protocol::client_request<protocol::get_collection_id_request_body> req;
    const auto opaque = session_->next_opaque();
    opaque_ = opaque;
    req.opaque(opaque);
    req.body().collection_path(request.id.collection_path());
    auto data = req.data(session_->supports_feature(protocol::hello_feature::snappy));
    // Published without state_written: the document request is not written by this attempt. It also
    // keeps session_ readable to a claim, for its telemetry.
    if (!publish_dispatch(state_published)) {
      return;
    }
    session_->write_and_subscribe(
      opaque,
      std::move(data),
      on_strand([self = this->shared_from_this()](
                  std::error_code ec,
                  retry_reason /* reason */,
                  io::mcbp_message&& msg,
                  std::optional<key_value_error_map_info> /* error_info */) mutable {
        // As for the document request: a late lookup response updates nothing.
        if (ec != asio::error::operation_aborted && self->is_completed()) {
          return;
        }
        if (ec == asio::error::operation_aborted) {
          // From cancel(), after its claim; cancel() completes the command.
          return;
        }
        if (ec == errc::common::collection_not_found) {
          if (self->request.id.is_collection_resolved()) {
            return self->invoke_handler(ec);
          }
          return self->handle_unknown_collection();
        }
        if (ec) {
          return self->invoke_handler(ec);
        }
        protocol::client_response<protocol::get_collection_id_response_body> resp(std::move(msg));
        self->session_->update_collection_uid(self->request.id.collection_path(),
                                              resp.body().collection_uid());
        self->request.id.collection_uid(resp.body().collection_uid());
        return self->send();
      }));
    withdraw_if_completed();
  }

  void handle_unknown_collection(
    std::shared_ptr<app_telemetry_value_recorder> telemetry_recorder = nullptr)
  {
    // The server rejected the attempt in flight, which is then not applied. Reached from
    // GetCollectionID only before any attempt was written, when there is nothing to clear.
    state_.fetch_and(~state_written, std::memory_order_relaxed);
    const auto backoff = std::chrono::milliseconds(500);
    const auto time_left = deadline_expiry_ - std::chrono::steady_clock::now();
    if (time_left < backoff) {
      // Classified from the state the claim observed: without state_in_doubt no attempt may have
      // been applied, so the timeout is unambiguous.
      const auto observed = claim_completion();
      if ((observed & state_completed) != 0) {
        // An unknown_collection answer that lost the claim is still an answer from the server.
        report_wire_response_orphan();
        return;
      }
      CB_LOG_DEBUG(R"({} unknown collection response for "{}", time_left={}ms, id="{}")",
                   session_->log_prefix(),
                   logger::document(request.id),
                   std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count(),
                   id());
      request.retries.add_reason(retry_reason::key_value_collection_outdated);
      return complete(
        make_error_code(request.retries.idempotent() || (observed & state_in_doubt) == 0
                          ? errc::common::unambiguous_timeout
                          : errc::common::ambiguous_timeout),
        {},
        true,
        std::move(telemetry_recorder));
    }
    if (!accept_retry()) {
      // The claim came first: nothing is armed, and a document answer is reported. A collection
      // lookup's answer has no wire-response scope, so it is not.
      report_wire_response_orphan();
      return;
    }
    CB_LOG_DEBUG(R"({} unknown collection response for "{}", time_left={}ms, id="{}")",
                 session_->log_prefix(),
                 logger::document(request.id),
                 std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count(),
                 id());
    request.retries.add_reason(retry_reason::key_value_collection_outdated);
    retry_backoff.expires_after(backoff);
    retry_backoff.async_wait([self = this->shared_from_this()](std::error_code ec) mutable {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->request_collection_id();
    });
  }

  void send()
  {
    if (!begin_dispatch()) {
      return;
    }
    opaque_ = session_->next_opaque();
    request.opaque = *opaque_;
    if (request.id.use_collections() && !request.id.is_collection_resolved()) {
      if (session_->supports_feature(protocol::hello_feature::collections)) {
        auto collection_id = session_->get_collection_uid(request.id.collection_path());
        if (collection_id) {
          request.id.collection_uid(collection_id.value());
        } else {
          CB_LOG_DEBUG(
            R"({} no cache entry for collection, resolve collection id for "{}", timeout={}ms, id="{}")",
            session_->log_prefix(),
            logger::document(request.id),
            timeout_.count(),
            id());
          return request_collection_id();
        }
      } else {
        if (!request.id.has_default_collection()) {
          return invoke_handler(errc::common::unsupported_operation);
        }
      }
    }

    if (auto ec = request.encode_to(encoded, session_->context()); ec) {
      return invoke_handler(ec);
    }
    if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
      if (request.durability_level != durability_level::none) {
        encoded.body().durability(
          request.durability_level,
          static_cast<std::uint16_t>(static_cast<double>(timeout_.count()) * 0.9));
      }
    }

    auto data = encoded.data(session_->supports_feature(protocol::hello_feature::snappy));
    if (!publish_dispatch(state_written | state_published)) {
      return;
    }
    // Created only for a committed write, and skipped once a claim has landed.
    // Created outside the lock: the tracer is application code, and complete() needs the lock.
    // Published to the slot while open; ended here if complete() closed the slot since creation.
    auto dispatch_span = is_completed() ? nullptr : create_dispatch_span();
    if (dispatch_span) {
      {
        const std::scoped_lock lock(dispatch_span_mutex_);
        if (!dispatch_span_closed_) {
          dispatch_span_ = std::exchange(dispatch_span, nullptr);
        }
      }
      if (dispatch_span) {
        dispatch_span->end();
      }
    }
    session_->write_and_subscribe(
      request.opaque,
      std::move(data),
      on_strand([self = this->shared_from_this(), start = std::chrono::steady_clock::now()](
                  std::error_code ec,
                  retry_reason reason,
                  io::mcbp_message&& msg,
                  std::optional<key_value_error_map_info> /* error_info */) mutable {
        // A response delivered after the claim leaves spans, durations, telemetry, and retry
        // state untouched; a response already past this check when the claim lands finishes its
        // attempt span and latency, then skips retry and completion updates. operation_aborted
        // comes only from cancel(), which runs this handler on the claiming thread.
        if (ec != asio::error::operation_aborted && self->is_completed()) {
          // Only an answer read from the wire makes an orphan. The session's own cancellations, on
          // stop or on a write to a stopped session, deliver an empty message, whose magic is zero;
          // the code cannot tell them apart, since statuses such as not_my_vbucket also map to
          // request_canceled.
          if (msg.header.magic != 0) {
            // This frame's server duration goes into the record only: the completed command is
            // not changed.
            auto inputs = self->capture_orphan_inputs();
            const std::chrono::microseconds duration{ static_cast<std::int64_t>(
              protocol::parse_server_duration_us(msg)) };
            inputs.last_server_duration = duration;
            inputs.total_server_duration += duration;
            self->manager_->orphan_reporter()->add_orphan(self->make_orphan_attributes(inputs));
          }
          return;
        }
        // Marks this thread as handling a frame from the wire for this command, until the handler
        // returns, including through a retry decision that ends in invoke_handler().
        const wire_response_scope wire_scope{ msg.header.magic != 0 ? self.get() : nullptr };
        {
          const auto server_duration_us =
            static_cast<std::uint64_t>(protocol::parse_server_duration_us(msg));
          self->server_durations_.emplace_back(std::chrono::microseconds(server_duration_us));
          self->total_server_duration_ += std::chrono::microseconds(server_duration_us);
          if (auto dispatch_span = self->take_dispatch_span(); dispatch_span) {
            self->close_dispatch_span(dispatch_span, server_duration_us);
          }
        }
        auto telemetry_recorder = self->session_->app_telemetry_recorder(
          *self->manager_->app_telemetry_meter(), self->manager_->name());
        {
          auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
          auto category = app_telemetry_latency::kv_retrieval;
          switch (encoded_request_type::body_type::opcode) {
            case protocol::client_opcode::upsert:
            case protocol::client_opcode::insert:
            case protocol::client_opcode::replace:
            case protocol::client_opcode::remove:
            case protocol::client_opcode::increment:
            case protocol::client_opcode::decrement:
            case protocol::client_opcode::append:
            case protocol::client_opcode::prepend:
              category = app_telemetry_latency::kv_mutation_nondurable;
              if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
                if (self->request.durability_level != durability_level::none) {
                  category = app_telemetry_latency::kv_mutation_durable;
                }
              }
              break;
            default:
              break;
          }

          telemetry_recorder->record_latency(category, latency);
        }

#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
        {
          metrics::metric_attributes attrs{
            tracing::service::key_value,
            self->request.observability_identifier,
            metrics::standardized_error_type(ec),
            self->request.id.bucket(),
            self->request.id.scope(),
            self->request.id.collection(),
          };
          self->manager_->meter()->record_value(std::move(attrs), start);
        }
#endif

        self->retry_backoff.cancel();
        if (ec == asio::error::operation_aborted) {
          // From cancel(), after its claim; cancel() completes the command.
          return self->report_orphan();
        }
        if (ec == errc::common::request_canceled) {
          if (!self->request.retries.idempotent() && !allows_non_idempotent_retry(reason)) {
            if (msg.header.magic != 0) {
              // A frame from the server: reported once, whoever claims.
              self->report_orphan();
              return self->invoke_handler(ec, {}, std::move(telemetry_recorder));
            }
            // The session's own cancellation, on stop: it reports only if it is the completion.
            if ((self->claim_completion() & state_completed) != 0) {
              return;
            }
            self->report_orphan();
            return self->complete(ec, {}, true, std::move(telemetry_recorder));
          }
          if (self->completed_before_retry()) {
            return;
          }
          return self->retry_answer(reason, ec, msg.header.magic != 0);
        }
        key_value_status_code status = key_value_status_code::invalid;
        std::optional<key_value_error_map_info> error_code{};
        if (protocol::is_valid_status(msg.header.status())) {
          status = static_cast<key_value_status_code>(msg.header.status());
        } else {
          error_code = self->session_->decode_error_code(msg.header.status());
        }
        if (status == key_value_status_code::not_my_vbucket) {
          if (self->completed_before_retry()) {
            return;
          }
          return self->retry_answer(
            retry_reason::key_value_not_my_vbucket, ec, msg.header.magic != 0);
        }
        if (status == key_value_status_code::unknown_collection) {
          return self->handle_unknown_collection(std::move(telemetry_recorder));
        }
        if (status == key_value_status_code::config_only) {
          CB_LOG_DEBUG("{} server returned status 0x{:02x} ({}) meaning that the node does not "
                       "serve data operations, "
                       "requesting new "
                       "configuration and retrying",
                       self->session_->log_prefix(),
                       msg.header.status(),
                       status);
          self->manager_->fetch_config();
          if (self->completed_before_retry()) {
            return;
          }
          return self->retry_answer(
            retry_reason::service_response_code_indicated, ec, msg.header.magic != 0);
        }
        if (error_code && error_code.value().has_retry_attribute()) {
          reason = retry_reason::key_value_error_map_retry_indicated;
        } else {
          switch (status) {
            case key_value_status_code::locked:
              if constexpr (encoded_request_type::body_type::opcode !=
                            protocol::client_opcode::unlock) {
                /**
                 * special case for unlock command, when it should not be retried, because it does
                 * not make sense (someone else unlocked the document)
                 */
                reason = retry_reason::key_value_locked;
              }
              break;
            case key_value_status_code::temporary_failure:
              reason = retry_reason::key_value_temporary_failure;
              break;
            case key_value_status_code::sync_write_in_progress:
              reason = retry_reason::key_value_sync_write_in_progress;
              break;
            case key_value_status_code::sync_write_re_commit_in_progress:
              reason = retry_reason::key_value_sync_write_re_commit_in_progress;
              break;
            default:
              break;
          }
        }
        if (reason == retry_reason::do_not_retry) {
          self->invoke_handler(ec, std::move(msg), std::move(telemetry_recorder));
        } else if (!self->completed_before_retry()) {
          self->retry_answer(reason, ec, msg.header.magic != 0);
        }
      }));
    withdraw_if_completed();
  }

  // What an orphan record is built from, taken while no retry can be reassigning it. Cheap to take:
  // the record itself is built only when an orphan is reported.
  struct orphan_inputs {
    std::optional<io::mcbp_session> session{};
    std::uint32_t opaque{};
    std::chrono::microseconds last_server_duration{};
    std::chrono::microseconds total_server_duration{};
  };

  // Accepts a retry decided for this command, for retry_orchestrator or the unknown-collection
  // backoff to schedule. The attempt count advances in an exchange that fails once completed, so
  // a claim either precedes the acceptance, and nothing is scheduled, or follows it, as a timeout
  // during the backoff.
  [[nodiscard]] auto accept_retry() -> bool
  {
    auto current = state_.load(std::memory_order_relaxed);
    do {
      if ((current & state_completed) != 0) {
        return false;
      }
    } while (!state_.compare_exchange_weak(
      current, current + state_attempt, std::memory_order_relaxed, std::memory_order_relaxed));
    return true;
  }

  // The dispatch attempt state_ is on, for after_retry_decision().
  [[nodiscard]] auto dispatch_attempt() const -> std::uint32_t
  {
    return state_.load(std::memory_order_relaxed) & ~state_flags;
  }

  // Called once the retry decision for an answer from the server has returned; `attempt` and
  // `orphan` were taken before it. The answer is reported, from the captured inputs, only if the
  // command completed while the attempt count stayed unchanged: the claim beat accept_retry(), so
  // no retry was scheduled. An accepted retry or a later dispatch advanced the count; a claim after
  // either is a timeout during the backoff or the retry's own outcome. A completion by the answer's
  // own claim is its result.
  //
  // Relaxed: a value with state_completed set carries the attempt count of every exchange before
  // it, and a begin_dispatch() after the claim fails without changing it.
  void after_retry_decision(std::uint32_t attempt, orphan_inputs&& orphan)
  {
    if (wire_response_.command == this &&
        (wire_response_.completed_by_answer || wire_response_.orphan_reported)) {
      return;
    }
    const auto observed = state_.load(std::memory_order_relaxed);
    if ((observed & state_completed) == 0 || (observed & ~state_flags) != attempt) {
      return;
    }
    manager_->orphan_reporter()->add_orphan(make_orphan_attributes(orphan));
    if (wire_response_.command == this) {
      wire_response_.orphan_reported = true;
    }
  }

  // Called after write_and_subscribe(). A claim between publish_dispatch() and the registration
  // looked up opaque_ before the subscription existed, so it withdrew nothing. The session's
  // handler lock orders that lookup before the registration, and so the claim before this load. A
  // claim after the registration withdrew the subscription itself, and finding none here is
  // harmless.
  //
  // session_ and opaque_ are read only when state_published is also set. Acquire pairs with the
  // release in publish_dispatch(), and once completed no begin_dispatch() succeeds, so they stay
  // the published pair. Without state_published a later attempt has begun, which happens only after
  // this attempt's subscription was taken by its response; that attempt may be reassigning them.
  //
  // The subscription is discarded, not cancelled: its response handler would record telemetry and
  // end the dispatch span on this thread while the completion runs on another. The orphan it would
  // have reported for the document request is reported here instead; a collection lookup is not
  // reported, since the orphan reporter tracks document requests only. The discard leaves the
  // dispatch span alone: the claim may not have reached complete() yet, and complete() is
  // responsible for ending it.
  void withdraw_if_completed()
  {
    const auto observed = state_.load(std::memory_order_acquire);
    if ((observed & state_completed) == 0 || (observed & state_published) == 0) {
      return;
    }
    if (session_ && opaque_ && session_->discard(*opaque_) && *opaque_ == request.opaque) {
      manager_->orphan_reporter()->add_orphan(create_orphan_attributes());
    }
  }

  void send_to(io::mcbp_session session)
  {
    if (!begin_dispatch()) {
      return;
    }
    session_ = std::move(session);
    send();
  }

private:
  // The response handler a thread is running, for a frame read from the wire, and whether it has
  // reported its orphan. Thread-local: a retry that dispatches on another thread does not see it.
  struct wire_response_state {
    const void* command{ nullptr };
    bool orphan_reported{ false };
    // The answer's own claim completed the command, so it is the result, not an orphan.
    bool completed_by_answer{ false };
  };
  inline static thread_local wire_response_state wire_response_{};

  class wire_response_scope
  {
  public:
    explicit wire_response_scope(const void* command)
    {
      wire_response_ = wire_response_state{ command, false, false };
    }

    ~wire_response_scope()
    {
      wire_response_ = previous_;
    }

    wire_response_scope(const wire_response_scope&) = delete;
    wire_response_scope(wire_response_scope&&) = delete;
    auto operator=(const wire_response_scope&) -> wire_response_scope& = delete;
    auto operator=(wire_response_scope&&) -> wire_response_scope& = delete;

  private:
    wire_response_state previous_{ wire_response_ };
  };

  // Called by the response handler before it hands its answer to a retry decision. A claim that
  // landed since the handler's completed check means this answer arrived beside the completion: a
  // retry would be refused when it dispatches, so the answer is reported here, once, and nothing is
  // retried. A claim after the retry is scheduled is a timeout during the backoff, with no answer
  // outstanding.
  [[nodiscard]] auto completed_before_retry() -> bool
  {
    if (!is_completed()) {
      return false;
    }
    report_wire_response_orphan();
    return true;
  }

  // Hands an answer to a retry decision. For an answer from the server, the attempt and the orphan
  // attributes are taken first, while no retry can be reassigning the fields they come from.
  void retry_answer(retry_reason reason, std::error_code ec, bool from_wire)
  {
    if (!from_wire) {
      return io::retry_orchestrator::maybe_retry(manager_, this->shared_from_this(), reason, ec);
    }
    const auto attempt = dispatch_attempt();
    auto orphan = capture_orphan_inputs();
    io::retry_orchestrator::maybe_retry(manager_, this->shared_from_this(), reason, ec);
    after_retry_decision(attempt, std::move(orphan));
  }

  // Reports the orphan of the wire response this thread is handling for this command, if it has
  // not been reported yet. A frame from the server is reported exactly once, whoever claims.
  void report_wire_response_orphan()
  {
    if (wire_response_.command == this && !wire_response_.orphan_reported) {
      report_orphan();
    }
  }

  void report_orphan()
  {
    manager_->orphan_reporter()->add_orphan(create_orphan_attributes());
    if (wire_response_.command == this) {
      wire_response_.orphan_reported = true;
    }
  }

  // The dispatch span of the write in flight, for its response handler to end. Empty once taken.
  [[nodiscard]] auto take_dispatch_span() -> std::shared_ptr<couchbase::tracing::request_span>
  {
    const std::scoped_lock lock(dispatch_span_mutex_);
    return std::move(dispatch_span_);
  }

  // Claims the one completion of this command and returns the state before the claim. Acquire pairs
  // with the release in publish_dispatch(), so a claim that observes state_published sees the
  // session_ and opaque_ published with it.
  auto claim_completion() -> std::uint32_t
  {
    return state_.fetch_or(state_completed, std::memory_order_acquire);
  }

  // Starts a dispatch attempt. Returns false once a completion is claimed. Clearing state_published
  // first keeps a claim from reading session_ and opaque_ while this attempt reassigns them. The
  // previous attempt's state_written moves to state_earlier_written.
  //
  // Relaxed: this exchange publishes nothing to a claim and reads nothing a claim wrote. A claim
  // ordered before it may read the previous attempt's fields, but then this exchange fails, so no
  // write follows that read.
  [[nodiscard]] auto begin_dispatch() -> bool
  {
    auto current = state_.load(std::memory_order_relaxed);
    std::uint32_t next{};
    do {
      if ((current & state_completed) != 0) {
        return false;
      }
      next = (current & ~(state_published | state_written)) + state_attempt;
      if ((current & state_written) != 0) {
        next |= state_earlier_written;
      }
    } while (!state_.compare_exchange_weak(
      current, next, std::memory_order_relaxed, std::memory_order_relaxed));
    return true;
  }

  // Sets `bits` (state_published, and state_written for the write itself). Returns false, and the
  // attempt must stop, once a completion is claimed. Release publishes session_ and opaque_ to the
  // claim's acquire.
  [[nodiscard]] auto publish_dispatch(std::uint32_t bits) -> bool
  {
    auto current = state_.load(std::memory_order_relaxed);
    do {
      if ((current & state_completed) != 0) {
        return false;
      }
    } while (!state_.compare_exchange_weak(
      current, current | bits, std::memory_order_release, std::memory_order_relaxed));
    return true;
  }

  // Runs once, after claim_completion() succeeds. Reads session_ only when session_readable is
  // true; when telemetry_recorder is null, resolves counters from app_telemetry_meter.
  void complete(std::error_code ec,
                std::optional<io::mcbp_message>&& msg,
                bool session_readable,
                std::shared_ptr<app_telemetry_value_recorder> telemetry_recorder = nullptr)
  {
    // Ends the dispatch span no response handler took, before the caller can end its parent.
    // Closing prevents publication; a send() already inside create_dispatch_span() ends that span
    // itself, possibly after the caller has been answered. end() runs before the timeout is
    // delivered; see request_span::end().
    std::shared_ptr<couchbase::tracing::request_span> abandoned_span{};
    {
      const std::scoped_lock lock(dispatch_span_mutex_);
      dispatch_span_closed_ = true;
      abandoned_span = std::move(dispatch_span_);
    }
    if (abandoned_span) {
      abandoned_span->end();
    }
    retry_backoff.cancel();
    deadline.cancel();
    mcbp_command_handler handler{};
    std::swap(handler, handler_);
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
    // Kept non-null until destruction so a concurrent create_dispatch_span() can copy it safely.
    if (span_ != nullptr) {
      span_->end();
    }
#endif
    if (handler) {
      if (!telemetry_recorder) {
        telemetry_recorder = manager_->app_telemetry_meter()->value_recorder(
          session_readable && session_ ? session_->node_uuid() : "", manager_->name());
      }
      telemetry_recorder->update_counter(app_telemetry_counter::kv_r_total);
      if (ec == errc::common::unambiguous_timeout || ec == errc::common::ambiguous_timeout) {
        telemetry_recorder->update_counter(app_telemetry_counter::kv_r_timedout);
        auto time_left = deadline_expiry_ - std::chrono::steady_clock::now();
        CB_LOG_TRACE(R"([{}] timeout operation id="{}", {}, key="{}", partition={}, time_left={})",
                     session_readable && session_ ? session_->log_prefix() : manager_->log_prefix(),
                     id(),
                     encoded_request_type::body_type::opcode,
                     logger::document(request.id),
                     request.partition,
                     time_left);
      } else if (ec == errc::common::request_canceled) {
        telemetry_recorder->update_counter(app_telemetry_counter::kv_r_canceled);
      }
      handler(ec, std::move(msg));
    }
  }

  [[nodiscard]] auto create_orphan_attributes() -> orphan_attributes
  {
    return make_orphan_attributes(capture_orphan_inputs());
  }

  [[nodiscard]] auto capture_orphan_inputs() const -> orphan_inputs
  {
    return {
      session_,
      request.opaque,
      server_durations_.empty() ? std::chrono::microseconds{} : server_durations_.back(),
      total_server_duration_,
    };
  }

  [[nodiscard]] auto make_orphan_attributes(const orphan_inputs& inputs) const -> orphan_attributes
  {
    orphan_attributes attrs;
    attrs.operation_name = Request::observability_identifier;
    attrs.operation_id = fmt::format("0x{:x}", inputs.opaque);
    if (inputs.session) {
      attrs.connection_id = inputs.session->id();
      attrs.last_remote_socket = inputs.session->remote_address();
      attrs.last_local_socket = inputs.session->local_address();
    }
    attrs.last_server_duration = inputs.last_server_duration;
    attrs.total_server_duration = inputs.total_server_duration;
    attrs.total_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - started_at_);
    return attrs;
  }

  [[nodiscard]] auto create_dispatch_span() const
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span =
      manager_->tracer()->create_span(tracing::operation::step_dispatch, span_);
#else
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span =
      manager_->tracer()->create_span(tracing::operation::step_dispatch, parent_span_);
#endif
    if (dispatch_span->uses_tags()) {
      dispatch_span->add_tag(tracing::attributes::dispatch::network_transport, "tcp");
      // Prefer the typed capability so the opaque is not formatted to a string on the hot path;
      // spans that do not support it (external tracers) fall back to the string tag.
      if (!dispatch_span->try_set_dispatch_operation_id(request.opaque)) {
        dispatch_span->add_tag(tracing::attributes::dispatch::operation_id,
                               fmt::format("0x{:x}", request.opaque));
      }
      // Prefer the typed capability so the local id is not string-tagged on the hot path; external
      // tracers fall back to the string tag.
      if (!dispatch_span->try_set_dispatch_local_id(session_->id())) {
        dispatch_span->add_tag(tracing::attributes::dispatch::local_id, session_->id());
      }
    }
    return dispatch_span;
  }

  void close_dispatch_span(const std::shared_ptr<couchbase::tracing::request_span>& dispatch_span,
                           const std::uint64_t server_duration_us) const
  {
    if (dispatch_span->uses_tags()) {
      // Prefer the typed capability so the server duration and peer socket are not string-tagged on
      // the hot path; external tracers fall back to the string tags.
      if (!dispatch_span->try_set_dispatch_result(
            server_duration_us, session_->remote_hostname(), session_->remote_port())) {
        dispatch_span->add_tag(tracing::attributes::dispatch::server_duration, server_duration_us);
        dispatch_span->add_tag(tracing::attributes::dispatch::peer_address,
                               session_->remote_hostname());
        dispatch_span->add_tag(tracing::attributes::dispatch::peer_port, session_->remote_port());
      }
      // The built-in span does not keep the server (canonical) address; only external tracers
      // consume these, and their SSO-sized tag names do not allocate.
      dispatch_span->add_tag(tracing::attributes::dispatch::server_address,
                             session_->canonical_hostname());
      dispatch_span->add_tag(tracing::attributes::dispatch::server_port,
                             session_->canonical_port_number());
    }
    dispatch_span->end();
  }
};

} // namespace couchbase::core::operations
