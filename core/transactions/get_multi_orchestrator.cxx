/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include "get_multi_orchestrator.hxx"

#include "active_transaction_record.hxx"
#include "attempt_context_impl.hxx"
#include "core/document_id.hxx"
#include "core/transactions/error_class.hxx"
#include "core/transactions/exceptions.hxx"
#include "forward_compat.hxx"
#include "get_multi_transaction_id.hxx"

#include <algorithm>
#include <chrono>
#include <exception>
#include <queue>
#include <vector>

namespace couchbase::core::transactions
{
enum class get_multi_mode : std::uint8_t {
  prioritise_latency,
  disable_read_skew_detection,
  prioritise_read_skew_detection,
};

namespace
{
auto
convert_mode(transaction_get_multi_mode mode) -> get_multi_mode
{
  switch (mode) {
    case transaction_get_multi_mode::prioritise_latency:
      return get_multi_mode::prioritise_latency;
    case transaction_get_multi_mode::disable_read_skew_detection:
      return get_multi_mode::disable_read_skew_detection;
    case transaction_get_multi_mode::prioritise_read_skew_detection:
      return get_multi_mode::prioritise_read_skew_detection;
  }
  return get_multi_mode::prioritise_latency;
}

auto
convert_mode(transaction_get_multi_replicas_from_preferred_server_group_mode mode) -> get_multi_mode
{
  switch (mode) {
    case transaction_get_multi_replicas_from_preferred_server_group_mode::prioritise_latency:
      return get_multi_mode::prioritise_latency;
    case transaction_get_multi_replicas_from_preferred_server_group_mode::
      disable_read_skew_detection:
      return get_multi_mode::disable_read_skew_detection;
    case transaction_get_multi_replicas_from_preferred_server_group_mode::
      prioritise_read_skew_detection:
      return get_multi_mode::prioritise_read_skew_detection;
  }
  return get_multi_mode::prioritise_latency;
}
} // namespace

enum class get_multi_phase : std::uint8_t {
  first_doc_fetch,
  subsequent_to_first_doc_fetch,
  discovered_docs_in_t1,
  resolving_t1_atr_entry_missing,
};

struct get_multi_spec {
  std::size_t index{};
  core::document_id id{};
};

struct get_multi_result {
  get_multi_spec spec;
  std::optional<transaction_get_result> get_result{};
  std::exception_ptr error{ nullptr };

  [[nodiscard]] auto doc_exists() const -> bool
  {
    return get_result.has_value();
  }

  [[nodiscard]] auto has_transactional_metadata() const -> bool
  {
    if (const auto& result = get_result; result) {
      return result->links().atr_id().has_value();
    }
    return false;
  }

  [[nodiscard]] auto extract_transaction_id() const -> std::optional<transaction_id>
  {
    if (const auto& result = get_result; result) {
      return make_transaction_id(result->links().staged_transaction_id(),
                                 result->links().staged_attempt_id(),
                                 result->links().staged_operation_id());
    }
    return {};
  }

  void copy_content_from_staged_operation_into_result()
  {
    if (auto& result = get_result; result) {
      result->content(result->links().staged_content_json_or_binary());
    }
  }

  [[nodiscard]] auto extract_atr_document_id() const -> std::optional<core::document_id>
  {
    if (const auto& result = get_result; result) {
      auto id = result->links().atr_id();
      auto bucket = result->links().atr_bucket_name();
      auto scope = result->links().atr_scope_name();
      auto collection = result->links().atr_collection_name();

      if (id && bucket && scope && collection) {
        return core::document_id{
          bucket.value(),
          scope.value(),
          collection.value(),
          id.value(),
        };
      }
    }
    return {};
  }
};

struct classified_error {
  core::transactions::error_class error_class;
  core::transactions::external_exception cause;
};

auto
classify_error(const std::exception_ptr& err) -> classified_error
{
  try {
    std::rethrow_exception(err);
  } catch (const op_exception& e) {
    return classified_error{ error_class_from_external_exception(e.cause()), e.cause() };
  } catch (const transaction_operation_failed& e) {
    return classified_error{ e.ec(), e.cause() };
  } catch (...) {
    return classified_error{ error_class::FAIL_OTHER, external_exception::UNKNOWN };
  }
  return classified_error{ error_class::FAIL_OTHER, external_exception::UNKNOWN };
}

auto
contains_mutation(const std::optional<std::vector<doc_record>>& mutated_ids,
                  const core::document_id& id) -> bool
{
  if (!mutated_ids) {
    return false;
  }
  return std::any_of(mutated_ids->begin(), mutated_ids->end(), [&id](const auto& mutation) {
    return mutation == id;
  });
}

/**
 * Drives a single transactional get-multi operation: fetch N documents and, where there is
 * evidence of read skew, resolve it to return a consistent snapshot.
 *
 * This implements the algorithm in the "ExtGetMulti" transactions spec
 * (couchbase-transactions-specs/transactions-get-multi.md). The spec is written as a set of
 * function-like sections that call one another; the methods here mirror those sections:
 *   - fetch_multiple_documents()  -> "Fetching multiple documents"
 *   - fetch_individual_document() -> "Fetching an individual document"
 *   - disambiguate_results()      -> "Document disambiguation"
 *   - resolve_read_skew()         -> "Read skew resolution"
 * The driving loop (spec "GetMulti" / "Implementation") is realised asynchronously: each fetch
 * round completes into disambiguate_results(), which either finishes the operation or schedules a
 * further round via retry()/reset_and_retry(). The spec's control-flow signals (Continue, Retry,
 * ResetAndRetry, Completed, BoundExceeded) are expressed as those direct method calls rather than
 * as returned values.
 */
class get_multi_operation : public std::enable_shared_from_this<get_multi_operation>
{
public:
  static constexpr std::size_t default_number_of_concurrent_requests{ 100 };

  ~get_multi_operation() = default;
  get_multi_operation(const get_multi_operation&) = delete;
  get_multi_operation(get_multi_operation&&) = delete;
  auto operator=(const get_multi_operation&) -> get_multi_operation& = delete;
  auto operator=(get_multi_operation&&) -> get_multi_operation& = delete;

  get_multi_operation(
    std::shared_ptr<attempt_context_impl> attempt,
    const std::vector<core::document_id>& ids,
    get_multi_mode mode,
    std::size_t number_of_concurrent_requests,
    bool use_replicas,
    utils::movable_function<void(std::exception_ptr, std::vector<get_multi_result>)> callback)
    : attempt_{ std::move(attempt) }
    , responses_left_{ ids.size() }
    , mode_{ mode }
    , number_of_concurrent_requests_{ number_of_concurrent_requests }
    , use_replicas_{ use_replicas } // TODO(SA): weird warning
                                    // NOLINTNEXTLINE(bugprone-throw-keyword-missing)
    , callback_{ std::move(callback) }
  {
    results_.resize(ids.size());
    for (std::size_t index = 0; index < ids.size(); ++index) {
      specs_.emplace_back(get_multi_spec{ index, ids[index] });
      to_fetch_.emplace(get_multi_spec{ index, ids[index] });
    }
  }

  void handle_individual_document_error(get_multi_spec spec, const std::exception_ptr& err)
  {
    auto index = spec.index;

    if (err == nullptr) {
      results_[index] = { std::move(spec), {}, {} };
    } else {
      auto [ec, cause] = classify_error(err);

      if (cause == DOCUMENT_UNRETRIEVABLE_EXCEPTION || cause == DOCUMENT_NOT_FOUND_EXCEPTION) {
        results_[index] = { std::move(spec), {}, {} };
      } else {
        results_[index] = { std::move(spec), {}, err };
      }
    }

    --responses_left_;
  }

  void handle_individual_document_success(get_multi_spec spec,
                                          std::optional<transaction_get_result> res)
  {
    auto index = spec.index;
    results_[index] = { std::move(spec), std::move(res), {} };
    --responses_left_;
  }

  auto pop_next_spec() -> std::optional<get_multi_spec>
  {
    if (to_fetch_.empty()) {
      return {};
    }
    auto spec = to_fetch_.front();
    to_fetch_.pop();
    return spec;
  }

  void invoke_callback(std::exception_ptr error = nullptr)
  {
    if (auto callback = std::move(callback_); callback) {
      callback(std::move(error), std::move(results_));
    }
  }

  /**
   * Realises the spec's "Retry" signal: refetch the given subset of specs (retaining the rest of
   * the operation state and the current phase), then re-run disambiguation when they complete.
   */
  void retry(std::queue<get_multi_spec> specs)
  {
    to_fetch_ = std::move(specs);
    responses_left_ = to_fetch_.size();
    fetch_multiple_documents();
  }

  /**
   * Realises the spec's "ResetAndRetry" signal: discard the fetched results and refetch every
   * original spec from scratch (the state is too complex to resolve incrementally).
   */
  void reset_and_retry()
  {
    std::queue<get_multi_spec> to_fetch;
    for (const auto& spec : specs_) {
      to_fetch.emplace(spec);
    }
    // Restore one slot per spec before refetching: results are stored by spec index, so the
    // container must stay sized to the spec count. A bare clear() would leave size() at 0 and the
    // subsequent index assignments from the refetch handlers would be out of bounds.
    results_.clear();
    results_.resize(specs_.size());
    if (phase_ != get_multi_phase::first_doc_fetch) {
      phase_ = get_multi_phase::subsequent_to_first_doc_fetch;
    }
    retry(std::move(to_fetch));
  }

  void completed()
  {
    invoke_callback();
  }

  /**
   * Implements the spec's "Read skew resolution" section. Reaches here only when exactly one other
   * transaction T1 is involved in the fetched documents. Fetches T1's ATR entry and, based on its
   * state and the current phase, either: returns the documents as-is (T1 not committed), refetches
   * the documents we missed were in T1 (committed, "SubsequentToFirstDocFetch"/"DiscoveredDocsInT1"
   * branches), disambiguates a missing ATR entry, or resets and retries.
   */
  void resolve_read_skew()
  {
    std::string other_attempt_id;
    document_id atr_document_id{};

    for (const auto& result : results_) {
      if (!result.doc_exists()) {
        continue;
      }

      auto txn_id = result.extract_transaction_id();
      if (!txn_id) {
        continue;
      }
      if (other_attempt_id.empty()) {
        auto atr_id = result.extract_atr_document_id();
        if (!atr_id) {
          continue;
        }

        other_attempt_id = txn_id->attempt;
        atr_document_id = atr_id.value();

      } else if (other_attempt_id != txn_id->attempt) {
        return reset_and_retry();
      }
    }

    if (other_attempt_id.empty()) {
      return reset_and_retry();
    }

    active_transaction_record::get_atr(
      attempt_->cluster_ref(),
      atr_document_id,
      [other_attempt_id, self = shared_from_this()](std::error_code ec,
                                                    std::optional<active_transaction_record> atr) {
        if (ec) {
          return self->reset_and_retry();
        }

        std::vector<get_multi_result*> fetched_in_t1;
        for (auto& result : self->results_) {
          if (result.doc_exists()) {
            auto txn_id = result.extract_transaction_id();
            if (txn_id && txn_id->attempt == other_attempt_id) {
              fetched_in_t1.push_back(&result);
            }
          }
        }

        std::optional<atr_entry> attempt{};
        if (atr) {
          for (const auto& entry : atr->entries()) {
            if (entry.attempt_id() == other_attempt_id) {
              attempt = entry;
            }
          }
        }
        if (attempt) {
          // Reading T1's ATR entry must run the GETS_READING_ATR forward-compatibility check, as
          // the normal MAV get path does. The raw get_multi fetch performs no ATR read, so this
          // check would otherwise be skipped entirely.
          if (auto fc_err = check_forward_compat(forward_compat_stage::GETS_READING_ATR,
                                                 attempt->forward_compat());
              fc_err) {
            return self->invoke_callback(std::make_exception_ptr(fc_err.value()));
          }
          auto state = attempt->state();

          if (state == attempt_state::PENDING || state == attempt_state::ABORTED) {
            return self->completed();
          }

          if (state == attempt_state::COMMITTED) {
            if (self->phase_ == get_multi_phase::subsequent_to_first_doc_fetch) {
              // The read-skew victims are documents we fetched WITHOUT T1's metadata (e.g. read
              // before T1 committed) yet which T1's ATR entry records as mutated. They must be
              // re-fetched to obtain a consistent snapshot. Mirror the reference SDK: build this
              // set from the docs not fetched as part of T1 and filter by T1's ATR document list
              // -- not from the docs that happen to already carry T1's metadata (those are the
              // ones we have already resolved, not the stale ones).
              std::vector<get_multi_result> were_in_t1;
              for (auto& result : self->results_) {
                if (!result.doc_exists()) {
                  continue;
                }
                const auto txn_id = result.extract_transaction_id();
                const bool fetched_as_part_of_t1 =
                  txn_id.has_value() && txn_id->attempt == other_attempt_id;
                if (fetched_as_part_of_t1) {
                  continue;
                }
                if (contains_mutation(attempt->inserted_ids(), result.spec.id) ||
                    contains_mutation(attempt->replaced_ids(), result.spec.id) ||
                    contains_mutation(attempt->removed_ids(), result.spec.id)) {
                  were_in_t1.emplace_back(result);
                }
              }

              if (were_in_t1.empty()) {
                for (auto& result : fetched_in_t1) {
                  result->copy_content_from_staged_operation_into_result();
                }
                return self->completed();
              }

              self->phase_ = get_multi_phase::discovered_docs_in_t1;

              std::queue<get_multi_spec> to_fetch;
              for (const auto& result : were_in_t1) {
                to_fetch.emplace(result.spec);
              }
              return self->retry(std::move(to_fetch));
            }
            if (self->phase_ == get_multi_phase::discovered_docs_in_t1) {
              for (auto& result : fetched_in_t1) {
                result->copy_content_from_staged_operation_into_result();
              }
              return self->completed();
            }
          }

          return self->reset_and_retry();
        }

        // There is no ATR record for T1: either the ATR document holds no entry for
        // T1, or the ATR document itself is absent (get_atr yields an empty record on
        // document-not-found). Both mean the same thing and are handled identically
        // here, per the spec's "ATR entry missing" case.
        if (self->phase_ == get_multi_phase::resolving_t1_atr_entry_missing) {
          if (fetched_in_t1.empty()) {
            return self->reset_and_retry();
          }
          return self->completed();
        }
        self->phase_ = get_multi_phase::resolving_t1_atr_entry_missing;
        std::queue<get_multi_spec> to_fetch;
        for (const auto& result : fetched_in_t1) {
          to_fetch.emplace(result->spec);
        }
        return self->retry(std::move(to_fetch));
      });
  }

  /**
   * Implements the spec's "Document disambiguation" section. Runs once a fetch round has completed.
   * Counts how many distinct other transactions the fetched documents are involved in: zero means
   * no detectable read skew (done); exactly one proceeds to read-skew resolution; two or more is
   * too complex to resolve and resets. Also enforces the operation deadline (see spec "Timeouts").
   */
  void disambiguate_results()
  {
    if (std::chrono::steady_clock::now() >= deadline_) {
      // The read-skew resolution bound has been exceeded (spec "Global signal handling" for a
      // BoundExceeded signal). Disambiguation is only entered once a fetch round has fully
      // completed, so results_ holds an entry for every spec. Per the spec, when we have something
      // for every document we return it as a best-effort snapshot -- which may still contain read
      // skew we ran out of time to resolve -- rather than failing the operation. Any genuine
      // per-document fetch error recorded in results_ is still surfaced by the completion callback.
      // The bound is checked here between fetch rounds; an individual fetch still runs to the
      // regular key-value timeout, so a single slow document can overshoot the bound within a
      // round.
      return completed();
    }

    std::set<transaction_id> transaction_ids{};

    for (const auto& result : results_) {
      if (const auto& id = result.extract_transaction_id(); id) {
        transaction_ids.insert(id.value());
      }
    }

    switch (transaction_ids.size()) {
      case 0:
        // no read skew
        return completed();

      case 1:
        // one other transaction is involved, maybe we can resolve
        return resolve_read_skew();

      default:
        // several transactions, too complex to resolve
        return reset_and_retry();
    }
  }

  /**
   * Implements the spec's "Fetching an individual document" section. Performs a raw lookupIn for
   * the document body and transactional metadata (explicitly not a MAV read), runs the
   * forward-compatibility checks the normal get path would, and records the result. As each fetch
   * completes it pulls the next queued spec, and once the round is drained it advances the deadline
   * per the mode and hands off to disambiguate_results().
   */
  void fetch_individual_document(const get_multi_spec& spec)
  {
    auto handler = [spec, self = shared_from_this()](const std::exception_ptr& error,
                                                     std::optional<transaction_get_result> res) {
      if (!self->callback_) {
        // The operation already completed (e.g. a sibling fetch hit a forward-compat failure and
        // invoked the callback, moving results_ out). Concurrently-dispatched fetches can still
        // complete afterwards; ignore them so we never index the moved-from results_ vector.
        return;
      }
      if (res) {
        // The raw fetch does not perform MAV, so the forward-compatibility checks the normal get
        // path would run must be done here. Per spec, run two checks in order: GET_MULTI_GET
        // ("GM_G", so newer clients can block getMulti specifically) then GETS ("G", which older
        // clients use and which anything blocking get likely needs to block getMulti too).
        for (const auto stage :
             { forward_compat_stage::GET_MULTI_GET, forward_compat_stage::GETS }) {
          if (auto forward_compat_err = check_forward_compat(stage, res->links().forward_compat());
              forward_compat_err) {
            self->invoke_callback(std::make_exception_ptr(forward_compat_err.value()));
            return;
          }
        }
        self->handle_individual_document_success(spec, std::move(res));
      } else {
        self->handle_individual_document_error(spec, error);
      }
      if (auto next_spec = self->pop_next_spec(); next_spec) {
        return self->fetch_individual_document(next_spec.value());
      }
      if (self->responses_left_ == 0) {
        if (self->phase_ == get_multi_phase::first_doc_fetch) {
          // Set the read-skew bound exactly once, on the first-doc-fetch -> subsequent transition,
          // and leave it fixed for the remaining rounds. This matches the reference (JVM sets the
          // bound only when phase == FIRST_DOC_FETCH; reset() carries it through unchanged):
          // recomputing it every round in latency mode would push the 100ms bound indefinitely into
          // the future and defeat the best-effort BoundExceeded return.
          switch (self->mode_) {
            case get_multi_mode::disable_read_skew_detection:
              break;
            case get_multi_mode::prioritise_latency:
              self->deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds{ 100 };
              break;
            case get_multi_mode::prioritise_read_skew_detection:
              self->deadline_ = self->attempt_->expiry_time();
              break;
          }
          self->phase_ = get_multi_phase::subsequent_to_first_doc_fetch;
        }

        if (self->mode_ == get_multi_mode::disable_read_skew_detection) {
          return self->invoke_callback();
        }
        self->disambiguate_results();
      }
    };
    // get_multi must fetch the raw document body and transactional metadata with a plain lookupIn
    // -- explicitly NOT a MAV read, which would perform its own ATR lookup and resolution and rob
    // the orchestrator of the metadata it needs to detect and resolve read skew itself. Adapt
    // get_doc's classified-error callback into the (exception, result) form the handler expects,
    // following the spec's per-document error handling: a not-found/unretrievable document is
    // reported as absent rather than as a failure.
    attempt_->get_doc(
      spec.id,
      use_replicas_,
      [handler = std::move(handler)](std::optional<error_class> ec,
                                     std::optional<external_exception> cause,
                                     std::optional<std::string> message,
                                     std::optional<transaction_get_result> res) mutable {
        if (res) {
          return handler(nullptr, std::move(res));
        }
        if (!ec || ec == FAIL_DOC_NOT_FOUND || cause == DOCUMENT_NOT_FOUND_EXCEPTION ||
            cause == DOCUMENT_UNRETRIEVABLE_EXCEPTION) {
          return handler(nullptr, std::nullopt);
        }
        auto err = transaction_operation_failed(
          ec.value(), message.value_or("error fetching document in get_multi"));
        if (cause) {
          err.cause(cause.value());
        }
        if (ec == FAIL_EXPIRY) {
          err = err.expired();
        } else if (ec == FAIL_HARD) {
          err = err.no_rollback();
        }
        return handler(std::make_exception_ptr(err), std::nullopt);
      });
  }

  /**
   * Implements the spec's "Fetching multiple documents" section. Schedules the queued specs to be
   * fetched concurrently, with no more than default_number_of_concurrent_requests in flight at
   * once; each completing fetch starts the next, so the cap is maintained without a barrier.
   */
  void fetch_multiple_documents()
  {
    std::size_t requests_scheduled{ 0 };
    while (requests_scheduled < number_of_concurrent_requests_) {
      if (auto next_spec = pop_next_spec(); next_spec) {
        fetch_individual_document(next_spec.value());
        ++requests_scheduled;
      } else {
        break;
      }
    }
  }

private:
  std::shared_ptr<attempt_context_impl> attempt_;
  std::size_t responses_left_;
  get_multi_mode mode_;
  std::size_t number_of_concurrent_requests_;
  bool use_replicas_;
  utils::movable_function<void(std::exception_ptr, std::vector<get_multi_result>)> callback_;

  std::vector<get_multi_spec> specs_{};
  std::queue<get_multi_spec> to_fetch_{};
  std::vector<get_multi_result> results_{};

  std::chrono::steady_clock::time_point deadline_{};
  get_multi_phase phase_{ get_multi_phase::first_doc_fetch };
};

get_multi_orchestrator::get_multi_orchestrator(std::shared_ptr<attempt_context_impl> attempt,
                                               std::vector<core::document_id> ids)
  : attempt_{ std::move(attempt) }
  , ids_{ std::move(ids) }
{
}

void
get_multi_orchestrator::get_multi(
  transaction_get_multi_mode mode,
  utils::movable_function<void(std::exception_ptr, std::optional<transaction_get_multi_result>)>&&
    cb)
{
  auto operation = std::make_shared<get_multi_operation>(
    attempt_,
    ids_,
    convert_mode(mode),
    get_multi_operation::default_number_of_concurrent_requests,
    false,
    [cb = std::move(cb), self = shared_from_this()](std::exception_ptr error,
                                                    std::vector<get_multi_result> results) {
      if (error) {
        return cb(std::move(error), {});
      }

      std::vector<std::optional<codec::encoded_value>> content{};
      content.resize(results.size());
      std::exception_ptr first_error{ nullptr };
      for (auto& result : results) {
        if (auto& get = result.get_result; get) {
          content[result.spec.index] = get->content();
        }
        if (result.error && !first_error) {
          first_error = std::move(result.error);
        }
      }
      if (first_error) {
        return cb(first_error, transaction_get_multi_result{ content });
      }
      return cb(nullptr, transaction_get_multi_result{ content });
    });
  operation->fetch_multiple_documents();
}

void
get_multi_orchestrator::get_multi_replicas_from_preferred_server_group(
  transaction_get_multi_replicas_from_preferred_server_group_mode mode,
  utils::movable_function<
    void(std::exception_ptr,
         std::optional<transaction_get_multi_replicas_from_preferred_server_group_result>)>&& cb)
{
  auto operation = std::make_shared<get_multi_operation>(
    attempt_,
    ids_,
    convert_mode(mode),
    get_multi_operation::default_number_of_concurrent_requests,
    true,
    [cb = std::move(cb), self = shared_from_this()](std::exception_ptr error,
                                                    std::vector<get_multi_result> results) {
      if (error) {
        return cb(std::move(error), {});
      }

      std::vector<std::optional<codec::encoded_value>> content{};
      content.resize(results.size());
      std::exception_ptr first_error{ nullptr };
      for (auto& result : results) {
        if (auto& get = result.get_result; get) {
          content[result.spec.index] = get->content();
        }
        if (result.error && !first_error) {
          first_error = std::move(result.error);
        }
      }

      if (first_error) {
        return cb(first_error,
                  transaction_get_multi_replicas_from_preferred_server_group_result{ content });
      }

      return cb(nullptr,
                transaction_get_multi_replicas_from_preferred_server_group_result{ content });
    });
  operation->fetch_multiple_documents();
}
} // namespace couchbase::core::transactions
