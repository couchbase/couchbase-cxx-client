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

#include "active_transaction_record.hxx"
#include "atr_ids.hxx"
#include "attempt_context_impl.hxx"
#include "cleanup_testing_hooks.hxx"
#include "uid_generator.hxx"

#include "internal/client_record.hxx"
#include "internal/logging.hxx"
#include "internal/transaction_fields.hxx"
#include "internal/transactions_cleanup.hxx"
#include "internal/utils.hxx"

#include <algorithm>
#include <chrono>
#include <functional>

namespace couchbase::core::transactions
{

transactions_cleanup_attempt::transactions_cleanup_attempt(const atr_cleanup_entry& entry)
  : atr_id_(entry.atr_id_)
  , attempt_id_(entry.attempt_id_)
  , success_(false)
  , state_(attempt_state::NOT_STARTED)
{
}

transactions_cleanup::transactions_cleanup(std::shared_ptr<core::cluster> cluster,
                                           const couchbase::transactions::transactions_config::built& config)
  : cluster_(cluster)
  , config_(config)
  , client_uuid_(uid_generator::next())
  , running_(false)
{
    running_ = config.cleanup_config.cleanup_client_attempts || config.cleanup_config.cleanup_lost_attempts;
    if (config.cleanup_config.cleanup_client_attempts) {
        cleanup_thr_ = std::thread(std::bind(&transactions_cleanup::attempts_loop, this));
    }
    if (config_.metadata_collection) {
        add_collection(
          { config_.metadata_collection->bucket, config_.metadata_collection->scope, config_.metadata_collection->collection });
    }
    for (auto& k : config_.cleanup_config.collections) {
        add_collection(k);
    }
}

static uint64_t
byteswap64(uint64_t val)
{
    std::size_t ii;
    uint64_t ret = 0;
    for (ii = 0; ii < sizeof(uint64_t); ii++) {
        ret <<= 8ull;
        ret |= val & 0xffull;
        val >>= 8ull;
    }
    return ret;
}

/**
 * ${Mutation.CAS} is written by kvengine with
 * 'macroToString(htonll(info.cas))'.  Discussed this with KV team and, though
 * there is consensus that this is off (htonll is definitely wrong, and a string
 * is an odd choice), there are clients (SyncGateway) that consume the current
 * string, so it can't be changed.  Note that only little-endian servers are
 * supported for Couchbase, so the 8 byte long inside the string will always be
 * little-endian ordered.
 *
 * Looks like: "0x000058a71dd25c15"
 * Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch
 * time in millionths of a second)
 */
static uint64_t
parse_mutation_cas(const std::string& cas)
{
    if (cas.empty()) {
        return 0;
    }
    return byteswap64(stoull(cas, nullptr, 16)) / 1000000;
}

static const std::string CLIENT_RECORD_DOC_ID = "_txn:client-record";
static const std::string FIELD_RECORDS = "records";
static const std::string FIELD_CLIENTS_ONLY = "clients";
static const std::string FIELD_CLIENTS = FIELD_RECORDS + "." + FIELD_CLIENTS_ONLY;
static const std::string FIELD_HEARTBEAT = "heartbeat_ms";
static const std::string FIELD_EXPIRES = "expires_ms";
static const std::string FIELD_OVERRIDE = "override";
static const std::string FIELD_OVERRIDE_EXPIRES = "expires";
static const std::string FIELD_OVERRIDE_ENABLED = "enabled";
static const std::string FIELD_NUM_ATRS = "num_atrs";

#define SAFETY_MARGIN_EXPIRY_MS 2000

template<class R, class P>
bool
transactions_cleanup::interruptable_wait(std::chrono::duration<R, P> delay)
{
    // wait for specified time, _or_ until the condition variable changes
    std::unique_lock<std::mutex> lock(mutex_);
    if (!running_.load()) {
        return false;
    }
    cv_.wait_for(lock, delay, [&]() { return !running_.load(); });
    return running_.load();
}

void
transactions_cleanup::clean_collection(const couchbase::transactions::transaction_keyspace& keyspace)
{
    // first make sure the collection is in the list
    while (running_.load()) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (collections_.end() == std::find(collections_.begin(), collections_.end(), keyspace)) {
                lost_attempts_cleanup_log->info(
                  "{} cleanup for {} ending, no longer in collection cleanup list", static_cast<void*>(this), keyspace);
                return;
            }
        }
        lost_attempts_cleanup_log->info("{} cleanup for {} starting", static_cast<void*>(this), keyspace);
        // we are running, and collection is in the list, so lets clean it.
        try {
            auto details = get_active_clients(keyspace, client_uuid_);

            auto all_atrs = atr_ids::all();

            std::chrono::microseconds cleanup_window =
              std::chrono::duration_cast<std::chrono::microseconds>(config_.cleanup_config.cleanup_window);
            auto start = std::chrono::steady_clock::now();
            lost_attempts_cleanup_log->info("{} {} active clients (including this one), {} ATRs to check in {}ms",
                                            static_cast<void*>(this),
                                            details.num_active_clients,
                                            all_atrs.size(),
                                            config_.cleanup_config.cleanup_window.count());

            for (auto it = all_atrs.begin() + details.index_of_this_client; it < all_atrs.end(); it += details.num_active_clients) {
                auto atrs_left_for_this_client =
                  std::distance(it, all_atrs.end()) / std::max<decltype(it)::difference_type>(1, details.num_active_clients);
                auto now = std::chrono::steady_clock::now();
                std::chrono::microseconds elapsed_in_cleanup_window = std::chrono::duration_cast<std::chrono::microseconds>(now - start);
                std::chrono::microseconds remaining_in_cleanup_window = cleanup_window - elapsed_in_cleanup_window;
                std::chrono::microseconds budget_for_this_atr = std::chrono::microseconds(
                  remaining_in_cleanup_window.count() / std::max<decltype(it)::difference_type>(1, atrs_left_for_this_client));
                // clean the ATR entry
                std::string atr_id = *it;
                if (!running_.load()) {
                    lost_attempts_cleanup_log->debug("{} cleanup of {} complete", static_cast<void*>(this), keyspace);
                    return;
                }

                try {
                    handle_atr_cleanup({ keyspace.bucket, keyspace.scope, keyspace.collection, atr_id });
                } catch (const std::exception& e) {
                    lost_attempts_cleanup_log->error(
                      "{} cleanup of atr {} failed with {}, moving on", static_cast<void*>(this), atr_id, e.what());
                }

                auto atr_end = std::chrono::steady_clock::now();
                std::chrono::microseconds atr_used = std::chrono::duration_cast<std::chrono::microseconds>(atr_end - now);
                std::chrono::microseconds atr_left = budget_for_this_atr - atr_used;

                // Too verbose to log, but leaving here commented as it may be useful later for internal debugging
                /*lost_attempts_cleanup_log->info("{} {} atrs_left_for_this_client={} elapsed_in_cleanup_window={}us "
                                            "remaining_in_cleanup_window={}us budget_for_this_atr={}us atr_used={}us atr_left={}us",
                                            bucket_name,
                                            atr_id,
                                            atrs_left_for_this_client,
                                            elapsed_in_cleanup_window.count(),
                                            remaining_in_cleanup_window.count(),
                                            budget_for_this_atr.count(),
                                            atr_used.count(),
                                            atr_left.count());*/

                if (atr_left.count() > 0 && atr_left.count() < 1000000000) { // safety check protects against bugs
                    std::this_thread::sleep_for(atr_left);
                }
            }
        } catch (const std::exception& ex) {
            lost_attempts_cleanup_log->error("{} cleanup failed with {}, trying again in 3 sec...", static_cast<void*>(this), ex.what());
            // we must have gotten an exception trying to get the client records.   Let's wait 3 sec and try again
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

const atr_cleanup_stats
transactions_cleanup::handle_atr_cleanup(const core::document_id& atr_id, std::vector<transactions_cleanup_attempt>* results)
{
    atr_cleanup_stats stats;
    auto atr = active_transaction_record::get_atr(cluster_, atr_id);
    if (atr) {
        // ok, loop through the attempts and clean them all.  The entry will
        // check if expired, nothing much to do here except call clean.
        stats.exists = true;
        stats.num_entries = atr->entries().size();
        for (const auto& entry : atr->entries()) {
            // If we were passed results, then we are testing, and want to set the
            // check_if_expired to false.
            atr_cleanup_entry cleanup_entry(entry, atr_id, *this, results == nullptr);
            try {
                if (results) {
                    results->emplace_back(cleanup_entry);
                }
                cleanup_entry.clean(lost_attempts_cleanup_log, results ? &results->back() : nullptr);
                if (results) {
                    results->back().success(true);
                }
            } catch (const std::exception& e) {
                lost_attempts_cleanup_log->error(
                  "{} cleanup of {} failed: {}, moving on", static_cast<void*>(this), cleanup_entry, e.what());
                if (results) {
                    results->back().success(false);
                }
            }
        }
    }
    return stats;
}

void
transactions_cleanup::create_client_record(const couchbase::transactions::transaction_keyspace& keyspace)

{
    try {
        auto id = document_id(keyspace.bucket, keyspace.scope, keyspace.collection, CLIENT_RECORD_DOC_ID);
        core::operations::mutate_in_request req{ id };
        req.store_semantics = couchbase::store_semantics::insert;
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::insert(FIELD_CLIENTS, tao::json::empty_object).xattr().create_path(),
              // subdoc::opcode::set_doc used in replace w/ empty path
              // ExtBinaryMetadata
              couchbase::mutate_in_specs::replace_raw({}, std::vector<std::byte>{ std::byte{ 0x00 } }),
          }
            .specs();
        wrap_durable_request(req, config_);
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        auto ec = config_.cleanup_hooks->client_record_before_create(keyspace.bucket);
        if (ec) {
            throw client_error(*ec, "client_record_before_create hook raised error");
        }
        cluster_->execute(
          req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        wrap_operation_future(f);

    } catch (const client_error& e) {
        lost_attempts_cleanup_log->trace("{} create_client_record got error {}", static_cast<void*>(this), e.what());
        auto ec = e.ec();
        switch (ec) {
            case FAIL_DOC_ALREADY_EXISTS:
                lost_attempts_cleanup_log->trace("{} client record already exists, moving on", static_cast<void*>(this));
                return;
            default:
                throw;
        }
    }
}

const client_record_details
transactions_cleanup::get_active_clients(const couchbase::transactions::transaction_keyspace& keyspace, const std::string& uuid)
{
    std::chrono::milliseconds min_retry(1000);
    if (config_.cleanup_config.cleanup_window < min_retry) {
        min_retry = config_.cleanup_config.cleanup_window;
    }
    return retry_op_exponential_backoff_timeout<client_record_details>(
      min_retry, std::chrono::seconds(1), config_.cleanup_config.cleanup_window, [this, keyspace, uuid]() -> client_record_details {
          client_record_details details;
          // Write our client record, return details.
          try {
              auto id = document_id{ keyspace.bucket, keyspace.scope, keyspace.collection, CLIENT_RECORD_DOC_ID };
              core::operations::lookup_in_request req{ id };
              req.specs =
                lookup_in_specs{
                    lookup_in_specs::get(FIELD_RECORDS).xattr(),
                    lookup_in_specs::get(subdoc::lookup_in_macro::vbucket).xattr(),
                }
                  .specs();
              wrap_request(req, config_);
              auto barrier = std::make_shared<std::promise<result>>();
              auto f = barrier->get_future();
              auto ec = config_.cleanup_hooks->client_record_before_get(keyspace.bucket);
              if (ec) {
                  throw client_error(*ec, "client_record_before_get hook raised error");
              }
              cluster_->execute(req, [barrier](core::operations::lookup_in_response resp) {
                  barrier->set_value(result::create_from_subdoc_response(resp));
              });
              auto res = wrap_operation_future(f);
              std::vector<std::string> active_client_uids;
              auto hlc = res.values[1].content_as();
              auto now_ms = now_ns_from_vbucket(hlc) / 1000000;
              details.override_enabled = false;
              details.override_expires = 0;
              if (res.values[0].status == subdoc_result::status_type::success) {
                  auto records = res.values[0].content_as();
                  lost_attempts_cleanup_log->trace("client records: {}", core::utils::json::generate(records));
                  for (const auto& [key, value] : records.get_object()) {
                      if (key == FIELD_OVERRIDE) {
                          for (const auto& [override, param] : value.get_object()) {
                              if (override == FIELD_OVERRIDE_ENABLED) {
                                  details.override_enabled = param.get_boolean();
                              } else if (override == FIELD_OVERRIDE_EXPIRES) {
                                  details.override_expires = param.as<std::uint64_t>();
                              }
                          }
                      } else if (key == FIELD_CLIENTS_ONLY) {
                          for (const auto& [other_client_uuid, cl] : value.get_object()) {
                              uint64_t heartbeat_ms = parse_mutation_cas(cl.at(FIELD_HEARTBEAT).get_string());
                              auto expires_ms = cl.at(FIELD_EXPIRES).as<std::uint64_t>();
                              auto expired_period = static_cast<int64_t>(now_ms) - static_cast<int64_t>(heartbeat_ms);
                              bool has_expired = expired_period >= static_cast<int64_t>(expires_ms) && now_ms > heartbeat_ms;
                              if (has_expired && other_client_uuid != uuid) {
                                  details.expired_client_ids.push_back(other_client_uuid);
                              } else {
                                  active_client_uids.push_back(other_client_uuid);
                              }
                          }
                      }
                  }
              }
              if (std::find(active_client_uids.begin(), active_client_uids.end(), uuid) == active_client_uids.end()) {
                  active_client_uids.push_back(uuid);
              }
              std::sort(active_client_uids.begin(), active_client_uids.end());
              auto this_idx =
                std::distance(active_client_uids.begin(), std::find(active_client_uids.begin(), active_client_uids.end(), uuid));
              details.num_active_clients = static_cast<std::uint32_t>(active_client_uids.size());
              details.index_of_this_client = static_cast<std::uint32_t>(this_idx);
              details.num_active_clients = static_cast<std::uint32_t>(active_client_uids.size());
              details.num_expired_clients = static_cast<std::uint32_t>(details.expired_client_ids.size());
              details.num_existing_clients = details.num_expired_clients + details.num_active_clients;
              details.client_uuid = uuid;
              details.cas_now_nanos = now_ms * 1000000;
              details.override_active = (details.override_enabled && details.override_expires > details.cas_now_nanos);
              lost_attempts_cleanup_log->trace("{} client details {}", static_cast<void*>(this), details);
              if (details.override_active) {
                  lost_attempts_cleanup_log->trace("{} override enabled, will not update record", static_cast<void*>(this));
                  return details;
              }

              // update client record, maybe cleanup some as well...
              core::operations::mutate_in_request mutate_req{ id };
              auto mut_specs = couchbase::mutate_in_specs{
                  couchbase::mutate_in_specs::upsert(FIELD_CLIENTS + "." + uuid + "." + FIELD_HEARTBEAT, subdoc::mutate_in_macro::cas)
                    .xattr()
                    .create_path(),
                  couchbase::mutate_in_specs::upsert(FIELD_CLIENTS + "." + uuid + "." + FIELD_EXPIRES,
                                                     config_.cleanup_config.cleanup_window.count() / 2 + SAFETY_MARGIN_EXPIRY_MS)
                    .xattr()
                    .create_path(),
                  couchbase::mutate_in_specs::upsert(FIELD_CLIENTS + "." + uuid + "." + FIELD_NUM_ATRS, atr_ids::all().size())
                    .xattr()
                    .create_path(),
              };
              for (std::size_t idx = 0; idx < std::min(details.expired_client_ids.size(), static_cast<std::size_t>(12)); idx++) {
                  lost_attempts_cleanup_log->trace("adding {} to list of clients to be removed when updating this client",
                                                   details.expired_client_ids[idx]);
                  mut_specs.push_back(couchbase::mutate_in_specs::remove(FIELD_CLIENTS + "." + details.expired_client_ids[idx]).xattr());
              }
              mutate_req.specs = mut_specs.specs();
              ec = config_.cleanup_hooks->client_record_before_update(keyspace.bucket);
              if (ec) {
                  throw client_error(*ec, "client_record_before_update hook raised error");
              }
              wrap_durable_request(mutate_req, config_);
              auto mutate_barrier = std::make_shared<std::promise<result>>();
              auto mutate_f = mutate_barrier->get_future();
              lost_attempts_cleanup_log->trace("updating record");
              cluster_->execute(mutate_req, [mutate_barrier](core::operations::mutate_in_response resp) {
                  mutate_barrier->set_value(result::create_from_subdoc_response(resp));
              });
              res = wrap_operation_future(mutate_f);

              // just update the cas, and return the details
              details.cas_now_nanos = res.cas;
              lost_attempts_cleanup_log->debug("{} get_active_clients found {}", static_cast<void*>(this), details);
              return details;
          } catch (const client_error& e) {
              auto ec = e.ec();
              switch (ec) {
                  case FAIL_DOC_NOT_FOUND:
                      lost_attempts_cleanup_log->debug("{} client record not found, creating new one", static_cast<void*>(this));
                      create_client_record(keyspace);
                      throw retry_operation("Client record didn't exist. Creating and retrying");
                  default:
                      throw; // retry_operation(fmt::format("got error '' while processing client record, retrying...", e.what()));
              }
          }
      });
}

void
transactions_cleanup::remove_client_record_from_all_buckets(const std::string& uuid)
{
    for (const auto& keyspace : collections_) {
        try {
            retry_op_exponential_backoff_timeout<void>(
              std::chrono::milliseconds(10), std::chrono::milliseconds(250), std::chrono::milliseconds(500), [this, keyspace, uuid]() {
                  try {
                      // insure a client record document exists...
                      create_client_record(keyspace);
                      // now, proceed to remove the client uuid if it exists
                      auto ec = config_.cleanup_hooks->client_record_before_remove_client(keyspace.bucket);
                      if (ec) {
                          throw client_error(*ec, "client_record_before_remove_client hook raised error");
                      }
                      document_id id{ keyspace.bucket, keyspace.scope, keyspace.collection, CLIENT_RECORD_DOC_ID };
                      core::operations::mutate_in_request req{ id };
                      req.specs =
                        couchbase::mutate_in_specs{
                            couchbase::mutate_in_specs::remove(FIELD_CLIENTS + "." + uuid).xattr(),
                        }
                          .specs();
                      wrap_durable_request(req, config_);
                      auto barrier = std::make_shared<std::promise<result>>();
                      auto f = barrier->get_future();
                      cluster_->execute(req, [barrier](core::operations::mutate_in_response resp) {
                          barrier->set_value(result::create_from_subdoc_response(resp));
                      });
                      wrap_operation_future(f);
                      lost_attempts_cleanup_log->debug("{} removed {} from {}", static_cast<void*>(this), uuid, keyspace);
                  } catch (const client_error& e) {
                      lost_attempts_cleanup_log->debug("{} error removing client records {}", static_cast<void*>(this), e.what());
                      auto ec = e.ec();
                      switch (ec) {
                          case FAIL_DOC_NOT_FOUND:
                              lost_attempts_cleanup_log->debug("{} no client record in {}, ignoring", static_cast<void*>(this), keyspace);
                              return;
                          case FAIL_PATH_NOT_FOUND:
                              lost_attempts_cleanup_log->debug(
                                "{} client {} not in client record for {}, ignoring", static_cast<void*>(this), uuid, keyspace);
                              return;
                          default:
                              throw retry_operation("retry remove until timeout");
                      }
                  }
              });
        } catch (const std::exception& e) {
            lost_attempts_cleanup_log->error(
              "{} Error removing client record {} from {}: {}", static_cast<void*>(this), uuid, keyspace, e.what());
        }
    }
}

const atr_cleanup_stats
transactions_cleanup::force_cleanup_atr(const core::document_id& atr_id, std::vector<transactions_cleanup_attempt>& results)
{
    lost_attempts_cleanup_log->trace("{} starting force_cleanup_atr: atr_id {}", static_cast<void*>(this), atr_id);
    return handle_atr_cleanup(atr_id, &results);
}

void
transactions_cleanup::force_cleanup_entry(atr_cleanup_entry& entry, transactions_cleanup_attempt& attempt)
{
    try {
        entry.clean(attempt_cleanup_log, &attempt);
        attempt.success(true);

    } catch (const std::runtime_error& e) {
        attempt_cleanup_log->error("error attempting to clean {}: {}", entry, e.what());
        attempt.success(false);
    }
}

void
transactions_cleanup::force_cleanup_attempts(std::vector<transactions_cleanup_attempt>& results)
{
    attempt_cleanup_log->trace("starting force_cleanup_attempts");
    while (atr_queue_.size() > 0) {
        auto entry = atr_queue_.pop(false);
        if (!entry) {
            attempt_cleanup_log->error("pop failed to return entry, but queue size {}", atr_queue_.size());
            return;
        }
        results.emplace_back(*entry);
        try {
            entry->clean(attempt_cleanup_log, &results.back());
            results.back().success(true);
        } catch (std::runtime_error&) {
            results.back().success(false);
        }
    }
}

void
transactions_cleanup::attempts_loop()
{
    try {
        attempt_cleanup_log->debug("cleanup attempts loop starting...");
        while (interruptable_wait(cleanup_loop_delay_)) {
            while (auto entry = atr_queue_.pop()) {
                if (!running_.load()) {
                    attempt_cleanup_log->debug("loop stopping - {} entries on queue", atr_queue_.size());
                    return;
                }
                if (entry) {
                    attempt_cleanup_log->trace("beginning cleanup on {}", *entry);
                    try {
                        entry->clean(attempt_cleanup_log);
                    } catch (...) {
                        // catch everything as we don't want to raise out of this thread
                        attempt_cleanup_log->info("got error cleaning {}, leaving for lost txn cleanup", entry.value());
                    }
                }
            }
        }
        attempt_cleanup_log->info("stopping - {} entries on queue", atr_queue_.size());
    } catch (const std::runtime_error& e) {
        attempt_cleanup_log->error("got error \"{}\" in attempts_loop", e.what());
    }
}

void
transactions_cleanup::add_attempt(attempt_context& ctx)
{
    auto& ctx_impl = static_cast<attempt_context_impl&>(ctx);
    switch (ctx_impl.state()) {
        case attempt_state::NOT_STARTED:
        case attempt_state::COMPLETED:
        case attempt_state::ROLLED_BACK:
            attempt_cleanup_log->trace("attempt in state {}, not adding to cleanup", attempt_state_name(ctx_impl.state()));
            return;
        default:
            if (config_.cleanup_config.cleanup_client_attempts) {
                attempt_cleanup_log->debug("adding attempt {} to cleanup queue", ctx_impl.id());
                atr_queue_.push(ctx);
            } else {
                attempt_cleanup_log->trace("not cleaning client attempts, ignoring {}", ctx_impl.id());
            }
    }
}

void
transactions_cleanup::add_collection(couchbase::transactions::transaction_keyspace keyspace)
{

    if (keyspace.valid() && config_.cleanup_config.cleanup_lost_attempts) {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = std::find(collections_.begin(), collections_.end(), keyspace);
        if (it == collections_.end()) {
            collections_.emplace_back(std::move(keyspace));
            // start cleaning right away
            lost_attempt_cleanup_workers_.emplace_back([this, keyspace = collections_.back()]() { this->clean_collection(keyspace); });
        }
        lock.unlock();
        lost_attempts_cleanup_log->info("added {} to lost transaction cleanup", keyspace);
    }
}

void
transactions_cleanup::close()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        running_ = false;
        cv_.notify_all();
    }
    if (cleanup_thr_.joinable()) {
        cleanup_thr_.join();
        attempt_cleanup_log->info("cleanup attempt thread closed");
    }
    for (auto& t : lost_attempt_cleanup_workers_) {
        lost_attempts_cleanup_log->info("shutting down all lost attempt threads...");
        if (t.joinable()) {
            t.join();
        }
    }
    lost_attempts_cleanup_log->info("all lost attempt cleanup threads closed");
    if (true) {
        remove_client_record_from_all_buckets(client_uuid_);
    }
}

transactions_cleanup::~transactions_cleanup()
{
    close();
}
} // namespace couchbase::core::transactions
