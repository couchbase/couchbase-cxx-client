/* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "logger.hxx"

#include "configuration.hxx"
#include "core/logger/level.hxx"
#include "custom_rotating_file_sink.hxx"
#include "logger_unit_test_api.hxx"

#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/common.h>
#include <spdlog/details/console_globals.h>
#include <spdlog/details/fmt_helper.h>
#include <spdlog/details/os.h>
#include <spdlog/logger.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
const std::string file_logger_name{ "couchbase_cxx_client_file_logger" };
const std::string protocol_logger_name{ "couchbase_cxx_client_protocol_logger" };

/**
 * Custom log pattern which the loggers will use. %* is the thread id; see thread_id_flag.
 */
const std::string log_pattern{ "[%Y-%m-%d %T.%e] %4oms [%^%4!l%$] [%P,%*] %v" };

// Every call the logger makes into spdlog's registry runs under this lock, and notify_fork() holds
// it across the fork, so no registry mutex is left locked in the child. Taken after fork_mutex and
// before every other logger lock.
std::mutex registry_mutex;

std::mutex after_registry_unlock_mutex;
std::function<void()> after_registry_unlock_callback{};
std::atomic<bool> after_registry_unlock_installed{ false };

class scoped_registry_lock
{
public:
  scoped_registry_lock() = default;
  scoped_registry_lock(const scoped_registry_lock&) = delete;
  scoped_registry_lock(scoped_registry_lock&&) = delete;
  auto operator=(const scoped_registry_lock&) -> scoped_registry_lock& = delete;
  auto operator=(scoped_registry_lock&&) -> scoped_registry_lock& = delete;

  ~scoped_registry_lock()
  {
    lock_.unlock();
    if (!after_registry_unlock_installed.load(std::memory_order_acquire)) {
      return;
    }
    std::function<void()> callback{};
    {
      const std::scoped_lock hook_lock(after_registry_unlock_mutex);
      callback = after_registry_unlock_callback;
    }
    if (callback) {
      try {
        callback();
      } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
        // unit test callback; registry_mutex is already unlocked
      }
    }
  }

private:
  std::unique_lock<std::mutex> lock_{ registry_mutex };
};

// spdlog caches each thread's id in a thread_local, so in a forked child the forking thread keeps
// reporting its id in the parent. %* replaces that one stale id with the thread's id in the child.
std::size_t forking_thread_id{ 0 }; // written by prepare, read by child, on the forking thread
std::atomic<std::size_t> stale_thread_id{ 0 };
std::atomic<std::size_t> forked_thread_id{ 0 };

class thread_id_flag : public spdlog::custom_flag_formatter
{
public:
  void format(const spdlog::details::log_msg& msg,
              const std::tm& /* tm */,
              spdlog::memory_buf_t& dest) override
  {
    auto id = msg.thread_id;
    if (id == stale_thread_id.load(std::memory_order_relaxed)) {
      id = forked_thread_id.load(std::memory_order_relaxed);
    }
    spdlog::details::fmt_helper::append_int(id, dest);
  }

  [[nodiscard]] auto clone() const -> std::unique_ptr<custom_flag_formatter> override
  {
    return std::make_unique<thread_id_flag>();
  }
};

auto
make_formatter() -> std::unique_ptr<spdlog::formatter>
{
  auto formatter = std::make_unique<spdlog::pattern_formatter>();
  formatter->add_flag<thread_id_flag>('*').set_pattern(log_pattern);
  return formatter;
}

struct sdk_async_pool {
  explicit sdk_async_pool(std::shared_ptr<spdlog::details::thread_pool> p)
    : pool{ std::move(p) }
  {
  }

  std::shared_ptr<spdlog::details::thread_pool> pool{};
  std::atomic<bool> shutting_down{ false };
};

// Guards file_logger, protocol_logger, and their pool handles; declared first to outlive them at
// static teardown. Threads cache weak references and lock strong ones only during a call.
std::mutex file_logger_mutex;

/**
 * Instances of spdlog (async) file logger.
 * The files logger requires a rotating file sink which is manually configured
 * from the parsed settings.
 * The loggers act as a handle to the sinks. They do the processing of log
 * messages and send them to the sinks, which do the actual writing (to file,
 * to stream etc.) or further processing.
 */
std::shared_ptr<spdlog::logger> file_logger{};
std::optional<std::weak_ptr<sdk_async_pool>> file_logger_pool{};
std::atomic_int file_logger_version{ 0 };

/**
 * Instance of the protocol logger.
 */
std::shared_ptr<spdlog::logger> protocol_logger{};
std::optional<std::weak_ptr<sdk_async_pool>> protocol_logger_pool{};
std::atomic_int protocol_logger_version{ 0 };

std::mutex log_callback_mutex;
std::shared_ptr<couchbase::core::logger::log_callback> log_callback{};
std::atomic_int log_callback_version{ 0 };
// Mirrors "is log_callback set?" so the hot-path predicate can be answered with a single atomic
// load, without copying the owning shared_ptr. Updated under log_callback_mutex alongside
// log_callback.
std::atomic<bool> log_callback_present{ false };

// True once any thread_local logger or callback cache on this thread runs its destructor.
// Static destructors run after thread_local teardown and fall back to locking directly.
thread_local bool thread_caches_destroyed{ false };

// What a thread last read of one logger: weak references to the logger and any async pool, whether
// the logger was absent, and the version they were read at.
struct cached_logger {
  cached_logger() = default;
  cached_logger(const cached_logger&) = delete;
  cached_logger(cached_logger&&) = delete;
  auto operator=(const cached_logger&) -> cached_logger& = delete;
  auto operator=(cached_logger&&) -> cached_logger& = delete;
  ~cached_logger()
  {
    thread_caches_destroyed = true;
  }

  std::weak_ptr<spdlog::logger> logger{};
  std::optional<std::weak_ptr<sdk_async_pool>> async_pool{};
  bool absent{ true };
  int version{ -1 };
};

thread_local cached_logger cached_file_logger{};
thread_local cached_logger cached_protocol_logger{};

std::mutex after_log_snapshot_mutex;
std::function<void()> after_log_snapshot_callback{};
std::atomic<bool> after_log_snapshot_installed{ false };

void
run_after_log_snapshot()
{
  std::function<void()> callback{};
  {
    const std::scoped_lock hook_lock(after_log_snapshot_mutex);
    callback = after_log_snapshot_callback;
  }
  if (callback) {
    try {
      callback();
    } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
      // unit test callback; no logger lock is held
    }
  }
}

std::mutex after_cached_version_match_mutex;
std::function<void()> after_cached_version_match_callback{};
std::atomic<bool> after_cached_version_match_installed{ false };

void
run_after_cached_version_match()
{
  std::function<void()> callback{};
  {
    const std::scoped_lock hook_lock(after_cached_version_match_mutex);
    callback = after_cached_version_match_callback;
  }
  if (callback) {
    try {
      callback();
    } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
      // unit test callback; no logger lock is held
    }
  }
}

class active_logger_snapshot
{
public:
  active_logger_snapshot() = default;
  active_logger_snapshot(std::shared_ptr<spdlog::logger> logger,
                         std::shared_ptr<sdk_async_pool> pool)
    : logger_{ std::move(logger) }
    , pool_{ std::move(pool) }
  {
  }
  active_logger_snapshot(const active_logger_snapshot&) = delete;
  active_logger_snapshot(active_logger_snapshot&&) noexcept = default;
  auto operator=(const active_logger_snapshot&) -> active_logger_snapshot& = delete;
  auto operator=(active_logger_snapshot&& other) noexcept -> active_logger_snapshot&
  {
    if (this != &other) {
      release();
      logger_ = std::move(other.logger_);
      pool_ = std::move(other.pool_);
    }
    return *this;
  }

  ~active_logger_snapshot() noexcept
  {
    release();
  }

  [[nodiscard]] auto operator->() const noexcept -> spdlog::logger*
  {
    return logger_.get();
  }

  [[nodiscard]] explicit operator bool() const noexcept
  {
    return logger_ != nullptr;
  }

private:
  void release() noexcept
  {
    if (logger_ != nullptr && pool_ != nullptr &&
        pool_->shutting_down.load(std::memory_order_acquire)) {
      // shutdown() may already have queued its flush before this call queued its message, and the
      // periodic flusher is stopped; queue a flush while the pool is still held so the worker
      // flushes the sink before terminate drains the queue.
      try {
        logger_->flush();
      } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
        // spdlog catches sink and queue errors inside flush_(); the destructor is noexcept
      }
    }
    pool_.reset();
    logger_.reset();
  }

  std::shared_ptr<spdlog::logger> logger_{};
  std::shared_ptr<sdk_async_pool> pool_{};
};

auto
lock_active_logger(std::shared_ptr<spdlog::logger> logger,
                   const std::optional<std::weak_ptr<sdk_async_pool>>& async_pool)
  -> active_logger_snapshot
{
  if (logger == nullptr) {
    return {};
  }
  if (!async_pool.has_value()) {
    return { std::move(logger), {} };
  }
  auto pool = async_pool->lock();
  if (pool == nullptr) {
    return {};
  }
  return { std::move(logger), std::move(pool) };
}

// Re-reads under file_logger_mutex if the cached weak_ptr expired concurrently.
auto
read_logger(cached_logger& cache,
            const std::shared_ptr<spdlog::logger>& current,
            const std::optional<std::weak_ptr<sdk_async_pool>>& current_pool,
            const std::atomic_int& version) -> std::shared_ptr<spdlog::logger>
{
  if (thread_caches_destroyed) {
    const std::scoped_lock lock(file_logger_mutex);
    return current;
  }
  if (cache.version == version) {
    if (cache.absent) {
      return {};
    }
    if (after_cached_version_match_installed.load(std::memory_order_acquire)) {
      run_after_cached_version_match();
    }
    if (auto logger = cache.logger.lock(); logger != nullptr) {
      return logger;
    }
  }
  const std::scoped_lock lock(file_logger_mutex);
  cache.logger = current;
  cache.async_pool = current_pool;
  cache.absent = current == nullptr;
  cache.version = version;
  return current;
}

auto
read_logger_for_log(cached_logger& cache,
                    const std::shared_ptr<spdlog::logger>& current,
                    const std::optional<std::weak_ptr<sdk_async_pool>>& current_pool,
                    const std::atomic_int& version) -> active_logger_snapshot
{
  if (thread_caches_destroyed) {
    const std::scoped_lock lock(file_logger_mutex);
    return lock_active_logger(current, current_pool);
  }
  if (const int cached_version = cache.version; cached_version == version) {
    if (cache.absent) {
      return {};
    }
    if (after_cached_version_match_installed.load(std::memory_order_acquire)) {
      run_after_cached_version_match();
    }
    if (auto logger = cache.logger.lock(); logger != nullptr) {
      // After shutdown() without reset(), the installed logger stays alive while its pool has
      // expired; return the empty snapshot without taking file_logger_mutex unless the slot
      // version changed concurrently.
      if (auto snapshot = lock_active_logger(std::move(logger), cache.async_pool);
          snapshot || version == cached_version) {
        return snapshot;
      }
    }
  }
  const std::scoped_lock lock(file_logger_mutex);
  cache.logger = current;
  cache.async_pool = current_pool;
  cache.absent = current == nullptr;
  cache.version = version;
  return lock_active_logger(current, current_pool);
}

auto
get_file_logger() -> std::shared_ptr<spdlog::logger>
{
  return read_logger(cached_file_logger, file_logger, file_logger_pool, file_logger_version);
}

auto
get_protocol_logger() -> std::shared_ptr<spdlog::logger>
{
  return read_logger(
    cached_protocol_logger, protocol_logger, protocol_logger_pool, protocol_logger_version);
}

auto
get_file_logger_for_log() -> active_logger_snapshot
{
  auto snapshot =
    read_logger_for_log(cached_file_logger, file_logger, file_logger_pool, file_logger_version);
  if (snapshot && after_log_snapshot_installed.load(std::memory_order_acquire)) {
    run_after_log_snapshot();
  }
  return snapshot;
}

auto
get_protocol_logger_for_log() -> active_logger_snapshot
{
  auto snapshot = read_logger_for_log(
    cached_protocol_logger, protocol_logger, protocol_logger_pool, protocol_logger_version);
  if (snapshot && after_log_snapshot_installed.load(std::memory_order_acquire)) {
    run_after_log_snapshot();
  }
  return snapshot;
}

auto
get_custom_callback() -> std::shared_ptr<couchbase::core::logger::log_callback>
{
  struct cached_callback {
    cached_callback() = default;
    cached_callback(const cached_callback&) = delete;
    cached_callback(cached_callback&&) = delete;
    auto operator=(const cached_callback&) -> cached_callback& = delete;
    auto operator=(cached_callback&&) -> cached_callback& = delete;
    ~cached_callback()
    {
      thread_caches_destroyed = true;
    }

    std::shared_ptr<couchbase::core::logger::log_callback> callback{};
    int version{ -1 };
  };

  if (thread_caches_destroyed) {
    const std::scoped_lock lock(log_callback_mutex);
    return log_callback;
  }
  thread_local cached_callback cache{};
  if (cache.version != log_callback_version) {
    const std::scoped_lock lock(log_callback_mutex);
    cache.callback = log_callback;
    cache.version = log_callback_version;
  }
  return cache.callback;
}

void
update_callback_logger(const std::shared_ptr<couchbase::core::logger::log_callback>& new_callback)
{
  const std::scoped_lock lock(log_callback_mutex);
  log_callback = new_callback;
  log_callback_present.store(new_callback != nullptr, std::memory_order_relaxed);
  ++log_callback_version;
}

// Assumes registry_mutex is held. Updates the spdlog registry entry and the SDK's slot in one
// critical section, and returns the replaced logger so the caller releases its last reference
// outside all logger mutexes.
auto
publish_logger(std::shared_ptr<spdlog::logger>& slot,
               std::optional<std::weak_ptr<sdk_async_pool>>& slot_pool,
               std::atomic_int& version,
               const std::string& name,
               std::shared_ptr<spdlog::logger> new_logger,
               std::optional<std::weak_ptr<sdk_async_pool>> new_pool = std::nullopt)
  -> std::shared_ptr<spdlog::logger>
{
  spdlog::drop(name);
  if (new_logger != nullptr) {
    spdlog::register_logger(new_logger);
  }
  const std::scoped_lock lock(file_logger_mutex);
  auto replaced = std::move(slot);
  slot = std::move(new_logger);
  slot_pool = std::move(new_pool);
  ++version;
  return replaced;
}

void
update_file_logger(std::shared_ptr<spdlog::logger> new_logger)
{
  // Released after the mutex: the last reference can destroy the logger, and a sink that logs from
  // its destructor would take the mutex again.
  std::shared_ptr<spdlog::logger> replaced{};
  {
    const scoped_registry_lock lock;
    replaced = publish_logger(
      file_logger, file_logger_pool, file_logger_version, file_logger_name, std::move(new_logger));
  }
}

struct logger_flush_snapshot {
  std::shared_ptr<spdlog::logger> logger{};
  std::optional<std::weak_ptr<sdk_async_pool>> async_pool{};
};

void
flush_snapshot(const logger_flush_snapshot& snapshot)
{
  if (snapshot.logger == nullptr) {
    return;
  }
  if (snapshot.async_pool.has_value()) {
    // An async logger left installed by a prior shutdown() has an expired pool; skipping it avoids
    // spdlog's missing-pool error, while holding a live pool keeps it alive across post_flush().
    const auto pool = snapshot.async_pool->lock();
    if (pool == nullptr) {
      return;
    }
    snapshot.logger->flush();
    return;
  }
  snapshot.logger->flush();
}

std::atomic<bool> periodic_flusher_started{ false };

std::mutex fork_mutex;
std::mutex error_report_mutex;
// Guards sdk_thread_pool(). Never held while the pool is destroyed, since that joins its worker.
std::mutex sdk_thread_pool_mutex;

// True on the SDK async logging worker. SDK log, flush, and shutdown calls become no-ops there
// because the blocking queue has a single consumer and cannot drain or join itself.
thread_local bool on_sdk_log_worker{ false };

// Owns the shared thread pool for asynchronous SDK loggers (spdlog::async_logger holds its pool
// weakly). Kept separate from spdlog's global pool so application and SDK loggers do not replace
// each other's pool.
//
// Constructed via sdk_thread_pool() after spdlog's registry, console mutex, and the file-scope
// logger globals above, so reverse static destruction joins the worker before tearing those down.
// Its destructor drops both SDK registry entries and clears both slots before releasing the pool
// and loggers outside all locks.
class sdk_thread_pool_owner
{
public:
  sdk_thread_pool_owner() = default;
  sdk_thread_pool_owner(const sdk_thread_pool_owner&) = delete;
  sdk_thread_pool_owner(sdk_thread_pool_owner&&) = delete;
  auto operator=(const sdk_thread_pool_owner&) -> sdk_thread_pool_owner& = delete;
  auto operator=(sdk_thread_pool_owner&&) -> sdk_thread_pool_owner& = delete;

  ~sdk_thread_pool_owner()
  {
    std::shared_ptr<spdlog::logger> replaced_file{};
    std::shared_ptr<spdlog::logger> replaced_protocol{};
    std::shared_ptr<sdk_async_pool> pool_handle{};
    const auto clear_slots = [&]() noexcept {
      if (file_logger != nullptr) {
        replaced_file = std::move(file_logger);
      }
      file_logger_pool.reset();
      ++file_logger_version;
      if (protocol_logger != nullptr) {
        replaced_protocol = std::move(protocol_logger);
      }
      protocol_logger_pool.reset();
      ++protocol_logger_version;
    };
    const auto detach_pool = [&]() noexcept {
      if (pool != nullptr) {
        pool_handle = std::move(pool);
        pool_handle->shutting_down.store(true, std::memory_order_release);
      }
    };
    try {
      const std::scoped_lock registry_lock(registry_mutex);
      try {
        spdlog::drop(file_logger_name);
        spdlog::drop(protocol_logger_name);
      } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
        // the process is exiting, and the registry is left as it is
      }
      try {
        const std::scoped_lock logger_lock(file_logger_mutex);
        clear_slots();
      } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
        clear_slots();
      }
      try {
        const std::scoped_lock pool_lock(sdk_thread_pool_mutex);
        detach_pool();
      } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
        detach_pool();
      }
    } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
      clear_slots();
      detach_pool();
    }
    pool_handle.reset();
    replaced_protocol.reset();
    replaced_file.reset();
  }

  std::shared_ptr<sdk_async_pool> pool{};
};

auto
sdk_thread_pool() -> std::shared_ptr<sdk_async_pool>&
{
  static auto& pool = []() -> std::shared_ptr<sdk_async_pool>& {
    static_cast<void>(spdlog::details::registry::instance());
    static_cast<void>(spdlog::details::console_mutex::mutex());
    static sdk_thread_pool_owner owner{};
    return owner.pool;
  }();
  return pool;
}

// Constructs the pool's owner, and the spdlog statics before it, when this library is
// initialised. A shared library is initialised before the application, whose statics are then
// destroyed before the owner, so one that logs through the SDK from its destructor still finds the
// pool. Linked statically, the order against the application's own statics is unspecified.
[[maybe_unused]] const bool sdk_thread_pool_owner_constructed =
  (static_cast<void>(sdk_thread_pool()), true);

constexpr std::chrono::seconds periodic_flush_interval{ 1 };

// replaces spdlog's default handler, whose mutex notify_fork() cannot hold across a fork
void
report_logger_error(const std::string& message)
{
  const std::scoped_lock lock(error_report_mutex);
  static std::chrono::steady_clock::time_point last_report{};
  const auto now = std::chrono::steady_clock::now();
  if (now - last_report < std::chrono::seconds{ 1 }) {
    return;
  }
  last_report = now;
  // Without a lock of its own: console_mutex is taken before error_report_mutex in prepare. Never
  // destroyed: an async logger can report an error during static destruction.
  static auto* const sink = []() {
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    auto* stderr_sink = new spdlog::sinks::stderr_sink_st();
    stderr_sink->set_pattern("[couchbase logger error] %v");
    return stderr_sink;
  }();
  try {
    sink->log(spdlog::details::log_msg{ "", spdlog::level::err, message });
  } catch (const std::exception&) { // NOLINT(bugprone-empty-catch)
    // the report is dropped; an exception here would escape spdlog and end the logging thread
  }
}

// Destroying them in the child would write to the parent's files and join missing threads. Runs
// with the fork locks held, so no thread the child starts can read file_logger before the reset.
void
abandon_inherited_loggers()
{
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
  static auto* inherited = new std::vector<std::shared_ptr<void>>();
  spdlog::apply_all([](const std::shared_ptr<spdlog::logger>& logger) {
    inherited->push_back(logger);
  });
  if (protocol_logger) {
    inherited->push_back(protocol_logger);
    protocol_logger.reset();
  }
  protocol_logger_pool.reset();
  ++protocol_logger_version;
  if (auto pool = spdlog::details::registry::instance().get_tp(); pool) {
    inherited->push_back(pool);
    spdlog::details::registry::instance().set_tp(nullptr);
  }
  if (auto& pool = sdk_thread_pool(); pool) {
    inherited->push_back(pool);
    pool.reset();
  }
  spdlog::drop_all();
  if (file_logger) {
    inherited->push_back(file_logger);
    file_logger.reset();
  }
  file_logger_pool.reset();
  ++file_logger_version;
  periodic_flusher_started = false;
}

void
release_fork_locks()
{
  error_report_mutex.unlock();
  spdlog::details::console_mutex::mutex().unlock();
  custom_rotating_file_sink_mt::unlock_all_after_fork();
  sdk_thread_pool_mutex.unlock();
  log_callback_mutex.unlock();
  file_logger_mutex.unlock();
}
} // namespace

namespace couchbase::core::logger
{
auto
translate_level(level level) -> spdlog::level::level_enum
{
  switch (level) {
    case level::trace:
      return spdlog::level::level_enum::trace;
    case level::debug:
      return spdlog::level::level_enum::debug;
    case level::info:
      return spdlog::level::level_enum::info;
    case level::warn:
      return spdlog::level::level_enum::warn;
    case level::err:
      return spdlog::level::level_enum::err;
    case level::critical:
      return spdlog::level::level_enum::critical;
    case level::off:
      return spdlog::level::level_enum::off;
  }
  return spdlog::level::level_enum::trace;
}

auto
translate_level(spdlog::level::level_enum spdlog_level) -> level
{
  switch (spdlog_level) {
    case spdlog::level::level_enum::trace:
      return level::trace;
    case spdlog::level::debug:
      return level::debug;
    case spdlog::level::info:
      return level::info;
    case spdlog::level::warn:
      return level::warn;
    case spdlog::level::err:
      return level::err;
    case spdlog::level::critical:
      return level::critical;
    default:
      break;
  }
  return level::off;
}

auto
level_from_str(const std::string& str) -> level
{
  switch (spdlog::level::from_str(str)) {
    case spdlog::level::level_enum::trace:
      return level::trace;
    case spdlog::level::level_enum::debug:
      return level::debug;
    case spdlog::level::level_enum::info:
      return level::info;
    case spdlog::level::level_enum::warn:
      return level::warn;
    case spdlog::level::level_enum::err:
      return level::err;
    case spdlog::level::level_enum::critical:
      return level::critical;
    case spdlog::level::level_enum::off:
      return level::off;
    default:
      break;
  }
  // return highest level if we don't understand
  return level::trace;
}

auto
should_log(level lvl) -> bool
{
  if (on_sdk_log_worker) {
    return false;
  }
  // One snapshot: reset() can clear the logger between two reads.
  const auto logger = get_file_logger();
  return logger != nullptr && logger->should_log(translate_level(lvl));
}

auto
has_custom_callback() -> bool
{
  // A log macro asks this on every call; answer it with a single atomic load rather than copying
  // the owning shared_ptr that get_custom_callback() returns.
  return log_callback_present.load(std::memory_order_relaxed);
}

namespace detail
{
void
log(const char* file, int line, const char* function, level lvl, std::string_view msg)
{
  if (on_sdk_log_worker) {
    return;
  }
  if (const auto logger = get_file_logger_for_log()) {
    logger->log(spdlog::source_loc{ file, line, function }, translate_level(lvl), msg);
  }
}

void
log_custom_logger(const char* file, int line, const char* function, level lvl, std::string_view msg)
{
  if (auto callback = get_custom_callback()) {
    (*callback)(msg, lvl, { file, function, line });
  }
}
} // namespace detail

void
flush()
{
  // A flush is queued like a message, behind which the worker would wait in a full queue.
  if (on_sdk_log_worker) {
    return;
  }
  logger_flush_snapshot file_to_flush{};
  logger_flush_snapshot protocol_to_flush{};
  {
    const std::scoped_lock lock(file_logger_mutex);
    file_to_flush = { file_logger, file_logger_pool };
    protocol_to_flush = { protocol_logger, protocol_logger_pool };
  }
  flush_snapshot(file_to_flush);
  flush_snapshot(protocol_to_flush);
}

void
shutdown()
{
  // A sink runs on the pool's worker; detaching and resetting the pool there would post terminate
  // to a queue only this thread drains or join this thread with itself.
  if (on_sdk_log_worker) {
    return;
  }
  // Stops spdlog's periodic flusher, drops every logger registered in this spdlog instance, and
  // resets spdlog's global pool; detaching sdk_thread_pool() below drains and joins the SDK's own
  // async logging worker once its last reference is released outside the locks.
  logger_flush_snapshot file_to_flush{};
  logger_flush_snapshot protocol_to_flush{};
  std::shared_ptr<sdk_async_pool> pool_handle{};
  {
    // Under registry_mutex, which a logger's creation holds throughout, so that a logger created
    // concurrently either comes before and is flushed and shut down, or comes after and gets a pool
    // and a flusher of its own.
    const scoped_registry_lock registry_lock;
    spdlog::details::registry::instance().shutdown();
    periodic_flusher_started = false;
    {
      const std::scoped_lock logger_lock(file_logger_mutex);
      file_to_flush = { file_logger, file_logger_pool };
      protocol_to_flush = { protocol_logger, protocol_logger_pool };
    }
    {
      const std::scoped_lock pool_lock(sdk_thread_pool_mutex);
      pool_handle = std::move(sdk_thread_pool());
      if (pool_handle != nullptr) {
        pool_handle->shutting_down.store(true, std::memory_order_release);
      }
    }
  }
  // Flushed after the locks, as queueing to a full async pool waits on its worker and a synchronous
  // sink can take locks of its own, and before pool_handle.reset() posts terminate behind the
  // queued flushes and joins the worker.
  flush_snapshot(file_to_flush);
  flush_snapshot(protocol_to_flush);
  file_to_flush = {};
  protocol_to_flush = {};
  // Release outside the locks because destroying the last pool reference joins the worker. Any
  // concurrent log call holding a snapshot joins the worker when its snapshot releases.
  pool_handle.reset();
}

auto
is_initialized() -> bool
{
  return get_file_logger() != nullptr;
}

void
notify_fork(couchbase::fork_event event)
{
  switch (event) {
    case couchbase::fork_event::prepare: {
      // A throw restarts the flusher and releases, in reverse order, the locks taken so far. Sink
      // locks that lock_all_for_fork() took before throwing stay held.
      std::unique_lock fork_lock(fork_mutex);
      forking_thread_id = spdlog::details::os::thread_id();
      std::unique_lock registry_lock(registry_mutex);
      // Stopped under registry_mutex, so no other thread holds the registry's flusher lock at the
      // fork. The flusher never takes registry_mutex, so joining it here cannot deadlock.
      if (periodic_flusher_started) {
        spdlog::flush_every(std::chrono::seconds::zero());
      }
      try {
        std::unique_lock file_logger_lock(file_logger_mutex);
        std::unique_lock log_callback_lock(log_callback_mutex);
        std::unique_lock sdk_thread_pool_lock(sdk_thread_pool_mutex);
        custom_rotating_file_sink_mt::lock_all_for_fork();
        try {
          std::unique_lock console_lock(spdlog::details::console_mutex::mutex());
          error_report_mutex.lock();
          console_lock.release();
        } catch (...) {
          custom_rotating_file_sink_mt::unlock_all_after_fork();
          throw;
        }
        // held until parent or child
        sdk_thread_pool_lock.release();
        log_callback_lock.release();
        file_logger_lock.release();
      } catch (...) {
        // parent is not called for an aborted prepare, so the flusher restarts here
        if (periodic_flusher_started) {
          spdlog::flush_every(periodic_flush_interval);
        }
        throw;
      }
      registry_lock.release();
      fork_lock.release();
      break;
    }

    case couchbase::fork_event::parent: {
      // released even when starting the flusher thread throws
      const std::unique_lock fork_lock(fork_mutex, std::adopt_lock);
      const std::unique_lock registry_lock(registry_mutex, std::adopt_lock);
      release_fork_locks();
      if (periodic_flusher_started) {
        spdlog::flush_every(periodic_flush_interval);
      }
      break;
    }

    case couchbase::fork_event::child: {
      const std::unique_lock fork_lock(fork_mutex, std::adopt_lock);
      const std::unique_lock registry_lock(registry_mutex, std::adopt_lock);
      forked_thread_id.store(spdlog::details::os::_thread_id(), std::memory_order_relaxed);
      stale_thread_id.store(forking_thread_id, std::memory_order_relaxed);
      abandon_inherited_loggers();
      release_fork_locks();
      break;
    }
  }
}

namespace
{
std::atomic<bool> log_redaction_enabled{ false };
} // namespace

void
set_log_redaction(bool enable)
{
  const auto was_enabled = log_redaction_enabled.exchange(enable, std::memory_order_relaxed);
  if (enable && !was_enabled) {
    CB_LOG_INFO_RAW(
      "Log redaction enabled: sensitive values are wrapped in redaction tags. This log still "
      "contains identifying information until those tags are processed. Once redacted, diagnosis "
      "and support of issues may be challenging or not possible");
  }
}

auto
is_log_redaction_enabled() -> bool
{
  return log_redaction_enabled.load(std::memory_order_relaxed);
}

auto
create_file_logger_impl(const std::string& logger_name,
                        std::shared_ptr<spdlog::logger>& slot,
                        std::optional<std::weak_ptr<sdk_async_pool>>& slot_pool,
                        std::atomic_int& version,
                        const configuration& logger_settings) -> std::optional<std::string>
{
  // Released after registry_mutex: the last reference can destroy the replaced logger, and a sink
  // that logs from its destructor would take file_logger_mutex again.
  std::shared_ptr<spdlog::logger> replaced{};
  std::shared_ptr<spdlog::logger> logger{};
  std::optional<std::weak_ptr<sdk_async_pool>> async_pool{};

  try {
    // Initialise the loggers.
    //
    // The structure is as follows:
    //
    // file_logger = sends log messages to sink
    //   |__dist_sink_mt = Distribute log messages to multiple sinks
    //       |     |__custom_rotating_file_sink_mt = adds opening & closing
    //       |                                       hooks to the file
    //       |__ (color)__stderr_sink_mt = Send log messages to console
    //
    // When a new log message is being submitted to the file_logger it
    // is subject to the log level specified on the file_logger. If it
    // is to be included it is passed down to the dist_sink which will
    // evaluate if the message should be passed on based on its log level.
    // It'll then try to pass the message to the file sink and the
    // console sink and they will evaluate if the message should be
    // logged or not. This means that we should set the file sink
    // loglevel to TRACE so that all messages which goes all the way
    // will end up in the file. Due to the fact that ns_server can't
    // keep up with the rate we might produce log we want the console
    // sink to drop everything below WARNING (unless we're running
    // unit tests (through testapp).
    //
    // When the user change the verbosity level we'll modify the
    // level for the file_logger object causing it to allow more
    // messages to go down to the various sinks.

    auto sink = std::make_shared<spdlog::sinks::dist_sink_mt>();
    sink->set_level(spdlog::level::trace);

    if (const auto& fname = logger_settings.filename; !fname.empty()) {
      auto cyclesz = logger_settings.cycle_size;

      if (!spdlog::details::os::getenv("COUCHBASE_CXX_CLIENT_MAXIMIZE_LOGGER_CYCLE_SIZE").empty()) {
        cyclesz = 1024LLU * 1024 * 1024; // use up to 1 GB log file size
      }

      auto fsink = std::make_shared<custom_rotating_file_sink_mt>(fname, cyclesz, make_formatter());
      fsink->set_level(spdlog::level::trace);
      sink->add_sink(fsink);
    }

    if (logger_settings.console) {
      auto stderrsink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();

      // Set the formatting pattern of this sink
      stderrsink->set_formatter(make_formatter());
      stderrsink->set_level(translate_level(logger_settings.console_sink_log_level));
      sink->add_sink(stderrsink);
    }
    if (nullptr != logger_settings.sink) {
      logger_settings.sink->set_formatter(make_formatter());
      sink->add_sink(logger_settings.sink);
    }

    const scoped_registry_lock lock;

    if (logger_settings.unit_test) {
      logger = std::make_shared<spdlog::logger>(logger_name, sink);
    } else {
      // Shared, so that creating one logger never takes the pool from another.
      std::shared_ptr<sdk_async_pool> pool_handle{};
      {
        const std::scoped_lock pool_lock(sdk_thread_pool_mutex);
        auto& shared = sdk_thread_pool();
        if (shared == nullptr) {
          shared = std::make_shared<sdk_async_pool>(
            std::make_shared<spdlog::details::thread_pool>(logger_settings.buffer_size, 1, []() {
              on_sdk_log_worker = true;
            }));
        }
        pool_handle = shared;
      }
      logger = std::make_shared<spdlog::async_logger>(
        logger_name, sink, pool_handle->pool, spdlog::async_overflow_policy::block);
      async_pool = pool_handle;
    }

    logger->set_formatter(make_formatter());
    logger->set_error_handler(report_logger_error);
    logger->set_level(translate_level(logger_settings.log_level));

    // Set the flushing interval policy
    spdlog::flush_every(periodic_flush_interval);
    periodic_flusher_started = true;

    replaced = publish_logger(
      slot, slot_pool, version, logger_name, std::move(logger), std::move(async_pool));
  } catch (const spdlog::spdlog_ex& ex) {
    std::string msg = std::string{ "Log initialization failed: " } + ex.what();
    return msg;
  }
  return {};
}

/**
 * Initialises the loggers. Called if the logger configuration is
 * specified in a separate settings object.
 */
auto
create_file_logger(const configuration& logger_settings) -> std::optional<std::string>
{
  return create_file_logger_impl(
    file_logger_name, file_logger, file_logger_pool, file_logger_version, logger_settings);
}

auto
create_protocol_logger(const configuration& logger_settings) -> std::optional<std::string>
{
  if (logger_settings.filename.empty()) {
    return "File name is missing";
  }
  auto config = logger_settings;
  config.log_level = couchbase::core::logger::level::trace;
  return create_file_logger_impl(
    protocol_logger_name, protocol_logger, protocol_logger_pool, protocol_logger_version, config);
}

auto
should_log_protocol() -> bool
{
  return !on_sdk_log_worker && get_protocol_logger() != nullptr;
}

namespace detail
{
void
log_protocol(const char* file, int line, const char* function, std::string_view msg)
{
  if (on_sdk_log_worker) {
    return;
  }
  if (const auto logger = get_protocol_logger_for_log()) {
    logger->log(spdlog::source_loc{ file, line, function }, spdlog::level::level_enum::trace, msg);
  }
}
} // namespace detail

auto
get() -> spdlog::logger*
{
  return get_file_logger().get();
}

void
reset()
{
  // Released after registry_mutex: destroying either replaced logger can run a sink destructor
  // that logs.
  std::shared_ptr<spdlog::logger> replaced_file{};
  std::shared_ptr<spdlog::logger> replaced_protocol{};
  {
    const scoped_registry_lock lock;
    replaced_file =
      publish_logger(file_logger, file_logger_pool, file_logger_version, file_logger_name, nullptr);
    replaced_protocol = publish_logger(protocol_logger,
                                       protocol_logger_pool,
                                       protocol_logger_version,
                                       protocol_logger_name,
                                       nullptr);
  }
}

void
create_blackhole_logger()
{
  auto new_logger = std::make_shared<spdlog::logger>(
    file_logger_name, std::make_shared<spdlog::sinks::null_sink_mt>());
  new_logger->set_level(spdlog::level::off);
  new_logger->set_formatter(make_formatter());

  update_file_logger(new_logger);
}

void
create_console_logger()
{
  auto stderrsink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
  auto new_logger = std::make_shared<spdlog::logger>(file_logger_name, stderrsink);
  new_logger->set_level(spdlog::level::info);
  new_logger->set_formatter(make_formatter());
  new_logger->set_error_handler(report_logger_error);
  update_file_logger(new_logger);
}

void
register_log_callback(log_callback callback)
{
  auto new_callback = std::make_shared<log_callback>(std::move(callback));
  update_callback_logger(new_callback);
}

void
unregister_log_callback()
{
  update_callback_logger(nullptr);
}

void
register_spdlog_logger(const std::shared_ptr<spdlog::logger>& l)
{
  // reset() leaves no file logger, so each use checks its own snapshot.
  try {
    if (const auto logger = get_file_logger_for_log()) {
      logger->debug("Registering logger {}", l->name());
    }
    const scoped_registry_lock lock;
    spdlog::register_logger(l);
  } catch (const spdlog::spdlog_ex& e) {
    if (const auto logger = get_file_logger_for_log()) {
      logger->warn("Exception caught when attempting to register the logger {} in the spdlog "
                   "registry. The verbosity of this logger "
                   "cannot be changed at runtime. e.what()={}",
                   l->name(),
                   e.what());
    }
  }
}

void
unregister_spdlog_logger(const std::string& n)
{
  const scoped_registry_lock lock;
  spdlog::drop(n);
}

auto
check_log_levels(level lvl) -> bool
{
  auto level = translate_level(lvl);
  bool correct = true;
  const scoped_registry_lock lock;
  spdlog::apply_all([level, &correct](const std::shared_ptr<spdlog::logger>& l) {
    if (l->level() != level) {
      correct = false;
    }
  });
  return correct;
}

auto
get_lowest_log_level() -> level
{
  auto lowest = spdlog::level::off;
  const scoped_registry_lock lock;
  // Apply the function to each registered spdlog::logger except protocol logger
  spdlog::apply_all([&lowest](const std::shared_ptr<spdlog::logger>& l) {
    lowest = std::min(l->level(), lowest);
  });
  return translate_level(lowest);
}

void
set_log_levels(level lvl)
{
  auto level = translate_level(lvl);
  std::unique_lock lock(registry_mutex);
  // Apply the function to each registered spdlog::logger except protocol logger
  spdlog::apply_all([level](const std::shared_ptr<spdlog::logger>& l) {
    if (l->name() == protocol_logger_name) {
      l->set_level(spdlog::level::trace);
      return;
    }

    try {
      l->set_level(level);
    } catch (const spdlog::spdlog_ex& e) {
      l->warn("Exception caught when attempting to change the verbosity "
              "of logger {} to spdlog level {}. e.what()={}",
              l->name(),
              to_short_c_str(level),
              e.what());
    }
  });
  lock.unlock();

  flush();
}

namespace unit_test_api
{
void
set_after_registry_unlock(std::function<void()> callback)
{
  const bool installed = static_cast<bool>(callback);
  const std::scoped_lock lock(after_registry_unlock_mutex);
  after_registry_unlock_callback = std::move(callback);
  after_registry_unlock_installed.store(installed, std::memory_order_release);
}

void
set_after_log_snapshot(std::function<void()> callback)
{
  const bool installed = static_cast<bool>(callback);
  const std::scoped_lock lock(after_log_snapshot_mutex);
  after_log_snapshot_callback = std::move(callback);
  after_log_snapshot_installed.store(installed, std::memory_order_release);
}

void
set_after_cached_version_match(std::function<void()> callback)
{
  const bool installed = static_cast<bool>(callback);
  const std::scoped_lock lock(after_cached_version_match_mutex);
  after_cached_version_match_callback = std::move(callback);
  after_cached_version_match_installed.store(installed, std::memory_order_release);
}
} // namespace unit_test_api
} // namespace couchbase::core::logger
