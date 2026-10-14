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

#include "test_helper_integration.hxx"

#ifndef _WIN32

#include "utils/logger.hxx"

#include "core/io/dns_client.hxx"
#include "core/io/dns_config.hxx"
#include "core/io/streams.hxx"
#include "core/transactions.hxx"
#include "core/transactions/attempt_context_testing_hooks.hxx"
#include "core/transactions/cleanup_testing_hooks.hxx"
#include "core/transactions/internal/transactions_cleanup.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/analytics_options.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/diagnostics_result.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/fork_event.hxx>
#include <couchbase/get_any_replica_options.hxx>
#include <couchbase/lookup_in_all_replicas_options.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/password_authenticator.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/service_type.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/transaction_options.hxx>
#include <couchbase/upsert_options.hxx>

#include <tao/json/value.hpp>

#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <asio/write.hpp>

#include <spdlog/fmt/bundled/format.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <utility>
// <csignal> rather than <signal.h>: clang-tidy's modernize-deprecated-headers is enabled.
// kill(2) is POSIX rather than C, but this whole file is already POSIX-only and the build
// defines _GNU_SOURCE, so the declaration comes through.
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Regression guard for cluster::notify_fork().
//
// cluster_impl::notify_fork() used to start the post-fork IO thread and only
// then call io_context::notify_fork(), which asio forbids. Because close() does
// not wake a thread already blocked in epoll_wait() -- and the replacement
// epoll_create1() reuses the same fd number -- the child's IO thread stayed
// bound to the epoll instance inherited from the parent, and went on
// dereferencing descriptor_state pointers that are only valid in the parent's
// address space. Valgrind reported that as three invalid accesses in the child
// (op_queue.hpp:34 and epoll_reactor.hpp:76, reached from epoll_reactor.ipp).
//
// What this test does and does NOT guard, measured rather than assumed:
//
//   * It does NOT deterministically detect the ordering bug. Measured against the
//     unfixed core/impl/public_cluster.cxx it passed 2 of 3 runs, so do not read a
//     pass here as proof that the reactor is sound. The one failure was the
//     PARENT's post-fork read, and its cause was a separate defect -- the child
//     shutting down a descriptor fork() shares -- addressed later in this series.
//   * The deterministic guard for the ordering itself is the
//     Expects(!io_thread_.joinable()) precondition in cluster_impl::notify_fork,
//     which aborts in program order if the restart is ever moved back above the
//     fixup. Verified: reintroducing the old order makes this test SIGABRT.
//   * What this test DOES pin down is the post-fork contract for both
//     processes: the child must be able to use the cluster after
//     notify_fork(child), the parent must still work after
//     notify_fork(parent), and a child failure must reach the parent instead of
//     being swallowed.
//
// Unlike "example: using fork() for scaling" this needs no sample bucket, so it
// runs anywhere the normal integration suite runs.

// Under `ctest --test-action memcheck` this test reports a large defect count (~1700)
// while every other test in the shard reports none. Measured, so it is not chased again:
// that number is loss records, not memory errors. With
// --errors-for-leak-kinds=definite,indirect both processes report 0 errors from 0
// contexts; the child has 0 definitely-lost and 0 indirectly-lost bytes and no invalid
// access anywhere. What it does have is ~17k blocks still reachable at exit, because the
// child leaves through _Exit() (see below), plus a handful of possibly-lost records that
// are all pthread TLS -- allocate_dtv/_dl_allocate_tls from the IO, resolver, cleanup and
// ATR-pool threads that existed before the fork and do not exist in the child. That is an
// unavoidable consequence of fork(2) duplicating the heap but only one thread.
//
// The defect the original bug produced looked nothing like this: three *invalid accesses*
// in the child, at op_queue.hpp:34 and epoll_reactor.hpp:76.
//
// This also costs real wall-clock time, which is why the wait for the child below is
// generous. cmake/Testing.cmake passes --leak-check=full --show-reachable=yes
// --num-callers=50, so at exit memcheck symbolises a 50-frame trace for every one of those
// still-reachable blocks. Seen in CI: the child completed its get, then produced no output
// for the next four minutes while valgrind wrote that report, and an earlier four-minute
// deadline here killed it and reported a wedge that had not happened.
//
// Do not try to fix that by closing the cluster in the child before _Exit(): measured, it
// takes the child from ~17.0k still-reachable blocks to ~14.4k and the loss-record count
// actually rises, because the bulk of them are process-wide statics -- logger, TLS library
// tables, the Catch2 registry, the harness -- that _Exit() skips regardless. The parent
// reports "0 bytes in use at exit" only because it leaves through a normal return and runs
// that teardown. Closing in the child buys nothing here and adds a failure mode.

namespace
{
// The child is a forked copy of the Catch2 binary. It must leave through
// _Exit() so it neither runs Catch2's reporter (which would print a second,
// confusing test summary) nor the static destructors and the test guard's
// teardown, none of which are valid in a fork child.
[[noreturn]] void
leave_child(int status)
{
  // The parent cannot otherwise tell a child that never reached this call from one that
  // reached it and is still inside memcheck's exit-time report: both are silent and alive.
  //
  // write(2) rather than stdio: only the forking thread survives fork(2), so a stdio lock held
  // by another thread at the fork is held forever in this copy, and fputs would deadlock on it.
  // A marker that never appears is exactly the false diagnosis this probe exists to prevent.
  static constexpr char marker[] = "CHILD: reached _Exit\n";
  [[maybe_unused]] const auto written = ::write(STDOUT_FILENO, marker, sizeof(marker) - 1);
  std::_Exit(status);
}

// Long enough for memcheck's exit-time report in a healthy child, described at the top of this
// file; short of CTest's own per-test timeout.
constexpr int child_deadline_minutes{ 15 };

// A forked child's wait status. `error` is set when the child did not exit within the deadline or
// could not be waited for.
struct child_wait {
  int status{};
  std::string error{};
};

// Kills the child only once the deadline has passed. A wait that fails other than with EINTR
// leaves it alone: the pid may no longer be ours to signal.
auto
wait_for_child(pid_t child_pid) -> child_wait
{
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::minutes{ child_deadline_minutes };
  child_wait result{};
  while (true) {
    const auto rc = waitpid(child_pid, &result.status, WNOHANG);
    const auto wait_errno = rc < 0 ? errno : 0;
    if (rc == child_pid) {
      return result;
    }
    if (rc < 0 && wait_errno != EINTR) {
      result.error = fmt::format("waitpid() failed: {}", std::strerror(wait_errno));
      return result;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 100 });
  }
  kill(child_pid, SIGKILL);
  while (waitpid(child_pid, &result.status, 0) < 0 && errno == EINTR) {
    // reap the killed child so it cannot outlive this test
  }
  result.error =
    fmt::format("the child did not exit within {} minutes and was killed", child_deadline_minutes);
  return result;
}

// Total CPU the process has burned, in clock ticks. Fields 14 and 15 of /proc/<pid>/stat are
// utime and stime. The comm field ahead of them is parenthesised and may itself hold spaces and
// parentheses, so the scan starts after its last ')' rather than at a fixed offset.
//
// Linux only, by consequence rather than by guard: this file is already POSIX-only, and macOS
// has no /proc, so the open fails and this reports nothing. The caller then says the CPU time
// was unavailable and the rest of the diagnosis still works. Nothing here is worth a
// platform-specific implementation -- the failure this probes has only ever been seen on the
// Linux valgrind legs.
auto
cpu_ticks(pid_t pid) noexcept -> std::optional<unsigned long long>
try {
  std::ifstream stat{ "/proc/" + std::to_string(pid) + "/stat" };
  std::string line;
  if (!std::getline(stat, line)) {
    return {};
  }
  const auto comm_end = line.rfind(')');
  if (comm_end == std::string::npos) {
    return {};
  }
  std::istringstream fields{ line.substr(comm_end + 1) };
  // The field after ')' is state, which is field 3.
  std::string ignored;
  for (int index = 3; index < 14; ++index) {
    if (!(fields >> ignored)) {
      return {};
    }
  }
  unsigned long long utime{};
  unsigned long long stime{};
  if (!(fields >> utime >> stime)) {
    return {};
  }
  return utime + stime;
} catch (...) {
  // A diagnostic, read while a wedged child is still unreaped: it may not throw past here.
  return {};
}

constexpr std::size_t mcbp_header_size{ 24 };

// The local and peer ports of each KV connection in a diagnostics report. An endpoint that has not
// connected yet reports port 0 and is left out.
auto
kv_endpoint_ports(const couchbase::diagnostics_result& report)
  -> std::vector<std::pair<std::uint16_t, std::uint16_t>>
{
  const auto port_of = [](const std::string& address) -> std::uint16_t {
    const auto colon = address.rfind(':');
    std::uint16_t port{};
    if (colon == std::string::npos ||
        std::from_chars(address.data() + colon + 1, address.data() + address.size(), port).ec !=
          std::errc{}) {
      return 0;
    }
    return port;
  };
  std::vector<std::pair<std::uint16_t, std::uint16_t>> ports;
  const auto endpoints = report.endpoints();
  const auto kv = endpoints.find(couchbase::service_type::key_value);
  if (kv == endpoints.end()) {
    return ports;
  }
  for (const auto& endpoint : kv->second) {
    const auto local = port_of(endpoint.local());
    const auto peer = port_of(endpoint.remote());
    if (local != 0 && peer != 0) {
      ports.emplace_back(local, peer);
    }
  }
  return ports;
}

// The port of a socket's own address (getsockname) or its peer's (getpeername).
auto
socket_port(int fd, int (*query)(int, sockaddr*, socklen_t*)) -> std::optional<std::uint16_t>
{
  sockaddr_storage address{};
  socklen_t length = sizeof(address);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (query(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    return {};
  }
  if (address.ss_family == AF_INET) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return ntohs(reinterpret_cast<const sockaddr_in*>(&address)->sin_port);
  }
  if (address.ss_family == AF_INET6) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return ntohs(reinterpret_cast<const sockaddr_in6*>(&address)->sin6_port);
  }
  return {};
}

// The inode of the socket behind a descriptor. fork(2) shares it; a socket opened later never has
// the same one, even on a reused descriptor number and local port.
auto
socket_inode(int fd) -> std::optional<ino_t>
{
  struct stat st{};
  if (fstat(fd, &st) != 0 || !S_ISSOCK(st.st_mode)) {
    return {};
  }
  return st.st_ino;
}

// This process's sockets whose local and peer ports match one of `ports`.
auto
sockets_with_ports(const std::vector<std::pair<std::uint16_t, std::uint16_t>>& ports)
  -> std::vector<int>
{
  std::vector<int> fds;
  const auto limit = std::min<long>(sysconf(_SC_OPEN_MAX), 4096);
  for (int fd = 0; fd < limit; ++fd) {
    struct stat st{};
    if (fstat(fd, &st) != 0 || !S_ISSOCK(st.st_mode)) {
      continue;
    }
    const auto local = socket_port(fd, getsockname);
    const auto peer = socket_port(fd, getpeername);
    if (local && peer &&
        std::find(ports.begin(), ports.end(), std::pair{ local.value(), peer.value() }) !=
          ports.end()) {
      fds.push_back(fd);
    }
  }
  return fds;
}

// The server echoes the opaque unchanged, so its byte order does not matter.
auto
noop_request(std::uint32_t opaque) -> std::array<std::uint8_t, mcbp_header_size>
{
  std::array<std::uint8_t, mcbp_header_size> frame{};
  frame[0] = 0x80; // request
  frame[1] = 0x0a; // NOOP
  std::memcpy(&frame[12], &opaque, sizeof(opaque));
  return frame;
}

// The end offset of the NOOP response carrying `opaque` within the bytes queued on `fd`, when
// it and every frame ahead of it are queued complete. Reads nothing out of the queue.
auto
queued_noop_response_end(int fd, std::uint32_t opaque) -> std::optional<std::size_t>
{
  int available{};
  if (ioctl(fd, FIONREAD, &available) != 0 || available <= 0) {
    return {};
  }
  std::vector<std::uint8_t> bytes(static_cast<std::size_t>(available));
  const auto peeked = recv(fd, bytes.data(), bytes.size(), MSG_PEEK | MSG_DONTWAIT);
  if (peeked <= 0) {
    return {};
  }
  const auto size = static_cast<std::size_t>(peeked);
  std::size_t offset{};
  while (offset + mcbp_header_size <= size) {
    std::uint32_t body_length{};
    std::memcpy(&body_length, &bytes[offset + 8], sizeof(body_length));
    const auto end = offset + mcbp_header_size + ntohl(body_length);
    if (end > size) {
      return {};
    }
    std::uint32_t frame_opaque{};
    std::memcpy(&frame_opaque, &bytes[offset + 12], sizeof(frame_opaque));
    // 0x18 is the response magic once tracing has been negotiated, 0x81 otherwise.
    const auto magic = bytes[offset];
    if ((magic == 0x81 || magic == 0x18) && bytes[offset + 1] == 0x0a && frame_opaque == opaque) {
      return end;
    }
    offset = end;
  }
  return {};
}
} // namespace

TEST_CASE("integration: cluster remains usable in a forked child", "[integration]")
{
  // Capability checks happen in a nested scope so the guard -- which runs its own
  // IO threads -- is destroyed well before the fork; only the forking thread
  // survives into the child, so no guard may be live across it.
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }

  // Connection details come from the environment for the same reason: nothing
  // that owns a thread may straddle the fork.
  const auto ctx = test::utils::test_context::load_from_environment();
  test::utils::init_logger();

  // The child inherits this process's stdout buffer; unbuffer it so anything the
  // child prints before _Exit() is not lost, which is the only diagnostic a
  // failing run gets beyond the exit code.
  setbuf(stdout, nullptr);

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  // Everything here is 20-30x slower under memcheck, so do not race the
  // stock timeouts -- this is the same profile the valgrind CI leg selects.
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());

  const auto parent_id = test::utils::uniq_id("fork-parent");
  const auto child_id = test::utils::uniq_id("fork-child");

  // Do real I/O before forking so the reactor actually has MCBP sockets
  // registered -- with no registered descriptors the child's reactor has
  // nothing inherited to trip over and the scenario is not exercised.
  {
    auto collection = cluster.bucket(ctx.bucket).default_collection();
    auto [err, res] = collection.upsert(parent_id, tao::json::value{ { "side", "parent" } }).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(!res.cas().empty());
  }

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    // Hand the cluster back a runnable io_context before failing. After
    // notify_fork(prepare) it is stopped with no IO thread, and ~cluster_impl waits
    // on a completion only that thread can deliver -- so bailing out here without
    // notify_fork(parent) would hang this process on teardown rather than report a
    // failed fork, and take the rest of the suite's run with it.
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }

  if (child_pid == 0) {
    // No exception may escape into Catch2 from here. This process is a fork of the
    // test binary, so unwinding out of the child would run Catch2's reporter and the
    // static destructors -- exactly what leave_child() exists to avoid. And
    // notify_fork(child) is a throwing call: asio reports a failure to re-register a
    // descriptor with the child's new epoll instance as an exception.
    try {
      cluster.notify_fork(couchbase::fork_event::child);

      // Handles acquired before the fork refer to the connections
      // notify_fork(child) replaces, so re-acquire from the cluster.
      auto collection = cluster.bucket(ctx.bucket).default_collection();

      auto [upsert_err, upsert_res] =
        collection.upsert(child_id, tao::json::value{ { "side", "child" } }).get();
      if (upsert_err.ec() || upsert_res.cas().empty()) {
        // Only the exit code crosses back to the parent, so say why here or a red
        // run carries no reason at all.
        fmt::print("CHILD(pid={}): upsert failed: {}\n", getpid(), upsert_err.ec().message());
        leave_child(1);
      }

      auto [get_err, get_res] = collection.get(child_id).get();
      if (get_err.ec()) {
        fmt::print("CHILD(pid={}): get failed: {}\n", getpid(), get_err.ec().message());
        leave_child(2);
      }
      if (get_res.content_as<tao::json::value>()["side"].get_string() != "child") {
        leave_child(3);
      }
    } catch (const std::exception& e) {
      fmt::print("CHILD(pid={}): threw: {}\n", getpid(), e.what());
      leave_child(4);
    } catch (...) {
      fmt::print("CHILD(pid={}): threw an unknown exception\n", getpid());
      leave_child(4);
    }
    leave_child(0);
  }

  // The parent must still work on its own connections after the fork. This is the
  // check that catches a child which tore down a descriptor the parent shares, so it
  // has to stay a hard requirement -- but a timeout is not that symptom, it just
  // means the cluster was slow, which under a sanitizer or a loaded CI box it
  // will sometimes be. So retry on a timeout and stop immediately on anything else:
  // errc::network::cluster_closed or a connection reset is the interesting outcome
  // and must not be retried away.
  //
  // NOTHING from here to the reap below may throw out of this TEST_CASE, because the
  // waitpid() is the only thing guaranteeing the child does not outlive the test. Three
  // things here can throw: a Catch2 assertion, notify_fork(parent) (documented to throw
  // when the I/O backend cannot be carried across the fork), and tao's JSON decode on a
  // malformed or wrongly-typed body. The last one is not hypothetical here -- a child that
  // stole bytes from this connection would produce exactly a mangled body, so the decode
  // is likeliest to throw precisely when the defect under test is present, which is the
  // worst moment to lose the diagnostic and leak the child. So this section only records;
  // every check happens after the reap.
  std::string parent_side_error{};
  couchbase::error parent_read_err{};
  bool parent_read_succeeded{ false };
  bool parent_read_own_document{ false };
  try {
    cluster.notify_fork(couchbase::fork_event::parent);

    auto collection = cluster.bucket(ctx.bucket).default_collection();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes{ 2 };
    do {
      auto [err, res] = collection.get(parent_id).get();
      parent_read_err = err;
      if (!err.ec()) {
        parent_read_succeeded = true;
        parent_read_own_document =
          res.content_as<tao::json::value>()["side"].get_string() == "parent";
        break;
      }
      if (err.ec() != couchbase::errc::common::unambiguous_timeout &&
          err.ec() != couchbase::errc::common::ambiguous_timeout) {
        break;
      }
    } while (std::chrono::steady_clock::now() < deadline);
  } catch (const std::exception& e) {
    parent_side_error = e.what();
  } catch (...) {
    parent_side_error = "unknown exception";
  }

  // Bounded wait, then kill. A wedged child is precisely the failure mode this area is
  // about -- a post-fork reconnect that never completes -- and a blocking waitpid() would
  // hand that case to CTest's own timeout: the shard spends its budget and reports
  // "timeout" with nothing to read. Polling turns it into a fast, named failure, and the
  // SIGKILL makes sure the wedged child is gone rather than left running for the rest of
  // the suite. EINTR keeps the poll going; anything else is a real waitpid() failure.
  //
  // The budget only has to stay under CTest's own per-test timeout -- its 1500 second
  // default here -- and be long enough never to fire on a child that is merely slow.
  // Memcheck's exit-time report over the child's still-reachable blocks, described at the
  // top of this file, is what sets that floor. What this guards is an actual hang, which is
  // unbounded, so the headroom costs nothing on a child that exits normally.
  int status{};
  pid_t reaped{ -1 };
  const auto child_deadline =
    std::chrono::steady_clock::now() + std::chrono::minutes{ child_deadline_minutes };
  do {
    reaped = waitpid(child_pid, &status, WNOHANG);
    if (reaped == child_pid || (reaped < 0 && errno != EINTR)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 100 });
  } while (std::chrono::steady_clock::now() < child_deadline);

  const auto child_wedged = reaped != child_pid;
  std::string child_cpu{};
  if (child_wedged) {
    // A child inside memcheck's exit-time report and a child blocked on something look identical
    // from here -- both silent, both alive. They differ in whether they are burning CPU, which no
    // single total can show, so sample twice and report the interval as well.
    const auto before = cpu_ticks(child_pid);
    const auto sampled_at = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds{ 2 });
    // sleep_for guarantees a minimum, and this runs on a loaded valgrind runner, so the gap is
    // measured rather than assumed: a rate quoted against the wrong interval is a wrong rate.
    const auto sampled_over = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - sampled_at);
    const auto after = cpu_ticks(child_pid);
    kill(child_pid, SIGKILL);
    while ((reaped = waitpid(child_pid, &status, 0)) < 0 && errno == EINTR) {
      // reap the killed child so it cannot outlive this test
    }
    // Only now, because this allocates: until the reap above, an exception would have unwound
    // with the child still running.
    child_cpu = (before.has_value() && after.has_value())
                  ? fmt::format(" (child CPU {} ticks, {} of them in the last {}ms)",
                                after.value(),
                                after.value() - before.value(),
                                sampled_over.count())
                  : " (child CPU time unavailable)";
  }

  if (child_wedged) {
    // Deliberately does not claim a cause. The first time this fired, the child had already
    // completed every operation and was sitting in valgrind's exit-time leak report; saying
    // "wedged in the post-fork reconnect" would have sent the reader in the wrong direction.
    // Two things narrow it without guessing: "CHILD: reached _Exit" in the child's output says it
    // got as far as the exit call, and a CPU figure still rising says it is working rather than
    // blocked. Check both before concluding anything.
    FAIL("child did not exit within the deadline and was killed after "
         << child_deadline_minutes << " minutes" << child_cpu
         << "; see the child's output above for how far it got");
  }
  if (!parent_side_error.empty()) {
    FAIL("parent threw after the fork: " << parent_side_error);
  }
  if (!parent_read_succeeded) {
    FAIL(
      "parent could not read its own document after the fork: " << parent_read_err.ec().message());
  }
  REQUIRE(parent_read_own_document);
  REQUIRE(reaped == child_pid);
  REQUIRE(WIFEXITED(status));
  // 1 = upsert failed (this is what a dropped/replaced impl looks like:
  // errc::network::cluster_closed), 2 = get failed, 3 = wrong content,
  // 4 = something in the child threw.
  REQUIRE(WEXITSTATUS(status) == 0);

  cluster.close().get();
}

// CXXCBC-913: a forked child must not read from connections it inherited. The parent queues a
// NOOP response on each of its KV connections while its own I/O is stopped, so the response can
// only leave the queue through the child. The queue is shared across fork(2), so the parent finds
// it intact afterwards exactly when the child read nothing. Over TLS the plain-text NOOP cannot be
// sent, so the parent's queue check is skipped.
namespace couchbase
{
auto
extract_core_cluster(const couchbase::cluster& cluster) -> const core::cluster&;
auto
extract_core_transactions(const couchbase::cluster& cluster)
  -> const std::shared_ptr<core::transactions::transactions>&;
} // namespace couchbase

TEST_CASE("integration: a forked child leaves the parent's connections unread", "[integration]")
{
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  const auto tls = couchbase::core::utils::parse_connection_string(ctx.connection_string).tls;
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());
  auto collection = cluster.bucket(ctx.bucket).default_collection();
  const auto parent_id = test::utils::uniq_id("fork-unread");
  {
    auto [err, res] = collection.upsert(parent_id, tao::json::value{ { "side", "parent" } }).get();
    REQUIRE_SUCCESS(err.ec());
  }

  // The inherited core's teardown releases its buckets. The label listener goes only with the core.
  const auto& inherited_core = couchbase::extract_core_cluster(cluster);
  const auto inherited_bucket = std::weak_ptr(inherited_core.find_bucket_by_name(ctx.bucket));
  const auto inherited_listener = std::weak_ptr(inherited_core.cluster_label_listener());
  auto [diagnostics_err, report] = cluster.diagnostics().get();
  REQUIRE_SUCCESS(diagnostics_err.ec());
  const auto ports = kv_endpoint_ports(report);

  cluster.notify_fork(couchbase::fork_event::prepare);
  // The parent runs no I/O from here until notify_fork(parent).
  constexpr std::uint32_t opaque{ 0x31424346 };
  const auto sockets = sockets_with_ports(ports);
  std::vector<std::optional<ino_t>> inherited_inodes{};
  std::transform(
    sockets.begin(), sockets.end(), std::back_inserter(inherited_inodes), socket_inode);
  std::string setup_error{};
  if (sockets.empty()) {
    setup_error =
      fmt::format("none of the {} KV connections in the diagnostics report is open", ports.size());
  }
  if (!tls) {
    const auto request = noop_request(opaque);
    for (const auto fd : sockets) {
      if (::write(fd, request.data(), request.size()) != static_cast<ssize_t>(request.size())) {
        setup_error = fmt::format("write to fd {} failed: {}", fd, std::strerror(errno));
      }
    }
    const auto all_queued = [&sockets]() {
      return std::all_of(sockets.begin(), sockets.end(), [](int fd) {
        return queued_noop_response_end(fd, opaque).has_value();
      });
    };
    const auto queue_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 30 };
    while (setup_error.empty() && !all_queued()) {
      if (std::chrono::steady_clock::now() > queue_deadline) {
        setup_error = "the NOOP responses were not queued within 30 seconds";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{ 10 });
    }
  }
  if (!setup_error.empty()) {
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL(setup_error);
  }

  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      cluster.notify_fork(couchbase::fork_event::child);
      // The reopen opens its sockets on the IO thread and may reuse the inherited descriptor
      // numbers checked below. Waiting for it orders those sockets before the checks.
      [[maybe_unused]] const auto ready =
        cluster.wait_until_ready(std::chrono::seconds{ 30 }).get();
    } catch (...) {
      leave_child(4);
    }
    if (!tls) {
      // Observation window: a child reading the inherited connections drains the queues here.
      const auto window_end = std::chrono::steady_clock::now() + std::chrono::seconds{ 2 };
      const auto any_queued = [&sockets]() {
        return std::any_of(sockets.begin(), sockets.end(), [](int fd) {
          return queued_noop_response_end(fd, opaque).has_value();
        });
      };
      while (any_queued() && std::chrono::steady_clock::now() < window_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds{ 10 });
      }
    }
    // The child closes every connection it inherited: a descriptor still open on the same socket
    // is the parent's connection, held open by the child.
    for (std::size_t i = 0; i < sockets.size(); ++i) {
      if (inherited_inodes[i].has_value() && socket_inode(sockets[i]) == inherited_inodes[i]) {
        leave_child(5);
      }
    }
    // A handle acquired before the fork still holds the inherited core here, so only a completed
    // teardown can have released its buckets.
    if (!inherited_bucket.expired()) {
      leave_child(6);
    }
    // A wrapper SDK drops such a handle in the child when its garbage collector runs. After that
    // nothing holds the inherited core.
    {
      [[maybe_unused]] const auto dropped = std::move(collection);
    }
    if (!inherited_listener.expired()) {
      leave_child(7);
    }
    leave_child(0);
  }

  const auto child = wait_for_child(child_pid);

  // Judged before notify_fork(parent), whose own reads would consume the queues. Each response is
  // then read out through its end, so the parent's sessions resume on a frame boundary.
  std::vector<int> drained{};
  std::vector<int> short_reads{};
  if (!tls) {
    for (const auto fd : sockets) {
      const auto end = queued_noop_response_end(fd, opaque);
      if (!end) {
        drained.push_back(fd);
        continue;
      }
      std::vector<std::uint8_t> discard(end.value());
      std::size_t consumed{ 0 };
      while (consumed < discard.size()) {
        const auto rc = recv(fd, discard.data() + consumed, discard.size() - consumed, MSG_WAITALL);
        if (rc < 0 && errno == EINTR) {
          continue;
        }
        if (rc <= 0) {
          break;
        }
        consumed += static_cast<std::size_t>(rc);
      }
      if (consumed != discard.size()) {
        short_reads.push_back(fd);
      }
    }
  }
  cluster.notify_fork(couchbase::fork_event::parent);

  if (!tls) {
    INFO("connections the child read from: " << drained.size() << " of " << sockets.size());
    CHECK(drained.empty());
    // A partial read leaves that session mid-frame, so the get below would fail for a reason
    // other than the child.
    INFO("connections left mid-frame: " << short_reads.size());
    CHECK(short_reads.empty());
  }
  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 4 = notify_fork(child) threw, 5 = the child kept an inherited connection open,
  // 6 = the inherited core's teardown did not complete, 7 = the inherited core was not destroyed.
  CHECK(WEXITSTATUS(child.status) == 0);
  {
    auto [err, res] = collection.get(parent_id).get();
    INFO("parent get after the fork: " << err.ec().message());
    CHECK_FALSE(err.ec());
  }
  cluster.close().get();
}

namespace
{
struct client_record_lookup {
  bool listed{ false };
  // Set when the lookup itself failed, so that an unlisted client is not mistaken for a failed
  // read.
  std::string error{};
};

// Whether the transactions client record in the bucket's default collection lists the client.
auto
lookup_client_record(const couchbase::cluster& cluster,
                     const std::string& bucket,
                     const std::string& client_uuid) -> client_record_lookup
{
  auto [err, res] = cluster.bucket(bucket)
                      .default_collection()
                      .lookup_in("_txn:client-record",
                                 couchbase::lookup_in_specs{
                                   couchbase::lookup_in_specs::get("records.clients").xattr(),
                                 })
                      .get();
  if (err.ec()) {
    return { false, err.ec().message() };
  }
  if (!res.exists(0)) {
    return {};
  }
  const auto clients = res.content_as<tao::json::value>(0);
  return { clients.is_object() && clients.find(client_uuid) != nullptr };
}
} // namespace

// CXXCBC-914: a forked child must not remove the transactions client record it inherited. The
// record belongs to the parent, which stays registered under it. The parent's own close() goes
// through the same hook, which shows the hook is reachable.
TEST_CASE("integration: a forked child leaves the parent's client record registered",
          "[integration]")
{
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  static std::atomic<int> removals{ 0 };
  removals = 0;
  auto cleanup_hooks = std::make_shared<couchbase::core::transactions::cleanup_testing_hooks>();
  cleanup_hooks->client_record_before_remove_client =
    [](const std::string&,
       couchbase::core::utils::movable_function<void(
         std::optional<couchbase::core::transactions::error_class>)>&& handler) {
      ++removals;
      handler({});
    };

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  // Registers the keyspace when the cluster opens, so close() has a record to remove there.
  options.transactions().cleanup_config().add_collection({ ctx.bucket, "_default", "_default" });
  options.transactions().test_factories(
    std::make_shared<couchbase::core::transactions::attempt_context_testing_hooks>(),
    cleanup_hooks);
  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());

  // A cleanup thread registers the parent after the cluster opens.
  const auto parent_uuid = couchbase::extract_core_transactions(cluster)->cleanup().client_uuid();
  client_record_lookup last_lookup{};
  const auto registered = test::utils::wait_until(
    [&last_lookup, &registering = cluster, &ctx, &parent_uuid]() {
      last_lookup = lookup_client_record(registering, ctx.bucket, parent_uuid);
      return last_lookup.listed;
    },
    std::chrono::seconds{ 30 });
  if (!registered) {
    FAIL("the parent did not register in the client record within 30 seconds; last lookup error: "
         << last_lookup.error);
  }

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      // Nothing else holds the inherited cluster, so its teardown completes in here.
      cluster.notify_fork(couchbase::fork_event::child);
    } catch (...) {
      leave_child(4);
    }
    if (removals != 0) {
      leave_child(5);
    }
    leave_child(0);
  }

  const auto child = wait_for_child(child_pid);
  // The parent's cleanup stays stopped until notify_fork(parent), so it cannot re-register before
  // this read. A separate cluster does the read: the parent's I/O is stopped too.
  std::string observer_error{};
  bool parent_still_registered{ false };
  try {
    auto observer_options = couchbase::cluster_options(ctx.username, ctx.password);
    observer_options.apply_profile("wan_development");
    auto [observer_err, observer] =
      couchbase::cluster::connect(ctx.connection_string, observer_options).get();
    if (observer_err.ec()) {
      observer_error = observer_err.ec().message();
    } else {
      const auto lookup = lookup_client_record(observer, ctx.bucket, parent_uuid);
      parent_still_registered = lookup.listed;
      observer_error = lookup.error;
      observer.close().get();
    }
  } catch (const std::exception& e) {
    observer_error = e.what();
  }
  cluster.notify_fork(couchbase::fork_event::parent);
  cluster.close().get();

  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 4 = notify_fork(child) threw, 5 = the child tried to remove the parent's client record.
  CHECK(WEXITSTATUS(child.status) == 0);
  INFO("observer: " << observer_error);
  CHECK(parent_still_registered);
  CHECK(removals > 0);
}

namespace
{
// Forwards each connection accepted on 127.0.0.1 to one server address, until stop(). A cluster
// that bootstraps through it loses its seed when it stops, while the server itself stays up.
class tcp_proxy
{
public:
  tcp_proxy(const std::string& host, std::uint16_t port)
  {
    asio::ip::tcp::resolver resolver{ io_ };
    target_ = resolver.resolve(host, std::to_string(port));
    acceptor_.open(asio::ip::tcp::v4());
    acceptor_.bind({ asio::ip::address_v4::loopback(), 0 });
    acceptor_.listen();
    accept();
    thread_ = std::thread{ [this] {
      io_.run();
    } };
  }

  tcp_proxy(const tcp_proxy&) = delete;
  tcp_proxy(tcp_proxy&&) = delete;
  auto operator=(const tcp_proxy&) -> tcp_proxy& = delete;
  auto operator=(tcp_proxy&&) -> tcp_proxy& = delete;

  ~tcp_proxy()
  {
    stop();
  }

  [[nodiscard]] auto port() const -> std::uint16_t
  {
    return acceptor_.local_endpoint().port();
  }

  // Joins the thread, then closes the listener and every forwarded connection.
  void stop()
  {
    if (!thread_.joinable()) {
      return;
    }
    io_.stop();
    thread_.join();
    asio::error_code ignored;
    acceptor_.close(ignored);
    for (const auto& socket : sockets_) {
      socket->close(ignored);
    }
  }

private:
  using socket_ptr = std::shared_ptr<asio::ip::tcp::socket>;

  void accept()
  {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket accepted) {
      if (ec) {
        return;
      }
      auto client = std::make_shared<asio::ip::tcp::socket>(std::move(accepted));
      auto server = std::make_shared<asio::ip::tcp::socket>(io_);
      sockets_.push_back(client);
      sockets_.push_back(server);
      asio::async_connect(
        *server, target_, [this, client, server](std::error_code connect_ec, const auto&) {
          if (connect_ec) {
            asio::error_code ignored;
            client->close(ignored);
            return;
          }
          forward(client, server);
          forward(server, client);
        });
      accept();
    });
  }

  void forward(const socket_ptr& from, const socket_ptr& to)
  {
    auto buffer = std::make_shared<std::array<char, 16 * 1024>>();
    from->async_read_some(
      asio::buffer(*buffer), [this, from, to, buffer](std::error_code ec, std::size_t n) {
        if (ec) {
          asio::error_code ignored;
          to->close(ignored);
          return;
        }
        asio::async_write(
          *to, asio::buffer(*buffer, n), [this, from, to, buffer](std::error_code write_ec, auto) {
            if (!write_ec) {
              forward(from, to);
            }
          });
      });
  }

  asio::io_context io_{};
  asio::ip::tcp::acceptor acceptor_{ io_ };
  asio::ip::tcp::resolver::results_type target_{};
  std::vector<socket_ptr> sockets_{};
  std::thread thread_{};
};

// The bootstrap timeout of the clusters below. A child whose notify_fork(child) waits for the
// bootstrap of an unreachable seed is held for all of it.
constexpr std::chrono::seconds reopen_bootstrap_timeout{ 10 };

auto
seconds_since(std::chrono::steady_clock::time_point start) -> double
{
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
}

// Connects through a tcp_proxy, stops the proxy, and forks. The child runs `child_body` against a
// cluster whose seed refuses connections, and exits with its result.
auto
fork_with_seed_gone(const std::function<int(couchbase::cluster&)>& child_body) -> child_wait
{
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  const auto parsed = couchbase::core::utils::parse_connection_string(ctx.connection_string);
  if (parsed.tls || parsed.bootstrap_nodes.empty() ||
      parsed.protocol != couchbase::core::utils::connection_string::protocol_type::mcbp) {
    SKIP("the proxy forwards plain MCBP to one bootstrap node");
  }
  const auto& seed = parsed.bootstrap_nodes.front();
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  // Destroyed before the fork, so neither its thread nor its io_context descriptors reach the
  // child.
  std::optional<tcp_proxy> proxy{};
  proxy.emplace(seed.address, seed.port == 0 ? parsed.default_port : seed.port);
  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  options.timeouts().bootstrap_timeout(reopen_bootstrap_timeout);
  auto [connect_err, cluster] =
    couchbase::cluster::connect(fmt::format("couchbase://127.0.0.1:{}=mcbp", proxy->port()),
                                options)
      .get();
  REQUIRE_SUCCESS(connect_err.ec());
  proxy.reset();

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      leave_child(child_body(cluster));
    } catch (const std::exception& e) {
      fmt::print("CHILD(pid={}): threw: {}\n", getpid(), e.what());
    } catch (...) {
      fmt::print("CHILD(pid={}): threw an unknown exception\n", getpid());
    }
    leave_child(99);
  }
  cluster.notify_fork(couchbase::fork_event::parent);
  auto child = wait_for_child(child_pid);
  cluster.close().get();
  return child;
}
} // namespace

// CXXCBC-1087: notify_fork(child) must not wait for the bootstrap of the replacement cluster. A
// child whose seed is gone would otherwise be held for the whole bootstrap timeout. Requests issued
// during that bootstrap wait for it, and still time out on their own timeout.
TEST_CASE("integration: a forked child does not wait for an unreachable seed", "[integration]")
{
  const auto bucket_name = test::utils::test_context::load_from_environment().bucket;
  const auto child = fork_with_seed_gone([&bucket_name](couchbase::cluster& cluster) {
    const auto half_bootstrap = reopen_bootstrap_timeout.count() / 2.0;
    auto start = std::chrono::steady_clock::now();
    cluster.notify_fork(couchbase::fork_event::child);
    if (const auto elapsed = seconds_since(start); elapsed >= half_bootstrap) {
      fmt::print("CHILD(pid={}): notify_fork(child) took {:.3f}s\n", getpid(), elapsed);
      return 1;
    }
    // A callback on the IO thread, during the reopen, gets the transactions the reopen set up.
    auto io_thread_transactions = std::make_shared<std::promise<bool>>();
    auto io_thread_transactions_seen = io_thread_transactions->get_future();
    cluster.query("SELECT 1=1",
                  couchbase::query_options{}.timeout(std::chrono::seconds{ 1 }),
                  [&cluster, io_thread_transactions](const auto& /* err */, const auto& /* res */) {
                    io_thread_transactions->set_value(
                      couchbase::extract_core_transactions(cluster) != nullptr);
                  });

    const auto options = couchbase::query_options{}.timeout(std::chrono::seconds{ 1 });
    start = std::chrono::steady_clock::now();
    auto [query_err, query_res] = cluster.query("SELECT 1=1", options).get();
    if (const auto elapsed = seconds_since(start);
        query_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print(
        "CHILD(pid={}): query: {} after {:.3f}s\n", getpid(), query_err.ec().message(), elapsed);
      return 2;
    }

    start = std::chrono::steady_clock::now();
    auto [stream_err, stream_res] = cluster.query_stream("SELECT 1=1", options).get();
    if (const auto elapsed = seconds_since(start);
        stream_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print("CHILD(pid={}): query_stream: {} after {:.3f}s\n",
                 getpid(),
                 stream_err.ec().message(),
                 elapsed);
      return 3;
    }

    start = std::chrono::steady_clock::now();
    auto [analytics_err, analytics_res] =
      cluster
        .analytics_query_stream("SELECT 1",
                                couchbase::analytics_options{}.timeout(std::chrono::seconds{ 1 }))
        .get();
    if (const auto elapsed = seconds_since(start);
        analytics_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print("CHILD(pid={}): analytics_query_stream: {} after {:.3f}s\n",
                 getpid(),
                 analytics_err.ec().message(),
                 elapsed);
      return 5;
    }

    // Key-value requests wait for the bootstrap too, and time out on their own timeout.
    auto collection = cluster.bucket(bucket_name).default_collection();
    start = std::chrono::steady_clock::now();
    auto [upsert_err, upsert_res] =
      collection
        .upsert(test::utils::uniq_id("fork-held"),
                tao::json::value{ { "a", 1 } },
                couchbase::upsert_options{}.timeout(std::chrono::seconds{ 1 }))
        .get();
    if (const auto elapsed = seconds_since(start);
        upsert_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print(
        "CHILD(pid={}): upsert: {} after {:.3f}s\n", getpid(), upsert_err.ec().message(), elapsed);
      return 6;
    }

    start = std::chrono::steady_clock::now();
    auto [replica_err, replica_res] =
      collection
        .get_any_replica(test::utils::uniq_id("fork-held"),
                         couchbase::get_any_replica_options{}.timeout(std::chrono::seconds{ 1 }))
        .get();
    if (const auto elapsed = seconds_since(start);
        replica_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print("CHILD(pid={}): get_any_replica: {} after {:.3f}s\n",
                 getpid(),
                 replica_err.ec().message(),
                 elapsed);
      return 7;
    }

    start = std::chrono::steady_clock::now();
    auto [lookup_err, lookup_res] =
      collection
        .lookup_in_all_replicas(
          test::utils::uniq_id("fork-held"),
          couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") },
          couchbase::lookup_in_all_replicas_options{}.timeout(std::chrono::seconds{ 1 }))
        .get();
    if (const auto elapsed = seconds_since(start);
        lookup_err.ec() != couchbase::errc::common::unambiguous_timeout ||
        elapsed >= half_bootstrap) {
      fmt::print("CHILD(pid={}): lookup_in_all_replicas: {} after {:.3f}s\n",
                 getpid(),
                 lookup_err.ec().message(),
                 elapsed);
      return 8;
    }

    if (!io_thread_transactions_seen.get()) {
      fmt::print("CHILD(pid={}): a callback on the IO thread saw no transactions\n", getpid());
      return 4;
    }

    // A transaction whose operations cannot reach the cluster reports an error.
    auto [txn_err, txn_res] = cluster.transactions()->run(
      [&collection](
        std::shared_ptr<couchbase::transactions::attempt_context> attempt) -> couchbase::error {
        auto [insert_err, doc] = attempt->insert(
          collection, test::utils::uniq_id("fork-txn"), tao::json::value{ { "a", 1 } });
        return insert_err;
      },
      couchbase::transactions::transaction_options{}.timeout(std::chrono::seconds{ 2 }));
    if (!txn_err.ec()) {
      fmt::print("CHILD(pid={}): a transaction without a cluster succeeded\n", getpid());
      return 9;
    }
    return 0;
  });
  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = notify_fork(child) waited for the bootstrap; 2 = the query, 3 = the streaming query, 5 =
  // the streaming analytics query, 6 = the upsert, 7 = get_any_replica, or 8 =
  // lookup_in_all_replicas did not time out on its own timeout; 4 = a callback on the IO thread saw
  // no transactions; 9 = a transaction succeeded without a cluster; 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

TEST_CASE("integration: closing a forked child's cluster cancels its pending reopen",
          "[integration]")
{
  const auto child = fork_with_seed_gone([](couchbase::cluster& cluster) {
    cluster.notify_fork(couchbase::fork_event::child);
    const auto start = std::chrono::steady_clock::now();
    cluster.close().get();
    if (const auto elapsed = seconds_since(start);
        elapsed >= reopen_bootstrap_timeout.count() / 2.0) {
      fmt::print("CHILD(pid={}): close() took {:.3f}s\n", getpid(), elapsed);
      return 1;
    }
    return 0;
  });
  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = close() waited for the bootstrap, 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

// A second fork from a child whose reopen is still pending. reopen() constructs the transactions,
// and so starts their cleanup, inside notify_fork(child). prepare stops that cleanup before
// io_.stop() and the IO thread join, so the second fork completes and the grandchild exits 0
// without a cleanup thread alive across it.
TEST_CASE("integration: a forked child can fork again while its reopen is pending", "[integration]")
{
  const auto child = fork_with_seed_gone([](couchbase::cluster& cluster) {
    cluster.notify_fork(couchbase::fork_event::child);
    cluster.notify_fork(couchbase::fork_event::prepare);
    const auto grandchild_pid = fork();
    if (grandchild_pid < 0) {
      cluster.notify_fork(couchbase::fork_event::parent);
      return 1;
    }
    if (grandchild_pid == 0) {
      cluster.notify_fork(couchbase::fork_event::child);
      leave_child(0);
    }
    cluster.notify_fork(couchbase::fork_event::parent);
    const auto grandchild = wait_for_child(grandchild_pid);
    if (!grandchild.error.empty() || !WIFEXITED(grandchild.status) ||
        WEXITSTATUS(grandchild.status) != 0) {
      fmt::print("CHILD(pid={}): grandchild failed: status={} {}\n",
                 getpid(),
                 grandchild.status,
                 grandchild.error);
      return 2;
    }
    return 0;
  });
  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = the second fork failed, 2 = the grandchild failed, 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

// Requests issued straight after notify_fork(child) wait for the reopen's bootstrap. A query
// issued before the cluster has a configuration would otherwise fail with service_not_available.
TEST_CASE("integration: a forked child's requests wait for its reopen", "[integration]")
{
  bool supports_query{ false };
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
    supports_query = integration.cluster_version().supports_query();
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());
  const auto upsert_id = test::utils::uniq_id("fork-reopen-upsert");
  const auto txn_id = test::utils::uniq_id("fork-reopen-txn");

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      cluster.notify_fork(couchbase::fork_event::child);
      // Both read or write origin_ while the reopen's bootstrap rewrites it.
      if (auto [origin_ec, origin] = couchbase::extract_core_cluster(cluster).origin(); origin_ec) {
        fmt::print("CHILD(pid={}): origin() failed: {}\n", getpid(), origin_ec.message());
        leave_child(4);
      }
      if (auto auth_err = cluster.set_authenticator(
            couchbase::password_authenticator{ ctx.username, ctx.password });
          auth_err.ec()) {
        fmt::print(
          "CHILD(pid={}): set_authenticator failed: {}\n", getpid(), auth_err.ec().message());
        leave_child(5);
      }
      if (supports_query) {
        auto [query_err, query_res] = cluster.query("SELECT 1=1", {}).get();
        if (query_err.ec()) {
          fmt::print("CHILD(pid={}): query failed: {}\n", getpid(), query_err.ec().message());
          leave_child(1);
        }
      }
      auto collection = cluster.bucket(ctx.bucket).default_collection();
      auto [upsert_err, upsert_res] =
        collection.upsert(upsert_id, tao::json::value{ { "side", "child" } }).get();
      if (upsert_err.ec()) {
        fmt::print("CHILD(pid={}): upsert failed: {}\n", getpid(), upsert_err.ec().message());
        leave_child(2);
      }
      auto [txn_err, txn_res] = cluster.transactions()->run(
        [&collection, &txn_id](
          std::shared_ptr<couchbase::transactions::attempt_context> attempt) -> couchbase::error {
          auto [insert_err, doc] =
            attempt->insert(collection, txn_id, tao::json::value{ { "a", 1 } });
          return insert_err;
        });
      if (txn_err.ec()) {
        fmt::print("CHILD(pid={}): transaction failed: {}\n", getpid(), txn_err.ec().message());
        leave_child(3);
      }
    } catch (const std::exception& e) {
      fmt::print("CHILD(pid={}): threw: {}\n", getpid(), e.what());
      leave_child(99);
    } catch (...) {
      fmt::print("CHILD(pid={}): threw an unknown exception\n", getpid());
      leave_child(99);
    }
    leave_child(0);
  }
  cluster.notify_fork(couchbase::fork_event::parent);
  const auto child = wait_for_child(child_pid);
  cluster.close().get();

  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = query failed, 2 = upsert failed, 3 = transaction failed, 4 = origin() failed, 5 =
  // set_authenticator() failed, 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

// close() after a reopen whose bootstrap is known to have succeeded. The pending case is covered by
// "closing a forked child's cluster cancels its pending reopen".
TEST_CASE("integration: closing a forked child's cluster after its reopen succeeds",
          "[integration]")
{
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      cluster.notify_fork(couchbase::fork_event::child);
      if (cluster.wait_until_ready(std::chrono::seconds{ 30 }).get().ec()) {
        leave_child(1);
      }
      cluster.close().get();
    } catch (const std::exception& e) {
      fmt::print("CHILD(pid={}): threw: {}\n", getpid(), e.what());
      leave_child(99);
    } catch (...) {
      fmt::print("CHILD(pid={}): threw an unknown exception\n", getpid());
      leave_child(99);
    }
    leave_child(0);
  }
  cluster.notify_fork(couchbase::fork_event::parent);
  const auto child = wait_for_child(child_pid);
  cluster.close().get();

  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = the reopen did not become ready, 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

// A second fork straight after notify_fork(child), without waiting for the reopen against a
// reachable cluster. The reopen creates the transactions and starts their cleanup threads before
// notify_fork(child) returns, so the cleanup prepare stops includes those threads, and the
// grandchild gets a working cluster.
TEST_CASE("integration: a forked child can fork again while its reopen succeeds", "[integration]")
{
  {
    test::utils::integration_test_guard integration;
    if (integration.cluster_version().is_mock()) {
      SKIP("the mock does not support the fork scenario");
    }
  }
  const auto ctx = test::utils::test_context::load_from_environment();
  test::utils::init_logger();
  setbuf(stdout, nullptr);

  auto options = couchbase::cluster_options(ctx.username, ctx.password);
  options.apply_profile("wan_development");
  auto [connect_err, cluster] = couchbase::cluster::connect(ctx.connection_string, options).get();
  REQUIRE_SUCCESS(connect_err.ec());

  cluster.notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    cluster.notify_fork(couchbase::fork_event::parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    try {
      cluster.notify_fork(couchbase::fork_event::child);
      cluster.notify_fork(couchbase::fork_event::prepare);
      const auto grandchild_pid = fork();
      if (grandchild_pid < 0) {
        cluster.notify_fork(couchbase::fork_event::parent);
        leave_child(1);
      }
      if (grandchild_pid == 0) {
        cluster.notify_fork(couchbase::fork_event::child);
        auto [upsert_err, upsert_res] =
          cluster.bucket(ctx.bucket)
            .default_collection()
            .upsert(test::utils::uniq_id("fork-grandchild"), tao::json::value{ { "a", 1 } })
            .get();
        if (upsert_err.ec()) {
          fmt::print(
            "GRANDCHILD(pid={}): upsert failed: {}\n", getpid(), upsert_err.ec().message());
          leave_child(1);
        }
        leave_child(0);
      }
      cluster.notify_fork(couchbase::fork_event::parent);
      const auto grandchild = wait_for_child(grandchild_pid);
      if (!grandchild.error.empty() || !WIFEXITED(grandchild.status) ||
          WEXITSTATUS(grandchild.status) != 0) {
        fmt::print("CHILD(pid={}): grandchild failed: status={} {}\n",
                   getpid(),
                   grandchild.status,
                   grandchild.error);
        leave_child(2);
      }
    } catch (const std::exception& e) {
      fmt::print("CHILD(pid={}): threw: {}\n", getpid(), e.what());
      leave_child(99);
    } catch (...) {
      fmt::print("CHILD(pid={}): threw an unknown exception\n", getpid());
      leave_child(99);
    }
    leave_child(0);
  }
  cluster.notify_fork(couchbase::fork_event::parent);
  const auto child = wait_for_child(child_pid);
  cluster.close().get();

  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = the second fork failed, 2 = the grandchild failed, 99 = the child threw.
  CHECK(WEXITSTATUS(child.status) == 0);
}

namespace
{
// Whether this process holds a UDP socket bound to `port` on an IPv4 address, among descriptors
// below 1024.
auto
holds_udp_socket_on(std::uint16_t port) -> bool
{
  for (int fd = 0; fd < 1024; ++fd) {
    int type{};
    socklen_t type_len = sizeof(type);
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &type_len) != 0 || type != SOCK_DGRAM) {
      continue;
    }
    sockaddr_storage address{};
    socklen_t address_len = sizeof(address);
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_len) != 0) {
      continue;
    }
    if (address.ss_family == AF_INET &&
        ntohs(reinterpret_cast<const sockaddr_in*>(&address)->sin_port) == port) {
      return true;
    }
  }
  return false;
}
} // namespace

// A process forked while a DNS SRV lookup is pending inherits the lookup's UDP socket and timers.
// The child's fixup must release the socket, or the child can read the response the parent is
// waiting for, and the inherited timers must do nothing when the child runs the io_context.
TEST_CASE("integration: a forked child releases an inherited DNS lookup", "[integration]")
{
  test::utils::init_logger();
  const auto dns_timeout = std::chrono::milliseconds{ 500 };
  bool lookup_completed{ false };
  asio::io_context io{};
  asio::ip::udp::socket silent_nameserver(io, { asio::ip::make_address("127.0.0.1"), 0 });
  // The lookup falls back to TCP on the same port. Only a child that retries the lookup can
  // connect.
  asio::ip::tcp::acceptor tcp_fallback(
    io, { asio::ip::make_address("127.0.0.1"), silent_nameserver.local_endpoint().port() });
  couchbase::core::io::dns::dns_client client(io);
  client.query_srv("srv.example.invalid",
                   "_couchbase",
                   couchbase::core::io::dns::dns_config{
                     "127.0.0.1", silent_nameserver.local_endpoint().port(), dns_timeout },
                   [&lookup_completed](couchbase::core::io::dns::dns_srv_response&& /* timeout */) {
                     lookup_completed = true;
                   });
  auto io_thread = std::thread([&io]() {
    io.run();
  });

  // The query's arrival gives the lookup's local port, and shows its socket is open.
  std::array<char, 512> query{};
  asio::ip::udp::endpoint lookup{};
  silent_nameserver.receive_from(asio::buffer(query), lookup);
  io.stop();
  io_thread.join();
  // Both of the lookup's deadlines expire while the io_context is stopped, so the child runs their
  // handlers as soon as it restarts it.
  std::this_thread::sleep_for(3 * dns_timeout);

  io.notify_fork(asio::execution_context::fork_prepare);
  const auto child_pid = fork();
  if (child_pid < 0) {
    const auto fork_errno = errno;
    io.notify_fork(asio::execution_context::fork_parent);
    FAIL("fork() failed: " << std::strerror(fork_errno));
  }
  if (child_pid == 0) {
    // The fixup cluster_impl::notify_fork(child) runs on its inherited io_context.
    io.notify_fork(asio::execution_context::fork_child);
    couchbase::core::io::detach_inherited_sockets(io);
    if (holds_udp_socket_on(lookup.port())) {
      leave_child(1);
    }
    try {
      io.restart();
      io.run_for(4 * dns_timeout);
    } catch (...) {
      leave_child(2);
    }
    leave_child(lookup_completed ? 3 : 0);
  }
  io.notify_fork(asio::execution_context::fork_parent);
  const auto child = wait_for_child(child_pid);
  tcp_fallback.non_blocking(true);
  asio::ip::tcp::socket retried(io);
  asio::error_code accept_ec{};
  tcp_fallback.accept(retried, accept_ec);

  CHECK(child.error == std::string{});
  REQUIRE(WIFEXITED(child.status));
  // 1 = the child still holds the lookup's socket, 2 = running the inherited io_context threw,
  // 3 = the inherited lookup completed in the child.
  CHECK(WEXITSTATUS(child.status) == 0);
  // The parent stops its io_context as soon as the query arrives, so a connection is the child's
  // retry.
  CHECK(accept_ec == asio::error::would_block);
}

#endif // _WIN32
