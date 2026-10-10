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

#include "core/utils/connection_string.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/diagnostics_result.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/fork_event.hxx>
#include <couchbase/service_type.hxx>

#include <tao/json/value.hpp>

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
  if (!tls) {
    for (const auto fd : sockets) {
      const auto end = queued_noop_response_end(fd, opaque);
      if (!end) {
        drained.push_back(fd);
        continue;
      }
      std::vector<std::uint8_t> discard(end.value());
      [[maybe_unused]] const auto consumed = recv(fd, discard.data(), discard.size(), MSG_WAITALL);
    }
  }
  cluster.notify_fork(couchbase::fork_event::parent);

  if (!tls) {
    INFO("connections the child read from: " << drained.size() << " of " << sockets.size());
    CHECK(drained.empty());
  }
  CHECK(child.error == std::string{});
  CHECK(WIFEXITED(child.status));
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

#endif // _WIN32
