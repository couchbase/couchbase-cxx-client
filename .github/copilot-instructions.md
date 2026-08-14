# Couchbase C++ Client — Copilot Instructions

High-performance C++17 client library for Couchbase Server. Used by both Copilot
code review and Copilot coding agent.

## Build system

CMake 3.19+ is required. Standard build:

```bash
cmake -S . -B build -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON
cmake --build build -j$(nproc)
```

Key CMake options:
- `COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON` — use bundled BoringSSL (preferred)
- `COUCHBASE_CXX_CLIENT_BUILD_TESTS=ON` — build test binaries
- `COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES=ON` — build examples

Dependencies are managed via `cmake/ThirdPartyDependencies.cmake` and fetched
automatically by CMake (Asio, GSL, nlohmann/json, BoringSSL/OpenSSL).

## Code style and formatting

- File extensions: for new Couchbase C++ client code, prefer headers `.hxx` and sources `.cxx`. Existing C-style or third_party headers may use `.h`, and should not be renamed just to match this convention. Avoid `.cpp` for new sources.
- `clang-format` with `IndentWidth: 2`, `ColumnLimit: 100`, `Standard: c++17`.
- Run `bin/check-clang-format` to verify. Apply with `clang-format -i <file>`.
- Run `bin/check-clang-tidy` for linting.
- All identifiers: `snake_case`. Template type parameters: `PascalCase`.
- Internal headers use `"..."`, system/external headers use `<...>`.
- No `#include <iostream>` or `#include <cstdio>` in non-test production code.
- No duplicate `#include` directives in the same translation unit.

## Project layout

```
couchbase/                      Public API — headers (.hxx) only. No core/ types exposed here.
core/                           Internal implementation (.hxx + .cxx). Selected subdirectories:
  io/                           MCBP and HTTP session/IO layer (Asio-based).
  mcbp/                         MCBP protocol codec and queue.
  protocol/                     MCBP wire types.
  protostellar/                 couchbase2:// (gRPC) transport and generated stubs.
  operations/ management/       Request/response types; management operations.
  transactions/ columnar/       Distributed transactions; columnar/analytics.
  topology/ sasl/ crypto/       Cluster map, authentication, TLS/crypto helpers.
  tracing/ metrics/ logger/     Observability.
  impl/                         Misc internal helpers (DNS SRV, retry, etc.).
  error_context/                Error context structs per operation type.
  utils/ platform/ meta/        Small shared utilities.
core/cluster.cxx                cluster / cluster_impl (pimpl) — connection lifecycle.
core/bucket.cxx                 bucket / bucket_impl — per-bucket KV routing.
core/crud_component.cxx         New KV operations (get, upsert, replace, …).
core/collections_component.cxx  CID resolution and dispatch.
core/response_handler.hxx       Interface for handling MCBP responses.
test/                           Integration + unit tests (Catch2).
test/framework/                 Hand-rolled test harness, NOT Catch2.
test/cng/                       couchbase2:// tests — built on test/framework/.
                                See test/cng/README.md.
tools/                          cbc CLI, fit_performer, system_metrics.
cmake/                          Build and packaging: dependency resolution, the RPM spec,
                                debian/ and APKBUILD templates, tarball_glob.txt. CI runs
                                the configure half of it, none of the packaging half —
                                see the packaging section below.
examples/                       Usage samples.
docs/                           Doxygen input and prose documentation.
bin/                            Helper scripts (check-clang-format, check-clang-tidy, …).
third_party/                    Bundled/fetched dependencies.
.clang-format                   C++ formatting rules (ColumnLimit: 100).
.cmake-format.yaml              CMake formatting rules (line_width: 120).
.clang-tidy                     Lint rules.
CPPLINT.cfg                     cpplint configuration.
```

## Architecture rules

**Public/private separation**: `couchbase/` headers must never include `core/`
headers or expose Asio/BoringSSL/tl::expected types that are not part of the
intentional public API.

**Pimpl pattern**: `cluster`, `bucket`, and similar heavyweight objects own a
`*_impl` class. New state belongs in `*_impl`, not in the outer class.

**Async first**: All KV and network operations are non-blocking (Asio-based).
No `std::future::get` or unbounded `std::condition_variable::wait` in
production paths.

**KV routing**: New KV operations go in `core/crud_component.cxx` and dispatch
via `collections_component::dispatch` using `mcbp::queue_request`. Do not add
new operations to the legacy `mcbp_command<>` template path.

**Error handling**: Use `std::error_code` or `tl::expected<T, std::error_code>`.
No exceptions in the public API surface (`couchbase/`). Internal `core/` code may use
exceptions where already accepted. `key_value_error_context` must be fully populated
before invoking callbacks.

**Tracing**: Every operation dispatched via `direct_dispatch`/`direct_re_queue`
must create and close a dispatch span via `bucket_impl::create_dispatch_span` /
`close_dispatch_span`.

**Timeouts**: Idempotent operations use `errc::common::unambiguous_timeout`;
non-idempotent use `errc::common::ambiguous_timeout`. Determined via
`queue_request::idempotent()`.

**Collection IDs**: On `unknown_collection` status, call
`collections_component::handle_collection_unknown` before falling back to
`retry_reason::key_value_collection_outdated`.

## Testing

Most tests require a running Couchbase cluster. Set up the environment:

```bash
export TEST_CONNECTION_STRING=$(cbdinocluster connstr \
  $(cbdinocluster ps --json 2>/dev/null | jq -r .[0].id) 2>/dev/null)
export TEST_USERNAME=Administrator
export TEST_PASSWORD=password
export TEST_BUCKET=default
```

Run a single test binary:

```bash
# Build and run one suite
(cd build && cmake --build . --target test/test_integration_crud_component \
  && ./test/test_integration_crud_component)

# Run a single test case
(cd build && ./test/test_integration_crud_component "integration: upsert")

# List all test cases in a suite
./build/test/test_integration_crud_component --list-tests
```

Run all test suites:

```bash
(cd build; find test -maxdepth 1 -type f -name 'test_*' -executable -print0 | \
  while IFS= read -r -d '' i; do
    cmake --build . --target "$i"
    "$i"
  done)
```

Every new KV operation added to `crud_component` must have a corresponding test
in `test/test_integration_crud_component.cxx`. Tests must not rely on fixed
`sleep_for` delays; use mutation tokens or polling loops instead.

### couchbase2:// (CNG) tests

`test/cng/` is a second suite with its own conventions, and the commands above do
not apply to it. It uses the hand-rolled harness in `test/framework/` rather
than Catch2, is gated by `COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS`, and runs under
ctest with the `cng` label. Each case declares whether it needs a cluster;
cluster-only cases skip (exit 77) when `TEST_CONNECTION_STRING` is unset.

**`test/cng/README.md` is the reference, and its setup is not a Couchbase cluster
in a container.** A `couchbase2://` endpoint is served by the Cloud Native
Gateway, which is deployed by the Couchbase Autonomous Operator and therefore
needs Kubernetes: k3d for a local cluster, then `cbdinocluster` with the `cao`
deployer. Consult that document before assuming how these tests are run or
proposing changes to them; several of its constraints are not guessable from the
code, among them that the gateway is only deployed when the cluster definition
carries `cao.gateway-version`, that release versions must be used because a
version with a build number resolves to an image needing an organization token,
that the operator reconciles service memory quotas away unless the
`CouchbaseCluster` resource itself is patched, and that a local gateway needs
`tls_verify=none` because its certificate cannot be chained.

## Common defects to flag in code review

1. `#include <iostream>` / `#include <cstdio>` in non-test `.cxx` files.
2. Duplicate `#include` in the same file.
3. A line over the limit **for its language**: 100 columns for C++ (`.clang-format`,
   `ColumnLimit: 100`), 120 for CMake (`.cmake-format.yaml`, `line_width: 120`).
   Applying the C++ limit to `.cmake`, `CMakeLists.txt` or `cmake/*.txt` produces
   findings the project does not want. Count the columns before reporting one.
4. Single-argument constructors without `explicit`.
5. Accessing an object after `std::move` in the same scope.
6. Magic numeric literals in production paths without a named constant.
7. `tl::expected<…>` or `std::error_code` return values without `[[nodiscard]]`.
8. Adding parameters to pure virtual methods without an ABI-impact comment.
9. `std::set<retry_reason>` in hot-path structs (allocates; prefer flat vector).
10. `std::this_thread::sleep_for` in integration tests.
11. Planning/notes files (`REFACTORING_PLAN.md`, AI-convention files like
    `GEMINI.md`) committed to the repo.
12. `core/` types or Asio headers exposed in `couchbase/` public headers.
13. A comment that contradicts the code beneath it — a stale comment is a false
    specification, not a cosmetic issue. See the documentation lens below.
14. A test that cannot fail. See the test lens below.

## Reviewing the build and packaging code

**Review CMake changes to the same standard as C++ changes.** A defect here costs
more than one in a `.cxx`, because it can stop every build at once rather than one
code path — as a failed `cmake/CPM.cmake` download does, taking out every job that
configures.

CI covers this code unevenly, and the split is what decides how much weight a
finding carries:

- **Exercised on every job**: everything the configure step runs — dependency
  resolution in `cmake/ThirdPartyDependencies.cmake`, `cmake/GrpcProtobuf.cmake`,
  `cmake/CPM.cmake`, compiler and warning options, and the whole target graph.
  A mistake here is usually caught, and caught loudly.
- **Not exercised anywhere**: the install and packaging half. No workflow sets
  `COUCHBASE_CXX_CLIENT_INSTALL`, so no CI leg runs the `install()` rules,
  `bundle_static_library`, the licence installs, or any package build, and none
  uses the RPM spec, `cmake/debian/`, `cmake/APKBUILD.in` or
  `cmake/tarball_glob.txt`. Green CI says nothing about any of it. Review is the
  only gate before a release build, so weigh findings there accordingly.

What has actually broken here, and is worth checking on any change:

- **The source tarball is filtered by patterns, and a missing file is only found
  at install time.** `cmake/tarball_glob.txt` decides what enters the tarball the
  packages build from. Vendored dependencies install *before* this project does,
  so one file a dependency's own `install()` reads and the tarball omits stops the
  whole `%install`/`dh_auto_install` step, after the entire vendored stack has
  compiled. When a dependency is added or bumped, check what its install rules
  read from its source tree, not just what gets linked.
- **The patterns are `find -wholename`, not shell globs.** `**/` still requires an
  intervening path segment, so a `**/` pattern **cannot match a file at the root
  of a tree**; write both the root and nested spellings. Suffixes are literal:
  `*.cmake.in` does not match `zconf.h.cmakein`.
- **That file is read by `xargs`, whose quote handling is disabled deliberately.**
  The read passes `-d "\n"`; without it an apostrophe anywhere in the file —
  including in a comment — aborts `xargs` and silently drops every pattern after
  that line, while the pipeline still exits 0 and the tarball target reports
  success. Apostrophes are therefore fine; removing the delimiter is not. Flag any
  change that drops `-d` from those `xargs` invocations in `cmake/Packaging.cmake`,
  or that adds a new pattern-file read without it.
- **A backslash argument inside `add_custom_command` needs four backslashes**
  (`"\\\\n"`): CMake collapses them once and the shell ninja runs the command in
  collapses them again. Two backslashes silently arrive as a bare letter.
- **Tri-state options must be matched with `STREQUAL` before any boolean test.**
  `if(<var>)` is true for every string CMake does not read as false, so an
  unrecognised or misspelt value takes the ON branch. See
  `COUCHBASE_CXX_CLIENT_SYSTEM_GRPC` in `cmake/GrpcProtobuf.cmake`.
- **Visibility on vendored libraries is not cosmetic.** Hiding gRPC's symbols in a
  build that links both the shared client and a stub library gives the process two
  gRPCs, two `ExecCtx` thread-locals, and a crash inside gRPC's own closure list.
- **A licence notice must ship for every dependency compiled into an artefact**,
  and only for those actually built: the installs in `cmake/Packaging.cmake` are
  conditional on the option that built the code. A package that gains a vendored
  dependency and not its notice is a licensing defect, not a nit.
- **dpkg and rpm differ on co-owned files.** rpm reference-counts an identical
  path owned by several subpackages; dpkg refuses it outright. Notices shared
  between Debian binary packages need a per-package copy via `dh_install`
  source/dest pairs.

## Review the same diff from several angles

This is a low-level C++ library: a single change is usually correct from one
viewpoint and wrong from another, and a reviewer who reads it once, linearly,
finds only the defects that viewpoint exposes. Pass over the diff several times,
each time as a different specialist, and say which concern produced a finding.
Not every lens applies to every change; skip the ones that do not, and spend the
effort on those that do.

**API and ABI steward.** Does a public header in `couchbase/` leak a `core/`
type, Asio, BoringSSL or `tl::expected` beyond the intentional surface? Does new
state belong in `*_impl` rather than the outer class? Does a signature, a virtual
method, or a struct layout change break users who compiled against the previous
release?

**Concurrency and lifetime.** Every KV and network path is asynchronous. Who owns
the object the completion handler touches, and can it die first? Is captured
state still alive when the callback runs? Can the handler run twice, or never, on
an error path? Is anything shared between an Asio thread and the caller without
synchronisation? A `shared_ptr` captured into a handler that the same object owns
is a cycle.

**Memory.** Dangling `string_view`/`span` into a buffer that has been reused or
freed; use after `std::move`; a reference captured by `&` in a lambda that
outlives the frame; an index or size computed from an untrusted length.

**Wire protocol.** Encoding and decoding are where silent corruption lives:
endianness, truncation, a field written with the wrong width, a partial read
treated as complete, an extras/key/value framing mistake. For protobuf, a
non-optional proto3 scalar cannot distinguish "absent" from "default" — code that
needs the distinction must track arrival explicitly.

**Errors and observability.** Is the error context fully populated before the
callback runs? Is a failure swallowed, or converted into a success? Is the
idempotent/non-idempotent timeout distinction right? Is the dispatch span closed
on every path, including the error ones? Silent fallbacks that hide a failure are
worse than the failure.

**Performance.** Allocations and copies on the hot path; a container chosen for
convenience in a per-operation struct; a `shared_ptr` copied where a reference
would do; work done under a lock that need not be.

**Portability.** The project builds with GCC, Clang and MSVC, on Linux, macOS and
Windows, x86-64 and aarch64. Warnings differ sharply between them, and a warning
that only Clang emits still fails every macOS and Windows leg — a `class`/`struct`
mismatch is the recurring example. Sanitizer builds are their own configuration:
flags applied per target are an ODR hazard when a type's layout depends on them.

**Security.** Certificate verification and TLS options; credentials or tokens
reaching a log; buffer arithmetic on attacker-controlled lengths.

**Documentation and prose.** Every English sentence attached to the change has to
describe what the code does, at every level: the comment above a line, the
docstring on a public method, the file's header comment, `docs/`, the commit
message, and the pull request body. They drift independently and nothing compares
them, so each is checked against the diff separately.

A comment that contradicts the code beneath it is a defect of the same
seriousness as a wrong condition: it is a specification, and the next person
changes the code to match it. Re-read the check before accepting the sentence
above it. The same applies to a comment that lists what something handles when the
list has grown, and to a commit message or PR body that claims behaviour the diff
does not implement — that text is what people read years later, and it is the
only record that is never re-run.

Comments carry the contract and the reason, not a narration of the code or of how
the change came about. Prefer deleting a comment to keeping one that will drift.
Readability is part of correctness here: a name that misdescribes what a variable
holds, or a function that has quietly grown a second responsibility, costs every
future reader and is worth raising.

**Tests.** Tests specify correct behaviour; they do not record current
behaviour, and they must not lock in a bug or an internal detail that is free to
change.

The litmus test for any test added alongside a fix: **could this test have caught
the bug being fixed?** If not, it is not pulling its weight. The order that
produces such a test is to decide what correct behaviour is, assert that, watch
it **fail** against the current or missing code, and only then implement. A test
that passes the moment it is written, with no change to the code, has demonstrated
nothing — it should be deleted or rewritten, not kept for the count.

So, when reviewing tests, ask what would have to break for this to fail, and
flag it when the answer is "nothing":

- an assertion on a value the code under test also produced
- a skip predicate that can never be false, so a live case cannot fail
- a check of an internal representation rather than the observable contract,
  which fails on refactoring and passes on regression
- a fixed `sleep_for` standing in for a condition
- setup that leaves preconditions implicit, so the case passes for a reason
  other than the one it names — state the preconditions and assert the
  postconditions in full, including what must *not* have changed

## What makes a review finding useful here

- **Verify the premise before reporting.** Several findings in this repository
  have been declined because the rule cited did not exist (a limit that is 120 and
  not 100), because the file was never compiled, or because the claimed mechanism
  did not occur when run. A finding that names a rule should name where the rule is
  configured.
- **State a concrete failure**: the input or configuration, and what goes wrong.
  "This could be a problem" cannot be actioned or refuted. A finding that predicts
  a specific behaviour is valuable even when it turns out to be wrong, because it
  can be tested.
- **Prefer the root cause to the instance.** If the same mistake appears at five
  call sites, say so once and name the shared cause; five separate comments on one
  cause read as five problems.
- **Suppressed findings are still findings.** Anything placed in a collapsed
  block is read and answered like the rest, so it is worth the same care.
- **Do not repeat a declined finding without new evidence.** A finding answered
  with a measurement is closed; raising it again unchanged costs a round and
  produces the same answer.
- **The change may already do what is asked.** Re-read the current diff before
  reporting; a review of an earlier revision produces findings the code has
  already addressed.
