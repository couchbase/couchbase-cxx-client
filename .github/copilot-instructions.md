# Couchbase C++ Client: Copilot Instructions

Instructions for Copilot code review and coding agents working on this
repository.

## References

Do not restate or guess what these files already specify:

- [README.md](../README.md): build commands, `ctest` environment variables,
  formatting and lint checks, and contributor conventions.
- [test/README.md](../test/README.md): the standard every test is written and
  reviewed against (`test/framework/`, watching assertions fail, choosing a
  venue that can distinguish the failure, requirements rather than skip guards,
  and the reviewer's checklist). New tests use `test/framework/`; Catch2 is
  being removed (CXXCBC-945) and no new test may use it.
- [test/cng/README.md](../test/cng/README.md): `couchbase2://` (Cloud Native
  Gateway) tests, including local k3d/Autonomous Operator constraints and the
  `pins_callback_queue` / `pin_callback_queue()` requirement for clusterless
  in-process `grpc::ServerBuilder` tests. Read it before touching `test/cng/`
  or `core/protostellar/`.
- [core/logger/redaction.hxx](../core/logger/redaction.hxx) (and
  `core/document_id_redaction.hxx` for `document_id`): `CB_LOG_*` redaction
  categories, list wrappers, and `not_sensitive()` / `not_redacted()`.

## Scope and verification

### For code review

- **Abstain from nitpicking and scope expansion.** Review only defects
  introduced or exposed by the diff. Do not suggest refactoring surrounding
  code, splitting existing functions, renaming untouched identifiers, cleaning
  up pre-existing code, or widening the feature or API scope of the change.
- **Leave style and mechanical checks to CI.** `.github/workflows/linters.yml`
  and `.github/workflows/abi.yml` already run `check-clang-format` (100 columns
  for C++ in `.clang-format`; `.cmake-format.yaml` sets 120 for CMake),
  `check-clang-tidy`, `check-clang-static-analyzer`, `check-cppcheck`,
  `check-log-annotations`, `check-export-map`, `check-action-pins.rb`, and
  `check-abi`. Do not count line columns, police include sorting or whitespace,
  argue over naming taste, or flag numeric literals as "magic numbers".
- **Every suggested change requires a minimal reproducer and a test design.**
  Whenever you report a defect or suggest a code, build, or packaging change,
  include both:
  1. **Minimal reproducer**: the concrete input, wire frame, configuration,
     error sequence, thread interleaving, or build/install command that fails
     against the diff as written.
  2. **Acceptance and verification test**: a test design following
     `test/README.md` that names the venue (unit test, in-process recording
     server, or real cluster; or the exact build/packaging check), sets the
     preconditions, performs the action, and asserts the distinguishing
     postcondition so the check fails before the suggested change and passes
     after it.

  A comment or pull-request description that contradicts the code is the one
  exception, and must quote both the prose and the contradicting code. If you
  cannot state a concrete failure and a falsifiable check, abstain from posting
  the comment.

### For coding and fixing agents

- **Write a minimal reproducer first and watch it fail.** Every bug fix or
  behavioural change needs an acceptance and verification test in
  `test/framework/` following `test/README.md`. Build and watch the named
  assertion fail against the unpatched code (or revert the fix as a negative
  control and confirm the control build compiled and failed). State how the
  assertion was observed failing in the commit or pull-request description.
- **Keep fixes minimal; do not expand scope.** Change only what is needed to
  fix the stated issue and pass its test. Do not refactor adjacent functions,
  rename unrelated symbols, touch callers outside the causal path, or widen
  scope because a review comment hinted at broader cleanup. When a review
  comment is out of scope, speculative, or already handled, answer with concrete
  evidence from the tree rather than making unrelated edits.
- **Run local checks.** Format touched C++ files with `clang-format -i`, run
  `./bin/check-log-annotations` when touching `CB_LOG_*` lines, and run
  `CB_TIDY_DIFF_BASE=origin/main ./bin/check-clang-tidy` when modifying library
  C++ sources.

## Architecture rules

**Public/private separation**: `couchbase/` headers must never include `core/`
headers or expose Asio, BoringSSL, or `tl::expected` types that are not part of
the intentional public API checked by `bin/check-export-map`.

**Pimpl pattern**: `cluster`, `bucket`, and similar heavyweight objects own a
`*_impl` class. New state belongs in `*_impl`, not in the outer class.

**Async first**: All KV and network operations are non-blocking (Asio-based).
No `std::future::get` or unbounded `std::condition_variable::wait` in
production paths.

**KV routing**: New KV operations go in `core/crud_component.cxx` and dispatch
via `collections_component::dispatch` using `mcbp::queue_request`. Do not add
new operations to the legacy `operations::mcbp_command<>` template path.

**Error handling**: Use `std::error_code` or `tl::expected<T, std::error_code>`
(`[[nodiscard]]`). No exceptions on the public API surface (`couchbase/`).
`key_value_error_context` must be populated before invoking callbacks.

**Tracing and timeouts**: Every dispatched request must close its dispatch and
request spans on every completion path, including cancellations and early
errors. Idempotent requests (and non-idempotent requests never sent over the
wire) report `errc::common::unambiguous_timeout`; in-flight non-idempotent
requests report `errc::common::ambiguous_timeout` (see
`mcbp::queue_request::idempotent()`).

**Collection IDs**: On `unknown_collection` status, call
`collections_component::handle_collection_unknown` before falling back to
`retry_reason::key_value_collection_outdated`.

**Source rules**: Internal headers use `"..."`, system and external headers use
`<...>`. No `#include <iostream>` or `#include <cstdio>` in non-test production
code. Never commit planning, scratch, or AI-convention files (`PLAN.md`,
`GEMINI.md`, `AGENTS.md`, etc.) to the repository.

## Reviewing the build and packaging code

Review CMake changes to the same standard as C++ changes. A defect here stops
every build at once rather than one code path.

CI covers this code unevenly, and the split decides how much weight a finding
carries:

- **Exercised on pull requests**: the configure and build graph
  (`cmake/ThirdPartyDependencies.cmake`, `cmake/GrpcProtobuf.cmake`,
  `cmake/CPM.cmake`, compiler and warning flags) plus `bin/check-abi` and
  `bin/check-export-map`, which build the shared library with
  `COUCHBASE_CXX_CLIENT_PACKAGE_BUILD=ON` (`BUILD_STATIC=OFF`,
  `BUILD_COUCHBASE2=OFF`).
- **Not exercised in CI**: `COUCHBASE_CXX_CLIENT_INSTALL`, `install()` rules,
  `bundle_static_library`, `COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2` packaging,
  licence installs, RPM/Debian/APK package builds (`cmake/debian/`,
  `cmake/APKBUILD.in`), and `cmake/tarball_glob.txt`. Green CI says nothing
  about any of it.

What has broken here, and is worth checking on relevant diffs:

- **Source tarball filtering (`cmake/tarball_glob.txt`)**: decides what enters
  the release source tarball, and omissions only fail at `%install` /
  `dh_auto_install` time after the vendored stack compiles. When a dependency is
  added or bumped, check what its `install()` rules read from its source tree.
- **`find -wholename` patterns, not shell globs**: `**/` requires at least one
  intervening path segment and cannot match a file at the root of a tree; write
  both root and nested spellings. Suffixes are literal (`*.cmake.in` does not
  match `zconf.h.cmakein`).
- **`xargs -d "\\\\n"` in `cmake/Packaging.cmake`**: disables `xargs` quote
  parsing so an apostrophe in `tarball_glob.txt` (including in a comment) does
  not abort `xargs` and silently drop later patterns while the pipeline still
  exits 0. Four backslashes are required inside `add_custom_command` because
  CMake and the shell each collapse them once. Flag any change that drops `-d`
  or adds a pattern-file read without it.
- **Tri-state CMake options**: match with `STREQUAL` before any boolean
  `if(<var>)` test, because `if(<var>)` treats any unrecognised string as true
  (see `COUCHBASE_CXX_CLIENT_SYSTEM_GRPC` in `cmake/GrpcProtobuf.cmake`).
- **Vendored library visibility**: hiding gRPC's symbols in a build linking both
  the shared client and a stub library gives the process two gRPC instances, two
  `ExecCtx` thread-locals, and a crash inside gRPC's closure list.
- **Licence notices and co-owned paths**: `cmake/Packaging.cmake` must install a
  licence notice for every dependency compiled into an artefact, gated on the
  option that built it. `rpm` reference-counts identical paths across
  subpackages, whereas `dpkg` rejects them; shared notices across Debian binary
  packages need per-package copies via `dh_install` source/dest pairs.

## Review the same diff from several angles

Pass over the diff for the specialist concerns that apply to it, skip those that
do not, and name the concern that produced each finding:

- **API and ABI**: public headers in `couchbase/` leaking `core/` or internal
  types; state placed on an outer class instead of `*_impl`; signature, virtual
  method, or struct layout changes breaking ABI compatibility.
- **Concurrency and lifetime**: ownership of objects touched by async completion
  handlers; captured state outliving its owner; handlers running twice or never
  on error/cancel paths; unsynchronised sharing between Asio threads and
  callers; `shared_ptr` cycles between an object and a handler it stores.
- **Memory**: dangling `string_view` or `span` into reused/freed buffers; use
  after `std::move`; `&`-captures outliving the stack frame; index or size
  arithmetic on untrusted lengths.
- **Wire protocol**: endianness, width truncation, partial reads treated as
  complete, MCBP framing mistakes, and non-optional proto3 scalars where absent
  must be distinguished from default.
- **Errors and observability**: error contexts populated before callbacks run;
  swallowed failures; timeout ambiguity classification; dispatch spans closed on
  every path including errors.
- **Performance**: avoidable allocations or copies on per-operation hot paths
  (such as `std::set<retry_reason>` where a flat vector suffices); work held
  under a lock unnecessarily.
- **Portability**: GCC, Clang, and MSVC across Linux, macOS, and Windows on
  `x86-64` and `aarch64` (`class`/`struct` forward-declaration mismatches fail
  Clang/MSVC builds; per-target sanitizer flags risk ODR layout mismatches).
- **Security**: TLS certificate verification options, credentials or tokens
  reaching logs, and unchecked arithmetic on network-supplied lengths.
- **False specifications in comments and prose**: a comment, docstring, commit
  message, or pull-request body that contradicts what the diff actually
  implements. Flag direct contradictions between prose and code; do not critique
  prose style or ask for narrative comments.
- **Tests**: check test diffs against `test/README.md`: above all, ask what
  would have to break in production code for the assertion to fail, and flag
  tests whose answer is "nothing".

## What makes a review finding useful here

- **Verify the premise before reporting.** Check where a cited rule is actually
  configured, whether the file is compiled, and whether the claimed failure path
  can happen.
- **A path that appears only as a deletion is usually a rename.** Test and
  framework files move in bulk; check paths against the branch rather than the
  diff hunk before reporting missing sources or targets.
- **State a concrete failure, reproducer, and test.** "This could be a problem"
  cannot be actioned or refuted. Pair every finding with its minimal reproducer
  and verification test design as required above.
- **Prefer the root cause to the instance.** If one mistake appears at five call
  sites, report the shared cause once rather than leaving five comments.
- **Suppressed findings are still findings.** Collapsed blocks are read and
  answered like the rest; do not use them to bypass the no-nitpick rule or the
  reproducer and test requirement.
- **Do not repeat a declined finding without new evidence.** A finding answered
  with a measurement or code trace is closed unless new facts contradict it.
- **Re-read the current diff.** Check the latest revision before posting so you
  do not ask for something the change already implements.
