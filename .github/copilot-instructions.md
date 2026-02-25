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

- File extensions: headers `.hxx`, sources `.cxx`. Never `.h` or `.cpp`.
- `clang-format` with `IndentWidth: 2`, `ColumnLimit: 100`, `Standard: c++17`.
- Run `bin/check-clang-format` to verify. Apply with `clang-format -i <file>`.
- Run `bin/check-clang-tidy` for linting.
- All identifiers: `snake_case`. Template type parameters: `PascalCase`.
- Internal headers use `"..."`, system/external headers use `<...>`.
- No `#include <iostream>` or `#include <cstdio>` in non-test production code.
- No duplicate `#include` directives in the same translation unit.

## Project layout

```
couchbase/          Public API — headers (.hxx) only. No core/ types exposed here.
core/               Internal implementation (.hxx + .cxx).
  io/               MCBP and HTTP session/IO layer (Asio-based).
  mcbp/             MCBP protocol codec and queue.
  impl/             Misc internal helpers (DNS SRV, retry, etc.).
  error_context/    Error context structs per operation type.
core/cluster.cxx    cluster / cluster_impl (pimpl) — connection lifecycle.
core/bucket.cxx     bucket / bucket_impl — per-bucket KV routing.
core/crud_component.cxx   New KV operations (get, upsert, replace, …).
core/collections_component.cxx  CID resolution and dispatch.
core/response_handler.hxx       Interface for handling MCBP responses.
test/               Integration + unit tests (Catch2).
examples/           Usage samples.
bin/                Helper scripts (check-clang-format, check-clang-tidy, …).
third_party/        Bundled/fetched dependencies.
.clang-format       Formatting rules.
.clang-tidy         Lint rules.
CPPLINT.cfg         cpplint configuration.
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
No exceptions in production code. `key_value_error_context` must be fully
populated before invoking callbacks.

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
(cd build; for i in $(fd test_ --type executable test/); do
  cmake --build . --target "$i"
  ./"$i"
done)
```

Every new KV operation added to `crud_component` must have a corresponding test
in `test/test_integration_crud_component.cxx`. Tests must not rely on fixed
`sleep_for` delays; use mutation tokens or polling loops instead.

## Common defects to flag in code review

1. `#include <iostream>` / `#include <cstdio>` in non-test `.cxx` files.
2. Duplicate `#include` in the same file.
3. Any added line over 100 characters.
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
