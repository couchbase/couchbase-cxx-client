# TD-0001: TSan False Positives from std::promise/std::future and barrier<T>

## Status
Active — mitigated by `.tsan_suppressions`

## Summary
The public C++ API (e.g. `core/impl/collection.cxx`, `core/impl/binary_collection.cxx`,
`core/impl/public_cluster.cxx`) and integration test utilities
(`test/utils/integration_shortcuts.cxx`, `test/test_integration_range_scan.cxx`)
use `std::promise`/`std::future` to bridge asynchronous Asio callbacks back to
synchronous callers.  Under ThreadSanitizer this pattern produces large numbers
of false data-race reports.

## Root Causes

### Source A — libstdc++ `std::promise`/`std::future`

libstdc++ implements the one-time-set synchronization of `std::promise`/`std::future`
shared state via `std::call_once` (backed by `pthread_once`) inside
`std::__future_base::_State_baseV2::_M_set_result`.

TSan does not fully model the happens-before edge established by `pthread_once`,
so it reports spurious data races between:

* Data **written** on the Asio io-thread immediately before `promise::set_value()`
* Data **read** on the main thread immediately after `future::get()`

This manifests as warnings on:
- SDK result type constructors and destructors (`mutation_result`, `get_result`,
  `lookup_in_result`, `query_result`, `analytics_result`, `range_scan_item`, etc.)
- `std::vector<std::byte>` copies via `__tsan_memcpy` (raw document body data)
- `tao::json` object (`std::map`/Rb_tree node) allocation/deallocation
- `std::char_traits<char>::copy` and `::compare` for string fields
- `std::default_delete<couchbase::*>` and `std::__new_allocator<couchbase::*>`
- SDK cluster internals (`cluster_impl`, `tracer_wrapper`, `meter_wrapper`)

The pattern affects every async SDK operation that is awaited through a
`std::promise`/`std::future` pair — i.e. the entire public synchronous API
wrapper layer.

### Source B — `test::utils::barrier<T>` shared_ptr control block

`barrier<T>` (introduced to replace `std::promise`/`std::future` in test
utilities) is heap-allocated as a `shared_ptr`.  The io-thread callback receives
a copy of the shared_ptr and may destroy its copy (triggering `operator delete`
on the control block) while the main thread is still blocked inside
`pthread_cond_wait` on the condvar embedded in that same control block.

TSan reports a race between the `operator delete` write and the atomic read
performed by `pthread_cond_wait` on the futex word, because the freed heap
memory overlaps with the futex word at the OS level.

This is a false positive: the main thread holds its own `shared_ptr` copy for
the entire duration of `barrier<T>::get()`, so the condvar cannot be freed
while it is in use.  By the time `get()` returns, the condvar has already been
signalled and the io-thread's copy being destroyed is safe.

## Mitigation

A `.tsan_suppressions` file at the project root suppresses all known false
positive patterns.  The suppressions are wired into the CMake test macros via:

- `cmake/Sanitizers.cmake` — sets `COUCHBASE_CXX_CLIENT_TSAN_SUPPRESSIONS`
  cache variable to `${PROJECT_SOURCE_DIR}/.tsan_suppressions` when
  `ENABLE_SANITIZER_THREAD=ON`
- `cmake/Testing.cmake` — `integration_test`, `transaction_test`, `unit_test`
  macros pass `TSAN_OPTIONS=suppressions=...` to `catch_discover_tests()`

When running tests manually, set:
```
TSAN_OPTIONS="suppressions=/path/to/.tsan_suppressions"
```

## Decision

Accepted as false positives — suppress rather than restructure.

The `std::promise`/`std::future` pattern in the public API wrapper layer is
correct: `future::get()` establishes a sequentially-consistent happens-before
between the writer (io thread) and the reader (main thread).  The race reports
are an artefact of TSan's incomplete model of `pthread_once`.

Restructuring all public API wrappers to use `std::atomic`-based or
`pthread_mutex`-based synchronization that TSan fully models would be a
significant effort with no correctness benefit, and would make the synchronous
wrappers less idiomatic.

## Future Refactoring Hints

If this technical debt is ever addressed:

1. **Replace `std::promise`/`std::future` in production API wrappers** with an
   inline synchronization primitive that TSan recognises — for example, a
   `std::atomic<bool>` flag protected by `std::atomic_thread_fence(seq_cst)`,
   or a `std::mutex`+`std::condition_variable` pair (as in `barrier<T>`).

2. **Replace `std::promise`/`std::future` in test utilities** — already done
   for `test/utils/integration_shortcuts.cxx` and
   `test/utils/integration_test_guard.cxx` via `test::utils::barrier<T>`.
   The remaining users in `test/test_integration_range_scan.cxx` (the
   `scan_vbucket`, `continue_scan`, and `get_vbucket_map` helpers) could be
   similarly migrated.

3. **Use LLVM libc++ instead of libstdc++** — libc++'s `std::promise`/`std::future`
   implementation uses a `std::mutex`+`std::condition_variable` pair that TSan
   fully models, eliminating Source A entirely.

## Affected Files

### Suppressions / CMake
- `.tsan_suppressions` — suppression rules
- `cmake/Sanitizers.cmake` — wires `TSAN_OPTIONS` for TSan builds
- `cmake/Testing.cmake` — propagates `TSAN_OPTIONS` to CTest

### Test utilities (already fixed — use `barrier<T>` instead of `std::future`)
- `test/utils/integration_shortcuts.hxx` — defines `test::utils::barrier<T>`
- `test/utils/integration_shortcuts.cxx` — uses `barrier<T>` in open/close helpers
- `test/utils/integration_test_guard.cxx` — uses `barrier<T>` in `public_cluster()`

### Test files (partially fixed)
- `test/test_integration_tracer.cxx` — fixed race in `test_span::add_tag()` by adding a mutex
- `test/test_integration_range_scan.cxx` — still uses `std::promise`/`std::future`
  in `get_vbucket_map`, `scan_vbucket`, `continue_scan` helpers (suppressed)

### Production code (uses `std::promise`/`std::future` — suppressed, not changed)
- `core/impl/collection.cxx`
- `core/impl/binary_collection.cxx`
- `core/impl/public_cluster.cxx`
