# TSAN-001: tracer/meter races during cluster lifecycle

## Summary

TSan reports data races on `tracer_` and `meter_` members of `cluster_impl`:
1. **Construction race**: Objects initialized on the connect thread, then accessed after
   `future::get()` on the caller thread. This is a **false positive** -- the promise/future
   provides synchronization but TSan can't see it through uninstrumented `__once_proxy`.
2. **Shutdown race (potential)**: `close()` handler moves `tracer_`/`meter_` on io thread while
   other io threads may still call `tracer()`/`meter()`.

## Fixes applied

1. Changed `tracer()` and `meter()` to return `shared_ptr<T>` by **value** instead of
   `const shared_ptr<T>&` to prevent dangling reference during shutdown.
2. Added `observability_mutex_` to protect `tracer_`/`meter_` during concurrent read+move.
3. Shutdown handler uses scoped lock + move under mutex.
4. Suppressions added for the `__once_proxy` false positive patterns.
