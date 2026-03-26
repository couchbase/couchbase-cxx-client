# TSAN-002: queue_request::try_callback accesses timers without lock

## Summary

`queue_request::try_callback()` reads and moves `deadline_` and `retry_backoff_` (both
`shared_ptr<asio::steady_timer>`) without holding `processing_mutex_`. Meanwhile,
`set_deadline()` and `set_retry_backoff()` write these same members from different threads
(dispatching thread sets timers, io thread calls `try_callback` on response). This is a data
race on non-atomic `shared_ptr` members.

`internal_cancel()` correctly acquires `processing_mutex_` before touching these fields, but
`try_callback()` does not.

## Affected code

| File | Line | Description |
|------|------|-------------|
| `core/mcbp/queue_request.cxx` | 152-153 | `cancel_timer(deadline_)` / `cancel_timer(retry_backoff_)` without lock |
| `core/mcbp/queue_request.cxx` | 140-145 | `set_deadline()` / `set_retry_backoff()` write without lock |
| `core/mcbp/queue_request.cxx` | 106-125 | `internal_cancel()` correctly locks `processing_mutex_` |

## Fix plan

Acquire `processing_mutex_` at the start of `try_callback()` before touching the timer
members, matching the pattern already used in `internal_cancel()`.

The lock scope covers timer cancellation and the `is_completed_` check, but releases before
invoking the callback to avoid holding the mutex during potentially long callback execution.
