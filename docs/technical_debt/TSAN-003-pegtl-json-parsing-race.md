# TSAN-003: PEGTL/JSON response parsing race

## Summary

TSan reports 7 data races in `tao::pegtl::memory_input::peek_char()` during JSON response
parsing. These occur because the JSON response body (`std::string http_body`) is constructed
on the io thread inside the HTTP callback chain, then accessed on the calling thread after
`future::get()`.

These races are all mediated by `std::promise::set_value()` / `std::future::get()` and are
**false positives** caused by the libstdc++ `__once_proxy` trampoline living in uninstrumented
`libstdc++.so`. They are already covered by the `race:__gthread_once` suppression.

## Evidence

- The write side stack always shows `__gthread_once` -> `std::call_once` -> `_M_set_result`
- Standalone reproducers with identical patterns produce 0 TSan warnings
- The reads happen after `future::get()` returns, which establishes happens-before

## Status

**No fix needed.** These are covered by the existing `race:__gthread_once` suppression in
`.tsan_suppressions`. The proper long-term fix is to build with `libc++` for TSan builds.
