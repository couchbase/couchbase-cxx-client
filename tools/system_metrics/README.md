# Overview

The system metrics take the majority of the logic from the [sigar couchbase project](https://github.com/couchbase/sigar) (System Information Gatherer and Reporter), which appears to be a fork of [this project](https://github.com/hyperic/sigar).

The goal was to reuse the code in a light-weight manner (we don't need all the logic Couchbase server needs).

# TODO

- Add more system-wide metrics?  `get_system_cpu()` reports the whole machine's busy and total CPU time; memory and thread metrics are per-process.
