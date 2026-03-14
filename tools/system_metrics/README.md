# Overview

The system metrics take the majority of the logic from the [sigar couchbase project](https://github.com/couchbase/sigar) (System Information Gatherer and Reporter).  Which looks to be a fork from [this project](https://github.com/hyperic/sigar).

The goal was to reuse the code in a light-weight manner (we don't need all the logic Couchbase server needs).

# TODO

- Add system-wide metrics?  Right now the focus is only on per-process metrics.  We could incorporate system-wide metrics fairly easily if we think those would be beneficial.
