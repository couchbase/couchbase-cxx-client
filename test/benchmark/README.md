# Benchmarks

The benchmarks measure the SDK's hot paths and its KV round trip. Each benchmark is a standalone executable. It reports latency and the process's resource use per operation, and throughput for per-operation keys. `bin/check-perf` runs two revisions side by side and gives each measurement a verdict.

## Building

Each `couchbase_add_benchmark()` call in `test/CMakeLists.txt` adds one executable. The function lives in `cmake/TestFramework.cmake`. The target is the path under `test/` with `/` replaced by `_`. A `benchmark_` prefix is added unless the name already has one. `benchmark/kv_fake` builds `benchmark_kv_fake`.

The benchmarks are part of the test tree, so `COUCHBASE_CXX_CLIENT_BUILD_TESTS` must be on. The `build_benchmarks` target builds all of them:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=ON
cmake --build build --config Release --target build_benchmarks
```

The executables land in `build/test/`. `build/benchmarks.txt` lists their paths, one per line. A multi-config generator writes `benchmarks_<CONFIG>.txt` instead. `bin/check-perf` finds the executables through that list.

A Debug build measures unoptimised code. Use `Release` for numbers and `RelWithDebInfo` for profiling.

## Running

Each executable runs every case it registers:

```sh
./build/test/benchmark_kv_fake
```

Positional arguments select what runs. An argument names a case, which selects all its keys, or one key in full. An argument that selects nothing fails the run.

```sh
./build/test/benchmark_kv_fake kv_get            # every key of kv_get
./build/test/benchmark_kv_fake kv_get/fake/t1    # one key
```

Options:

| Option | Meaning |
|---|---|
| `--list` | Print each case and what it requires, then exit. |
| `--warmup MS` | Run each key this long before measuring it. |
| `--min-time MS` | Measure each key at least this long. |
| `--max-time MS` | Stop measuring a key after this long, converged or not. |
| `--ci-target PCT` | Stop once the median's 95% confidence interval is within this relative half-width. |
| `--json FILE` | Write the result JSON that `bin/check-perf` reads. `test/benchmark/report.hxx` documents its schema. |
| `--bmf FILE` | Write Bencher Metric Format. |

`test/benchmark/bench_framework.hxx` holds the defaults of the four timing options. A key stops when both `--min-time` and `--ci-target` are met, or at `--max-time`. `--warmup`, `--min-time` and `--max-time` take at most a day in milliseconds.

Progress goes to stderr and the results table to stdout.

### Against a cluster

`benchmark_kv_cluster` needs a real cluster. It reads the same environment as the other test binaries:

| Variable | Meaning |
|---|---|
| `TEST_CONNECTION_STRING` | The cluster to connect to. Without it, every case is skipped. |
| `TEST_USERNAME` | The user to authenticate as. |
| `TEST_PASSWORD` | That user's password. |
| `TEST_BUCKET` | The bucket whose default collection the cases use. |

`configuration::from_environment()` in `test/framework/context.cxx` reads them. Before measuring, `benchmark_kv_cluster` asks the cluster for its server version and node count through the test framework's probe. They go into the result JSON, and `bin/check-perf` prints them under the machine description. A probe that cannot answer leaves them `null`. The probe keeps its own connection open, idle, for the rest of the run.

### Logging

`benchmark_kv_cluster` logs through the SDK's console logger, on stderr, which the probe initialises at `info`. `TEST_LOG_LEVEL` sets another level, such as `debug`, `warn` or `off`. The other executables open no probe, and the SDK logs nothing in them.

### Exit status

| Status | Meaning |
|---|---|
| 0 | No key failed, and not every case was skipped. |
| 1 | A key failed, a filter matched nothing, or an option was invalid. |
| 77 | Every selected case was skipped, such as `benchmark_kv_cluster` without a cluster. |

## Reading the results

Each key gets one console line. The key is the case name, or the case name and a variant, such as `kv_get/fake/t8`.

A per-operation key times each call by itself. Its line reads:

```text
kv_get/fake/t1   p50 29.9 µs [29.8, 30.0]   p99 46.7 µs   min 19.1 µs … max 633 µs   32.8k ops/s   0 errors   cpu 23.3 µs/op   csw 4.00/op   rss 22.2 MiB
```

A batched key times batches of iterations and divides. It has no percentiles. Its line reads `median … […, …]   mean … σ …   N iterations`, followed by the same resource values.

| Value | Meaning |
|---|---|
| `p50` / `median` | The median latency per operation, or per iteration for a batched key. |
| `[lower, upper]` | The bounds of the median's 95% confidence interval, in the median's unit. The interval need not be symmetric about the median. `[n/a]` for a key with fewer than 6 samples, too few for one; such a key never meets `--ci-target`. |
| `p99` | The 99th percentile latency. Per-operation keys only. |
| `min … max` | The fastest and slowest operation. Per-operation keys only. |
| `ops/s` | Successful operations per second of measurement. Per-operation keys only. |
| `errors` | Operations that failed. Their latency is not recorded. |
| `cpu …/op` | Process CPU time, user plus system, per operation. |
| `csw …/op` | Context switches, voluntary plus involuntary, per operation. Not on Windows. |
| `rss … MiB` | The process's peak resident set size when the key finished. |

The three resource values are process-wide. They count every thread of the process: the SDK's I/O threads, the runner's threads, and for `benchmark_kv_fake` the in-process fake server. CPU time per operation can therefore exceed the latency per operation. The CPU time and context switches cover only the measurement window, after warmup. They are divided by every operation or iteration in that window; for a batched key that includes the short batches that only size the next one, which the latency leaves out. The peak RSS is a high-water mark of the whole process. It never decreases from one key to the next in one run.

A value the platform cannot provide is left out of the console line and is `null` in the result JSON. Windows reads CPU time and peak RSS with `GetProcessTimes()` and `GetProcessMemoryInfo()`, and has no context switch count. Every other platform reads the counters with `getrusage()`.

A `SUSPECT` row is a batched median under 0.5 ns per iteration. The compiler most likely removed the work. A suspect key gets no BMF entry.

A batched key fails when no batch reached 1 ms before `--max-time`. It has no sample to take a median of. A longer `--max-time` fixes it.

## The cases

| Executable | Cases | What they measure |
|---|---|---|
| `benchmark_vbucket_hash` | `vbucket_hash` | CRC32 and key-to-vbucket mapping, on 1 and 8 threads. |
| `benchmark_json` | `json_generate_binary`, `json_streaming_lexer` | JSON encoding, and the streaming lexer on a query response. |
| `benchmark_mcbp_parser` | `mcbp_parser_next` | Parsing MCBP response frames from a buffer, plain and Snappy-compressed. |
| `benchmark_kv_fake` | `kv_get`, `kv_upsert`, `kv_upsert_get` | The KV round trip against an in-process fake server. |
| `benchmark_kv_cluster` | `kv_get`, `kv_upsert`, `kv_upsert_get` | The same KV round trip against a real cluster. |

The first three are component cases. They need no server, and every key is batched.

The KV cases share one implementation in `test/benchmark/kv_cases.hxx`. Both targets get the same documents, the same key sequence and the same cluster options. Their rows are therefore comparable. `kv_get` and `kv_upsert` connect once per profile:

- `default` sets the SDK's default values explicitly: metrics, tracing, application telemetry, compression, and transaction cleanup of lost and of client attempts are all on. A change of an SDK default therefore does not shift the numbers; the profile measures the new default once `connect_options()` is updated to it.
- `bare` turns off metrics, tracing, application telemetry and both transaction cleanups. Compression stays on. The gap between the two profiles is what those features cost.

In `kv_get` and `kv_upsert`, the default profile has three variants, and `bare` adds two:

- `<target>/t1` and `<target>/bare/t1` run one thread and one operation at a time.
- `<target>/t8` runs eight threads, each with one operation in flight.
- `<target>/q64` and `<target>/bare/q64` run one thread that keeps 64 operations in flight. They are batched, so their latency is the time per operation once round trips overlap.

Both profiles connect and seed the documents even when the filter selects keys of only one of them.

`kv_upsert_get` runs the default profile only. One operation is an UPSERT and then a GET of the same key, each waited for. It stores 100,000 documents of 512 bytes of JSON before measuring. Thread `w`, on its `n`-th operation, uses document `(n * 20 + w) % 100000`. Its variants:

- `<target>/t20` runs 20 threads that share one `cluster` object. Every UPSERT builds a `tao::json::value` and stores it through the default transcoder, and every GET decodes it.
- `<target>/t20/raw` is `t20` with the document serialised once, stored and read back through `raw_json_transcoder`, which does not parse it.
- `<target>/c20` is `t20` with a `cluster` object per thread, connected before and closed after the measurement.

`kv_upsert_get` seeds its documents and connects the 20 `cluster` objects of `c20` even when the filter selects only `t20` or `t20/raw`.

A failed GET or UPSERT fails the case in every variant, with the SDK's error message.

The target is the second segment of the key. `fake` is `test/benchmark/fake_server.cxx`, an MCBP server in the same process. It answers from memory, so the SDK's own cost dominates. `cluster` is the server from `TEST_CONNECTION_STRING`, so network and server time are included.

## Comparing two revisions

`bin/check-perf` builds and compares revisions. Run `bin/check-perf --help` for its usage. The comment at the top of the script describes every command.

To build one revision in Release into a directory:

```sh
bin/check-perf build HEAD~1 ../bench-base
```

To compare two sides, give each as a build directory or a revision. A revision is built under `.perf-work/` first.

```sh
bin/check-perf compare ../bench-base build --suite kv_fake --rounds 7 --out perf-report
```

| Option | Meaning |
|---|---|
| `--suite NAME` | Compare only `benchmark_NAME`. Repeatable. The default is every executable. |
| `--rounds N` | Rounds to run. The default is in `ROUNDS` in the script. |
| `--out DIR` | Where to write the reports. The default is `perf-report`. |

A round runs every selected executable of both sides, alternating which side goes first. `compare` writes four files to `--out`:

- `report.txt`, the table it also prints;
- `comment.md`, the same table in Markdown;
- `result.json`, every row with its statistics;
- `head.bmf.json`, the head's BMF.

Each row gets up to three verdicts: median latency, CPU time per operation, and throughput. All three follow the same rules. A row without CPU time or throughput on either side shows `-` for that verdict. The verdicts:

- `faster`, `slower`
- `likely faster`, `likely slower`: a hint to re-run, not a finding
- `noisy`
- `no change`
- `new`, `removed`
- `failed`

The table at the top of `bin/check-perf` defines each one, and the constants below it set the thresholds. A row that fewer than 6 rounds measured at both sides is `noisy`: its ratios are too few for a 95% interval. With `--rounds` below 6, every row is therefore `noisy`, `new`, `removed` or `failed`.

Throughput is the operations per second of all threads of the key. Only per-operation keys have it. On several threads, a change can raise the latency of each operation and still raise the throughput. Latency and CPU time use the per-round ratios head/base. Throughput uses base/head, since higher is better, so more throughput at head reads `faster`. Its change column shows head/base, so `+%` means more throughput.

A skip or a failure recorded before its case reached a key is keyed `<suite>/<case>`, such as `kv_cluster/kv_get`. `kv_fake` and `kv_cluster` share case names.

The `perf` workflow runs the same comparison:

- on every pull request to `main` or `release-*`;
- on every push to those branches;
- every night on `main`, at 10:17 UTC.

A pull request gets the report as a comment. The `perf-comment` workflow renders that comment from `result.json` with the code of the default branch. Every value is checked first, so a pull request cannot put markup, a mention or a reference into it.

A push to `main` and each nightly run add their rows to the performance history on the GitHub Pages site. It charts the latency, throughput, CPU time per operation and peak RSS of each key. Higher is better only for throughput. Every run is a point of its own, so the nightly runs extend the lines while `main` does not change. A point of a cluster key records the server version. A hollow ring marks a point whose CPU model or server version differs from the point before it. The deployed pages, the ABI page included, are rendered by that run's `bin/check-perf site` and `bin/check-abi`. Only the performance data and the ABI reports come from `refs/site/pages`.

To compare a build with itself, give the same directory twice. Any verdict other than `no change` then measures the machine's noise.

## Profiling with perf

The binary's own `cpu` and `csw` values cover the measurement window only. `perf` adds what they do not show: CPU migrations, system calls, and where the time goes.

Build with symbols and frame pointers:

```sh
cmake -S . -B build-perf -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=ON
cmake --build build-perf --target benchmark_kv_fake
```

Profile one key at a time. Raise `--min-time` so that connecting, seeding and warmup are a small share of the run. A `--min-time` above the default `--max-time` is capped by it, so raise both for a longer run.

### Counting events

```sh
perf stat -e task-clock,context-switches,cpu-migrations -- \
  ./build-perf/test/benchmark_kv_fake --warmup 300 --min-time 10000 --json kv.json kv_get/fake/t1
```

`perf stat` counts the whole process, including connecting, seeding and warmup. Divide its counts by `operations` from `kv.json` for an upper bound per operation. The binary's `cpu_ns_per_op` and `context_switches_per_op` are the exact values for the window.

Adding `syscalls:sys_enter_futex` to `-e` counts futex calls, a measure of thread hand-offs. Tracepoints need read access to `/sys/kernel/tracing`, which is usually root only.

### Sampling call stacks

```sh
perf record --call-graph dwarf -o kv.perf.data -- \
  ./build-perf/test/benchmark_kv_fake --warmup 300 --min-time 3000 kv_get/fake/t1
perf report -i kv.perf.data
```

`--call-graph dwarf` copies a slice of the stack with every sample and unwinds it afterwards. It needs no frame pointers, but the file grows fast, so a few seconds of measurement is enough. With the frame-pointer build, `--call-graph fp` writes a far smaller file. It can miss frames in libraries built without frame pointers. `perf report --no-children` sorts by self time rather than by time including callees.

### Permissions

`kernel.perf_event_paranoid` decides what a normal user may profile:

| Value | Allowed |
|---|---|
| 2 | Own processes, user space only. |
| 1 | Own processes, with kernel stacks. |
| 0 | System-wide counts as well. |
| -1 | Everything. |

Some distributions default to a value above 2, which blocks a normal user entirely. Check it, and lower it to 2 if it is higher:

```sh
sysctl kernel.perf_event_paranoid
sudo sysctl kernel.perf_event_paranoid=2
```
