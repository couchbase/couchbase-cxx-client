# cbc-pillowfight - Simple Workload Generator {#cbc-pillowfight}

### NAME

`cbc pillowfight` - run workload generator

### SYNOPSIS

`cbc pillowfight [options] <id>...`<br/>
`cbc pillowfight (-h|--help)`

### DESCRIPTION

Run simple workload generator that sends GET/UPSERT requests with optional N1QL queries.

### OPTIONS

<dl>
<dt>`-h|--help`</dt><dd>Show this screen.</dd>
<dt>`--verbose`</dt><dd>Include more context and information where it is applicable.</dd>
<dt>`--bucket-name=STRING`</dt><dd>Name of the bucket. [default: `default`]</dd>
<dt>`--scope-name=STRING`</dt><dd>Name of the scope. [default: `_default`]</dd>
<dt>`--collection-name=STRING`</dt><dd>Name of the collection. [default: `_default`]</dd>
<dt>`--operation-batch-size=INTEGER`</dt><dd>Number of the operations in a single batch (set to 1 to wait for completion after every operation). With `--pipeline`, the number of operations in flight per worker thread. [default: `100`]</dd>
<dt>`--batch-wait=DURATION`</dt><dd>Time to wait after the batch. [default: `0ms`]</dd>
<dt>`--pipeline`</dt><dd>Keep `--operation-batch-size` operations in flight per worker thread and send the next one as soon as any completes, instead of waiting for the whole batch. One slow operation, such as a query, then holds one slot rather than the whole batch. When the client is bound by the SDK's I/O thread, every operation in the window waits in its queue, so a smaller window lowers tail latency. With `--document-body-fill=random` and no `--document-body-pool-size`, the next operation is generated on pillowfight's I/O threads rather than the SDK's. Conflicts with `--batch-wait`.</dd>
<dt>`--number-of-io-threads=INTEGER`</dt><dd>Number of I/O threads for pillowfight's own timers and deferred sends, at least 1. The SDK runs its own I/O thread. [default: `1`]</dd>
<dt>`--number-of-worker-threads=INTEGER`</dt><dd>Number of the worker threads, at least 1. [default: `1`]</dd>
<dt>`--operation-ratio=TEXT`</dt><dd>Relative weights of the operations to generate, as "G:R:D:I:Q:U": Get, Replace, Delete, Insert, Query and Upsert. See OPERATION RATIO. [default: `1:1:0:0:0:0`]</dd>
<dt>`--query-statement=STRING`</dt><dd>The N1QL query statement to use (`{bucket_name}`, `{scope_name}` and `{collection_name}` will be substituted). [default: <code>SELECT COUNT(*) FROM \`{bucket_name}\` WHERE type = "fake_profile"</code>]</dd>
<dt>`--document-body-size=INTEGER`</dt><dd>Size of the body (if zero, it will use predefined document). [default: `0`]</dd>
<dt>`--document-body-fill=MODE`</dt><dd>How to fill a generated document body (allowed values: `constant`, `random`). `constant` repeats a single character; `random` draws every document from a seeded pseudo-random generator. Only `random` gives each document distinct bytes. [default: `constant`]</dd>
<dt>`--document-body-format=FORMAT`</dt><dd>Format of a generated document (allowed values: `json`, `binary`). `json` wraps the body in a JSON object and restricts it to a JSON-safe alphabet; `binary` stores the body as opaque bytes of exactly `--document-body-size`. [default: `json`]</dd>
<dt>`--document-body-pool-size=INTEGER`</dt><dd>Pre-generate this many bytes of document bodies per worker thread and cycle through them, instead of generating a fresh body for every document. Measured in bytes, with a floor of 1 MiB. [default: `0`]</dd>
<dt>`--document-body-seed=INTEGER`</dt><dd>Seed for `--document-body-fill=random`, so that a dataset can be reproduced. Each worker thread offsets it by its index. Without it the generator is seeded from the system entropy source.</dd>
<dt>`--number-of-keys-to-populate=INTEGER`</dt><dd>Preload this many keys per worker thread before running the workload. Gets, replaces, deletes and upserts pick from the keys known to the worker; inserts add new keys. Conflicts with `--number-of-items`. [default: `1000`]</dd>
<dt>`--number-of-items=INTEGER`</dt><dd>Use a fixed keyset, named as the C pillowfight names its documents. Enables keyset mode, described under KEYSET MODE below. Population retries a failed key under the same name until it is stored or a stop is requested, so a permanent error needs Ctrl-C. Conflicts with `--number-of-keys-to-populate`.</dd>
<dt>`--key-prefix=STRING`</dt><dd>Prefix for every key of the keyset, at most 230 bytes in the default collection and 226 in any other; empty when not given. Requires `--number-of-items`.</dd>
<dt>`--start-at=INTEGER`</dt><dd>First index of the keyset. Requires `--number-of-items`. [default: `0`]</dd>
<dt>`--no-population`</dt><dd>Skip populating the keyset; every key in the range must already exist. Requires `--number-of-items`; conflicts with `--populate-only`.</dd>
<dt>`--populate-only`</dt><dd>Populate the keyset, then exit. A failed key is retried until it is stored or a stop is requested, so a permanent error needs Ctrl-C. A stop that leaves the keyset incomplete exits with a non-zero status. Requires `--number-of-items`; conflicts with `--no-population`.</dd>
<dt>`--rate-limit=FLOAT`</dt><dd>Ceiling on total operations per second across all worker threads. Conflicts with `--no-throttle` and `--batch-wait`. See THROTTLING below.</dd>
<dt>`--no-throttle`</dt><dd>Disable throttling and overload detection. On a busy server cluster the workload then generates more temporary failures and timeouts.</dd>
<dt>`--operations-limit=INTEGER`</dt><dd>Stop and exit after the number of the operations reaches this limit. (zero for running indefinitely). With `--pipeline`, a send that fails gives its place back and is replaced only while other operations are still completing, so a run whose last sends fail ends below the limit. [default: `0`]</dd>
</dl>


### LOADING INCOMPRESSIBLE DOCUMENTS

Sizing a dataset to a target disk footprint needs documents that do not compress, so that the
bytes written follow from the number of documents. Two compressors stand in the way, and they
work at different scopes.

Per document, the SDK and the Key/Value service compress with snappy, keeping the compressed form
only when it is smaller by a configured margin — `--compression-minimum-ratio` here,
`min_compression_ratio` on the server. Random bytes never clear that bar.

Per data block, the storage engine compresses several documents together, with `lz4` by default.
This is the one that matters: a body reused across documents is stored once and back-referenced
for the rest of the block, so a load of a million identical documents occupies a fraction of
their combined size no matter how random that single body looks. `--document-body-fill=random`
gives every document its own bytes.

Measured over the generator's own output, compressed in 4096-byte blocks:

| `--document-body-fill` / `--document-body-format` | lz4 | snappy | zstd |
|---|---|---|---|
| `constant` / `json` | 67.79x | 18.29x | 80.72x |
| `random` / `json` | 1.00x | 1.00x | 1.33x |
| `random` / `binary` | 1.00x | 1.00x | 1.00x |

A JSON body has to stay inside a JSON string, so it is drawn from a 62-character alphabet. That is
invisible to `lz4` and `snappy`, neither of which entropy-codes, but not to `zstd`, which
`magma_data_compression_algo` can be set to. Binary bodies are drawn from the full byte range and
hold at 1.00x whichever algorithm is configured.

To load 100 million documents of exactly 1024 bytes:

```
cbc pillowfight --bucket-name default \
    --document-body-size 1024 \
    --document-body-fill random \
    --document-body-format binary \
    --number-of-keys-to-populate 100000000 \
    --operations-limit 1
```

`--operations-limit 1` ends the workload phase at the first completed batch, so the run is the
load and little else. `--document-body-format=binary` makes `--document-body-size` the exact size
of the stored document; under `json` the body is wrapped in an object and the document is larger
than the value given. Binary documents are opaque to the Query service, so `--operation-ratio`
should not ask for queries alongside it.

`--document-body-pool-size` bounds the work spent per document by pre-generating bodies and
cycling them. It is rarely worth setting: generating a fresh body costs a fraction of a
microsecond, far less than the operation carrying it. When it is set, it is measured in **bytes**,
not documents, because what has to be defeated is the compressor's look-back distance — a distance
in bytes, which a document count does not describe. A pool smaller than that distance puts the
same body into one block twice and the documents compress again, silently and by a large factor.
That is why the option refuses a pool below 1 MiB rather than warning about it.

### OPERATION RATIO

Each worker thread draws every operation at random, in proportion to its weight in `--operation-ratio`. The weights are whole numbers, separated by colons, in the order Get, Replace, Delete, Insert, Query, Upsert.

| `--operation-ratio` | Workload |
|---|---|
| `1:1:0:0:0:0` | as many gets as replaces (the default) |
| `9:1:0:0:0` | nine gets per replace; a ratio without the sixth term has no upserts |
| `0:0:0:0:0:1` | upserts only |

- Outside keyset mode, gets, replaces, deletes and upserts pick a key the worker already knows: one it populated or inserted. Inserts write a new key.
- An upsert creates the document when it is missing; a replace fails with `document_not_found`. On a full-eviction bucket, a replace of a document that is not in memory may make the server read it from disk first. An upsert does not need the old document, though the server may still read its metadata from disk when documents in the bucket carry extended attributes.
- A deleted key stays in the worker's list. Gets, replaces and deletes of it fail with `document_not_found` until an upsert writes it again.
- A worker that knows no key, because `--number-of-keys-to-populate` is 0, sends each get, replace, delete and upsert to a new key until it has inserted one. An inserted key becomes known in batch mode to the first batch that starts after the insert, and with `--pipeline` once the insert succeeds. The gets, replaces and deletes fail with `document_not_found`, and each upsert writes a document that no later operation picks.
- A term is read as a whole number up to its first non-digit, without a message: `1x` reads as 1. An empty term, or one such as `x` or `-1` that does not start with a digit, keeps its default: `9:x` runs `9:1:0:0:0:0`. A ratio whose weights all come out 0, such as `0:0:-1:0:0`, is an error.
- In keyset mode, deletes and inserts are rejected; see KEYSET MODE.

### KEYSET MODE

`--number-of-items` switches the workload to a fixed keyset that can be reused across runs, including a dataset loaded by the C pillowfight (`cbc-pillowfight`).

- Keys are `--key-prefix` followed by the index as 20 zero-padded digits, for every index in `[--start-at, --start-at + --number-of-items)`. This is the C pillowfight's format: `--key-prefix user_ --number-of-items 1000` names `user_00000000000000000000` to `user_00000000000000000999`.
- `--start-at + --number-of-items` must not exceed 2147483648. The C pillowfight prints larger indices as negative numbers, so they would name different documents.
- Population splits the range into contiguous slices, one per worker thread, and upserts the slices in turn on the main thread. A failed key is retried under the same name until it is stored or a stop is requested; a permanent error, such as a missing collection, needs Ctrl-C. `--no-population` skips population; `--populate-only` exits after it.
- Gets, replaces and upserts pick a key uniformly from the whole range. Deletes and inserts would add or remove keys, so a non-zero delete or insert weight in `--operation-ratio` is rejected. `--query-statement` runs as given, so a statement that inserts or deletes documents changes the keyset.

A dataset loaded by the C pillowfight may have gaps. Its sequential generator skips keys when `--num-items` is not divisible by `--num-threads`. It can also skip keys when `--start-at` is not zero, even with one thread. A get or replace of a missing key reports `document_not_found`. Loading with `--start-at 0` and a thread count that divides `--num-items` avoids the gaps.

To load a keyset with the C pillowfight and run the C++ workload over it:

```
cbc-pillowfight -U couchbase://127.0.0.1/default -u Administrator -P password -p user_ -I 100000 -t 1 --populate-only
cbc pillowfight --key-prefix user_ --number-of-items 100000 --no-population
```

### THROTTLING

By default the workload is throttled: a token bucket admits operations, and the rate adapts to signs of overload from the cluster.

| Options | Header line | Behaviour |
|---|---|---|
| none | `Throttle: adaptive (no ceiling)` | Unlimited until an overload; after a cut the rate grows back, then returns to unlimited. |
| `--rate-limit=R` | `Throttle: adaptive, ceiling R ops/s` | R operations per second across all worker threads on average, with bursts of up to `max(--operation-batch-size, 10)` operations; after a cut the rate grows back to R. |
| `--no-throttle` | `Throttle: off` | No bucket and no overload detection. |

Population is throttled as well. With `--rate-limit`, the ceiling applies to population and to the workload. The workload starts from the initial rate, so a cut during population does not carry over.

**Back-off.** Every second the completions since the last judgement are examined, once at least 100 have accumulated. The window counts as overloaded when temporary-failure retries amount to more than 5% of its completions, or when more than 20% of its completions took over 500 ms. One operation retried several times counts every retry. Queries do not count. Over `couchbase2://` no retry is counted, so only slow completions signal overload. An overloaded window halves the admitted rate. An unlimited rate is halved from the highest rate admitted in any second of the judged window, so a stalled second does not lower it. The rate never drops below the floor. The floor is `--operation-batch-size` operations per second, or the ceiling if that is lower. A cut at the floor is not a cut: it prints no line and starts no hold-off. The window after a cut cannot cut again.

**Recovery.** The rate before the first cut of an overload episode is the recovery target. Later cuts in the same episode keep it, and recovery clears it.

- Each healthy window doubles the rate, up to the target.
- With a ceiling, recovery ends when the rate reaches the ceiling.
- Without one, recovery ends when the rate reaches the target. It ends earlier once the bucket stops binding: less than 90% of the rate was admitted in the window. The rate then becomes unlimited again.
- When the highest admitted rate of the judged window is at or below the floor, the target is unknown, and the rate doubles until the bucket stops binding.

**Output.** The status line shows the last second's rate, the average over up to three minutes, and the admitted rate the throttle allows, or `unlimited`. `(backing off)` follows it while a cut is in effect. `--no-throttle` omits the `admitted` field.

```
uptime: 14s, rate: 19629 ops/s (avg 20474), admitted: unlimited, 0.00 err/s, total: 307112
uptime: 29s, rate: 1500 ops/s (avg 10723), admitted: 3200 ops/s (backing off), 0.00 err/s, total: 321700
```

Each cut and each recovery prints one line that stays in the scrollback. Its uptime counts from before the connection to the cluster, so it runs ahead of the status line's by the connection and population time. A cut line gives the retry and slow shares of the judged window, then the admitted rate before and after the cut. A cut from an unlimited rate prints `unlimited -> N`, where N is half the highest rate admitted in the judged window, or the floor when that is higher:

```
[20s] overload: 0.0% temporary-failure retries, 85.0% slow; admitted rate unlimited -> 100 ops/s
[34s] recovered: admitted rate unlimited
```

When stderr is not a terminal, every status line is printed as a new line, without the escape sequences that rewrite the current one. Captured logs stay readable.

The summary at exit, `--populate-only` included, adds:

```
Per-second rate: min 1, median 21554, max 25737 ops/s
Backing off: 14s, cuts: 1
Temporary-failure retries: 0
```

The per-second rate covers every second of the run, population included; a run shorter than one second prints `n/a`. `--no-throttle` prints only the per-second rate.

### EXAMPLES

The connection options are left out of these commands: add `--connection-string`, `--username` and `--password` for a cluster other than the defaults.

**Cap the workload at 20000 operations per second across all worker threads.**

```
cbc pillowfight --number-of-worker-threads 4 --rate-limit 20000
```

The rate still drops when the cluster reports overload, and grows back to 20000 afterwards. See THROTTLING.

**Find the most the cluster takes, accepting retries and timeouts.**

```
cbc pillowfight --number-of-worker-threads 8 --no-throttle
```

No token bucket admits operations and no overload cuts the rate. The error rate and the latency then show where the cluster saturates, unless the client saturates first. The SDK still retries temporary-failure and locked responses. A busy, out-of-memory or not-initialized response fails at once with a temporary-failure error and is not retried.

**Run one million operations, then exit.**

```
cbc pillowfight --operations-limit 1000000
```

In batch mode the limit is checked after each batch, so the run can end up to one batch per worker thread past it. With `--pipeline` the workers never issue more than the limit.

**Keep a slow query from stalling the key-value operations sent with it.**

```
cbc pillowfight --pipeline --operation-ratio 10:10:0:0:1
```

In batch mode the next batch starts only after the slowest operation of the current one, the query included. With `--pipeline` the query holds one slot of the window and the other slots keep cycling. The default `--query-statement` needs an index on the bucket.

**Overwrite documents with upserts.**

```
cbc pillowfight --operation-ratio 0:0:0:0:0:1
```

Upserts only; see OPERATION RATIO for how an upsert differs from a replace.

**Measure a read-heavy workload with 4 KiB documents.**

```
cbc pillowfight --operation-ratio 9:1:0:0:0 --document-body-size 4096 --document-body-format binary
```

Nine gets for every replace. `binary` stores exactly 4096 bytes per document; `json` would wrap the body and store more.

**Watch how the workload responds to a rebalance, and keep the record.**

```
cbc pillowfight --number-of-worker-threads 4 > pillowfight.log 2>&1
```

With output to a file, every status line is a new line. Each cut and each recovery prints its own line, so the log shows when the admitted rate fell and when it came back. The exit summary follows at the end.

**Load a dataset of one million documents once, and reuse it in later runs.**

```
cbc pillowfight --key-prefix user_ --number-of-items 1000000 --populate-only
```

A non-zero exit status means a stop left the dataset incomplete. Running the same command again stores every key of the range.

**Run a read-only workload against that dataset without loading it again.**

```
cbc pillowfight --key-prefix user_ --number-of-items 1000000 --no-population --operation-ratio 1:0:0:0:0
```

**Split one dataset between two client machines.**

```
cbc pillowfight --key-prefix user_ --start-at 0 --number-of-items 500000 --no-population
cbc pillowfight --key-prefix user_ --start-at 500000 --number-of-items 500000 --no-population
```

Each machine reads and replaces only its own half of the keys.

**Load a large dataset without starving other traffic on the cluster.**

```
cbc pillowfight --key-prefix user_ --number-of-items 10000000 --populate-only --rate-limit 5000
```

Population goes through the throttle, so it stores 5000 keys per second on average, with bursts of up to one population batch, and an overload still cuts the rate.

**Concentrate the workload on a small hot set inside a larger dataset.**

```
cbc pillowfight --key-prefix user_ --start-at 0 --number-of-items 10000 --no-population
```

Gets and replaces pick only from the first 10000 keys of the dataset.

**Run against a dataset that the C pillowfight loaded.** See the example at the end of KEYSET MODE.

### LOGGER OPTIONS

<dl>
<dt>`--log-level=LEVEL`</dt><dd>Log level (allowed values are: `trace`, `debug`, `info`, `warning`, `error`, `critical`, `off`). [default: `off`]</dd>
<dt>`--log-output=PATH`</dt><dd>File to send logs (when is not set, logs will be written to STDERR).</dd>
<dt>`--log-protocol=PATH`</dt><dd>File to send protocol logs.</dd>
</dl>

### CONNECTION OPTIONS

<dl>
<dt>`--connection-string=STRING`</dt><dd>Connection string for the cluster. [default: `couchbase://localhost`]</dd>
<dt>`--username=STRING`</dt><dd>Username for the cluster. [default: `Administrator`]</dd>
<dt>`--password=STRING`</dt><dd>Password for the cluster. [default: `password`]</dd>
<dt>`--certificate-path=STRING`</dt><dd>Path to the certificate.</dd>
<dt>`--key-path=STRING`</dt><dd>Path to the key.</dd>
<dt>`--ldap-compatible`</dt><dd>Whether to select authentication mechanism that is compatible with LDAP.</dd>
<dt>`--configuration-profile=STRING`</dt><dd>Apply configuration profile (might override other switches). (available profiles: `wan_development`)</dd>
</dl>

### SECURITY OPTIONS

<dl>
<dt>`--disable-tls`</dt><dd>Whether to disable TLS.</dd>
<dt>`--trust-certificate-path=STRING`</dt><dd>Path to the trust certificate bundle.</dd>
<dt>`--tls-verify-mode=MODE`</dt><dd>Path to the certificate (allowed values: peer, none). [default: `peer`]</dd>
</dl>

### TIMEOUT OPTIONS

<dl>
<dt>`--bootstrap-timeout=DURATION`</dt><dd>Timeout for overall bootstrap of the SDK. [default: `10000ms`]</dd>
<dt>`--connect-timeout=DURATION`</dt><dd>Timeout for socket connection. [default: `10000ms`]</dd>
<dt>`--resolve-timeout=DURATION`</dt><dd>Timeout to resolve DNS address for the sockets. [default: `2000ms`]</dd>
<dt>`--key-value-timeout=DURATION`</dt><dd>Timeout for Key/Value operations. [default: `2500ms`]</dd>
<dt>`--key-value-durable-timeout=DURATION`</dt><dd>Timeout for Key/Value durable operations. [default: `10000ms`]</dd>
<dt>`--query-timeout=DURATION`</dt><dd>Timeout for Query service. [default: `75000ms`]</dd>
<dt>`--search-timeout=DURATION`</dt><dd>Timeout for Search service. [default: `75000ms`]</dd>
<dt>`--eventing-timeout=DURATION`</dt><dd>Timeout for Eventing service. [default: `75000ms`]</dd>
<dt>`--analytics-timeout=DURATION`</dt><dd>Timeout for Analytics service. [default: `75000ms`]</dd>
<dt>`--view-timeout=DURATION`</dt><dd>Timeout for View service. [default: `75000ms`]</dd>
<dt>`--management-timeout=DURATION`</dt><dd>Timeout for management operations. [default: `75000ms`]</dd>
</dl>

### COMPRESSION OPTIONS

<dl>
<dt>`--disable-compression`</dt><dd>Whether to disable compression.</dd>
<dt>`--compression-minimum-size=INTEGER`</dt><dd>The minimum size of the document (in bytes), that will be compressed. [default: `32`]</dd>
<dt>`--compression-minimum-ratio=FLOAT`</dt><dd>The minimum compression ratio to allow compressed form to be used. [default: `0.83`]</dd>
</dl>

### DNS-SRV OPTIONS

<dl>
<dt>`--dns-srv-timeout=DURATION`</dt><dd>Timeout for DNS SRV requests. [default: `500ms`]</dd>
<dt>`--dns-srv-nameserver=STRING`</dt><dd>Hostname of the DNS server where the DNS SRV requests will be sent.</dd>
<dt>`--dns-srv-port=INTEGER`</dt><dd>Port of the DNS server where the DNS SRV requests will be sent.</dd>
</dl>

### NETWORK OPTIONS

<dl>
<dt>`--tcp-keep-alive-interval=DURATION`</dt><dd>Interval for TCP keep alive. [default: `60000ms`]</dd>
<dt>`--config-poll-interval=DURATION`</dt><dd>How often the library should poll for new configuration. [default: `2500ms`]</dd>
<dt>`--idle-http-connection-timeout=DURATION`</dt><dd>Period to wait before calling HTTP connection idle. [default: `4500ms`]</dd>
</dl>

### TRANSACTIONS OPTIONS

<dl>
<dt>`--transactions-durability-level=LEVEL`</dt><dd>Durability level of the transaction (allowed values: `none`, `majority`, `majority_and_persist_to_active`, `persist_to_majority`). [default: `majority`]</dd>
<dt>`--transactions-timeout=DURATION`</dt><dd>Timeout of the transaction. [default: `15000ms`]</dd>
<dt>`--transactions-metadata-bucket=STRING`</dt><dd>Bucket name where transaction metadata is stored.</dd>
<dt>`--transactions-metadata-scope=STRING`</dt><dd>Scope name where transaction metadata is stored. [default: `_default`]</dd>
<dt>`--transactions-metadata-collection=STRING`</dt><dd>Collection name where transaction metadata is stored. [default: `_default`]</dd>
<dt>`--transactions-query-scan-consistency=MODE`</dt><dd>Scan consistency for queries in transactions (allowed values: `not_bounded`, `request_plus`). [default: `request_plus`]</dd>
<dt>`--transactions-cleanup-ignore-lost-attempts`</dt><dd>Do not cleanup lost attempts.</dd>
<dt>`--transactions-cleanup-ignore-client-attempts`</dt><dd>Do not cleanup client attempts.</dd>
<dt>`--transactions-cleanup-window=DURATION`</dt><dd>Cleanup window. [default: `60000ms`]</dd>
</dl>

### METRICS OPTIONS

<dl>
<dt>`--disable-metrics`</dt><dd>Disable collecting and reporting metrics.</dd>
<dt>`--metrics-emit-interval=DURATION`</dt><dd>Interval to emit metrics report on INFO log level. [default: `600000ms`]</dd>
</dl>

### TRACING OPTIONS

<dl>
<dt>`--disable-tracing`</dt><dd>Disable collecting and reporting trace information.</dd>
<dt>`--tracing-orphaned-emit-interval=DURATION`</dt><dd>Interval to emit report about orphan operations. [default: `10000ms`]</dd>
<dt>`--tracing-orphaned-sample-size=INTEGER`</dt><dd>Size of the sample of the orphan report. [default: `64`]</dd>
<dt>`--tracing-threshold-emit-interval=DURATION`</dt><dd>Interval to emit report about operations exceeding threshold. [default: `10000ms`]</dd>
<dt>`--tracing-threshold-sample-size=INTEGER`</dt><dd>Size of the sample of the threshold report. [default: `64`]</dd>
<dt>`--tracing-threshold-key-value=DURATION`</dt><dd>Threshold for Key/Value service. [default: `500ms`]</dd>
<dt>`--tracing-threshold-query=DURATION`</dt><dd>Threshold for Query service. [default: `1000ms`]</dd>
<dt>`--tracing-threshold-search=DURATION`</dt><dd>Threshold for Search service. [default: `1000ms`]</dd>
<dt>`--tracing-threshold-analytics=DURATION`</dt><dd>Threshold for Analytics service. [default: `1000ms`]</dd>
<dt>`--tracing-threshold-management=DURATION`</dt><dd>Threshold for Management operations. [default: `1000ms`]</dd>
<dt>`--tracing-threshold-eventing=DURATION`</dt><dd>Threshold for Eventing service. [default: `1000ms`]</dd>
<dt>`--tracing-threshold-view=DURATION`</dt><dd>Threshold for View service. [default: `1000ms`]</dd>
</dl>

### BEHAVIOR OPTIONS

<dl>
<dt>`--user-agent-extra=STRING`</dt><dd>Append extra string SDK identifiers. [default: `cbc`].</dd>
<dt>`--network=STRING`</dt><dd>Network (a.k.a. Alternate Addresses) to use. [default: `auto`]</dd>
<dt>`--show-queries`</dt><dd>Log queries on INFO level.</dd>
<dt>`--enable-clustermap-notifications`</dt><dd>Allow server to send notifications when cluster configuration changes.</dd>
<dt>`--disable-mutation-tokens`</dt><dd>Do not request Key/Value service to send mutation tokens.</dd>
<dt>`--disable-unordered-execution`</dt><dd>Disable unordered execution for Key/Value service.</dd>
</dl>

### ENVIRONMENT

<dl>
<dt>CBC_LOG_LEVEL</dt><dd>Overrides default value for `--log-level`.</dd>
<dt>CBC_CONNECTION_STRING</dt><dd>Overrides default value for `--connection-string`.</dd>
<dt>CBC_USERNAME</dt><dd>Overrides default value for `--username`.</dd>
<dt>CBC_PASSWORD</dt><dd>Overrides default value for `--password`.</dd>
</dl>

### SEE ALSO

[cbc](#cbc).
