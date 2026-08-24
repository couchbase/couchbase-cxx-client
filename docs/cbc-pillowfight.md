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
<dt>`--operation-batch-size=INTEGER`</dt><dd>Number of the operations in a single batch (set to 1 to wait for completion after every operation). [default: `100`]</dd>
<dt>`--batch-wait=DURATION`</dt><dd>Time to wait after the batch. [default: `0ms`]</dd>
<dt>`--number-of-io-threads=INTEGER`</dt><dd>Number of the IO threads. [default: `1`]</dd>
<dt>`--number-of-worker-threads=INTEGER`</dt><dd>Number of the IO threads. [default: `1`]</dd>
<dt>`--operation-ratio=TEXT`</dt><dd>The ratio of the operations to generate in form "G:R:D:I:Q", where letters represent ratio of the operations in whole numbers: Get, Replace, Delete, Insert and Query respectively. (e.g. "5:0:0:1:0" would do on average 5 gets for every insert). [default: `1:1:0:0:0`]</dd>
<dt>`--query-statement=STRING`</dt><dd>The N1QL query statement to use (`{bucket_name}`, `{scope_name}` and `{collection_name}` will be substituted). [default: <code>SELECT COUNT(*) FROM \`{bucket_name}\` WHERE type = "fake_profile"</code>]</dd>
<dt>`--document-body-size=INTEGER`</dt><dd>Size of the body (if zero, it will use predefined document). [default: `0`]</dd>
<dt>`--document-body-fill=MODE`</dt><dd>How to fill a generated document body (allowed values: `constant`, `random`). `constant` repeats a single character; `random` draws every document from a seeded pseudo-random generator. Only `random` gives each document distinct bytes. [default: `constant`]</dd>
<dt>`--document-body-format=FORMAT`</dt><dd>Format of a generated document (allowed values: `json`, `binary`). `json` wraps the body in a JSON object and restricts it to a JSON-safe alphabet; `binary` stores the body as opaque bytes of exactly `--document-body-size`. [default: `json`]</dd>
<dt>`--document-body-pool-size=INTEGER`</dt><dd>Pre-generate this many bytes of document bodies per worker thread and cycle through them, instead of generating a fresh body for every document. Measured in bytes, with a floor of 1 MiB. [default: `0`]</dd>
<dt>`--document-body-seed=INTEGER`</dt><dd>Seed for `--document-body-fill=random`, so that a dataset can be reproduced. Each worker thread offsets it by its index. Without it the generator is seeded from the system entropy source.</dd>
<dt>`--number-of-keys-to-populate=INTEGER`</dt><dd>Preload keys before running workload, so that the worker will not generate new keys afterwards. [default: `1000`]</dd>
<dt>`--operations-limit=INTEGER`</dt><dd>Stop and exit after the number of the operations reaches this limit. (zero for running indefinitely) [default: `0`]</dd>
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
