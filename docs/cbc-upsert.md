# cbc-upsert - Store Documents on the Server {#cbc-upsert}

### NAME

`cbc upsert` - store document on the server

### SYNOPSIS

`cbc upsert [options] <id>...`<br/>
`cbc upsert [options] --inlined-value-separator=/ <id>/<value>...`<br/>
`cbc upsert (-h|--help)`

### DESCRIPTION

Store one or more documents on the server.

By default the document content read from the local filesystem, where `<id>` is
the file path to the document, but the value could be embedded with the `<id>`
and `--inlined-value-separator` switch.

### OPTIONS

<dl>
<dt>`-h,--help`</dt><dd>Print this help message and exit</dd>
<dt>`--verbose`</dt><dd>Include more context and information where it is applicable.</dd>
<dt>`--bucket-name TEXT`</dt><dd>Name of the bucket. [default: `default`]</dd>
<dt>`--scope-name TEXT`</dt><dd>Name of the scope. [default `_default`]</dd>
<dt>`--collection-name TEXT`</dt><dd>Name of the collection. [default: `_default`]</dd>
<dt>`--inlined-value-separator TEXT`</dt><dd>Specify value with the key instead of filesystem.</dd>
<dt>`--inlined-keyspace`</dt><dd>Extract bucket, scope, collection and key from the IDs (captures will be done with `/^(.*?):(.*?)\.(.*?):(.*)$/`).</dd>
<dt>`--json-lines`</dt><dd>Use JSON Lines format (https://jsonlines.org) to print results.</dd>
<dt>`--preserve-expiry`</dt><dd>Whether an existing document's expiry should be preserved. [default: `0`]</dd>
<dt>`--override-document-flags UINT`</dt><dd>Override document flags instead of derived from the content.</dd>
</dl>

### EXPIRATION

Set expiration time for the document(s). Either `--expire-relative` or `--expire-absolute` switch is allowed.

<dl>
<dt>`--expire-relative INT`</dt><dd>Expiration time in seconds from now</dd>
<dt>`--expire-absolute TEXT`</dt><dd>Absolute expiration time (format: **YYYY-MM-DDTHH:MM:SS**, e.g. the output of: <code>date --utc --iso-8601=seconds --date 'next month'</code>)</dd>
</dl>

### DURABILITY

Extra persistency requirements. Either `--durability-level` or combination of `--persist-to` and `--replicate-to` switches is allowed.

<dl>
<dt>`--durability-level TEXT`</dt><dd>Durability level for the server. (allowed values: `none`, `majority`, `majority_and_persist_to_active`, `persist_to_majority`)</dt>
</dl>

The following switches implement client-side poll-based durability requirements.

<dl>
<dt>`--persist-to TEXT`</dt><dd>Number of the nodes that have to have the document persisted. (allowed values: `none`, `active`, `one`, `two`, `three`, `four`)</dt>
<dt>`--replicate-to TEXT`</dt><dd>Number of the nodes that have to have the document replicated. (allowed values: `none`, `one`, `two`, `three`)</dt>
</dl>

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
<dt>`CBC_LOG_LEVEL`</dt><dd>Overrides default value for `--log-level`.</dd>
<dt>`CBC_CONNECTION_STRING`</dt><dd>Overrides default value for `--connection-string`.</dd>
<dt>`CBC_USERNAME`</dt><dd>Overrides default value for `--username`.</dd>
<dt>`CBC_PASSWORD`</dt><dd>Overrides default value for `--password`.</dd>
</dl>

### EXAMPLES

1. Create document the default collection of the `default` bucket with the key `myfile.json`, and contents of this file on the local system:

       cbc upsert myfile.json
2. Create document the default collection of the `default` bucket with the key `foo`, and content `{"bar":42}`:

       cbc upsert --inlined-value-separator=/ foo/{"bar":42}
3. Create document in the scope `myapp`, collection `users` of the bucket `accounts`:

       cbc upsert --bucket-name=accounts --scope-name=myapp --collection=users --inlined-value-separator=/ user_1/{"name":"john"}
4. All above, but inline bucket, scope, collection and value into the `<id>` to store two documents:

       cbc upsert --inlined-keyspace --inlined-value-separator=/ \
            accounts:myapp:users:user_1/{"name":"john"} \
            accounts:myapp:users:user_2/{"name":"jane"}
5. Create document with expiration time of 5 seconds:

       cbc upsert --expire-relative=5 my_file
6. Create document with abosolute expiration time in a year from now:

       TIMESTAMP=$(date --utc --iso-8601=seconds --date 'next year')
       # 2026-06-24T21:14:51+00:00

       cbc upsert --expire-absolute=$TIMESTAMP my_file
7. Persist document to majority of the replica set:

       cbc upsert --durability-level=majority --inlined-value-separator=/ foo/{"bar":42}
8. Wait until document will be persisted on at least one node from the replica set, but replicated to two nodes (and give it maximum time of 10 seconds to wait):

       cbc upsert --key-value-timeout=10s --persist-to=one --replicate-to=two --inlined-value-separator=/ foo/{"bar":42}

### SEE ALSO

[cbc](#cbc), [cbc-get](#cbc-get), [cbc-remove](#cbc-remove).
