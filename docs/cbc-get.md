# cbc-get - Retrieve Documents from the Server {#cbc-get}

### NAME

`cbc get` - retrieve document from the server

### SYNOPSIS

`cbc get [options] <id>...`<br/>
`cbc get [options] --project=STRING... <id>...`<br/>
`cbc get (-h|--help)`

### DESCRIPTION

Retrieve one or more documents from the server and print them to standard output.

### OPTIONS

<dl>
<dt>`-h|--help`</dt><dd>Show help message.</dd>
<dt>`--verbose`</dt><dd>Include more context and information where it is applicable.</dd>
<dt>`--bucket-name=STRING`</dt><dd>Name of the bucket. [default: `default`]</dd>
<dt>`--scope-name=STRING`</dt><dd>Name of the scope. [default: `_default`]</dd>
<dt>`--collection-name=STRING`</dt><dd>Name of the collection. [default: `_default`]</dd>
<dt>`--inlined-keyspace`</dt><dd>Extract bucket, scope, collection and key from the IDs (captures will be done with `/^(.*?):(.*?)\.(.*?):(.*)$/`).</dd>
<dt>`--with-expiry`</dt><dd>Return document expiry time, if set.</dd>
<dt>`--project=STRING`</dt><dd>Return only part of the document, that corresponds given JSON-pointer (could be used multiple times, up to 16).</dd>
<dt>`--hexdump`</dt><dd>Print value using hexdump encoding (safe for binary data on STDOUT).</dd>
<dt>`--pretty-json`</dt><dd>Try to pretty-print as JSON value (prints AS-IS is the document is not a JSON).</dd>
<dt>`--json-lines`</dt><dd>Use JSON Lines format (https://jsonlines.org) to print results.</dd>
</dl>


### LOGGER OPTIONS

<dl>
<dt>`--log-level=LEVEL`</dt><dd>Log level (allowed values are: `trace`, `debug`, `info`, `warning`, `error`, `critical`, `off`). [default: `off`]
<dt>`--log-output=PATH`</dt><dd>File to send logs (when is not set, logs will be written to STDERR).
</dl>

### CONNECTION OPTIONS

<dl>
<dt>`--connection-string=STRING`</dt><dd>Connection string for the cluster. [default: `couchbase://localhost`]</dd>
<dt>`--username=STRING`</dt><dd>Username for the cluster. [default: `Administrator`]</dd>
<dt>`--password=STRING`</dt><dd>Password for the cluster. [default: `password`]</dd>
<dt>`--certificate-path=STRING`</dt><dd>Path to the certificate.</dd>
<dt>`--key-path=STRING`</dt><dd>Path to the key.</dd>
<dt>`--ldap-compatible`</dt><dd>Whether to select authentication mechanism that is compatible with LDAP.</dd>
<dt>`--configuration-profile=STRING`</dt><dd>Apply configuration profile. (available profiles: `wan_development`)</dd>
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
<dt>`--transactions-expiration-time=DURATION`</dt><dd>Expiration time of the transaction. [default: `15000ms`]</dd>
<dt>`--transactions-key-value-timeout=DURATION`</dt><dd>Override Key/Value timeout just for the transaction.</dd>
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

[cbc](md_docs_2cbc), [cbc-query](md_docs_2cbc-query).
