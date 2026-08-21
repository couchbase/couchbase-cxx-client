# Metrics metadata

`metrics_metadata.json` describes the application telemetry metrics this client
reports, in the schema Couchbase Server components use for the same purpose.
ns_server reads such a file to attach `# TYPE` and `# HELP` to the metrics it
publishes on `/metrics`; with no file covering the `sdk_` namespace, those
metrics are published untyped and undocumented.

Metric names, their types and the histogram bucket bounds are fixed by
[SDK-RFC 84](https://github.com/couchbaselabs/sdk-rfcs/blob/f690b9c46bf562465c10fc5ff5e02d9ec29876ce/rfc/0084-app-service-level-telemetry.md),
so this file describes the specification rather than this implementation, and is
the same for every SDK implementing it.

Tracked by [CXXCBC-1001](https://jira.issues.couchbase.com/browse/CXXCBC-1001);
the server side is [MB-68858](https://jira.issues.couchbase.com/browse/MB-68858).

## Installing

Server components install their file with the `AddMetricsMetadata` macro from
`tlm`, which validates the JSON, sorts its keys and copies it to
`$install/etc/couchbase/<component>/metrics_metadata.json`:

    AddMetricsMetadata (JSON etc/metrics_metadata.json COMPONENT sdk)

This client is not part of the server build, so nothing invokes that macro here.
Which component name the file is installed under, and which repository the
server build takes it from, are ns_server's decisions and are tracked on
MB-68858.

## Keeping it in step with the emitter

The key set must equal the metric families `core/app_telemetry_meter.cxx` can
emit:

    diff <(python3 -c "import json;print('\n'.join(sorted(json.load(open('etc/metrics_metadata.json')))))") \
         <(grep -oE 'sdk_[a-z_0-9]+' core/app_telemetry_meter.cxx | LC_ALL=C sort -u)

Keys are metric families. A histogram appears once, as
`sdk_kv_retrieval_duration_milliseconds`, and not under the `_bucket`, `_sum`
and `_count` series it produces: Prometheus declares `TYPE` and `HELP` once per
family and the suffixed series inherit them.

`sdk_invalid_metric_total` is absent because ns_server emits it, not the SDK —
it counts telemetry lines ns_server failed to parse — and describes it in its
own metadata file.

## Labels

`labels` lists what `/metrics` carries, which is narrower than what the SDK
sends. `app_telemetry_scraper` keeps `le` and `alt_node` and discards `agent`,
`node`, `node_uuid` and `bucket`, so metrics that RFC 84 labels per node and
per bucket reach Prometheus aggregated across both.
