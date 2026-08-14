# CNG (`couchbase2://`) tests

These tests exercise the Protostellar / Cloud Native Gateway (CNG) transport. They are built
with the hand-rolled harness under `test/framework/` (not Catch2) and are enabled by the
`COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS` CMake option.

Each case declares what it requires of the environment, beside its registration rather than inside
its body — `{ needs::real_cluster() }`, `{ needs::service("n1ql") }`, and so on. The runner checks
those before the case and decides:

- **satisfied** — the case runs.
- **not satisfied** — the case is skipped, and the run ends with a list of what was missing and how
  many cases each gap turned away. A case with no requirements needs no server: it is pure, or it
  stands up an in-process gRPC server of its own.
- **undetermined** — the case **fails**. A configured gateway that cannot be reached is not the same
  as no gateway configured, and only the second is a reason to skip.

`ctest` reports a binary whose every case was skipped as *Skipped* (the harness exits `77`).
`<binary> --list-tests` prints each case with what it requires.

Getting the cluster-only tests to run means standing up a Couchbase cluster whose Cloud Native
Gateway is deployed, and pointing the tests at its `couchbase2://` endpoint. The rest of this
document shows how to do that locally with `cbdinocluster` and `k3d`.

## Prerequisites

Linux with:

- **Docker** — the container runtime everything else builds on.
- **[k3d](https://k3d.io)** — runs a lightweight Kubernetes (k3s) cluster inside Docker. CNG is
  deployed by the Couchbase Autonomous Operator, which needs Kubernetes.
- **[cbdinocluster](https://github.com/couchbaselabs/cbdinocluster)** — allocates Couchbase
  clusters for testing. Its `cao` deployer installs the operator into your k3d cluster and, when
  the cluster definition asks for it, the Cloud Native Gateway.
- A C++ toolchain plus **gRPC and protobuf ≥ 3.15**, required by the couchbase2 transport. System
  packages (`grpc-devel` / `libgrpc++-dev` and `protobuf-devel` / `libprotobuf-dev`) are only a
  convenience: when `find_package(gRPC)` does not find them the build fetches gRPC and its protobuf
  submodule and builds them in-tree, which needs no packages but makes the first build considerably
  longer. Building with `-stdlib=libc++` always fetches from source, because distribution packages
  are compiled against libstdc++ and would fail to link.

## 1. One-time Kubernetes setup

Create a local Kubernetes cluster with k3d and register it with cbdinocluster. Do this once per
machine (k3d clusters do not survive a reboot — see the note at the end to restart one):

```console
$ k3d cluster create mycluster
$ cbdinocluster init --auto --kube-config ~/.kube/config
```

`cbdinocluster init` records the k3d context so the `cao` deployer can use it.

`--auto` takes every default and asks nothing, which is what CI does and what you want here.
Without it the command is an interactive questionnaire covering each provider it supports — Docker,
Kubernetes, AWS, Azure, GCP, Capella, DNS and GitHub — and only two answers matter for this
walkthrough: **Kubernetes must be enabled**, and the kubeconfig it uses must be the one `k3d cluster
create` wrote. Declining Kubernetes, or pointing it at a different kubeconfig, leaves the `cao`
deployer unusable and the failure does not surface until `allocate` several steps later. The cloud
providers are unused here and can all be declined.

There is no need to name a context. `cbdinocluster` accepts `--k8s-context`, but its handler reads
a differently named key and the value is discarded, so it always falls back to the kubeconfig's
current context — which `k3d cluster create` has just set to the new cluster.

Confirm the Kubernetes side is healthy — this should list `cao` among the deployers with no
namespace error:

```console
$ cbdinocluster ps
```

## 2. Define a cluster with the Cloud Native Gateway

The gateway is deployed by the operator and is requested **per cluster** in the definition file.
A definition without a `cao.gateway-version` yields a working cluster with **no** `couchbase2://`
endpoint. Write `cluster-def.yaml`:

```yaml
nodes:
  - count: 1
    version: 8.0.0
    services: [kv]
  - count: 1
    version: 8.0.0
    services: [kv, n1ql, index]
  - count: 1
    version: 8.0.0
    services: [kv, fts]
  - count: 1
    version: 8.0.0
    services: [kv, cbas]
cao:
  operator-version: "2.8.0"
  gateway-version: "1.2.1"   # presence of this line is what enables CNG
```

**Use release versions, not build numbers.** cbdinocluster maps a version carrying a build number
(`8.1.0-1743`, `1.2.2-123`) to `ghcr.io/cb-vanilla/*`, which requires a Couchbase-organization token;
a plain release version maps to the public `couchbase/*` images on Docker Hub and needs no
credentials. CI pins release versions for exactly this reason, so the definition above is what an
outside contributor can actually pull.

There is deliberately no `docker:` memory block here: the `cao` deployer ignores it, so every service
quota stays at the operator's 256 MiB default no matter what the definition says. Two parts of the
suite need more than that — see [Cluster quota](#cluster-quota-bucket-and-search-management) — and it
has to be set on the allocated cluster rather than here. The four nodes cover every service the suite
can touch, and `fts` has to be among them or the live search case fails rather than reporting a
missing index. CI puts all of `kv, n1ql, index, fts` on one node, which is the smallest topology the
suite runs against.

Other `cao` keys cbdinocluster understands include `username` / `password` (default
`Administrator` / `password`) and `gateway-log-level`.

## 3. Allocate the cluster and get the connection string

```console
$ CBDC_ID=$(cbdinocluster allocate --deployer cao --def-file cluster-def.yaml)
$ cbdinocluster buckets add "$CBDC_ID" default --ram-quota-mb 256
$ cbdinocluster connstr --couchbase2 "$CBDC_ID"
couchbase2://<host>:<port>
```

The last command prints the `couchbase2://` endpoint. The bucket created above is all the tests
need: every cluster-only case addresses documents in the `_default` scope and `_default` collection,
so no extra scope or collection has to be provisioned. Should you want one anyway, for instance to
try a KV operation against a non-default collection by hand:

```console
$ cbdinocluster collections add-scope "$CBDC_ID" default myscope
$ cbdinocluster collections add       "$CBDC_ID" default myscope mycollection
```

### Cluster quota: bucket and search management

The operator's default service quotas are **256 MiB each**, and that is not enough for two parts of
the suite. Nothing in the definition file can change it: the `cao` deployer builds the
`CouchbaseCluster` spec itself and emits no `cluster:` block, and the `cao:` section of a cluster
definition has no quota fields — so the `docker:` memory keys, which do work for the docker deployer,
are silently ignored here. Raise the quotas on the allocated cluster:

```console
$ kubectl -n cbdc2-"$CBDC_ID" patch couchbasecluster cluster --type=merge -p '{
    "spec": {"cluster": {
      "dataServiceMemoryQuota":   "1024Mi",
      "searchServiceMemoryQuota": "2048Mi"
    }}}'
```

**Data service.** One 256 MB `default` bucket consumes the whole 256 MiB data quota, leaving the
cases that create a bucket of their own with nowhere to put one — the bucket-management round trip,
which reads its bucket back and asserts the settings survived, and the collection-management case
that gives a bucket its own maximum expiry to check that a collection's expiry is layered over it
rather than merged into it. Both report the gateway's own "ram quota specified is too large" and
skip, so the suite stays green while quietly testing less. Creating the bucket smaller
(`--ram-quota-mb 128`) frees enough room for the couchstore case; the 1024 MiB above is what a magma
bucket needs, since the gateway rejects a magma bucket under a 1024 MB quota (and history retention
requires a further 2048 MB).

**Search service.** At 256 MiB an FTS index is accepted and then never builds a usable partition, so
every query against it fails with `pindex not available` rather than returning hits. This is the
quota search index management needs to be testable end to end.

The operator owns both settings. Posting to `/pools/default` returns 200 and reads back as applied,
and is reconciled away within the minute — patch the resource and give it ~20 seconds to appear.

#### From nothing to a cluster the whole suite can use

Sections 1–3 in one sequence, with the quotas raised before the bucket is created so nothing has to
be resized afterwards:

```console
$ k3d cluster create mycluster                                   # once per machine
$ cbdinocluster init --auto --kube-config ~/.kube/config          # once per machine

$ CBDC_ID=$(cbdinocluster allocate --deployer cao --def-file cluster-def.yaml)
$ kubectl -n cbdc2-"$CBDC_ID" patch couchbasecluster cluster --type=merge -p '{
    "spec": {"cluster": {
      "dataServiceMemoryQuota":   "1024Mi",
      "searchServiceMemoryQuota": "2048Mi"
    }}}'
$ kubectl -n cbdc2-"$CBDC_ID" wait --for=condition=Available --timeout=5m couchbasecluster/cluster

$ cbdinocluster buckets add "$CBDC_ID" default --ram-quota-mb 256
$ export TEST_CONNECTION_STRING="$(cbdinocluster connstr --couchbase2 "$CBDC_ID")?tls_verify=none"
$ ctest --test-dir build -L cng --output-on-failure
```

The patch is a separate step because the quotas cannot be expressed in `cluster-def.yaml`, and it has
to come after `allocate` because the resource it patches does not exist until then.

### TLS: connect with `tls_verify=none`

`couchbase2://` implies TLS. The certificate the local gateway serves has a SAN for its in-cluster
DNS name (`*.cbdc2-<id>.svc`), which does **not** match the NodePort host/IP you connect to, and
the operator exposes only the gateway's leaf certificate, not a chainable root. You therefore
cannot build a verifying trust chain to a local dev gateway — disable verification by appending
`tls_verify=none` to the connection string:

```
couchbase2://<host>:<port>?tls_verify=none
```

(Capella and other managed endpoints ignore `tls_verify=none` and always enforce peer
verification; it only relaxes trust for local/dev gateways.)

## 4. Build the tests

```console
$ cmake -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCOUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2=ON \
    -DCOUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS=ON
$ cmake --build build
```

`COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2=ON` pulls in the gRPC/protobuf-based transport;
`COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS=ON` registers the CNG tests with ctest under the `cng`
label.

## 5. Run the tests

env-agnostic tests need no cluster:

```console
$ ctest --test-dir build -L cng --output-on-failure
```

To include the cluster-only tests, export the endpoint (with `tls_verify=none` for a local
gateway) first:

```console
$ export TEST_CONNECTION_STRING="couchbase2://<host>:<port>?tls_verify=none"
$ ctest --test-dir build -L cng --output-on-failure
```

A single test binary can also be run directly, which is handy while iterating:

```console
$ TEST_CONNECTION_STRING="couchbase2://<host>:<port>?tls_verify=none" \
    ./build/test/cng/cng_live_kv_test
```

### Overriding the defaults

`TEST_CONNECTION_STRING` is the only variable that decides *whether* the cluster-only tests run.
Everything else they need has a default that matches a freshly allocated cbdinocluster, and each can
be overridden:

| Variable | Default | Notes |
|---|---|---|
| `TEST_CB2_USERNAME` | `Administrator` | must be able to read and write the bucket below |
| `TEST_CB2_PASSWORD` | `password` | |
| `TEST_CB2_BUCKET` | `default` | must already exist; the tests do not create it |
| `TEST_CB2_SEARCH_INDEX` | `cng-index` | the FTS index the live search case queries; see below |

`TEST_CB2_SEARCH_INDEX` needs a word of its own, because the walkthrough above does **not** create an
FTS index, and an index created outside the gateway is not a substitute. The live search case
therefore accepts either hits or `index_not_found` — the latter is an answer only a gateway that ran
the query can give, so the round trip is proven either way.

What it does need is a cluster **running the search service** — the definition above has an `fts`
node for that reason. Query a cluster without one and the request fails as an internal error rather
than as a missing index, because there is nothing to report the index missing, and the case goes red
for a reason that has nothing to do with the client.

What it does not do is exercise hit decoding: fragments, field values and locations go untested
against a real server. Closing that gap means provisioning the index over `couchbase2://` itself,
which arrives with search index management (CXXCBC-901), and giving the search service the memory to
build a partition for it — see [Cluster quota](#cluster-quota-bucket-and-search-management).

Point the suite at a shared cluster, or at one whose credentials differ from cbdinocluster's, and
these are what you set:

```console
$ export TEST_CONNECTION_STRING="couchbase2://<host>:<port>"
$ export TEST_CB2_USERNAME=tester TEST_CB2_PASSWORD=s3cret TEST_CB2_BUCKET=cngtests
$ ctest --test-dir build -L cng --output-on-failure
```

## Teardown / housekeeping

```console
$ cbdinocluster rm "$CBDC_ID"     # free the cluster when done
$ k3d cluster stop mycluster      # stop Kubernetes (keeps it for next time)
```

k3d clusters do not survive a reboot. After restarting the machine, bring the existing cluster
back up before allocating again:

```console
$ k3d cluster start mycluster
```

The REST/management endpoint of a `cao` cluster is not `host:8091`; obtain it with
`cbdinocluster mgmt "$CBDC_ID"` if you need to talk to Couchbase directly.
