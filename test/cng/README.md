# CNG (`couchbase2://`) tests

These tests exercise the Protostellar / Cloud Native Gateway (CNG) transport. They are built
with the hand-rolled harness under `framework/` (not Catch2) and are enabled by the
`COUCHBASE_CXX_CLIENT_BUILD_CNG_TESTS` CMake option.

Each test declares an environment gate:

- **env-agnostic** — runs anywhere; uses in-process or loopback gRPC servers. No cluster needed.
- **cluster-only** — needs a reachable `couchbase2://` gateway; skipped when
  `TEST_CONNECTION_STRING` is unset (the harness exits `77`, which ctest reports as *Skipped*).

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

There is deliberately no `docker:` memory block here: the `cao` deployer ignores it, so service
quotas stay at the server defaults regardless. That is also why the bucket below is created with a
small explicit quota. The four nodes cover every service the suite can touch; if you only need KV
and query, a single `services: [kv, n1ql, index]` node is enough, which is what CI allocates.

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
