# RPM packaging

`packaging_rpm` builds the RPMs with [mock](https://rpm-software-management.github.io/mock/),
one build root at a time. The roots are selectable via
`COUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS`.

## Rootless RPM builds

By default `mock` runs on the host, which needs the `mock` group and
`consolehelper`. To build with no privileged host setup (rootless podman only):

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_TARGETS=ON -DCOUCHBASE_CXX_CLIENT_RPM_ROOTLESS=ON
cmake --build build --target packaging_rpm
```

In this mode every `mock` invocation runs inside a rootless podman container
(`cmake/rpm/Containerfile`, built automatically), so the host needs neither the
`mock` group nor sudo. This is the RPM-side counterpart to the Debian
`sbuild`/`mmdebstrap` unshare build.

Requirements:

- rootless podman — `podman info` must report `Host.Security.Rootless=true`
  (needs `/etc/subuid` and `/etc/subgid` entries for your user). Configure is
  gated on this and fails early with an actionable message otherwise.

Knobs:

- `COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE` — base image for the builder
  (default `registry.fedoraproject.org/fedora:44`; pin by digest for
  byte-for-byte reproducibility).
- `COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE` — tag for the locally built builder.
- `COUCHBASE_CXX_CLIENT_RPM_ROOTLESS_BOOTSTRAP_IMAGE` — see below.

### Cross-distro roots and nested podman

A Fedora builder image can build Fedora roots directly (the container's own dnf
bootstraps the chroot). Building EL/Amazon roots from a Fedora builder needs
mock's podman **bootstrap image**, i.e. podman running *inside* the build
container (nested user namespaces). Enable it with:

```bash
-DCOUCHBASE_CXX_CLIENT_RPM_ROOTLESS_BOOTSTRAP_IMAGE=ON
```

Nested podman works on a bare unprivileged host and in CI containers that permit
nested user namespaces (podman-in-podman / podman-in-docker). Where nesting is
not available, build each target family from a matching builder image instead
(set `COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE` to that family's base image and
build only its roots).
