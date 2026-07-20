# Packaging guide (for maintainers)

How to build distributable packages of the Couchbase C++ client. All package
builds are driven by CMake custom targets and produce **byte-for-byte
reproducible** artifacts. A single `SOURCE_DATE_EPOCH` — the release tag's
commit time, or (for an untagged commit) `HEAD`'s commit time, or an explicit
`SOURCE_DATE_EPOCH` you pass in — is frozen into the source tarball and reused
by every downstream builder, so repeated builds from the same commit are
deterministic. (It only falls back to wall-clock time if built with no git
history and no `SOURCE_DATE_EPOCH` set.)

## Package formats and targets

| Format | Enable option | Build target | Tool used |
|--------|---------------|--------------|-----------|
| Source tarball | (always available) | `packaging_tarball` | `tar` |
| RPM | `COUCHBASE_CXX_CLIENT_RPM_TARGETS=ON` | `packaging_rpm` (and `packaging_srpm`) | `mock` |
| DEB | `COUCHBASE_CXX_CLIENT_DEB_TARGETS=ON` | `packaging_deb` | `sbuild` + `mmdebstrap` |
| APK (Alpine) | `COUCHBASE_CXX_CLIENT_APK_TARGETS=ON` | `packaging_apk` | `abuild` |

Every packaging build first regenerates the reproducible source tarball, so a
release from a git checkout and from an unpacked tarball produce identical
output.

The `fit_performer` integration-test tool is packaged **separately** as
`couchbase-cxx-client-fit-performer` (RPM) / `couchbase-cxx-client-fit-performer`
(DEB). It is a leaf package that depends on the production library; nothing
depends on it, so it is only pulled in on demand. It is built only where system
gRPC + Protobuf ≥ 3.15 are available (Fedora, EL10+, Amazon Linux 2023).

---

## Prerequisites

The package targets generate the source manifest with `git ls-files`, so they
must be run from a **real git checkout**. If the manifest cannot be produced the
build **fails loudly** ("Package builds require a git checkout …") rather than
shipping an empty tarball.

> **Using jujutsu (jj)?** A bare `jj workspace add` directory contains only
> `.jj` and no `.git`, so package builds there fail the git-checkout guard.
> Build packages from the **co-located git checkout** (the working copy that
> contains `.git`) or from a **git worktree**, not a bare jj workspace.

All formats require **CMake ≥ 3.19** and the toolchain to build the SDK.
Per-format tools:

- **RPM:** `mock`, `rpmdevtools` (for `spectool`). For rootless builds also
  `podman` (see below).
- **DEB:** `dpkg-dev`, `sbuild`, `mmdebstrap`, and the target distros'
  archive-keyring packages (`ubuntu-keyring`, `debian-archive-keyring`,
  `kali-archive-keyring`). No root/sudo required — builds run in rootless
  `unshare` mode.
- **APK:** `abuild` (from `alpine-sdk`).

---

## Source tarball

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON
cmake --build build --target packaging_tarball
```

Output: `build/packaging/couchbase-cxx-client-<version>.tar.gz`. Two independent
runs produce a byte-identical archive.

---

## RPM packages

RPMs are built with `mock`, one build root at a time (mock cannot run multiple
roots concurrently). The default roots are:

```
rocky+epel-10, rocky+epel-9, rocky+epel-8, amazonlinux-2023, fedora-44, fedora-43
```

(each suffixed with the host arch, e.g. `-x86_64`).

### Standard (host mock)

Requires the invoking user to be in the `mock` group.

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_TARGETS=ON
cmake --build build --target packaging_rpm
```

Output: `build/packaging/rpm/<root>/*.rpm` and the SRPM under
`build/packaging/srpm/`.

### Selecting roots

Override the root list to build a subset (e.g. only Fedora 44, for fast local
iteration):

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_TARGETS=ON \
  -DCOUCHBASE_CXX_CLIENT_SUPPORTED_ROOTS="fedora-44-x86_64"
cmake --build build --target packaging_rpm
```

The roots build serially and fail-fast, so one broken root halts the rest —
selecting a subset is the way to skip a root that is temporarily broken (e.g. an
expired repo signing key).

### Rootless (no mock group, no sudo)

Runs each `mock` invocation inside a rootless `podman` container, so the host
needs **only rootless podman** — no `mock` group, no `consolehelper`, no sudo.
This is the RPM counterpart to the Debian rootless `sbuild` build.

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_TARGETS=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_ROOTLESS=ON
cmake --build build --target packaging_rpm
```

Requirements:

- Rootless podman — `podman info` must report `Host.Security.Rootless=true`
  (needs `/etc/subuid` and `/etc/subgid` entries for your user). Configure fails
  early with an actionable message if not.
- A builder image (`cmake/rpm/Containerfile`) is built automatically on first
  use.

Reproducibility knobs:

- `COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE` — base image for the builder
  (default `registry.fedoraproject.org/fedora:44`). **Pin by digest**
  (`registry.fedoraproject.org/fedora@sha256:…`) for byte-for-byte
  reproducibility over time.
- `COUCHBASE_CXX_CLIENT_RPM_BUILDER_IMAGE` — tag for the locally built builder.

### Rootless cross-distro roots (EL / Amazon from a Fedora builder)

A Fedora builder image bootstraps Fedora roots directly. Building EL/Amazon
roots from a Fedora builder needs mock's podman **bootstrap image**, i.e. podman
running *inside* the build container (nested user namespaces):

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_TARGETS=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_ROOTLESS=ON \
  -DCOUCHBASE_CXX_CLIENT_RPM_ROOTLESS_BOOTSTRAP_IMAGE=ON
cmake --build build --target packaging_rpm
```

Nested podman works on a bare unprivileged host and in CI containers that permit
nested user namespaces (podman-in-podman / podman-in-docker). Where nesting is
unavailable, build each target family from a matching builder image instead
(set `COUCHBASE_CXX_CLIENT_RPM_BUILDER_BASE` to that family's base and build
only its roots).

See `cmake/rpm/README.md` for more detail.

---

## DEB packages

DEBs are built with `sbuild` in rootless `unshare` mode, bootstrapping each
distro's chroot with `mmdebstrap --variant=buildd`. No root, no sudo, no
cowbuilder. The host only needs `dpkg-dev` to produce the source package; the
real build-dependencies are installed inside the chroot.

Default distros:

```
jammy, noble, resolute, bookworm, trixie, kali-rolling
```

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_DEB_TARGETS=ON
cmake --build build --target packaging_deb
```

Output: `build/packaging/results/<...>.<distro>.<arch>/`.

Requirements:

- `dpkg-dev`, `sbuild`, `mmdebstrap` on the host.
- The archive-keyring package for each target distro must be installed on the
  host (`ubuntu-keyring`, `debian-archive-keyring`, `kali-archive-keyring`).
  Configure fails early naming the missing keyring package if one is absent.

Select a subset of distros:

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_DEB_TARGETS=ON \
  -DCOUCHBASE_CXX_CLIENT_SUPPORTED_DISTROS="noble;bookworm"
cmake --build build --target packaging_deb
```

---

## APK packages (Alpine)

```bash
cmake -B build -S . -DCOUCHBASE_CXX_CLIENT_INSTALL=ON \
  -DCOUCHBASE_CXX_CLIENT_APK_TARGETS=ON
cmake --build build --target packaging_apk
```

Requires `abuild` (from `alpine-sdk`). Unlike dpkg/rpm, `abuild` does not derive
`SOURCE_DATE_EPOCH` from package metadata, so the build passes the frozen epoch
explicitly to keep `.apk` mtimes reproducible.

---

## Reproducibility

- One `SOURCE_DATE_EPOCH` is derived from the release tag's commit time (or
  `HEAD`'s commit time for an untagged commit, or an explicit `SOURCE_DATE_EPOCH`)
  and frozen into `cmake/TarballRelease.cmake`, so git-checkout and tarball builds
  share a single deterministic timestamp.
- The tarball is created with `tar --mtime=@<epoch> --mode=go+u,go-w` and
  `gzip -n`; file modes and gzip headers carry no builder-specific data.
- RPM: the spec sets `source_date_epoch_from_changelog` and
  `clamp_mtime_to_source_date_epoch` and pins `%_buildhost`. For rootless
  builds, pin the builder base image by digest to fix the toolchain.
- DEB/RPM auto-derive `SOURCE_DATE_EPOCH` from the (frozen) changelog date; APK
  is given it explicitly.

To verify: build any format twice and compare — the artifacts are identical.
