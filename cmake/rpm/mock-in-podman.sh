#!/usr/bin/env bash
# Run a single `mock <args...>` invocation inside a rootless podman container.
# The host needs only rootless podman; mock's privileges stay inside the container.
set -euo pipefail

: "${RPM_BUILDER_IMAGE:?}" "${RPM_PACKAGING_DIR:?}" "${MOCK_RESULTDIR:?}" "${SOURCE_DATE_EPOCH:?}"
PODMAN="${RPM_PODMAN:-podman}"
USE_BOOT="${MOCK_USE_BOOTSTRAP_IMAGE:-0}"

# Re-quote the mock argv so it survives the podman -> bash -> su hops intact.
MOCK_CMD=$(printf '%q ' mock "$@")

"$PODMAN" run --rm --privileged \
  -e "SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH" \
  -e "MOCK_CMD=$MOCK_CMD" \
  -e "USE_BOOT=$USE_BOOT" \
  -e "RESULTDIR=$MOCK_RESULTDIR" \
  -e "WORKDIR=$RPM_PACKAGING_DIR" \
  -v "$RPM_PACKAGING_DIR:$RPM_PACKAGING_DIR" \
  -w "$RPM_PACKAGING_DIR" \
  "$RPM_BUILDER_IMAGE" \
  bash -lc '
    set -euo pipefail
    if [ "$USE_BOOT" = 1 ]; then
      echo "config_opts[\"use_bootstrap_image\"] = True"  >> /etc/mock/site-defaults.cfg
    else
      echo "config_opts[\"use_bootstrap_image\"] = False" >> /etc/mock/site-defaults.cfg
    fi
    # simple isolation = plain chroot; nspawn needs real root/systemd and cannot run
    # rootless. Set it explicitly rather than relying on mock isolation=auto detection.
    echo "config_opts[\"isolation\"] = \"simple\"" >> /etc/mock/site-defaults.cfg
    # rootless podman maps host-uid -> container-root, so the bind-mounted resultdir
    # is root-owned inside; mock builds as unprivileged "builder" and must own it.
    mkdir -p "$RESULTDIR"
    chown -R builder:builder "$RESULTDIR"
    # su - resets the environment, so re-export SOURCE_DATE_EPOCH across the boundary
    # (belt-and-suspenders: the spec also derives it from the frozen %changelog). Quote
    # WORKDIR so a path with spaces survives.
    su - builder -c "export SOURCE_DATE_EPOCH=\"$SOURCE_DATE_EPOCH\"; cd \"$WORKDIR\" && $MOCK_CMD"
  '

# Hand the results back to the invoking user. Under rootless podman the in-container
# "builder" uid maps to an unmapped subuid on the host, so the RPMs land owned by that
# subuid. Re-enter the user namespace (where the host user is uid 0) and chown them to
# 0:0, i.e. back to the invoking user on the host -- keeps artifacts user-owned and
# easy to read/clean up. Best-effort: a normalization hiccup must not fail a good build.
"$PODMAN" unshare chown -R 0:0 "$MOCK_RESULTDIR" \
  || echo "warning: could not normalize ownership of $MOCK_RESULTDIR (rootless subuid mapping)" >&2
