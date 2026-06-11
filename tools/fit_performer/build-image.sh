#!/usr/bin/env bash
#
#  Copyright 2020-Present Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# Build the C++ SDK FIT performer Docker image locally and verify it starts.
#
# This mirrors what the "Build FIT performer image" GitHub workflow does via
# couchbaselabs/sdk-docker-build-action, but stays entirely local: it builds
# tools/fit_performer/Dockerfile with the repository root as the build context,
# then runs the resulting image and waits for the gRPC server to report that it
# is listening.

set -euo pipefail

TAG="cxx-fit-performer:local"
PORT="8060"
NO_CACHE=""
SKIP_RUN="false"
RUN_TIMEOUT="30"

usage() {
  cat <<'EOF'
Usage: tools/fit_performer/build-image.sh [options]

Builds the C++ FIT performer Docker image (Release) and verifies it starts.

Options:
  -t, --tag TAG        Image tag to build (default: cxx-fit-performer:local)
  -p, --port PORT      Host port to map when verifying (default: 8060)
      --no-cache       Pass --no-cache to docker build
      --skip-run       Build only; do not start a container to verify
      --timeout SECS   Seconds to wait for the "listening" log line (default: 30)
  -h, --help           Show this help

Examples:
  tools/fit_performer/build-image.sh
  tools/fit_performer/build-image.sh -t my/cxx-fit-performer:dev --no-cache
  tools/fit_performer/build-image.sh --skip-run
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--tag)     TAG="$2"; shift 2 ;;
    -p|--port)    PORT="$2"; shift 2 ;;
    --no-cache)   NO_CACHE="--no-cache"; shift ;;
    --skip-run)   SKIP_RUN="true"; shift ;;
    --timeout)    RUN_TIMEOUT="$2"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# Resolve the repository root so the script works from any directory. The build
# context MUST be the repo root because the performer links the full client.
REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
DOCKERFILE="${REPO_ROOT}/tools/fit_performer/Dockerfile"

echo "==> Repository root: ${REPO_ROOT}"
echo "==> Dockerfile:      ${DOCKERFILE}"
echo "==> Image tag:       ${TAG}"
echo

echo "==> Building image (this builds gRPC/protobuf/BoringSSL/OpenTelemetry from"
echo "    source on first run and can take 15+ minutes)..."
# shellcheck disable=SC2086 # NO_CACHE is intentionally word-split (empty or flag)
docker build ${NO_CACHE} \
  -f "${DOCKERFILE}" \
  -t "${TAG}" \
  --build-arg SDK=cxx \
  "${REPO_ROOT}"

echo
echo "==> Build succeeded: ${TAG}"

if [[ "${SKIP_RUN}" == "true" ]]; then
  echo "==> --skip-run set; not starting a container."
  exit 0
fi

CONTAINER_NAME="cxx-fit-performer-verify-$$"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo
echo "==> Starting container '${CONTAINER_NAME}' on host port ${PORT}..."
docker run -d --name "${CONTAINER_NAME}" -p "${PORT}:8060" "${TAG}" >/dev/null

echo "==> Waiting up to ${RUN_TIMEOUT}s for the performer to report it is listening..."
deadline=$((SECONDS + RUN_TIMEOUT))
listening="false"
while (( SECONDS < deadline )); do
  # The container must still be running.
  if [[ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null)" != "true" ]]; then
    echo "ERROR: container exited prematurely. Logs:" >&2
    docker logs "${CONTAINER_NAME}" >&2 || true
    exit 1
  fi
  if docker logs "${CONTAINER_NAME}" 2>&1 | grep -q "listening on '0.0.0.0:8060'"; then
    listening="true"
    break
  fi
  sleep 1
done

echo
echo "----- container logs -----"
docker logs "${CONTAINER_NAME}" 2>&1 || true
echo "--------------------------"
echo

if [[ "${listening}" != "true" ]]; then
  echo "ERROR: performer did not report listening within ${RUN_TIMEOUT}s." >&2
  exit 1
fi

echo "==> SUCCESS: '${TAG}' built and the FIT performer is listening on port ${PORT}."
echo "    The container will be removed automatically. To run it yourself:"
echo "      docker run --rm -p 8060:8060 ${TAG}"
