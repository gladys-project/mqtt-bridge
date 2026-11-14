#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_SHA:?A verified commit SHA is required}"
env_file="${GLADYS_ENV_FILE:-/etc/gladys/gladys.env}"
if [[ ! -r "$env_file" ]]; then
  printf 'Shared server env file is not readable: %s\n' "$env_file" >&2
  exit 1
fi

# Shell values override Compose interpolation from the shared env file.
export BRIDGE_IMAGE="mqtt-kafka-bridge:$GITHUB_SHA"
compose=(docker compose --env-file "$env_file" --project-name mqtt-kafka-bridge)
"${compose[@]}" config --quiet
docker build --tag "$BRIDGE_IMAGE" .
"${compose[@]}" up --detach --no-build --pull never --no-deps \
  --wait --wait-timeout 60 mqtt-kafka-bridge
