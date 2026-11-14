#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_SHA:?A verified commit SHA is required}"
env_file="${GLADYS_ENV_FILE:-/etc/gladys/gladys.env}"
if [[ ! -r "$env_file" ]]; then
  # A socket-mounted runner can access the daemon while lacking host file mounts.
  # Read the host file into a private, short-lived directory; never print it.
  umask 077
  env_dir=$(mktemp -d "${TMPDIR:-/tmp}/gladys-deploy.XXXXXXXX")
  trap 'rm -rf -- "$env_dir"' EXIT
  host_env_file="$env_file"
  env_file="$env_dir/gladys.env"
  reader_image='eclipse-temurin:25-jre@sha256:ecdc0adb37bde1a281c4d245a3cd535004b1fc8322d560f5b09463b248c81df0'
  if ! docker run --rm --network none --read-only --log-driver none --user 0:0 \
    --mount "type=bind,src=$host_env_file,dst=/run/gladys.env,readonly" \
    --entrypoint /bin/cat "$reader_image" /run/gladys.env > "$env_file"; then
    printf 'Cannot read server env file through Docker: %s. Check that it exists on the Docker host.\n' "$host_env_file" >&2
    exit 1
  fi
fi

# Shell values override Compose interpolation from the shared env file.
export BRIDGE_IMAGE="mqtt-kafka-bridge:$GITHUB_SHA"
compose=(docker compose --env-file "$env_file" --project-name mqtt-kafka-bridge)
"${compose[@]}" config --quiet
docker build --tag "$BRIDGE_IMAGE" .
"${compose[@]}" up --detach --no-build --pull never --no-deps \
  --wait --wait-timeout 60 mqtt-kafka-bridge
