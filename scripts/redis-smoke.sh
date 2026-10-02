#!/usr/bin/env bash
# Read-only checks against a fresh Docker Redis, with only owned-container cleanup.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

for prerequisite in docker cargo timeout; do
    if ! command -v "$prerequisite" >/dev/null; then
        printf 'Missing prerequisite: %s\n' "$prerequisite" >&2
        exit 1
    fi
done
if ! timeout 10s docker info >/dev/null; then
    printf 'Docker is unavailable. Start its daemon and enable WSL integration if needed.\n' >&2
    exit 1
fi

work_dir=$(mktemp -d)
name="swarmcrawl-smoke-${work_dir##*/}"
cidfile="$work_dir/container.id"
cleanup() {
    status=$?
    trap - EXIT
    # Docker writes the ID on creation, even if startup is later interrupted.
    if [[ -f "$cidfile" ]]; then
        IFS= read -r container_id < "$cidfile" || true
        if [[ "$container_id" =~ ^[0-9a-f]{64}$ ]]; then
            if ! timeout 10s docker rm --force "$container_id" >/dev/null; then
                printf 'Cleanup failed; remove test container %s manually.\n' "$container_id" >&2
                status=1
            fi
        else
            printf 'Missing container ID; inspect test container %s manually.\n' "$name" >&2
            status=1
        fi
    fi
    rm -f "$cidfile"
    rmdir "$work_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! timeout 120s docker run --detach --cidfile "$cidfile" --name "$name" \
    --publish '127.0.0.1::6379' redis:7.4-alpine \
    redis-server --save '' --appendonly no >/dev/null; then
    printf 'Redis startup failed for test container %s.\n' "$name" >&2
    exit 1
fi
IFS= read -r container_id < "$cidfile" || true
mapping=$(timeout 10s docker port "$container_id" 6379/tcp)
if [[ ! "$mapping" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
    printf 'Unexpected Docker loopback port mapping.\n' >&2
    exit 1
fi
export CRAWL_REDIS_URL="redis://${mapping}/0"
unset CRAWL_REDIS_TIMEOUT_SECS

ready=false
for ((attempt = 0; attempt < 50; attempt++)); do
    if [[ "$(timeout 2s docker exec "$container_id" redis-cli ping 2>/dev/null || true)" == PONG ]]; then
        ready=true
        break
    fi
    sleep 0.1
done
if [[ "$ready" != true ]]; then
    printf 'Test Redis did not become ready within the bounded startup poll.\n' >&2
    exit 1
fi

timeout 180s cargo run --locked -- check
timeout 180s cargo test --locked --test redis_connectivity -- --ignored
