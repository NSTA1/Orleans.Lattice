#!/usr/bin/env bash
# Main container of a runner Job execution: start one single-use runner from
# the JIT config the init container wrote, run exactly one job, and exit.
set -euo pipefail

config="${JIT_CONFIG_PATH:-/jit/config}"
if [ ! -s "$config" ]; then
  echo "runner-entrypoint: no JIT config at $config" >&2
  exit 1
fi

cd /home/runner
exec ./run.sh --jitconfig "$(cat "$config")"
