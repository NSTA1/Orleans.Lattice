#!/usr/bin/env bash
# Main container of a runner Job execution: start one single-use runner from
# the JIT config the init container wrote, run exactly one job, and exit.
#
# Idle watchdog. KEDA starts one execution per queued job, but a job cancelled
# (or taken by another runner) before this runner registers leaves it idle
# until the Job's replica timeout - hours of a dedicated node. So if no job has
# started within RUNNER_IDLE_TIMEOUT_SEC (default 600), the runner is stopped
# and the container exits. A job starting is visible as the runner's Worker
# process, which exists only while a job runs.
set -euo pipefail

config="${JIT_CONFIG_PATH:-/jit/config}"
if [ ! -s "$config" ]; then
  echo "runner-entrypoint: no JIT config at $config" >&2
  exit 1
fi

idle_timeout="${RUNNER_IDLE_TIMEOUT_SEC:-600}"
cd /home/runner
./run.sh --jitconfig "$(cat "$config")" &
runner_pid=$!

(
  waited=0
  while kill -0 "$runner_pid" 2>/dev/null; do
    if pgrep -f 'Runner.Worker' >/dev/null 2>&1; then
      exit 0
    fi
    if [ "$waited" -ge "$idle_timeout" ]; then
      echo "runner-entrypoint: no job started within ${idle_timeout}s; stopping the idle runner" >&2
      pkill -TERM -P "$runner_pid" 2>/dev/null || true
      kill -TERM "$runner_pid" 2>/dev/null || true
      exit 0
    fi
    sleep 10
    waited=$((waited + 10))
  done
) &

wait "$runner_pid"
