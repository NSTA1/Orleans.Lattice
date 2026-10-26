#!/usr/bin/env python3
"""Bound one Playwright attempt and reap its process group before returning."""

import ctypes
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def run(command: list[str], limit: float, grace: float) -> int:
    # Adopt orphaned grandchildren so an exited pwsh cannot hide live apt work.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "Cannot supervise installer descendants")
    process = subprocess.Popen(command, start_new_session=True)
    code = 1
    interrupted = 0

    def stop(signum, _frame):
        nonlocal interrupted
        interrupted = signum
        raise InterruptedError(f"Installer interrupted by signal {signum}")

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, stop)

    def send(signum):
        children = [
            int(pid) for pid in
            Path(f"/proc/self/task/{os.getpid()}/children").read_text().split()
        ]
        # These PIDs are our unreaped children, so they cannot be recycled
        # between enumeration and signaling. Include adopted setsid descendants.
        if any(os.getpgid(child) == process.pid for child in children):
            os.killpg(process.pid, signum)
        for child in children:
            try:
                os.kill(child, signum)
            except ProcessLookupError:
                pass

    def drained():
        process.poll()
        while True:
            try:
                child, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return True
            if child == 0:
                return False

    try:
        code = process.wait(timeout=limit)
        if code < 0:
            code = 128 - code
    except subprocess.TimeoutExpired:
        print(f"::warning::Playwright installer exceeded {limit:g} seconds.", flush=True)
        code = 124
    except InterruptedError as error:
        print(f"::warning::{error}", flush=True)
        code = 128 + interrupted
    finally:
        # A second termination request must not interrupt ownership cleanup.
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        # A drained group no longer belongs to us; do not signal a reusable id.
        if not drained():
            send(signal.SIGTERM)
        deadline = time.monotonic() + grace
        while not drained() and time.monotonic() < deadline:
            send(signal.SIGTERM)
            time.sleep(0.02)
        if not drained():
            print("::warning::Killing remaining installer processes after TERM grace.", flush=True)
            send(signal.SIGKILL)
            if code == 124:
                code = 137
        # Killing an intermediate parent can adopt another escaped descendant.
        # No second grace period: kill/reap until the subreaper owns no children.
        while not drained():
            send(signal.SIGKILL)
            time.sleep(0.02)
    return code


if __name__ == "__main__":
    sys.exit(run(sys.argv[3:], float(sys.argv[1]), float(sys.argv[2])))
