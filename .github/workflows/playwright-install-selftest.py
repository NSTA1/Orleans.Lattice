#!/usr/bin/env python3
"""Exercise the actual installer with real Linux privilege and process boundaries.

No packages or browsers are installed. Only the attempt clock and five-second
retry sleep are shortened; sudo, process groups, signals and file locks are real.
Run as a non-root user with passwordless sudo (the GitHub-hosted runner contract).
"""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

WORKFLOWS = Path(__file__).resolve().parent

WORKER = r'''
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path(__file__).resolve().parent.parent
if sys.argv[1] == "--hold":
    lock = open(root / "lock", "a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    owner = {
        "pid": os.getpid(), "uid": os.geteuid(), "group": os.getpgrp(),
    }
    with (root / "owners").open("a") as log:
        log.write(json.dumps(owner) + "\n")
    (root / "owner").write_text(json.dumps(owner))
    while True:
        signal.pause()

mode = (root / "mode").read_text()
arguments = sys.argv[1:]
with (root / "calls").open("a") as log:
    log.write(json.dumps({"uid": os.geteuid(), "args": arguments}) + "\n")
count = len((root / "calls").read_text().splitlines())
if mode == "fail":
    sys.exit(7)
if mode == "retry" and count == 1:
    sys.exit(8)
lock = open(root / "lock", "a")
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print("fixture: previous privileged installer still owns the lock; inner apt exit 100", flush=True)
    sys.exit(1)  # Playwright maps the apt failure to wrapper exit 1.
fcntl.flock(lock, fcntl.LOCK_UN)
if mode in ("hang", "exit-with-child", "exit-with-escaped-child", "exhaust") and (
        count == 1 or mode == "exhaust"):
    command = [sys.executable, __file__, "--hold"]
    if os.geteuid() != 0:
        command = [SUDO, "-n", "--", *command]
    (root / "owner").unlink(missing_ok=True)
    subprocess.Popen(
        command, start_new_session=mode == "exit-with-escaped-child",
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 5
    while not (root / "owner").exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("privileged lock holder did not become ready")
        time.sleep(0.01)
    if mode in ("exit-with-child", "exit-with-escaped-child"):
        sys.exit(9)
    while True:
        signal.pause()
'''


class PrivilegedInstallerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux" or os.geteuid() == 0:
            raise AssertionError("Run the privilege regression on Linux as a non-root sudo user")
        cls.sudo = shutil.which("sudo")
        cls.pwsh = shutil.which("pwsh")
        if cls.sudo is None or cls.pwsh is None:
            raise AssertionError("sudo and pwsh are required; do not skip the privilege regression")
        subprocess.run([cls.sudo, "-n", "true"], check=True, timeout=10)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="playwright-owner-")
        self.root = Path(self.temporary.name)
        self.root.chmod(0o777)
        self.addCleanup(self.cleanup)
        for name in ("install-playwright-browsers.sh", "run-playwright-install.py"):
            shutil.copyfile(WORKFLOWS / name, self.root / name)
        script = self.root / "test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1"
        script.parent.mkdir(parents=True)
        self.tools = self.root / "bin"
        self.tools.mkdir()
        for name in ("calls", "lock", "owner", "owners"):
            path = self.root / name
            path.touch()
            path.chmod(0o666)
        (self.root / "owner").unlink()
        self.write_tool("worker", f"#!{sys.executable}\nSUDO = {self.sudo!r}\n" + WORKER)
        script.write_text(
            f"& '{self.tools / 'worker'}' @args\nexit $LASTEXITCODE\n",
            encoding="utf-8",
        )
        self.write_tool("pwsh", f'#!/bin/bash\nexec "{self.pwsh}" -NoProfile -NonInteractive "$@"\n')
        # sudo's secure_path intentionally does not inherit the fixture PATH.
        # Resolve the clock shim before elevation, but use real sudo for the exec.
        self.write_tool("sudo", f"""\
            #!/bin/bash
            [ "$1" = --non-interactive ] && [ "$2" = -- ] && [ "$3" = python3 ] || exit 96
            shift 3
            exec "{self.sudo}" -n -- "{self.tools / 'python3'}" "$@"
        """)
        self.write_tool("python3", f"""\
            #!/bin/bash
            [ "$2" = 120 ] && [ "$3" = 10 ] || exit 95
            script="$1"
            shift 3
            exec "{sys.executable}" "$script" 2 0.3 "$@"
        """)
        self.write_tool("timeout", """\
            #!/bin/bash
            [ "$1" = --kill-after=10s ] && [ "$2" = 120s ] || exit 97
            shift 2
            exec /usr/bin/timeout --kill-after=0.3s 2s "$@"
        """)
        self.write_tool("sleep", "#!/bin/bash\nexit 0\n")
        self.env = dict(os.environ, PATH=f"{self.tools}:{os.environ['PATH']}")

    def write_tool(self, name, body):
        path = self.tools / name
        path.write_text(textwrap.dedent(body), encoding="utf-8", newline="\n")
        path.chmod(0o755)

    def cleanup(self):
        # Only fixture PIDs, never a package-manager name, lock path or process scan.
        for owner in (self.root / "owners").read_text().splitlines():
            pid = json.loads(owner)["pid"]
            try:
                command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            except FileNotFoundError:
                continue
            if os.fsencode(self.tools / "worker") in command and b"--hold" in command:
                subprocess.run([self.sudo, "-n", "kill", "-KILL", str(pid)],
                               check=True, timeout=10)
        self.temporary.cleanup()

    def install(self, mode, cache="true", *browsers):
        (self.root / "mode").write_text(mode)
        return subprocess.run(
            ["bash", str(self.root / "install-playwright-browsers.sh"), cache,
             *(browsers or ("chromium",))],
            cwd=self.root, env=self.env, text=True, capture_output=True, timeout=15,
        )

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def assert_owner_reaped(self):
        owners = (self.root / "owners").read_text().splitlines()
        self.assertGreater(len(owners), 0)
        for line in owners:
            owner = json.loads(line)
            self.assertEqual(0, owner["uid"], owner)
            self.assertFalse(Path(f"/proc/{owner['pid']}").exists(), owner)
        import fcntl
        with (self.root / "lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_timeout_reaps_privileged_term_ignoring_descendant_before_retry(self):
        result = self.install("hang")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(2, len(self.calls()), result.stdout)
        self.assertIn("exit code 137", result.stdout)
        self.assertIn("Killing remaining installer processes", result.stdout)
        self.assert_owner_reaped()

    def test_cleanup_does_not_signal_unrelated_privileged_process(self):
        marker = self.root / "unrelated"
        sentinel = subprocess.Popen([
            self.sudo, "-n", "--", sys.executable, "-c",
            "import os, pathlib, signal, sys; "
            "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); signal.pause()",
            str(marker),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(marker.exists(), "Unrelated privileged sentinel did not start")
            pid = marker.read_text()
            result = self.install("hang")
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            subprocess.run([self.sudo, "-n", "kill", "-0", pid], check=True, timeout=5)
            self.assert_owner_reaped()
        finally:
            if marker.exists():
                subprocess.run([self.sudo, "-n", "kill", "-KILL", marker.read_text()],
                               check=True, timeout=5)
            sentinel.wait(timeout=5)

    def test_exited_parent_cannot_leave_its_privileged_descendant_for_next_attempt(self):
        result = self.install("exit-with-child")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(2, len(self.calls()), result.stdout)
        self.assertIn("exit code 9", result.stdout)
        self.assert_owner_reaped()

    def test_exited_parent_cannot_leave_a_privileged_setsid_descendant_for_next_attempt(self):
        result = self.install("exit-with-escaped-child")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(2, len(self.calls()), result.stdout)
        self.assertIn("exit code 9", result.stdout)
        self.assert_owner_reaped()
        owner = json.loads((self.root / "owner").read_text())
        self.assertEqual(owner["pid"], owner["group"], "Probe must actually call setsid")

    def test_group_only_cleanup_mutation_is_rejected_by_the_setsid_regression(self):
        runner = self.root / "run-playwright-install.py"
        source = runner.read_text()
        self.assertEqual(1, source.count("for child in children:"))
        self.assertEqual(1, source.count("os.waitpid(-1,"))
        runner.write_text(source.replace(
            "for child in children:", "for child in ():",
        ).replace("os.waitpid(-1,", "os.waitpid(-process.pid,"))
        result = self.install("exit-with-escaped-child")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, len(self.calls()), result.stdout)
        self.assertIn("exit code 9", result.stdout)
        self.assertEqual(2, result.stdout.count("failed with exit code 1."))
        self.assertIn("still owns the lock", result.stdout)
        owner = json.loads((self.root / "owner").read_text())
        self.assertEqual(0, owner["uid"])
        self.assertEqual(owner["pid"], owner["group"])
        self.assertTrue(Path(f"/proc/{owner['pid']}").exists(), owner)
        print("Group-only supervisor: setsid owner survives, wrapper exits 9/1/1 (caught).")

    def test_timeout_exhaustion_cleans_every_attempt_and_preserves_failure(self):
        result = self.install("exhaust")
        self.assertEqual(137, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, len(self.calls()), result.stdout)
        self.assertIn("failed after three bounded attempts", result.stdout)
        self.assert_owner_reaped()

    def test_success_and_cache_miss_keep_downloads_unprivileged_and_selection_intact(self):
        result = self.install("pass", "", "chromium", "firefox", "webkit")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual([
            {"uid": 0, "args": ["install-deps", "chromium", "firefox", "webkit"]},
            {"uid": os.geteuid(), "args": ["install", "chromium", "firefox", "webkit"]},
        ], self.calls())

    def test_non_timeout_failure_exhausts_without_download_and_preserves_exit(self):
        result = self.install("fail", "false")
        self.assertEqual(7, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, len(self.calls()), result.stdout)
        self.assertTrue(all(call["args"] == ["install-deps", "chromium"] for call in self.calls()))
        self.assertIn("::error::", result.stdout)

    def test_original_unprivileged_timeout_is_rejected_by_the_lock_regression(self):
        installer = self.root / "install-playwright-browsers.sh"
        source = installer.read_text()
        invocation = 'if "${command[@]}"; then'
        self.assertEqual(1, source.count(invocation))
        installer.write_text(source.replace(
            invocation,
            'if timeout --kill-after=10s 120s pwsh "$script" "$@"; then',
        ))
        result = self.install("hang")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, len(self.calls()), result.stdout)
        self.assertIn("exit code 124", result.stdout)
        self.assertEqual(2, result.stdout.count("failed with exit code 1."))
        self.assertIn("still owns the lock", result.stdout)
        self.assertIn("inner apt exit 100", result.stdout)
        owner = json.loads((self.root / "owner").read_text())
        self.assertEqual(0, owner["uid"])
        self.assertTrue(Path(f"/proc/{owner['pid']}").exists(), owner)
        print("Original helper wrapper exits: 124/1/1; inner apt failures: 100/100 (caught).")

    def test_native_regression_is_ungated_in_lightweight_ci(self):
        workflow = (WORKFLOWS / "ci.yml").read_text()
        match = re.search(
            r"      - name: Self-test privileged Playwright installer cleanup\n(.*?)(?=      - |\Z)",
            workflow, re.DOTALL,
        )
        self.assertIsNotNone(match, "Native privilege regression is not wired to CI")
        self.assertNotIn("if:", match[1])
        self.assertIn("run: python3 .github/workflows/playwright-install-selftest.py", match[1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
