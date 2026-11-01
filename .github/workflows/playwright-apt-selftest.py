#!/usr/bin/env python3
"""Real signed APT acquisitions through the actual bounded Playwright helper.

Only private fixture packages are installed, into an isolated dpkg root/status.
No host packages, apt configuration, locks, trust stores or caches are changed.
"""

from datetime import datetime, timezone
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.dont_write_bytecode = True
from playwright_apt_mirrors import MirrorConfigurationError, rewrite_sources, rotate_mirrors

WORKFLOWS = Path(__file__).resolve().parent
PACKAGES = {"tools": "fixture-a-tools", "chromium": "fixture-z-chromium",
            "firefox": "fixture-y-firefox", "webkit": "fixture-x-webkit"}

APT_WORKER = r'''
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys

root = Path(__file__).resolve().parent.parent
args = sys.argv[1:]
config = Path(os.environ["APT_CONFIG"])
parts_assignment = subprocess.check_output(
    ["apt-config", "shell", "PARTS", "Dir::Etc::sourceparts/d"], text=True)
parts = Path(shlex.split(parts_assignment)[0].split("=", 1)[1])
sources = {path.name: path.read_text() for path in parts.iterdir() if path.is_file()}
with (root / "calls").open("a") as log:
    log.write(json.dumps({"uid": os.geteuid(), "args": args, "config": str(config),
                         "sources": sources,
                         "dump": subprocess.check_output(["apt-config", "dump"], text=True)}) + "\n")
if args[0] == "install":
    assert os.geteuid() != 0
    assert config == root / "apt.conf", "Dependency overlay leaked into browser download"
    (root / "browser-cache").write_text(" ".join(args[1:]))
    sys.exit(0)
packages = PACKAGES
dependencies = list(dict.fromkeys([packages["tools"], *(packages[name] for name in args[1:])]))
for command in (["apt-get", "update"],
                ["apt-get", "install", "-y", "--no-install-recommends", *dependencies]):
    code = subprocess.call(command)
    if code:
        sys.exit(1)
'''


class ConfiguredMirrorTests(unittest.TestCase):
    def test_only_existing_unique_unqualified_candidates_can_advance(self):
        text = "http://slow.invalid/ priority:1\nhttps://fast.invalid/ priority:2\n"
        rotated = rotate_mirrors(text, 1)
        self.assertEqual("https://fast.invalid/\tpriority:1\nhttp://slow.invalid/\tpriority:2\n",
                         rotated)
        for unsupported in (
            text.replace("priority:2", "priority:1"),
            text.replace("priority:2", "priority:2 arch:amd64"),
            text.replace("https://fast.invalid/", "file:/unconfigured/"),
            text.replace("https://fast.invalid/", "https://user:secret@fast.invalid/"),
            "http://only.invalid/ priority:1\n",
        ):
            with self.subTest(unsupported=unsupported), self.assertRaises(MirrorConfigurationError):
                rotate_mirrors(unsupported, 1)

    def test_source_rewrite_preserves_disabled_stanzas_fields_and_other_repositories(self):
        active = ("Types: deb deb-src\nURIs: mirror+file:/configured\nSuites: noble noble-updates\n"
                  "Components: main universe\nSigned-By: /unchanged-keyring.gpg\n")
        disabled = "\nEnabled: no\nURIs: mirror+file:/disabled\nSuites: other\n"
        other = "\nTypes: deb\nURIs: https://other.invalid/\nSuites: vendor\n"
        result = rewrite_sources(active + disabled + other, ".sources",
                                 lambda line: line.replace("/configured", "/attempt"))
        self.assertEqual(active.replace("/configured", "/attempt") + disabled + other, result)

    def test_signed_regression_is_ungated_in_lightweight_ci(self):
        yaml = (WORKFLOWS / "ci.yml").read_text()
        block = re.search(
            r"      - name: Self-test bounded Playwright APT mirror failover\n(.*?)(?=      - |\Z)",
            yaml, re.DOTALL,
        )
        self.assertIsNotNone(block)
        self.assertNotIn("if:", block[1])
        self.assertIn("run: python3 .github/workflows/playwright-apt-selftest.py", block[1])


class SignedAptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux" or os.geteuid() == 0:
            raise AssertionError("Run on Linux as the non-root GitHub runner sudo user")
        cls.sudo = shutil.which("sudo")
        cls.pwsh = shutil.which("pwsh")
        for tool in ("sudo", "pwsh", "apt-config", "apt-get", "dpkg-deb", "gpg", "gpgconf"):
            if shutil.which(tool) is None:
                raise AssertionError(f"{tool} is required; do not skip the signed APT regression")
        subprocess.run([cls.sudo, "-n", "true"], check=True, timeout=5)
        cls.keys = tempfile.TemporaryDirectory(prefix="playwright-apt-keys-")
        cls.addClassCleanup(cls.keys.cleanup)
        cls.home = Path(cls.keys.name)
        cls.addClassCleanup(lambda: subprocess.run(
            ["gpgconf", "--homedir", str(cls.home), "--kill", "gpg-agent"],
            check=True, timeout=5,
        ))
        cls.gpg("--batch", "--pinentry-mode", "loopback", "--passphrase", "",
                "--quick-generate-key", "APT fixture <apt@example.invalid>", "ed25519", "sign", "0")
        cls.key = cls.gpg("--export").stdout

    @classmethod
    def gpg(cls, *arguments):
        return subprocess.run(
            ["gpg", "--homedir", str(cls.home), *arguments],
            check=True, capture_output=True, timeout=10,
        )

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="playwright-apt-fixture-")
        self.root = Path(self.temporary.name)
        self.root.chmod(0o755)
        self.servers = []
        self.requests = []
        self.mode = "fallback"
        self.addCleanup(self.cleanup)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.build_repository()
        for index in range(3):
            self.start_mirror(index)
        self.build_apt_configuration()
        self.build_installer()
        self.original_files = {
            path: path.read_bytes() for path in self.root.glob("apt*") if path.is_file()
        }
        self.original_files.update({
            path: path.read_bytes() for path in (self.root / "sources.list.d").iterdir()
        })
        self.original_files.update({
            path: path.read_bytes() for path in (self.root / "apt.conf.d").iterdir()
        })
        self.original_files[self.root / "mirrors.txt"] = (self.root / "mirrors.txt").read_bytes()

    def build_repository(self):
        self.payloads = {}
        entries = {"main": [], "universe": []}
        for group, package in PACKAGES.items():
            build = self.root / package
            (build / "DEBIAN").mkdir(parents=True)
            (build / "DEBIAN/control").write_text(
                f"Package: {package}\nVersion: 1.0\nArchitecture: all\n"
                "Maintainer: Fixture <apt@example.invalid>\nDescription: Isolated regression fixture\n"
            )
            payload = build / f"usr/share/{package}/payload"
            payload.parent.mkdir(parents=True)
            self.payloads[package] = os.urandom(4096 if group == "tools" else 131072)
            payload.write_bytes(self.payloads[package])
            filename = f"{package}_1.0_all.deb"
            subprocess.run(
                ["dpkg-deb", "-Znone", "--build", "--root-owner-group",
                 str(build), str(self.repo / filename)],
                check=True, capture_output=True, timeout=5,
            )
            data = (self.repo / filename).read_bytes()
            entries["universe" if group == "tools" else "main"].append(
                (build / "DEBIAN/control").read_text()
                + f"Filename: {filename}\nSize: {len(data)}\n"
                + f"SHA256: {hashlib.sha256(data).hexdigest()}\n\n"
            )
        for suite in ("noble", "noble-updates", "vendor"):
            hashes = []
            for component, packages in entries.items():
                relative = f"{component}/binary-amd64/Packages"
                path = self.repo / "dists" / suite / relative
                path.parent.mkdir(parents=True)
                path.write_text("" if suite == "vendor" else "".join(packages))
                path.with_suffix(".gz").write_bytes(gzip.compress(path.read_bytes(), mtime=0))
                for file in (path, path.with_suffix(".gz")):
                    data = file.read_bytes()
                    hashes.append(f" {hashlib.sha256(data).hexdigest()} {len(data)} "
                                  f"{file.relative_to(self.repo / 'dists' / suite)}\n")
            release = self.repo / "dists" / suite / "Release"
            release.write_text(
                f"Suite: {suite}\nCodename: {suite}\n"
                f"Date: {datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S +0000')}\n"
                "Architectures: amd64\nComponents: main universe\nSHA256:\n" + "".join(hashes)
            )
            self.gpg("--batch", "--yes", "--pinentry-mode", "loopback", "--passphrase", "",
                     "--clearsign", "--output", str(release.with_name("InRelease")), str(release))
            release.with_name("InRelease").chmod(0o644)

    def start_mirror(self, index):
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format, *arguments):
                pass

            def do_GET(self):
                relative = self.path.lstrip("/")
                if ".." in relative.split("/"):
                    self.send_error(400)
                    return
                path = fixture.repo / relative
                if not path.is_file():
                    self.send_error(404)
                    return
                data = path.read_bytes()
                if index > 0 and fixture.mode == "signature" and path.name == "InRelease":
                    data = data.replace(b"Components: main", b"Components: evil")
                if index > 0 and fixture.mode == "hash" and path.suffix == ".deb":
                    data = data[:-1] + bytes([data[-1] ^ 1])
                start = 0
                if self.headers.get("Range"):
                    start = int(self.headers["Range"].split("=")[1].split("-")[0])
                event = {"mirror": index, "path": relative, "start": start, "bytes": 0}
                fixture.requests.append(event)
                self.send_response(206 if start else 200)
                self.send_header("Content-Length", str(len(data) - start))
                self.send_header("Last-Modified", "Tue, 14 Nov 2023 22:13:20 GMT")
                self.send_header("Accept-Ranges", "bytes")
                if start:
                    self.send_header("Content-Range", f"bytes {start}-{len(data)-1}/{len(data)}")
                self.end_headers()
                slow = (path.suffix == ".deb" and "fixture-a-tools" not in path.name
                        and (index == 0 or fixture.mode == "all-slow"))
                try:
                    for offset in range(start, len(data), 1024):
                        block = data[offset:offset + 1024]
                        self.wfile.write(block)
                        self.wfile.flush()
                        event["bytes"] += len(block)
                        if slow:
                            time.sleep(0.1)
                except (BrokenPipeError, ConnectionResetError):
                    # Expected when the owned installer reaches its outer deadline.
                    return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append((server, thread))

    def build_apt_configuration(self):
        (self.root / "keyring.gpg").write_bytes(self.key)
        (self.root / "apt.conf.d").mkdir()
        (self.root / "sources.list.d").mkdir()
        (self.root / "dpkg").mkdir()
        (self.root / "dpkg/status").touch()
        (self.root / "installed").mkdir()
        for directory in ("state/lists/partial", "cache/archives/partial", "log"):
            (self.root / directory).mkdir(parents=True)
        (self.root / "mirrors.txt").write_text("".join(
            f"http://127.0.0.1:{server.server_port}/\tpriority:{index}\n"
            for index, (server, _) in enumerate(self.servers, 1)
        ))
        (self.root / "sources.list.d/ubuntu.sources").write_text(
            "Types: deb\n"
            f"URIs: mirror+file:{self.root / 'mirrors.txt'}\n"
            "Suites: noble noble-updates\nComponents: main universe\n"
            f"Architectures: amd64\nSigned-By: {self.root / 'keyring.gpg'}\n"
        )
        (self.root / "sources.list.d/other.list").write_text(
            f"deb [signed-by={self.root / 'keyring.gpg'}] file:{self.repo} vendor main\n"
        )
        (self.root / "apt.conf").write_text(
            f'Dir::Etc "{self.root}";\nDir::Etc::main "-";\n'
            'Dir::Etc::parts "apt.conf.d";\nDir::Etc::sourcelist "sources.list";\n'
            'Dir::Etc::sourceparts "sources.list.d";\n'
            f'Dir::State "{self.root / "state"}";\n'
            f'Dir::State::status "{self.root / "dpkg/status"}";\n'
            f'Dir::Cache "{self.root / "cache"}";\nDir::Log "{self.root / "log"}";\n'
            'APT::Architecture "amd64";\nAcquire::Languages "none";\n'
            'Acquire::Retries "0";\nAcquire::http::Timeout "1";\n'
            'APT::Keep-Downloaded-Packages "true";\n'
            'Binary::apt-get::APT::Keep-Downloaded-Packages "true";\n'
            'DPkg::Options {\n'
            f' "--admindir={self.root / "dpkg"}";\n'
            f' "--instdir={self.root / "installed"}";\n'
            f' "--log={self.root / "dpkg.log"}";\n'
            '};\n'
        )
        # These later fragments deliberately override the early APT_CONFIG file.
        # The retry must preserve effective settings, not merely copy that file.
        (self.root / "apt.conf.d/zz-policy").write_text(
            'Acquire::Retries "1";\nAcquire::AllowInsecureRepositories "false";\n'
            'APT::Get::AllowUnauthenticated "false";\nAcquire::https::Verify-Peer "true";\n'
            'Acquire::https::Verify-Host "true";\n'
        )

    def build_installer(self):
        for name in ("install-playwright-browsers.sh", "run-playwright-install.py",
                     "playwright_apt_mirrors.py"):
            shutil.copyfile(WORKFLOWS / name, self.root / name)
        for name in ("calls",):
            (self.root / name).touch()
            (self.root / name).chmod(0o666)
        tools = self.root / "bin"
        tools.mkdir()
        self.write_tool("worker", f"#!{sys.executable}\nPACKAGES = {PACKAGES!r}\n" + APT_WORKER)
        script = self.root / "test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1"
        script.parent.mkdir(parents=True)
        script.write_text(f"& '{tools / 'worker'}' @args\nexit $LASTEXITCODE\n")
        self.write_tool("pwsh", f'#!/bin/bash\nexec "{self.pwsh}" -NoProfile -NonInteractive "$@"\n')
        self.write_tool("sudo",
                        '#!/bin/bash\n'
                        '[ "$1" = --non-interactive ] && [ "$2" = -- ] && [ "$3" = python3 ] || exit 96\n'
                        f'shift 3\nexec "{self.sudo}" -n -- "{tools / "python3"}" "$@"\n')
        self.write_tool("python3",
                        '#!/bin/bash\n[ "$2" = 120 ] && [ "$3" = 10 ] || exit 95\n'
                        'script="$1"\nshift 3\n'
                        f'export APT_CONFIG="{self.root / "apt.conf"}"\n'
                        f'exec "{sys.executable}" "$script" 4 0.3 "$@"\n')
        self.write_tool("sleep", "#!/bin/bash\nexit 0\n")
        self.env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}",
                        APT_CONFIG=str(self.root / "apt.conf"))

    def write_tool(self, name, body):
        path = self.root / "bin" / name
        path.write_text(body, encoding="utf-8", newline="\n")
        path.chmod(0o755)

    def cleanup(self):
        for server, thread in self.servers:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        # dpkg owns some fixture directories. Delete only this recorded fixture root.
        subprocess.run(
            [self.sudo, "-n", "--", sys.executable, "-c",
             "import shutil,sys; shutil.rmtree(sys.argv[1])", str(self.root)],
            check=True, timeout=5,
        )
        self.temporary.cleanup()

    def install(self, cache="true", *browsers):
        result = subprocess.run(
            ["bash", str(self.root / "install-playwright-browsers.sh"), cache,
             *(browsers or ("chromium",))],
            cwd=self.root, env=self.env, text=True, capture_output=True, timeout=25,
        )
        print(result.stdout, end="")
        print(result.stderr, end="")
        return result

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def assert_configuration_preserved(self):
        for path, data in self.original_files.items():
            self.assertEqual(data, path.read_bytes(), path)
        for call in self.calls():
            if call["config"] != str(self.root / "apt.conf"):
                self.assertFalse(Path(call["config"]).parent.exists(), call["config"])
            for policy in ('Acquire::Retries "1";',
                           'Acquire::AllowInsecureRepositories "false";',
                           'APT::Get::AllowUnauthenticated "false";',
                           'Acquire::https::Verify-Peer "true";',
                           'Acquire::https::Verify-Host "true";'):
                self.assertIn(policy, call["dump"])
            self.assertIn(f'Dir::State::status "{self.root / "dpkg/status"}";', call["dump"])
            self.assertIn(f'Dir::Cache "{self.root / "cache"}";', call["dump"])
            for name, text in call["sources"].items():
                actual = re.sub(r"mirror\+file:/[^\s]+",
                                f"mirror+file:{self.root / 'mirrors.txt'}", text)
                self.assertEqual((self.root / "sources.list.d" / name).read_text(), actual)
            self.assertEqual({"ubuntu.sources", "other.list"}, set(call["sources"]))

    def test_slow_progressing_primary_fails_over_with_cached_archives_and_full_dependencies(self):
        result = self.install("", "chromium", "firefox", "webkit")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(1, result.stdout.count("failed with exit code 124."))
        self.assertEqual([
            ["install-deps", "chromium", "firefox", "webkit"],
            ["install-deps", "chromium", "firefox", "webkit"],
            ["install", "chromium", "firefox", "webkit"],
        ], [call["args"] for call in self.calls()])
        self.assertEqual([0, 0, os.geteuid()], [call["uid"] for call in self.calls()])
        for package, data in self.payloads.items():
            self.assertEqual(data, (self.root / f"installed/usr/share/{package}/payload").read_bytes())
        self.assertEqual("chromium firefox webkit", (self.root / "browser-cache").read_text())
        slow = [r for r in self.requests if r["mirror"] == 0 and r["path"].endswith(".deb")
                and "fixture-a-tools" not in r["path"]]
        fast = [r for r in self.requests if r["mirror"] == 1
                and r["path"] in {s["path"] for s in slow}]
        self.assertTrue(any(1024 < r["bytes"] < 131072 for r in slow), slow)
        self.assertTrue(any(r["start"] > 0 for r in fast), fast)
        tools = [r for r in self.requests if r["path"].startswith("fixture-a-tools")]
        self.assertEqual(1, len(tools), tools)
        self.assert_configuration_preserved()

    def test_old_same_mirror_retry_mutation_exhausts_despite_continuous_progress(self):
        installer = self.root / "install-playwright-browsers.sh"
        source = installer.read_text()
        self.assertEqual(1, source.count("mirror_step=$((mirror_step + 1))"))
        installer.write_text(source.replace("mirror_step=$((mirror_step + 1))", "mirror_step=0"))
        result = self.install()
        self.assertEqual(124, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, result.stdout.count("failed with exit code 124."))
        self.assertEqual(3, len(self.calls()))
        downloads = [r for r in self.requests if "fixture-z" in r["path"]]
        self.assertGreaterEqual(len(downloads), 3, downloads)
        self.assertTrue(all(r["mirror"] == 0 and r["bytes"] > 0 for r in downloads), downloads)
        self.assertFalse((self.root / "installed/usr/share/fixture-z-chromium/payload").exists())
        self.assert_configuration_preserved()
        print("Old repeated mirror preference: 124/124/124 despite retained download progress (caught).")

    def test_all_configured_mirrors_slow_exhausts_three_attempts_and_cleans_overlays(self):
        self.mode = "all-slow"
        result = self.install()
        self.assertEqual(124, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, len(self.calls()))
        self.assertEqual({0, 1, 2}, {r["mirror"] for r in self.requests if "fixture-z" in r["path"]})
        self.assertIn("failed after three bounded attempts", result.stdout)
        self.assert_configuration_preserved()

    def test_changed_release_signature_is_rejected_without_installing(self):
        self.mode = "signature"
        result = self.install()
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertRegex(result.stdout + result.stderr, r"BADSIG|invalid signature")
        self.assertFalse((self.root / "installed/usr/share/fixture-z-chromium/payload").exists())
        self.assert_configuration_preserved()

    def test_changed_package_hash_is_rejected_without_installing(self):
        self.mode = "hash"
        result = self.install()
        output = result.stdout + result.stderr
        self.assertNotEqual(0, result.returncode, output)
        tampered = [r for r in self.requests if r["mirror"] > 0 and "fixture-z" in r["path"] and r["bytes"] > 0]
        self.assertTrue(tampered, "precondition: a mirror served the tampered archive")
        # apt 2 reports the rejection as "Hash Sum mismatch"; apt 3 (ubuntu-26.04)
        # ignores the tampered mirror's copy and moves to the next mirror. Either
        # way the rejection must name a tampering mirror, never the clean one.
        ports = "|".join(str(server.server_port) for server, _ in self.servers[1:])
        self.assertRegex(output,
                         rf"Hash Sum mismatch|Ign:\d+ http://127\.0\.0\.1:(?:{ports}) \S+ \S+ fixture-z-chromium ")
        self.assertFalse((self.root / "installed/usr/share/fixture-z-chromium/payload").exists())
        self.assert_configuration_preserved()

    def test_unsupported_configuration_fails_closed_without_touching_sources(self):
        # Direct sources have no configured mirror candidates to advance through.
        source = self.root / "sources.list.d/ubuntu.sources"
        source.write_text(source.read_text().replace(
            f"mirror+file:{self.root / 'mirrors.txt'}",
            f"http://127.0.0.1:{self.servers[0][0].server_port}/",
        ))
        self.original_files[source] = source.read_bytes()
        result = self.install()
        self.assertEqual(78, result.returncode, result.stdout + result.stderr)
        self.assertEqual(1, len(self.calls()))
        self.assertIn("no supported configured mirror list", result.stdout)
        self.assert_configuration_preserved()


if __name__ == "__main__":
    unittest.main(verbosity=2)
