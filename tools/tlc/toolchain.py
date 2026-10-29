#!/usr/bin/env python3
"""The single reader of tools/tlc/toolchain.json for the tla-toolchain action.

toolchain.json (contract C3 of #5148) is the one place the TLA+ toolchain is
pinned: the tla2tools.jar release URL and its SHA-256, and the Java
distribution and version. The composite action under
.github/actions/tla-toolchain reads its pins through this script, so no
workflow, and not the action itself, carries a copy that could drift.

Subcommands:

  pins        Validate toolchain.json and print its pins as GITHUB_OUTPUT lines.
  cache-key   Print the checkpoint-overlay cache key for the resolved JDK.
  verify      Verify a built overlay's receipt and classes against patch.json.
              Fails closed: any mismatch exits non-zero, whether the overlay
              was just built or restored from the cache.

Run with --self-test to check the validation, key and verification rules
against synthetic bundles plus the repository's own pins.
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BUNDLE = Path(__file__).resolve().parent
KEYS = ("jarUrl", "jarSha256", "javaDistribution", "javaVersion")
JAR_URL = re.compile(
    r"https://github\.com/tlaplus/tlaplus/releases/download/v[0-9][0-9A-Za-z.\-]*/tla2tools\.jar")
SHA256 = re.compile(r"[0-9a-f]{64}")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._\-]*")
RECEIPT = "verified-toolchain.json"
RECEIPT_PINS = ("originalJarSha256", "originalSourceSha256", "patchedSourceSha256")
# Bump to invalidate every cached overlay without touching a pinned input.
CACHE_KEY_VERSION = "v1"


class ToolchainError(Exception):
    """A pin, an input or a built overlay disagrees with what was reviewed."""


def sha(data):
    return hashlib.sha256(data).hexdigest().upper()


def read_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ToolchainError(f"{path}: unreadable: {error}") from error


def load_pins(bundle=BUNDLE):
    """Return the validated pins. Every field is checked; nothing is defaulted."""
    path = bundle / "toolchain.json"
    pins = read_json(path)
    if not isinstance(pins, dict) or sorted(pins) != sorted(KEYS):
        raise ToolchainError(f"{path}: expected exactly the keys {', '.join(KEYS)}")
    for key in KEYS:
        if not isinstance(pins[key], str):
            raise ToolchainError(f"{path}: {key} must be a string")
    if not JAR_URL.fullmatch(pins["jarUrl"]):
        raise ToolchainError(f"{path}: jarUrl must be a tlaplus GitHub release URL for tla2tools.jar")
    if not SHA256.fullmatch(pins["jarSha256"]):
        raise ToolchainError(f"{path}: jarSha256 must be 64 lower-case hex digits")
    for key in ("javaDistribution", "javaVersion"):
        if not TOKEN.fullmatch(pins[key]):
            raise ToolchainError(f"{path}: {key} must be a single token")
    patch = read_json(bundle / "patch.json")
    if patch.get("originalJarSha256") != pins["jarSha256"].upper():
        raise ToolchainError(
            f"{path}: jarSha256 disagrees with patch.json originalJarSha256, the jar the overlay was reviewed against")
    return pins


def output_lines(pins):
    return [
        f"jar-url={pins['jarUrl']}",
        f"jar-sha256={pins['jarSha256']}",
        f"java-distribution={pins['javaDistribution']}",
        f"java-version={pins['javaVersion']}",
    ]


def cache_inputs(bundle):
    """Every checked-in file the overlay build reads, as repository-relative names."""
    patch = read_json(bundle / "patch.json")
    return sorted({"toolchain.json", "patch.json", "build.py", *patch.get("fixtureSha256", {})})


def cache_key(bundle, java_versions, runner_os, runner_arch):
    """Key on every build input plus the resolved java/javac versions.

    The overlay is compiled by the resolved JDK, and a patch-level JDK change
    is invisible to every checked-in file, so the version output is part of
    the key: a new JDK rebuilds (and regression-checks) the overlay.
    """
    digest = hashlib.sha256()
    for name in cache_inputs(bundle):
        digest.update(name.encode("utf-8") + b"\0" + sha((bundle / name).read_bytes()).encode("ascii") + b"\n")
    digest.update(b"java\0" + java_versions.encode("utf-8"))
    return f"tla-overlay-{CACHE_KEY_VERSION}-{runner_os}-{runner_arch}-{digest.hexdigest()}"


def java_versions(java_home):
    suffix = ".exe" if os.name == "nt" else ""
    lines = []
    for tool in ("java", "javac"):
        result = subprocess.run([str(java_home / "bin" / f"{tool}{suffix}"), "-version"],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        if result.returncode != 0:
            raise ToolchainError(f"{tool} -version exited {result.returncode}")
        lines.append(result.stdout.decode("utf-8", "replace").replace("\r\n", "\n"))
    return "".join(lines)


def verify(bundle, build, jar):
    """Fail closed unless the overlay is exactly the reviewed one.

    Mirrors the NUnit harness's own check (test/lattice/Formal/
    TlcCheckpointToolchain.cs) so a mismatch fails the provisioning step,
    before any test runs, rather than surfacing later as a fixture failure.
    """
    patch = read_json(bundle / "patch.json")
    expected = patch.get("compiledClassSha256")
    if not isinstance(expected, dict) or not expected:
        raise ToolchainError("patch.json pins no compiled overlay classes")
    receipt_path = build / RECEIPT
    classes = build / "compiled"
    if not receipt_path.is_file() or not classes.is_dir():
        raise ToolchainError(f"overlay receipt or classes missing under {build}")
    receipt = read_json(receipt_path)
    for name in RECEIPT_PINS:
        if receipt.get(name) != patch.get(name):
            raise ToolchainError(f"overlay receipt differs from patch.json {name}")
    if receipt.get("compiledClassSha256") != expected:
        raise ToolchainError("overlay receipt differs from patch.json compiledClassSha256")
    if not jar.is_file() or sha(jar.read_bytes()) != patch.get("originalJarSha256"):
        raise ToolchainError(f"{jar} differs from patch.json originalJarSha256")
    actual = {path.relative_to(classes).as_posix(): path for path in classes.rglob("*") if path.is_file()}
    if sorted(actual) != sorted(expected):
        raise ToolchainError(
            f"overlay class set differs from its pin: extra {sorted(set(actual) - set(expected))}, "
            f"missing {sorted(set(expected) - set(actual))}")
    for name, digest in expected.items():
        if sha(actual[name].read_bytes()) != digest:
            raise ToolchainError(f"overlay class {name} differs from its pinned SHA-256")


class SelfTests(unittest.TestCase):
    JAR = b"jar bytes"
    CLASSES = {"tlc2/tool/A.class": b"class a", "tlc2/tool/A$1.class": b"class a1"}

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.bundle / "build.py").write_bytes(b"# builder\n")
        (self.bundle / "Probe.java").write_bytes(b"class Probe {}\n")
        self.patch = {
            "originalSourceSha256": sha(b"source"),
            "patchedSourceSha256": sha(b"patched"),
            "originalJarSha256": sha(self.JAR),
            "fixtureSha256": {"Probe.java": sha(b"class Probe {}\n")},
            "compiledClassSha256": {name: sha(data) for name, data in self.CLASSES.items()},
        }
        self.write_patch()
        self.pins = {
            "jarUrl": "https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar",
            "jarSha256": sha(self.JAR).lower(),
            "javaDistribution": "temurin",
            "javaVersion": "17",
        }
        self.write_pins()
        self.jar = self.root / "tla2tools.jar"
        self.jar.write_bytes(self.JAR)
        self.build = self.root / "build"
        for name, data in self.CLASSES.items():
            path = self.build / "compiled" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        self.receipt = {name: self.patch[name] for name in RECEIPT_PINS}
        self.receipt["compiledClassSha256"] = dict(self.patch["compiledClassSha256"])
        self.write_receipt()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.root, ignore_errors=True)

    def write_patch(self):
        (self.bundle / "patch.json").write_text(json.dumps(self.patch), encoding="utf-8")

    def write_pins(self, pins=None):
        (self.bundle / "toolchain.json").write_text(json.dumps(pins or self.pins), encoding="utf-8")

    def write_receipt(self):
        (self.build / RECEIPT).write_text(json.dumps(self.receipt), encoding="utf-8")

    def test_repository_pins_are_well_formed_and_agree_with_patch_json(self):
        pins = load_pins(BUNDLE)
        self.assertEqual(len(output_lines(pins)), 4)
        self.assertTrue(set(cache_inputs(BUNDLE)) >= {"toolchain.json", "patch.json", "build.py"})
        for name in cache_inputs(BUNDLE):
            self.assertTrue((BUNDLE / name).is_file(), name)

    def test_pins_are_emitted_as_github_outputs(self):
        self.assertEqual(output_lines(load_pins(self.bundle)), [
            "jar-url=https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar",
            f"jar-sha256={sha(self.JAR).lower()}",
            "java-distribution=temurin",
            "java-version=17",
        ])

    def test_malformed_pins_are_refused(self):
        cases = {
            "missing key": {k: v for k, v in self.pins.items() if k != "javaVersion"},
            "extra key": {**self.pins, "jarSha512": "x"},
            "upper-case digest": {**self.pins, "jarSha256": self.pins["jarSha256"].upper()},
            "short digest": {**self.pins, "jarSha256": self.pins["jarSha256"][:-1]},
            "plain http": {**self.pins, "jarUrl": self.pins["jarUrl"].replace("https", "http")},
            "other host": {**self.pins, "jarUrl": self.pins["jarUrl"].replace("github.com", "example.com")},
            "other asset": {**self.pins, "jarUrl": self.pins["jarUrl"].replace("tla2tools.jar", "x.jar")},
            "non-string": {**self.pins, "javaVersion": 17},
            "version with spaces": {**self.pins, "javaVersion": "17 || true"},
            "disagrees with patch.json": {**self.pins, "jarSha256": "a" * 64},
        }
        for name, pins in cases.items():
            with self.subTest(name):
                self.write_pins(pins)
                with self.assertRaises(ToolchainError):
                    load_pins(self.bundle)

    def test_cache_key_covers_every_build_input_and_the_jdk(self):
        base = cache_key(self.bundle, "openjdk 17.0.12", "Linux", "X64")
        self.assertEqual(base, cache_key(self.bundle, "openjdk 17.0.12", "Linux", "X64"))
        self.assertTrue(base.startswith(f"tla-overlay-{CACHE_KEY_VERSION}-Linux-X64-"))
        self.assertNotEqual(base, cache_key(self.bundle, "openjdk 17.0.13", "Linux", "X64"))
        self.assertNotEqual(base, cache_key(self.bundle, "openjdk 17.0.12", "Linux", "ARM64"))
        self.assertEqual(cache_inputs(self.bundle), ["Probe.java", "build.py", "patch.json", "toolchain.json"])
        for name in cache_inputs(self.bundle):
            with self.subTest(name):
                path = self.bundle / name
                original = path.read_bytes()
                path.write_bytes(original + b" ")
                try:
                    self.assertNotEqual(base, cache_key(self.bundle, "openjdk 17.0.12", "Linux", "X64"))
                finally:
                    path.write_bytes(original)

    def test_a_consistent_overlay_verifies(self):
        verify(self.bundle, self.build, self.jar)

    def test_every_overlay_mismatch_fails_closed(self):
        def class_changed():
            (self.build / "compiled" / "tlc2/tool/A.class").write_bytes(b"tampered")

        def class_extra():
            (self.build / "compiled" / "tlc2/tool/B.class").write_bytes(b"extra")

        def class_missing():
            (self.build / "compiled" / "tlc2/tool/A$1.class").unlink()

        def receipt_missing():
            (self.build / RECEIPT).unlink()

        def compiled_missing():
            import shutil
            shutil.rmtree(self.build / "compiled")

        def receipt_field(name):
            def mutate():
                self.receipt[name] = "0" * 64
                self.write_receipt()
            return mutate

        def receipt_classes():
            self.receipt["compiledClassSha256"]["tlc2/tool/A.class"] = "0" * 64
            self.write_receipt()

        def receipt_unreadable():
            (self.build / RECEIPT).write_text("{", encoding="utf-8")

        def jar_changed():
            self.jar.write_bytes(b"other jar")

        def jar_missing():
            self.jar.unlink()

        def patch_pins_nothing():
            self.patch["compiledClassSha256"] = {}
            self.write_patch()

        cases = {
            "class changed": class_changed, "class extra": class_extra, "class missing": class_missing,
            "receipt missing": receipt_missing, "compiled missing": compiled_missing,
            "receipt classes": receipt_classes, "receipt unreadable": receipt_unreadable,
            "jar changed": jar_changed, "jar missing": jar_missing, "patch pins nothing": patch_pins_nothing,
            **{f"receipt {name}": receipt_field(name) for name in RECEIPT_PINS},
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                self.tearDown()
                self.setUp()
                mutate()
                with self.assertRaises(ToolchainError):
                    verify(self.bundle, self.build, self.jar)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--self-test", action="store_true")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("pins")
    key = commands.add_parser("cache-key")
    key.add_argument("--java-home", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("--jar", type=Path, required=True)
    check.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    if args.self_test:
        unittest.main(argv=["toolchain"], exit=True)
    try:
        if args.command == "pins":
            print("\n".join(output_lines(load_pins())))
        elif args.command == "cache-key":
            load_pins()
            versions = java_versions(args.java_home)
            print(versions, file=sys.stderr, end="")
            print("key=" + cache_key(BUNDLE, versions, os.environ.get("RUNNER_OS", "local"),
                                     os.environ.get("RUNNER_ARCH", "local")))
        elif args.command == "verify":
            load_pins()
            verify(BUNDLE, args.build.resolve(), args.jar.resolve())
            print("PINNED_CHECKPOINT_OVERLAY_RECEIPT_VERIFIED")
        else:
            parser.error("a subcommand or --self-test is required")
    except ToolchainError as error:
        print(f"::error::{error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
