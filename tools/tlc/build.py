import argparse
import hashlib
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path


def sha(data):
    return hashlib.sha256(data).hexdigest().upper()


parser = argparse.ArgumentParser(description="Build and regression-check the pinned TLC checkpoint-order overlay.")
parser.add_argument("--java-home", type=Path, required=True)
parser.add_argument("--jar", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--source", type=Path, help="Optional offline upstream source, subject to the same exact hash.")
args = parser.parse_args()
bundle = Path(__file__).resolve().parent
metadata = json.loads((bundle / "patch.json").read_text())
jar = args.jar.resolve(strict=True)
if sha(jar.read_bytes()) != metadata["originalJarSha256"]:
    raise ValueError("Original jar differs from reviewed pin.")
for name, digest in metadata["fixtureSha256"].items():
    if sha((bundle / name).read_bytes()) != digest:
        raise ValueError(f"Reviewed regression fixture differs: {name}")
if args.source:
    original = args.source.read_bytes()
else:
    with urllib.request.urlopen(metadata["upstreamSourceUrl"], timeout=60) as response:
        original = response.read()
if sha(original) != metadata["originalSourceSha256"]:
    raise ValueError("Upstream source differs from reviewed pin.")
old = metadata["oldCheckpoint"]
new = metadata["newCheckpoint"]
text = original.decode("utf-8")
if text.count(old) != 1:
    raise ValueError("Expected exactly one reviewed checkpoint method.")
patched = text.replace(old, new).encode("utf-8")
if sha(patched) != metadata["patchedSourceSha256"]:
    raise ValueError("Patched source differs from reviewed pin.")
output = args.output.resolve()
output.mkdir()
source = output / "ModelChecker.java"
source.write_bytes(patched)
classes = output / "compiled"
probes = output / "probe-classes"
classes.mkdir()
probes.mkdir()
executable_suffix = ".exe" if os.name == "nt" else ""
java = args.java_home.resolve() / "bin" / f"java{executable_suffix}"
javac = args.java_home.resolve() / "bin" / f"javac{executable_suffix}"
results = {}


def run(name, arguments, expected, timeout=120):
    result = subprocess.run([str(argument) for argument in arguments], cwd=output,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    for suffix, data in (("stdout", result.stdout), ("stderr", result.stderr)):
        with (output / f"{name}.{suffix}.log").open("xb") as stream:
            stream.write(data)
    results[name] = {"arguments": [str(argument) for argument in arguments], "native": result.returncode}
    print(name, result.returncode, flush=True)
    if result.returncode != expected:
        raise RuntimeError(f"{name}: expected {expected}, got {result.returncode}; retained logs: {output}")
    return result


run("java-version", [java, "-version"], 0)
run("javac-version", [javac, "-version"], 0)
run("compile-probe", [javac, "-J-Xmx128m", "-J-XX:ActiveProcessorCount=1", "-cp", jar,
                      "-d", probes, bundle / "CheckpointOrderProbe.java"], 0)
model = bundle / "WalGcStarvationDrive"
before = run("before", [java, "-Xmx256m", "-XX:ActiveProcessorCount=1",
                        "-cp", os.pathsep.join(map(str, (probes, jar))),
                        "tlc2.tool.CheckpointOrderProbe", output / "before-execution", model], 1)
if (b"CHECKPOINT_CAPTURED_NODES=2 LIVE_NODES=2" not in before.stdout or
        b"LIVENESS CHECKPOINT INCLUDED POST-RESUME WORK" not in before.stderr):
    raise AssertionError("Original method did not reproduce the reviewed mixed-cut failure.")
run("compile-patch", [javac, "-J-Xmx128m", "-J-XX:ActiveProcessorCount=1", "-cp", jar,
                      "-d", classes, source], 0)
class_pins = {path.relative_to(classes).as_posix(): sha(path.read_bytes())
              for path in sorted(classes.rglob("*.class"))}
if class_pins != metadata["compiledClassSha256"]:
    raise AssertionError("Compiled overlay differs from reviewed class pins; retain output for review.")
after = run("after", [java, "-Xmx256m", "-XX:ActiveProcessorCount=1",
                      "-cp", os.pathsep.join(map(str, (classes, probes, jar))),
                      "tlc2.tool.CheckpointOrderProbe", output / "after-execution", model], 0)
for marker in (b"CHECKPOINT_CAPTURED_NODES=1 LIVE_NODES=2",
               b"FAILED_BEGIN_RESUMES_WITHOUT_COMMIT", b"ACTUAL_CHECKPOINT_CUT_REGRESSION_PASSED"):
    if marker not in after.stdout:
        raise AssertionError(f"Missing patched-method regression marker: {marker!r}")
smoke = run("tiny-all17", [java, "-Xmx256m", "-XX:ActiveProcessorCount=1",
                          "-Xlog:class+load=info:file=class-load.log",
                          "-cp", os.pathsep.join(map(str, (classes, jar))), "tlc2.TLC",
                          "-workers", "2", "-lncheck", "seqfinal",
                          "-fp", "130", "-seed", "980653360040586054",
                          "-config", bundle / "WalGcStarvationDrive.tiny.cfg",
                          "-metadir", output / "tiny-states", model], 0)
smoke_text = smoke.stdout.decode()
statistics = re.findall(r"(?m)^(\d+) states generated, (\d+) distinct states found, (\d+) states left on queue\.\r?$",
                        smoke_text)
if (statistics != [("37148", "7902", "0")] or
        smoke_text.count("Model checking completed. No error has been found.") != 1 or
        "Finished checking temporal properties" not in smoke_text):
    raise AssertionError("Tiny all17 smoke differs from the reviewed result.")
loads = [line for line in (output / "class-load.log").read_text().splitlines()
         if " tlc2.tool.ModelChecker source:" in line]
if len(loads) != 1:
    raise AssertionError("Expected one actual ModelChecker class-load record.")
loaded_uri = urllib.parse.urlparse(loads[0].split(" source: ", 1)[1])
expected_uri = urllib.parse.urlparse(classes.as_uri())
if loaded_uri.scheme != "file" or loaded_uri.path.rstrip("/") != expected_uri.path:
    raise AssertionError("Actual ModelChecker did not load from this built overlay.")
if sha(jar.read_bytes()) != metadata["originalJarSha256"]:
    raise AssertionError("Original shared jar changed.")
with (output / "verified-toolchain.json").open("x", encoding="utf-8") as stream:
    json.dump({
        "qualification": "Actual method before/after plus tiny all17 only; not full-bound or recovery proof.",
        "upstreamRevision": metadata["upstreamRevision"],
        "upstreamSourceUrl": metadata["upstreamSourceUrl"],
        "originalSourceSha256": sha(original),
        "patchedSourceSha256": sha(patched),
        "originalJarSha256": sha(jar.read_bytes()),
        "compiledClassSha256": class_pins,
        "actualModelCheckerClassLoad": loads[0],
        "statistics": statistics,
        "executions": results,
    }, stream, indent=2)
print("PINNED_CHECKPOINT_TOOLCHAIN_REGRESSION_AND_TINY_SMOKE_PASSED", flush=True)
