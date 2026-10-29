# Pinned TLC checkpoint-order repair (OWNER166)

The unchanged `tla2tools` v1.7.4 jar resumes workers before its intern-table and
liveness `beginChkpt` snapshots. A worker can therefore extend the saved liveness
graph after the queue/trace/fingerprint cut, producing an inconsistent recovery
image. `patch.json` pins the exact upstream revision, source bytes, original jar,
one-method replacement, and all four compiled overlay classes.

The only source change moves `resumeAll` into `finally` after **all** begin
snapshots. Failed begins still release workers and never reach commits.
Assertions, properties, search, worker count and liveness strategy are unchanged.
The original jar is never rewritten. Upstream source is fetched from its immutable
GitHub revision; no upstream source tree or session binary is vendored.

## Build and use

Use Python 3 and the JDK (including `javac`, not just a JRE) that
`toolchain.json` pins. Download the jar `toolchain.json` names to
`tools/tla2tools.jar` and check it against the SHA-256 recorded there.
From the repository root on PowerShell:

```powershell
python tools\tlc\build.py --java-home "$env:JAVA_HOME" --jar tools\tla2tools.jar --output tools\tlc\build
```

On Linux:

```bash
python3 tools/tlc/build.py --java-home "$JAVA_HOME" --jar tools/tla2tools.jar --output tools/tlc/build
```

The output must be a **new directory**. For a rebuild, preserve the previous
evidence elsewhere and remove only `tools/tlc/build`, or use a fresh output path.
`--source` accepts an offline upstream `ModelChecker.java`, under the same byte
hash check. Compilation is serial, with a 128 MiB heap; each regression JVM has
a 256 MiB heap. Outputs, logs, downloaded source and provenance are gitignored
under `tools/tlc/build`. All checked-in inputs have LF checkout attributes, so
Windows and Linux clean clones preserve their byte contracts.

The NUnit harness always puts `tools/tlc/build/compiled` **before** the original
jar on the JVM classpath. It rejects a missing verification receipt, a different
jar, missing/extra overlay files, or mismatched class hashes rather than silently
using the broken original. Missing Java/jar retains the existing local skip / CI
failure policy; once those exist, an absent or invalid repair fails locally too.
`TLA_TOOLS_JAR` can still select the original pinned jar at another absolute path.
CI provisions all of this through one composite action,
`.github/actions/tla-toolchain`. `toolchain.json` is the single source of the
jar URL, its SHA-256, and the Java distribution and version; no workflow, and
not the action itself, carries a copy. The action restores the overlay from the
Actions cache, keyed on `toolchain.json`, `patch.json`, `build.py`, the
regression fixtures and the resolved `java`/`javac` versions, and runs this
builder only on a miss, so a warm leg makes no request for the upstream source.
Either way it then runs `toolchain.py verify`, which holds the receipt, the
jar and the exact overlay class set to the pins and fails the leg on any
mismatch.

For manual TLC invocations, use the same classpath order (semicolon on Windows,
colon on Linux):

```powershell
java -cp "C:\repo\tools\tlc\build\compiled;C:\repo\tools\tla2tools.jar" tlc2.TLC -config Model.cfg Model.tla
```

## Executable evidence and limits

`CheckpointOrderProbe.java` calls the **real** `ModelChecker.checkpoint` against
its real disk-backed liveness graph. A controlled queue proxy writes a second
graph node synchronously when workers resume. The identical probe must fail on
the original jar (saved/live nodes 2/2) and pass on the overlay (1/2). It also
injects a begin `IOException` and requires worker release with no committed queue.
The builder rejects an unexpected exit code or missing diagnostic marker.

The frozen `WalGcStarvationDrive.*` files are **tooling-only regression inputs**,
not an adopted scheduling specification, not manifest-listed, and not a replacement
for any full GC proof. Only the tiny configuration runs here: all 17 properties,
37,148 generated / 7,902 distinct states / zero queued, with the pinned seed and
fingerprint. A JVM class-load log must show that `ModelChecker` actually came
from the newly built overlay. `verified-toolchain.json` records hashes, commands,
exit codes, counts and that provenance only after every check passes.

This qualifies the repaired toolchain, **not** full-bound GC correctness or a
recovery proof. Previously rejected mixed-cut checkpoints remain rejected; do
not use this overlay to rehabilitate an old checkpoint.
