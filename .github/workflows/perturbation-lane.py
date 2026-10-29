#!/usr/bin/env python3
"""Plan and summarise the weekly production-perturbation lane (#5163).

`.github/workflows/formal-perturbations.yml` runs every production perturbation
(contract C7, epic #5148) through `tools/perturbations/run-perturbations.py`.
This script is the lane's bookkeeping, kept out of the workflow so it can be
tested:

    plan       Select the perturbations (every one, or the areas and files a
               dispatch names), split them round-robin into shards, and decide
               whether the run maintains the tracking issue. Writes `matrix`,
               `shards`, `count`, `full` and `track` to GITHUB_OUTPUT.
    record     Write `shard.json` beside a shard's result: the shard number,
               the files it was given and the runner's exit code, so a shard
               whose runner died before writing a result is still accounted for.
    summarise  Read every shard's `shard.json`, `result.json` and logs, write
               the job summary (per-perturbation verdicts and durations, and
               each shard's control), a combined `report.json` and, when the
               run is red, `findings.md` for the tracking issue. Writes
               `verdict=green|red` to GITHUB_OUTPUT.

A run is green only when every planned shard produced a result, every control
passed, every planned perturbation was run, and every one was detected. A run
that selected nothing is red, never vacuously green.

Discovery is the runner's own `discover`, loaded from
`tools/perturbations/run-perturbations.py`, so the lane and the runner cannot
disagree about which files exist.

    python3 .github/workflows/perturbation-lane.py --self-test
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

RUNNER = Path("tools") / "perturbations" / "run-perturbations.py"
EXTENSION = ".perturbation"
DEFAULT_SHARDS = 1
MAX_SHARDS = 8
INFRASTRUCTURE_MARKERS = ("MSB4166",)
TITLE = "Weekly production-perturbation lane failing"


class LaneError(Exception):
    """The selection or the lane's inputs are unusable."""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_runner(root: Path):
    path = root / RUNNER
    spec = importlib.util.spec_from_file_location("run_perturbations", path)
    if spec is None or spec.loader is None:
        raise LaneError(f"cannot load the perturbation runner at {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------

def select(root: Path, selection: str, discover) -> tuple[list[str], bool]:
    """Return (repository-relative perturbation paths, whether the selection is every perturbation)."""
    tokens = selection.replace(",", " ").split()
    found: list[Path] = []
    if not tokens:
        found = list(discover(root, None))
    for token in tokens:
        if token.endswith(EXTENSION):
            path = (root / token).resolve()
            if not path.is_file():
                raise LaneError(f"the selected perturbation '{token}' does not exist")
            found.append(path)
        else:
            area = list(discover(root, [token]))
            if not area:
                raise LaneError(f"the selected area '{token}' has no spec/{token}/perturbations/*{EXTENSION}")
            found.extend(area)
    files: list[str] = []
    for path in found:
        relative = path.resolve().relative_to(root.resolve()).as_posix()
        if relative not in files:
            files.append(relative)
    if not files:
        raise LaneError("no perturbation was selected, and a lane that runs nothing must not report green")
    everything = sorted(p.resolve().relative_to(root.resolve()).as_posix() for p in discover(root, None))
    return files, sorted(files) == everything


def shard(files: list[str], shards: int) -> list[list[str]]:
    count = max(1, min(shards, len(files)))
    groups: list[list[str]] = [[] for _ in range(count)]
    for index, file in enumerate(files):
        groups[index % count].append(file)
    return groups


def parse_shards(text: str) -> int:
    if not text.strip():
        return DEFAULT_SHARDS
    try:
        value = int(float(text))
    except ValueError as error:
        raise LaneError(f"shards must be a whole number, not '{text}'") from error
    if not 1 <= value <= MAX_SHARDS:
        raise LaneError(f"shards must be between 1 and {MAX_SHARDS}, not {value}")
    return value


def should_track(event: str, ref_name: str, default_branch: str, full: bool, requested: bool) -> bool:
    """Whether this run opens, updates or closes the tracking issue.

    The schedule always does. A dispatch on the default branch does when it ran
    every perturbation, because only a full run can say the lane is green. A
    dispatch anywhere does when it asks to, which is how a branch validates the
    tracking path before it merges.
    """
    if event == "schedule":
        return True
    if requested:
        return True
    return event == "workflow_dispatch" and bool(default_branch) and ref_name == default_branch and full


def plan(root: Path, selection: str, shards_text: str, event: str, ref_name: str, default_branch: str,
         track_requested: bool, discover) -> dict:
    files, full = select(root, selection, discover)
    groups = shard(files, parse_shards(shards_text))
    matrix = {"include": [{"shard": str(n), "files": " ".join(g)} for n, g in enumerate(groups, start=1)]}
    return {
        "matrix": matrix,
        "shards": len(groups),
        "count": len(files),
        "full": full,
        "track": should_track(event, ref_name, default_branch, full, track_requested),
    }


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

def record(shard_number: str, files: str, exit_code: int) -> dict:
    return {"shard": int(shard_number), "files": files.split(), "exitCode": exit_code}


# ---------------------------------------------------------------------------
# summarise
# ---------------------------------------------------------------------------

def failed_detectors(detectors: list[str], failed_tests: list[str]) -> list[str]:
    """Map failed test display names back to the detectors that own them."""
    names = set()
    for test in failed_tests:
        head = test.split("(", 1)[0]
        names.add(head.rsplit(".", 1)[-1])
    return [d for d in detectors if d.split(".", 1)[1] in names]


def infrastructure(logs: Path, name: str) -> str:
    log = logs / f"{name}.log"
    if not log.is_file():
        return ""
    text = log.read_text(encoding="utf-8", errors="replace")
    return next((m for m in INFRASTRUCTURE_MARKERS if m in text), "")


def cell(text: object) -> str:
    return str(text).replace("|", "\\|").replace("\r", " ").replace("\n", " ").strip()


def summarise(shards_dir: Path, expect: int) -> dict:
    """Collect every shard into one report with a verdict and a list of findings."""
    present: dict[int, Path] = {}
    findings: list[str] = []
    for marker in sorted(shards_dir.rglob("shard.json")):
        try:
            number = int(json.loads(marker.read_text(encoding="utf-8"))["shard"])
        except (ValueError, KeyError, TypeError) as error:
            findings.append(f"{marker} is not a shard record: {error}")
            continue
        if number in present:
            findings.append(f"shard {number} was recorded twice ({present[number]} and {marker.parent})")
            continue
        present[number] = marker.parent

    shards: list[dict] = []
    perturbations: list[dict] = []
    for number in range(1, expect + 1):
        directory = present.get(number)
        if directory is None:
            findings.append(f"shard {number} uploaded no record; its job did not finish")
            shards.append({"shard": number, "verdict": "missing", "control": None, "files": []})
            continue
        meta = json.loads((directory / "shard.json").read_text(encoding="utf-8"))
        planned = list(meta.get("files", []))
        entry = {"shard": number, "files": planned, "exitCode": meta.get("exitCode"), "control": None,
                 "ref": None, "verdict": "ran"}
        shards.append(entry)
        result_path = directory / "result.json"
        if not result_path.is_file():
            entry["verdict"] = "no-result"
            findings.append(f"shard {number}: the runner exited {meta.get('exitCode')} without a result; "
                            "see its logs artifact")
            for file in planned:
                perturbations.append({"shard": number, "name": Path(file).stem, "path": file, "module": "",
                                      "row": "", "detectors": [], "verdict": "not-run", "failedDetectors": [],
                                      "failedTests": [], "message": "the shard produced no result",
                                      "seconds": 0.0, "infrastructure": ""})
            continue
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if result.get("schema") != 1:
            findings.append(f"shard {number}: unsupported result schema {result.get('schema')!r}")
        entry["ref"] = result.get("ref")
        control = result.get("control") or {}
        entry["control"] = control
        if control.get("verdict") != "passed":
            findings.append(f"shard {number}: control {control.get('verdict', 'missing')}: "
                            f"{control.get('message') or 'no message'}")
        ran = []
        for item in result.get("perturbations", []):
            detectors = list(item.get("detectors", []))
            failed = list(item.get("failedTests", []))
            verdict = item.get("verdict", "")
            perturbations.append({
                "shard": number,
                "name": item.get("name", ""),
                "path": item.get("path", ""),
                "module": item.get("module", ""),
                "row": item.get("row", ""),
                "detectors": detectors,
                "verdict": verdict,
                "failedDetectors": failed_detectors(detectors, failed),
                "failedTests": failed,
                "message": item.get("message", ""),
                "seconds": item.get("seconds", 0.0),
                "infrastructure": infrastructure(directory / "logs", item.get("name", ""))
                if verdict == "build-failed" else "",
            })
            ran.append(item.get("path", ""))
        missing = [f for f in planned if f not in ran]
        extra = [f for f in ran if f not in planned]
        if missing or extra:
            findings.append(f"shard {number}: ran {sorted(ran)} but was planned {sorted(planned)}")
        for file in missing:
            perturbations.append({"shard": number, "name": Path(file).stem, "path": file, "module": "",
                                  "row": "", "detectors": [], "verdict": "not-run", "failedDetectors": [],
                                  "failedTests": [], "message": "planned for this shard but absent from its result",
                                  "seconds": 0.0, "infrastructure": ""})

    undetected = [p for p in perturbations if p["verdict"] != "detected"]
    if not perturbations:
        findings.append("no perturbation ran, and a lane that checked nothing is not green")
    green = not findings and not undetected and bool(perturbations)
    return {"schema": 1, "verdict": "green" if green else "red", "expectedShards": expect, "shards": shards,
            "perturbations": perturbations, "findings": findings}


def summary_markdown(report: dict, ref_name: str, sha: str) -> str:
    perturbations = report["perturbations"]
    detected = sum(1 for p in perturbations if p["verdict"] == "detected")
    lines = ["## Production perturbations", "",
             f"**Verdict: {report['verdict']}** - {detected} of {len(perturbations)} perturbations detected "
             f"across {report['expectedShards']} shard(s) on `{cell(ref_name)}` at `{cell(sha)}`.", ""]
    if report["findings"]:
        lines += ["### Findings", ""] + [f"- {cell(f)}" for f in report["findings"]] + [""]
    lines += ["### Controls (unperturbed tree)", "",
              "| Shard | Verdict | Seconds | Detectors green | Message |",
              "|---|---|---|---|---|"]
    for entry in report["shards"]:
        control = entry.get("control")
        if not control:
            lines.append(f"| {entry['shard']} | {entry['verdict']} | - | - | - |")
            continue
        counts = control.get("detectors", {})
        green = sum(1 for c in counts.values() if c.get("passed") and not c.get("failed") and not c.get("other"))
        lines.append(f"| {entry['shard']} | {cell(control.get('verdict', ''))} | {control.get('seconds', 0)} | "
                     f"{green} of {len(counts)} | {cell(control.get('message', ''))} |")
    lines += ["", "### Perturbations", "",
              "| Shard | Perturbation | Module | Row | Verdict | Failed detectors | Seconds | Message |",
              "|---|---|---|---|---|---|---|---|"]
    for p in perturbations:
        verdict = p["verdict"] + (f" ({p['infrastructure']}: infrastructure, re-run)" if p["infrastructure"] else "")
        lines.append(f"| {p['shard']} | `{cell(p['name'])}` | {cell(p['module'])} | {cell(p['row'])} | "
                     f"{cell(verdict)} | {cell(', '.join(p['failedDetectors']) or '-')} | {p['seconds']} | "
                     f"{cell(p['message'])} |")
    total = round(sum(float(p["seconds"] or 0) for p in perturbations)
                  + sum(float((e.get("control") or {}).get("seconds", 0) or 0) for e in report["shards"]), 1)
    lines += ["", f"Total measured: {total} s (controls plus perturbations, summed over shards).", ""]
    return "\n".join(lines)


def findings_markdown(report: dict, ref_name: str, sha: str, run_url: str) -> str:
    lines = [f"The weekly production-perturbation lane failed on `{cell(ref_name)}` at {cell(sha)}: {run_url}", ""]
    failing = [p for p in report["perturbations"] if p["verdict"] != "detected"]
    if failing:
        lines += ["| Perturbation | Module | Row | Verdict | Detectors | Message |", "|---|---|---|---|---|---|"]
        for p in failing:
            verdict = p["verdict"] + (f" ({p['infrastructure']})" if p["infrastructure"] else "")
            lines.append(f"| `{cell(p['path'] or p['name'])}` | {cell(p['module'])} | {cell(p['row'])} | "
                         f"{cell(verdict)} | {cell(', '.join(p['detectors']))} | {cell(p['message'])} |")
        lines.append("")
    lines += [f"- {cell(finding)}" for finding in report["findings"]]
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Self-test (no dotnet, no git, no network)
# ---------------------------------------------------------------------------

def self_test() -> int:
    failures: list[str] = []
    checks = 0

    def expect(condition: bool, message: str) -> None:
        nonlocal checks
        checks += 1
        if not condition:
            failures.append(message)

    discover = load_runner(repo_root()).discover

    with tempfile.TemporaryDirectory() as work:
        root = Path(work)
        for area, names in (("alpha", ["A1", "A2", "A3"]), ("beta", ["B1"])):
            directory = root / "spec" / area / "perturbations"
            directory.mkdir(parents=True)
            (directory / "README.md").write_text("not a perturbation", encoding="utf-8")
            for name in names:
                (directory / f"{name}{EXTENSION}").write_text("x", encoding="utf-8")
        (root / "spec" / "gamma").mkdir(parents=True)

        files, full = select(root, "", discover)
        expect(files == ["spec/alpha/perturbations/A1.perturbation", "spec/alpha/perturbations/A2.perturbation",
                         "spec/alpha/perturbations/A3.perturbation", "spec/beta/perturbations/B1.perturbation"]
               and full, f"empty selection is every perturbation: {files} {full}")
        files, full = select(root, "alpha", discover)
        expect(len(files) == 3 and not full, "an area selects only that area")
        files, full = select(root, "alpha, beta", discover)
        expect(len(files) == 4 and full, "naming every area is the full selection")
        files, full = select(root, "spec/beta/perturbations/B1.perturbation beta", discover)
        expect(files == ["spec/beta/perturbations/B1.perturbation"] and not full, "files and areas deduplicate")
        for bad in ("gamma", "delta", "spec/alpha/perturbations/Z.perturbation"):
            try:
                select(root, bad, discover)
                failures.append(f"selection '{bad}' must be refused")
            except LaneError:
                checks += 1
        empty = Path(work) / "empty"
        (empty / "spec").mkdir(parents=True)
        try:
            select(empty, "", discover)
            failures.append("a repository with no perturbation must be refused, not planned as green")
        except LaneError:
            checks += 1

        expect(shard(["a", "b", "c", "d", "e"], 2) == [["a", "c", "e"], ["b", "d"]], "round-robin shards")
        expect(shard(["a", "b"], 8) == [["a"], ["b"]], "shards never exceed perturbations")
        expect(shard(["a"], 1) == [["a"]], "one shard")
        expect(parse_shards("") == DEFAULT_SHARDS and parse_shards("3") == 3 and parse_shards("2.0") == 2,
               "shard parsing")
        for bad in ("0", "9", "x"):
            try:
                parse_shards(bad)
                failures.append(f"shards '{bad}' must be refused")
            except LaneError:
                checks += 1

        expect(should_track("schedule", "main", "", True, False), "the schedule tracks")
        expect(should_track("workflow_dispatch", "main", "main", True, False), "a full dispatch on main tracks")
        expect(not should_track("workflow_dispatch", "main", "main", False, False), "a partial dispatch on main does not")
        expect(not should_track("workflow_dispatch", "topic", "main", True, False), "a branch dispatch does not by default")
        expect(should_track("workflow_dispatch", "topic", "main", False, True), "a branch dispatch can ask to track")

        planned = plan(root, "", "2", "workflow_dispatch", "main", "main", False, discover)
        expect(planned["shards"] == 2 and planned["count"] == 4 and planned["full"] and planned["track"],
               f"plan: {planned}")
        expect(planned["matrix"]["include"][1] == {
            "shard": "2", "files": "spec/alpha/perturbations/A2.perturbation spec/beta/perturbations/B1.perturbation"},
            f"matrix: {planned['matrix']}")

        def write_shard(base: Path, number: int, files: list[str], result: dict | None, code: int = 0,
                        logs: dict[str, str] | None = None) -> None:
            directory = base / f"perturbation-shard-1-{number}"
            directory.mkdir(parents=True)
            (directory / "shard.json").write_text(json.dumps(record(str(number), " ".join(files), code)),
                                                  encoding="utf-8")
            if result is not None:
                (directory / "result.json").write_text(json.dumps(result), encoding="utf-8")
            for name, text in (logs or {}).items():
                (directory / "logs").mkdir(exist_ok=True)
                (directory / "logs" / f"{name}.log").write_text(text, encoding="utf-8")

        def result(items: list[tuple[str, str, list[str]]], control: str = "passed") -> dict:
            return {"schema": 1, "ref": "abc", "ok": all(v == "detected" for _, v, _ in items),
                    "control": {"verdict": control, "message": "" if control == "passed" else "not green: F.m",
                                "detectors": {"F.m": {"passed": 1, "failed": 0, "other": 0},
                                              "G.n": {"passed": 1, "failed": 0, "other": 0}},
                                "seconds": 10.0},
                    "perturbations": [{"name": n, "path": f"spec/a/perturbations/{n}.perturbation", "module": "M",
                                       "row": "R|1", "file": "src/x.cs", "detectors": ["F.m", "G.n"],
                                       "verdict": v, "failedTests": t, "message": "", "seconds": 5.0}
                                      for n, v, t in items]}

        def path(name: str) -> str:
            return f"spec/a/perturbations/{name}.perturbation"

        base = root / "green"
        write_shard(base, 1, [path("P1")], result([("P1", "detected", ["m(True)", "Other.n"])]))
        write_shard(base, 2, [path("P2")], result([("P2", "detected", ["n"])]))
        report = summarise(base, 2)
        expect(report["verdict"] == "green" and not report["findings"], f"green report: {report['findings']}")
        expect(report["perturbations"][0]["failedDetectors"] == ["F.m", "G.n"], "failed tests map to detectors")
        text = summary_markdown(report, "main", "abc")
        expect("**Verdict: green** - 2 of 2" in text and "R\\|1" in text and "| 1 | passed | 10.0 | 2 of 2 |" in text,
               f"summary: {text}")

        base = root / "undetected"
        write_shard(base, 1, [path("P1"), path("P2")],
                    result([("P1", "detected", ["m"]), ("P2", "undetected", [])]), code=1)
        report = summarise(base, 1)
        expect(report["verdict"] == "red", "an undetected perturbation is red")
        findings = findings_markdown(report, "topic", "abc", "https://run/1")
        expect("`topic` at abc: https://run/1" in findings and "`spec/a/perturbations/P2.perturbation`" in findings
               and "P1.perturbation" not in findings, f"findings: {findings}")

        base = root / "control"
        write_shard(base, 1, [path("P1")], result([("P1", "control-failed", [])], control="failed"), code=1)
        report = summarise(base, 1)
        findings = findings_markdown(report, "main", "abc", "u")
        expect(report["verdict"] == "red" and "- shard 1: control failed: not green: F.m" in findings
               and "control-failed" in findings, f"control findings: {findings}")

        base = root / "missing"
        write_shard(base, 1, [path("P1")], result([("P1", "detected", ["m"])]))
        report = summarise(base, 2)
        expect(report["verdict"] == "red" and any("shard 2 uploaded no record" in f for f in report["findings"]),
               "a missing shard is red")

        base = root / "crashed"
        write_shard(base, 1, [path("P1")], None, code=2)
        report = summarise(base, 1)
        expect(report["verdict"] == "red" and report["perturbations"][0]["verdict"] == "not-run"
               and any("exited 2 without a result" in f for f in report["findings"]), "a crashed runner is red")

        base = root / "partial"
        write_shard(base, 1, [path("P1"), path("P2")], result([("P1", "detected", ["m"])]))
        report = summarise(base, 1)
        expect(report["verdict"] == "red" and [p["verdict"] for p in report["perturbations"]] == ["detected", "not-run"],
               "a planned perturbation absent from the result is red")

        base = root / "infra"
        write_shard(base, 1, [path("P1")], result([("P1", "build-failed", [])]), code=1,
                    logs={"P1": "error MSB4166: Child node exited prematurely"})
        report = summarise(base, 1)
        expect(report["perturbations"][0]["infrastructure"] == "MSB4166" and report["verdict"] == "red",
               "an MSBuild node crash is flagged as infrastructure and still red")
        expect("build-failed (MSB4166: infrastructure, re-run)" in summary_markdown(report, "main", "abc"),
               "the summary names the infrastructure cause")

        base = root / "vacuous"
        write_shard(base, 1, [], result([]))
        report = summarise(base, 1)
        expect(report["verdict"] == "red", "a run that executed no perturbation is not green")

    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    print(f"perturbation-lane self-test: {'FAILED' if failures else 'passed'} "
          f"({checks} checks, {len(failures)} failure(s))")
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def write_outputs(path: str | None, values: dict[str, str]) -> None:
    lines = "".join(f"{k}={v}\n" for k, v in values.items())
    if path:
        with open(path, "a", encoding="utf-8") as sink:
            sink.write(lines)
    else:
        sys.stdout.write(lines)


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    parser = argparse.ArgumentParser(description="Plan and summarise the weekly production-perturbation lane.")
    commands = parser.add_subparsers(dest="command", required=True)

    p = commands.add_parser("plan")
    p.add_argument("--selection", default="")
    p.add_argument("--shards", default="")
    p.add_argument("--event", default="")
    p.add_argument("--ref-name", default="")
    p.add_argument("--default-branch", default="")
    p.add_argument("--track", default="false")
    p.add_argument("--github-output", default=None)

    r = commands.add_parser("record")
    r.add_argument("--shard", required=True)
    r.add_argument("--files", required=True)
    r.add_argument("--exit-code", type=int, required=True)
    r.add_argument("--output", required=True)

    s = commands.add_parser("summarise")
    s.add_argument("--shards-dir", required=True)
    s.add_argument("--expect", type=int, required=True)
    s.add_argument("--ref-name", default="")
    s.add_argument("--sha", default="")
    s.add_argument("--run-url", default="")
    s.add_argument("--summary", default=None)
    s.add_argument("--report", required=True)
    s.add_argument("--findings", required=True)
    s.add_argument("--github-output", default=None)

    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            root = repo_root()
            result = plan(root, args.selection, args.shards, args.event, args.ref_name, args.default_branch,
                          args.track.strip().lower() == "true", load_runner(root).discover)
            for entry in result["matrix"]["include"]:
                print(f"shard {entry['shard']}: {entry['files']}")
            write_outputs(args.github_output, {
                "matrix": json.dumps(result["matrix"], separators=(",", ":")),
                "shards": str(result["shards"]),
                "count": str(result["count"]),
                "full": str(result["full"]).lower(),
                "track": str(result["track"]).lower(),
            })
            return 0
        if args.command == "record":
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(record(args.shard, args.files, args.exit_code), indent=2),
                                         encoding="utf-8")
            return 0
        report = summarise(Path(args.shards_dir), args.expect)
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
        Path(args.findings).write_text(
            findings_markdown(report, args.ref_name, args.sha, args.run_url) if report["verdict"] == "red" else "",
            encoding="utf-8")
        markdown = summary_markdown(report, args.ref_name, args.sha)
        if args.summary:
            with open(args.summary, "a", encoding="utf-8") as sink:
                sink.write(markdown + "\n")
        else:
            print(markdown)
        write_outputs(args.github_output, {"verdict": report["verdict"]})
        return 0
    except LaneError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
