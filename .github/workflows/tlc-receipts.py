#!/usr/bin/env python3
"""Per-module TLC verdict receipts (contract C5 of #5148, issue #5164).

WHAT A RECEIPT IS
-----------------
A receipt records that every TLC case of one specification module passed at one
fingerprint (contract C4, `tlc-fingerprint.py`). The fingerprint covers the
module's specification closure, cfgs, manifest and mutations, the TLC-affecting
harness and the toolchain, so a module whose fingerprint has a trusted receipt
cannot change its verdict and its cases need not run again.

- Full receipt: Actions cache key `tlc-verdict-v1-<Module>-<fingerprint>`,
  payload `{ "schema", "module", "fingerprint", "cases", "runId", "runAttempt",
  "sha", "event", "ref", "verifiedAt", "source" }`. Written only after every
  census case (contract C2) of that module passed in one run.
- Partial receipt: key `tlc-verdict-part-v1-<Module>-<fingerprint>-<runId>-
  <shard>-<tier>`, one per module and leg work item, written by the leg as soon
  as that item finishes, when every census case of the module the item selects
  passed. A cancelled run therefore keeps its completed work: the next run
  assembles a full receipt from ONE run's partials when they cover the census.
  The prefix differs from the full key's, so a C5 prefix never matches one.

Every entry is cached at the workspace-relative path `.tlc-receipts/<key>.json`;
save and restore must use that exact path, because the cache version hashes it.

TRUST
-----
Receipts are read only from the pull request's own scope, its base branch and
`main` (for a push or dispatch: the pushed branch and `main`). That is the
Actions cache's own scoping, which `candidates` restates by listing only those
refs. A pull request writes only to its own scope, so it cannot plant a receipt
its base or `main` would read.

SUBCOMMANDS
-----------
  candidates  LOOKUP 1/2: list the trusted cache entries for each module's
              fingerprint and write the restore manifest
  plan        LOOKUP 2/2: validate restored receipts, assemble one run's
              partials, and write the plan directory (cached-modules.txt,
              receipt-plan.json, census, categories, fingerprints) that
              plan-test-matrix.py --tlc-cached-modules consumes
  partials    WRITE (leg): `<receipts-dir>/<Module>/partial-<run>-<shard>-<tier>.json`
              for each finished item whose selected census cases all passed
  assemble    WRITE (run): `<receipts-dir>/<Module>/receipt.json` for exactly the
              modules whose every census case passed, from leg evidence or a
              directory of trx files, and a per-module verdict table
  stage       SAVE: copy every receipt under a receipts directory to its cache
              path and write the save manifest

The cache itself is driven by the `.github/actions/tlc-receipts` composite
action (operation `lookup` or `save`), because the cache runtime token exists
only inside an action step. This script decides; the action moves bytes.

Usage:
  tlc-receipts.py candidates --fingerprints F --refs R[,R...] --output-manifest F
                             [--repo OWNER/NAME | --listing-file F]
  tlc-receipts.py plan --fingerprints F --census F --case-categories F
                       --restored-dir D --output-dir D [--ignore-receipts]
                       [--summary-file F]
  tlc-receipts.py partials --evidence-dir D --plan-dir D --receipts-dir D
  tlc-receipts.py assemble --fingerprints F --census F
                           (--evidence-dir D | --trx-dir D) [--receipt-plan F]
                           --receipts-dir D [--summary-file F] [--require-complete]
  tlc-receipts.py stage --receipts-dir D --output-manifest F
  tlc-receipts.py --self-test
"""

from __future__ import annotations

import argparse
import datetime
import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

RECEIPT_PREFIX = "tlc-verdict-v1-"
PARTIAL_PREFIX = "tlc-verdict-part-v1-"
LISTING_PREFIX = "tlc-verdict-"
CACHE_DIRECTORY = ".tlc-receipts"
MODULE_CATEGORY_PREFIX = "TlcModule_"
EVIDENCE_GLOB = "tlc-evidence-*.jsonl"
SCHEMA = 1

# Bounds the cache listing per ref. Older entries beyond it are not read, which
# fails toward running more, never toward trusting more.
MAX_LISTING_PAGES = 20


# ---------------------------------------------------------------------------
# Names and keys
# ---------------------------------------------------------------------------

def normalise(name: str) -> str:
    """A test name with all whitespace removed. The TRX joins a parameterised
    case's arguments with ',' and the census (NUnit's FullName) with ', ';
    module, mutation and variant names are identifiers, so dropping whitespace
    loses nothing."""
    return re.sub(r"\s+", "", name)


def receipt_key(module: str, fingerprint: str) -> str:
    return f"{RECEIPT_PREFIX}{module}-{fingerprint}"


def partial_prefix(module: str, fingerprint: str) -> str:
    return f"{PARTIAL_PREFIX}{module}-{fingerprint}-"


def partial_key(module: str, fingerprint: str, run_id: str, shard: str, tier: str) -> str:
    return f"{partial_prefix(module, fingerprint)}{run_id}-{shard}-{tier}"


def cache_path(key: str) -> str:
    return f"{CACHE_DIRECTORY}/{key}.json"


def run_url(run_id: str) -> str:
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    return f"{server}/{repository}/actions/runs/{run_id}" if repository and run_id else ""


def run_identity() -> dict:
    return {
        "runId": os.environ.get("GITHUB_RUN_ID", ""),
        "runAttempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
        "sha": os.environ.get("GITHUB_SHA", ""),
        "event": os.environ.get("GITHUB_EVENT_NAME", ""),
        "ref": os.environ.get("GITHUB_REF", ""),
        "verifiedAt": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# VSTest filter evaluation
#
# The planner and the legs must agree with `dotnet test --filter` on which
# census cases a work item selects. The grammar used by test-shards.json is a
# small subset: conditions `Property(=|!=|~|!~)Value` joined by `&` and `|`,
# with parentheses, `&` binding tighter than `|`. Values are compared
# case-insensitively, as VSTest does. A multi-valued property (TestCategory)
# satisfies `=`/`~` when ANY value does, and `!=`/`!~` only when NO value
# does - the universal reading VSTest applies, which the TLC partition check
# (CiMemberPullRequestTieringTests.TlcPartition) also relies on.
# ---------------------------------------------------------------------------

_OPERATORS = ("!=", "!~", "=", "~")


def _tokenise(expression: str) -> list[str]:
    tokens: list[str] = []
    current = ""
    for char in expression:
        if char in "()&|":
            if current.strip():
                tokens.append(current.strip())
            current = ""
            tokens.append(char)
        else:
            current += char
    if current.strip():
        tokens.append(current.strip())
    return tokens


def parse_filter(expression: str):
    """Parse a filter into nested tuples: ('or', [...]), ('and', [...]),
    ('cond', property, operator, value)."""
    tokens = _tokenise(expression)
    position = 0

    def peek() -> str | None:
        return tokens[position] if position < len(tokens) else None

    def take() -> str:
        nonlocal position
        if position >= len(tokens):
            raise ValueError(f"unexpected end of filter: {expression!r}")
        token = tokens[position]
        position += 1
        return token

    def parse_or():
        terms = [parse_and()]
        while peek() == "|":
            take()
            terms.append(parse_and())
        return terms[0] if len(terms) == 1 else ("or", terms)

    def parse_and():
        terms = [parse_atom()]
        while peek() == "&":
            take()
            terms.append(parse_atom())
        return terms[0] if len(terms) == 1 else ("and", terms)

    def parse_atom():
        token = take()
        if token == "(":
            node = parse_or()
            if take() != ")":
                raise ValueError(f"unbalanced parenthesis in filter: {expression!r}")
            return node
        if token in ")&|":
            raise ValueError(f"unexpected {token!r} in filter: {expression!r}")
        for operator in _OPERATORS:
            index = token.find(operator)
            if index > 0:
                name, value = token[:index].strip(), token[index + len(operator):].strip()
                if name not in ("TestCategory", "FullyQualifiedName"):
                    raise ValueError(f"unsupported filter property {name!r} in {expression!r}")
                return ("cond", name, operator, value)
        raise ValueError(f"not a filter condition: {token!r} in {expression!r}")

    node = parse_or()
    if position != len(tokens):
        raise ValueError(f"trailing tokens in filter: {expression!r}")
    return node


def evaluate(node, fqn: str, categories: list[str]) -> bool:
    kind = node[0]
    if kind == "or":
        return any(evaluate(child, fqn, categories) for child in node[1])
    if kind == "and":
        return all(evaluate(child, fqn, categories) for child in node[1])
    _, name, operator, value = node
    wanted = value.casefold()
    if name == "TestCategory":
        values = [category.casefold() for category in categories]
    else:
        values = [fqn.casefold()]
    if operator == "=":
        return any(item == wanted for item in values)
    if operator == "!=":
        return all(item != wanted for item in values)
    if operator == "~":
        return any(wanted in item for item in values)
    return all(wanted not in item for item in values)


def selected_cases(filter_expression: str | None, census: dict[str, list[str]],
                   categories: dict[str, list[str]]) -> dict[str, list[str]]:
    """The census cases a filter selects, by module. A null filter selects
    every case."""
    node = parse_filter(filter_expression) if filter_expression else None
    selected: dict[str, list[str]] = {}
    for module, names in census.items():
        for name in names:
            if node is None or evaluate(node, name, categories.get(name, [])):
                selected.setdefault(module, []).append(name)
    return selected


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def read_json(path: str | Path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str | Path, document) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_fingerprints(path: str | Path) -> dict[str, str]:
    document = read_json(path)
    if document.get("schema") != 1 or not isinstance(document.get("modules"), dict):
        raise ValueError(f"{path}: not a schema-1 C4 fingerprint document")
    result = {}
    for module, entry in document["modules"].items():
        fingerprint = entry.get("fingerprint") if isinstance(entry, dict) else None
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            raise ValueError(f"{path}: module {module!r} has no sha256 fingerprint")
        result[module] = fingerprint
    if not result:
        raise ValueError(f"{path}: no modules")
    return result


def load_census(path: str | Path) -> dict[str, list[str]]:
    document = read_json(path)
    if not isinstance(document, dict) or not document:
        raise ValueError(f"{path}: the census is empty")
    for module, names in document.items():
        if not isinstance(names, list) or not names or not all(isinstance(name, str) for name in names):
            raise ValueError(f"{path}: module {module!r} has no cases")
    return document


def load_categories(path: str | Path) -> dict[str, list[str]]:
    document = read_json(path)
    if not isinstance(document, dict):
        raise ValueError(f"{path}: not a case-category map")
    return document


def census_of(module: str, census: dict[str, list[str]]) -> set[str]:
    return {normalise(name) for name in census.get(module, [])}


# ---------------------------------------------------------------------------
# candidates
# ---------------------------------------------------------------------------

def list_caches(repo: str, refs: list[str]) -> list[dict]:
    """Every cache entry whose key starts with `tlc-verdict-`, in the given refs."""
    entries: list[dict] = []
    for ref in refs:
        for page in range(1, MAX_LISTING_PAGES + 1):
            completed = subprocess.run(
                ["gh", "api", "-X", "GET", f"repos/{repo}/actions/caches",
                 "-f", f"key={LISTING_PREFIX}", "-f", f"ref={ref}", "-f", "per_page=100",
                 "-f", f"page={page}", "-f", "sort=created_at", "-f", "direction=desc"],
                capture_output=True, text=True, check=False)
            if completed.returncode != 0:
                print(f"::warning title=TLC receipts::Listing caches for {ref} failed; "
                      f"treating it as holding no receipts: {completed.stderr.strip()}")
                break
            caches = json.loads(completed.stdout or "{}").get("actions_caches", [])
            entries.extend({"key": cache["key"], "ref": cache.get("ref", ref)} for cache in caches)
            if len(caches) < 100:
                break
    return entries


def candidates(fingerprints: dict[str, str], listing: list[dict], refs: list[str]) -> list[dict]:
    """The restore manifest: each module's full receipt when one is listed in a
    trusted ref, otherwise every partial for its fingerprint."""
    trusted = [entry for entry in listing if entry.get("ref") in refs]
    keys = {entry["key"] for entry in trusted}
    manifest: list[dict] = []
    for module in sorted(fingerprints):
        fingerprint = fingerprints[module]
        full = receipt_key(module, fingerprint)
        if full in keys:
            manifest.append({"module": module, "key": full, "path": cache_path(full)})
            continue
        prefix = partial_prefix(module, fingerprint)
        for key in sorted(key for key in keys if key.startswith(prefix)):
            manifest.append({"module": module, "key": key, "path": cache_path(key)})
    return manifest


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------

def validate_receipt(document, module: str, fingerprint: str, expected: set[str]) -> str | None:
    """None when the receipt proves the module; otherwise why it does not."""
    if not isinstance(document, dict):
        return "not a JSON object"
    if document.get("module") != module:
        return f"names module {document.get('module')!r}"
    if document.get("fingerprint") != fingerprint:
        return "carries a different fingerprint"
    cases = document.get("cases")
    if not isinstance(cases, list):
        return "lists no cases"
    missing = expected - {normalise(case) for case in cases if isinstance(case, str)}
    if missing:
        return f"does not cover {len(missing)} census case(s)"
    if not str(document.get("runId", "")).strip():
        return "names no run"
    return None


def plan(fingerprints: dict[str, str], census: dict[str, list[str]], restored_dir: str | Path,
         ignore_receipts: bool = False) -> dict:
    """Classify every census module as cached (with the receipt that proves it)
    or to run."""
    restored = Path(restored_dir)
    modules: dict[str, dict] = {}
    unknown = sorted(set(census) - set(fingerprints))
    if unknown:
        raise ValueError("census modules without a fingerprint: " + ", ".join(unknown))

    for module in sorted(census):
        fingerprint = fingerprints[module]
        entry: dict = {"status": "run", "fingerprint": fingerprint}
        modules[module] = entry
        if ignore_receipts:
            entry["reason"] = "receipts ignored for this run"
            continue
        expected = census_of(module, census)

        full_path = restored / f"{receipt_key(module, fingerprint)}.json"
        if full_path.is_file():
            try:
                document = read_json(full_path)
            except (OSError, json.JSONDecodeError) as error:
                document, problem = None, f"unreadable ({error})"
            else:
                problem = validate_receipt(document, module, fingerprint, expected)
            if problem is None:
                entry.update({"status": "cached", "source": "receipt", "runId": str(document["runId"])})
                continue
            entry["reason"] = f"receipt rejected: {problem}"

        by_run: dict[str, list[dict]] = {}
        for path in sorted(restored.glob(f"{partial_prefix(module, fingerprint)}*.json")):
            try:
                document = read_json(path)
            except (OSError, json.JSONDecodeError):
                continue
            if (isinstance(document, dict) and document.get("kind") == "partial"
                    and document.get("module") == module and document.get("fingerprint") == fingerprint
                    and str(document.get("runId", "")).strip()):
                by_run.setdefault(str(document["runId"]), []).append(document)
        for run_id in sorted(by_run, key=lambda value: (len(value), value), reverse=True):
            covered = {normalise(case) for part in by_run[run_id] for case in part.get("cases", [])
                       if isinstance(case, str)}
            if expected <= covered:
                first = by_run[run_id][0]
                receipt = {
                    "schema": SCHEMA, "module": module, "fingerprint": fingerprint,
                    "cases": sorted(census[module]), "source": "partials",
                    **{field: str(first.get(field, "")) for field in ("runId", "runAttempt", "sha", "event", "ref")},
                    "verifiedAt": max(str(part.get("verifiedAt", "")) for part in by_run[run_id]),
                }
                entry.update({"status": "cached", "source": "partials", "runId": run_id, "receipt": receipt})
                entry.pop("reason", None)
                break
        else:
            entry.setdefault("reason", "no trusted receipt for this fingerprint")
    return {"schema": SCHEMA, "modules": modules}


def write_plan_summary(handle, document: dict) -> None:
    modules = document["modules"]
    cached = [module for module, entry in modules.items() if entry["status"] == "cached"]
    handle.write(f"### TLC verdict receipts: {len(cached)} of {len(modules)} module(s) cached\n\n")
    handle.write("A cached module's cases do not run: a trusted receipt (contract C5) proves they all "
                 "passed at the same fingerprint, which covers the module's specification, the TLC "
                 "harness and the toolchain.\n\n")
    handle.write("| Module | Plan | Receipt run |\n| --- | --- | --- |\n")
    for module in sorted(modules):
        entry = modules[module]
        if entry["status"] == "cached":
            url = run_url(entry["runId"])
            link = f"[{entry['runId']}]({url})" if url else entry["runId"]
            via = " (assembled from partial receipts)" if entry.get("source") == "partials" else ""
            handle.write(f"| `{module}` | cached{via} | {link} |\n")
        else:
            handle.write(f"| `{module}` | run ({entry.get('reason', '')}) | - |\n")
    handle.write("\n")


# ---------------------------------------------------------------------------
# Evidence (written by run-test-leg.py) and partial receipts
# ---------------------------------------------------------------------------

def load_evidence(evidence_dir: str | Path) -> list[dict]:
    items = []
    for path in sorted(glob.glob(os.path.join(str(evidence_dir), "**", EVIDENCE_GLOB), recursive=True)):
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    items.append(json.loads(line))
    return items


def load_plan_dir(plan_dir: str | Path) -> tuple[dict, dict[str, list[str]], dict[str, list[str]]]:
    root = Path(plan_dir)
    return (read_json(root / "receipt-plan.json"), load_census(root / "census.json"),
            load_categories(root / "categories.json"))


TRX_NAMESPACE = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}


def trx_outcomes(path: str | Path) -> tuple[list[str], list[str]]:
    """(passed, failed) fully qualified test names from one trx file: the
    definition's class name joined to the result's test name. An unreadable
    file contributes nothing, which fails toward running more."""
    import xml.etree.ElementTree as ElementTree

    try:
        root = ElementTree.parse(str(path)).getroot()
    except (OSError, ElementTree.ParseError):
        return [], []
    classes: dict[str, str] = {}
    for definition in root.findall("t:TestDefinitions/t:UnitTest", TRX_NAMESPACE):
        method = definition.find("t:TestMethod", TRX_NAMESPACE)
        if definition.get("id") and method is not None and method.get("className"):
            classes[definition.get("id")] = method.get("className")
    passed: list[str] = []
    failed: list[str] = []
    for result in root.findall("t:Results/t:UnitTestResult", TRX_NAMESPACE):
        name = result.get("testName", "")
        owner = classes.get(result.get("testId", ""), "")
        if not name or not owner:
            continue
        if result.get("outcome") == "Passed":
            passed.append(f"{owner}.{name}")
        elif result.get("outcome") == "Failed":
            failed.append(f"{owner}.{name}")
    return sorted(passed), sorted(failed)


def load_trx_dir(trx_dir: str | Path) -> list[dict]:
    """Evidence items from every trx file under a directory (no filter: a trx
    from a downloaded artifact does not say which item produced it)."""
    items = []
    for path in sorted(glob.glob(os.path.join(str(trx_dir), "**", "*.trx"), recursive=True)):
        passed, failed = trx_outcomes(path)
        items.append({"label": os.path.basename(path), "passed": passed, "failed": failed})
    return items


def partials(evidence: list[dict], receipt_plan: dict, census: dict[str, list[str]],
             categories: dict[str, list[str]], identity: dict) -> list[dict]:
    """Partial receipts: one per (module to run, finished item) whose selected
    census cases all passed."""
    results = []
    for item in evidence:
        if "filter" not in item:
            continue
        selected = selected_cases(item.get("filter"), census, categories)
        passed = {normalise(name) for name in item.get("passed", [])}
        for module, names in sorted(selected.items()):
            entry = receipt_plan["modules"].get(module)
            if entry is None or entry["status"] != "run":
                continue
            if not {normalise(name) for name in names} <= passed:
                continue
            results.append({
                "schema": SCHEMA, "kind": "partial", "module": module,
                "fingerprint": entry["fingerprint"], "shard": item["shard"], "tier": item["tier"],
                "cases": sorted(names), **identity,
            })
    return results


def key_of(document: dict) -> str:
    """The cache key a receipt document is saved under, derived from its content."""
    module, fingerprint = document.get("module"), document.get("fingerprint")
    if not (isinstance(module, str) and re.fullmatch(r"[A-Za-z0-9_]+", module)):
        raise ValueError(f"receipt names no valid module: {module!r}")
    if not (isinstance(fingerprint, str) and re.fullmatch(r"[0-9a-f]{64}", fingerprint)):
        raise ValueError(f"receipt for {module} carries no sha256 fingerprint")
    if document.get("kind") == "partial":
        parts = [str(document.get(field, "")) for field in ("runId", "shard", "tier")]
        if not all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in parts):
            raise ValueError(f"partial receipt for {module} lacks a run, shard or tier")
        return partial_key(module, fingerprint, *parts)
    return receipt_key(module, fingerprint)


def receipt_file(receipts_dir: str | Path, document: dict) -> Path:
    """`<dir>/<Module>/receipt.json`, or `<dir>/<Module>/partial-<run>-<shard>-<tier>.json`."""
    root = Path(receipts_dir) / document["module"]
    if document.get("kind") == "partial":
        return root / f"partial-{document['runId']}-{document['shard']}-{document['tier']}.json"
    return root / "receipt.json"


def write_receipts(documents: list[dict], receipts_dir: str | Path) -> list[Path]:
    paths = []
    for document in documents:
        key_of(document)
        path = receipt_file(receipts_dir, document)
        write_json(path, document)
        paths.append(path)
    return paths


def stage(receipts_dir: str | Path, root: str | Path = ".") -> list[dict]:
    """Copy every receipt under a receipts directory to its cache path and
    return the save manifest ([{key, path}])."""
    manifest = []
    for path in sorted(Path(receipts_dir).glob("*/*.json")):
        document = read_json(path)
        key = key_of(document)
        write_json(Path(root) / cache_path(key), document)
        manifest.append({"key": key, "path": cache_path(key)})
    return manifest


# ---------------------------------------------------------------------------
# assemble
# ---------------------------------------------------------------------------

def assemble(evidence: list[dict], receipt_plan: dict | None, fingerprints: dict[str, str],
             census: dict[str, list[str]], identity: dict) -> tuple[dict[str, dict], list[dict]]:
    """(status per module, full receipts to write). Without a receipt plan
    (nightly coverage, which never reads receipts) every module counts as run."""
    passed = {normalise(name) for item in evidence for name in item.get("passed", [])}
    failed = {normalise(name) for item in evidence for name in item.get("failed", [])}
    planned = (receipt_plan or {}).get("modules", {})
    report: dict[str, dict] = {}
    receipts: list[dict] = []
    for module in sorted(census):
        if module not in fingerprints:
            raise ValueError(f"census module {module} has no fingerprint")
        entry = planned.get(module, {"status": "run"})
        if entry["status"] == "cached":
            report[module] = {"status": "cached", "runId": entry["runId"], "source": entry.get("source", "")}
            if entry.get("source") == "partials" and isinstance(entry.get("receipt"), dict):
                receipts.append(entry["receipt"])
            continue
        expected = census_of(module, census)
        missing = expected - passed
        if not missing:
            report[module] = {"status": "ran"}
            receipts.append({
                "schema": SCHEMA, "module": module, "fingerprint": fingerprints[module],
                "cases": sorted(census[module]), "source": "run", **identity,
            })
        elif missing & failed:
            report[module] = {"status": "failed", "failed": len(missing & failed)}
        else:
            report[module] = {"status": "incomplete", "missing": len(missing)}
    return report, receipts


def write_assemble_summary(handle, report: dict[str, dict]) -> None:
    counts: dict[str, int] = {}
    for entry in report.values():
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    handle.write("### TLC verdict per module\n\n")
    handle.write(", ".join(f"{counts.get(status, 0)} {status}"
                           for status in ("ran", "cached", "failed", "incomplete")) + ".\n\n")
    handle.write("| Module | Verdict |\n| --- | --- |\n")
    for module in sorted(report):
        entry = report[module]
        status = entry["status"]
        if status == "cached":
            url = run_url(entry["runId"])
            link = f"[{entry['runId']}]({url})" if url else entry["runId"]
            text = f"cached (receipt from run {link})"
        elif status == "ran":
            text = "ran; every census case passed, receipt written"
        elif status == "failed":
            text = f"FAILED ({entry['failed']} census case(s) failed); no receipt"
        else:
            text = (f"INCOMPLETE ({entry['missing']} census case(s) never reported a result); "
                    "no receipt")
        handle.write(f"| `{module}` | {text} |\n")
    handle.write("\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    if argv[:1] == ["--self-test"]:
        result = unittest.main(argv=["tlc-receipts"], exit=False, module=__name__).result
        return 0 if result.wasSuccessful() and result.testsRun > 0 else 1

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    listing = commands.add_parser("candidates")
    listing.add_argument("--fingerprints", required=True)
    listing.add_argument("--refs", required=True, help="comma-separated trusted refs")
    listing.add_argument("--repo")
    listing.add_argument("--listing-file", help="a saved listing ([{key, ref}]) instead of the API")
    listing.add_argument("--output-manifest", required=True)

    planning = commands.add_parser("plan")
    planning.add_argument("--fingerprints", required=True)
    planning.add_argument("--census", required=True)
    planning.add_argument("--case-categories", required=True)
    planning.add_argument("--restored-dir", required=True)
    planning.add_argument("--output-dir", required=True)
    planning.add_argument("--ignore-receipts", action="store_true")
    planning.add_argument("--summary-file")

    parting = commands.add_parser("partials")
    parting.add_argument("--evidence-dir", required=True)
    parting.add_argument("--plan-dir", required=True)
    parting.add_argument("--receipts-dir", required=True)

    assembling = commands.add_parser("assemble")
    assembling.add_argument("--fingerprints", required=True)
    assembling.add_argument("--census", required=True)
    source = assembling.add_mutually_exclusive_group(required=True)
    source.add_argument("--evidence-dir", help="leg results holding tlc-evidence-*.jsonl")
    source.add_argument("--trx-dir", help="a directory of trx files from one run")
    assembling.add_argument("--receipt-plan", help="the plan's receipt-plan.json (cached modules)")
    assembling.add_argument("--receipts-dir", required=True)
    assembling.add_argument("--summary-file")
    assembling.add_argument("--require-complete", action="store_true",
                            help="exit 1 when a module neither has a receipt nor reported every case")

    staging = commands.add_parser("stage")
    staging.add_argument("--receipts-dir", required=True)
    staging.add_argument("--output-manifest", required=True)

    args = parser.parse_args(argv)

    if args.command == "candidates":
        fingerprints = load_fingerprints(args.fingerprints)
        refs = [ref.strip() for ref in args.refs.split(",") if ref.strip()]
        if args.listing_file:
            entries = read_json(args.listing_file)
        elif args.repo:
            entries = list_caches(args.repo, refs)
        else:
            parser.error("candidates needs --repo or --listing-file")
        manifest = candidates(fingerprints, entries, refs)
        write_json(args.output_manifest, manifest)
        print(f"{len(manifest)} trusted receipt cache entr(ies) to restore from {', '.join(refs)}.")
        return 0

    if args.command == "plan":
        fingerprints = load_fingerprints(args.fingerprints)
        census = load_census(args.census)
        categories = load_categories(args.case_categories)
        document = plan(fingerprints, census, args.restored_dir, args.ignore_receipts)
        output = Path(args.output_dir)
        write_json(output / "receipt-plan.json", document)
        write_json(output / "census.json", census)
        write_json(output / "categories.json", categories)
        write_json(output / "fingerprints.json", read_json(args.fingerprints))
        cached = sorted(module for module, entry in document["modules"].items() if entry["status"] == "cached")
        (output / "cached-modules.txt").write_text("".join(f"{module}\n" for module in cached), encoding="utf-8")
        if args.summary_file:
            with open(args.summary_file, "a", encoding="utf-8") as handle:
                write_plan_summary(handle, document)
        print(f"TLC receipts: {len(cached)} of {len(document['modules'])} module(s) cached.")
        return 0

    if args.command == "stage":
        manifest = stage(args.receipts_dir)
        write_json(args.output_manifest, manifest)
        print(f"{len(manifest)} TLC receipt(s) staged for the cache.")
        return 0

    identity = run_identity()

    if args.command == "partials":
        receipt_plan, census, categories = load_plan_dir(args.plan_dir)
        evidence = load_evidence(args.evidence_dir)
        if not identity["runId"]:
            identity["runId"] = "local"
        documents = partials(evidence, receipt_plan, census, categories, identity)
        write_receipts(documents, args.receipts_dir)
        print(f"{len(documents)} partial TLC receipt(s) from {len(evidence)} finished TLC item(s).")
        return 0

    fingerprints = load_fingerprints(args.fingerprints)
    census = load_census(args.census)
    evidence = load_evidence(args.evidence_dir) if args.evidence_dir else load_trx_dir(args.trx_dir)
    receipt_plan = read_json(args.receipt_plan) if args.receipt_plan else None
    report, receipts = assemble(evidence, receipt_plan, fingerprints, census, identity)
    paths = write_receipts(receipts, args.receipts_dir)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as handle:
            write_assemble_summary(handle, report)
    for path in paths:
        print(f"receipt: {path.as_posix()}")
    print(f"{len(receipts)} TLC receipt(s) written to {args.receipts_dir}.")
    incomplete = sorted(module for module, entry in report.items() if entry["status"] == "incomplete")
    if incomplete and args.require_complete:
        print("::error title=TLC receipts::Module(s) " + ", ".join(incomplete) + " neither had a trusted "
              "receipt nor reported every census case in this run, so their verdict is unknown. A cached "
              "module excluded more than the receipts cover, or a routing change dropped a case.")
        return 1
    return 0


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

FP_A = "a" * 64
FP_B = "b" * 64
CENSUS = {
    "Alpha": ["N.T.The_base_specification_holds(Alpha)", "N.T.Each_property_fires(Alpha, M1)"],
    "Beta": ["N.T.The_base_specification_holds(Beta)"],
}
CATEGORIES = {
    "N.T.The_base_specification_holds(Alpha)": ["Tlc", "TlcModule_Alpha"],
    "N.T.Each_property_fires(Alpha, M1)": ["Tlc", "TlcModule_Alpha", "TlcShardX"],
    "N.T.The_base_specification_holds(Beta)": ["Tlc", "TlcModule_Beta", "TlcShardX"],
}


class FilterTests(unittest.TestCase):
    def matches(self, expression: str, fqn: str, categories: list[str]) -> bool:
        return evaluate(parse_filter(expression), fqn, categories)

    def test_category_operators_are_existential_and_universal(self):
        self.assertTrue(self.matches("TestCategory=TlcShardX", "a.b", ["Tlc", "TlcShardX"]))
        self.assertFalse(self.matches("TestCategory!=TlcShardX", "a.b", ["Tlc", "TlcShardX"]))
        self.assertTrue(self.matches("TestCategory!=Chaos&TestCategory!=Coyote", "a.b", ["Tlc"]))
        self.assertTrue(self.matches("TestCategory=tlcshardx", "a", ["TLCSHARDX"]))

    def test_precedence_and_parentheses(self):
        self.assertTrue(self.matches("TestCategory=A|TestCategory=B&TestCategory=C", "x", ["A"]))
        self.assertFalse(self.matches("(TestCategory=A|TestCategory=B)&TestCategory=C", "x", ["A"]))

    def test_name_contains(self):
        expression = ("(FullyQualifiedName~N.T&FullyQualifiedName!~N.T.Smoke)"
                      "&(TestCategory!=Chaos&TestCategory!=Coyote)")
        self.assertTrue(self.matches(expression, "N.T.Base(Alpha)", []))
        self.assertFalse(self.matches(expression, "N.T.Smoke", []))

    def test_malformed_filters_are_refused(self):
        for expression in ("(TestCategory=A", "TestCategory", "Foo=Bar", "TestCategory=A)"):
            with self.subTest(expression=expression), self.assertRaises(ValueError):
                parse_filter(expression)

    def test_selected_cases_groups_by_module(self):
        self.assertEqual(selected_cases("TestCategory=TlcShardX", CENSUS, CATEGORIES),
                         {"Alpha": ["N.T.Each_property_fires(Alpha, M1)"],
                          "Beta": ["N.T.The_base_specification_holds(Beta)"]})
        self.assertEqual(sum(map(len, selected_cases(None, CENSUS, CATEGORIES).values())), 3)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.root = Path(self.work.name)
        self.fingerprints = {"Alpha": FP_A, "Beta": FP_B}
        self.identity = {"runId": "42", "runAttempt": "1", "sha": "s", "event": "pull_request",
                         "ref": "refs/pull/1/merge", "verifiedAt": "2026-10-29T00:00:00Z"}

    def tearDown(self):
        self.work.cleanup()

    def restore(self, key: str, document: dict) -> None:
        write_json(self.root / f"{key}.json", document)

    def full(self, module: str, fingerprint: str, cases: list[str], run_id: str = "7") -> dict:
        return {"schema": 1, "module": module, "fingerprint": fingerprint, "cases": cases, "runId": run_id}

    def test_keys_are_c5_and_partials_cannot_match_a_c5_prefix(self):
        self.assertEqual(receipt_key("Alpha", FP_A), f"tlc-verdict-v1-Alpha-{FP_A}")
        self.assertFalse(partial_key("Alpha", FP_A, "1", "s", "deterministic").startswith(RECEIPT_PREFIX))
        self.assertEqual(cache_path("k"), ".tlc-receipts/k.json")

    def test_candidates_read_only_trusted_refs_and_prefer_the_full_receipt(self):
        listing = [
            {"key": receipt_key("Alpha", FP_A), "ref": "refs/heads/main"},
            {"key": partial_key("Alpha", FP_A, "9", "s", "t"), "ref": "refs/heads/main"},
            {"key": receipt_key("Beta", FP_B), "ref": "refs/pull/99/merge"},
            {"key": partial_key("Beta", FP_B, "9", "s", "t"), "ref": "refs/pull/1/merge"},
            {"key": receipt_key("Beta", "c" * 64), "ref": "refs/heads/main"},
        ]
        manifest = candidates(self.fingerprints, listing, ["refs/pull/1/merge", "refs/heads/main"])
        self.assertEqual([entry["key"] for entry in manifest],
                         [receipt_key("Alpha", FP_A), partial_key("Beta", FP_B, "9", "s", "t")])
        self.assertTrue(all(entry["path"] == cache_path(entry["key"]) for entry in manifest))

    def test_a_valid_receipt_caches_its_module(self):
        self.restore(receipt_key("Alpha", FP_A), self.full("Alpha", FP_A, CENSUS["Alpha"]))
        document = plan(self.fingerprints, CENSUS, self.root)
        self.assertEqual(document["modules"]["Alpha"]["status"], "cached")
        self.assertEqual(document["modules"]["Alpha"]["runId"], "7")
        self.assertEqual(document["modules"]["Beta"]["status"], "run")

    def test_whitespace_in_argument_lists_does_not_matter(self):
        cases = [name.replace(", ", ",") for name in CENSUS["Alpha"]]
        self.restore(receipt_key("Alpha", FP_A), self.full("Alpha", FP_A, cases))
        self.assertEqual(plan(self.fingerprints, CENSUS, self.root)["modules"]["Alpha"]["status"], "cached")

    def test_a_receipt_that_does_not_prove_the_module_is_rejected(self):
        bad = {
            "wrong module": self.full("Beta", FP_A, CENSUS["Alpha"]),
            "wrong fingerprint": self.full("Alpha", FP_B, CENSUS["Alpha"]),
            "missing case": self.full("Alpha", FP_A, CENSUS["Alpha"][:1]),
            "no run": self.full("Alpha", FP_A, CENSUS["Alpha"], run_id=""),
        }
        for label, document in bad.items():
            with self.subTest(label=label):
                self.restore(receipt_key("Alpha", FP_A), document)
                entry = plan(self.fingerprints, CENSUS, self.root)["modules"]["Alpha"]
                self.assertEqual(entry["status"], "run")
                self.assertIn("receipt rejected", entry["reason"])

    def test_ignore_receipts_runs_every_module(self):
        self.restore(receipt_key("Alpha", FP_A), self.full("Alpha", FP_A, CENSUS["Alpha"]))
        document = plan(self.fingerprints, CENSUS, self.root, ignore_receipts=True)
        self.assertEqual({entry["status"] for entry in document["modules"].values()}, {"run"})

    def test_a_module_without_a_fingerprint_fails(self):
        with self.assertRaises(ValueError):
            plan({"Alpha": FP_A}, CENSUS, self.root)

    def partial(self, run_id: str, cases: list[str], shard: str) -> None:
        self.restore(partial_key("Alpha", FP_A, run_id, shard, "deterministic"),
                     {"schema": 1, "kind": "partial", "module": "Alpha", "fingerprint": FP_A,
                      "shard": shard, "tier": "deterministic", "cases": cases, "runId": run_id,
                      "verifiedAt": "2026-10-29T00:00:00Z"})

    def test_partials_from_one_run_assemble_a_receipt(self):
        self.partial("5", CENSUS["Alpha"][:1], "catch-all")
        self.partial("5", CENSUS["Alpha"][1:], "x")
        entry = plan(self.fingerprints, CENSUS, self.root)["modules"]["Alpha"]
        self.assertEqual((entry["status"], entry["source"], entry["runId"]), ("cached", "partials", "5"))
        self.assertEqual(entry["receipt"]["cases"], sorted(CENSUS["Alpha"]))

    def test_partials_from_two_runs_do_not_combine(self):
        self.partial("5", CENSUS["Alpha"][:1], "catch-all")
        self.partial("6", CENSUS["Alpha"][1:], "x")
        self.assertEqual(plan(self.fingerprints, CENSUS, self.root)["modules"]["Alpha"]["status"], "run")

    def test_a_leg_writes_partials_only_for_fully_passed_modules_it_ran(self):
        receipt_plan = {"modules": {"Alpha": {"status": "run", "fingerprint": FP_A},
                                    "Beta": {"status": "cached", "fingerprint": FP_B, "runId": "1"}}}
        evidence = [{"shard": "x", "tier": "deterministic", "filter": "TestCategory=TlcShardX",
                     "passed": ["N.T.Each_property_fires(Alpha,M1)", "N.T.The_base_specification_holds(Beta)"],
                     "failed": []},
                    {"shard": "all", "tier": "deterministic", "filter": "FullyQualifiedName~N.T",
                     "passed": ["N.T.Each_property_fires(Alpha,M1)"],
                     "failed": ["N.T.The_base_specification_holds(Alpha)"]}]
        documents = partials(evidence, receipt_plan, CENSUS, CATEGORIES, self.identity)
        self.assertEqual([(doc["module"], doc["shard"]) for doc in documents], [("Alpha", "x")])
        self.assertEqual(documents[0]["runId"], "42")

    def test_assemble_reports_every_module_and_writes_receipts_only_for_complete_ones(self):
        census = dict(CENSUS, Gamma=["N.T.The_base_specification_holds(Gamma)"],
                      Delta=["N.T.The_base_specification_holds(Delta)"])
        assembled = {"schema": 1, "module": "Delta", "fingerprint": FP_B, "cases": census["Delta"],
                     "runId": "3", "source": "partials"}
        receipt_plan = {"modules": {
            "Alpha": {"status": "run", "fingerprint": FP_A},
            "Beta": {"status": "cached", "fingerprint": FP_B, "runId": "8", "source": "receipt"},
            "Gamma": {"status": "run", "fingerprint": FP_A},
            "Delta": {"status": "cached", "fingerprint": FP_B, "runId": "3", "source": "partials",
                      "receipt": assembled},
        }}
        evidence = [{"passed": CENSUS["Alpha"], "failed": []}]
        fingerprints = {"Alpha": FP_A, "Beta": FP_B, "Gamma": FP_A, "Delta": FP_B}
        report, receipts = assemble(evidence, receipt_plan, fingerprints, census, self.identity)
        self.assertEqual({module: entry["status"] for module, entry in report.items()},
                         {"Alpha": "ran", "Beta": "cached", "Gamma": "incomplete", "Delta": "cached"})
        self.assertEqual(sorted(doc["module"] for doc in receipts), ["Alpha", "Delta"])
        alpha = next(doc for doc in receipts if doc["module"] == "Alpha")
        self.assertEqual((alpha["runId"], alpha["fingerprint"], alpha["source"]), ("42", FP_A, "run"))

        evidence.append({"passed": [], "failed": census["Gamma"]})
        report, _ = assemble(evidence, receipt_plan, fingerprints, census, self.identity)
        self.assertEqual(report["Gamma"]["status"], "failed")

    def test_without_a_plan_every_module_counts_as_run(self):
        report, receipts = assemble([{"passed": CENSUS["Beta"], "failed": []}], None,
                                    self.fingerprints, CENSUS, self.identity)
        self.assertEqual(report, {"Alpha": {"status": "incomplete", "missing": 2}, "Beta": {"status": "ran"}})
        self.assertEqual([doc["fingerprint"] for doc in receipts], [FP_B])

    def test_trx_files_are_read_as_evidence(self):
        trx = self.root / "trx" / "a" / "x.trx"
        trx.parent.mkdir(parents=True)
        ns = "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"
        trx.write_text(
            f'<TestRun xmlns="{ns}"><TestDefinitions>'
            '<UnitTest id="1"><TestMethod className="N.T" name="x"/></UnitTest>'
            '<UnitTest id="2"><TestMethod className="N.T" name="y"/></UnitTest></TestDefinitions>'
            '<Results><UnitTestResult testId="1" testName="The_base_specification_holds(Beta)" outcome="Passed"/>'
            '<UnitTestResult testId="2" testName="Each_property_fires(Alpha,M1)" outcome="Failed"/></Results>'
            '</TestRun>', encoding="utf-8")
        (evidence,) = load_trx_dir(self.root / "trx")
        self.assertEqual(evidence["passed"], ["N.T.The_base_specification_holds(Beta)"])
        self.assertEqual(evidence["failed"], ["N.T.Each_property_fires(Alpha,M1)"])

    def test_stage_derives_each_key_from_the_receipt_and_refuses_a_malformed_one(self):
        receipts = self.root / "receipts"
        write_receipts([self.full("Alpha", FP_A, CENSUS["Alpha"]),
                        {"schema": 1, "kind": "partial", "module": "Beta", "fingerprint": FP_B,
                         "runId": "9", "shard": "s", "tier": "deterministic", "cases": []}], receipts)
        self.assertTrue((receipts / "Alpha" / "receipt.json").is_file())
        manifest = stage(receipts, self.root)
        self.assertEqual(sorted(entry["key"] for entry in manifest),
                         sorted([receipt_key("Alpha", FP_A), partial_key("Beta", FP_B, "9", "s", "deterministic")]))
        self.assertTrue(all((self.root / entry["path"]).is_file() for entry in manifest))
        write_json(receipts / "Gamma" / "receipt.json", {"module": "Gamma", "fingerprint": "nope"})
        with self.assertRaises(ValueError):
            stage(receipts, self.root)

    def test_cli_round_trip(self):
        fingerprints = self.root / "fp.json"
        write_json(fingerprints, {"schema": 1, "modules": {m: {"fingerprint": f, "inputs": []}
                                                           for m, f in self.fingerprints.items()}})
        census, categories = self.root / "census.json", self.root / "categories.json"
        write_json(census, CENSUS)
        write_json(categories, CATEGORIES)
        restored = self.root / "restored"
        restored.mkdir()
        write_json(restored / f"{receipt_key('Beta', FP_B)}.json", self.full("Beta", FP_B, CENSUS["Beta"]))
        out = self.root / "plan"
        summary = self.root / "summary.md"
        self.assertEqual(main(["plan", "--fingerprints", str(fingerprints), "--census", str(census),
                               "--case-categories", str(categories), "--restored-dir", str(restored),
                               "--output-dir", str(out), "--summary-file", str(summary)]), 0)
        self.assertEqual((out / "cached-modules.txt").read_text(encoding="utf-8"), "Beta\n")
        self.assertIn("1 of 2 module(s) cached", summary.read_text(encoding="utf-8"))

        legs = self.root / "legs" / "leg-1"
        legs.mkdir(parents=True)
        (legs / "tlc-evidence-a.jsonl").write_text(json.dumps(
            {"shard": "all", "tier": "deterministic", "filter": "TestCategory=TlcModule_Alpha",
             "passed": CENSUS["Alpha"], "failed": []}) + "\n", encoding="utf-8")
        cwd = os.getcwd()
        os.chdir(self.root)
        try:
            self.assertEqual(main(["partials", "--evidence-dir", "legs", "--plan-dir", "plan",
                                   "--receipts-dir", "partials"]), 0)
            self.assertEqual(main(["assemble", "--fingerprints", "plan/fingerprints.json",
                                   "--census", "plan/census.json", "--evidence-dir", "legs",
                                   "--receipt-plan", "plan/receipt-plan.json", "--receipts-dir", "receipts",
                                   "--summary-file", "summary.md", "--require-complete"]), 0)
            self.assertEqual(main(["stage", "--receipts-dir", "receipts", "--output-manifest", "save.json"]), 0)
        finally:
            os.chdir(cwd)
        manifest = read_json(self.root / "save.json")
        self.assertEqual([entry["key"] for entry in manifest], [receipt_key("Alpha", FP_A)])
        self.assertTrue((self.root / manifest[0]["path"]).is_file())
        self.assertEqual(len(list((self.root / "partials").glob("*/partial-*.json"))), 1)
        self.assertIn("1 ran, 1 cached, 0 failed, 0 incomplete", summary.read_text(encoding="utf-8"))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
