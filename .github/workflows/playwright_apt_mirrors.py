"""Attempt-local APT sources using only the runner's configured mirror candidates."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlsplit


class MirrorConfigurationError(ValueError):
    pass


def apt_output(*arguments, deadline):
    return subprocess.run(
        ["apt-config", *arguments], check=True, text=True, capture_output=True,
        timeout=max(0.001, deadline - time.monotonic()),
    ).stdout


def rotate_mirrors(text, step):
    candidates = []
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = line.split()
        if len(fields) != 2 or not re.fullmatch(r"priority:[1-9][0-9]*", fields[1]):
            raise MirrorConfigurationError("Expected unqualified mirrors with explicit priorities")
        uri = urlsplit(fields[0])
        if (uri.scheme not in ("http", "https") or not uri.hostname or uri.username
                or uri.password or uri.query or uri.fragment):
            raise MirrorConfigurationError("Unsupported configured mirror URI")
        candidates.append((int(fields[1].split(":")[1]), fields[0]))
    candidates.sort()
    if (len({priority for priority, _ in candidates}) != len(candidates)
            or len({uri for _, uri in candidates}) != len(candidates)
            or step >= len(candidates)):
        raise MirrorConfigurationError("No distinct configured mirror for this bounded retry")
    ordered = candidates[step:] + candidates[:step]
    print(f"Playwright dependency retry prefers configured mirror {ordered[0][1]}", flush=True)
    return "".join(f"{uri}\tpriority:{index}\n"
                   for index, (_, uri) in enumerate(ordered, 1))


def rewrite_sources(text, suffix, replace):
    if suffix == ".list":
        return "".join(
            replace(line) if re.match(r"^\s*deb(?:-src)?\s", line) else line
            for line in text.splitlines(keepends=True)
        )
    # Preserve every field, option, comment and unrelated repository verbatim.
    paragraphs = re.split(r"(\r?\n[ \t]*\r?\n)", text)
    for index, paragraph in enumerate(paragraphs):
        if re.search(r"(?im)^Enabled:\s*no\s*$", paragraph):
            continue
        lines = paragraph.splitlines(keepends=True)
        in_uris = False
        for number, line in enumerate(lines):
            if not line.lstrip().startswith("#"):
                if not line[:1].isspace():
                    in_uris = line.lower().startswith("uris:")
                if in_uris:
                    lines[number] = replace(line)
        paragraphs[index] = "".join(lines)
    return "".join(paragraphs)


@contextmanager
def mirror_overlay(step, deadline):
    if step not in (1, 2):
        raise MirrorConfigurationError("Only the existing second and third attempts may fail over")
    config = apt_output("dump", deadline=deadline)
    # A binary-specific directory override would bypass the attempt-local paths.
    if re.search(r"(?im)^Binary::[^ ]*::Dir(?:::| )", config):
        raise MirrorConfigurationError("Binary-specific APT directory overrides are unsupported")
    paths = {}
    output = apt_output(
        "shell", "SOURCES", "Dir::Etc::sourcelist/f",
        "PARTS", "Dir::Etc::sourceparts/d", deadline=deadline,
    )
    for assignment in shlex.split(output):
        key, value = assignment.split("=", 1)
        paths[key] = Path(value)
    if set(paths) != {"SOURCES", "PARTS"} or not all(p.is_absolute() for p in paths.values()):
        raise MirrorConfigurationError("Cannot resolve the active APT source paths")
    with tempfile.TemporaryDirectory(prefix="playwright-apt-") as temporary:
        root = Path(temporary)
        root.chmod(0o755)
        parts = root / "sources.list.d"
        parts.mkdir()
        mirrors = {}

        def replace(line):
            def replace_uri(match):
                original = Path(match[1])
                if original not in mirrors:
                    target = root / f"mirrors-{len(mirrors)}.txt"
                    target.write_text(rotate_mirrors(original.read_text(), step), encoding="utf-8")
                    mirrors[original] = target
                return f"mirror+file:{mirrors[original]}"
            return re.sub(r"(?<!\S)mirror\+file:(/[^\s]+)", replace_uri, line)

        def copy(source, target):
            text = source.read_bytes().decode("utf-8")
            target.write_bytes(rewrite_sources(text, source.suffix, replace).encode("utf-8"))
            shutil.copymode(source, target)

        sources = root / "sources.list"
        if paths["SOURCES"].exists():
            copy(paths["SOURCES"], sources)
        else:
            sources.touch()
        if paths["PARTS"].exists():
            for source in paths["PARTS"].iterdir():
                if source.is_file() and source.suffix in (".list", ".sources"):
                    copy(source, parts / source.name)
        if not mirrors:
            raise MirrorConfigurationError("Active APT sources have no supported configured mirror list")
        # APT_CONFIG loads first. Snapshot its fully merged configuration, then
        # prevent the original fragments/main file from overriding our source paths.
        # Trust, TLS, caches, dpkg paths and binary-specific options remain intact.
        empty_parts = root / "apt.conf.d"
        empty_parts.mkdir()
        empty_main = root / "empty.conf"
        empty_main.touch()
        overlay = root / "apt.conf"
        overrides = {
            "Dir::Etc::sourcelist": sources, "Dir::Etc::sourceparts": parts,
            "Dir::Etc::parts": empty_parts, "Dir::Etc::main": empty_main,
        }
        overlay.write_text(config + "\n" + "".join(
            f"{key} {json.dumps(str(value))};\n" for key, value in overrides.items()
        ), encoding="utf-8")
        overlay.chmod(0o600)
        yield dict(os.environ, APT_CONFIG=str(overlay))
