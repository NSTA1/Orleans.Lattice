#!/usr/bin/env node
// Makes the series voice's Python environment in the per-user state directory
// (tools/lib/layout.js, stateDir), where narration and auditions find it
// without VIDEOS_VOICE_PYTHON - so a scheduled run in a fresh worktree has a
// voice without any setting of its own:
//
//   npm run voice:setup [-- --python <python3.11>] [--force]
//
// It needs a Python 3.11 (chatterbox-tts pins torch 2.6.0, which has no wheels
// for newer Pythons): the one --python names, or else the first of
// `uv python find 3.11`, `py -3.11` and `python3.11` that answers. It creates
// the environment (afresh with --force), installs voice/requirements.txt with
// pip, and checks that the voice's packages import. pip reads PIP_INDEX_URL
// and PIP_EXTRA_INDEX_URL, for a CPU-only torch index on Linux, say. The
// models download on the first narration, into the Hugging Face cache.
import { spawnSync } from "node:child_process";
import { existsSync, rmSync } from "node:fs";
import path from "node:path";
import { workspaceRoot } from "./lib/hyperframes.js";
import { stateDir } from "./lib/layout.js";

const argv = process.argv.slice(2);
const fail = (message) => {
  console.error(`voice:setup: ${message}`);
  process.exit(1);
};
const run = (command, args, options = {}) => spawnSync(command, args, { encoding: "utf8", windowsHide: true, ...options });

// A Python that answers as 3.11, by the interpreter it really is.
const asPython311 = (command, args = []) => {
  const answer = run(command, [...args, "-c", "import sys; print(sys.executable); print('%d.%d' % sys.version_info[:2])"]);
  if (answer.status !== 0) return null;
  const [executable, version] = answer.stdout.trim().split(/\r?\n/);
  return version === "3.11" ? executable : null;
};

const target = path.join(stateDir, "voice");
const python = process.platform === "win32" ? path.join(target, "Scripts", "python.exe") : path.join(target, "bin", "python");

if (argv.includes("--force") && existsSync(target)) {
  console.log(`voice:setup: removing ${target}`);
  rmSync(target, { recursive: true, force: true });
}

if (!existsSync(python)) {
  const named = argv.includes("--python") ? argv[argv.indexOf("--python") + 1] : null;
  const uvFound = named ? null : run("uv", ["python", "find", "3.11"]);
  const base = named
    ? asPython311(named)
    : (uvFound.status === 0 && asPython311(uvFound.stdout.trim())) || asPython311("py", ["-3.11"]) || asPython311("python3.11");
  if (!base) {
    fail(
      named
        ? `'${named}' is not a Python 3.11`
        : "found no Python 3.11; install one (for example 'uv python install 3.11') and pass it with --python <interpreter>",
    );
  }
  console.log(`voice:setup: making the environment at ${target}, from ${base}`);
  if (run(base, ["-m", "venv", target], { stdio: "inherit" }).status !== 0 || !existsSync(python)) fail("could not make the environment");
}

console.log("voice:setup: installing voice/requirements.txt (the first time, torch and the voice's packages: about 1 GB)");
const requirements = path.join(workspaceRoot, "voice", "requirements.txt");
if (run(python, ["-m", "pip", "install", "--disable-pip-version-check", "-r", requirements], { stdio: "inherit" }).status !== 0) {
  fail("pip could not install voice/requirements.txt; see its output above");
}
const check = run(python, ["-c", "import chatterbox, faster_whisper, torch; print(torch.__version__)"]);
if (check.status !== 0) fail(`the environment does not import the voice's packages:\n${check.stderr}`);
console.log(`voice:setup: ready (torch ${check.stdout.trim()}). Narration and auditions find ${python} without VIDEOS_VOICE_PYTHON.`);
