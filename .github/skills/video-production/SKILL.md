---
name: video-production
description: Orleans.Lattice educational video series conventions. Use when creating, editing, reviewing, narrating or rendering anything under videos/ (the HyperFrames workspace) or a companion page under docs/videos/ - episodes, shared components, scripts, the series voice, captions, or the videos CI lane.
---

# Video production

The video series lives in `videos/`, a [HyperFrames](https://hyperframes.heygen.com/)
workspace: every video is HTML, CSS and a paused GSAP timeline, rendered
deterministically to MP4. Read these before changing anything there:

- `videos/README.md` - how to work in the folder: commands, layout, rules, CI.
- `videos/series.md` - the plan: audiences, the Build / Evaluate / Operate
  paths, the episode list, where to start, open decisions.
- `videos/frame.md` - the camera design system: type scale, colour roles, the
  order-diagram vocabulary, motion, code, captions, audio.

For HyperFrames itself (the composition contract, timing attributes, GSAP
adapters, the CLI), use the upstream HyperFrames agent skills. Install them for
your user, never into this repository: `npx skills add heygen-com/hyperframes -g`.

## Rules that are easy to break

1. **Never run `hyperframes init` in the repository.** It writes `AGENTS.md`
   and `CLAUDE.md` containing em-dashes, and the em-dash gate scans every
   tracked file. The workspace is already initialised.
2. **Plain ASCII in every file under `videos/`**, scripts and on-screen text
   included. Run `npm run ascii`. Write symbols as HTML entities (`&#8852;`).
   Model-written narration and imported registry blocks are the usual sources
   of stray em-dashes and smart quotes.
3. **Root-relative paths in every composition**, including files under
   `compositions/`: `assets/brand/brand.css`, not `../../assets/brand/brand.css`.
   Compositions are served with `videos/` as their base URL, and `lint` rejects
   `../`. `data-composition-src` resolves from the workspace root too.
4. **No network at render time.** Load GSAP from
   `node_modules/gsap/dist/gsap.min.js`, never a CDN. Fonts and media are local.
5. **No literal colours, fonts or sizes in a composition.** Use the docs
   site's tokens by the site's names (`--lt-surface`, `--lt-ink`,
   `--lt-diagram-join`, ...) and the camera tokens (`--lv-type-*`,
   `--lv-safe-*`), and set type only with `var(--lv-font-sans)` and
   `var(--lv-font-mono)`. Never use the site's `--lt-font-*` stacks directly:
   the renderer fetches every fallback family they name from Google Fonts.
   The yellow marker (`--lt-marker`) means the join or "you are here", and
   nothing else.
6. **Code on screen comes only from a companion page.** Put the code in
   `docs/videos/<slug>.md` as a column-0 ` ```csharp verify ` fence with
   `<!-- video-snippet: <id> -->` on the line directly above, show it with
   `<code data-snippet="<id>"></code>`, and run `npm run snippets`. The
   repository compiles the fence; `npm run snippets:check` fails CI on drift.
7. **Run the CLI through the npm scripts** (`npm run lint`, `npm run check`,
   `npm run render -- ...`). They use the pinned CLI with telemetry, update
   checks and skill installs off. `npx hyperframes@latest` bypasses the pin.
8. **Commit no renders and no narration audio.** `renders/` and `snapshots/`
   are ignored; hosting is an open decision in `series.md`.
9. **Label unreleased packages on screen**, and make no claim the corpus does
   not make.

## Making an episode

Follow "How an episode is made" in `videos/series.md`: brief, script (fact-
checked by the Docs agent before it is locked), `npm run narrate -- <slug>`,
storyboard, composition timed from `renders/narration/<slug>/cues.json`,
companion page, `npm run check`, render. An episode's slug is kebab-case and is
shared by `episodes/<slug>/`, `compositions/episodes/<slug>.html` and
`docs/videos/<slug>.md`.

## Gotchas

- `check`, `snapshot` and `lint` take a project directory and default to
  `index.html`; only `render` takes `-c <composition>`. An episode is rendered
  with `npm run render -- -c compositions/episodes/<slug>.html -o renders/<slug>.mp4`.
- A sub-composition in a `<template>` wrapper renders only through a root that
  includes it. Episodes are full HTML documents; shared components are
  templates.
- `hyperframes snapshot` sends frames to Gemini whenever `GEMINI_API_KEY` is
  set. `tools/hf.js` adds `--describe false` unless you pass `--describe`.
- Narration needs Python with `kokoro-onnx` and `soundfile`; the first run
  downloads the Kokoro model (about 340 MB). The CLI's own interpreter probe
  can miss a virtual environment, or time out on a loaded machine, and report
  Kokoro as not installed: set `HYPERFRAMES_PYTHON` to the interpreter.
- `check` fails on WCAG contrast. On paper the marker yellow is never a text
  colour; set text on it in `--lt-marker-ink`, and ring a marker node in ink.
- The design system is read from `docs-site/` (`template/public/` and
  `figures/join-figures.json`) before every preview, check and render, and the
  command stops if any part is missing. To work against a branch that is
  changing the design, set `VIDEOS_DOCS_SITE` to that checkout's `docs-site`.
- The join figure takes its labels from the site's scenario for it, by id
  (`"scenario": "gcounter"`); never restate them. Only the diamond layout is
  ported; a chain scenario (Max/Min-Register, OR-Set/OR-Flag) fails the render
  until its geometry is ported with the first episode that needs it.
- Colours are tweened as values read from the tokens once
  (`getComputedStyle(root).getPropertyValue("--lt-...")`), never with CSS
  transitions, which are not seekable.
- Rendering launches one Chrome per worker (about 256 MB each); on a loaded
  machine pass `--workers 1`.
- Before rendering, the CLI probes `chrome --version` and `ffmpeg -version`
  with a 5-second timeout. On a machine short of memory a cold start can miss
  it and the render stops with "Failed to run ... --version". Run the command
  again once the binaries are warm; it is not a workspace fault.
