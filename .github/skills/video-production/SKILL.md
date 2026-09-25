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
   `shared/` and `episodes/`: `shared/brand/brand.css`, not
   `../../shared/brand/brand.css`. Compositions are served with `videos/` as
   their base URL, and `lint` rejects `../`. `data-composition-src` resolves
   from the workspace root too.
4. **One place for each thing.** An episode's own material lives in
   `episodes/<slug>/` (brief, script, storyboard, `composition.html`, and an
   `assets/` folder only for media no other episode uses). Anything a second
   episode could use lives once in `shared/` and takes variables. Nothing goes
   at the `videos/` root, and a test fails if it does.
5. **No network at render time.** Load GSAP from
   `node_modules/gsap/dist/gsap.min.js`, never a CDN. Fonts and media are local.
6. **No literal colours, fonts or sizes in a composition.** Use the docs
   site's tokens by the site's names (`--lt-surface`, `--lt-ink`,
   `--lt-diagram-join`, ...) and the camera tokens (`--lv-type-*`,
   `--lv-safe-*`), and set type only with `var(--lv-font-sans)` and
   `var(--lv-font-mono)`. Never use the site's `--lt-font-*` stacks directly:
   the renderer fetches every fallback family they name from Google Fonts.
   The yellow marker (`--lt-marker`) means the join or "you are here", and
   nothing else. Use the shared notation in `shared/brand/brand.css`
   (`.lv-scene`, `.lv-heading`, `.lv-node`, `.lv-status`, `.lv-code`) rather
   than drawing a new device.
7. **Words on screen come from the site where it has them.** A variable
   written `site:home.thesis` (or any path into `home.js` or `packages.js`)
   is read from the site's own words; package lists are generated from
   `PACKAGES.md`, released packages only and never the Explorer.
8. **Code on screen comes only from a companion page.** Put the code in
   `docs/videos/<slug>.md` as a column-0 ` ```csharp verify ` fence with
   `<!-- video-snippet: <id> -->` on the line directly above, show it with
   `<code data-snippet="<id>"></code>`, and run `npm run snippets`. The
   repository compiles the fence; `npm run snippets:check` fails CI on drift.
9. **Run the CLI through the npm scripts** (`npm run lint`, `npm run check`,
   `npm run render -- ...`, each with `--episode <slug>` for an episode). They
   use the pinned CLI with telemetry, update checks and skill installs off.
   `npx hyperframes@latest` bypasses the pin.
10. **Never type timing.** A clip's `data-start` and `data-duration`, its
    `beats` and `end`, the root's `data-duration` and the narration block are
    stamped by `npm run timeline -- <slug>`. Change the script, narrate, stamp.
11. **Commit no renders and no narration audio.** `renders/` and `snapshots/`
    are ignored. What is committed is each episode's published cut, in
    `docs-site/media/`, written by `npm run publish -- <slug>` from a reviewed
    render (`series.md`, "Hosting - decided").
12. **Label unreleased packages on screen**, and make no claim the corpus does
    not make.

## Making an episode

Follow "How an episode is made" in `videos/series.md`: brief, script (fact-
checked by the Docs agent before it is locked), `npm run narrate -- <slug>`,
storyboard, `episodes/<slug>/composition.html` built from shared scenes with a
`data-scene` per script scene, `npm run timeline -- <slug>`,
`episodes/<slug>/episode.json` (path, order, poster moment), companion page
(`npm run companions`, `npm run snippets`), `npm run check -- --episode
<slug>`, `npm run render -- --episode <slug> --quality high -o
renders/<slug>-high.mp4`, review, then `npm run publish -- <slug>` and commit
the three files it writes to `docs-site/media/` with the episode. An episode's
slug is kebab-case and is shared by `episodes/<slug>/`,
`renders/narration/<slug>/`, `renders/<slug>-high.mp4`,
`docs/videos/<slug>.md` and `docs-site/media/<slug>-<cut>.*`. The
introduction is the worked example.

## Gotchas

- `check`, `snapshot` and `lint` take a project directory and open only its
  `index.html`, and `lint` finds other compositions only under a folder named
  `compositions/`. `tools/hf.js --episode <slug>` stands the episode in as
  `index.html` for one command, keeps the smoke test in `renders/` meanwhile
  (never at the root: a second root composition fails lint), and puts it back
  however the command ends. An interrupted run is repaired on the next one.
- `lint` fails when an `<audio src>` file is missing, and narration is not
  committed. `npm run check:episodes` checks an episode that is not narrated
  on this machine against a silent stand-in of its stamped length, and removes
  the stand-in afterwards.
- Loudness is measured as delivered: the render puts the mono narration on
  both channels of a stereo track, which reads 3 LU louder than the mono file,
  so `npm run narrate` measures and masters it in stereo.
- A sub-composition in a `<template>` wrapper renders only through a root that
  includes it. Episodes are full HTML documents; shared components are
  templates.
- `hyperframes snapshot` sends frames to Gemini whenever `GEMINI_API_KEY` is
  set. `tools/hf.js` adds `--describe false` unless you pass `--describe`.
- Narration is Chatterbox, cloned from `voice/reference.wav`, in a Python 3.11
  environment of its own (`voice/requirements.txt`); set `VIDEOS_VOICE_PYTHON`
  to its interpreter. `tools/voice_worker.py` loads the model once per run and
  prints its protocol on stdout behind a `@@voice ` mark, with everything else
  on stderr (in `renders/narration/<slug>/voice.log`). Never name a Python file
  after a package it imports: the worker was once `chatterbox.py`, and Python
  imported it in place of the `chatterbox` package.
- A generative voice slips ("Neither awaits its turn"), so every clip is heard
  back by two recognisers and remade until it matches the script. A
  recogniser tends to miss the first word of audio that starts with no
  silence, so the worker pads what it hears, not the clip. When a clip never
  passes, `npm run narrate` lists the cue; listen to it before publishing.
  Chatterbox pauses at a hyphen, so its respellings in `voice/lexicon.json`
  have none ("Orleens").
- A clip's name hashes what shapes the voice, including the pinned lines of
  `voice/requirements.txt` (not its comments): changing a setting, the
  reference clip or a pin re-speaks every cue, which takes the better part of
  an hour per episode.
- The Kokoro engine (`"provider": "kokoro"`) needs Python with `kokoro-onnx`
  and `soundfile`; set `HYPERFRAMES_PYTHON` when the CLI's probe cannot find
  it. Its phonemizer reads a word with two readings one way whatever the
  sentence ("lives" as the plural of life), which is why its lexicon forms
  exist; `npm run phonemes -- <slug>` shows what it will receive.
- `check` fails on WCAG contrast. On paper the marker yellow is never a text
  colour; set text on it in `--lt-marker-ink`, and ring a marker node in ink.
- The design system is read from `docs-site/` (`template/public/`,
  `figures/join-figures.json` and the home page's words in `pages/index.md`)
  and from `PACKAGES.md` beside it, before every preview, check and render, and
  the command stops if any part is missing or the home page has changed shape.
  To work against a branch that is changing the design, set `VIDEOS_DOCS_SITE`
  to that checkout's `docs-site`.
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
  again once the binaries are warm; it is not a workspace fault. With every
  core busy (other sessions' test runs), the FFmpeg probe can fail many times
  in a row even though `ffmpeg -version` answers in 50 ms from a shell: under
  memory pressure the 180 MB static binary's pages are evicted during the
  render's minute-long start-up, and re-reading them misses the 5 seconds.
  Keep FFmpeg resident while the render starts - one idle
  `ffmpeg -re -f lavfi -i anullsrc=r=8000:cl=mono -t 2400 -f null NUL` in the
  background, plus a loop running `ffprobe -version` - and stop them
  afterwards; with that, the pilot's render passed its probes first time.
  `check` and `snapshot` have no such probe.
