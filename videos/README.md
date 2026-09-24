# Orleans.Lattice videos

The educational video series for Orleans.Lattice: short, narrated explainers
that give each kind of reader a clean way in, and then hand them to the
documentation.

This folder is a [HyperFrames](https://hyperframes.heygen.com/) workspace. A
video here is HTML, CSS and a paused GSAP timeline that headless Chrome renders
frame by frame and FFmpeg encodes, so the same source always produces the same
video. A video is reviewed, diffed and rebuilt like any other source file.

- **The plan** - audiences, the Build / Evaluate / Operate paths, the episode
  list, where to start, and the decisions still open - is in
  [series.md](series.md).
- **The look** - canvas, type, colour roles, the order-diagram vocabulary,
  motion, code, captions and audio - is in [frame.md](frame.md).
- **Agent conventions** are in the
  [video-production skill](../.github/skills/video-production/SKILL.md).

No episode exists yet. [index.html](index.html) is a workspace smoke test that
proves the toolchain end to end.

## Getting started

You need Node.js 22 or newer and FFmpeg on your `PATH`. Narration also needs
Python 3 with Kokoro (`pip install kokoro-onnx soundfile`); if the CLI cannot
find it, for example because it is in a virtual environment, set
`HYPERFRAMES_PYTHON` to that interpreter. Docker is optional, for
byte-reproducible renders.

The videos are drawn in the documentation site's design system, which the
workspace reads from `docs-site/`: its tokens, fonts and mark in
`template/public/`, and its join-figure scenarios in `figures/join-figures.json`
(see [frame.md](frame.md)). Every command that loads a composition copies them
in first and stops if any is missing. To work against a branch that is changing
the design, set `VIDEOS_DOCS_SITE` to that checkout's `docs-site`.

```bash
cd videos
npm ci
npm run preview      # HyperFrames Studio with live reload
npm run check        # lint, runtime, layout, motion and WCAG contrast
npm run render -- --quality draft -o renders/smoke.mp4
```

## Commands

| Command | What it does |
| --- | --- |
| `npm run preview` | HyperFrames Studio on the workspace, with live reload |
| `npm run lint` | static checks on every composition |
| `npm run check` | lint, plus runtime, layout, motion and contrast checks of `index.html` in headless Chrome |
| `npm run snapshot` | key frames of `index.html` as PNGs under `snapshots/` |
| `npm run render -- -c compositions/episodes/<slug>.html -o renders/<slug>.mp4` | render an episode |
| `npm run render -- --docker ...` | render in Docker (pinned Chromium, fonts and FFmpeg) when output must be byte-reproducible |
| `npm run narrate -- <slug>` | narration clips, cue timeline and captions for `episodes/<slug>/SCRIPT.md` |
| `npm run voice:samples` | the voice audition set, under `renders/voice-samples/` |
| `npm run snippets` | copy the compiled snippets from the companion pages into the compositions |
| `npm run snippets:check` | fail if a composition's code has drifted from its companion page |
| `npm run ascii` | fail on any non-ASCII character in this folder |
| `npm test` | unit tests for the tools |

Every command runs the pinned CLI from `node_modules` through
[tools/hf.js](tools/hf.js), which switches off HyperFrames telemetry, update
checks and skill installation. `snapshot` never sends frames to a hosted vision
model unless you pass `--describe` yourself.

## Layout

```text
videos/
  index.html                 workspace smoke test, not an episode
  hyperframes.json           project settings
  compositions/components/   shared, variable-driven components
  compositions/episodes/     one standalone composition per episode, <slug>.html
  episodes/<slug>/           an episode's BRIEF.md, SCRIPT.md and STORYBOARD.md
  assets/brand/brand.css     imports the site's design system; adds the camera sizes
  assets/brand/motion.js     the site's eases and the join figure's timings, for GSAP
  assets/brand/site/         the copy of the site's design system, never committed
  voice/                     series voice, audition candidates, pronunciation lexicon
  tools/                     workspace tooling and its tests
  renders/, snapshots/       output, never committed
```

Each episode's companion page lives with the rest of the documentation, at
`docs/videos/<slug>.md`. It holds the transcript, the compiled snippets the
video shows, and the player.

## Rules

1. **Plain ASCII only.** `npm run ascii` enforces it here, and the repository's
   hygiene gates reject em-dashes and mojibake in every tracked file. Write a
   symbol that must appear on screen as an HTML entity (`&#8852;` for a join).
2. **Never run `hyperframes init` in this folder, and never install skills into
   the repository.** `init` writes an `AGENTS.md` and a `CLAUDE.md` full of
   em-dashes. Install the HyperFrames agent skills for your user instead:
   `npx skills add heygen-com/hyperframes -g`.
3. **Paths are root-relative** (`assets/...`, `compositions/...`,
   `node_modules/...`), including in files under `compositions/`. Compositions
   are served with this folder as their base URL, and `lint` rejects `../`.
4. **Nothing is fetched at render time.** GSAP and its plugins come from
   `node_modules`, not a CDN, and media are local files. Set type only with
   `var(--lv-font-sans)` and `var(--lv-font-mono)`: the site's own stacks name
   fallback families that the renderer would fetch from Google Fonts.
5. **Review registry blocks before committing them.** `hyperframes add` installs
   from the upstream registry; run `npm run ascii` and read what arrived.
6. **Code on screen comes only from compiled snippets** (`npm run snippets`).
7. **Commit no renders and no narration audio** until hosting is decided (see
   [series.md, Open decisions](series.md#open-decisions)).

## CI

The videos lane ([videos.yml](../.github/workflows/videos.yml)) runs on pull
requests that touch this folder or the site's design system
(`docs-site/template/public`, `docs-site/figures`). It runs the unit tests, the ASCII check, the
snippet drift check, `lint` and `check`, then draft-renders the smoke test and
uploads it as an artifact.

The repository's `build-and-test` job skips its package tests for a change
confined to `videos/`, as it does for `docs/` and `benchmark/`. The content
gates still scan every file here.

## Licences

HyperFrames and the Kokoro-82M voice model are Apache-2.0. GSAP is used under
its standard no-charge licence. None of them is vendored: all three arrive from
npm or pip at install time.
