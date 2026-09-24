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

The first episode is the pilot, [Orleans.Lattice in three minutes](episodes/introduction/):
the front door every path links back to. [index.html](index.html) is a
workspace smoke test that proves the toolchain end to end.

## Getting started

You need Node.js 22 or newer and FFmpeg on your `PATH`. Narration also needs
Python 3 with Kokoro (`pip install kokoro-onnx soundfile`); if the CLI cannot
find it, for example because it is in a virtual environment, set
`HYPERFRAMES_PYTHON` to that interpreter. Docker is optional, for
byte-reproducible renders.

The videos are drawn in the documentation site's design system, which the
workspace reads from `docs-site/`: its tokens, fonts and mark in
`template/public/`, its join-figure scenarios in `figures/join-figures.json`,
the words its home page puts on screen in `pages/index.md`, and the package
catalogue in `PACKAGES.md` beside it (see [frame.md](frame.md)). Every command
that loads a composition copies them in first and stops if any is missing. To
work against a branch that is changing the design, set `VIDEOS_DOCS_SITE` to
that checkout's `docs-site`.

```bash
cd videos
npm ci
npm run preview                                   # HyperFrames Studio with live reload
npm run check                                     # lint, runtime, layout, motion and WCAG contrast
npm run render -- --quality draft -o renders/smoke.mp4

npm run narrate -- introduction                   # speak the script, master the track
npm run timeline -- introduction                  # stamp its timing into the composition
npm run check -- --episode introduction           # check the episode
npm run render -- --episode introduction --quality draft -o renders/introduction.mp4
```

## Commands

| Command | What it does |
| --- | --- |
| `npm run preview` | HyperFrames Studio on the workspace, with live reload |
| `npm run lint` | static checks on the smoke test and the shared components it mounts |
| `npm run check` | lint, plus runtime, layout, motion and contrast checks of `index.html` in headless Chrome |
| `npm run snapshot` | key frames of `index.html` as PNGs under `snapshots/` |
| `npm run <command> -- --episode <slug>` | any of the above, and `render`, on `episodes/<slug>/composition.html` instead of the smoke test |
| `npm run check:episodes` | `check` on every episode; one not narrated on this machine is checked against silence of its stamped length |
| `npm run render -- --episode <slug> --quality high -o renders/<slug>-high.mp4` | render an episode; naming the output also writes the render's receipt, a digest of what it was rendered from, which `publish` checks |
| `npm run render -- --docker ...` | render in Docker (pinned Chromium, fonts and FFmpeg) when output must be byte-reproducible |
| `npm run narrate -- <slug>` | speak `episodes/<slug>/SCRIPT.md` in the series voice, one cached clip per cue, and master the joined track to the series loudness; also writes the cue timeline and WebVTT captions |
| `npm run phonemes -- <slug>` | how the voice will read each cue - its spoken form and phonemes - with words that have two readings flagged; run it before narrating (`--flagged` for only those cues) |
| `npm run timeline -- <slug>` | stamp the narration's timeline into the episode's composition (`--check` fails if it is out of date) |
| `npm run review -- <slug>` | a local review page for the episode's latest render: the player with captions, its size, bit rate and delivered loudness, and the transcript (serve `videos/` over HTTP to watch it) |
| `npm run voice:samples` | the voice audition set, under `renders/voice-samples/` |
| `npm run snippets` | copy the compiled snippets from the companion pages into the compositions |
| `npm run snippets:check` | fail if a composition's code has drifted from its companion page |
| `npm run companions` | write each companion page's video block (from `episode.json` and the composition) and transcript (from the script) |
| `npm run companions:check` | fail if a companion page has drifted from its episode, or `docs-site/media/` lacks a pinned file or holds one no page pins |
| `npm run publish -- <slug>` | while the render's receipt matches the sources, write the episode's video, captions and poster into `docs-site/media/` under a new cut, remove its earlier cut, and record the cut in `episode.json` and the companion page |
| `npm run ascii` | fail on any non-ASCII character in this folder |
| `npm test` | unit tests for the tools and the browser runtime |

Every command runs the pinned CLI from `node_modules` through
[tools/hf.js](tools/hf.js), which switches off HyperFrames telemetry, update
checks and skill installation. `snapshot` never sends frames to a hosted vision
model unless you pass `--describe` yourself.

The CLI's project commands open only `<project>/index.html`, and its lint finds
other compositions only under a folder named `compositions/`. `--episode`
therefore stands the episode's composition in as `index.html` for the length of
the command, keeping the smoke test in `renders/` meanwhile and putting it back
afterwards, however the command ends. Nothing else changes: an episode's paths
are root-relative, exactly as the smoke test's are.

## Layout

```text
videos/
  README.md, series.md, frame.md    how to work here, the plan, the look
  package.json, hyperframes.json    the workspace
  index.html                        workspace smoke test, not an episode
  shared/                           everything more than one episode uses
    brand/brand.css                 imports the site's design system; adds the camera sizes and notation
    brand/motion.js                 the site's eases and the join figure's timings, for GSAP
    brand/scene.js                  what every scene does the same way: beats, exits, the site's words
    brand/code.js                   colours code in the site's syntax roles
    brand/site/                     the copy of the site's design system and words, never committed
    components/                     the scenes, variable-driven: title-card, remember, many-places,
                                    store-overview, cluster, join-diagram, seams, journey, ways
  episodes/<slug>/                  everything one episode owns
    BRIEF.md, SCRIPT.md             the brief, and the narration it is timed from
    STORYBOARD.md                   what is on screen for each cue
    episode.json                    its path, its place on it, its poster's moment, and its published cut
    composition.html                the episode: shared scenes, its words, stamped timing
    assets/                         media no other episode uses, if any
  voice/                            series voice, audition candidates, pronunciation lexicon, heteronyms
  tools/                            workspace tooling and its tests
  renders/, snapshots/              output, never committed
```

Nothing that belongs to one episode sits at the root, and nothing two episodes
could use is copied into either of them: a scene or an asset that a second
episode needs moves to `shared/` and takes variables. An episode's slug names
its folder, its narration (`renders/narration/<slug>/`), its render and its
companion page. Episodes are not grouped by reader path: the path is recorded
in `episode.json` and in [series.md](series.md), so re-cutting the paths never
moves a file.

Each episode's companion page lives with the rest of the documentation, at
`docs/videos/<slug>.md`. It holds the episode's video block, which the
documentation site replaces with its player, the transcript, and the compiled
snippets the video shows. The published video, its captions and its poster are
committed to `docs-site/media/`, which the site plays (`npm run publish`).

## Rules

1. **Plain ASCII only.** `npm run ascii` enforces it here, and the repository's
   hygiene gates reject em-dashes and mojibake in every tracked file. Write a
   symbol that must appear on screen as an HTML entity (`&#8852;` for a join).
2. **Never run `hyperframes init` in this folder, and never install skills into
   the repository.** `init` writes an `AGENTS.md` and a `CLAUDE.md` full of
   em-dashes. Install the HyperFrames agent skills for your user instead:
   `npx skills add heygen-com/hyperframes -g`.
3. **Paths are root-relative** (`shared/...`, `episodes/...`, `renders/...`,
   `node_modules/...`), including in files under `shared/` and `episodes/`.
   Compositions are served with this folder as their base URL, and `lint`
   rejects `../`.
4. **Nothing is fetched at render time.** GSAP and its plugins come from
   `node_modules`, not a CDN, and media are local files. Set type only with
   `var(--lv-font-sans)` and `var(--lv-font-mono)`: the site's own stacks name
   fallback families that the renderer would fetch from Google Fonts.
5. **Review registry blocks before committing them.** `hyperframes add` installs
   from the upstream registry into `shared/`; run `npm run ascii` and read what
   arrived.
6. **Code on screen comes only from compiled snippets** (`npm run snippets`),
   and the words on screen from the site where the site has them: a variable
   written `site:home.thesis` reads the home page, and package lists are
   generated from `PACKAGES.md`.
7. **Timing is stamped, never typed.** A clip's `data-start` and
   `data-duration`, its beats and the episode's length come from the narration
   (`npm run timeline`). Change the script, narrate, and stamp again.
8. **One place for each thing.** An episode's own material stays in its
   folder, anything reused lives once in `shared/`, and nothing goes at the
   root.
9. **Commit no renders and no narration audio.** What is committed is each
   episode's published cut, in `docs-site/media/`, which `npm run publish`
   writes from a reviewed render (see
   [series.md, Hosting - decided](series.md#hosting---decided)).

## CI

The videos lane ([videos.yml](../.github/workflows/videos.yml)) runs on pull
requests that touch this folder, the companion pages, the published media
(`docs-site/media`), or the parts of the site it reads
(`docs-site/template/public`, `docs-site/figures`, `docs-site/pages`,
`PACKAGES.md`). It runs the unit tests, the ASCII check, the snippet and
companion-page checks, `lint` and `check` on the smoke test and
`check:episodes` on every episode, then draft-renders the smoke test and
uploads it as an artifact. Narration audio is not committed, so CI checks each
episode's pictures, timing and structure against silence of its stamped
length; hearing it means narrating it locally, or watching its published cut.

The repository's `build-and-test` job skips its package tests for a change
confined to `videos/`, as it does for `docs/` and `benchmark/`. The content
gates still scan every file here.

## Licences

HyperFrames and the Kokoro-82M voice model are Apache-2.0. GSAP is used under
its standard no-charge licence. None of them is vendored: all three arrive from
npm or pip at install time.
