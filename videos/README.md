# Orleans.Lattice videos

The educational video series for Orleans.Lattice: short, narrated explainers
that give each kind of reader a clean way in, and then hand them to the
documentation.

This folder is a [HyperFrames](https://hyperframes.heygen.com/) workspace. A
video here is HTML, CSS and a paused GSAP timeline that headless Chrome renders
frame by frame and FFmpeg encodes, so the same source always produces the same
video. A video is reviewed, diffed and rebuilt like any other source file.

- **The plan** - audiences, the Build / Evaluate / Operate paths, the episode
  list, the production order, and the decisions still open - is in
  [series.md](series.md).
- **The look** - canvas, type, colour roles, the order-diagram vocabulary,
  motion, code, captions and audio - is in [frame.md](frame.md).
- **How the series is made, reviewed and released** - one item at a time, by
  a scheduled automation, approved by merging and released by a person - is in
  [Production, review and release](#production-review-and-release).
- **Agent conventions** are in the
  [video-production skill](../.github/skills/video-production/SKILL.md).

The first episode is the pilot, [Orleans.Lattice in three minutes](episodes/introduction/):
the front door every path links back to. [index.html](index.html) is a
workspace smoke test that proves the toolchain end to end.

## Getting started

You need Node.js 22 or newer and FFmpeg on your `PATH`. Docker is optional,
for byte-reproducible renders. Narration also needs the series voice's Python
environment (see [The series voice](#the-series-voice)).

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

npm run narrate -- introduction                   # speak the script, check every clip, master the track
npm run timeline -- introduction                  # stamp its timing into the composition
npm run check -- --episode introduction           # check the episode
npm run render -- --episode introduction --quality draft -o renders/introduction.mp4
```

## The series voice

The narration is Emma, read by Chatterbox (Resemble AI, MIT licence) and
cloned from [voice/reference.wav](voice/reference.wav); the settings are in
[voice/voice.json](voice/voice.json) and the reasons in
[series.md, "Voice"](series.md#voice). It runs locally on the CPU, in a Python
3.11 environment of its own, which `npm run voice:setup` makes in the per-user
state directory (see [Production, review and release](#production-review-and-release)),
where narration and auditions find it:

```bash
uv python install 3.11                            # or any Python 3.11
npm run voice:setup                               # or: npm run voice:setup -- --python <a python3.11>
```

To use an environment of your own instead, set `VIDEOS_VOICE_PYTHON` to its
interpreter.

The first narration downloads the voice model (about 3 GB, pinned to one
revision) and two speech recognisers (about 0.6 GB) from Hugging Face; after
that it needs no network. `npm run narrate` starts
[tools/voice_worker.py](tools/voice_worker.py) once, which loads them in a
minute or two, then speaks each cue and transcribes it; a clip that is not
heard exactly as the script says it is made again with the next seed. Expect
the voice to take eight to twenty times as long as the speech it makes, so a
three-minute episode takes the better part of an hour on a laptop, and a
re-run speaks only the cues that changed. `VIDEOS_VOICE_THREADS` sets how many
CPU threads it uses (8 by default).

The first engine, Kokoro, remains available for the voice audition set
(`npm run voice:samples`) and for comparison (`"provider": "kokoro"`;
`pip install kokoro-onnx soundfile`, and `HYPERFRAMES_PYTHON` when the CLI
cannot find that interpreter). Takes (`npm run audition`) are for the
Chatterbox voice only.

## Production, review and release

The series is made one item at a time, in the order [series.md](series.md) sets
("Production order"), which [series.json](series.json) holds as data. A
scheduled automation runs the
[Video Producer](../.github/agents/video-producer.agent.md) agent on the machine
that has the series voice, and a person can run it too. Each run does at most
one thing: it acts on the review feedback on the open episode pull request, or,
when none is open, makes the next item and opens its pull request to `main`.

- **One run at a time.** A run takes the production lease first
  (`npm run series -- lease take --owner <run>`), renews it while it works and
  releases it when it stops. A lease that is not renewed lapses by itself.
- **What outlives a worktree.** Each run works in a fresh worktree, so what must
  survive one lives in a per-user state directory outside every checkout:
  `%LOCALAPPDATA%\orleans-lattice\videos` on Windows,
  `$XDG_STATE_HOME/orleans-lattice/videos` elsewhere (`~/.local/state` when
  `XDG_STATE_HOME` is unset), or wherever `VIDEOS_HOME`
  names. It holds the series voice's Python environment (`npm run voice:setup`),
  a copy of every narration clip and take, and the lease. A fresh checkout
  copies clips and takes back from it, so an unchanged cue is never spoken
  again and a picked take is never lost.
- **Review.** The pull request's description is its review packet
  (`npm run packet`). It holds:
  - the video, playing in place from a review copy attached to the pull
    request;
  - its length, size, loudness and cut;
  - the moments to listen to, and why: a take picked by ear, a clip that did
    not pass its checks (a recogniser did not hear it exactly, or it is too
    slow, too fast or carries a stray sound), a clip heard exactly only after
    more than one attempt, words the voice has not said before, and lines
    changed since the published cut;
  - the script with its timings, and the takes there are to pick from;
  - the claims and their sources;
  - a ledger of the feedback acted on.

  Comment in plain words, with a time where it helps, or with
  `/retake <cue>`, `/pick <cue> <take>` or `/reword <cue> <text>`. The next run
  acts on every comment it has not answered.

  The automation acts as the repository's own account, so GitHub does not
  notify you of its pull requests. Watch
  [the open episode pull requests](https://github.com/NSTA1/Orleans.Lattice/pulls?q=is%3Apr+is%3Aopen+label%3Avideo-series)
  instead.
- **Approval is the merge**, and only a person merges. At most one episode pull
  request is open at a time, so episodes are approved in order. The required
  check backs that up: it fails any pull request that publishes an episode
  ahead of one before it (`npm run series -- check`, in the content gates of
  `ci.yml`).
- **Release is a second step.** Approved episodes wait on `main`, because the
  site is built from the newest release line, never from `main`. The
  [Promote videos](../.github/workflows/promote-videos.yml) workflow is
  dispatched only by a person. It copies the approved episodes onto that line,
  either all of them or those up to the item you name, checks the line, builds
  the site as a gate, then pushes and deploys. The next release wave publishes
  every approved episode anyway, because a line is cut from `main`.

## Commands

| Command | What it does |
| --- | --- |
| `npm run preview` | HyperFrames Studio on the workspace, with live reload |
| `npm run lint` | static checks on the smoke test and the shared components it mounts |
| `npm run check` | lint, plus runtime, layout, motion and contrast checks of `index.html` in headless Chrome |
| `npm run snapshot` | key frames of `index.html` as PNGs under `snapshots/` |
| `npm run <command> -- --episode <slug>` | any of the above, and `render`, on `episodes/<slug>/composition.html` instead of the smoke test |
| `npm run check:episodes` | `check` on every episode; one not narrated on this machine is checked against silence of its stamped length |
| `npm run render -- --episode <slug> --quality high -o renders/<slug>-high.mp4` | render an episode; naming the output also writes the render's receipt, a digest of what it was rendered from, which `publish` checks. `--warm` keeps FFmpeg and Chrome resident while the render starts, for a machine short of memory |
| `npm run render -- --docker ...` | render in Docker (pinned Chromium, fonts and FFmpeg) when output must be byte-reproducible |
| `npm run narrate -- <slug>` | speak `episodes/<slug>/SCRIPT.md` in the series voice, one cached clip per cue, each heard back by two local recognisers and made again with the next seed when it does not match the script; then master the joined track to the series loudness, and write the cue timeline and WebVTT captions |
| `npm run review -- <slug> --audio` | a page with the narration alone, every cue listed to play from, highlighting what changed and what the checks could not settle: approve the voice by ear before rendering |
| `npm run audition -- <slug> <cue>... [--takes N]` | several takes of a line (4 by default), each checked, on a page to listen and pick from; `--pick <cue>=<take>` uses a take in the narration. Takes and picks stay on this machine, under `renders/` and in the state directory; only the published cut is committed |
| `npm run phonemes -- <slug>` | for the Kokoro engine: how its phonemizer will read each cue, with words that have two readings flagged (`--flagged` for only those cues) |
| `npm run timeline -- <slug>` | stamp the narration's timeline into the episode's composition (`--check` fails if it is out of date) |
| `npm run review -- <slug>` | a local review page for a render of the episode (its `-high` render first, or the file `--video` names): the player with captions, its size, bit rate and delivered loudness, and the transcript (serve `videos/` over HTTP to watch it) |
| `npm run voice:samples` | the voice audition set, under `renders/voice-samples/` |
| `npm run snippets` | copy the compiled snippets from the companion pages into the compositions |
| `npm run snippets:check` | fail if a composition's code has drifted from its companion page |
| `npm run companions` | write each companion page's video block (from `episode.json` and the composition), transcript (from the script) and Where next (from the plan) |
| `npm run companions:check` | fail if a companion page has drifted from its episode, or `docs-site/media/` lacks a pinned file or holds one no page pins |
| `npm run publish -- <slug>` | while the render's receipt matches the sources, write the episode's video, captions and poster into `docs-site/media/` under a new cut, remove its earlier cut, and record the cut in `episode.json` and the companion page; every other page whose Where next names the episode now links to it |
| `npm run series -- check` | fail if `series.json` and `series.md` disagree, or an episode is published ahead of one before it in the production order; the required check runs it too |
| `npm run series -- status` | every item of the production order: done, next, held or to make |
| `npm run series -- next [--json]` | the next item to make, or the hold that stops the queue; `--json` adds the ending the plan gives it and its companion page's Where next |
| `npm run series -- ending <code>` | the ending the plan gives an episode, named by its item code or its slug, as narration to start its closing scene from |
| `npm run series -- endings` | fail if a published episode's closing scene does not name what the plan says it leads to |
| `npm run series -- lease take --owner <run>` | take the production lease (and `renew`, `release` or `status` it), so that one run makes the series at a time |
| `npm run packet -- <slug>` | the episode's review packet, the body of its pull request; `--review-copy` makes a copy of the video small enough to attach, and `--upload` attaches it, with `GH_TOKEN` set to the token of the account that raises the pull request |
| `npm run voice:setup` | make the series voice's Python environment in the state directory |
| `npm run ascii` | fail on any non-ASCII character in this folder |
| `npm test` | unit tests for the tools and the browser runtime |

Every command that runs the HyperFrames CLI runs the pinned one from
`node_modules`, with HyperFrames telemetry, update checks and skill installation
switched off: the project commands through [tools/hf.js](tools/hf.js), and the
Kokoro engine's speech through the runner it shares,
[tools/lib/hyperframes.js](tools/lib/hyperframes.js). Chatterbox narration and
auditions do not use the CLI: they run
[tools/voice_worker.py](tools/voice_worker.py) in the series voice's Python
environment. `snapshot` never sends frames to a hosted vision model unless you
pass `--describe` yourself.

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
  series.json                       the plan's production order, as data
  package.json, package-lock.json   the workspace and its pinned toolchain
  hyperframes.json, meta.json       HyperFrames project configuration
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
    episode.json                    its path, its place on it, the series items it completes, its poster's moment, and its published cut
    composition.html                the episode: shared scenes, its words, stamped timing
    assets/                         media no other episode uses, if any
  voice/                            the series voice: its settings, reference clip, Python requirements and lexicon;
                                    the Kokoro audition candidates, their audition script and heteronyms
  tools/                            workspace tooling and its tests
  renders/, snapshots/              output, never committed: renders, narration, takes, review pages
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
9. **Commit no renders, no narration audio and no takes.** What is committed is
   each episode's published cut, in `docs-site/media/`, which `npm run publish`
   writes from a reviewed render (see
   [series.md, Hosting - decided](series.md#hosting---decided)), and the
   voice's reference clip, `voice/reference.wav`, which defines the voice.
   Takes made to pick from (`npm run audition`) live in `renders/takes/`, and a
   picked take in the local clip cache, with a copy of both in the per-user
   state directory.

## CI

The videos lane ([videos.yml](../.github/workflows/videos.yml)) is advisory, not a
required check. It runs on pull requests that touch this folder, the companion
pages, the published media (`docs-site/media`), the parts of the site it reads
(`docs-site/template/public`, `docs-site/figures`, `docs-site/pages`,
`PACKAGES.md`) or the workflow itself, on pushes to `*/epic/**` integration
branches that touch the same paths, and by hand. It runs the unit tests, the
ASCII check, the snippet and companion-page checks, the series plan's check
and its endings check, `lint` and `check` on the smoke test and
`check:episodes` on every episode, then draft-renders the smoke test and
uploads it as an artifact. Narration audio is not committed, so CI checks each
episode's pictures, timing and structure against silence of its stamped
length; hearing it means narrating it locally, or watching its published cut.

The repository's `build-and-test` job skips its package tests for a change
confined to `videos/`, as it does for `docs/` and `benchmark/`. The content
gates still scan every file here, and they run `node videos/tools/series.js
check`, so the order of the series is part of the required check: the videos
lane is advisory, and merging is how an episode is approved.

## Licences

HyperFrames and the Kokoro-82M voice model are Apache-2.0. Chatterbox
(`chatterbox-tts` and its model) and faster-whisper are MIT, and the Whisper
models it runs are MIT too. GSAP is used under its standard no-charge licence.
None of them is vendored: HyperFrames and GSAP arrive from npm, the Python
runtimes from pip, and the models are downloaded the first time narration
runs. The reference clip, `voice/reference.wav`, is Kokoro's Emma reading this
series' own script.
