# The Orleans.Lattice video series - plan

This is the plan for the series: who it is for, how it is shaped, what is in
it, where it starts, and the tooling and decisions behind it. How a frame looks
is in [frame.md](frame.md); how to work in this folder is in
[README.md](README.md).

## Contents

- [Purpose](#purpose)
- [Shape: one front door, three paths, two deep dives](#shape-one-front-door-three-paths-two-deep-dives)
- [Episodes](#episodes)
- [Format rules](#format-rules)
- [Where to start](#where-to-start)
- [How an episode is made](#how-an-episode-is-made)
- [Tooling in place](#tooling-in-place)
- [Voice](#voice)
- [Open decisions](#open-decisions)
- [Deferred and out of scope](#deferred-and-out-of-scope)

## Purpose

A set of short, narrated explainers that give each kind of reader a clean way
in, and then hand them to the documentation. A video is an entry point, not a
second copy of the docs: it states one idea well and links to the page that
holds the detail.

The series follows the same principles as the documentation site:

1. **Route every reader fast.** A developer, an evaluator and an operator each
   find their first video within a click of the front door.
2. **The repository is the source of truth.** Every claim traces to the corpus,
   and code on screen is code the repository compiles.
3. **Prove, do not assert.** Guarantees are stated as precisely as the docs
   state them, with the test, specification or measurement behind them.
4. **Honest status.** Unreleased and in-progress packages are labelled
   wherever they appear.
5. **One programming model, Local to Global.** The deployment journey is the
   organising story, not a feature list.

## Shape: one front door, three paths, two deep dives

The paths are the documentation site's reader paths, with the same names, so a
reader who arrives by video and a reader who arrives by page end up in the same
place.

```mermaid
flowchart TD
    Door["Front door<br/>Orleans.Lattice in three minutes"]
    Door --> Build["Build<br/>developers"]
    Door --> Evaluate["Evaluate<br/>architects and tech leads"]
    Door --> Operate["Operate<br/>operators"]
    Evaluate --> Secure["Deep dive: Secure"]
    Operate --> Secure
    Evaluate --> How["Deep dive: How it works"]
    Build --> How
```

- **Front door** (three minutes at most, for everyone): a first minute in plain
  words for any viewer - what state is, why keeping it in more than one place
  is hard, and what Orleans.Lattice does differently - then what the platform
  is, the three positions it takes, and the Local -> Team -> Global journey
  with an unchanged `ILattice` programming model. It ends by pointing each
  reader to a path.
- **Each path opens with an entry episode** that assumes only the front door.
  Later episodes on the path assume the entry episode.
- **Deep dives** are reached from the paths rather than from the front door:
  Secure from Evaluate and Operate, How it works from Evaluate and Build.

## Episodes

Sources are the pages each episode is drawn from and links on to. Every episode
is a proposal until its brief is written.

### Front door

| Episode | Beats | Sources |
| --- | --- | --- |
| Orleans.Lattice in three minutes | in plain words: what state is, why it is hard to keep in more than one place, and what Lattice does instead; then the store lives in the cluster; conflict resolution is algebraic (the join); everything else is a seam; Local -> Team -> Global with the same programming model; pick your path | [README](../README.md) "What is it?", "Why it exists", "The deployment journey"; [reference architecture](../reference-architecture.md) "Disaster recovery" |

### Build (developers)

| Episode | Idea | Sources |
| --- | --- | --- |
| **Hello, Lattice** (entry) | register the silo, resolve `ILattice`, write and read typed values | [README Quick Start](../README.md#quick-start), [API reference](../docs/lattice/api.md) |
| Scans and cursors | ordered, range-bounded scans; cursors that survive failover | [API reference](../docs/lattice/api.md) |
| Atomic writes | all-or-nothing across keys and across trees | [Atomic writes](../docs/lattice/atomic-writes.md) |
| TTL and soft delete | per-entry expiry; recovery inside the retention window | [TTL](../docs/lattice/ttl.md) |
| Going durable | from the in-memory WAL to the file and Azure Table backends | [File WAL](../docs/lattice.storage.file/README.md), [WAL storage providers](../docs/lattice/wal-storage-providers.md) |
| Moving in from another store | bulk-loading from Redis, a relational database or Cosmos DB | [External store migration](../docs/lattice/external-store-migration.md) |

### Evaluate (architects and tech leads)

| Episode | Idea | Sources |
| --- | --- | --- |
| **When Lattice fits** (entry) | the three positions against a database, a cache and a queue; the categories it composes into | [README](../README.md) "Why it exists", "What you can build" |
| The deployment journey | Local, Team, Global: what each stage adds, and what stays the same | [README](../README.md#the-deployment-journey) |
| The guarantees, and how they are tested | consistency, crash safety, convergence; chaos tests and the verification tier | [Consistency](../docs/lattice/consistency.md), [chaos tests](../docs/lattice/chaos-tests.md) |
| A reference estate | active-active across regions on Azure Container Apps | [Reference architecture](../reference-architecture.md) |

### Operate (operators)

| Episode | Idea | Sources |
| --- | --- | --- |
| **What the write-ahead log guarantees** (entry) | the durability boundary, and how the backend choice changes it | [WAL](../docs/lattice/wal.md), [WAL storage providers](../docs/lattice/wal-storage-providers.md) |
| Backup and cold restore | a shared sink as the source of truth; restoring into a fresh cluster | [Backup](../docs/lattice.backup/README.md), [disaster recovery](../docs/lattice.backup/disaster-recovery.md) |
| Scaling on a signal | the cluster-aggregate signal an autoscaler such as KEDA scrapes | [Scaling](../docs/lattice.scaling/README.md) |
| Metrics and dashboards | what the instruments mean and where they are charted | [Dashboards](../docs/lattice.dashboards/README.md) |
| Diagnosing a tree | reading a `DiagnoseAsync` report, symptom by symptom | [Troubleshooting](../docs/lattice/troubleshooting.md) |

### Deep dive: Secure (from Evaluate and Operate)

| Episode | Idea | Sources |
| --- | --- | --- |
| Fail-closed by default | default-deny policy per tree, prefix or key, on the core data path | [Security](../docs/lattice/security.md) |
| Identity | OIDC and Entra membership resolving credentials to subjects | [OIDC](../docs/lattice.membership.oidc/README.md), [Entra](../docs/lattice.membership.entra/README.md) |
| One gate for every surface | gRPC client, operator console and AI agent all pass the same check | [Security](../docs/lattice/security.md) |

### Deep dive: How it works (from Evaluate and Build)

| Episode | Idea | Sources |
| --- | --- | --- |
| Conflict-free merges in 90 seconds | two concurrent writes and their join: why merges need no lock and no consensus | [CRDT primitives](../docs/crdt/readme.md), [state primitives](../docs/lattice/state-primitives.md) |
| Clocks and version vectors | ordering events without a shared clock | [Version vector](../docs/crdt/versionvector.md) |
| Trees that split online | sharded B+ trees rebalancing under load without downtime | [Architecture](../docs/lattice/architecture.md), [tree structure](../docs/lattice/tree-structure.md) |
| Atomic commit without consensus | the protocol behind all-or-nothing writes | [Verified atomic commit](../docs/lattice/verified-atomic-commit.md) |
| How it is verified | TLA+, Coyote and chaos tests, and what each one proves | [Verified atomic commit](../docs/lattice/verified-atomic-commit.md), [verified WAL](../docs/lattice/verified-wal.md), [chaos tests](../docs/lattice/chaos-tests.md) |

## Format rules

- **Two to five minutes, one idea.** The front door is three minutes at most; a
  deep-dive concept can be ninety seconds.
- **Plain words first where the audience is mixed.** An episode that any
  viewer may open - the front door, and each path's entry episode - begins
  with a short part that uses no technical terms and one everyday example,
  drawn in the same notation as the rest, and then says "in technical terms"
  where the rest begins.
- **No presenter on screen.** Diagrams, code and narration only, so a change to
  the product is a re-render rather than a re-shoot.
- **Every episode ends on a next step**: the next episode on its path, and its
  companion page.
- **Evergreen or versioned.** How it works episodes describe the model and
  rarely change. Build and Operate episodes describe the API and the operating
  surface; they state the release line they describe and are re-rendered when
  it changes.
- **Built from shared scenes.** Diagrams and cards are shared components, so a
  fix to one propagates to every episode that uses it.
- **Only what has shipped.** An episode covers released packages; anything else
  is labelled as unreleased on screen.

## Where to start

1. **The tooling** (this folder as it stands): the workspace, its guards, the
   CI lane, the voice pipeline, and the site's design system read through the brand seam. Nothing here is an episode.
2. **Pilot: the front door** - [episodes/introduction/](episodes/introduction/),
   built from nine shared scenes: the title card; for everyone, what an
   application remembers and the same value kept in more than one place; then
   the store in its cluster, the cluster, the join, the core and its seams,
   the deployment journey, and the ways in. It is the root every path links back to, it forces the style,
   voice and pacing decisions before there are ten episodes to change, and its
   source is the most-reviewed prose in the repository. It is scripted,
   fact-checked, narrated, stamped, checked and published: the docs site plays
   it on the home page and under Videos. Expect to re-cut it after steps 3 and 4.
3. **Hello, Lattice**, the Build entry: it proves the compiled-code path, and
   the README Quick Start it draws on is already written as verified snippets.
4. **Conflict-free merges in 90 seconds**, the first How it works episode: it is
   evergreen, and it stress-tests the diagram vocabulary.

Together those cover the widest audiences and all three production modes:
narrative and diagram, code walkthrough, and concept animation.

## How an episode is made

1. **Brief** - `episodes/<slug>/BRIEF.md`: the path and audience, the one idea,
   the corpus pages it draws on, and what the viewer can do afterwards.
2. **Script** - `episodes/<slug>/SCRIPT.md`: narration under a
   `## Narration` heading, one paragraph per cue, `### <scene>` headings to
   label scenes, HTML comments for direction notes. Written form, plain ASCII,
   British spelling. The Docs agent fact-checks it against the corpus before it
   is locked, and `npm run phonemes -- <slug>` shows how the voice will read
   it, flagging words with two readings.
3. **Narration** - `npm run narrate -- <slug>`: one clip per cue in the series
   voice, cached by what it says so a re-run speaks only the cues that changed;
   the cues joined on a timeline and mastered to the series loudness; and
   WebVTT captions. A `<!-- pause N -->` comment in the script adds N seconds
   of silence, and a new scene waits a little longer than the next cue in a
   scene does (`voice/voice.json`).
4. **Storyboard** - `episodes/<slug>/STORYBOARD.md`: what is on screen for each
   cue.
5. **Composition** - `episodes/<slug>/composition.html`, built from the shared
   scenes in `shared/components/`. Each scene clip names the script scene it
   shows (`data-scene`), and `npm run timeline -- <slug>` stamps its window,
   its beats (one per cue) and the narration track from the timeline, so the
   pictures move on the words and nothing is timed by hand.
6. **Companion page** - `docs/videos/<slug>.md`: a short introduction whose
   first sentence is the episode's one-line idea, then the video block and the
   transcript, both written by `npm run companions`, and the compiled snippets
   shown on screen (`npm run snippets`). `episodes/<slug>/episode.json` names
   the episode's path, its place on it, and the moment its poster shows.
7. **Check and render** - `npm run check -- --episode <slug>`, then
   `npm run render -- --episode <slug> --quality high -o renders/<slug>-high.mp4`,
   which also writes the render's receipt: what it was rendered from.
8. **Review** - `npm run review -- <slug>`: the local review page. Watch it
   once with sound and once muted.
9. **Publish** - `npm run publish -- <slug>`: while the render's receipt still
   matches the sources, it writes the video, its captions and its poster into
   `docs-site/media/`, named by their cut, removes the episode's earlier cut,
   and records the new cut in `episode.json` and the companion page. Commit
   the three files with the episode: the pull request carries the video, and
   the site plays what is committed.

## Tooling in place

What is in place, and why.

| Item | Why | Where |
| --- | --- | --- |
| No upstream scaffolding in git | `hyperframes init` writes `AGENTS.md` and `CLAUDE.md` with dozens of em-dashes, and the em-dash gate scans every tracked file in the repository | skills are installed per user; [README.md](README.md) rules |
| An ASCII check | catches non-ASCII text from imported blocks and model-written scripts before the repository gates do | `npm run ascii` |
| Media file types classified | the hygiene scanner fails every content gate on a tracked file whose type it does not know, and the repository tracked no video or audio before this | `test/shared/Orleans.Lattice.Testing/Hygiene/HygieneFiles.cs` |
| Its own CI lane | a change here matches no package, so `build-and-test` would otherwise fan out to the full test suite on every video pull request | `videos/**` is carved out in `ci.yml`; [videos.yml](../.github/workflows/videos.yml) runs this folder's checks |
| Outside `docs/` | the site build publishes and link-checks every markdown file under `docs/`, which would include every brief and script | this folder; companion pages alone go in `docs/videos/` |
| A pinned toolchain | HyperFrames is pre-1.0 and moves fast; renders must not change because a dependency did | exact versions in `package.json` and the lockfile; [tools/hf.js](tools/hf.js) runs the pinned CLI with telemetry, update checks and skill installs off |
| No network at render time | a render that fetches is neither reproducible nor local-first | GSAP and its plugins from `node_modules`; local media; font stacks limited to the site's self-hosted faces, because the renderer fetches any other named family from Google Fonts; `--docker` for byte-reproducible renders |
| The site's design system and words, read rather than copied | the videos must look like the documentation, say what it says, and follow it when it changes | `tools/hf.js` copies `docs-site/template/public` (tokens, fonts, mark), `docs-site/figures/join-figures.json`, the home page's words (`docs-site/pages/index.md`) and the package catalogue (`PACKAGES.md`) into an ignored folder before every render; [shared/brand/brand.css](shared/brand/brand.css) imports it and adds only camera sizes and the notation; [shared/brand/motion.js](shared/brand/motion.js) reads the site's eases; scenes quote the site through `site:` variables, the seams are generated, and the join figure reads each CRDT's scenario from the site |
| Compiled code on screen | a snippet that compiles today can rot tomorrow; the repository already compiles every verify fence | `npm run snippets`, `npm run snippets:check` |
| A local voice pipeline | one consistent voice, no account or key, reproducible from the script | [voice/](voice/), `npm run voice:samples`, `npm run narrate` |
| Loudness as delivered | every episode at -16 LUFS with the true peak below -1 dBTP, measured on the stereo track a viewer hears | `npm run narrate` masters the joined narration with one gain and a peak limiter, then measures it again ([tools/lib/loudness.js](tools/lib/loudness.js)) |
| Timing stamped from the narration | HyperFrames reads timing statically from the HTML, and hand-typed timings drift from the words | `npm run timeline -- <slug>` writes every clip's window, its beats and the narration track; `--check` fails when they are out of date |
| One folder per episode, one place for what is shared | the series will have dozens of episodes, and a scene fixed once must be fixed everywhere | `episodes/<slug>/` and `shared/`, defined once in [tools/lib/layout.js](tools/lib/layout.js) and guarded by a test that keeps episode material off the root |
| Project commands on an episode | the CLI's project commands open only `index.html`, and its lint finds compositions only in `compositions/` | `--episode <slug>` stands the episode in as `index.html` for one command and puts the smoke test back afterwards ([tools/lib/episode.js](tools/lib/episode.js)); `npm run check:episodes` checks every episode, against silence where narration is not generated |
| Companion pages from the episode | a companion page's transcript must say what the video says, and its video block must pin what is committed | `npm run companions`, `npm run companions:check` |
| Publishing from a receipt | a published video must show what the repository says, and its captions and poster must come from the same cut | `npm run render ... -o <file>` writes a receipt of the render's sources; `npm run publish -- <slug>` checks it and writes the files to commit into `docs-site/media/` ([tools/lib/publication.js](tools/lib/publication.js)) |
| Captions a reader can follow | a caption that ends mid-phrase, or runs past two lines, makes the viewer wait or squint | at most two lines of 42 characters, broken after a clause and never after a word that belongs with the next ([tools/lib/narration.js](tools/lib/narration.js)) |
| Agent conventions | agents do most of the authoring and must know the rules above | `.github/skills/video-production/SKILL.md` |

## Voice

- **Engine:** Kokoro-82M, run locally by the HyperFrames CLI. It needs no
  account or key, and no network once the CLI has downloaded the model on the
  first narration run; it costs nothing, and the same script and settings
  give the same narration, so audio is as reproducible as the pictures.
  Hosted voices (HeyGen, ElevenLabs) are richer but need a key, and neither is
  reproducible.
- **The series voice is Emma** (`bf_emma`, British English), chosen by
  audition from eight candidates. `npm run voice:samples` renders the same
  script in every candidate voice under `renders/voice-samples/`, and the
  choice is recorded in [voice/voice.json](voice/voice.json). Changing it later
  means re-narrating every episode, so it changes rarely.
- **Pronunciation:** [voice/lexicon.json](voice/lexicon.json) maps written forms
  to spoken ones. "Orleans" is said or-LEENZ, with the stress on the second
  syllable, so the lexicon respells it "Or-leens"; left alone, the phonemizer
  says OR-lee-unz. Other entries spell out initialisms ("CRDTs", "gRPC").
  Captions keep the written form. Prefer rewording a script to adding an entry,
  and check a new entry by ear with `npm run voice:samples -- --voices bf_emma`.
- **Words with two readings:** the phonemizer picks one reading of a heteronym
  without looking at the sentence. It reads "lives" as the plural of life
  everywhere, so the pilot's "the store lives in the cluster" came out wrong,
  and the lexicon now respells the verb ("livs"). Before narrating a script, run
  `npm run phonemes -- <slug>`: it prints each cue's spoken form and phonemes
  exactly as the voice will receive them, and flags the words in
  [voice/heteronyms.json](voice/heteronyms.json) so their reading can be
  confirmed. The phonemizer's British English also gives BATH words a short
  vowel ("answer", "last"); that is the voice's accent, not a misreading.

## Open decisions

### Hosting - decided

**Decided on 2026-09-24: option A, in the docs site.** Each episode's
published cut - its video, captions and poster - is committed to
`docs-site/media/`, and the site plays it from its own origin with a native
`<video>`. The pull request that publishes an episode therefore carries the
video itself, so it is reviewed and versioned with its page, and it reaches
the live site with the next docs deploy. The options, and the numbers the
decision rests on, follow; how it works is at the end of this section.

The question was where the rendered MP4s live and how a page plays them, given
that the site's player should play them from the site's own origin.

| Option | Upside | Cost |
| --- | --- | --- |
| A. Commit renders to git under `videos/` | simplest; the page points at a file | every re-render adds a full binary to history forever (video does not delta-compress); several CI jobs check out full history, so every run would download every render ever committed; GitHub blocks files over 100 MB |
| B. Git LFS under `videos/` | the same model with small history; CI checkouts skip LFS unless asked | storage and bandwidth quota; contributors need git-lfs; the docs deploy must check out LFS to publish the files |
| C. Commit sources only; render in the docs deploy | nothing binary in history; the published video is always in step with the published docs; the pictures are deterministic, so renders can be cached by content | render time in the deploy job; the GitHub Pages 1 GB site limit; the narration must be committed as compressed audio or regenerated in CI |
| D. An external host, embedded | adaptive streaming and discovery | a third-party dependency and its tracking on every page, against the local-first grain |
| E. Commit sources; publish renders as GitHub release assets | nothing binary in history; no limit on a release's total size or its bandwidth (each file under 2 GiB); renders versioned with the release they describe | the video is served from GitHub's release CDN rather than the site's origin; a workflow must render and attach it |

What the pilot measured (the introduction, 2:52, 1920x1080 at 30 fps):

- **Size:** 10.8 MB at high quality (502 kbit/s: H.264 about 320, AAC stereo
  about 175), and 11.6 MB for the published re-render of the same sources,
  which differs only by encoder variation; the two-minute first cut was 8.0 MB, and a draft is about three
  quarters of the high-quality size. That is about 4 MB a minute, so the 24
  episodes planned here, at about three minutes each, are roughly 300 MB a
  full set, and every re-render of one episode is another 10 to 20 MB.
- **Render time:** 3 min 39 s for the high-quality render on one worker, on a
  laptop with every core busy: about 1.3 times real time. Its start-up probes
  failed repeatedly under that load until FFmpeg was kept resident (see the
  video-production skill); CI's dedicated runner has no such contention.
- **Narration:** the mastered track is 5.7 MB as WAV, 1.5 MB as 96 kbit/s AAC
  and 0.7 MB as 48 kbit/s Opus - small enough to commit.

What that means for each option: A puts 300 MB in history for the first set and
more with every re-cut, downloaded by every full-history CI checkout. B fits
GitHub Free's Git LFS allowance (10 GiB of storage, 10 GiB a month of
bandwidth) for a while, but every pushed version counts against storage, every
CI or deploy checkout counts against bandwidth, and past the allowance LFS
stops working until the month ends. C fits the Pages limits (1 GB a site, 100
GB a month) with room, and renders only what changed if renders are cached by
content. E has no size or bandwidth limit at all.

Checked against the live services on 2026-09-24:

- **Pages can play the videos.** The site answers byte-range requests
  (`206 Partial Content`, through its CDN), and Pages serves `.mp4` as
  `video/mp4`, so a native `<video>` element starts a fast-start MP4 at once
  and seeks anywhere in it. Every render here is fast-start: the MP4's index
  comes first. This is progressive playback, which is what a documentation
  site needs; adaptive streaming (HLS or DASH) would also work as static
  files, but is not worth it at 4 MB a minute.
- **The site has room.** The published site is 41.7 MB (725 files) of its
  1 GB, so the current cut of all 24 planned episodes, about 260 MB, fits.
  100 GB a month is about 9,000 full plays of the introduction, and a visitor
  who never presses play downloads only the poster (the player is
  `preload="none"`).
- **Release files are not part of the site.** They count against neither the
  site's 1 GB nor git history, and have no total-size or bandwidth limit. But
  a release file is served through a signed link that expires within the hour,
  as `application/octet-stream` with `Content-Disposition: attachment`: right
  for a download, not dependable for playing in place.

An earlier recommendation here - keep each render as a release file and have
the docs deploy copy the current cut into the site - was not taken: the video
belongs in the pull request that publishes it, and in the site, not in a
release beside them.

How it works:

- **Only the published cut is committed**, once per episode per cut, as
  `docs-site/media/<slug>-<cut>.mp4`, `.vtt` and `.jpg`. The cut is a digest
  of the three files, so a new cut is a new file name and nothing cached under
  an old name goes stale. Renders, drafts and narration audio are not
  committed: the published MP4 carries its sound.
- **The companion page pins the cut** in its video block, which
  `npm run companions` writes from `episode.json` and the composition, so a
  page and the video it plays always come from the same commit.
  `npm run companions:check` fails CI when a pinned file is missing, or a
  committed one is pinned by no page.
- **The site plays what is committed.** `docs-site/stage.ps1` replaces each
  video block with the player when its three files are in `docs-site/media/`,
  copies them into the site once, and generates the Videos tab and the home
  page's introduction from the pages that pin a cut.
- **The cost, accepted:** each published cut adds its size to git history for
  good (about 4 MB a minute; 11.6 MB for the introduction), and every
  full-history CI checkout downloads it. Keep re-cuts deliberate, and publish
  from a review, not from a draft.

### Companion pages on the site - built

Built with the pilot. Companion pages under `docs/videos/` have their own
tab, Videos, generated from each page's video block: published episodes only,
grouped by path in the order of the shape above, each with its title, one-line
idea and length. The front door plays on the home page, in its own section
after the first viewport. A page whose video is not published is left out of
the site. The site's side is `docs-site/stage.ps1`, and the player's style is
in [DESIGN.md](../DESIGN.md).

### Music

Off by default. Decide with the pilot whether the series has a signature bed at
all.

## Deferred and out of scope

- **AI and agents path** - the MCP server, vector search and RepoContext - once
  `lattice.vector` and RepoContext have shipped.
- **The Explorer** is out of scope while it is redesigned.
- **What's new** episodes per release wave, drawn from the changelog.
- **Sample spotlights**: one variable-driven template filled per sample, once
  the core paths exist.
- **One CRDT, one join**: a short per scenario in the site's
  `docs-site/figures/join-figures.json` (thirteen today), each its join figure
  narrated from the scenario's own step, settled and rule texts, so the video
  and the explainer page it embeds on cannot disagree. The diamond scenarios
  are ready; the chain layouts are ported first.
