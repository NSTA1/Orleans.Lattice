---
agent_spec: "docs/agents/capabilities.yaml"
---

# Explorer user journeys

The Explorer user journeys are a role-ordered, end-to-end test series that drives
every area of the Explorer through a real browser, against a production-shaped
cluster on Azurite, in CI. They live in `test/lattice.explorer.e2e/` and were
built by epic [#5401](https://github.com/NSTA1/Orleans.Lattice/issues/5401).
This page describes what the journeys are, how to run them, how CI gates them,
and how known defects are tracked while they stay open.

They complement, and do not replace, the component tests in
`test/lattice.explorer/` (bUnit) and the single-host browser tests in
`test/lattice.explorer.uitests/`.

## The estate

Each journey runs against an *estate* that the harness starts for it:

- Real host processes: a two-silo west region behind a TLS gateway with active
  health checks, an Explorer head whose one endpoint is that gateway, and a
  line-feed service that writes orders into the cluster.
- Every storage provider on Azurite, with the backup sink on a separate
  account. An estate with an east region adds a third account for east,
  peers the regions for replication, and runs its own pinned Prometheus
  container.
- Two storylines, one estate each: **Contoso**, a non-tenanted manufacturing
  estate that carries the main story, and **Fabrikam**, a tenanted hosting
  provider that carries the tenancy storyline.

The journey hosts (`JourneySilo`, `JourneyExplorer`, `JourneyGateway` and
`LineFeed`) live under `test/lattice.explorer.e2e/Hosts/`. Every table and
container an estate creates carries a run-unique prefix.

## Chapters and personas

A journey is one *chapter* of a storyline, played by one or more personas, each
signed in to their own browser session. Chapters are ordered by role:

| Group | Chapters | What they cover |
|---|---|---|
| Admin (Contoso) | A1 to A12 | First contact, the access model, provisioning, schema, apps, backups, replication, capacity and WAL operations, telemetry and the cluster area in depth. |
| Users (Contoso) | U1 to U6 | A data steward's day, a schema owner's week, a team on the task board, the auditor's review, personalising the console, and the schema rule workshop. |
| Crisis (Contoso) | C1 to C4 | Incident recovery, silo failover, offboarding, and running the console under a subpath. |
| Tenancy (Fabrikam) | T1 to T7 | Tenant onboarding through region lifecycle. |

The story outline (`Story/StoryOutline.cs`) declares every chapter with its
tracking issue and the chapters it depends on. A chapter's GitHub issue is the
source of truth for its steps.

Each journey lives in its own folder, `Journeys/<id>/`, derives from
`JourneyChapter`, and declares its chapter with `[JourneyChapter]`. Adding a
journey edits no shared file: the CI planner discovers it from the directory.

Every step ends in a *checkpoint* that sweeps the page with axe-core, fails on a
browser console error or on an Explorer or gateway `fail:` or `crit:` log
record, and files a masked, full-page screenshot with its SHA-256 in
`artifacts/screenshots/<jid>/manifest.json`.

## Run modes

`LATTICE_E2E_MODE` selects how the journeys run:

- **`isolated`** (the default when the variable is unset). Each chapter gets a
  fresh estate. The chapter's *contracts* establish and verify everything it
  consumes through the APIs, its UI steps run, and its own contract verifies
  what it produced. Isolated mode answers "does this one journey work?".
- **`story`**. One estate per storyline. The chapters run in dependency order
  with nothing seeded, each consuming what the chapters before it produced
  through the UI. A failed chapter blocks its dependants, which are reported as
  blocked, by name, and never as passed. A chapter whose journey does not exist
  yet is *stood in* by its contract, and the report says so.

Any other value fails the run rather than falling back to a default.

## Chapter contracts

A *chapter contract* (`IChapterContract`) seeds and verifies one chapter's state
through the facades, never through the UI. `EstablishAsync` produces the
chapter's state and must be idempotent; `VerifyAsync` asserts it. Contracts are
found by reflection, and a session establishes each one at most once, in
dependency order.

Contracts are what make isolated mode possible: a chapter can start from the
exact state its predecessors would have left, without running them. The
contract conformance fixtures (`Contracts/<Storyline>ContractConformanceTests.cs`)
run every contract of a storyline in order, so a contract that drifts from its
journey is caught.

The Contoso contracts are in phase 1. The Fabrikam contracts are phase 2 work
(see [Phase 2](#phase-2)). The CI planner plans a storyline's story leg only once
the tree holds a contract for one of its chapters, so `story-fabrikam` does not
run until they land.

## Running journeys locally

You need Docker (for Azurite and, on an estate with an east region, Prometheus)
and the Playwright Chromium browser. Building the test project also builds the journey hosts, so a local
run launches what it just compiled. When no storage connection strings are set,
the harness starts an Azurite container for each account it needs and removes
it with the estate.

Run one journey in isolated mode, here A3:

```powershell
dotnet build test/lattice.explorer.e2e --configuration Release
$env:LATTICE_E2E_REQUIRED = '1'
dotnet test test/lattice.explorer.e2e --no-build --configuration Release `
  --filter "FullyQualifiedName~Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3."
```

Run the Contoso storyline in story mode:

```powershell
$env:LATTICE_E2E_MODE = 'story'
$env:LATTICE_E2E_REQUIRED = '1'
dotnet test test/lattice.explorer.e2e --no-build --configuration Release `
  --filter "FullyQualifiedName~Orleans.Lattice.Explorer.EndToEnd.Tests.Story.ContosoStoryTests"
```

Set `LATTICE_E2E_REQUIRED=1`, as CI does. Without it, a workstation that cannot
start or reach storage reports the fixtures as inconclusive, which a test run
summary counts as neither passed nor failed. With it, unreachable storage fails
the run.

The lane also reads `LATTICE_E2E_STORAGE`, `LATTICE_E2E_BACKUP_STORAGE` and
`LATTICE_E2E_EAST_STORAGE` (connection strings to existing accounts),
`LATTICE_E2E_HOSTS_DIR` (a directory of published hosts, as CI publishes them
once per job), and `LATTICE_E2E_VIEWPORT`.

## CI gating

The journeys run in their own advisory workflow,
`.github/workflows/explorer-journeys.yml`. Its planner,
`.github/workflows/explorer-journeys-plan.py`, decides which legs a run executes;
its docstring is the contract, and its self-test pins it.

| Leg | What it runs |
|---|---|
| `harness` | Every end-to-end fixture that is not a journey, a story, the coverage gate or contract conformance: the estate smoke test, fault seams, parity, checkpoints, and the catalogue and outline checks. It also stops Azurite and requires the smoke fixture to fail, proving the lane fails closed. |
| `isolated-<jid>` | One leg per folder under `Journeys/`, in isolated mode. |
| `story-contoso`, `story-fabrikam` | Each storyline in story mode on one estate, once the tree holds a contract for one of its chapters. |
| `contract-conformance` | One shard per storyline's conformance fixture. |
| `story-contoso-narrow` | Nightly only: the Contoso story at a 390x844 viewport. |
| `coverage-gate` | Runs after the legs and unions every leg's coverage hits. |

Which legs run depends on the event:

- **A member pull request into an integration branch** runs the `harness` leg
  and the isolated legs of the chapters it touches. A file under an Explorer
  area folder (`src/lattice.explorer/UI/Areas/<Area>/`) adds the chapters whose
  sources name that area. Any other file that can change what a journey sees -
  the rest of the Explorer, the harness, the hosts, a facade, or the lane
  itself - runs every isolated leg. Documentation feeds no chapter. A member
  pull request runs no story leg and no conformance shard.
- **The landing pull request** (an integration branch into `main`), a push,
  and `workflow_dispatch` run every leg and every conformance shard. A nightly
  schedule adds `story-contoso-narrow`.
- **A member pull request labelled `journeys-full`** runs the full set, as the
  landing pull request does.
- **Editing a member pull request's body** runs no leg. It runs the screenshot
  provenance check: every journey pull request carries a
  `## Journey screenshots` section, ordered and captioned, and the check
  matches it against the run it names. `tools/Get-JourneyScreenshots.ps1 -Run
  <id> -Journey <jid>` prints that section for a run.

On a member pull request, the merge gates are CI (`build-and-test`), UI Tests,
the pull request's own isolated legs, the harness leg, the coverage gate and
screenshot provenance. The story legs and contract conformance are advisory:
they run when the integration branch lands and on demand.

`.github/instructions/testing.instructions.md` describes the lane alongside
the repository's other test tiers.

## The feature-coverage gate

`FeatureCoverage/explorer-features.tsv` catalogues the Explorer's features. Some
rows are reflected from the Explorer's surface, and others are curated against
anchors in these docs. Each journey step records the features it exercises, and
the `coverage-gate` job reports every catalogue feature that has neither a
runtime hit nor a reasoned exemption in `coverage-exemptions.tsv`. It fails on
an exemption for a feature the catalogue does not contain. It currently
reports uncovered features without failing on them (`CoverageGate.Hard`).

## Defects and gaps found by a journey

A journey that finds a problem follows the epic's policy:

- **An Explorer or UI defect** (a page, a component, area gating, wording,
  accessibility) is fixed in the epic, with a regression assertion in the step
  that found it.
- **Any other defect** (core, replication, WAL, storage, scheduling, tenancy,
  the auth engine, a facade, CI infrastructure) is filed as an issue and the
  affected step is wrapped in `KnownDefect("#N", "...")`. The wrapped body
  asserts the defect's *observed symptom*, so the wrapper passes only while the
  defect still reproduces. Once it is fixed the assertion fails, the wrapper
  throws `PendingWrapperResolvedException`, and the step must be unwrapped and
  driven for real. A wrapper can never hide a fix.
- **A missing non-UI concept** is filed as an issue and the step is wrapped in
  `KnownGap("#N", "...")`, which asserts the affordance is still absent and
  fails the same way once it lands.
- **A story chapter stood in by its contract** whose contract fails because of
  a tracked defect is listed in the story fixture's `KnownDefectIssues`. The
  chapter and its dependants are reported against that issue rather than
  failing the leg. A chapter whose journey exists is never tolerated this way.

The `PendingWrapperGateTests` fixture scans the journeys for every wrapper,
lists them in the CI job summary, and fails on any wrapper it cannot read, so
none is hidden. Wrappers do not fail the run (`PendingWrapperGate.Hard` is
off): the epic lands with wrappers, provided each one is counted, reported and
linked to an open issue.

### Wrapper census at landing

At phase 1's landing the journeys carry no `KnownGap` wrappers and the following
`KnownDefect` wrappers, every one linked to an open issue:

| Issue | Where | Step | Symptom the wrapper asserts |
|---|---|---|---|
| [#5594](https://github.com/NSTA1/Orleans.Lattice/issues/5594) | `Journeys/A7/GoingMultiRegionJourney.cs` (2 sites) | A7.3, A7.7 | Enabling replication for an already-enrolled tree ignores its bootstrap source, so east never receives west's pre-enable rows. |
| [#5595](https://github.com/NSTA1/Orleans.Lattice/issues/5595) | `Journeys/U3/TaskBoardTeamJourney.cs` | U3.1 | The apps workspace answers the task board's editor with no apps, so its window shows a not-found page. |
| [#5595](https://github.com/NSTA1/Orleans.Lattice/issues/5595) | `Journeys/U5/PersonalisingTheConsoleJourney.cs` (2 sites) | U5 step 3 | The apps workspace answers a nested-group viewer with no apps, so `a/` completions omit the task board. |
| [#5597](https://github.com/NSTA1/Orleans.Lattice/issues/5597) | `Journeys/U1/DataStewardJourney.Browse.cs` | U1 step 2 | A caller granted only on `factory-orders` cannot reach it through its alias `orders`. |
| [#5613](https://github.com/NSTA1/Orleans.Lattice/issues/5613) | `Journeys/U1/DataStewardJourney.Operate.cs` | U1 step 12 | A tag-index reconcile by a caller with Admin on the index tree fails in its repair phase. |
| [#5534](https://github.com/NSTA1/Orleans.Lattice/issues/5534) | `Story/ContosoStoryTests.cs`, `Contracts/ContosoContractConformanceTests.cs` | C1 stand-in | A coordinated restore causes repeated re-seeds, and C1's withheld east batch bypasses strict-ingest diversion. C1 is phase 2 work. |

That is nine wrapper sites over five open issues. The authoritative list for any
run is the one `PendingWrapperGateTests` publishes in that run's job summary.

## Phase 2

The cross-region, migration and multi-tenant chapters - U2, C1 to C4, the
Fabrikam chapter contracts, and T1 to T7 - continue in epic
[#5585](https://github.com/NSTA1/Orleans.Lattice/issues/5585), which starts from
`main` once phase 1 has landed. Until then, story mode stands those chapters in
with their contracts where one exists.
