# MultiSiteManufacturing Sample — Plan

> Status: **M1–M7 complete** (committed on `feature/sample-manufacturing`,
> 73/73 tests green). **Executing M8 — Blazor Server shell + read-only
> dashboard.** All §12 review items resolved below.


## 1. Goals

Demonstrate `Orleans.Lattice` as the persistence layer for a **regulated
process-engineering traceability system**, using the turbine-engine part
lifecycle (forge → heat-treat → machining → NDT → MRB → FAI) as the
concrete vertical. The sample must:

1. Ship a **working inventory system** — not a scripted replay. An operator
   can create parts, advance them through process stages, record
   inspections, raise non-conformances, issue MRB dispositions, and sign
   off FAI, and the sample behaves like a minimal MES/QMS thin slice.
2. Bulk-load a realistic **pre-seeded inventory** on silo startup (~50
   parts across every lifecycle state) so the dashboard looks lived-in the
   moment the browser connects.
3. Expose all outside-facing APIs over **gRPC**, using server-streaming
   RPCs for any feed that's naturally a subscription (part views, site
   states).
4. Render a **Blazor Server** dashboard with real-time push updates
   (`IAsyncEnumerable<T>` + `StateHasChanged`) — no polling timers.
5. Provide a **persistent, clearly-labelled chaos fly-out side panel**
   for injecting site-level failures (pause, delay, reorder). Chaos
   configuration is held in dedicated Orleans grains with Azure Table
   Storage persistence, so it survives process restarts.
6. Persist all grain state (inventory, facts, site chaos configuration)
   to **Azure Table Storage** — Azurite locally for development,
   inspectable via Azure Storage Explorer.

## 2. Non-goals

- Multi-silo Orleans clustering. **Single host** (single ASP.NET Core
  process running silo + Blazor + gRPC), localhost clustering.
- Authentication / RBAC. A label on the dashboard ("operator: demo")
  suffices for the sample.
- gRPC reflection / gRPC-UI tooling. Overkill for this sample.
- grpc-web / browser gRPC client. Blazor Server consumes services
  in-process; no browser-side gRPC surface needed.
- CLI companion tool. Skipped for v1 — the gRPC surface is exercised by
  integration tests.
- A scripted divergence scenario. Divergence (baseline-vs-lattice) will
  emerge naturally once chaos presets cause re-ordered fact delivery;
  there is no scripted saga or "trigger divergence" button in v1.
- Full MES/QMS parity. We ship a single product family (HPT blade) with
  a simplified severity lattice.
- Container orchestration / k8s. `dotnet run` on a developer workstation,
  with Azurite running locally.

## 3. Domain model

### 3.1 Severity lattice

```
Nominal  <  UnderInspection  <  FlaggedForReview  <  Rework  <  Scrap
```

Totally ordered for the fold. `Scrap` is terminal (no further dispositions
accepted). `UseAsIs` is modelled as an explicit `MRBDisposition` fact that
demotes `FlaggedForReview` back to `Nominal` — mirroring the Olympics
sample's "appeal overturned" mechanic.

### 3.2 Identifiers

| Type | Shape | Example |
|---|---|---|
| `PartSerialNumber` | `{family}-{year}-{seq:D5}` | `HPT-BLD-S1-2028-00142` |
| `PartFamily` | short code | `HPT-BLD-S1`, `HPT-DSK-S1`, `LPC-BLD` |
| `ProcessStage` | enum | `Forge`, `HeatTreat`, `Machining`, `NDT`, `MRB`, `FAI` |
| `ProcessSite` | enum — **readable names**, see below | `OhioForge` |

#### Process sites (decision 11 — seven sites, readable names)

| Enum value | Display name | Stage | Role |
|---|---|---|---|
| `OhioForge` | Ohio Forge | Forge | Raw forging |
| `NagoyaHeatTreat` | Nagoya Heat Treatment | HeatTreat | Vacuum / aging cycles |
| `StuttgartMachining` | Stuttgart Machining | Machining | 5-axis milling |
| `StuttgartCmmLab` | Stuttgart CMM Lab | Machining (inspection) | Dimensional verification |
| `ToulouseNdtLab` | Toulouse NDT Lab | NDT | FPI / eddy current / X-ray |
| `CincinnatiMrb` | Cincinnati MRB | MRB | Material review board disposition |
| `BristolFai` | Bristol FAI | FAI | First-article inspection & certification |

Every UI surface uses the display name. Enum values use PascalCase with
no hyphens so they're comfortable identifiers in C#.

### 3.3 Fact union

All facts carry `PartSerialNumber`, `FactId`, `HybridLogicalClock`,
`ProcessSite` (origin), `OperatorId`, and a human `Description`.

| Fact kind | Payload |
|---|---|
| `ProcessStepCompleted` | `stage`, `heatLot` (if applicable), `processParameters` (opaque dict) |
| `InspectionRecorded` | `inspection` (`CMM`/`FPI`/`EddyCurrent`/`XRay`/`Visual`), `outcome` (`Pass`/`Fail`), `measurements` (opaque), `instrumentCalDate` |
| `NonConformanceRaised` | `ncNumber`, `defectCode`, `severity` (`Minor`/`Major`/`Critical`) |
| `MRBDisposition` | `ncNumber`, `disposition` (`UseAsIs`/`Rework`/`Scrap`/`ReturnToVendor`) |
| `ReworkCompleted` | `reworkOperation` (free text), `retestPassed` (bool) |
| `FinalAcceptance` | `faiReportId`, `inspectorId`, `certIssued` |

### 3.4 Fold to `ComplianceState`

Max of:

- `ProcessStepCompleted` → `Nominal`
- `InspectionRecorded(Pass)` → `Nominal`; `(Fail)` → `FlaggedForReview`
- `NonConformanceRaised(Minor)` → `FlaggedForReview`; `(Major)` → `Rework`; `(Critical)` → `Scrap`
- `MRBDisposition(UseAsIs)` → demotes prior `FlaggedForReview` to `Nominal` (explicit)
- `MRBDisposition(Rework)` → `Rework`
- `MRBDisposition(Scrap)` → `Scrap` (terminal)
- `ReworkCompleted(retestPassed=true)` → stays `Rework` until explicit re-NDT `Pass` + fresh MRB `UseAsIs`
- `FinalAcceptance` → `Nominal` only if no outstanding `FlaggedForReview`/`Rework`/`Scrap`

Same shape as `FactFolding.cs` in the Olympics sample — deterministic
across delivery orders.

## 4. Architecture

```
┌──────────────────────────────────────────────────────┐
│                 Blazor Server (browser)              │
│  Dashboard · Part detail · Chaos fly-out             │
└────────────────────▲─────────────────────────────────┘
                     │  SignalR circuit (built-in)
┌────────────────────┴─────────────────────────────────┐
│       ASP.NET Core host (single process, single silo)│
│                                                       │
│  Blazor components ──► Orleans GrainFactory (local)  │
│                                                       │
│  gRPC services ──────► FederationRouter              │
│    Inventory                │                         │
│    FactIngress              ▼                         │
│    SiteControl        ┌──────────────┐                │
│    Compliance         │  IFactBackend│ (×2)          │
│                       │  baseline    │                │
│                       │  lattice     │                │
│                       └──────┬───────┘                │
│                              │                        │
│              ┌───────────────┴────────────────┐       │
│              ▼                                ▼       │
│      Orleans grains                  Orleans.Lattice  │
│      (baseline LWW +                 (fact store,     │
│       IProcessSiteGrain              Azure Table      │
│       IPartGrain)                    Storage)         │
└──────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌──────────────────────────────┐
              │  Azure Table Storage         │
              │  (Azurite in dev,            │
              │   real Azure in demo)        │
              │  Inspected via Azure         │
              │  Storage Explorer            │
              └──────────────────────────────┘
```

**Design choice — Blazor consumes Orleans directly, not via gRPC loopback.**
Blazor Server is in-process; adding a gRPC hop on top would just cost
serialization. gRPC is the **external** contract — used by integration
tests and any third-party integration. The dashboard and the gRPC surface
consume the same `FederationRouter` / backend instances via DI.

### 4.1 Persistence (decision 4)

Both Orleans grain state and the Lattice fact store back onto **Azure
Table Storage**:

- **Dev**: Azurite (a local storage emulator). README documents
  installation (`npm install -g azurite` or the VS Code extension) and
  the connection string `UseDevelopmentStorage=true`.
- **Demo / inspection**: point the connection string at a real Azure
  Storage account; open Azure Storage Explorer to browse the tables and
  watch facts/grain state evolve live as operators drive the UI.

Tables used:
- `msmfgGrainState` — Orleans grain state (inventory, part grains, site
  chaos config, seed-idempotency flag).
- `msmfgLatticeFacts` — Orleans.Lattice fact store.

### 4.2 Chaos state as Orleans grains (decision 10)

Site chaos configuration is **not** held in `FederationRouter` memory
(as it was in the Olympics sample). Instead:

- `IProcessSiteGrain` — one grain per `ProcessSite` enum value, keyed by
  the site name. Persists `IsPaused`, `DelayMs`, `ReorderEnabled`,
  `PendingFacts` queue metadata.
- `ISiteRegistryGrain` — singleton aggregator that exposes
  `WatchSites` streams and fans out preset-apply commands to the per-site
  grains.

The `FederationRouter` consults `IProcessSiteGrain` when routing each
fact. This moves chaos state from "ephemeral process memory" to
"durable, inspectable, survives restart" — which matches how a real MES
would persist site availability flags.

### 4.3 Fault-injection tiers

Fault injection is layered deliberately so that each tier models a
distinct class of real-world failure and can be exercised independently
from the UI and from tests.

| Tier | Where | Models | Controls | Status |
|---:|---|---|---|---|
| 1 | `IProcessSiteGrain.AdmitAsync` (origin site) | Site unavailable, WAN latency, out-of-order delivery from a region | `IsPaused`, `DelayMs`, `ReorderEnabled` | Pause + delay built in M4; **reorder buffer lands in M7** |
| 2 | `ChaosFactBackend : IFactBackend` decorator (per backend) | Transient storage failure, per-call jitter, duplicate writes; applied to one backend only to create baseline-vs-lattice divergence | `JitterMsMin`, `JitterMsMax`, `TransientFailureRate`, `WriteAmplificationRate` on `IBackendChaosGrain` keyed by backend name (`"baseline"`, `"lattice"`) | **New in M7** |
| 3 | Reorder buffer inside `ProcessSiteGrain` | Cross-site out-of-order arrival after a regional pause lifts | `ReorderEnabled` flag (already on `SiteConfig`, currently unwired) | **Wired in M7** |

Storage-provider-level chaos (wrapping the `TableServiceClient` itself)
is explicitly **out of scope** — Tier 2 exercises the same failure modes
at a cleaner seam without coupling tests to the Azure SDK.

The decorator composes around the inner backend inside the
`FederationRouter` fan-out so the router remains oblivious; Tier 2 is
opt-in and suppressed under the `Testing` environment (same pattern as
the seeder) so contract tests remain deterministic.

## 5. Project layout

```
samples/MultiSiteManufacturing/
├── plan.md                                           (this document)
├── README.md                                         (user-facing)
├── MultiSiteManufacturing.sln
├── docs/
│   ├── architecture.md                               (sequence diagrams)
│   └── glossary.md                                   (MRB / NCR / FAI / FPI / CMM)
├── src/
│   ├── MultiSiteManufacturing.Contracts/             (netstandard2.0)
│   │   └── Protos/
│   │       ├── common.proto
│   │       ├── inventory.proto
│   │       ├── facts.proto
│   │       ├── sites.proto
│   │       └── compliance.proto
│   └── MultiSiteManufacturing.Host/                  (net10.0, Microsoft.NET.Sdk.Web)
│       ├── Program.cs
│       ├── Domain/         (shared POCO / records, fold)
│       ├── Baseline/       (baseline backend + grains)
│       ├── Lattice/        (lattice backend + fact store)
│       ├── Federation/     (router + grain-backed site state)
│       ├── Inventory/      (seeder, operator-facing operations)
│       ├── Grpc/           (service implementations)
│       ├── Components/     (Blazor Razor components)
│       │   ├── Layout/
│       │   ├── Pages/
│       │   └── Shared/
│       └── wwwroot/        (static assets, CSS)
└── test/
    └── MultiSiteManufacturing.Tests/                 (NUnit, net10.0)
        ├── Domain/
        ├── Fold/
        ├── Federation/
        └── Grpc/                                     (contract tests via in-proc channel)
```

Rationale:
- **Contracts** as `netstandard2.0` so the protos are consumable from a
  hypothetical separate client without dragging .NET 10 runtime deps.
- **Host** as a single project for ease of `dotnet run` and debugging.
- **No Cli project** (decision 5).
- **No Scenarios folder** — there's no scripted saga in v1 (decision 7).

## 6. gRPC API surface

All services live under `multisitemfg.v1`. Wire format is protobuf;
server-streaming where the consumer naturally wants a live feed. No
reflection endpoint (decision 2), no grpc-web (decision 9).

### 6.1 `InventoryService`

```proto
rpc CreatePart     (CreatePartRequest)     returns (Part);
rpc GetPart        (GetPartRequest)        returns (PartView);
rpc ListParts      (ListPartsRequest)      returns (stream PartSummary);  // server stream
rpc WatchPart      (WatchPartRequest)      returns (stream PartView);     // live updates
rpc WatchInventory (WatchInventoryRequest) returns (stream PartSummary);  // dashboard feed
```

### 6.2 `FactIngressService`

```proto
rpc EmitFact       (FactEnvelope)         returns (EmitResult);
rpc EmitFactStream (stream FactEnvelope)  returns (EmitSummary);          // bulk
```

### 6.3 `SiteControlService`

```proto
rpc ListSites       (google.protobuf.Empty)   returns (ListSitesResponse);
rpc ConfigureSite   (ConfigureSiteRequest)    returns (SiteState);           // pause / delay / reorder
rpc WatchSites      (google.protobuf.Empty)   returns (stream SiteState);    // pending counts etc.
rpc TriggerPreset   (TriggerPresetRequest)    returns (PresetResult);        // canned chaos presets
rpc ListBackends    (google.protobuf.Empty)   returns (ListBackendsResponse);
rpc ConfigureBackend(ConfigureBackendRequest) returns (BackendChaosState);   // jitter / fault rate / write-amp
```

`ConfigureSite` and `ConfigureBackend` are full-state PUTs (supply any
subset of fields; omitted fields are left unchanged). `ListBackends`
returns the current `BackendChaosState` for each registered backend
(`"baseline"` and `"lattice"`). Preset application fans out to the
`IProcessSiteGrain`s via `ISiteRegistryGrain` and (for backend-targeted
presets) to the per-backend `IBackendChaosGrain` instances.

### 6.4 `ComplianceService`

```proto
rpc GetPartCompliance   (GetPartComplianceRequest)   returns (PartComplianceView);
rpc WatchDivergence     (google.protobuf.Empty)      returns (stream DivergenceReport);
```

`DivergenceReport` is the "baseline says X, lattice says Y" feed — one row
per part where the two backends currently disagree, pushed whenever the
set changes. Divergence emerges organically from chaos-induced reorder
(no scripted trigger).

## 7. UI design (Blazor Server)

### 7.1 Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  Multi-Site Manufacturing — Digital Thread Demo           [ ☰ Chaos ] │ ← fly-out toggle
├─────────────────────────────────────────────────────────────────────┤
│  Filters: family [▼]  state [▼]  site [▼]            [+ New part]    │
├──────────────────────────────┬──────────────────────────────────────┤
│  Inventory                   │  Divergence feed                      │
│  ┌──────┬──────┬────────┐    │  ┌────────────┬──────────┬────────┐  │
│  │ SN   │ Stage│ State  │    │  │ SN         │ Baseline │ Lattice│  │
│  │ ...  │ ...  │ [Rework]   │  │ HPT-...142 │ Nominal  │ Rework │  │  ← red row
│  │      │      │         │    │  │  ...       │          │        │  │
│  └──────┴──────┴────────┘    │  └────────────┴──────────┴────────┘  │
│  (click part → detail pane)  │                                      │
└──────────────────────────────┴──────────────────────────────────────┘
```

### 7.2 Chaos fly-out

Triggered by the top-right `☰ Chaos` button; slides in from the right.
All chaos state lives in `IProcessSiteGrain` (persistent); reloading the
browser re-renders the current state from grain storage. The fly-out
open/closed bit is UI-local.

Sections inside the fly-out:
1. **Per-site controls** (Tier 1 + 3) — table of the seven sites (by
   display name) with Paused / Delay / Reorder / Pending / Forwarded,
   each row editable live with explicit labels.
2. **Backend storage chaos** (Tier 2) — one row per backend (`baseline`,
   `lattice`) with sliders for Jitter min/max, Transient fault rate,
   and Write amplification rate. Applying fault rate to only one
   backend is the canonical way to surface baseline-vs-lattice
   divergence without a scripted saga.
3. **Canned presets** — single-click buttons that configure multiple
   knobs at once:
   - *Transoceanic backhaul outage*: pauses Stuttgart CMM Lab +
     Toulouse NDT Lab, delay 4 s.
   - *Customs hold*: delays Nagoya Heat Treatment by 8 s.
   - *MRB weekend*: pauses Cincinnati MRB entirely.
   - *Lattice storage flakes*: applies a 10 % transient fault rate and
     50–250 ms jitter to the **lattice** backend only — drives the
     divergence feed organically.
   - *Clear all*: resets every site and every backend to nominal.
4. **Active chaos summary** — a prominent banner at the top of the
   dashboard (outside the fly-out) showing "⚠ 2 sites paused, 1 delayed,
   lattice backend flaky" so the operator can never leave the fly-out
   open, forget about it, and be confused by downstream effects.

Clear labelling rule: every chaos control has an explicit plain-English
description ("Simulate 4-second latency at Toulouse NDT Lab" — not
"delay=4000"). Presets have a tooltip describing the real-world
scenario they model.

### 7.3 Real-time update strategy

Each Blazor component that displays live data owns an
`IAsyncEnumerable<T>` subscription acquired in `OnInitializedAsync` and
cancelled in `Dispose`. Internally these subscriptions are backed by
**`System.Threading.Channels.Channel<T>`** owned by the underlying
services (`InventoryService`, `SiteRegistry`, `DivergenceTracker`). The
services push whenever the domain state changes (grain callback, router
completion, etc.). The Razor component receives messages, applies them to
its local view-model, and calls `InvokeAsync(StateHasChanged)`.

No polling. No `Timer`. No `setInterval`. Browser reload reconnects the
SignalR circuit; components resubscribe on `OnInitializedAsync`.

The gRPC server-streaming RPCs are thin adapters over the same channels.

## 8. Bulk-load strategy

### 8.1 Seed shape

On startup, a `IHostedService` (`InventorySeeder`) bulk-loads ~50 parts
across a spread of lifecycle states:

| Count | State | Stage reached |
|---:|---|---|
| 10 | `Nominal` | Forge only |
| 8  | `Nominal` | HeatTreat complete |
| 14 | `Nominal` | Machining complete, CMM pass *(absorbs the original `UnderInspection` bucket — see note)* |
| 6  | `FlaggedForReview` | NDT raised minor NC |
| 5  | `Rework` | MRB dispositioned rework |
| 4  | `Nominal` | FAI signed off (fully complete) |
| 3  | `Scrap` | Critical NC |

> **Seed-shape deviation (M6):** the original plan reserved 6 parts for
> `UnderInspection`, but the fact grammar has no `InspectionStarted`
> transition — `UnderInspection` is not reachable by folding any fact
> sequence in v1. Those 6 parts were folded into the Machining+CMM pass
> bucket so the total remains 50. The deviation is also documented in
> `InventorySeeder` XML comments.

Serial numbers are deterministic (`HPT-BLD-S1-2028-00001` … `-00050`) so
running the sample always produces the same seeded inventory for demos.

### 8.2 Implementation

The seeder emits facts through `FederationRouter` just like a live
producer would — this means the seed flows through both the baseline and
lattice backends on startup, guaranteeing they agree before the operator
starts chaos. The seeder runs **only once per Azure Table Storage
account**: it checks a dedicated `IInventorySeedStateGrain` (singleton)
for a persisted `HasSeeded` flag and no-ops on subsequent calls. With
Azure Table Storage, this means re-running the host against the same
storage account preserves the seeded inventory (and any operator
mutations) — exactly what a real MES would do.

Chaos knobs are disabled during seed: the seeder asks
`ISiteRegistryGrain` to snapshot current site state, sets every site to
(delay=0, paused=false) for the duration of the seed, then restores the
snapshot. This keeps seed time deterministic even if a previous session
left chaos presets active.

### 8.3 Operator-driven mutations

Once seeded, the operator drives all further facts through the UI:
- `+ New part` → `InventoryService.CreatePart` → emits a synthetic
  `ProcessStepCompleted(Forge, …)` at HLC=now.
- Detail-pane action buttons → corresponding fact kinds.

These flow through the federation router and therefore honor any active
chaos. This is the core "inventory-system-with-chaos-knobs" UX the user
asked for.

## 9. Testing strategy

| Layer | Framework | What's covered |
|---|---|---|
| Domain fold | NUnit | Every fact kind, every severity transition, `MRBDisposition(UseAsIs)` demotion, idempotency under duplicate facts |
| Backends | NUnit + TestCluster | Baseline LWW vs. Lattice divergence under reorder (reuse of Olympics test strategy) |
| Site grain | NUnit + TestCluster | `IProcessSiteGrain` persistence, pause/resume semantics, preset fan-out |
| Seeder | NUnit | Idempotency (runs twice → same counts), spread correctness (every state bucket populated) |
| gRPC contracts | NUnit + `Grpc.Net.Client` in-proc channel | Request/response shape, error codes, stream completion on cancel |
| UI smoke | `bUnit` (tentative) | Dashboard renders, chaos fly-out opens, clicking a canned preset configures the expected sites. Scope TBD — may defer. |

Chaos-style long-running tests remain `[Category("Chaos")]` and excluded
from the iterative dev filter.

Test cluster uses Orleans in-memory storage (no Azurite dependency in
the test suite — keeps CI fast and hermetic).

## 10. Migration from Olympics

**Hard replacement** (decision confirmed):
- Delete `samples/OlympicsFederation/` entirely (M1 step 2).
- Update root `README.md` samples table to list
  `samples/MultiSiteManufacturing` (M1 step 3).
- `.github/copilot-instructions.md` and related docs are domain-agnostic;
  verify no stale references after the delete.

What survives the move (copy-forward with renames / minor tidying):
- `FederationRouter` (rename `SourceCluster` → `ProcessSite`, extract
  chaos state into `IProcessSiteGrain`).
- `HybridLogicalClock` usage, retry-on-`EnumerationAbortedException`
  pattern in the fact store.
- Fan-out `IFactBackend` contract.
- Baseline-vs-lattice divergence detection (becomes the divergence feed).

What gets discarded:
- The scripted-scenario loop (`ScenarioLoop`, `ScriptedScenario`) —
  replaced by bulk-load + operator actions. No scripted divergence saga
  in v1 (decision 7).
- The plain HTML/JS dashboard — replaced wholesale by Blazor Server.
- The live-config endpoint — no longer needed.

## 11. (reserved — was "Migration")

Numbering preserved for cross-reference stability; migration content
merged into §10.

## 12. Decisions from review

All §12 open questions are resolved. Answers recorded here verbatim:

1. **Single host vs. split silo/web processes?** → **Single host.**
   One ASP.NET Core process hosts silo + Blazor + gRPC.
2. **gRPC reflection / gRPC-UI?** → **Overkill — don't add.**
3. **Authentication / operator identity?** → **No authentication.**
   Dashboard shows a static "operator: demo" label.
4. **Persistence?** → **Azure Table Storage** (Azurite for dev),
   inspected via **Azure Storage Explorer**. Both Orleans grain state
   and Orleans.Lattice fact store back onto Table Storage.
5. **CLI tool?** → **No CLI.**
6. **Per-part grain vs. per-site grain?** → **Per-part grain**
   (`IPartGrain`) for operator-facing state. Per-site state lives in
   `IProcessSiteGrain` (see decision 10).
7. **Scripted divergence scenario?** → **No divergence scenario for
   now.** Divergence emerges organically from chaos-induced reorder;
   there is no "trigger divergence" button in v1.
8. **Blazor WebAssembly vs. Server?** → **Blazor Server.**
9. **Use `Grpc.AspNetCore.Web`?** → **Skip.**
10. **Chaos fly-out persistence?** → **Maintain with dedicated grain(s)** —
    `IProcessSiteGrain` per site plus an `ISiteRegistryGrain` singleton,
    both persisted to Azure Table Storage. Chaos state survives process
    restart.
11. **How many process sites to model?** → **Seven**, with readable
    display names (see §3.2 table). The reviewer flagged that opaque
    codes like `CMM-DE` are unhelpful; every UI surface uses the full
    display name (e.g. "Stuttgart CMM Lab").

## 13. Milestones

| # | Deliverable | Rough effort |
|---:|---|---|
| M0 | Plan reviewed + accepted | ✅ done |
| M1 | Repo restructure: delete Olympics, scaffold new dir tree, solution/csproj setup, Azurite+Table Storage wired in `Program.cs`, build green on empty projects | ✅ done |
| M2 | Domain + fold + NUnit fold tests (port from Olympics) | ✅ done |
| M3 | Baseline + Lattice backends + `IFactBackend` + fan-out router, reusing Olympics plumbing with renames | ✅ done |
| M4 | `IProcessSiteGrain` + `ISiteRegistryGrain` + router integration + grain tests | ✅ done |
| M5 | gRPC contracts (proto) + service implementations + contract tests via in-proc channel | ✅ done |
| M6 | Bulk-load seeder + `IInventorySeedStateGrain` + idempotency test + deterministic seed | ✅ done |
| **M7** | **Fault-injection infrastructure (§4.3):** `ChaosFactBackend : IFactBackend` decorator + `IBackendChaosGrain` (jitter, transient fault rate, write amplification) + wire reorder buffer in `ProcessSiteGrain` (Tier 3) + `ListBackends` / `ConfigureBackend` RPCs on `SiteControlService` + "Lattice storage flakes" preset + domain tests for all three tiers | ✅ done |
| M8 | Blazor Server shell + main dashboard (read-only) wired to real-time channels | 1 day |
| M9 | Operator action forms (new part, record inspection, raise NCR, MRB disposition, rework complete, FAI sign-off) | 1 day |
| M10 | Chaos fly-out (two-section: site controls + backend storage chaos) with canned presets + active-chaos banner | 0.5 day |
| M11 | Divergence feed (organic, from chaos-induced reorder + backend fault rate) wired into dashboard + `WatchDivergence` gRPC stream | 0.5 day |
| M12 | README, glossary, architecture doc, Azurite setup instructions, screenshots | 0.5 day |
| M13 | Test pass, polish, Chaos-category stress test | 0.5 day |

**Total estimate: ~7.5 developer-days** of focused work.

**Checkpoint cadence**: pause for reviewer confirmation between each
milestone boundary (at minimum between M1 → M2, M6 → M7, and M7 → M8).

## 14. Sign-off

All review items resolved — see §12. **M1–M7 executed and committed on
`feature/sample-manufacturing` (73/73 tests green).** Execution of
**M8 (Blazor Server shell + read-only dashboard)** is underway; the
chaos fly-out now lands on a complete fault-injection surface.
