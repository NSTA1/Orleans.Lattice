# MultiSiteManufacturing Sample — Plan

> Status: **M1–M12 complete** (committed on `feature/sample-manufacturing`,
> 148/148 tests green). **Next: M13 — docs (README, glossary, architecture,
> Azurite setup, screenshots).**
> All §12 review items resolved below.

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
  with Azurite running locally. (M12 lifts the single-silo restriction:
  `run.ps1` launches two localhost silos that cluster via shared
  Azurite — still no containers.)
- True Orleans transport-level partition. M12c simulates an inter-silo
  partition via a router-level hash filter; it does not actually drop
  packets between silos. This is clearly documented in
  `FederationRouter.IsDroppedByPartitionAsync`.

## 3. Domain model

### 3.1 Severity lattice

```
Nominal  <  UnderInspection  <  FlaggedForReview  <  Rework  <  Scrap
```

Totally ordered for the fold. `Scrap` is terminal (no further dispositions
accepted). `UseAsIs` is modelled as an explicit `MRBDisposition` fact that
demotes `FlaggedForReview` back to `Nominal`.

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

The fold is a running `Max` over the severity lattice, with a small
`retestArmed` flag threaded through to gate the `UseAsIs`-demotes-`Rework`
transition. Per-fact rules (see `StateTransitions.Apply`):

- `ProcessStepCompleted` → no state change.
- `InspectionRecorded(Pass)` → no state change; arms the retest flag when
  the part is in `Rework`.
- `InspectionRecorded(Fail)` → `Max(current, FlaggedForReview)`.
- `NonConformanceRaised(Minor)` → `Max(current, FlaggedForReview)`;
  `(Major)` → `Max(current, Rework)`; `(Critical)` → `Scrap` (terminal).
- `MRBDisposition(UseAsIs)` → demotes `FlaggedForReview` → `Nominal`
  unconditionally; demotes `Rework` → `Nominal` **only** when the retest
  flag is armed; otherwise no-op.
- `MRBDisposition(Rework)` → `Max(current, Rework)`.
- `MRBDisposition(Scrap | ReturnToVendor)` → `Scrap` (terminal).
- `ReworkCompleted(retestPassed=true)` → no state change; arms the retest
  flag when in `Rework`.
- `ReworkCompleted(retestPassed=false)` → `Max(current, FlaggedForReview)`
  and clears the retest-armed flag. A failed retest is defect evidence
  and must be observable, even when a prior `UseAsIs` (or a race trio)
  had demoted the part to `Nominal`.
- `FinalAcceptance` → no state change (its presence without outstanding
  severity is the "done" signal).

`Scrap` is terminal: any fact applied to a part already in `Scrap` is a
no-op. `ComplianceFold.Fold` orders facts by HLC before application
(`WallClockTicks`, then `Counter`, then `FactId` as a stable tiebreaker),
so concurrent producers converge on the same state. The arrival-order
baseline (`NaiveFold.Step`) delegates to the same `StateTransitions.Apply`
— the *only* difference between the two folds is the order in which
facts are applied.

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
│      (arrival-order base-            (fact store,     │
│       line + IProcessSite-           Azure Table      │
│       Grain + IPartGrain)            Storage)         │
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

> **M12 note:** the "single process, single silo" box in the diagram
> becomes two ASP.NET Core hosts in localhost mode (silo A on 5001 +
> 11111/30000, silo B on 5002 + 11112/30001), clustered via shared
> Azurite. Both silos see the same grain directory and the same Lattice
> tree; only silo A runs the `InventorySeeder`.

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
- `msmfgLatticeFacts` — Orleans.Lattice fact store (per-part fact log
  plus the `site-activity` secondary index tree and the `part-crdt`
  CRDT tree added in M12).

### 4.2 Chaos state as Orleans grains (decision 10)

- `IProcessSiteGrain` — one grain per `ProcessSite` enum value, keyed by
  the site name. Persists `IsPaused`, `DelayMs`, `ReorderEnabled`,
  `PendingFacts` queue metadata.
- `ISiteRegistryGrain` — singleton aggregator that exposes `WatchSites`
  streams and fans out preset-apply commands to the per-site grains.
- `IBackendChaosGrain` — per-backend (`"baseline"` / `"lattice"`)
  configuration for jitter, transient failure rate, and write
  amplification (see §4.3 Tier 2).
- `IPartitionChaosGrain` — M12 addition. Singleton flag flipped by
  `ChaosPreset.SiloPartition`; consulted by each silo's
  `FederationRouter.IsDroppedByPartitionAsync` and by `PartCrdtStore`.

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
| 1 | `IProcessSiteGrain.AdmitAsync` (origin site) | Site unavailable, WAN latency, out-of-order delivery from a region | `IsPaused`, `DelayMs`, `ReorderEnabled` | Pause + delay built in M4; reorder buffer wired in M7 |
| 2 | `ChaosFactBackend : IFactBackend` decorator (per backend) | Transient storage failure, per-call jitter, duplicate writes; applied to one backend only to create baseline-vs-lattice divergence | `JitterMsMin`, `JitterMsMax`, `TransientFailureRate`, `WriteAmplificationRate` on `IBackendChaosGrain` keyed by backend name | M7 |
| 3 | Reorder buffer inside `ProcessSiteGrain` | Cross-site out-of-order arrival after a regional pause lifts | `ReorderEnabled` flag on `SiteConfig` | M7 |
| 4 | `FederationRouter.IsDroppedByPartitionAsync` + `PartCrdtStore` shadow prefix | Simulated inter-silo partition (not true transport drop) | `IPartitionChaosGrain.IsPartitioned`, toggled by `ChaosPreset.SiloPartition` | M12 |

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
├── run.ps1                                           (two-silo launcher, M12)
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
│       ├── Domain/         (shared POCO / records, fold, next-action resolver)
│       ├── Baseline/       (baseline backend + grains)
│       ├── Lattice/        (lattice backend + fact store + CRDT store + site activity index)
│       ├── Federation/     (router + grain-backed site state + partition chaos)
│       ├── Inventory/      (seeder, operator-facing operations)
│       ├── Operator/       (OperatorActions facade, NextActionResolver)
│       ├── Grpc/           (service implementations)
│       ├── Dashboard/      (broadcaster + channel hub)
│       ├── Components/     (Blazor Razor components)
│       │   ├── Layout/
│       │   ├── Pages/
│       │   └── Shared/
│       └── wwwroot/        (static assets, CSS)
└── test/
    └── MultiSiteManufacturing.Tests/                 (NUnit, net10.0)
        ├── Domain/
        ├── Federation/
        ├── Inventory/
        ├── Lattice/
        ├── Operator/
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
├─────────────────────────────────────────────────────────────────────┤
│  Inventory                                                           │
│  ┌──────────────┬────────┬──────────┬──────────┬───────┬──────────┐  │
│  │ SN           │ Stage  │ Baseline │ Lattice  │ Facts │ Actions  │  │
│  │ HPT-...142   │ NDT    │ [Review] │ [Nominal]│   7   │  ✓ Fix   │  │ ← red row
│  │ HPT-...143   │ Mach.  │ [Nominal]│ [Nominal]│   3   │  ⚠ Race  │  │
│  │  ...         │  ...   │   ...    │   ...    │  ...  │   ...    │  │
│  └──────────────┴────────┴──────────┴──────────┴───────┴──────────┘  │
│  (rows where baseline ≠ lattice are highlighted red; click serial →  │
│   detail pane)                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.2 Chaos fly-out

Triggered by the top-right `☰ Chaos` button; slides in from the right.
All chaos state lives in `IProcessSiteGrain` / `IBackendChaosGrain` /
`IPartitionChaosGrain` (all persistent); reloading the browser re-renders
the current state from grain storage. The fly-out open/closed bit is
UI-local.

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
     50–250 ms jitter to the **lattice** backend only — surfaces
     baseline ↔ lattice divergence as red-highlighted rows organically.
   - *Silo partition* (M12): flips `IPartitionChaosGrain.IsPartitioned`
     so inter-silo facts and CRDT writes are confined to silo-local
     shadow prefixes until the flag clears; `PartitionHealHostedService`
     promotes the shadows on heal.
   - *Clear all*: resets every site, every backend, and the partition
     flag to nominal.
4. **Active chaos summary** — a prominent banner at the top of the
   dashboard (outside the fly-out) showing "⚠ 2 sites paused, 1 delayed,
   lattice backend flaky, inter-silo partition active" so the operator
   can never leave the fly-out open, forget about it, and be confused by
   downstream effects.

Clear labelling rule: every chaos control has an explicit plain-English
description ("Simulate 4-second latency at Toulouse NDT Lab" — not
"delay=4000"). Presets have a tooltip describing the real-world
scenario they model.

### 7.3 Real-time update strategy

Each Blazor component that displays live data owns an
`IAsyncEnumerable<T>` subscription acquired in `OnInitializedAsync` and
cancelled in `Dispose`. Internally these subscriptions are backed by
**`System.Threading.Channels.Channel<T>`** owned by the underlying
services (`InventoryService`, `SiteRegistry`, `DivergenceTracker`,
`DashboardBroadcaster`). The services push whenever the domain state
changes (grain callback, router completion, etc.). The Razor component
receives messages, applies them to its local view-model, and calls
`InvokeAsync(StateHasChanged)`.

No polling. No `Timer`. No `setInterval`. Browser reload reconnects the
SignalR circuit; components resubscribe on `OnInitializedAsync`.

The gRPC server-streaming RPCs are thin adapters over the same channels.

## 8. Bulk-load strategy

### 8.1 Seed shape

On startup (silo A only, under localhost clustering), a `IHostedService`
(`InventorySeeder`) bulk-loads ~50 parts across a spread of lifecycle
states:

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

HLCs are seeded across a 5-day window with at least a 60-minute stride
between consecutive activities on the same part (jitter 60–180 min) so
the seeded dashboard looks like it's been running for days, not seconds.

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
- Part-detail page's single "Next: …" button → `NextActionResolver`
  picks the deterministic next step from the HLC-sorted fact log;
  inline branch buttons appear only when the state genuinely requires
  operator choice (MRB disposition, NDT outcome, rework retest).
- Always-available "Raise non-conformance" form (a defect can be
  discovered at any lifecycle stage).

These flow through the federation router and therefore honor any active
chaos. This is the core "inventory-system-with-chaos-knobs" UX the user
asked for.

## 9. Testing strategy

| Layer | Framework | What's covered |
|---|---|---|
| Domain fold | NUnit | Every fact kind, every severity transition, `MRBDisposition(UseAsIs)` demotion, `ReworkCompleted(RetestPassed=false)` escalation, idempotency under duplicate facts |
| Next-action resolver | NUnit | Every resolved `NextAction` value including null guards and unordered-fact HLC sorting |
| Backends | NUnit + TestCluster | Arrival-order baseline vs. HLC-ordered lattice divergence under concurrent writes and reorder |
| Site grain | NUnit + TestCluster | `IProcessSiteGrain` persistence, pause/resume semantics, preset fan-out |
| CRDT store (M12) | NUnit + TestCluster | `PartCrdtStore` LWW register + G-Set semantics, shadow-prefix writes during partition, `HealLocalShadowAsync` promotion |
| Site-activity index (M12) | NUnit + TestCluster | Secondary-index key format, reverse scan, per-serial dedup |
| Partition chaos (M12) | NUnit | `FederationRouter.IsDroppedByPartitionAsync` hash filter; two-router simulation against one test cluster |
| Seeder | NUnit | Idempotency (runs twice → same counts), spread correctness (every state bucket populated) |
| gRPC contracts | NUnit + `Grpc.Net.Client` in-proc channel | Request/response shape, error codes, stream completion on cancel |
| UI smoke | `bUnit` (tentative) | Dashboard renders, chaos fly-out opens, clicking a canned preset configures the expected sites. Scope TBD — may defer. |

Chaos-style long-running tests remain `[Category("Chaos")]` and excluded
from the iterative dev filter.

Test cluster uses Orleans in-memory storage (no Azurite dependency in
the test suite — keeps CI fast and hermetic).

## 10. Milestones

| # | Deliverable | Status |
|---:|---|---|
| M0 | Plan reviewed + accepted | ✅ done |
| M1 | Repo restructure: scaffold new dir tree, solution/csproj setup, Azurite+Table Storage wired in `Program.cs`, build green on empty projects | ✅ done |
| M2 | Domain + fold + NUnit fold tests | ✅ done |
| M3 | Baseline + Lattice backends + `IFactBackend` + fan-out router | ✅ done |
| M4 | `IProcessSiteGrain` + `ISiteRegistryGrain` + router integration + grain tests | ✅ done |
| M5 | gRPC contracts (proto) + service implementations + contract tests via in-proc channel | ✅ done |
| M6 | Bulk-load seeder + `IInventorySeedStateGrain` + idempotency test + deterministic seed | ✅ done |
| M7 | **Fault-injection infrastructure (§4.3):** `ChaosFactBackend : IFactBackend` decorator + `IBackendChaosGrain` (jitter, transient fault rate, write amplification) + wire reorder buffer in `ProcessSiteGrain` (Tier 3) + `ListBackends` / `ConfigureBackend` RPCs on `SiteControlService` + "Lattice storage flakes" preset + domain tests for all three tiers | ✅ done |
| M8 | **Blazor Server shell + main dashboard (read-only) wired to real-time channels:** `FederationRouter.FactRouted` / `ChaosConfigChanged` events + `DashboardBroadcaster` (`IHostedService` + per-subscriber `Channel<T>`) + Pico.css v2 via jsDelivr CDN + `MainLayout` / `Dashboard` page / `InventoryGrid` / `DivergencePanel` / `ChaosBanner` + 5 broadcaster tests | ✅ done |
| M9 | **Operator action forms:** `OperatorClock` (monotonic HLC singleton) + `OperatorActions` facade (6 fact-kind methods) + `NewPartDialog` + "+ New part" button on Dashboard + clickable serials in `InventoryGrid` + `/parts/{serial}` detail page with fact trail & 5 action forms + 9 `OperatorActionsTests` | ✅ done |
| M10 | **Chaos fly-out** (two-section: site controls + backend storage chaos) with canned presets + active-chaos banner — `ChaosFlyout.razor` (slide-in side panel wired to `FederationRouter` + `DashboardBroadcaster`), `ChaosPresetInfo`, `MainLayout.razor` toggle + slide-in CSS, 5 `ChaosPresetInfoTests` | ✅ done |
| M11 | Divergence feed (organic, from chaos-induced reorder + backend fault rate) wired into dashboard + `WatchDivergence` gRPC stream | ✅ done |
| M11a | Per-row **⚠ Race** button on `InventoryGrid` — emits a concurrent NCR(Minor) → Inspection(Pass) → MRB(UseAsIs) trio via `OperatorActions.RaceAsync`. Lattice always folds by HLC to Nominal; combined with the *Baseline reorder storm* preset or a multi-second site delay, baseline folds the shuffled arrival order to Flagged — the canonical UI-driven way to force a row in the Divergence feed. Paired with a per-row **✓ Fix** button that replaces Race on diverged rows and emits a single `MrbDisposition(UseAsIs)` via `OperatorActions.FixAsync` to restore baseline ↔ lattice agreement. | ✅ done |
| M12 | **Make it actually a Lattice demo.** Today the sample exercises `HybridLogicalClock` + an HLC-sorted event replay, which any event-sourced Orleans codebase could reproduce. M12 adds the features that are *uniquely* `Orleans.Lattice`: <br/>**(a) Second silo on localhost** — update `Program.cs` to support `--silo-id` and an override port, document `dotnet run -- --silo-id a` / `--silo-id b` in the README. Lifts the "Single host" non-goal in §2; both silos join the same Orleans cluster and share Azure Table Storage. <br/>**(b) CRDT-typed grain state on `IPartGrain`** — at least one property that merges without sorting events: `CurrentOperator : LWW<OperatorId>` and `ProcessLabels : GSet<string>`. Surfaced on the part-detail page. Writes from either silo converge on read, demonstrating the CRDT merge story naked Orleans grains can't tell. <br/>**(c) Partition-and-heal preset** — new `ChaosPreset.SiloPartition` that drops inter-silo traffic for N seconds via a transport filter, then heals. Operators on silo A write to one set of parts, operators on silo B to another overlapping set; on heal, `LWW`/`GSet` merge is observably correct. <br/>**(d) B+ tree range query** — new `/inventory/by-site/{site}` view uses `ILattice` range scan over a `{site}/{stage}/{serial}` composite key to list parts by site, exercising the sharded tree directly (not the per-key grain). Add a "Parts currently at $site" panel to the dashboard. <br/>**(e) Tests** — partition-heal integration test (writes on both silos during partition → merged state after heal), range-scan contract test, `LWW`/`GSet` convergence property tests. <br/>Keeps the existing baseline-vs-lattice story intact; M12 is additive. | ✅ done |
| M13 | README, glossary, architecture doc, Azurite setup instructions, screenshots, `run.ps1` walk-through | 0.5 day |
| M14 | Test pass, polish, Chaos-category stress test | 0.5 day |

> **M12 implementation notes (actual vs. plan):**
>
> - **(a)** Delivered via `--silo-id a|b` in `Program.cs`, `SiloIdentity`
>   DI record, and a top-level `run.ps1`. Silo A owns HTTP 5001 + Orleans
>   ports 11111/30000; silo B owns HTTP 5002 + 11112/30001. Only silo A
>   runs the `InventorySeeder`.
> - **(b)** `PartCrdtStore` (not `IPartGrain`) owns the CRDT state, keyed
>   directly into an `ILattice` tree. `Orleans.Lattice.Primitives.LwwValue<T>`
>   is `internal` in the library, so the LWW register is modelled as a
>   single-key write (last HLC wins inside the tree) and the G-Set is
>   modelled as one key per label read back via `ScanKeysAsync` — both
>   visible on `/parts/{serial}`.
> - **(c)** `ChaosPreset.SiloPartition` flips a flag on
>   `IPartitionChaosGrain`; each silo's `FederationRouter` drops facts
>   whose serial hashes to the other silo's bucket. This is *simulation,
>   not a true transport partition* (see the §2 non-goal) and is
>   documented in `FederationRouter.IsDroppedByPartitionAsync`.
>   **CRDT writes are also gated by the same flag:** during partition
>   `PartCrdtStore` writes to a silo-local shadow prefix
>   (`shadow/{siloId}/…`) inside the shared Lattice tree, and reads
>   merge `(shared ∪ own-shadow)` — so silo A and silo B observably
>   diverge while the flag is true. `PartitionHealHostedService` on each
>   silo polls the grain every 2s and, on the `true→false` transition,
>   invokes `PartCrdtStore.HealLocalShadowAsync()` which promotes every
>   shadow entry into the shared prefix (LWW register + G-Set union)
>   and deletes the shadow keys. The chaos banner gains a "inter-silo
>   partition active" entry via a new `ChaosOverview.PartitionActive`
>   field so operators can see partition state at a glance.
> - **(d)** `SiteActivityIndex` (hosted service, formerly
>   `SiteStageIndex`) writes a secondary index into a dedicated
>   lattice tree on **every** routed `Fact` — not just
>   `ProcessStepCompleted`. Keys are
>   `{site}/{wallTicks:D20}/{counter:D10}/{serial}`; the value holds
>   a short activity label (`"Step: Machining"`,
>   `"Inspection: CMM Pass"`, `"MRB: UseAsIs"`, …). The dashboard's
>   "Parts by site" panel renders the result of
>   `ILattice.ScanEntriesAsync(reverse: true)` inline when the operator
>   clicks a site link — no separate page — exercising a direct B+
>   tree range query under the `{site}/` prefix. Results are deduped
>   by serial so each part appears once with its most recent activity
>   at that site. Generalizing beyond `ProcessStepCompleted` means
>   inspection-only sites like `StuttgartCmmLab` now surface their CMM
>   inspection history instead of appearing permanently empty.
> - **(e)** 16 new tests: 10 × `PartCrdtStoreTests`, 4 × `SiteStageIndexTests`,
>   3 × `PartitionChaosTests`. Two-silo `TestCluster` integration is
>   deferred to M13/M14 because `Orleans.TestingHost`'s localhost
>   clustering hand-off is brittle under Azurite. Partition semantics
>   are exercised by constructing two `FederationRouter` instances with
>   distinct `SiloIdentity` against a single test cluster, which covers
>   the same logic.
>
> **Post-M12 UX polish:** the part-detail "Record action" block no
> longer renders six `<details>` forms with free-form dropdowns.
> `NextActionResolver.Resolve(facts)` inspects the HLC-sorted fact log
> and returns a single suggested next step (`CompleteHeatTreat`,
> `CompleteMachining`, `RecordCmmInspection`, `RecordNdtInspection`,
> `SignOffFai`, `CompleteRework`, `IssueMrb`, or `Terminal`). The page
> renders **one** primary button for deterministic states, and shows
> branch buttons inline only when the state genuinely requires operator
> choice: 4 MRB buttons for an open NCR, Pass/Fail buttons for a pending
> NDT inspection, and retest-passed/failed buttons for ordered rework.
> A compact "Raise non-conformance" form remains always available (a
> defect can be discovered at any lifecycle stage). 17 new tests in
> `NextActionResolverTests` cover every state transition including null
> guards and unordered-fact HLC sorting.
>
> **Fold semantic fix (post-M12):** `ReworkCompleted(RetestPassed=false)`
> was originally a no-op for state (only cleared the retest-armed flag).
> That made operator clicks on "Rework complete (retest failed)" visibly
> do nothing — especially noticeable after the ⚠ Race button demoted a
> part to `Nominal` on the lattice side. The fold now escalates a failed
> retest to `Max(current, FlaggedForReview)`, consistent with "a failed
> retest is defect evidence". Covered by 3 new tests in
> `ComplianceFoldTests` (Nominal→Flagged, Rework preserved, Scrap
> terminal).

