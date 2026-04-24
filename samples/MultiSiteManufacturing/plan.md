# MultiSiteManufacturing Sample — Plan

> Status: **M1–M14 complete** (committed on `feature/sample-manufacturing`,
> 163/163 non-chaos tests green). Docs (README, glossary, architecture,
> Azurite setup, screenshots) remain as a follow-up outside the milestone
> numbering.

## 1. Goals

Demonstrate `Orleans.Lattice` as the persistence layer for a **regulated
process-engineering traceability system**, using the turbine-engine part
lifecycle (forge → heat-treat → machining → NDT → MRB → FAI) as the
concrete vertical. The sample must:

1. Ship a **working inventory system** — not a scripted replay. An operator
   can create parts, advance them through process stages, record
   inspections, raise non-conformances, issue MRB dispositions, and sign
   off FAI, and the sample behaves like a minimal MES/QMS thin slice.
2. Bulk-load a realistic **pre-seeded inventory** on silo startup (5
   representative parts — one per reachable `ComplianceState` outcome)
   so the dashboard looks lived-in the moment the browser connects.
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
- Kubernetes / production orchestration. M14 adds Docker Compose as the
  supported local topology (two clusters × two silos × per-cluster
  Azurite, only HTTP ports 5001–5004 exposed to the host), but there is
  no Helm chart, no k8s manifest, and no TLS/auth hardening.
- True Orleans transport-level partition at the router layer. M12c
  simulates an intra-cluster split via a router-level hash filter; it
  does not drop packets between silos. (M14 adds a genuine Tier-5
  transport-level partition option via `docker network disconnect` — 
  see §4.3 — but the Tier-4 hash-filter sim still exists as the fast
  path that requires no Docker interaction.)

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
  `ChaosPreset.ClusterSplit`; consulted by each silo's
  ``FederationRouter.IsDroppedByPartitionAsync` and by `PartCrdtStore`.
- `IReplicationDisconnectGrain` — M14 addition. Singleton flag flipped by
  `ChaosPreset.ReplicationDisconnect`; consulted by
  `ReplicatorGrain.TickAsync` (outbound ship) and the
  `POST /replicate/{tree}` inbound endpoint (503 short-circuit).

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
| 4 | `FederationRouter.IsDroppedByPartitionAsync` + `PartCrdtStore` shadow prefix | Simulated inter-silo partition (not true transport drop) | `IPartitionChaosGrain.IsPartitioned`, toggled by `ChaosPreset.ClusterSplit` | M12 |
| 4b | `ReplicatorGrain.TickAsync` early-return + `POST /replicate/{tree}` 503 | App-level cross-cluster replication pause — outbound ship and inbound apply both suppressed; replog grows locally, resumes from cursor on heal | `IReplicationDisconnectGrain.IsDisconnected`, toggled by `ChaosPreset.ReplicationDisconnect` | M14 |
| 5 | `docker network disconnect msmfg_wan <silo>` | **Genuine** cross-cluster transport partition — silos on the disconnected container can't reach the peer cluster's `/replicate/{tree}` endpoint; replog grows locally, replicator backs off, replay resumes after `docker network connect`. | Manual `docker network` commands (Compose-only) | M14 |

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
│  Inventory                                                           │
│  ┌──────────────┬────────┬──────────┬──────────┬───────┬──────────┐  │
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
All chaos state lives in `IProcessSiteGrain` / `IBackendChaosGrain` /`
IPartitionChaosGrain` (all persistent); reloading the browser re-renders
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
   - *Cluster split* (M12): flips `IPartitionChaosGrain.IsPartitioned`
     shadow prefixes until the flag clears; `PartitionHealHostedService`
     promotes the shadows on heal. Intra-cluster only — cross-cluster
     replication still runs.
   - *Replication disconnect* (M14): flips
     `IReplicationDisconnectGrain.IsDisconnected` so the outbound
     replicator tick is a no-op and the inbound endpoint returns 503.
     The local replog grows until the flag clears, at which point
     replication resumes from the current cursor and catches the peer
     up with the accumulated backlog. The app-level equivalent of
     `docker network disconnect msmfg_wan`.
   - *Clear all*: resets every site, every backend, the cluster-split
     flag, and the replication-disconnect flag to nominal.
4. **Active chaos summary** — a prominent banner at the top of the
   dashboard (outside the fly-out) showing "⚠ 2 sites paused, 1 delayed,
   lattice backend flaky, cluster split active, cross-cluster replication disconnected" so the operator
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
services (`InventoryService`, `SiteRegistry`, `DivergenceTracker``,
`DashboardBroadcaster`). The services push whenever the domain state
changes (grain callback, router completion, etc.). The Razor component
receives messages, applies them to its local view-model, and calls
`InvokeAsync(StateHasChanged)`.

**Cluster-wide fan-out via an Orleans Azure Storage Queue stream.**
`FederationRouter` events (`FactRouted`, `FactReplicated`) are
silo-local, but a Blazor Server circuit is pinned to one silo — so a
naïve local-only fan-out would only reach users whose circuit happens
to live on the silo that handled the fact. To fix this,
`DashboardBroadcaster` on every silo publishes each incoming fact to a
single cluster-wide Orleans stream backed by **Azure Storage Queues**
(provider `DashboardStreams`, namespace `msmfg.dashboard.facts`,
queue `msmfgdashboard-0`) and subscribes to the same stream. Orleans
stream pub/sub delivers each message to every subscribed silo, where
the local broadcaster rebuilds the `PartSummaryUpdate` /
`SiteActivityIndexEntry` and writes to its per-circuit `Channel<T>`
instances. A fact therefore reaches every Blazor circuit regardless of
which silo originated it or which silo the user is connected to. The
in-memory / single-silo quick-start swaps the queue provider for the
memory-stream provider under the same name so the broadcaster wiring
is identical across modes.

Durability + reliability properties of the queue provider:

- **Silo restart / transient outage.** Messages enqueued while a silo
  is down are picked up when it comes back — the feed self-heals
  without operator action.
- **PubSubStore (Azure Table).** Subscription metadata is persisted
  in Azure Table Storage so a silo restart resumes existing
  subscriptions rather than resetting the feed.
- **Publish retries.** `DashboardBroadcaster.PublishToBroadcastStreamAsync`
  retries transient publish failures (100 ms / 400 ms / 2 s
  exponential backoff) before logging at Warning and dropping the
  update — a single lost publish means one missed animation tick,
  not a persistent feed outage.
- **Subscribe retries.** `StartAsync` retries `SubscribeAsync` with
  bounded backoff (up to ~32 s across five attempts) so slow queue
  provisioning or PubSubStore readiness at silo startup doesn't
  crash the host; a permanent failure logs at Error and leaves the
  silo functional without the live feed (page reloads fall back to
  the initial snapshot path).
- **Mid-flight reconnect.** The subscription is registered with an
  `onError` callback that kicks off a background resubscribe, so a
  transient queue-agent fault doesn't silently silence the silo.
- **Poison-fact isolation.** `OnBroadcastReceived` wraps fan-out in
  a top-level try/catch; a single bad fact is logged and dropped
  rather than allowed to propagate back into Orleans, which would
  otherwise retry delivery indefinitely and block every subsequent
  message on the same queue.

## 8. Bulk-load strategy

### 8.1 Seed shape

On startup (every silo; gated by a singleton `IInventorySeedStateGrain`
flag persisted to Azure Table Storage so only the first silo to win
the race actually seeds), a `IHostedService` (`InventorySeeder`) bulk-loads
**5 parts** — one representative per reachable `ComplianceState` outcome —
to keep the demo inventory small and readable while still exercising every
fold transition the UI needs to render:

| Seq | State | Stage reached |
|---:|---|---|
| 1 | `Nominal` | Forge only |
| 2 | `Nominal` | Full lifecycle including FAI signed off |
| 3 | `FlaggedForReview` | Machining + CMM pass, then NDT raised minor NC |
| 4 | `Rework` | Major NC + MRB Rework disposition |
| 5 | `Scrap` | Critical NC |

> **Reachability note:** the fact grammar has no `InspectionStarted`
> transition, so `UnderInspection` is not reachable by folding any fact
> sequence in v1. The seed therefore skips it. The constant
> `InventorySeeder.TotalParts = 5` is the canonical count referenced by
> the idempotency test.

Serial numbers are deterministic (`HPT-BLD-S1-2028-00001` … `-00005`)
across runs, so the same five parts show up every time. HLCs are
**not** deterministic across runs — each fact is stamped relative to
`DateTimeOffset.UtcNow` so the dashboard always shows "recent"
activity (the seed window scrolls with wall-clock time); within a
single run the intra-part cadence is a jittered 60–180 minute gap and
the ordering between parts is stable.

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
| UI smoke | not implemented | Dashboard renders, chaos fly-out opens, clicking a canned preset configures the expected sites. Deferred — the gRPC + grain + backend test layers cover the logic; the UI is thin Razor over services. |

Chaos-style long-running tests remain `[Category("Chaos")]` and excluded
from the iterative dev filter.

Test cluster uses Orleans in-memory storage (no Azurite dependency in
the test suite — keeps CI fast and hermetic).

## 10. Milestones

| # | Deliverable | Status |
|---|---|---|
| M1 | Repo restructure: scaffold new dir tree, solution/csproj setup, Azurite+Table Storage wired in `Program.cs`, build green on empty projects | ✅ done |
| M2 | Domain + fold + NUnit fold tests | ✅ done |
| M3 | Baseline + Lattice backends + `IFactBackend` + fan-out router | ✅ done |
| M4 | `IProcessSiteGrain` + `ISiteRegistryGrain` + router integration + grain tests | ✅ done |
| M5 | gRPC contracts (proto) + service implementations + contract tests via in-proc channel | ✅ done |
| M6 | Bulk-load seeder + `IInventorySeedStateGrain` + idempotency test + deterministic seed | ✅ done |
| M7 | **Fault-injection infrastructure (§4.3):** `ChaosFactBackend : IFactBackend` decorator + `IBackendChaosGrain` (jitter, transient fault rate, write amplification) + wire reorder buffer in `ProcessSiteGrain` (Tier 3) + `ListBackends` / `ConfigureBackend` RPCs on `SiteControlService` + "Lattice storage flakes" preset + domain tests for all three tiers | ✅ done |
| M8 | Dashboard (Blazor Server) — parts grid, part-detail page, live push via `DashboardBroadcaster` / channel hub, with cluster-wide fan-out over an Orleans Azure Storage Queue stream (provider `DashboardStreams`, namespace `msmfg.dashboard.facts`, PubSubStore in Azure Table) so every silo's Blazor circuits see every fact and the feed survives silo restarts | ✅ done |
| M9 | Operator actions — next-action resolver, `OperatorActions` facade, UI buttons driving `InventoryService` / `FactIngressService` | ✅ done |
| M10 | Chaos fly-out side panel — site chaos rows, backend chaos sliders, canned presets, active-chaos banner | ✅ done |
| M11 | Federation routing hardening — deterministic fan-out, `FactRouted` / `ChaosConfigChanged` events, dashboard subscriber | ✅ done |
| M12 | Intra-cluster silo partition (§4.3 Tier 4): `IPartitionChaosGrain`, FNV-1a ordinal hash filter in `FederationRouter`, `PartCrdtStore` shadow-prefix writes, `PartitionHealHostedService`, `SiteActivityIndex` secondary B+ tree, two-silo `run.ps1` launcher | ✅ done |
| M13 | **Cross-cluster replication (§11):** two independent Orleans clusters (`forge`, `heattreat`) each with two silos and its own Azurite, linked by per-tree HTTP replication over an HLC-ordered replog. Opt-in per tree, loop-broken via `RequestContext`, compacted by a janitor grain, with FUTURE seams for the library-native change-feed + continuous-merge capability. | ✅ done |
| M14 | **Docker Compose topology (§12):** replace the localhost multi-process launcher with a Compose file that runs two Azurite containers and four silos across three networks (`forge-net`, `heattreat-net`, `wan`), publishing only HTTP ports 5001–5004 to the host. `Program.cs` honours `ASPNETCORE_URLS` and binds Orleans endpoints on `0.0.0.0`. `run.ps1` is a `docker compose` wrapper. Enables Tier-5 genuine transport-level partition via `docker network disconnect`. | ✅ done |
| — | Docs (README.md, `docs/architecture.md`, `docs/glossary.md`, Azurite setup, screenshots) | 🗒️ deferred, outside milestone numbering |

## 11. Cross-cluster replication (M13)

### 11.1 Motivation

Up through M12 the sample ran as a single Orleans cluster with two
silos — a reasonable HA topology, but not what "multi-site" actually
implies at the infrastructure level. Real factory floors run their own
cluster per site, own storage per site, and converge state across the
WAN. M13 makes the sample model that: two independent Orleans clusters
(`forge`, `heattreat`), two independent Azurite instances, four silos
total — linked only by an explicit application-level replication hop
running on top of the existing lattice.

The cross-cluster path is deliberately **outside** `Orleans.Lattice`:
zero library changes, zero `LatticeOptions` edits. This both respects
the current library surface and leaves clean FUTURE seams for when the
library gains its planned change-feed / cross-tree continuous-merge
capability — at which point the M13 replicator collapses into a thin
adapter over the library primitive.

### 11.2 Topology

Each cluster is a full Orleans deployment:

| Cluster | `ClusterId` | Silos | HTTP ports | Silo ports | Gateway ports | Azurite (blob/queue/table) |
|---|---|---|---|---|---|---|
| `forge` | `msmfg-forge` | A, B | 5001 / 5002 | 11111 / 11112 | 30000 / 30001 | 10000 / 10001 / 10002 |
| `heattreat` | `msmfg-heattreat` | A, B | 5003 / 5004 | 11121 / 11122 | 30010 / 30011 | 20000 / 20001 / 20002 |

Silo process args: `--cluster <name> --silo-id <a|b>`.
`appsettings.cluster.<name>.json` supplies the per-cluster connection
string, ports, and the peer list.

`run.ps1` launches two Azurite processes (distinct port sets, distinct
workspace dirs) and four silos, and the two clusters reach each other
over plain HTTP to localhost peer ports — the replication loop is
transport-agnostic at the protocol boundary but trivially observable on
the dev box.

### 11.3 What requires replication (the discovery problem)

The replicator's core question is "what has changed locally since I
last synced with peer X?". M13 answers it with an **HLC-ordered
replication log** (the "replog") maintained per replicated tree.

- A lightweight `IOutgoingGrainCallFilter`
  (`LatticeReplicationFilter`) sits on the outgoing call path and
  fires after every `ILattice.SetAsync` / `DeleteAsync` on a tree
  listed in `ReplicationTopology.ReplicatedTrees`. It appends one
  envelope to a sibling lattice tree `_replog__{tree}` keyed
  `{wallTicks:D20}{counter:D10}|{clusterId}|{op}|{key}` — zero-padded
  HLC first so a forward lex scan is HLC-ascending, cluster id as
  tiebreaker for cross-cluster ordering, original key last for
  uniqueness.
- **Implementation gotcha.** The filter must read the call arguments
  (`context.Request.GetArgument(0)` for the user key) **before**
  awaiting `context.Invoke()`. Orleans codegen releases reference-type
  slots on the invokable as soon as the wire message is dispatched, so
  any reference-type arg read back after the await is `null`. Struct
  args such as `CancellationToken` survive the release but are not
  useful here. The filter stashes `methodName`, `treeName`, and the
  original key into local variables before the await, then acts on
  them after the call completes.
- The replog **envelope value** does not carry the user bytes. The
  replicator looks up the primary tree's current `(value, hlc)` at
  ship time, so a key that's been overwritten since the log entry
  landed still ships its latest value — which is correct under
  last-writer-wins semantics.
- A `RequestContext` flag (`lattice.replay = sourceCluster`) is set on
  inbound replay so the filter skips appending to the replog when the
  write is itself a replicated apply. This breaks the A → B → A
  cycle at the application layer without any library support.

> **Future seam.** When `Orleans.Lattice` ships native change-feed
> events, `LatticeReplicationFilter` and the `_replog__{tree}` tree
> are replaced by a direct subscription. The rest of the pipeline
> (`ReplicatorGrain`, `ReplogJanitorGrain`, inbound endpoint) stays as
> it is — the replog is purely the discovery mechanism. Each file
> carries a `FUTURE:` comment marking the replacement seam.

### 11.4 Replicator grain

One `IReplicatorGrain` per `(tree, peer-cluster)` pair, grain key
`"{tree}|{peer}"`. Backed by Orleans grain storage on the
`msmfgGrainState` table — **not** by the lattice — because the
replicator's cursor is operational state, not domain state, and has a
different lifecycle.

Persistent state:

```csharp
internal sealed record ReplicatorState
{
    public HybridLogicalClock Cursor            { get; init; }
    public DateTimeOffset     LastContactUtc    { get; init; }
    public long               TotalRowsShipped  { get; init; }
    public int                ConsecutiveErrors { get; init; }
}
```

The grain doesn't track per-key state — the replog is the truth.

**Tick cadence.** Orleans reminders have a **1-minute minimum period**,
so the grain uses a split schedule:

- A **1-minute reminder** (`keepalive`) acts purely as a durable
  re-activation trigger — it ensures the grain reanimates after silo
  restart or idle deactivation and re-registers its timer.
- A **3-second grain timer** (`this.RegisterGrainTimer(TickAsync, 1s, 3s)'])
  drives the actual shipping loop. Grain timers have no minimum period
  and are auto-disposed on deactivation, so this is the idiomatic
  sub-minute-cadence pattern under Orleans 10.

Tick loop:
1. Scan `_replog__{tree}` with a half-open range from `Cursor+` to
   the replog's end, bounded by a batch size.
2. In-memory dedupe: keep only the highest HLC per key in the batch.
3. For each surviving entry call `GetAsync(tree, key)` on the primary
   tree and ship `(key, value?, hlc, op)` — `op = Delete` when the
   log envelope is a tombstone and the primary read returns `null`.
4. POST to the peer via `ReplicationHttpClient.SendAsync`, which
   iterates `peer.BaseUrls` in order and returns on the first 2xx;
   only throws (and bumps the error counter) when every URL has
   failed. A single peer-silo restart therefore never stalls shipping.
5. On ack, advance `Cursor` to the highest HLC in the batch, persist
   state, reschedule immediately if more is pending; on failure,
   exponential backoff.

### 11.5 Inbound endpoint

A minimal API endpoint on each silo: `POST /replicate/{tree}`. The
handler validates the shared-secret header, then for each entry:

```csharp
RequestContext.Set("lattice.replay", batch.SourceCluster);
if (entry.Op == ReplicationOp.Delete)
    await tree.DeleteAsync(entry.Key);
else
    await tree.SetAsync(entry.Key, entry.Value);
```

Idempotency is a consequence of the sample's key disciplines — see
§11.7. No HLC is threaded through on apply; the receiver's lattice
assigns its own HLC, which is fine for immutable-keyed trees. The
LWW-register half of `mfg-part-crdt` (the `/operator` key) would be
a divergence risk under the same apply path, so the outgoing
`LatticeReplicationFilter` drops those keys at origin via
`ReplicationTopology.IsKeyReplicated` — they never enter the replog
and the inbound endpoint never sees them. The FUTURE seam for
library-native source-HLC-preserving apply is exactly the place
where that filter goes away and the register can replicate too.

### 11.6 Compaction

`IReplogJanitorGrain` per tree (reminder every 10 min): reads every
peer replicator's `Cursor`, takes the **min**, and deletes replog
entries with `hlc <= min - retention` (retention = 24 h). Never prunes
ahead of the slowest peer.

### 11.7 Which trees opt in, and why

`ReplicationTopology.ReplicatedTrees` is explicit **tree-level**
opt-in; `ReplicationTopology.IsKeyReplicated(tree, key)` layers an
additional **per-key** filter on top so a single tree can mix
replicated and cluster-local sub-keys. The shipped defaults:

| Tree | Key shape | Replicated? | Why |
|---|---|---|---|
| `mfg-facts` | `{serial}/{wallTicks:D20}/{counter:D10}/{factId}` | **Yes (default)** | Every fact is a new immutable key; double-apply is an idempotent `SetAsync` on an existing key with an identical value. |
| `mfg-site-activity-index` | `{site}/{wallTicks:D20}/{counter:D10}/{serial}` | **Yes (default)** | Same reasoning — one entry per fact, never overwritten. |
| `mfg-part-crdt` (labels) | `{serial}/labels/{label}` | **Yes** when the tree is opted in | G-Set semantics — union is commutative, associative, idempotent. Two clusters that both add the same label converge trivially. |
| `mfg-part-crdt` (operator register) | `{serial}/operator` | **Never** — filtered at origin by `IsKeyReplicated` | LWW by the receiver's local HLC, not the source HLC. Concurrent cross-cluster writes would diverge. The outgoing filter drops these keys before they hit the replog; the inbound endpoint never sees them. |

The shipped cluster config (`appsettings.cluster.*.json`) opts all
three trees in. The per-key filter keeps the operator register
cluster-local even when `mfg-part-crdt` is in `ReplicatedTrees`, so
the G-Set half ships correctly while the LWW half stays safe.

The `mfg-part-crdt` per-key filter is the **only** tree-specific
filter today. All other replicated trees use write-once immutable
keys and `IsKeyReplicated` is the identity for them. When
`Orleans.Lattice` ships native source-HLC-preserving apply, the
operator-register filter collapses and the whole tree can replicate
unconditionally — that is the future seam referenced in §11.3.

### 11.8 Failure modes and anti-entropy

- **Peer unreachable** — reminder retries with backoff.
- **Filter fails after a primary write succeeds** — narrow race. A
  second reminder on each replicator runs a periodic full-tree sweep
  (`AntiEntropyCursor`) that re-ships any primary entry whose HLC is
  newer than the cursor. O(N) but bounded; acceptable as a backstop.
- **Duplicate delivery** — idempotent under the key disciplines above.
- **A → B → A replay cycle** — broken by the `lattice.replay`
  `RequestContext` flag.

### 11.9 Test surface

Integration-style end-to-end tests (two full Orleans clusters) are out
of scope for the Orleans `TestingHost` fixture used by the rest of the
sample (it materialises a single cluster). M13 ships unit tests for
the deterministic pieces:

| Area | Framework | What's covered |
|---|---|---|
| Replog key encoding | NUnit | Zero-padding, lex-sort matches HLC order with cluster-id tiebreaker |
| `ReplicationTopology` parsing | NUnit | Config binding, default-opt-in tree list, peer URL validation |
| Wire types | NUnit | JSON round-trip for `ReplicationBatch` / `ReplicationEntry` / `ReplicationAck` |

All tests live in `test/MultiSiteManufacturing.Tests/Replication/` and
run with the rest of the suite (`dotnet test --filter "TestCategory!=Chaos"`).
Two-cluster end-to-end is exercised manually via `run.ps1` (launches
two Azurites + four silos on the per-cluster ports documented in §11.2).


## 12. Docker Compose topology (M14)

### 12.1 Motivation

Through M13 the sample ran via `run.ps1` spawning two Azurite processes
and four silo processes on the developer machine, clustering over
loopback. That worked but was fragile: PowerShell automatic-variable
shadowing (`$args`, `$Host`) hid Azurite startup failures; the seeder
and the replication bootstrap raced silo membership; "multi-site" was
still a single flat loopback address space. M14 replaces the process
launcher with Docker Compose so the topology is structural, not
accidental.

### 12.2 Topology

Two networks, scoped so that no silo in one cluster shares a network
with a silo in the other cluster. The **only** bridge between clusters
is the Traefik proxy, which is multi-homed onto both cluster nets:

| Network | Purpose | Services |
|---|---|---|
| `forge-net` | Forge cluster membership + Azurite access + cross-cluster ingress for heattreat | `azurite-forge`, `silo-forge-{a,b}`, `traefik-forge`, **`traefik-heattreat`** |
| `heattreat-net` | Heattreat cluster membership + Azurite access + cross-cluster ingress for forge | `azurite-heattreat`, `silo-heattreat-{a,b}`, `traefik-heattreat`, **`traefik-forge`** |

There is **no** shared `wan` network. The peer Traefik is attached to
the local cluster net specifically so local silos can resolve and
reach it, without ever being on a network a peer silo can see. The
resulting isolation guarantees:

| From → To | Path | Reachable? |
|---|---|---|
| `silo-forge-a` → `silo-forge-b` | `forge-net` | Yes (direct, same cluster) |
| `silo-forge-*` → `traefik-heattreat` | `forge-net` (Traefik is multi-homed) | Yes (via proxy) |
| `silo-forge-*` → `silo-heattreat-*` | — | **No shared network — blocked** |
| `silo-heattreat-*` → `silo-forge-*` | — | **No shared network — blocked** |
| `azurite-forge` ↔ `azurite-heattreat` | — | No shared network — blocked |

Azurite is deliberately reachable only from its own silos, mirroring
the real per-site storage isolation the cross-cluster design models.

Only **two** host ports are published — one per cluster, both going to
that cluster's Traefik:

| Host port | Container | Role |
|---|---|---|
| 5001 | `traefik-forge:80` | Forge UI (sticky LB) + replication inbound (round-robin LB) |
| 5002 | `traefik-heattreat:80` | Heattreat UI (sticky LB) + replication inbound (round-robin LB) |

Silo HTTP (`:8080`), Orleans silo (`:11111`), and gateway (`:30000`)
are `expose`-d only on their internal networks — no silo port is
reachable from the host. Each Traefik runs two routers against the
same backend pool:

| Router | Rule | LB strategy | Why |
|---|---|---|---|
| `{cluster}-replicate` | `PathPrefix(`/replicate`)`, priority 100 | round-robin + active health check | Replication POSTs are stateless; either silo can apply a batch; faster convergence on silo restart than app-layer URL walking. |
| `{cluster}-web` | `PathPrefix(`/`)` | sticky cookie (`msmfg_{cluster}_affinity`) | Blazor Server's SignalR circuit must pin to a single silo for the browser tab's lifetime, or the reconnect handshake tears the circuit down. |

Replication path under this topology:

```
silo-forge-X  --(forge-net)--> traefik-heattreat --(heattreat-net)--> silo-heattreat-{a|b}
```

The peer config therefore lists a single URL per peer
(`http://traefik-heattreat:80` from forge; `http://traefik-forge:80`
from heattreat). `ReplicationHttpClient` still walks a multi-URL
`BaseUrls` list; see §12.3 for the three deployment shapes the list
supports.

### 12.3 Config override via env vars

`appsettings.cluster.{name}.json` still ships the same localhost defaults.
In Docker, compose overrides just
the fields that differ:

- `ConnectionStrings__AzureTableStorage` → `http://azurite-{cluster}:10002/...`
- `Replication__Peers__0__Name` → peer cluster short name.
- `Replication__Peers__0__BaseUrls__0` → `http://traefik-{peer}:80`.
  A single URL per peer because Traefik's round-robin `/replicate/*`
  router plus its 2 s health check handles silo-level failover inside
  the peer cluster. `ReplicationHttpClient.SendAsync` still iterates
  the list in order and short-circuits on first 2xx, so adding more
  URLs only matters for topologies where the client — not an LB — is
  the unit of failover. The three supported shapes:

    1. **Single LB endpoint (the Compose default).** One URL per peer.
       Traefik absorbs silo churn; the client never notices.
    2. **Explicit per-silo fan-out (localhost dev, no LB).** One URL
       per silo — the shape `appsettings.cluster.*.json` ships with
       for `dotnet run` without Docker. The client tries each silo in
       order until one accepts the batch.
    3. **Multi-zone failover (advanced).** One URL per availability
       zone, each pointing at that zone's regional LB — e.g.
       `https://heattreat-az1.example.com`,
       `https://heattreat-az2.example.com`. The client stays on the
       primary zone while it's healthy and only falls over when every
       silo behind that zone's LB has failed. Because the walk
       short-circuits on first success, the primary zone absorbs all
       traffic under nominal conditions — ideal when cross-zone
       bandwidth is metered.

- `Cluster__SiloPortA=11111 Cluster__SiloPortB=11111` — both silos can
  use the same Orleans port since each container has its own IP.
- `ASPNETCORE_URLS=http://+:8080` — `Program.cs` skips its own
  `UseUrls` call when this env var is set.
- `Seeder__Enabled` — explicit boolean override for the inventory
  seeder. When present, takes precedence over the legacy
  cluster-and-silo heuristic. Compose sets `Seeder__Enabled=true` on
  `silo-forge-a` and `Seeder__Enabled=false` on the other three silos
  so the seed decision lives declaratively in the topology file.

### 12.4 Genuine partition (Tier 5)

```
# Sever forge -> heattreat (remove peer Traefik from local cluster net):
docker network disconnect msmfg_forge-net     msmfg-traefik-heattreat
docker network disconnect msmfg_heattreat-net msmfg-traefik-forge
# ... demonstrate divergence ...
docker network connect    msmfg_forge-net     msmfg-traefik-heattreat
docker network connect    msmfg_heattreat-net msmfg-traefik-forge
```

Disconnecting the peer Traefik from the local cluster net removes the
only route from local silos to the peer cluster (recall: silos have no
direct network path to peer silos in this topology — the Traefik is
the single chokepoint by design). The replicator's HTTP POST fails,
exponential backoff engages, and the replog grows locally. The peer
cluster sees nothing. On reconnect the replicator ships the
accumulated batch in HLC order and the peer converges. This is a
genuine transport drop, complementing (not replacing) the Tier-4
hash-filter sim which remains useful in tests where Docker is not
available.

Disconnecting a single silo from its own cluster net is a different
failure mode (the silo is removed from the UI/replication LB by
Traefik's health check within ~2 s, and Orleans re-activates the
replicator grain on the surviving silo); the cross-cluster topology
is unaffected. "Silo X of the peer cluster is restarting" is handled
by the peer Traefik with no visible hiccup to the local cluster.

