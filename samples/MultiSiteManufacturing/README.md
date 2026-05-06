# MultiSiteManufacturing — an Orleans.Lattice sample

A working thin slice of a regulated process-engineering traceability
system (turbine-blade lifecycle: forge → heat-treat → machining → NDT
→ MRB → FAI) that uses **`Orleans.Lattice`** as the fact store and
convergent state layer for an inventory system running across two
independent Orleans clusters.

It is not a scripted replay. Operators create parts, advance them
through process stages, record inspections, raise non-conformances,
issue MRB dispositions, and sign off FAI from a Blazor dashboard — and
the system behaves like a minimal MES/QMS slice backed by Lattice.

> See [`architecture.md`](./architecture.md) for the structural view
> (topology, component graph, grain interdependencies, Lattice trees,
> replication sequence) and [`approach.md`](./approach.md) for the
> implementation rationale and gotchas. Unfamiliar term? Check the
> [`glossary.md`](./glossary.md).

## Lattice capabilities demonstrated

| Capability | How it shows up |
|---|---|
| **Ordered fact log per entity** | Every domain event (`ProcessStepCompleted`, `InspectionRecorded`, `NonConformanceRaised`, `MRBDisposition`, `ReworkCompleted`, `FinalAcceptance`) is an immutable key in the `mfg-facts` tree, keyed `{serial}/{wallTicks:D20}/{counter:D10}/{factId}` so a forward range scan yields HLC-ascending history. |
| **HLC-ordered fold → convergent state** | `ComplianceFold.Fold` sorts facts by `(WallClockTicks, Counter, FactId)` before applying them, so concurrent producers across sites converge on the same `ComplianceState`. Contrasted live in the UI against a naïve arrival-order baseline running over the same fact stream. |
| **Divergence visible under chaos** | Two backends (`baseline`, `lattice`) receive the same facts via a fan-out router. Chaos-induced reorder causes the arrival-order baseline to drift; the HLC-ordered lattice fold does not. Divergent rows surface in the dashboard organically — no scripted saga. |
| **Secondary index as a second tree** | `mfg-site-activity-index` keys facts as `{site}/{wallTicks:D20}/{counter:D10}/{serial}` for reverse-chronological per-site activity feeds — a worked example of a secondary-index tree paired with a primary fact tree. |
| **Typed CRDT delta shipping** | `mfg-part-labels` is one OR-Set per serial, accessed through `lattice.OrSet(serial)` and replicated cross-cluster as `ReplicationMode.OrSet` — the package ships typed `add` / `remove` / `merge` deltas instead of raw byte writes. The companion `mfg-part-operator` tree is a per-serial LWW register kept cluster-local (LWW across clusters with disjoint HLCs is meaningless). |
| **Partition tolerance via shadow prefixes** | During a simulated intra-cluster partition, `PartCrdtStore` writes to a shadow key prefix; `PartitionHealHostedService` promotes shadows back onto the canonical keys on heal. |
| **Range scans as primitives** | The site-activity feed and the partition-heal sweep are plain half-open range scans over lex-ordered keys — no custom indexing layer. |
| **Cross-cluster replication via the shipped package** | `Orleans.Lattice.Replication` provides the WAL, shipper, applier, and dead-letter handling; `Orleans.Lattice.Replication.Grpc` provides the push transport. Each tree opts in by `ReplicationMode` (`LwwRegister` for `mfg-facts` and `mfg-site-activity-index`, `OrSet` for `mfg-part-labels`); see [`docs/lattice.replication/`](../../docs/lattice.replication/) for the wire format and bootstrap protocol. |
| **Receiver-side applier decoration** | `BaselineReplicationApplier` decorates the package's `IReplicationApplier` singleton; on every cross-cluster apply it mirrors `mfg-facts` writes into the local naive `BaselineFactBackend` and raises `FederationRouter.FactReplicated`, so the side-by-side divergence visualisation and the dashboard activity feed both update without polling. |
| **Durable operational state via Orleans grains** | Chaos configuration (`IProcessSiteGrain`, `IBackendChaosGrain`, `IPartitionChaosGrain`, `IReplicationDisconnectGrain`) persists to Azure Table Storage — restart the host and the system resumes exactly where it left off. The replication WAL and per-peer cursors are managed by `Orleans.Lattice.Replication` against the same storage account. |
| **Idempotent bulk-load on startup** | `InventorySeeder` emits 5 representative parts (one per reachable `ComplianceState`) through the same router operators use. A singleton `IInventorySeedStateGrain` gates the seed so re-running against the same storage account preserves inventory and operator mutations. |

## Fault-injection surface

The dashboard's chaos fly-out drives five tiers of fault injection,
each modelling a distinct real-world failure class:

| Tier | Models | Toggle |
|---:|---|---|
| 1 | Site unavailable / WAN latency | `IsPaused`, `DelayMs` on `IProcessSiteGrain` |
| 2 | Per-backend storage jitter, transient failure, write amplification | `IBackendChaosGrain` wrapping one backend |
| 3 | Cross-site out-of-order arrival after a pause lifts | `ReorderEnabled` on `IProcessSiteGrain` |
| 4 | Simulated intra-cluster silo partition | `IPartitionChaosGrain` + router hash filter |
| 4b | App-level cross-cluster replication pause | `IReplicationDisconnectGrain` |
| 5 | Genuine cross-cluster transport partition | `docker network disconnect` against the peer Traefik |

## Known limitation: cross-cluster receiver catch-up

In normal operation cross-cluster replication works exactly as designed.
Facts created on the US silo arrive on the EU silo over the gRPC push
transport within seconds, and OR-Set deltas on `mfg-part-labels`
reconcile across clusters as typed CRDT merges. The seeded inventory
visible on both dashboards on first boot is the live-incremental path
doing its job — nothing special has to happen for a fresh boot.

The known limitation only bites when one side's per-peer cursor lags
far enough behind the peer that the peer has GC'd write-ahead-log
entries the receiver still needs. That happens in two scenarios:

- **Asymmetric uptime.** One cluster runs long enough for TTL-driven
  WAL compaction to age out old entries, then the other cluster is
  brought up fresh, or its Azurite volume is wiped while the first
  keeps running.
- **Long-disconnected receiver.** A receiver stays disconnected long
  enough that the peer prunes ahead of where the receiver's saved
  cursor is.

In both cases the receiver detects the WAL gap, triggers
auto-bootstrap, and falls back to `ISnapshotProvider.ExportAsync`. The
underlying replication library ships only the seam there, not a
default cross-cluster implementation, so on a receiver the default
provider reads the local (empty) tree and copies nothing — the
receiver loops, never catching up.

When booting both clusters fresh together (the supported demo path),
this loop is never entered: neither side has compacted any WAL, both
shippers stream the live tail from the beginning, and replication
"just works". The Tier-4b and Tier-5 chaos toggles also stay safely
inside the WAL retention window for the demo's purposes — pause for a
minute, the WAL accumulates, replication catches up on resume.

The planned fix is tracked in
[`roadmap-cross-cluster-bootstrap.md`](../../src/lattice.replication/roadmap-cross-cluster-bootstrap.md).
When those items land, the auto-bootstrap path will copy the missing
state from the canonical sender so a long-disconnected or
freshly-wiped receiver catches up automatically.

## Running

The supported local topology is Docker Compose: two Azurite containers,
four silos (two per cluster), and a Traefik proxy per cluster — host
ports `5001` (US) and `5002` (EU).

```powershell
./run.ps1
```

See [`architecture.md`](./architecture.md) for the full network and
port layout and the Tier-5 partition commands.

### Observability

The compose topology also includes a single Prometheus + Grafana pair
giving cross-cluster visibility into both regions:

| Service | Host port | Purpose |
|---|---:|---|
| `prometheus` | — | Scrapes `silo-{us,eu}-{a,b}:8080/metrics` (multi-homed onto both cluster networks). |
| `grafana` | `3000` | Renders the three dashboards shipped by `Orleans.Lattice.Dashboards`. |

Open <http://localhost:3000> (anonymous Viewer access — admin/admin if
you want edit rights). Under *Dashboards → Orleans.Lattice* you'll find:

- **Orleans.Lattice — Overview** — throughput, leaf-write percentiles,
  cache hit-rate, splits, atomic-write outcomes.
- **Orleans.Lattice — Commit Path** — WAL-only per-step latency,
  activation replay duration.
- **Orleans.Lattice — Replication** — ship/apply/lag percentiles,
  dead-letter churn, per-peer entries/bytes behind.

> **Note** — every replicated tree in the sample ships through
> `Orleans.Lattice.Replication`'s gRPC push transport: `mfg-facts`
> and `mfg-site-activity-index` as `LwwRegister`, `mfg-part-labels`
> as `OrSet` (typed CRDT delta shipping). The only sample-specific
> seam remaining is `BaselineReplicationApplier`, a decorator on the
> package's `IReplicationApplier` that mirrors cross-cluster
> `mfg-facts` writes into the divergence-visualisation backend.
> See [`docs/lattice.replication/`](../../docs/lattice.replication/) for the wire format and bootstrap protocol.

The JSON for these dashboards is bind-mounted read-only from
`src/lattice.dashboards/Grafana/` — a CI test in the package keeps
them in sync with the live meter instruments, and the
`OpenTelemetry.Exporter.Prometheus.AspNetCore` exporter in
`src/MultiSiteManufacturing.Host/Program.cs` exposes the
`orleans.lattice` and `orleans.lattice.replication` meters at
`/metrics` on each silo.

## Project layout

```
samples/MultiSiteManufacturing/
├── README.md                         (this document — capabilities)
├── architecture.md                   (topology, components, grains, trees, replication)
├── approach.md                       (rationale, semantics, gotchas)
├── glossary.md                       (domain + technical terms)
├── run.ps1                           (docker compose wrapper)
├── src/
│   ├── MultiSiteManufacturing.Contracts/   (gRPC .proto surface)
│   └── MultiSiteManufacturing.Host/        (ASP.NET Core + Orleans + Blazor)
└── test/
    └── MultiSiteManufacturing.Tests/       (NUnit)
```

## Scope

The sample is deliberately narrow: one product family (HPT blade), a
five-state severity lattice, no authentication, no grpc-web, no
Kubernetes manifests, no CLI tool. It exists to exercise
`Orleans.Lattice` under realistic ordering and partition scenarios —
not to be a production MES.
