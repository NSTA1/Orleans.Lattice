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
| **Mixed CRDT semantics in one tree** | `mfg-part-crdt` hosts both a G-Set (`{serial}/labels/{label}`) and an LWW register (`{serial}/operator`) under per-key filters, showing how one tree can mix commutative and last-writer-wins keys. |
| **Partition tolerance via shadow prefixes** | During a simulated intra-cluster partition, `PartCrdtStore` writes to a shadow key prefix; `PartitionHealHostedService` promotes shadows back onto the canonical keys on heal. |
| **Range scans as primitives** | The replog, the site-activity feed, and the partition-heal sweep are all plain half-open range scans over lex-ordered keys — no custom indexing layer. |
| **Cross-cluster replication built on Lattice** | An HLC-ordered replog (`_replog__{tree}`) is itself a Lattice tree. An `IReplicatorGrain` ships batches per `(tree, peer)` over HTTP; an `IReplogJanitorGrain` compacts entries behind the slowest peer's cursor. Zero library changes. |
| **Loop-break on replicated apply** | Inbound replay sets a `RequestContext` flag that the outgoing filter checks, so the same `SetAsync` / `DeleteAsync` code path serves both operator writes and replicated applies without re-shipping them. |
| **Durable operational state via Orleans grains** | Chaos configuration (`IProcessSiteGrain`, `IBackendChaosGrain`, `IPartitionChaosGrain`, `IReplicationDisconnectGrain`) and replicator cursors (`IReplicatorGrain`) persist to Azure Table Storage — restart the host and the system resumes exactly where it left off. |
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

## Running

The supported local topology is Docker Compose: two Azurite containers,
four silos (two per cluster), and a Traefik proxy per cluster — host
ports `5001` (US) and `5002` (EU).

```powershell
./run.ps1
```

See [`architecture.md`](./architecture.md) for the full network and
port layout and the Tier-5 partition commands.

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
