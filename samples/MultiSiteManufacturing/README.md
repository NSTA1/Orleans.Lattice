# Multi-Site Manufacturing - an Orleans.Lattice sample

A working thin slice of a regulated process-engineering traceability
system (turbine-blade lifecycle: forge → heat-treat → machining → NDT
→ MRB → FAI) that uses **`Orleans.Lattice`** as the fact store and
convergent state layer for an inventory system running across two
independent Orleans clusters.

It is not a scripted replay. Operators create parts, advance them
through process stages, record inspections, raise non-conformances,
issue MRB dispositions, and sign off FAI from a Blazor dashboard - and
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
| **Divergence visible under chaos** | Two backends (`baseline`, `lattice`) receive the same facts via a fan-out router. Chaos-induced reorder causes the arrival-order baseline to drift; the HLC-ordered lattice fold does not. Divergent rows surface in the dashboard organically - no scripted saga. |
| **Folded materialised view + read-time join** | The dashboard summary is not a sample-owned read model. A library-maintained folded view (`mfg-compliance`, registered via `AddLatticeViews`/`AddFoldedView`) folds each part's `mfg-facts` in business-HLC order into an accumulator carrying the lattice compliance state, latest process stage, and fact count. The snapshot scans that view and joins each part's arrival-order `BaselineState` from the baseline backend at read time - the divergence between the two independently-maintained halves cannot be reproduced by any fold over `mfg-facts`, so it is joined per part rather than materialised. The library keeps the folded half current directly off the write-ahead log, so no application-side summary tree remains. See [`docs/lattice/materialised-views.md`](../../docs/lattice/materialised-views.md). |
| **Tag-index secondary view** | `mfg-site-activity` keys facts part-major as `{serial}/{site}` and the built-in `Orleans.Lattice` tag index (opened through the injected `ILatticeTagIndexFactory`, membership tree `tag-mfg-site`) tags each key with its site. `ListAtSiteAsync` answers "parts at site X" via `WithAnyTags(site)` - the site is deliberately *not* a key prefix, so the tag index is the genuine access path. A worked example of the built-in tag index replacing a hand-rolled secondary-index tree. |
| **Typed CRDT delta shipping** | `mfg-part-labels` is one OR-Set per serial, accessed through `lattice.OrSet(serial)` and replicated cross-cluster as `ReplicationMode.OrSet` - the package ships typed `add` / `remove` / `merge` deltas instead of raw byte writes. The companion `mfg-part-operator` tree is a per-serial LWW register kept cluster-local - see *Per-tree replication policy* below for the rationale. |
| **Partition tolerance via shadow prefixes** | During a simulated intra-cluster partition, `PartCrdtStore` writes to a shadow key prefix; `PartitionHealHostedService` promotes shadows back onto the canonical keys on heal. |
| **Range scans as primitives** | The per-part fact-history fold and the partition-heal sweep are plain half-open range scans over lex-ordered keys - no custom indexing layer. The per-site view instead uses the built-in tag index (see above). |
| **Cross-cluster replication via the shipped package** | `Orleans.Lattice.Replication` provides the WAL, shipper, applier, and dead-letter handling; `Orleans.Lattice.Replication.Grpc` provides the push transport. Each tree opts in by `ReplicationMode` (`LwwRegister` for `mfg-facts` and `mfg-site-activity`; `OrFlag` for its `tag-mfg-site` membership tree; `OrSet` for `mfg-part-labels`); see [`docs/lattice.replication/`](../../docs/lattice.replication/) for the wire format and bootstrap protocol. |
| **Receiver-side applier decoration** | `BaselineReplicationApplier` decorates the package's `IReplicationApplier` singleton; on every cross-cluster apply it mirrors `mfg-facts` writes into the local naive `BaselineFactBackend` and raises `FederationRouter.FactReplicated`, so the side-by-side divergence visualisation and the dashboard activity feed both update without polling. |
| **Durable operational state via Orleans grains** | Chaos configuration (`IProcessSiteGrain`, `IBackendChaosGrain`, `IPartitionChaosGrain`, `IReplicationDisconnectGrain`) persists to Azure Table Storage - restart the host and the system resumes exactly where it left off. The lattice tree write-ahead log persists to the same storage account via `Orleans.Lattice.Storage.AzureTable` (Azurite locally), so tree state survives silo restarts. The replication WAL and per-peer cursors are managed by `Orleans.Lattice.Replication` against the same storage account. |
| **Idempotent bulk-load on startup** | `InventorySeeder` emits 5 representative parts (one per reachable `ComplianceState`) through the same router operators use. A singleton `IInventorySeedStateGrain` gates the seed so re-running against the same storage account preserves inventory and operator mutations. |

## Per-tree replication policy

Each Lattice tree opts into the replication mode that matches the
convergence semantics of the data it stores. The choice is per-tree,
not global, because not every CRDT has meaningful cross-cluster
behaviour - and demonstrating that explicitly is part of why the
sample exists.

| Tree | Replication mode | Rationale |
|---|---|---|
| `mfg-facts` | `LwwRegister` | Each fact key is `{serial}/{wallTicks:D20}/{counter:D10}/{factId}` - globally unique, so LWW per key never collides. |
| `mfg-site-activity` | `LwwRegister` | Part-major activity rows keyed `{serial}/{site}`; the newest fact for a part at a site wins per key, so LWW converges. |
| `tag-mfg-site` | `OrFlag` | Tag-index membership rows for the per-site view. Under active-active replication both clusters tag keys, so the index authors flag-CRDT (enable-wins) membership dots that converge without a single-writer assumption; an LWW membership tree would silently drop a posting written concurrently in the other cluster. |
| `mfg-part-labels` | `OrSet` | Process labels are an additive set; typed OR-Set deltas (`add` / `remove` / `merge`) reconcile concurrent writes from any silo or cluster without conflict. |
| `mfg-part-operator` | *not replicated (cluster-local by design)* | A bare LWW register over a key both clusters would write to - and HLCs from disjoint cluster ID spaces have no meaningful global order, so "last write wins" between US and EU is semantically arbitrary. The sample keeps the register cluster-local; a production system that genuinely needs cross-cluster operator handover would model it as an OR-Set of `(replica, operator)` tuples or as an explicit acquire/release token, and the part-detail UI labels the assign button **"Assign (local-only)"** so the choice is visible to the operator. |

This is a *design choice*, not a limitation: opting
`mfg-part-operator` out of replication is the right answer for a bare
LWW register across disjoint HLC namespaces. Within a single cluster
the register still converges across silos via the lattice's internal
HLC, which is what the per-tree replication mode is selecting against.

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
four silos (two per cluster), and a Traefik proxy per cluster - host
ports `5001` (US) and `5002` (EU).

```powershell
./run.ps1
```

Once the stack is up, open the two dashboards side-by-side and the
shared observability pane:

| URL | What it is |
|---|---|
| <http://localhost:5001> | US-cluster Blazor dashboard (sticky-cookie pinned to `silo-us-a` or `silo-us-b`). |
| <http://localhost:5002> | EU-cluster Blazor dashboard (sticky-cookie pinned to `silo-eu-a` or `silo-eu-b`). |
| <http://localhost:3000> | Grafana - anonymous Viewer access, or `admin`/`admin` for edit rights. The three Lattice dashboards live under *Dashboards → Orleans.Lattice* (see [Observability](#observability) below). |

See [`architecture.md`](./architecture.md) for the full network and
port layout and the Tier-5 partition commands.

### Observability

The compose topology also includes a single Prometheus + Grafana pair
giving cross-cluster visibility into both regions:

| Service | Host port | Purpose |
|---|---:|---|
| `prometheus` | - | Scrapes `silo-{us,eu}-{a,b}:8080/metrics` (multi-homed onto both cluster networks). |
| `grafana` | `3000` | Renders the three dashboards shipped by `Orleans.Lattice.Dashboards`. |

Open <http://localhost:3000> (anonymous Viewer access - admin/admin if
you want edit rights). Under *Dashboards → Orleans.Lattice* you'll find:

- **Orleans.Lattice - Overview** - throughput, leaf-write percentiles,
  cache hit-rate, splits, atomic-write outcomes.
- **Orleans.Lattice - Commit Path** - WAL-only per-step latency,
  activation replay duration.
- **Orleans.Lattice - Replication** - ship/apply/lag percentiles,
  dead-letter churn, per-peer entries/bytes behind.

> **Note** - every replicated tree in the sample ships through
> `Orleans.Lattice.Replication`'s gRPC push transport: `mfg-facts` and
> `mfg-site-activity` as `LwwRegister`, its `tag-mfg-site` membership
> tree as `OrFlag` (enable-wins flag-CRDT membership), `mfg-part-labels`
> as `OrSet` (typed CRDT delta
> shipping). The only sample-specific
> seam remaining is `BaselineReplicationApplier`, a decorator on the
> package's `IReplicationApplier` that mirrors cross-cluster
> `mfg-facts` writes into the divergence-visualisation backend.
> See [`docs/lattice.replication/`](../../docs/lattice.replication/) for the wire format and bootstrap protocol.

The JSON for these dashboards is bind-mounted read-only from
`src/lattice.dashboards/Grafana/` - a CI test in the package keeps
them in sync with the live meter instruments, and the
`OpenTelemetry.Exporter.Prometheus.AspNetCore` exporter in
`src/MultiSiteManufacturing.Host/Program.cs` exposes the
`orleans.lattice` and `orleans.lattice.replication` meters at
`/metrics` on each silo.

## Exploring the cluster with Orleans.Lattice.Explorer

Each silo co-hosts the read-only **Orleans.Lattice.Api.State** gRPC surface on
its dedicated `:8081` h2c listener - the same one the cross-cluster replication
service uses. Each cluster's Traefik exposes it through the existing published
endpoint (`5001` US, `5002` EU) via a non-sticky, round-robin,
`PathPrefix(`/orleans.lattice.api.state/`)` router with an active health check,
so the
[Orleans.Lattice.Explorer](../../src/lattice.explorer) can browse the running
cluster's trees, views, metrics, topology, and data with no new host ports. The
health check probes each silo's `:8080` HTTP port and evicts a stopped silo
within ~2s, so the explorer transparently fails over to the surviving silo
instead of flickering. The sticky Blazor `/` router that pins each browser tab's
SignalR circuit is untouched; the state-API router just has a higher-priority,
more-specific prefix.

`run-explorer.ps1` launches the explorer pointed at a cluster. It seeds the
endpoint (and, optionally, a sign-in credential) through the explorer's
launcher-friendly environment bootstrap, so nothing in your per-user explorer
config is hand-edited.

### Anonymous (default)

```powershell
./run.ps1                 # state-API authorization is OFF by default
./run-explorer.ps1        # Blazor web explorer -> US cluster (http://localhost:5001)
./run-explorer.ps1 -Cluster eu
```

The explorer connects anonymously over loopback h2c (insecure-loopback-dev
transport mode). Open the printed `http://localhost:5290` once the web head
starts.

### With state-API authentication

Supply a username and password to `run.ps1`. It generates the salted PBKDF2 hash
with `tools/New-LatticeStateCredential.ps1` and delivers it to every silo
container as `LATTICE_STATE_USER_<username>` through a git-ignored `.env` file;
the plaintext password never reaches a container env, a command line, or the
compose file. The host then enables `RequireAuthorization = true` with the
reference `EnvVarCredentialAuthorizer`, so an anonymous explorer is rejected and
a signed-in one succeeds.

```powershell
./run.ps1 -Username alice -Password 'Sup3rSecret'
./run-explorer.ps1 -Username alice -Password 'Sup3rSecret'
```

### Windows desktop explorer

```powershell
./run-explorer.ps1 -Client windows
./run-explorer.ps1 -Client windows -Username alice -Password 'Sup3rSecret'
```

`./run.ps1 -Down` and `./run.ps1 -Clean` delete the generated `.env`.

### Inspecting change history

The sample enables a durable **change-history** view (with full-value retention) over
two CRDT trees on startup and then seeds a multi-revision timeline into them, so the
Explorer's **History** tab has something non-trivial - and durable - to show out of
the box (see [`docs/lattice/change-history.md`](../../docs/lattice/change-history.md)):

- `mfg-part-operator` (last-writer-wins register) gets a sequence of operator
  handoffs on one part's key, so the History tab renders successive values plus diffs.
- `mfg-part-labels` (process-label OR-Set) gets interleaved label adds and removes on
  the same part's key, so the History tab renders element-level member changes.

Both are seeded for part `HPT-BLD-S1-2028-00002`. To see it:

1. Start the cluster and explorer: `./run.ps1` then `./run-explorer.ps1`.
2. In the explorer, open tree `mfg-part-operator` (or `mfg-part-labels`), select key
   `HPT-BLD-S1-2028-00002`, and open the **History** tab.
3. Toggle live-follow, then add or remove a label on that part's detail page in the
   sample UI and watch the new revision appear at the top of the timeline.

The durable view is enabled by a small startup activator (`HistoryShowcaseActivator`)
that sets a value-retaining retention mode and creates a history view on each tree;
the change-history doc explains the retention modes and the truncation caveats.

## Project layout

```
samples/MultiSiteManufacturing/
├── README.md                         (this document - capabilities)
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
`Orleans.Lattice` under realistic ordering and partition scenarios -
not to be a production MES.
