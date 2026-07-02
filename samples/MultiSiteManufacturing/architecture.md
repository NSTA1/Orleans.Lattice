# MultiSiteManufacturing - architecture

Structural view of the sample: physical and network topology, how a
single silo is wired internally, how Orleans grains depend on each
other, what Lattice trees exist and what they hold, and how
cross-cluster replication flows end-to-end.

For rationale, semantics, and implementation gotchas see
[`approach.md`](./approach.md). For a capability overview see
[`README.md`](./README.md).

---

## 1. Physical and network topology

Docker Compose runs two Azurite containers, four silos (two per
cluster), and a Traefik proxy per cluster. The only cross-cluster
link is the peer Traefik, multi-homed onto both cluster networks.

```mermaid
flowchart TB
    subgraph host["Developer host"]
        browser1["Browser - US UI<br/>localhost:5001"]
        browser2["Browser - EU UI<br/>localhost:5002"]
    end

    subgraph usNet["us-net"]
        azF["azurite-us<br/>:10000/10001/10002"]
        siloFA["silo-us-a<br/>HTTP :8080 · Silo :11111 · GW :30000"]
        siloFB["silo-us-b<br/>HTTP :8080 · Silo :11111 · GW :30000"]
        tFE["traefik-us<br/>:80 (multi-homed)"]
        tHE1["traefik-eu<br/>(multi-homed)"]
    end

    subgraph euNet["eu-net"]
        azH["azurite-eu<br/>:10000/10001/10002"]
        siloHA["silo-eu-a<br/>HTTP :8080 · Silo :11111 · GW :30000"]
        siloHB["silo-eu-b<br/>HTTP :8080 · Silo :11111 · GW :30000"]
        tHE2["traefik-eu<br/>:80 (multi-homed)"]
        tFE2["traefik-us<br/>(multi-homed)"]
    end

    browser1 -->|"host :5001"| tFE
    browser2 -->|"host :5002"| tHE2

    tFE -->|"web · sticky"| siloFA
    tFE -->|"web · sticky"| siloFB
    tHE2 -->|"web · sticky"| siloHA
    tHE2 -->|"web · sticky"| siloHB

    siloFA --- azF
    siloFB --- azF
    siloHA --- azH
    siloHB --- azH

    siloFA -.->|"gRPC push (LatticeReplication)"| tHE1
    siloFB -.->|"gRPC push (LatticeReplication)"| tHE1
    siloHA -.->|"gRPC push (LatticeReplication)"| tFE2
    siloHB -.->|"gRPC push (LatticeReplication)"| tFE2

    tHE1 ===|"round-robin · /orleans.lattice.replication.*"| siloHA
    tHE1 ===|"round-robin · /orleans.lattice.replication.*"| siloHB
    tFE2 ===|"round-robin · /orleans.lattice.replication.*"| siloFA
    tFE2 ===|"round-robin · /orleans.lattice.replication.*"| siloFB
```

Reachability matrix:

| From → To | Path | Reachable? |
|---|---|---|
| `silo-us-a` → `silo-us-b` | `us-net` | Yes (same cluster) |
| `silo-us-*` → `traefik-eu` | `us-net` (Traefik multi-homed) | Yes |
| `silo-us-*` → `silo-eu-*` | - | **No shared network - blocked** |
| `silo-eu-*` → `silo-us-*` | - | **No shared network - blocked** |
| `azurite-us` ↔ `azurite-eu` | - | **No shared network - blocked** |

Three host ports are published:

| Host port | Container | Role |
|---|---|---|
| 5001 | `traefik-us:80` | US UI (sticky) + replication gRPC inbound (round-robin). Open <http://localhost:5001>. |
| 5002 | `traefik-eu:80` | EU UI (sticky) + replication gRPC inbound (round-robin). Open <http://localhost:5002>. |
| 3000 | `grafana:3000` | Cross-cluster Grafana - Prometheus-backed, multi-homed onto both cluster networks. Open <http://localhost:3000> (anonymous Viewer; `admin`/`admin` for edit). |

Silo HTTP (`:8080`), Orleans silo (`:11111`), gateway (`:30000`),
and Prometheus (`:9090`) ports are internal-only.

Each Traefik runs three routers over the same backend pool:

| Router | Rule | LB |
|---|---|---|
| `{cluster}-replicate` | `PathPrefix(/orleans.lattice.replication.)`, priority 200 | round-robin, no health check |
| `{cluster}-state` | `PathPrefix(/orleans.lattice.api.state/)`, priority 200 | round-robin + active health check (probes `:8080`) |
| `{cluster}-web` | `PathPrefix(/)` | sticky cookie `msmfg_{cluster}_affinity` |

### Tier-5 partition commands

Disconnecting the peer Traefik from the local cluster network removes
the only route from local silos to the peer cluster.

```powershell
# Sever US ↔ EU:
docker network disconnect msmfg_us-net msmfg-traefik-eu
docker network disconnect msmfg_eu-net msmfg-traefik-us
# ... demonstrate divergence ...
docker network connect    msmfg_us-net msmfg-traefik-eu
docker network connect    msmfg_eu-net msmfg-traefik-us
```

---

## 2. In-silo component graph

Each silo is a single ASP.NET Core process hosting Blazor Server,
gRPC, Orleans, and the package's replication WAL + gRPC push
transport. Both UI and gRPC call paths share the same
`FederationRouter` and backend instances via DI.

```mermaid
flowchart LR
    subgraph browser["Browser"]
        ui["Blazor UI<br/>(SignalR circuit)"]
    end

    subgraph silo["ASP.NET Core silo process"]
        direction LR
        razor["Razor components"]
        grpc["gRPC services<br/>Inventory · FactIngress<br/>SiteControl · Compliance"]

        subgraph app["Application layer"]
            router["FederationRouter"]
            chaosBase["ChaosFactBackend<br/>(baseline)"]
            chaosLat["ChaosFactBackend<br/>(lattice)"]
            baseBE["Baseline backend<br/>(arrival-order grains)"]
            latBE["Lattice backend<br/>(HLC-ordered fold)"]
            broadcaster["DashboardBroadcaster<br/>(Channel&lt;T&gt; · cluster stream)"]
        end

        subgraph orleans["Orleans grains"]
            siteG["IProcessSiteGrain × 7"]
            backG["IBackendChaosGrain × 2"]
            partG["IPartitionChaosGrain"]
            replDisc["IReplicationDisconnectGrain"]
            seedG["IInventorySeedStateGrain"]
        end

        subgraph lattice["Orleans.Lattice"]
            facts["mfg-facts"]
            siteIdx["mfg-site-activity<br/>+ tag-mfg-site (tag index)"]
            labels["mfg-part-labels (OrSet)"]
            opReg["mfg-part-operator (LWW)"]
        end

        subgraph repl["Orleans.Lattice.Replication<br/>(WAL + gRPC push)"]
            wal["WAL · per replicated tree"]
            ship["Shipper grain · per (tree × peer)"]
            apply["Applier · receiver-side"]
            grpc2["gRPC service /<br/>push transport"]
        end

        mirror["BaselineReplicationApplier<br/>(IReplicationApplier decorator)"]
        dashStream[/"Azure Storage Queue stream<br/>DashboardStreams · msmfg.dashboard.facts<br/>queue msmfgdashboard-0<br/>(durable cluster-wide fan-out)"/]
    end

    tables[("Azure Table Storage<br/>msmfgGrainState<br/>msmfgLatticeFacts")]

    ui <--> razor
    razor --> router
    grpc --> router
    router --> siteG
    router --> partG
    router --> chaosBase
    router --> chaosLat
    chaosBase --> baseBE
    chaosLat --> latBE
    latBE --> facts
    latBE --> siteIdx
    latBE --> labels
    latBE --> opReg
    router -.->|"FactRouted · FactReplicated · ChaosConfigChanged"| broadcaster
    broadcaster -.->|"publish Fact"| dashStream
    dashStream -.->|"subscribe → fan out to circuits"| broadcaster
    broadcaster -.-> razor

    facts -.->|"WAL append"| wal
    siteIdx -.->|"WAL append"| wal
    labels -.->|"OrSet delta"| wal
    wal --> ship
    ship --> grpc2
    grpc2 -->|"push to peer"| apply
    apply --> facts
    apply --> siteIdx
    apply --> labels
    apply -.->|"decorated by"| mirror
    mirror --> baseBE
    mirror -.->|"FactReplicated"| broadcaster

    orleans --- tables
    lattice --- tables
    repl --- tables
```

---

## 3. Grain interdependencies

Who calls whom inside a single silo. Solid arrows are direct method
calls; dashed arrows are event channels consumed by UI subscribers.

```mermaid
flowchart TB
    router["FederationRouter"]
    siteReg["ISiteRegistryGrain<br/>(singleton)"]
    siteG["IProcessSiteGrain<br/>(per site × 7)"]
    backG["IBackendChaosGrain<br/>(per backend × 2)"]
    partG["IPartitionChaosGrain<br/>(singleton)"]
    replDisc["IReplicationDisconnectGrain<br/>(singleton)"]
    seedG["IInventorySeedStateGrain<br/>(singleton)"]
    seeder["InventorySeeder<br/>(IHostedService)"]
    mirror["BaselineReplicationApplier<br/>(IReplicationApplier decorator)"]
    broadcaster["DashboardBroadcaster"]
    healSvc["PartitionHealHostedService"]
    crdtStore["PartCrdtStore"]

    router -->|"AdmitAsync"| siteG
    router -->|"IsPartitioned"| partG
    router -->|"GetConfig"| backG
    router -.->|"FactRouted · FactReplicated · ChaosConfigChanged"| broadcaster

    siteReg -->|"WatchSites · preset fan-out"| siteG
    siteReg -.->|"SiteStateChanged"| broadcaster

    seeder -->|"HasSeeded?"| seedG
    seeder -->|"snapshot / zero / restore"| siteReg
    seeder -->|"emit seed facts"| router

    mirror -.->|"FactReplicated"| broadcaster

    healSvc -->|"IsPartitioned?"| partG
    healSvc -->|"promote shadows"| crdtStore
```

Key invariants:

- `FederationRouter` only **reads** chaos grains; it never writes
  them. Writes come from the UI / gRPC control surface via
  `ISiteRegistryGrain` and direct grain calls.
- Cross-cluster replication is opaque to the application: the
  package's WAL + shipper + applier sit below the lattice, and the
  sample observes the receiver-side stream by decorating the
  package's `IReplicationApplier` with `BaselineReplicationApplier`,
  so each cross-cluster apply also mirrors `mfg-facts` writes into
  `BaselineFactBackend` and raises `FactReplicated`. WAL compaction
  is a package concern; the sample does not configure it.
- `PartitionHealHostedService` only runs shadow promotion when
  `IPartitionChaosGrain.IsPartitioned` has flipped back to `false`.

---

## 4. Lattice trees

All five trees persist to `msmfgLatticeFacts` in Azure Table Storage.
Orleans grain state (chaos toggles, seed flag, baseline part grains,
inventory) persists to `msmfgGrainState`. The replication WAL is a
package-managed table separate from the lattice trees themselves.

```mermaid
flowchart LR
    subgraph primary["Primary"]
        facts["mfg-facts<br/>{serial}/{wallTicks:D20}/{counter:D10}/{factId}<br/>→ fact bytes"]
    end

    subgraph derived["Derived / sibling"]
        siteIdx["mfg-site-activity<br/>{serial}/{site}<br/>→ HLC + activity label"]
        siteTag["tag-mfg-site (tag index)<br/>site → {serial}/{site} keys"]
        labels["mfg-part-labels<br/>{serial} → OrSet&lt;label&gt;<br/>(typed CRDT - replicated)"]
        opReg["mfg-part-operator<br/>{serial} → operator id (LWW)<br/>(cluster-local)"]
    end

    facts -->|"value + site tag written together"| siteIdx
    siteIdx -->|"site tag posting"| siteTag
    facts -->|"labels written on raise / disposition"| labels
    facts -->|"operator stamped on each fact"| opReg
```

| Tree | Key shape | Role | Replicated |
|---|---|---|---|
| `mfg-facts` | `{serial}/{wallTicks:D20}/{counter:D10}/{factId}` | Immutable per-part fact log. Forward range scan = HLC-ascending history. | Yes |
| `mfg-site-activity` | `{serial}/{site}` → HLC + activity label | Part-major activity rows; the per-site view reads them through the tag index. | Yes |
| `tag-mfg-site` | tag-index membership (`tag \0 treeId \0 key`) | Posting list mapping each `ProcessSite` to its `{serial}/{site}` keys; powers `ListAtSiteAsync` via `WithAnyTags(site)`. | Yes |
| `mfg-part-labels` | `{serial}` (one OrSet per serial) | Per-part label set (`damaged`, `awaiting-mrb`, `awaiting-rework`, `accepted`, `scrapped`, …). | Yes - `ReplicationMode.OrSet` (typed CRDT delta shipping) |
| `mfg-part-operator` | `{serial}` (one LWW register per serial) | Per-part current operator id. | No (cluster-local) - LWW across clusters with disjoint HLCs is meaningless |

The dashboard's per-part summary is no longer a sample-owned tree. It is the
library-maintained folded view `mfg-compliance` over `mfg-facts` (registered via
`AddLatticeViews`/`AddFoldedView`), joined at read time with each part's baseline
compliance state. See the access patterns below.

Access patterns:

- Per-part history → forward range scan of `mfg-facts` with prefix
  `{serial}/`.
- Per-site recent activity → tag-index union query
  (`WithAnyTags(site)`) over the `tag-mfg-site` index, reading each
  matched `{serial}/{site}` key's value from `mfg-site-activity`.
  The site is deliberately the key *suffix*, so a range scan cannot
  answer this query - the tag index is the access path.

- All-parts dashboard snapshot → scan of the library-maintained folded view
  `mfg-compliance` (the fact-derived half: lattice compliance state, latest
  process stage, fact count), joined per part with the baseline compliance
  state from the baseline backend. The library maintainer keeps the folded
  half current directly off the `mfg-facts` write-ahead log, so a dashboard
  load reads one pre-folded accumulator per part instead of re-folding every
  part's `mfg-facts` prefix. `BaselineState` is folded in arrival order by the
  baseline backend, deliberately diverging from the HLC-ordered lattice fold
  (the red-row highlight and divergence stream are the demo), so it cannot be
  reproduced by any fold over `mfg-facts` and is joined per part at read time
  rather than materialised. While a dashboard is being watched, a bounded
  background pass also reconciles the fanned-out set against the `mfg-facts`
  tree, so parts written directly - bypassing `FederationRouter`, which raises
  no fact-stream event - are still fanned out live to the attached dashboard
  within a few cadences.

Cross-cluster shipping and WAL compaction are package concerns - see
[`docs/lattice.replication/`](../../docs/lattice.replication/) for
the WAL key shape, the gRPC push protocol, and the receiver-side
apply pipeline.

---

## 5. Cross-cluster replication flow

Cross-cluster replication is provided by
`Orleans.Lattice.Replication` (WAL + shipper + applier) wired with
the `Orleans.Lattice.Replication.Grpc` push transport. From the
sample's perspective the flow is opaque: a write on the US cluster
lands in the local lattice, the package's WAL captures it, the
shipper streams it to the EU cluster's gRPC service, and the EU
applier merges it back into the local lattice using the appropriate
CRDT semantics (LWW for `mfg-facts` and `mfg-site-activity`, OrFlag
enable-wins membership for its `tag-mfg-site` membership tree,
typed OrSet deltas for `mfg-part-labels`).

```mermaid
sequenceDiagram
    autonumber
    participant UI as Blazor UI (us)
    participant Router as FederationRouter
    participant Lat as Lattice backend
    participant Tree as mfg-facts (us)
    participant WAL as Replication WAL (us)
    participant Ship as Shipper (us)
    participant Traefik as traefik-eu
    participant Apply as Applier (eu)
    participant PeerTree as mfg-facts (eu)
    participant Mirror as BaselineReplicationApplier (eu)
    participant PeerBase as Baseline backend (eu)

    UI->>Router: EmitFact(env)
    Router->>Lat: AppendAsync(env)
    Lat->>Tree: SetAsync(key, bytes)
    Tree-->>WAL: append (origin=us, hlc, payload)

    loop package shipping cadence
        Ship->>WAL: drain [Cursor+, end]
        Ship->>Traefik: gRPC push (LatticeReplication.Push)
        Traefik->>Apply: round-robin to silo-eu-{a|b}
        Apply->>PeerTree: merge entry per CRDT mode
        Apply-->>Ship: ack (peer cursor advanced)
    end

    Note over Apply,Mirror: IReplicationApplier decorator (eu side)
    Apply->>Mirror: ApplyAsync(entry)
    Mirror->>PeerBase: decode + EmitAsync (mfg-facts only)
    Mirror-->>Apply: forward to inner applier
    Note over Mirror: DashboardBroadcaster pushes<br/>PartSummaryUpdate to Blazor subs
```

Failure modes and their recovery:

| Scenario | Effect | Recovery |
|---|---|---|
| Peer unreachable | Push transport's RPC fails | Package-internal exponential backoff; shipper retries from the same cursor. |
| Silo-B of peer restarts | Traefik health check evicts it within ~2 s | Next push lands on silo-A; transparent to the shipper. |
| Duplicate delivery | Same entry merged twice | CRDT-idempotent: LWW collapses to identity, OrSet add/remove dots are deduped by replica id, write-once `mfg-facts` keys are stable. |
| A → B → A cycle | Receiver re-emits a remote-origin entry | Broken by the package's per-origin high-water-mark - replicated applies are short-circuited before they hit the WAL again. |
| Replication-disconnect preset | `IReplicationDisconnectGrain.IsDisconnected = true` | `ChaosReplicationTransport` decorates the package's `IReplicationTransport` and returns `Accepted=false` while the flag is set; the package shipper holds its per-peer cursor steady, the WAL grows locally, and on clear the WAL drains in HLC order. |
| Tier-5 `docker network disconnect` | gRPC push fails at transport | Identical to "peer unreachable"; shipper backs off and catches up on reconnect. |
| Baseline applier decode fails | Single entry skipped on peer's baseline; lattice apply still succeeds | Logged; subsequent entries continue to apply. Baseline is a demo-visualisation backend, not a correctness-critical store. |
| Receiver fallen out of WAL retention window | Receiver's per-peer cursor is older than the sender's oldest WAL entry | Auto-bootstrap drains a point-in-time snapshot from the sender cluster over the gRPC remote-snapshot transport (`IRemoteSnapshotTransport` / `RemoteSnapshotProvider`); the receiver catches up automatically. See [`docs/lattice.replication/snapshot-bootstrap.md`](../../docs/lattice.replication/snapshot-bootstrap.md). |

See [`docs/lattice.replication/`](../../docs/lattice.replication/)
for the gRPC wire format, bootstrap protocol, and dead-letter
handling.

---

## 6. Configuration overlay

`appsettings.cluster.{name}.json` ships the localhost defaults.
Compose overrides only what has to change in containers:

| Key | Purpose |
|---|---|
| `ConnectionStrings__AzureTableStorage` | Per-cluster Azurite URL (`http://azurite-{cluster}:10002/...`). |
| `PackageReplication__PeerClusterId` | Peer cluster short name (used as the WAL origin tag). |
| `PackageReplication__PeerGrpcEndpoint` | Peer Traefik URL for the gRPC push transport. |
| `Cluster__SiloPortA` / `SiloPortB` | Both `11111` under Compose - each container has its own IP. |
| `ASPNETCORE_URLS` | `http://+:8080` in Compose; `Program.cs` skips its own `UseUrls` when this is set. |
| `Seeder__Enabled` | Explicit boolean - `true` on `silo-us-a`, `false` elsewhere. |

The package's gRPC push transport accepts a single peer endpoint per
peer. Multi-zone failover is delegated to the load balancer in front
of each peer cluster; in this Compose topology Traefik fills that
role.
