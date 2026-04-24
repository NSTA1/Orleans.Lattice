# MultiSiteManufacturing — architecture

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
        browser1["Browser — US UI<br/>http://localhost:5001"]
        browser2["Browser — EU UI<br/>http://localhost:5002"]
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

    siloFA -.->|"POST /replicate/{tree}"| tHE1
    siloFB -.->|"POST /replicate/{tree}"| tHE1
    siloHA -.->|"POST /replicate/{tree}"| tFE2
    siloHB -.->|"POST /replicate/{tree}"| tFE2

    tHE1 ===|"round-robin · /replicate"| siloHA
    tHE1 ===|"round-robin · /replicate"| siloHB
    tFE2 ===|"round-robin · /replicate"| siloFA
    tFE2 ===|"round-robin · /replicate"| siloFB
```

Reachability matrix:

| From → To | Path | Reachable? |
|---|---|---|
| `silo-us-a` → `silo-us-b` | `us-net` | Yes (same cluster) |
| `silo-us-*` → `traefik-eu` | `us-net` (Traefik multi-homed) | Yes |
| `silo-us-*` → `silo-eu-*` | — | **No shared network — blocked** |
| `silo-eu-*` → `silo-us-*` | — | **No shared network — blocked** |
| `azurite-us` ↔ `azurite-eu` | — | **No shared network — blocked** |

Only two host ports are published:

| Host port | Container | Role |
|---|---|---|
| 5001 | `traefik-us:80` | US UI (sticky) + replication inbound (round-robin) |
| 5002 | `traefik-eu:80` | EU UI (sticky) + replication inbound (round-robin) |

Silo HTTP (`:8080`), Orleans silo (`:11111`), and gateway (`:30000`)
ports are internal-only.

Each Traefik runs two routers over the same backend pool:

| Router | Rule | LB |
|---|---|---|
| `{cluster}-replicate` | `PathPrefix(/replicate)`, priority 100 | round-robin + active health check |
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
gRPC, Orleans, and the replication outbound/inbound endpoints. Both
UI and gRPC call paths share the same `FederationRouter` and backend
instances via DI.

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
            broadcaster["DashboardBroadcaster<br/>(Channel&lt;T&gt;)"]
        end

        subgraph orleans["Orleans grains"]
            siteG["IProcessSiteGrain × 7"]
            backG["IBackendChaosGrain × 2"]
            partG["IPartitionChaosGrain"]
            replG["IReplicatorGrain × (tree × peer)"]
            janG["IReplogJanitorGrain"]
            seedG["IInventorySeedStateGrain"]
        end

        subgraph lattice["Orleans.Lattice"]
            facts["mfg-facts"]
            siteIdx["mfg-site-activity-index"]
            crdt["mfg-part-crdt"]
            replog["_replog__{tree}"]
        end

        filter["LatticeReplicationFilter<br/>(IOutgoingGrainCallFilter)"]
        repClient["ReplicationHttpClient"]
        inbound["POST /replicate/{tree}<br/>(minimal API)"]
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
    latBE --> crdt
    router -.->|"events"| broadcaster
    broadcaster -.-> razor

    latBE -->|"SetAsync / DeleteAsync"| filter
    filter --> replog
    replG -->|"scan Cursor+ → end"| replog
    replG -->|"read current value"| facts
    replG --> repClient
    janG -->|"prune ≤ min(cursor) − 24h"| replog

    inbound --> latBE
    inbound -->|"decode + replay (mfg-facts only)"| baseBE

    orleans --- tables
    lattice --- tables
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
    replG["IReplicatorGrain<br/>(per tree × peer)"]
    janG["IReplogJanitorGrain<br/>(per tree)"]
    broadcaster["DashboardBroadcaster"]
    healSvc["PartitionHealHostedService"]
    crdtStore["PartCrdtStore"]
    inbound["POST /replicate/{tree}"]

    router -->|"AdmitAsync"| siteG
    router -->|"IsPartitioned"| partG
    router -->|"GetConfig"| backG
    router -.->|"FactRouted · ChaosConfigChanged"| broadcaster

    siteReg -->|"WatchSites · preset fan-out"| siteG
    siteReg -.->|"SiteStateChanged"| broadcaster

    seeder -->|"HasSeeded?"| seedG
    seeder -->|"snapshot / zero / restore"| siteReg
    seeder -->|"emit seed facts"| router

    replG -->|"IsDisconnected?"| replDisc
    replG -->|"tick: scan + ship"| janG

    janG -->|"read Cursor"| replG

    healSvc -->|"IsPartitioned?"| partG
    healSvc -->|"promote shadows"| crdtStore

    inbound -->|"apply with RequestContext"| crdtStore
```

Key invariants:

- `FederationRouter` only **reads** chaos grains; it never writes
  them. Writes come from the UI / gRPC control surface via
  `ISiteRegistryGrain` and direct grain calls.
- `IReplicatorGrain` persists its own cursor; `IReplogJanitorGrain`
  reads every peer replicator's cursor to compute the safe-to-prune
  watermark — never prunes ahead of the slowest peer.
- `PartitionHealHostedService` only runs shadow promotion when
  `IPartitionChaosGrain.IsPartitioned` has flipped back to `false`.

---

## 4. Lattice trees

All four trees persist to `msmfgLatticeFacts` in Azure Table Storage.
Orleans grain state (chaos, replicator cursors, seed flag, baseline
part grains, inventory) persists to `msmfgGrainState`.

```mermaid
flowchart LR
    subgraph primary["Primary"]
        facts["mfg-facts<br/>{serial}/{wallTicks:D20}/{counter:D10}/{factId}<br/>→ fact bytes"]
    end

    subgraph derived["Derived / sibling"]
        siteIdx["mfg-site-activity-index<br/>{site}/{wallTicks:D20}/{counter:D10}/{serial}<br/>→ fact id"]
        crdt["mfg-part-crdt<br/>{serial}/labels/{label} — G-Set<br/>{serial}/operator — LWW register"]
    end

    subgraph ops["Operational"]
        replog["_replog__{tree}<br/>{wallTicks}{counter}|{clusterId}|{op}|{key}<br/>→ tombstone / pointer"]
    end

    facts -->|"written together"| siteIdx
    facts -->|"labels written on raise / disposition"| crdt
    facts -. filter .-> replog
    siteIdx -. filter .-> replog
    crdt -. filter .-> replog
```

| Tree | Key shape | Role | Replicated |
|---|---|---|---|
| `mfg-facts` | `{serial}/{wallTicks:D20}/{counter:D10}/{factId}` | Immutable per-part fact log. Forward range scan = HLC-ascending history. | Yes |
| `mfg-site-activity-index` | `{site}/{wallTicks:D20}/{counter:D10}/{serial}` | Per-site reverse-chronological activity feed. | Yes |
| `mfg-part-crdt` | `{serial}/labels/{label}` · `{serial}/operator` | Mixed G-Set + LWW register per part. | Labels yes · register filtered at origin |
| `_replog__{tree}` | `{wallTicks:D20}{counter:D10}\|{clusterId}\|{op}\|{key}` | Per-tree HLC-ordered replication log. | No (per-cluster) |

Range-scan patterns:

- Per-part history → forward scan of `mfg-facts` with prefix
  `{serial}/`.
- Per-site recent activity → reverse scan of
  `mfg-site-activity-index` with prefix `{site}/`.
- Shipping a batch → forward scan of `_replog__{tree}` from `Cursor+`
  to end, bounded by batch size.
- Janitor prune → forward range delete of `_replog__{tree}` up to
  `min(peer cursors) − 24h`.

---

## 5. Cross-cluster replication flow

A single operator write on the US cluster, propagating to the
EU cluster.

```mermaid
sequenceDiagram
    autonumber
    participant UI as Blazor UI (us)
    participant Router as FederationRouter
    participant Lat as Lattice backend
    participant Tree as mfg-facts (us)
    participant Filter as LatticeReplicationFilter
    participant Replog as _replog__mfg-facts (us)
    participant Rep as IReplicatorGrain<br/>("mfg-facts|eu")
    participant Traefik as traefik-eu
    participant Inbound as POST /replicate/mfg-facts<br/>(eu silo)
    participant PeerTree as mfg-facts (eu)
    participant PeerBase as Baseline backend (eu)

    UI->>Router: EmitFact(env)
    Router->>Lat: AppendAsync(env)
    Lat->>Tree: SetAsync(key, bytes)
    Tree-->>Filter: outgoing call completes
    Filter->>Replog: SetAsync(replog-key, envelope)
    Note over Filter: Skips if<br/>RequestContext["lattice.replay"] set

    loop grain timer · every 3 s
        Rep->>Replog: scan [Cursor+, end] batched
        Rep->>Rep: dedupe by key, keep highest HLC
        Rep->>Tree: GetAsync(key) — current value + HLC
        Rep->>Traefik: POST /replicate/mfg-facts (batch)
        Traefik->>Inbound: round-robin to silo-eu-{a|b}
        Inbound->>Inbound: RequestContext["lattice.replay"] = "us"
        Inbound->>PeerTree: SetAsync / DeleteAsync per entry
        PeerTree-->>Filter: outgoing call completes
        Note over Filter: Sees replay flag → skips replog append<br/>(loop broken)
        Inbound->>PeerBase: decode + EmitAsync (Set entries only, mfg-facts tree)
        Note over Inbound,PeerBase: Baseline has no retraction concept —<br/>Delete entries skipped
        Inbound-->>Traefik: 200 OK
        Traefik-->>Rep: ack
        Rep->>Rep: advance Cursor, persist
    end
```

Failure modes and their recovery:

| Scenario | Effect | Recovery |
|---|---|---|
| Peer unreachable | `ReplicationHttpClient` fails all URLs → error counter ↑ | Exponential backoff; next tick retries from same cursor. |
| Silo-B of peer restarts | Traefik health check removes it from `/replicate` pool within ~2 s | Next POST lands on silo-A; no replicator visibility. |
| Filter fails after primary write succeeds | One entry missing from replog | `AntiEntropyCursor` reminder re-scans primary and re-ships entries with `hlc > cursor`. |
| Duplicate delivery | Same `SetAsync` applied twice | Idempotent under write-once key discipline (and LWW for the few mutable keys that replicate). |
| A → B → A cycle | Would re-append replog on inbound apply | Broken by `RequestContext["lattice.replay"]` check in the filter. |
| Cluster split preset | `IReplicationDisconnectGrain.IsDisconnected = true` | Tick is a no-op; inbound returns 503; replog grows locally; resumes from cursor on clear. |
| Tier-5 `docker network disconnect` | HTTP POST fails at transport layer | Identical to "peer unreachable"; replicator backs off, catches up on reconnect. |
| Baseline replay decode fails | Single entry skipped on peer's baseline; lattice apply still succeeds | Logged; subsequent entries continue to apply. Baseline is a demo-visualisation backend, not a correctness-critical store. |

---

## 6. Configuration overlay

`appsettings.cluster.{name}.json` ships the localhost defaults.
Compose overrides only what has to change in containers:

| Key | Purpose |
|---|---|
| `ConnectionStrings__AzureTableStorage` | Per-cluster Azurite URL (`http://azurite-{cluster}:10002/...`). |
| `Replication__Peers__0__Name` | Peer cluster short name. |
| `Replication__Peers__0__BaseUrls__0` | Peer Traefik URL (one entry — Traefik handles silo failover). |
| `Cluster__SiloPortA` / `SiloPortB` | Both `11111` under Compose — each container has its own IP. |
| `ASPNETCORE_URLS` | `http://+:8080` in Compose; `Program.cs` skips its own `UseUrls` when this is set. |
| `Seeder__Enabled` | Explicit boolean — `true` on `silo-us-a`, `false` elsewhere. |

`ReplicationHttpClient.SendAsync` still iterates a multi-URL
`BaseUrls` list, so three deployment shapes are supported:

1. **Single LB endpoint (Compose default)** — one URL per peer;
   Traefik absorbs silo churn.
2. **Per-silo fan-out (localhost dev without an LB)** — one URL per
   silo; client retries the list until one accepts.
3. **Multi-zone failover** — one URL per availability zone, each
   pointing at that zone's regional LB; client stays on the primary
   zone while healthy.
