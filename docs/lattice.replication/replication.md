# Orleans.Lattice.Replication

Cross-cluster replication for [Orleans.Lattice](../../README.md) - captures every mutation at commit time, ships it between Orleans clusters under the source cluster's HLC, and applies it on the receiver with CRDT-aware merges, causal delivery, snapshot bootstrap, and dead-letter quarantine.

## What is it?

`Orleans.Lattice.Replication` is the **end-to-end cross-cluster replication subsystem** that layers on top of `Orleans.Lattice`. It is more than a wire format - it covers the full producer/transport/receiver pipeline plus the operational surface around it:

- **Capture.** Mutations are intercepted at commit time on the producing cluster and written to a per-tree WAL via the pluggable `IWalStorageProvider` seam (in-memory default, optional Azure Table Storage backend).
- **Ship.** A per-peer `IReplicationShipperGrain` streams batches to each peer over a long-lived push transport (`IReplicationTransport`, with gRPC as the canonical binding).
- **Apply.** Inbound entries flow through `IReplicationApplier`, which performs per-origin HWM dedup, causal-dependency parking via the causal-apply buffer, shadow-forward de-duplication, and CRDT-aware merges (LWW-Register, OR-Set, PN-Counter, VersionVector).
- **Bootstrap.** New or fallen-off-the-log peers seed via the snapshot subsystem (`ISnapshotProvider`, `IRemoteSnapshotTransport`, `LatticeRemoteSnapshotService`, `RemoteSnapshotProvider`) coordinated by the per-tree `LatticeBootstrapCoordinatorGrain` with crash-resumable state.
- **Operate.** Dead-letter quarantine for poison entries, per-tree merge-mode resolution, operator-driven re-seed, fall-off-log detection, admin introspection (`ILatticeReplicationAdmin`, `ILatticeWalIntrospection`), shared-secret-based mutual auth between clusters, and first-class metrics (apply duration, lag, FIFO violations, bootstrap retries, dead-letter rates) are all in-scope.

No external broker, no shared database, no host-level outgoing-call filter.

It supports:

- Per-tree opt-in with a declared `LatticeMergeMode` (LWW-Register, OR-Set, PN-Counter, VersionVector; MV-Register on the roadmap).
- Origin-stamped HLC on every record, with at-most-once apply per `(origin, hlc)`.
- Causal+ delivery - vector-clock-stamped entries with receiver-side dependency satisfaction across point writes, atomic multi-key writes, maintenance rewrites, and structural shadow-forwards.
- Active-active topology: any peer can write to any tree; conflicting updates converge deterministically.
- Atomic batch delivery - replicated `SetManyAtomicAsync` arrives on every peer as a single visible unit.
- Long-lived gRPC streaming push transport, with HTTP/2 multiplexing per peer cluster.
- Pluggable `IReplicationTransport` seam - gRPC is the canonical implementation; in-process and custom transports plug into the same contract.
- Snapshot bootstrap for new and re-seeded peers; auto-bootstrap on fall-off-the-log.
- Per-tree dead-letter queue for poison entries; replication continues past them.
- First-class per-peer metrics and lag observability.
- Pluggable `IWalStorageProvider` durability seam - in-memory default, optional Azure Table Storage backend.

## Core Properties

- **Convergent under concurrent writes.** Two clusters writing to the same key arrive at the same final state, deterministically, without coordination.
- **Causally consistent.** A receiver never observes a write before the writes it causally depends on - across point writes, atomic multi-key writes, maintenance rewrites, and structural shadow-forwards. See the [roadmap](../../src/lattice.replication/roadmap.md) for scope details.
- **Cycle-safe.** Origin attribution is durable metadata on every record, not ambient context - replicating into and back out of a peer cluster never loops a mutation back to its source.
- **At-most-once apply.** Re-delivery of the same `(origin, hlc, key, op)` is idempotent. Counters do not double-increment, sets do not re-add.
- **No host-level coupling.** Replication is produced by the silo at commit time. Hosts neither install outgoing-call filters nor route mutations through their own pipeline.

Behaviour is validated end-to-end by active-active convergence chaos tests across OR-Set, PN-Counter, and LWW-Register primitives running through real `AddLatticeReplication` silos.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Active-active topology** | Any peer can write to any tree. Multi-cluster concurrent updates converge to the same state by CRDT mode, not by post-merge LWW-on-bytes. | [Replication Modes](replication-modes.md) |
| **At-most-once apply** | Re-delivery of the same record is idempotent. Per-origin high-water-mark prevents double-apply for counters, sets, and registers. | [Replication Apply](replication-apply.md) |
| **Atomic batch delivery** | Replicated `SetManyAtomicAsync` arrives on every receiver as a single visible unit. No reader observes a partial-set state across clusters. | [Replication Apply](replication-apply.md) |
| **Auto-bootstrap on fall-off-log** | Peers whose cursor falls behind the retained WAL are re-seeded from a fresh snapshot automatically - no operator intervention. | [Auto-Bootstrap](auto-bootstrap.md) |
| **Causal+ ordering** | A receiver never observes a write before its causal dependencies - point writes, atomic multi-key writes, maintenance rewrites, and structural shadow-forwards all preserve causal order. | [WAL](wal.md) |
| **Dead-letter queue** | Poison entries - schema skew, oversized values, corrupt HLC - are quarantined per tree after a configurable retry budget; replication continues past them. | [Dead-Letter Queue](dead-letter-queue.md) |
| **gRPC push transport** | Long-lived gRPC streaming sender / receiver pair. Push latency is sub-second, well below reminder-cadence pull. | [gRPC Push Transport](grpc-push-transport.md) |
| **Health check** | ASP.NET Core / Kubernetes `IHealthCheck` reporting `Degraded` when entries-behind, last-contact age, or consecutive-error streak crosses a soft bound, `Unhealthy` when sustained for longer than the configured grace window. | [Health Check](health-check.md) |
| **Observability** | Per-peer entries-behind, bytes-behind, seconds-behind, consecutive-errors, last-contact metrics on `LatticeReplicationMetrics`. | [Observability](observability.md) |
| **Origin-stamped HLC** | Every replicated record carries `(originClusterId, hlc)`. Cycles break naturally, transitive topologies preserve causality, and applies are idempotent by identity. | [Replication Apply](replication-apply.md) |
| **Per-tree opt-in + per-key filter** | Declare which trees replicate and (optionally) which keys within a tree. Granular enough to ship operator-visible labels while keeping per-shift counters local. | [Replication Modes](replication-modes.md) |
| **Pluggable transport** | `IReplicationTransport` is the public seam. gRPC is the canonical implementation; in-process and custom transports plug into the same contract. | [Transport](transport.md) |
| **Receiver-side flow control** | The receiver stamps optional `SuggestedBatchSize` / `PauseForMs` hints onto every ack; the sender clamps its per-tick batch cap and pauses on request. A struggling receiver throttles in-band without timing out RPCs; a recovered receiver re-accelerates by lifting the hints. | [Receiver Flow Control](receiver-flow-control.md) |
| **Snapshot bootstrap** | New or re-seeded peers receive a point-in-time snapshot, then switch to incremental shipping at the snapshot's HLC. | [Snapshot Bootstrap](snapshot-bootstrap.md) |
| **Typed CRDT deltas** | The wire carries typed deltas for LWW-Register, OR-Set, PN-Counter, and VersionVector. Receivers merge by mode, not by opaque-byte LWW. | [Deltas](deltas.md) |

## Quick Start

Add replication on top of an existing `Orleans.Lattice` silo. The minimum end-to-end setup is `AddLattice` + `AddWalStorage` + `AddLatticeReplication` + a transport. On the producer/sender silo:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeReplication(opts =>
        {
            opts.ClusterId = "site-a";
            opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                ["operator-labels"]  = LatticeMergeMode.OrSet,
                ["machine-counters"] = LatticeMergeMode.PnCounter,
            };
            opts.ReplicationPeers = new[] { "site-b" };
        });
});

// Cross-cluster gRPC binding - one entry per peer cluster wires both
// the live-push transport and the bootstrap snapshot transport.
builder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.Peers["site-b"] = new Uri("https://site-b.example:5001");
});
```

On the receiver silo, register the binding (same single helper) and map the endpoint routes:

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeReplicationGrpc();

var app = builder.Build();
app.MapLatticeReplicationGrpc();
```

For a working multi-cluster example exercising HLC-ordered facts, typed OR-Set replication, and gRPC push, see the `MultiSiteManufacturing` project under [`samples/`](../../samples).

## Reference

For day-to-day use and operations:

- [Replication Modes](replication-modes.md) - per-tree opt-in, `LatticeMergeMode` selection, per-key filter.
- [Observability](observability.md) - `LatticeReplicationMetrics` instruments, per-peer lag, error counters.
- [Dead-Letter Queue](dead-letter-queue.md) - quarantine model, operator surface, replay.
- [Snapshot Bootstrap](snapshot-bootstrap.md) - point-in-time bootstrap, snapshot HLC, incremental cutover.
- [Auto-Bootstrap](auto-bootstrap.md) - fall-off-the-log detection and automatic re-seed.
- [Transport Security](transport-security.md) - shared-secret authentication, HTTPS-by-default, custom secret sources, env-var convention.

For internals (the "how"):

- [Change Feed](change-feed.md) - `IChangeFeed` seam, per-shard cursor, async enumerable shape.
- [Replication Apply](replication-apply.md) - receiver-side applier, per-origin high-water-mark, recent-apply cache, atomic batch buffering.
- [Replication Drivers](replication-drivers.md) - production drivers that turn the dormant seams into a running pipeline.
- [Transport](transport.md) - `IReplicationTransport` seam, batch shape, acks.
- [gRPC Push Transport](grpc-push-transport.md) - canonical transport: streaming RPC, channel reuse, custom marshallers.
- [Receiver Flow Control](receiver-flow-control.md) - `IReceiverFlowControlPolicy` seam, ack-stamped hints, sender clamping / pause composition.
- [Wire Format](wire-format.md) - `ReplicationBatchEnvelope`, `IReplicationBatchEncoder`, wire version negotiation.
- [Deltas](deltas.md) - typed CRDT delta records on the wire.
- [WAL](wal.md) - per-shard replication write-ahead log, turn-safe batching, causal+ entry schema.

## Roadmap

The feature plan - including the items still ahead of GA (observable topology, MV-Register dispatch, performance follow-ons) and the satisfied dependencies - lives in [`src/lattice.replication/roadmap.md`](../../src/lattice.replication/roadmap.md).
