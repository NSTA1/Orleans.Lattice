# Samples

Each sample lives under [`samples/`](../../samples) and is a self-contained runnable project.

## Feature gallery

Minimal, single-feature samples - one per row in the [README feature table](../../README.md#features). Each is an independent console app that hosts a single-silo in-process cluster (like [HelloWorld](#helloworld)), demonstrates exactly one capability with heavily-commented, before/after output, and carries its own README with a "When to use / When not to use" note. Run any of them with `dotnet run --project samples/<Name>`.

| Sample | What it shows |
|---|---|
| [AtomicWrites](../../samples/AtomicWrites/README.md) | `SetManyAtomicAsync` all-or-nothing multi-key writes, a failed-guard batch that leaves no partial state, and the cross-tree `IGrainFactory` overload. |
| [Authorization](../../samples/Authorization/README.md) | Single-silo default-deny authorization with group and nested-group membership: a group nested inside another group, per-tree/prefix/key rules, read-visibility range pruning, and a runtime grant via nesting. |
| [BulkLoading](../../samples/BulkLoading/README.md) | Seeding an empty tree via one-shot `BulkLoadAsync` and streaming `IAsyncEnumerable` ingestion. |
| [ChangeHistory](../../samples/ChangeHistory/README.md) | Reading a key's revision timeline with `ScanEntryHistoryAsync`. |
| [ConflictFreeMerges](../../samples/ConflictFreeMerges/README.md) | Two CRDT writers converging to the same result regardless of merge order. |
| [CrossClusterAuthorization](../../samples/CrossClusterAuthorization/README.md) | Two in-process clusters where the reserved membership and authorization-policy system trees converge over gRPC replication, so a grant or revoke authored on one site becomes enforced on the other. |
| [CrossClusterReplication](../../samples/CrossClusterReplication/README.md) | Two in-process clusters over gRPC where a write on one converges onto the other. |
| [Diagnostics](../../samples/Diagnostics/README.md) | The `DiagnoseAsync` per-tree health snapshot: shard depth, live keys, tombstones, hotness. |
| [DurableCursors](../../samples/DurableCursors/README.md) | A server-checkpointed cursor resuming from its last yielded key after a client restart. |
| [EntraAuthorization](../../samples/EntraAuthorization/README.md) | Single-silo authorization driven by a real Microsoft Entra ID identity: the signed-in Azure CLI user's `oid` is the tree owner (sole bootstrap administrator), so the owner writes and reads a value while an anonymous request is denied by the default-deny gate. |
| [Events](../../samples/Events/README.md) | Subscribing to the per-tree `LatticeTreeEvent` Orleans stream. |
| [HistoryViews](../../samples/HistoryViews/README.md) | An opt-in durable per-key history view whose revisions survive WAL garbage collection. |
| [MaterialisedViews](../../samples/MaterialisedViews/README.md) | A filter view and a sum-aggregation view maintained off the source tree's WAL. |
| [Metrics](../../samples/Metrics/README.md) | Reading the `orleans.lattice` meter instruments with a `MeterListener`. |
| [OnlineReshard](../../samples/OnlineReshard/README.md) | Growing the physical shard count online with reads, writes, and data intact throughout. |
| [PasswordProtection](../../samples/PasswordProtection/README.md) | A username/password front door for the State API gRPC surface (`AddEnvVarCredentialAuthorizer`) composed with per-tree authorization: a bootstrap admin plus a read-only user, wrong-password and anonymous calls rejected, and one tree hidden from the reader. |
| [PredicateOperations](../../samples/PredicateOperations/README.md) | Server-side `Expression<Func<T, bool>>` push-down so only matching keys or values cross the wire. |
| [Resize](../../samples/Resize/README.md) | Changing `MaxLeafKeys` / `MaxInternalChildren` on a live, populated tree. |
| [RetryPolicy](../../samples/RetryPolicy/README.md) | An idempotency-keyed retry policy recovering from simulated transient storage faults. |
| [SnapshotCursors](../../samples/SnapshotCursors/README.md) | Strict snapshot isolation: mid-iteration writes stay invisible to an open snapshot cursor. |
| [Snapshots](../../samples/Snapshots/README.md) | An offline point-in-time copy of a whole tree into an independent destination tree. |
| [SoftDeleteRecovery](../../samples/SoftDeleteRecovery/README.md) | Soft-deleting a tree within its retention window, recovering it, then purging permanently. |
| [StronglyConsistentScans](../../samples/StronglyConsistentScans/README.md) | `CountAsync` / `ScanKeysAsync` / `ScanEntriesAsync` returning the exact live key set under concurrent writes. |
| [TagIndexes](../../samples/TagIndexes/README.md) | Tagging keys and querying them back with `WithAllTags` (intersection) and `WithAnyTags` (union). |
| [TreeRegistry](../../samples/TreeRegistry/README.md) | Enumerating all user trees and their per-tree configuration overrides. |
| [Ttl](../../samples/Ttl/README.md) | Per-entry time-to-live: a key visible before its TTL and gone after it expires. |

## HelloWorld

[`samples/HelloWorld`](../../samples/HelloWorld)

Minimal interactive REPL over a single-silo, in-memory Orleans cluster. Starts a silo configured with `AddLattice(...)` + in-memory grain storage and reminders, then prompts for commands - `create`, `read`, `update`, `delete`, `list`, `exit` - and applies each one against a tree named `hello-world`. Every operation is timed with `Stopwatch` and reported as `[OK]` / `[FAIL]` with the elapsed milliseconds, so it doubles as a quick sanity check that a locally-built `Orleans.Lattice` package behaves correctly.

Run it with:

```shell
dotnet run --project samples/HelloWorld
```

## MultiSiteManufacturing

[`samples/MultiSiteManufacturing`](../../samples/MultiSiteManufacturing)

Regulated process-engineering traceability demo built on Blazor Server + gRPC + Orleans + Orleans.Lattice, backed by Azure Table Storage and Azure Storage Queues (Azurite for local development). Models a turbine-blade lifecycle (forge -> heat-treat -> machining -> NDT -> MRB -> FAI) across seven process sites, with a bulk-loaded inventory, operator-driven fact emission, a chaos fly-out for injecting site-level pause/delay/reorder, and a live divergence feed comparing a baseline LWW backend against the Orleans.Lattice fact store.

The sample runs as **two independent Orleans clusters** (`us` and `eu`), each with two silos, connected by an opt-in cross-cluster replication link over gRPC so changes in one cluster converge on the other.

Supporting documentation lives alongside the sample:

- [`README.md`](../../samples/MultiSiteManufacturing/README.md) - overview, run instructions, and feature tour.
- [`approach.md`](../../samples/MultiSiteManufacturing/approach.md) - implementation rationale, gotchas, and the reasoning behind each design choice.
- [`architecture.md`](../../samples/MultiSiteManufacturing/architecture.md) - structural view: topology, component graph, grain interdependencies, Lattice trees, replication sequence.
- [`glossary.md`](../../samples/MultiSiteManufacturing/glossary.md) - domain and implementation terms.

Run it with:

```shell
./samples/MultiSiteManufacturing/run.ps1
```

The script builds the host image if needed, starts both clusters (four silos plus two Azurites plus two Traefik proxies) under Docker Compose, and prints the per-cluster URLs - `http://localhost:5001` for `us` and `http://localhost:5002` for `eu`. Use `-Down` to tear everything back down, `-Clean` to wipe state between runs, and `-Logs` to tail silo logs.

## VehicleFleetSimulator

[`samples/VehicleFleetSimulator`](../../samples/VehicleFleetSimulator)

A simulated vehicle fleet that streams structured telemetry events over gRPC, imported into this repo to drive forthcoming WAL benchmarks for `Orleans.Lattice` and `Orleans.Lattice.Replication` and as the foundation for a future sample that bridges the simulator's event stream into a Lattice tree. Currently independent of the lattice library - it builds and runs on its own, with its own `VehicleFleetSimulator.slnx`.

The full stack (Azurite + Silo + gRPC API + Blazor WASM UI) runs under Docker Compose:

```shell
./samples/VehicleFleetSimulator/run.ps1
```

UI on `http://localhost:8090`, API on `http://localhost:8080`. See [`samples/VehicleFleetSimulator/README.md`](../../samples/VehicleFleetSimulator/README.md) for the full project layout, the on-import test-parallelism fix, and the planned Lattice-bridge sample.
