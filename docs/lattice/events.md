# Tree Events

Orleans.Lattice publishes metadata-only event notifications on a per-tree Orleans stream so that caches, projections, audit pipelines, and dashboards can react to tree mutations without polling.

Publication is opt-in (`LatticeOptions.PublishEvents` silo-wide default, overridable per tree via `ILattice.SetPublishEventsEnabledAsync`), **fire-and-forget**, and never affects the write-path outcome: a missing stream provider or a downstream queue failure is logged at `Warning` and swallowed. Events carry only metadata — **key name and operation kind, never the value bytes** — so subscribers that need the new value must `GetAsync` it themselves.

> **Tree events vs. [mutation observers](api.md#mutation-observers).** Pick events when you need out-of-process, fire-and-forget, metadata-only notifications for UI updates, cache invalidation, dashboards, or audit projections. Pick [`IMutationObserver`](api.md#mutation-observers) when you need an in-process, synchronous hook with the full value bytes on the write path — typically to feed a replication WAL or transactional outbox in another library. Observers add latency to every write in the silo; events do not.

## Event shape

```csharp verify
// LatticeTreeEvent is a readonly record struct with these fields:
LatticeTreeEvent evt = default;
LatticeTreeEventKind kind = evt.Kind;        // Set, Delete, DeleteRange, SplitCommitted, ...
string treeId = evt.TreeId;                   // logical tree name
string? key = evt.Key;                        // single-key events; null for tree-level events
int? shardIndex = evt.ShardIndex;             // physical shard index when relevant
string? opId = evt.OperationId;               // correlation id for saga writes
DateTimeOffset at = evt.AtUtc;                // silo-side timestamp
```

### `LatticeTreeEventKind`

| Kind | `Key` | `ShardIndex` | Emitted by |
|---|---|---|---|
| `Set` | key written | routing shard | `ILattice.SetAsync`, `SetAsync` + TTL, `GetOrSetAsync` (only when newly written), `SetIfVersionAsync` (only when applied), `SetManyAsync` (per entry), `SetManyAtomicAsync` (per entry, stamped with the saga's `OperationId`) |
| `Delete` | key deleted | routing shard | `ILattice.DeleteAsync` (only when the key existed) |
| `DeleteRange` | `"{start}..{end}"` | `null` | `ILattice.DeleteRangeAsync` (one event when ≥ 1 key was deleted) |
| `AtomicWriteCompleted` | `null` | `null` | `IAtomicWriteGrain` on terminal success. `OperationId` is the saga's idempotency key. |
| `SplitCommitted` | `null` | source shard | `ITreeShardSplitGrain` after the finalise phase. |
| `CompactionCompleted` | `null` | compacted shard | `ITombstoneCompactionGrain` after a successful pass. |
| `SnapshotCompleted` | `null` | `null` | `ITreeSnapshotGrain` on terminal success. |
| `ResizeCompleted` | `null` | `null` | `ITreeResizeGrain` on terminal success. |
| `ReshardCompleted` | `null` | `null` | `ITreeReshardGrain` on terminal success. |
| `TreeDeleted` | `null` | `null` | `ITreeDeletionGrain.DeleteTreeAsync`. |
| `TreeRecovered` | `null` | `null` | `ITreeDeletionGrain.RecoverAsync`. |
| `TreePurged` | `null` | `null` | `ITreeDeletionGrain.PurgeNowAsync` / terminal purge reminder. |

### Correlation

Atomic writes (`ILattice.SetManyAtomicAsync(entries, operationId)`) stamp every per-entry `Set` event **and** the terminal `AtomicWriteCompleted` event with the caller-supplied `operationId`. This lets subscribers stitch the per-key and saga-complete records into a single logical transaction.

Non-saga writes leave `OperationId` as `null`.

### Rollback emits per-key events but no terminal event

When a `SetManyAtomicAsync` saga fails and compensates, each compensating write flows through `ILattice.SetAsync` / `DeleteAsync` with the saga's `OperationId` still stamped on `RequestContext`. Subscribers therefore see the **forward** per-key `Set` / `Delete` events *and* the **reverse** compensating events, all tagged with the same `OperationId`, but **no** terminal `AtomicWriteCompleted` event. The absence of the terminal event (combined with the presence of later reverse events for the same `OperationId`) is the signal that the saga rolled back.

### Operations that deliberately do not emit events

The following APIs intentionally skip event publication to keep their bulk I/O profile predictable:

- `ILattice.BulkLoadAsync` — bulk-import path is optimised for throughput and assumes the importer already knows the full keyset.
- `ILattice.DeleteRangeStepAsync` — stateful cursor-driven range delete advances one batch at a time; only the coordinating `ILattice.DeleteRangeAsync` (executed as a single logical range operation) emits a `DeleteRange` event.

## Delivery semantics

- **Stream addressing.** Namespace is `"orleans.lattice.events"` (constant `LatticeEventConstants.StreamNamespace`) and the stream id is the **logical tree id**. One subscription per tree id, no fan-out filter grains.
- **Metadata-only.** Values are never included. Subscribers that need the new bytes must issue a follow-up `ILattice.GetAsync(evt.Key)`.
- **Best-effort.** Publication happens after the write is durable but is not part of the write commit. The underlying Orleans stream provider determines redelivery and ordering guarantees (e.g. MemoryStreams is at-most-once per activation; EventHub/AzureQueue streams are at-least-once with ordering per partition).
- **Fail-silent.** Missing provider, serialization failures, and downstream queue exceptions are logged and discarded. The write-path return value is unchanged.
- **No default provider.** Lattice does not register a stream provider on your behalf. You must add one explicitly (e.g. `siloBuilder.AddMemoryStreams("Default")` plus `AddMemoryGrainStorage("PubSubStore")`) and name it in `LatticeOptions.EventStreamProviderName`.

## Setup

On the silo:

```csharp verify
siloBuilder
    .AddLattice((services, name) => services.AddMemoryGrainStorage(name))
    .ConfigureLattice(o =>
    {
        o.PublishEvents = true;
        o.EventStreamProviderName = "Default";
    })
    .AddMemoryStreams("Default")
    .AddMemoryGrainStorage("PubSubStore");
```

On the cluster client, register a matching stream provider (same name) and call `SubscribeToEventsAsync`:

```csharp verify
// Assumes the client was configured with .AddMemoryStreams("Default") during build.
var tree = client.GetGrain<ILattice>("my-tree");
var handle = await tree.SubscribeToEventsAsync(
    client,
    async evt =>
    {
        Console.WriteLine($"[{evt.AtUtc:O}] {evt.Kind} key={evt.Key}");
        await Task.CompletedTask;
    },
    providerName: "Default",
    cancellationToken);

// Later, when shutting down:
await handle.UnsubscribeAsync();
```

### Missing provider

If the silo has `PublishEvents = true` but no matching `IStreamProvider` registered, `SubscribeToEventsAsync` throws `InvalidOperationException` with an actionable message ("register one via siloBuilder.AddMemoryStreams/AddEventHubStreams/etc."). This is the one hard-fail in the pipeline — publication itself continues to noop, but subscribing on a mis-configured client is treated as a programming error.

## Per-tree override

`LatticeOptions.PublishEvents` is the **silo-wide default**. Individual trees can opt in or out at runtime via `ILattice.SetPublishEventsEnabledAsync(bool?)`:

```csharp verify
// Force publication on for this tree regardless of the silo default.
await tree.SetPublishEventsEnabledAsync(true, cancellationToken);

// Force publication off (e.g. for a high-volume tree whose events are uninteresting).
await tree.SetPublishEventsEnabledAsync(false, cancellationToken);

// Clear the override and inherit the silo default again.
await tree.SetPublishEventsEnabledAsync(null, cancellationToken);
```

The override is persisted on the tree's registry entry (`TreeRegistryEntry.PublishEvents`) and survives silo restarts. Resolution order on every publish site:

1. Per-tree override if set.
2. Otherwise `LatticeOptions.PublishEvents`.

**Propagation.** The activation that handled the call observes the change immediately. Other activations (on other silos, or other stateless-worker instances on the same silo) refresh their cached value within ~5 seconds, so writes landing on a different silo may emit under the previous setting for a brief window. This is intentional: the per-site cache keeps publication latency negligible and avoids a registry round-trip on every write.

**System trees.** Trees whose id starts with `_lattice_` (e.g. the internal registry tree) never consult the registry for their own override — doing so would deadlock the non-reentrant registry activation that is currently servicing the write. For system trees the silo-wide `LatticeOptions.PublishEvents` is always authoritative.

## What **not** to rely on

- **Not a change log.** `LatticeTreeEvent` is not persisted; a silo restart without subscribers attached loses any in-flight events. For durable audit trails use a durable stream provider (EventHubs, AzureQueue) or maintain a secondary projection tree.
- **Not transactional.** Events are published after the write is durable, not as part of the same commit. A silo crash between the write and the publish loses the event; the write survives.
- **Not totally ordered across shards.** Events for the same key are ordered by the originating `IShardRootGrain`'s single-activation serialisation. Events for different keys (or different shards) may be interleaved by the stream provider.

## See also

- [Configuration](configuration.md#publishevents) — option reference.
- [Atomic Writes](atomic-writes.md) — how `SetManyAtomicAsync` stamps `OperationId` on every per-entry event.
- [Mutation observers](api.md#mutation-observers) — the in-process, synchronous, value-carrying alternative for write-path integrations.
- [`ILattice` API reference](api.md#ilattice) — full method surface.
