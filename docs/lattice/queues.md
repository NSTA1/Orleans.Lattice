# Queues

`ILatticeQueue<T>` is a typed, cluster-internal, single-cluster FIFO queue embedded in your Orleans cluster. Each logical queue is one coordinator grain over a reserved system tree; entries are appended at the tail and consumed from the head in insertion order. It is the consolidation point for every system-tree-backed FIFO buffer in the library - the replication dead-letter queue runs on the same engine.

## Resolving a queue

Resolve a queue by name from any `IGrainFactory`. Each distinct name is an independent queue; resolving the same name returns a facade over the same coordinator grain.

```csharp verify
ILatticeQueue<string> queue = grainFactory.GetLatticeQueue<string>("work-items");
long id = await queue.EnqueueAsync("payload", cancellationToken);
int depth = await queue.CountAsync(cancellationToken);
LatticeQueueEntry<string>? head = await queue.TryDequeueAsync(cancellationToken);
```

Values are serialized with an injectable `ILatticeSerializer<T>`, defaulting to `JsonLatticeSerializer<T>.Default`. Pass your own serializer as the optional third argument to `GetLatticeQueue<T>` for custom wire formats. Because serialization happens client-side, `T` does not need to be Orleans-serializable.

## Surface

| Member | Behaviour |
|---|---|
| `EnqueueAsync(item, ct)` | Appends `item`, returns the assigned monotonic `long` id. |
| `TryDequeueAsync(ct)` | Removes and returns the head entry, or `null` when empty. |
| `PeekAsync(ct)` | Returns the head entry without removing it, or `null` when empty. |
| `CountAsync(ct)` | Number of entries currently parked (served from the in-memory cache). |
| `ListAsync(ct)` | Ascending-id snapshot of every parked entry, for diagnostics. |

`LatticeQueueEntry<T>` carries the monotonic `EntryId` assigned at enqueue time alongside the deserialized `Value`. Entry ids are recomputed as `max(stored id) + 1` on activation, so monotonicity survives silo restart.

## Bounded queues

Set `LatticeOptions.QueueCapacity` (resolved per queue via `IOptionsMonitor<LatticeOptions>.Get(queueName)`) to cap a queue. When the bound is reached, enqueueing evicts the oldest entry first (FIFO eviction). Leaving it `null` (the default) keeps the queue unbounded. When set it must be at least `1`.

## Performance and scope

The queue is **strictly cluster-internal**: it is not a CRDT-replicated primitive and never ships a `LatticeMergeMode`. Destructive dequeue is fundamentally non-monotonic, so coordination-free cross-cluster FIFO is outside the library's CRDT-merge model.

A few properties follow from the FIFO contract and the hash-partitioned backing store:

- **The single coordinator grain is the throughput ceiling.** One activation per logical queue serializes all operations to preserve FIFO order. Sharding cannot relieve this - ordering is the contract. Applications needing higher throughput should fan work across several independently-named queues (partitioned lanes) and hash a producer key to a lane.
- **Head, tail, and count are served from memory.** The grain bulk-loads on activation and serves `Count` / `Peek` from the in-memory cache, so the hot path never range-scans the backing tree. A head-cursor row is persisted so steady-state dequeue and cold start skip already-dequeued ids rather than re-walking from the head of the prefix.
- **`ListAsync` is an O(shards) fan-out.** Because the backing store hashes monotonic entry keys uniformly across every physical shard, an ascending-id snapshot is a k-way merge across all shards. It is intended for diagnostic / control-plane use, not the hot path - prefer `CountAsync` and `PeekAsync` there.

Scope queue throughput to diagnostic / control-plane workloads rather than primary data-plane traffic, or partition across lanes as above.
