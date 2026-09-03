# Bulk Loading

Orleans.Lattice offers a dedicated bulk-load path for populating a tree from a
large dataset without paying the per-key cost of individual `SetAsync` calls.
This page explains when to reach for `ILattice.BulkLoadAsync` instead of the
general batched-write API `ILattice.SetManyAsync`, and quantifies the
efficiency difference in terms of the number of grain calls and write
round-trips each one performs.

## Two ways to write many entries at once

| API | Works against | Atomic | Best for |
|---|---|---|---|
| `SetManyAsync` | any tree (empty or already populated) | No | ongoing batched writes into a live tree |
| `BulkLoadAsync` (one-shot) | an **empty** tree only | The whole load is one-shot per shard | the initial import that seeds a brand-new tree |
| `BulkLoadAsync` (streaming extension) | an **empty** tree, fed incrementally | each chunk is committed independently | importing a dataset too large to hold in memory |

Both `SetManyAsync` and `BulkLoadAsync` fan out across shards in parallel and
batch their underlying write-ahead-log appends per leaf, so both are far
cheaper than a loop of single-key `SetAsync` calls. The difference between
them is structural: `SetManyAsync` inserts each entry into a living tree and
maintains that tree as it grows, whereas `BulkLoadAsync` computes the final
tree shape up front and writes it once. The [efficiency
comparison](#efficiency-compared-with-setmanyasync) below makes that concrete.

## `SetManyAsync` - batched writes into a live tree

`SetManyAsync` inserts or updates a batch of key-value pairs against a tree
that may already contain data. It routes each entry to its owning shard,
fans the per-shard slices out in parallel, and inside each shard batches the
commit so the whole slice for a given leaf is written in a single append
rather than one append per key.

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.SetManyAsync(new List<KeyValuePair<string, byte[]>>
{
    KeyValuePair.Create("user:1", Encoding.UTF8.GetBytes("Alice")),
    KeyValuePair.Create("user:2", Encoding.UTF8.GetBytes("Bob")),
});
```

`SetManyAsync` is **not atomic**: a partial failure leaves the batch
half-applied with no rollback. When all-or-nothing semantics are required,
use `SetManyAtomicAsync` instead. Because it tolerates a non-empty tree and
performs the normal tree maintenance (routing, node growth) on every call, it
is the right choice for continuous or incremental batch ingestion.

## `BulkLoadAsync` - one-shot import into an empty tree

`BulkLoadAsync` seeds a brand-new tree from a complete list of entries. It
sorts the input internally (the entries do **not** need to be pre-sorted),
packs each leaf to its configured capacity, and builds the upper levels of
the tree in a single bottom-up pass with no node splits.

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
var data = new[]
{
    new { Key = "user:1", Value = "Alice" },
    new { Key = "user:2", Value = "Bob" },
};
var entries = data
    .Select(d => KeyValuePair.Create(d.Key, Encoding.UTF8.GetBytes(d.Value)))
    .ToList();

await tree.BulkLoadAsync(entries);
```

It is a **one-shot initial-import primitive**: every shard must be empty when
it is called. Calling it against a tree that already holds data throws
`InvalidOperationException`. The call is idempotent on retry - re-issuing the
same load after it has completed is a no-op rather than an error - so a crash
mid-import can be retried safely. For append-style ingestion that re-flushes
batches over time, use `SetAsync`, `SetManyAsync`, or the streaming extension
below; `BulkLoadAsync` is not intended to be called repeatedly against a
continuously-fed tree.

## Streaming bulk load (extension method)

For datasets too large to materialise in memory, the `LatticeExtensions.BulkLoadAsync`
extension accepts an `IAsyncEnumerable` and flushes it in fixed-size chunks:

```csharp verify
async IAsyncEnumerable<KeyValuePair<string, byte[]>> ReadFromSource()
{
    // Yield entries in ascending key order (from your data source).
    for (int i = 0; i < 1_000_000; i++)
        yield return KeyValuePair.Create($"k:{i:D8}", Encoding.UTF8.GetBytes($"v{i}"));
    await Task.CompletedTask;
}

await tree.BulkLoadAsync(
    ReadFromSource(),
    grainFactory,
    chunkSize: 10_000);
```

The extension buffers entries per shard and flushes each shard's buffer
independently once it reaches `chunkSize` (default `10_000`). Flushes to
different shards run in parallel; flushes to the same shard are sequential so
key order is preserved. Each chunk is committed independently and is safe to
retry on failure. The streaming extension appends each chunk to the right
edge of the tree, so entries must arrive in ascending key order (as the
example's comment notes).

## Resumable chunked bulk load (`BulkAppendChunkAsync`)

`BulkLoadAsync` drives the whole stream in one call, so a dropped connection
loses the in-flight position. When an import must survive that - a multi-hour
load, or one driven from an external orchestrator that owns its own
checkpointing - drive the chunks yourself through `ILattice.BulkAppendChunkAsync`,
which is the idempotent, resumable primitive the streaming extension is built on.

| Method | Signature | Description |
|--------|-----------|-------------|
| `BulkAppendChunkAsync` | `Task<int> BulkAppendChunkAsync(string operationId, IReadOnlyList<KeyValuePair<string, byte[]>> sortedEntries, CancellationToken cancellationToken = default)` | Appends one chunk of a bulk load, returning the number of entries appended after any write interception. Enforces the whole-tree `LatticeOperation.BulkLoad` gate. |

Two caller obligations make it safe to re-drive:

- **`operationId` must be stable per chunk.** The implementation derives a
  per-shard operation id of the form `"{operationId}-{shardIndex}"`, and each
  shard records the last completed id, so re-driving the *same* chunk after a
  dropped connection reapplies nothing. Because a shard remembers only its
  single most-recently-completed id, resume from the last un-acknowledged chunk
  and never re-drive a chunk that a later chunk has already superseded on the
  same shard.
- **Keys must ascend**, both within a chunk and across the whole stream.
  Hash-partitioning a globally sorted stream preserves relative order within
  each partition, which is what keeps the per-shard appends on the right edge
  of the tree.

Throws `ArgumentException` when `operationId` is null or empty, and
`ArgumentNullException` when `sortedEntries` is null. An empty chunk is a
no-op that returns `0`.

## Efficiency compared with `SetManyAsync`

The dataset is the same in both cases, but the work done to land it differs.
Inserting `N` entries through `SetManyAsync` writes them into a tree that
grows underneath the writes: as leaves fill they split, and each split
propagates a new separator up through the internal levels, adding structural
write work and leaving leaves only partially filled. `BulkLoadAsync` skips all
of that - it sorts once, packs every leaf to capacity, and writes each level
of the finished tree exactly once, committing each shard a single time.

| | `SetManyAsync` (building a tree from empty) | `BulkLoadAsync` |
|---|---|---|
| Per-key routing/traversal | once per entry | replaced by a single sort pass |
| Node splits + separator propagation | incurred as leaves fill | none - the shape is computed up front |
| Leaf occupancy | partially filled after splits | packed to the configured capacity |
| Commits per shard | one per batch call | one for the whole load |
| Re-applies to an existing tree | yes | no - empty shards only |
| Atomic | no | one-shot per shard, idempotent on retry |

Because `BulkLoadAsync` produces fully-packed leaves, the resulting tree also
has fewer, denser leaves than the same data inserted incrementally, which
keeps later reads and range scans cheaper.

### Write round-trip budget for a large import

Both bulk paths collapse a leaf's worth of entries into a single batched
write-ahead-log append for that leaf, instead of one append per key. The
batch size is bounded by the tree's configured maximum keys per leaf
(default `128`) and by the WAL batching limits `LatticeOptions.WalMaxBatchEntries`
(default `100`) and `LatticeOptions.WalMaxBatchBytes` (default 4 MiB).

So a 1,000,000-entry initial import lands as roughly `1,000,000 / 128`, about
**7,800 packed leaves**, and therefore pays on the order of one batched WAL
append per leaf - a few thousand write round-trips in total, not one million.
Running the same import as individual `SetAsync` calls would pay close to one
million WAL round-trips; running it through `SetManyAsync` batches the appends
per leaf as well, but still performs the per-entry routing and the leaf splits
that `BulkLoadAsync` avoids. See
[the batched leaf write path](wal.md#batched-leaf-write-path) for the contract
that backs the per-leaf batching.
