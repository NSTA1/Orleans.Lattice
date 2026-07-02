# Bulk Loading

## What it shows

Bulk loading seeds an **empty** tree far more cheaply than a loop of `SetAsync`
calls: it computes the finished tree shape up front, packs each leaf to
capacity, and commits each shard once instead of splitting nodes as the tree
grows. This sample demonstrates both entry points - the one-shot
`ILattice.BulkLoadAsync(entries)` that takes the whole dataset at once, and the
streaming `BulkLoadAsync(IAsyncEnumerable, grainFactory, chunkSize)` overload
that flushes fixed-size chunks for datasets too large to hold in memory.

## Run it

```
dotnet run --project samples/BulkLoading
```

## Expected output

```
== BulkLoading sample ==

1) One-shot BulkLoadAsync of 5000 entries into an empty tree...
   CountAsync -> 5000
   product:002500 = item-2500
   -> the whole dataset landed in one shot.

2) Streaming BulkLoadAsync of 20000 entries (chunkSize 4000)...
   CountAsync -> 20000
   k:00012345 = v12345
   -> ingested incrementally without buffering the whole set.

Done.
```

## When to use

- The initial import that seeds a brand-new (empty) tree from a known dataset.
- One-shot form when the dataset fits in memory; the streaming form when it does
  not (feed it an `IAsyncEnumerable` in ascending key order and it flushes in
  chunks).

## When not to use

- Ongoing or incremental writes into a tree that already holds data.
  `BulkLoadAsync` requires empty shards and throws otherwise; use `SetAsync` /
  `SetManyAsync` for continuous ingestion.
- Streaming input that is not in ascending key order - each chunk is appended to
  the right edge of the tree, so unsorted input is not supported by the streaming
  overload.

## Feature doc

[docs/lattice/bulk-loading.md](../../docs/lattice/bulk-loading.md)
