# Orleans.Lattice.Vector

Allocation-lean **approximate nearest-neighbour** index over dense `float`
vectors, for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

The algorithmic core in the `Orleans.Lattice.Vector` namespace has no Orleans
dependency and can be used standalone. The `Orleans.Lattice.Vector.Persistence`
namespace adds `DurableVectorIndex`, which persists that index on a Lattice tree;
it is the only part of the package bound to Orleans.

## Why

Exact k-nearest-neighbour search costs one distance computation per stored
vector, so query cost grows with the corpus and nothing bounds it. `VectorIndex`
replaces that with an inverted file: query cost grows with the **square root** of
the corpus instead.

## Structure

A seeded k-means pass partitions the corpus into roughly `sqrt(n)` cells. Each
cell **owns its members' vectors contiguously**, so scoring a cell is a straight
streaming scan rather than a scattered walk over the whole corpus - that layout
is worth several times the naive one. A search ranks the cell centroids and
scores only the best few cells. The single `Probes` dial trades accuracy for
latency, and the achieved recall against exact search is measured, not asserted -
see the numbers below.

## Shape of the API

```csharp
var index = new VectorIndex(new VectorIndexOptions { Dimensions = 768 });
index.EnsureCapacity(100_000);

index.Add(key: 1, vector);          // Upsert(key, vector) replaces in place
index.Remove(key: 1);               // first-class delete, idempotent
index.Train();                      // build the partitioning

Span<VectorSearchResult> hits = stackalloc VectorSearchResult[10];
var found = index.Search(query, hits, out var mode);
```

`mode` is `Approximate` when the partitioning answered and `Exhaustive` when the
index scored every live vector, so a consumer never presents an approximate
answer as an exact one. `index.State` reports `Empty`, `Building`, or `Ready`;
`Building` is the honest "still warming up" signal, during which searches remain
correct and exact.

## Guarantees

- **Zero steady-state query allocation.** Results are written into a
  caller-owned span, probe scratch is stack-allocated up to 128 partitions and
  pooled beyond, and no metric needs a normalised copy of the query. The build
  path allocates only the contiguous backing block (once, when `EnsureCapacity`
  is called up front) and pooled training scratch.
- **Contiguous storage.** Each cell holds its members in one flat `float` block,
  never as a per-vector object graph and never as an index into a shared block. A
  delete backfills the hole with the cell's last member, so a block stays dense
  and a probe never tests liveness per vector.
- **Deletes are first class.** A removed vector leaves its cell immediately, so it
  can never surface in a later result.
- **Determinism.** A result set is totally ordered by descending score with
  ascending key breaking ties, and training samples in key order from an
  explicit seed. The same key / vector set with the same options produces
  identical results regardless of insertion or deletion order.
- **Vectorised arithmetic.** Every kernel runs through
  `System.Numerics.Tensors.TensorPrimitives`, which dispatches to the widest SIMD
  width the hardware offers.

## Persisting it: `DurableVectorIndex`

An index that has to be rebuilt on every activation just relocates the cost it
exists to remove. `DurableVectorIndex` persists the index on a Lattice tree,
maintains it in place as vectors are written and retired, builds it in the
background when it does not yet exist, and rebuilds it rather than trusting it
when what was persisted cannot be verified.

```csharp
var index = await DurableVectorIndex.OpenAsync(
    new LatticeVectorIndexStore(tree), source, options);

await index.BuildStepAsync();                 // one bounded, resumable slice
await index.UpsertAsync("doc-1", vector);     // in place, no rebuild
await index.RemoveAsync("doc-2");             // journalled, survives a crash
await index.FlushAsync();                     // only the cells that moved
```

At 250,000 vectors a restart costs **1.1 s to reload** against **24.8 s to
rebuild**. Opening lazily reads only the centroids - 0.52 s - and fetches a cell
the first time a query probes it, so the box answers in 75 ms while holding 12%
of the corpus, and warms as it serves. The answer is identical to the fully
resident index, because a query is scored against exactly the cells it selects.

**The coherence contract.** The index is a derived projection of the store of
record, which is authoritative.

- A retired vector never appears in a result, before or after a restart: a
  removal writes a durable tombstone before it is applied and drops it only once
  the removal is durable, so a crash mid-deletion completes the deletion rather
  than resurrecting the vector.
- The index may lag the source only in the *missing* direction, which costs
  recall and never correctness. Outstanding work is reported by `Progress`.
- Every inconsistency is resolved by discarding index state and recomputing.
  Nothing here ever writes to a store of record - that asymmetry is what makes
  throwing the index away always safe.
- Persisted state is admitted only if its manifest, every record checksum, every
  partition's chunk set, and its declared vector count all agree. A truncated,
  corrupt, incomplete, or version-incompatible index is rebuilt, never partly
  served.
- Every search reports its mode. Before the partitioning exists the index answers
  by exhaustive scan, which is *exact* - slower, not worse - and must be surfaced
  as warming up, never as an error.

**Drift.** Incremental maintenance keeps the index correct indefinitely, but it
cannot keep the cells descriptive once the corpus moves away from the
distribution they were trained on. `UpdatesSinceTraining` is the signal;
`RetrainAsync` is the repair, and it re-reads nothing because the corpus is
already resident.

## The chunking seam underneath

`CreateSnapshot(maxItemsPerChunk)` produces a bounded, version-stamped chunk plan
- never a single unbounded record. Centroid chunks come first, so a reader that
has applied only those can already call `SelectPartitions` to learn which cells a
query needs, and fetch nothing else. Because a cell already stores its members
contiguously, a vector chunk is a slice of that cell rather than a gather across
the corpus. `VectorIndexHeader.TryRead` refuses a format version this build does
not understand rather than misreading it, and `ApplyChunk` is order-independent
and idempotent, so a restore can resume.

## Threading

An index is safe for concurrent readers **or** a single writer, not both. A host
that mutates concurrently with searches serialises access itself; the natural
home is a single-threaded grain.

## Measured recall and cost

Every figure below is produced by a committed harness in the repository, not by
reasoning. Recall runs in the ordinary unit lane on every build, so a regression
breaks the suite rather than a document.

At the default configuration (partitions `sqrt(n)`, probes `2 * sqrt(partitions)`)
over 20,000 vectors at 64 dimensions, k = 10:

| Corpus geometry | recall@10 floor | measured |
|---|---|---|
| Clustered - every real embedding space | 0.95 | **1.000** |
| Unclustered - adversarial, independent Gaussian | 0.55 | **0.588** |

Recall holds as the corpus grows even though the default scans a shrinking
fraction of it: 0.968 at five thousand vectors (25% scanned), 0.998 at twenty
thousand (17%), 1.000 at sixty thousand (13%).

Scale, at 384 dimensions on a 16-core x64 box. `exhaustive` is the same index
answering by full scan before it was trained, so the comparison is like for like:

| vectors | train s | approx ms | exhaustive ms | speedup | bytes/vector |
|---|---|---|---|---|---|
| 10,000 | 0.33 | 0.25 | 0.76 | 3.1x | 1,563 |
| 100,000 | 0.81 | 2.57 | 12.02 | 4.7x | 1,552 |
| 1,000,000 | 10.66 | 10.05 | 138.51 | 13.8x | 1,549 |

The speedup **grows with the corpus**, which is the whole point: exhaustive
latency is proportional to the corpus and approximate latency is not.

Full tables - the probe dial against recall, the churn cycle, the restart and
lazy-load sweep, and the per-operation allocation audit - are in
`test/lattice.vector/MEASUREMENTS.md` in the repository.
