# Orleans.Lattice.Vector

An allocation-lean approximate nearest-neighbour (ANN) vector index for
Orleans.Lattice, with durable persistence on a Lattice tree and incremental
maintenance.

The package answers one question - *which of my stored vectors are nearest to
this query vector?* - without reading the whole corpus to do it, and without
rebuilding the index every time the process restarts.

## Why it exists

An exact k-nearest-neighbour search has to score every stored vector. That is
correct, and for a small corpus it is also fast. It stops being fast in a way
that no amount of tuning fixes: query cost is proportional to corpus size, so a
tree that has grown by an order of magnitude answers an order of magnitude more
slowly, and on a cold process every leaf holding a vector has to be activated
before the first query can be answered.

`Orleans.Lattice.Vector` breaks that proportionality. It partitions the corpus
once, then scores only the partitions a query actually needs. The fraction of the
corpus scanned *falls* as the corpus grows, so the index gets relatively cheaper
at exactly the scale where the exact scan gets unaffordable.

Because an index that had to be rebuilt on every activation would simply relocate
the cost, the index is also persisted on a Lattice tree of its own, loaded back in
bounded chunks, and maintained in place as vectors are written and retired.

## What it gives you

- **Sub-linear query cost.** Measured speedup over an exhaustive scan of the same
  data grows with the corpus: about 3x at 10,000 vectors and about 14x at
  1,000,000 (dimension 384).
- **A published, measured recall target.** Approximate means approximate, so the
  contract is stated and tested rather than assumed. See
  [Recall and accuracy](recall.md).
- **First-class deletes.** Vectors are retired in place, in constant time per
  deletion, rather than accumulating tombstones until a rebuild.
- **Durability.** The index survives a restart: reloading is roughly 23x cheaper
  than rebuilding at 250,000 vectors, and the gap widens with the corpus.
- **Lazy partial load.** Opening an index reads only its centroids; a query then
  fetches only the partitions it probes, so a box warms as it serves instead of
  paying for the whole corpus up front.
- **Honest status reporting.** The index always says which path answered a query
  (approximate or exhaustive) and whether it is still building, so a caller can
  never mistake a warming index for a settled one.

## Two layers, deliberately separable

The package is split so the algorithm can be used, tested and benchmarked without
a silo:

- **`VectorIndex`** - the in-memory index core. Pure algorithm, no Orleans types,
  no I/O. Build, insert, upsert, remove, train and query.
- **`DurableVectorIndex`** - the durable orchestrator. Persists the core on a
  Lattice tree, maintains it incrementally, and rebuilds it when the persisted
  form cannot be trusted.

`DurableVectorIndex` is written against the narrow `IVectorIndexStore` seam, so it
is exercised in tests against an in-memory fake with no cluster.
`LatticeVectorIndexStore` is the single type that binds to `ILattice`.

## Quick start

```csharp verify
using Orleans.Lattice.Vector;

// A 3-dimensional cosine index over a tiny corpus.
var index = new VectorIndex(new VectorIndexOptions
{
    Dimensions = 3,
    Metric = VectorDistanceMetric.Cosine,
});

index.Add(1, new float[] { 1f, 0f, 0f });
index.Add(2, new float[] { 0f, 1f, 0f });
index.Add(3, new float[] { 0.9f, 0.1f, 0f });

// Partition the corpus. Below the training minimum the index stays exact,
// answering by exhaustive scan, which is correct but not sub-linear.
index.Train();

// Results are written into a caller-owned span: the query path allocates nothing.
Span<VectorSearchResult> results = stackalloc VectorSearchResult[2];
var found = index.Search(new float[] { 1f, 0f, 0f }, results, out var mode);

// 'mode' reports how the answer was produced, so an approximate result is never
// mistaken for an exact one.
_ = mode == VectorSearchMode.Approximate;
_ = found;
```

## Documentation

| Topic | What it covers |
|---|---|
| [Architecture](architecture.md) | The index structure, why it was chosen, and the durable layout |
| [API](api.md) | The public surface, type by type |
| [Configuration](configuration.md) | Every option, its default, and when to change it |
| [Recall and accuracy](recall.md) | The measured recall target, how it is verified, and distribution drift |

## Constraints worth knowing before you adopt it

- **Threading**: an index is safe for concurrent *readers* or a *single* writer,
  not both. It does no locking. A grain's single-threaded turn is the natural
  home.
- **Training is synchronous and expensive** (about 10.7 s for 1,000,000 vectors at
  dimension 384) and transiently holds two copies of the corpus. Keep it off the
  request path; the index answers exhaustively and reports that it is building
  meanwhile.
- **Keys are `long`.** A consumer whose identifiers are strings owns a
  collision-free mapping; `DurableVectorIndex` provides one
  (`VectorKeyDictionary`). Do not hash string identifiers into a `long` - a
  collision returns the wrong record silently.
- **A lazily loaded index is read-only** and enforces it. It does not hold the
  partitions a mutation would update, so applying one would silently lose it.
- **Give the index its own tree, or at least its own key prefix.** Its recovery
  path deletes whole key ranges beneath that prefix.
