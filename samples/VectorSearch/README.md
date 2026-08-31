# Vector search

Approximate nearest-neighbour search with `Orleans.Lattice.Vector`: sub-linear
query cost, honest per-query reporting, first-class deletes, and accuracy that is
measured rather than asserted.

## What this sample shows

1. **Building an index** over a clustered corpus of 10,000 vectors - the shape a
   real embedding model produces.
2. **The exhaustive-to-approximate transition.** Before `Train()` there is no
   partitioning, so the index answers *exactly* by scanning everything. It reports
   `VectorSearchMode.Exhaustive` and says so. After `Train()` it reports
   `Approximate` and scores only the partitions a query probes.
3. **Measured recall against an exact oracle.** The sample computes brute-force
   top-k over the same corpus and reports recall@10, so you can see the published
   floor being met rather than take it on trust.
4. **First-class deletes.** A retired vector never reappears, with no tombstone
   left to compact away.

## Running it

```
dotnet run --project samples/VectorSearch
```

No silo, no cluster, no storage. The index core is a pure algorithm with no
Orleans dependency, which is precisely what makes it testable and benchmarkable in
isolation.

## What to look for in the output

- **The mode changes** from `Exhaustive` to `Approximate` across `Train()`, and
  the exhaustive answer is exact. A warming index is not degraded - it is correct
  and slower.
- **The probe fraction.** The sample prints what proportion of partitions each
  query scores. That fraction *falls* as a corpus grows, which is the property
  that breaks the "query cost is proportional to corpus size" law. It is why the
  default probe count is derived from `sqrt(partitionCount)` and must never be set
  to a fixed fraction of the partitions.
- **Recall at or above the published floor** of 0.95 for a clustered corpus.

## Going further

This sample uses the in-memory core. For a real deployment you also want the
durable layer, `DurableVectorIndex`, which persists the index on a Lattice tree in
bounded chunks and maintains it incrementally, so a restart *reloads* the index
rather than rebuilding it - about 23x cheaper at 250,000 vectors, and the gap
widens with the corpus.

See:

- [Package overview](../../docs/lattice.vector/README.md)
- [Architecture](../../docs/lattice.vector/architecture.md) - the index structure and the durable layout
- [Recall and accuracy](../../docs/lattice.vector/recall.md) - the measured target, and distribution drift
- [Configuration](../../docs/lattice.vector/configuration.md) - every option, and the one setting that can silently undo the whole thing
