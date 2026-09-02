# Orleans.Lattice.Vector measurements

Every figure here is produced by a committed, reproducible harness in this test
project. Nothing is asserted from reasoning; if a number moves, the harness that
produced it moves with it.

- **Recall** is measured by `VectorIndexRecallTests`, which computes the exact
  top-k by brute force over a fixed corpus and compares. It runs in the ordinary
  unit lane on every build, so a regression breaks the suite rather than a
  document.
- **Build time, query latency, and bytes per vector** are measured by
  `Benchmarks/VectorIndexScaleBenchmarkTests`, gated on `LATTICE_VECTOR_BENCH` so
  it never slows an ordinary lane. Reproduce with:

  ```powershell
  $env:LATTICE_VECTOR_BENCH = "1"
  dotnet test test/lattice.vector/Orleans.Lattice.Vector.Tests.csproj -c Release --filter "TestCategory=Benchmark"
  ```

Corpora are synthetic but reproducible: `VectorCorpus` uses its own xorshift
stream rather than `Random`, so the same corpus is generated on any runtime.

## The recall contract

At the default configuration - partitions derived as `sqrt(n)`, probes derived as
`2 * sqrt(partitions)` - the published floors are:

| Corpus geometry | recall@10 floor | measured |
|---|---|---|
| Clustered (every real embedding space) | 0.95 | **1.000** |
| Unclustered (adversarial, independent Gaussian) | 0.55 | **0.588** |

Both are enforced as assertions, so the published figure cannot drift away from
the code. Recall was also measured across four seeds at 20,000 vectors and was
1.0000 for every one, so the clustered figure does not depend on a lucky seed.

## Recall against the probe dial

20,000 vectors, 64 dimensions, 141 partitions, k = 10, 50 queries. `scanned` is
the fraction of the corpus each query touches.

### Clustered corpus (128 clusters)

| probes | scanned | recall@10 |
|---|---|---|
| 1 | 0.7% | 0.3560 |
| 2 | 1.4% | 0.6480 |
| 4 | 2.8% | 0.8460 |
| 8 | 5.7% | 0.9700 |
| 16 | 11.3% | 1.0000 |
| 24 (default) | 17.0% | 1.0000 |
| 32 | 22.7% | 1.0000 |
| 64 | 45.4% | 1.0000 |
| 141 | 100.0% | 1.0000 |

### Unclustered corpus (independent Gaussian)

With no cluster structure to exploit, recall tracks the fraction scanned almost
exactly. This is the honest worst case, and it is why the package documents the
probe dial rather than claiming a single universal recall.

| probes | scanned | recall@10 |
|---|---|---|
| 1 | 0.7% | 0.0820 |
| 4 | 2.8% | 0.1980 |
| 24 (default) | 17.0% | 0.5880 |
| 36 | 25.5% | 0.7260 |
| 71 | 50.4% | 0.9120 |
| 141 | 100.0% | 1.0000 |

Probing every partition reproduces the exact result set in both corpora, which
the harness asserts.

## Recall holds as the corpus grows

The default probe rule deliberately scans a *shrinking* fraction of a growing
corpus - a fixed fraction would put the corpus size back in the cost. This
measures that recall nevertheless holds, which is what makes the default safe to
ship as a primary retrieval path.

| corpus | partitions | probes | scanned | recall@10 |
|---|---|---|---|---|
| 5,000 | 71 | 18 | 25.4% | 0.9680 |
| 20,000 | 141 | 24 | 17.0% | 0.9980 |
| 60,000 | 245 | 32 | 13.1% | 1.0000 |

## Recall across a churn cycle

4,000 vectors, a quarter retired and replaced from the same space, with **no**
retraining, versus an index rebuilt from scratch over the same final contents:

| index | recall@10 |
|---|---|
| Churned, not retrained | 1.0000 |
| Rebuilt over the same contents | 1.0000 |

Deletes and inserts maintain the cells in place, so a maintenance loop does not
have to retrain to stay accurate. Retraining remains the answer to a genuine
distribution shift, where new vectors land outside the region the cells were
fitted to.

## Scale: build, query, and memory

384 dimensions (a common embedding width), clustered corpus, k = 10, default
configuration. Measured on a 16-core x64 box, .NET 10, Release. `exhaustive ms`
is the same index answering by full scan before it was trained, so the comparison
is like for like on identical data and identical arithmetic.

| vectors | partitions | probes | insert s | train s | approx ms | exhaustive ms | speedup | bytes/vector | total MB |
|---|---|---|---|---|---|---|---|---|---|
| 10,000 | 100 | 20 | 0.07 | 0.33 | 0.245 | 0.756 | 3.1x | 1,563 | 15 |
| 50,000 | 224 | 30 | 0.03 | 0.55 | 1.477 | 5.777 | 3.9x | 1,554 | 74 |
| 100,000 | 316 | 36 | 0.08 | 0.81 | 2.566 | 12.021 | 4.7x | 1,552 | 148 |
| 250,000 | 500 | 46 | 0.28 | 1.98 | 4.047 | 28.671 | 7.1x | 1,551 | 370 |
| 1,000,000 | 1,000 | 64 | 1.78 | 10.66 | 10.052 | 138.512 | 13.8x | 1,549 | 1,478 |

Timings vary by a few tens of percent between runs on a shared machine; a second
run of the identical sweep gave speedups of 2.9x, 3.1x, 4.6x, 8.9x, and 12.4x for
the same five rows. The recall figures, by contrast, were bit-identical across
runs, which is the determinism guarantee doing its job. Treat the latency numbers
as a band and the shape - a speedup that grows with the corpus - as the result.

The speedup **grows with the corpus** - about 3x at ten thousand, 13x at a
million. That is the point: exhaustive latency is proportional to the corpus while
approximate latency is not, so the gap widens rather than holding at a constant
factor. Extrapolated to the 73,537-vector live corpus that motivated this work,
the default probes about 12 percent of the cells, so a consumer that pages cells
in on demand touches roughly an eighth of the store rather than all of it.

Bytes per vector is `dimensions * 4 + 12` plus the centroid block: 1,536 bytes of
vector, 4 for the cached norm, 8 for the key. There is no per-vector object
header and no posting-list indirection, so the figure barely moves with corpus
size.

### Recall and latency together, at 100,000 vectors

384 dimensions, 316 partitions, k = 10. Recall here is exact and reproducible;
the latency column is a single unwarmed pass per configuration and is noisier
than the sweep above - read it for shape, not for absolute numbers.

| probes | scanned | recall@10 |
|---|---|---|
| 1 | 0.3% | 0.0200 |
| 4 | 1.3% | 0.1060 |
| 8 | 2.5% | 0.1920 |
| 16 | 5.1% | 0.1920 |
| 36 (default) | 11.4% | 1.0000 |
| 64 | 20.3% | 1.0000 |
| 158 | 50.0% | 1.0000 |
| 316 | 100.0% | 1.0000 |

Queries here are drawn from a different cluster placement than the corpus, which
is why recall climbs late and steeply: a query far from every corpus cluster has
its true neighbours spread over many cells. It is the pessimistic case for a
partitioned index, and the default still reaches 1.0000 while scanning an eighth
of the corpus.

## Allocation

Asserted by `VectorIndexAllocationTests`, which follows the same four-part probe
rule as the durable fixture below: differential rather than absolute, a
**full-size** warm-up, the **minimum** kept across repeats, and set-up kept
outside the measured window. Its battery test proves the probe can fail, with a
sink that provably escapes. The synchronous query and mutation paths use
`GC.GetAllocatedBytesForCurrentThread`; training uses
`GC.GetTotalAllocatedBytes(precise: true)`, because it may hand its assignment
pass to the thread pool and a per-thread figure would under-count that work.

| path | allocated |
|---|---|
| Approximate search, 2,000 queries | **0 bytes** |
| Exhaustive search, 500 queries | **0 bytes** |
| Search with 200 probes (past the stack-allocation bound, pooled scratch) | **0 bytes** |
| `SelectPartitions`, 2,000 calls | **0 bytes** |
| `Contains` / `TryGetVector`, 2,000 calls | **0 bytes** |
| `Remove` of an absent key, 1,000 calls | **0 bytes** |
| Inserting 4,000 vectors after `EnsureCapacity` | **0 bytes** |
| Retraining 20,000 vectors | 1.01x the cells it must retain |

The training figure is the meaningful one: at 1.01x the retained cell blocks,
essentially every scratch buffer the k-means pass uses came from
`ArrayPool<T>.Shared` rather than the heap.

## The durable index

Everything below is about `DurableVectorIndex`, which persists the index of the
sections above on a Lattice tree, maintains it in place, and rebuilds it rather
than trusting it when what was persisted cannot be verified.

`Benchmarks/DurableVectorIndexLoadBenchmarkTests` produces the timing figures,
gated on the same `LATTICE_VECTOR_BENCH` variable. The recall and allocation
figures are asserted in the ordinary unit lane, so they cannot drift away from
the code.

### Restart cost

384 dimensions, clustered corpus, chunks of at most 1,024 vectors, measured over
an in-memory store so the figures isolate the index's own work from a particular
backing store's latency.

| vectors | partitions | build s | full load s | lazy open s | first lazy query ms | resident after one query | resident MB | persisted MB | records | persisted bytes/vector |
|---|---|---|---|---|---|---|---|---|---|---|
| 10,000 | 100 | 0.92 | 0.10 | 0.045 | 14.3 | 31.9% | 15 | 15 | 10,203 | 1,592 |
| 50,000 | 224 | 2.74 | 0.17 | 0.117 | 20.7 | 25.9% | 74 | 75 | 50,451 | 1,583 |
| 100,000 | 316 | 9.90 | 0.62 | 0.205 | 48.6 | 21.4% | 148 | 151 | 100,635 | 1,581 |
| 250,000 | 500 | 24.79 | 1.10 | 0.524 | 74.7 | 12.0% | 370 | 377 | 251,017 | 1,579 |

The point of the whole exercise is the gap between the second and third columns:
at 250,000 vectors a restart costs **1.1 seconds to reload** against **24.8
seconds to rebuild**, a factor of 23, and the gap widens with the corpus because
building is dominated by a training pass that loading does not repeat.

Persisted size tracks resident size almost exactly - 1,579 bytes per vector at
384 dimensions against 1,549 in memory - because a chunk stores the vector and
its key and nothing else. The record count is the corpus divided by the chunk
size, plus one commit record per partition, so no single record grows with the
corpus.

### Lazy load

A lazy open reads only the centroids, then fetches a cell the first time a query
actually probes it. At 250,000 vectors that is **0.52 s to open** and **75 ms for
the first query**, after which the box holds **12% of the corpus** and every
subsequent query over the same cells needs no store access at all. The resident
fraction falls as the corpus grows because the probe count grows with the square
root of the partition count rather than in proportion to it.

The answer is identical to the fully resident index, not merely close to it:
a query is scored against exactly the cells it selects, so fetching only those
cannot change the ranking. `DurableVectorIndexLazyLoadTests` asserts equality
across a sweep of queries.

### Incremental maintenance

Recall is measured against brute force over the corpus **as it stands after the
churn**, so a partitioning that had stopped describing its own data would show up
rather than being masked by the corpus it was trained on.

| workload | recall@10 floor | measured |
|---|---|---|
| Clustered, after 20% re-embedded, 10% retired, 10% added | 0.95 | **0.9975** |
| Unclustered, same workload | 0.55 | **0.7875** |

The cost of a flush is proportional to what moved, not to the corpus. Across 45
partitions a flush with nothing dirty costs **1 write** (the manifest alone) and
a flush after a single update costs **7**.

### Distribution drift, and why it needs a signal rather than an error

Incremental maintenance keeps the index *correct* indefinitely: every vector sits
in the cell nearest to it among the trained centroids, and nothing stale is ever
returned. What it cannot do is keep the cells *descriptive* once the corpus moves
away from the distribution they were trained on. Replacing a fifth of a 3,000
vector corpus with vectors drawn around an entirely different set of cluster
centres drops recall@10 from 1.000 to **0.875** - with every individual record
still perfectly valid, which is exactly why the loss is quiet.

`RetrainAsync` is the repair, and it restores recall to **1.000**. It re-reads
nothing, because the corpus is already resident, so it costs a training pass and
one rewrite rather than a pass over the store of record:

| vectors | partitions | retrain s | flush after 100 scattered updates ms | writes |
|---|---|---|---|---|
| 50,000 | 224 | 2.50 | 510 | 214 |
| 250,000 | 500 | 25.34 | 4,259 | 238 |

The second column of that table is the honest cost of scattered updates: the unit
of persistence is one cell, so 100 updates landing in 100 different cells rewrite
100 cells. A maintenance loop that batches its updates before flushing pays for
the distinct cells it touched, not for the updates it applied.

`UpdatesSinceTraining` is the signal a host watches to decide when to retrain; a
quarter of the corpus is a reasonable threshold.

### Allocation

Asserted by `Persistence/DurableVectorIndexAllocationTests`, which follows the
three-part probe rule this epic settled after three separate false negatives, all
of which look exactly like a passing test:

1. **Differential, never absolute.** Each path runs at two loop sizes after a
   **full-size** warm-up - the largest window that will be measured - and the
   assertion is on the growth, so a one-off tiered-JIT or pool-priming cost lands
   in both samples and cancels.
2. **The battery test's allocation provably escapes.** It stores `new object()`
   into a **static field**. That escape is load-bearing: substituting the
   non-escaping `new long[1].Length` form makes the JIT elide the allocation
   entirely and the battery test then reports zero, becoming the false negative it
   exists to prevent. Verified by doing exactly that and watching it fail.
3. **No short circuit on one sample.** Every measurement is repeated and the
   **minimum** is kept, clamped at zero. A single noisy attempt where the small
   window absorbed more noise than the large one would otherwise report a
   genuinely allocating loop as allocation-free.

The per-thread counter is used only on paths that never await, where it excludes
unrelated threads' noise and makes the differential tighter; everything
asynchronous uses `GC.GetTotalAllocatedBytes(precise: true)`, because a
continuation may resume on another thread and the per-thread figure is then not
merely noisy but wrong.

Every figure below was produced twice, under default tiering and under
`DOTNET_TieredCompilation=0` (which forces full optimisation from the first call,
where escape analysis is most aggressive), with identical results.

| path | allocated |
|---|---|
| Search, 2,000 queries | **0 bytes** |
| Resolving a result identifier, 2,000 calls | **0 bytes** |
| Looking up a key by identifier, 2,000 calls | **0 bytes** |
| Re-embedding a known identifier, 1,000 updates | **0 bytes** |
| A flush with nothing dirty | 1,176 bytes per flush, 2 KB budget |
| A warm lazy search | **0 bytes** measured, 64 byte budget |
| Loading a 2,000 vector index | under 6x the cells it retains |

The re-embed figure is the one that matters for a maintenance loop: an identifier
that is already mapped needs neither a store round trip nor a dictionary
insertion, so the update is a synchronous span copy and costs nothing at all.
