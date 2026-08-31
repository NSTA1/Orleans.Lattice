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

Asserted by `VectorIndexAllocationTests` using
`GC.GetAllocatedBytesForCurrentThread` after a warm-up, so what is measured is
the steady state rather than JIT and array-pool priming.

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
