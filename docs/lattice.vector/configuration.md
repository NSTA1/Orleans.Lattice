# Configuration

Every option in `Orleans.Lattice.Vector`, what it does, and when to change it.

## Index options (`VectorIndexOptions`)

These shape the in-memory core and are fixed for the life of an index. A persisted
index records its dimensionality, metric, trained partition and probe counts, and
seed in its header, and restoring rejects a header that contradicts the options
it is restored with on dimensionality or metric.

| Option | Default | What it does |
|---|---|---|
| `Dimensions` | *(required)* | The vector width. Every vector added must match it. |
| `Metric` | `Cosine` | `Cosine` or `DotProduct`. Cosine needs no pre-normalised input: the index caches each vector's norm and computes the query's norm once, so cosine costs one dot product per candidate. Do not normalise on the way in on the index's account. |
| `PartitionCount` | `0` (auto) | How many cells to partition into. `0` derives it from the corpus size, which is what you want unless you are reproducing a specific measurement. |
| `Probes` | `0` (auto) | How many partitions a query scores. `0` derives it. **See the warning below before setting this.** |
| `Seed` | a fixed constant | Seeds the deterministic generator. Exposed so a build can be reproduced bit-for-bit on any machine and any runtime. |
| `TrainingSampleSize` | `32768` | Caps how many vectors the k-means pass samples, which is what stops build cost scaling with the corpus. |
| `MaxTrainingIterations` | `10` | Bounds the k-means pass. |
| `MinimumTrainingCount` | `1024` | Below this the index does not partition at all and answers exactly by exhaustive scan. That is correct behaviour for a small corpus, not a failure. |

`MaximumPartitionCount` (16,384) is a public constant bounding the partition
count that `AutoPartitionCount` derives (`round(sqrt(n))`, clamped to
`[1, MaximumPartitionCount]`); an explicit `PartitionCount` is not clamped
against it, only against the live vector count, and an explicit `Probes` is
capped at the trained partition count. `AutoPartitionCount(int)` and
`AutoProbes(int)` expose the derivations so a caller can predict them.

The setters validate on assignment: `Dimensions`, `TrainingSampleSize`,
`MaxTrainingIterations`, and `MinimumTrainingCount` reject a value that is not
positive, and `PartitionCount` and `Probes` reject a negative one, each with
`ArgumentOutOfRangeException`. `Validate()`, which the `VectorIndex` constructor
runs, additionally rejects an options instance whose `Dimensions` was never set
or whose `Metric` is not a defined member. The constructor copies the options, so
changing the instance afterwards does not affect the index.

### Do not set `Probes` to a fraction of `PartitionCount`

This is the one setting that can silently undo the package's entire purpose.

Total query cost is `C + probes * (n / C)`. With `C = sqrt(n)`, a `probes` term
proportional to `C` puts `n` straight back into the second term - the index
becomes linear in the corpus again while still reporting `Approximate` and still
returning good recall. It looks like it is working.

The default derivation is `clamp(2 * ceil(sqrt(partitionCount)), 8, partitionCount)`,
which makes the fraction of the corpus scanned *fall* as the corpus grows: about
25% at 5,000 vectors, about 6% at 1,000,000.

If you need higher recall, raise `Probes` to a **fixed number**, measure both the
recall and the scanned fraction at your largest expected corpus size, and confirm
the scanned fraction still falls as the corpus grows. Do not express it as a
proportion of `PartitionCount`.

## Durable options (`DurableVectorIndexOptions`)

These shape persistence and maintenance.

| Option | Default | What it does |
|---|---|---|
| `Index` | a new `VectorIndexOptions` | The core options above. |
| `KeyPrefix` | `vidx/` | The key prefix every durable record lives under. |
| `MaxItemsPerChunk` | `1024` | A ceiling on the centroids or vectors one persisted chunk carries. A chunk is actually written at the largest item count that keeps the record within a fixed 64 KiB byte ceiling, capped by this value, so at typical embedding widths the byte ceiling decides (about 42 vectors per chunk at dimension 384, 21 at 768) and this knob binds only for very narrow vectors (13 dimensions or fewer at the default). Either way, no record grows with the corpus. |
| `IngestBatchSize` | `4096` | How many source vectors one background build step ingests before returning. Bounds the work a single `BuildStepAsync` does. |
| `IngestSliceBudget` | 5 seconds (`DefaultIngestSliceBudget`) | Wall-clock ceiling on one build step: the step checkpoints and returns at the first source item that finds the budget spent, and the budget is also a deadline raced against each source read, so a slow or stalled source cannot hold the step. A non-positive value removes the bound, leaving `IngestBatchSize` as the only one. |
| `TimeProvider` | `TimeProvider.System` | The clock `IngestSliceBudget` is measured against; a test substitutes a fake. Must not be `null`. |
| `KeyReservationBlock` | `1024` | How many identifiers the key dictionary reserves per durable watermark write. A crash burns the remainder of a block rather than reissuing. |

`KeyPrefix` and `TimeProvider` reject `null`, and `MaxItemsPerChunk`,
`IngestBatchSize`, and `KeyReservationBlock` reject a value that is not positive,
each on assignment; `IngestSliceBudget` accepts any value. `Validate()`, which
opening an index runs, requires `Index` to be set and validates it. Opening also
rejects a source whose dimensionality differs from `Index.Dimensions`, and copies
the options, so later changes to the instance have no effect.

### Give the index its own tree, or at least its own prefix

The recovery path **deletes whole key ranges** beneath `KeyPrefix`. That is safe
and correct for a derived projection - discarding index state and recomputing is
always the right answer when it cannot be verified - but it is emphatically not
safe for anything else sharing that prefix. A dedicated tree is the simplest way
to be sure.

### Choosing `MaxItemsPerChunk`

You rarely need to. The item count a chunk is written at is derived from the
index's own dimensionality: the largest count that keeps one record within a
fixed 64 KiB byte ceiling - small enough to stay off the .NET large object heap
and for one write batch to coalesce many records - capped by `MaxItemsPerChunk`.
A wide embedding is therefore bounded by bytes without any tuning, and the knob
only takes effect for very narrow vectors, or when you lower it below the
byte-derived count. Smaller chunks mean more records and more round trips, but
finer-grained lazy loading and smaller rewrites; larger chunks mean the opposite.
The property that matters is that **no record grows with the corpus**, which any
positive value preserves.

## Costs worth knowing when you tune

- **Training is synchronous and expensive** - about 10.7 s for 1,000,000 vectors at
  dimension 384 - and transiently holds two copies of the corpus. It is a build
  step of its own precisely so a host that cannot afford it right now simply does
  not call it. The index answers exhaustively and reports that it is building
  meanwhile.
- **The unit of persistence is one chunk.** A flush revisits only the cells whose
  version stamp moved, and within them rewrites only the chunks whose content
  changed (each rendered chunk is content-hashed against what the store holds),
  so re-embedding one vector rewrites a few chunks rather than its cell. A flush
  with nothing dirty costs a single write, the manifest. Every touched cell still
  costs its own commit record, so 100 updates landing in 100 different cells
  rewrite roughly 100 chunks plus 100 commit records. Batch before flushing, and
  you pay for the distinct chunks and cells you touched rather than for the
  updates you applied.
- **`EnsureCapacity` before a bulk load** makes the insert run allocate nothing.
- **Memory is `dimensions * 4 + 12` bytes per vector, plus the centroid block** -
  about 1,549 bytes per vector measured at dimension 384 and 1,000,000 vectors
  (the centroid block amortises away as the corpus grows, so a smaller corpus
  measures slightly higher: 1,563 bytes at 10,000). That is roughly 1.5 GB at
  1,000,000 vectors. Persisted size tracks resident size closely, because a chunk
  stores the vector and its key and nothing else.

## When to retrain

Incremental maintenance keeps the index correct indefinitely, but cannot keep the
cells descriptive once the corpus moves away from the distribution they were
trained on. The index exposes an update counter since the last training pass as
the drift signal, and `RetrainAsync` as the repair; retraining re-reads nothing,
because the corpus is already resident.

See [Recall and accuracy](recall.md) for the measured effect: recall fell to 0.875
under a corpus-shift workload and returned to 1.000 after retraining.
