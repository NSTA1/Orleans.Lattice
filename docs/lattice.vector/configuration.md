# Configuration

Every option in `Orleans.Lattice.Vector`, what it does, and when to change it.

## Index options (`VectorIndexOptions`)

These shape the in-memory core and are fixed for the life of an index. A persisted
index records them in its header, and restoring rejects a header that contradicts
the options it is restored with on dimensionality or metric.

| Option | Default | What it does |
|---|---|---|
| `Dimensions` | *(required)* | The vector width. Every vector added must match it. |
| `Metric` | `Cosine` | `Cosine` or `DotProduct`. Cosine needs no pre-normalised input: the index caches each vector's norm and computes the query's norm once, so cosine costs one dot product per candidate. Do not normalise on the way in on the index's account. |
| `PartitionCount` | `0` (auto) | How many cells to partition into. `0` derives it from the corpus size, which is what you want unless you are reproducing a specific measurement. |
| `Probes` | `0` (auto) | How many partitions a query scores. `0` derives it. **See the warning below before setting this.** |
| `Seed` | a fixed constant | Seeds the deterministic generator. Exposed so a build can be reproduced bit-for-bit on any machine and any runtime. |
| `TrainingSampleSize` | tuned | Caps how many vectors the k-means pass samples, which is what stops build cost scaling with the corpus. |
| `MaxTrainingIterations` | tuned | Bounds the k-means pass. |
| `MinimumTrainingCount` | tuned | Below this the index does not partition at all and answers exactly by exhaustive scan. That is correct behaviour for a small corpus, not a failure. |

`MaximumPartitionCount` is a public constant bounding `PartitionCount`.
`AutoPartitionCount(int)` and `AutoProbes(int)` expose the derivations so a caller
can predict them.

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
| `MaxItemsPerChunk` | `1024` | Caps items per persisted chunk, which is what keeps records bounded regardless of corpus size. |

### Give the index its own tree, or at least its own prefix

The recovery path **deletes whole key ranges** beneath `KeyPrefix`. That is safe
and correct for a derived projection - discarding index state and recomputing is
always the right answer when it cannot be verified - but it is emphatically not
safe for anything else sharing that prefix. A dedicated tree is the simplest way
to be sure.

### Choosing `MaxItemsPerChunk`

Smaller chunks mean more records and more round trips, but finer-grained lazy
loading and smaller rewrites. Larger chunks mean the opposite. The default suits a
few-hundred-to-few-thousand-dimension corpus; the property that matters is that
**no record grows with the corpus**, which any positive value preserves.

## Costs worth knowing when you tune

- **Training is synchronous and expensive** - about 10.7 s for 1,000,000 vectors at
  dimension 384 - and transiently holds two copies of the corpus. It is a build
  step of its own precisely so a host that cannot afford it right now simply does
  not call it. The index answers exhaustively and reports that it is building
  meanwhile.
- **The unit of persistence is one cell.** A flush with nothing dirty costs a
  single write; a flush after one update costs a handful. But 100 updates landing
  in 100 different cells rewrite 100 cells. Batch before flushing, and you pay for
  the distinct cells you touched rather than for the updates you applied.
- **`EnsureCapacity` before a bulk load** makes the insert run allocate nothing.
- **Memory is `dimensions * 4 + 12` bytes per vector** - about 1,549 bytes at
  dimension 384, or roughly 1.5 GB at 1,000,000 vectors. Persisted size tracks
  resident size closely, because a chunk stores the vector and its key and nothing
  else.

## When to retrain

Incremental maintenance keeps the index correct indefinitely, but cannot keep the
cells descriptive once the corpus moves away from the distribution they were
trained on. The index exposes an update counter since the last training pass as
the drift signal, and `RetrainAsync` as the repair; retraining re-reads nothing,
because the corpus is already resident.

See [Recall and accuracy](recall.md) for the measured effect: recall fell to about
0.83 under a corpus-shift workload and returned to 1.000 after retraining.
