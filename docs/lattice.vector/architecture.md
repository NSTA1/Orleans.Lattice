# Architecture

How `Orleans.Lattice.Vector` is built, why the index structure was chosen, and
how the durable form is laid out on a Lattice tree.

## The index structure: inverted file (IVF)

The core is an **inverted file** index. A seeded k-means pass partitions the
corpus into roughly `sqrt(n)` cells, each with a centroid. A query ranks the
centroids, then scores only the vectors in the cells it probes.

A graph-based structure (HNSW and relatives) was the main alternative. IVF was
chosen against five criteria:

| Criterion | IVF as built | Graph alternative |
|---|---|---|
| Build cost | Bounded k-means over a capped sample, so build cost stops scaling with the corpus past the cap. About 10.7 s for 1,000,000 vectors at dimension 384. | Incremental graph construction, materially heavier and not capped. |
| Query cost | Sub-linear: `C` centroid comparisons plus `probes * (n / C)`. Measured speedup over exhaustive grows from about 3x at 10,000 to about 14x at 1,000,000. | Also sub-linear, typically with a better constant factor. |
| Memory per vector | `dimensions * 4 + 12` bytes. No per-vector object header and no adjacency structure. | Adds a whole adjacency graph per vector. |
| Incremental insert | Assign to the nearest centroid and append. No retrain needed for correctness. | Supported, but each insert mutates shared graph structure. |
| Delete | First class and constant time in the corpus size: the cell's hole is backfilled with its last member, which copies one vector's worth of floats regardless of how many vectors the index holds. Never a tombstone, so a deleted vector cannot resurface. | The known weak point, usually tombstones plus periodic rebuild. |

**The criterion that actually decided it is durability.** An IVF cell is a natural
bounded chunk, and a query provably touches only the cells it probes, so a partial
load is not merely possible - it is the normal mode. A graph traversal hops
arbitrarily across the structure, so there is no bounded subset provably
sufficient to answer a query, and the durable layer would be forced to hold the
entire graph resident. Since the whole point of the package is to make a restart
cheap, the structure was chosen for the seam rather than for the best possible
constant factor.

## Two properties that are load-bearing

These are not tuning choices. Changing either silently destroys the package's
reason to exist.

### Cells own their vectors contiguously

The first implementation used posting lists of slot identifiers into one global
vector block - the textbook layout. Measured during development it reached only
about 2.6x speedup at 1,000,000 vectors and was *slower than exhaustive* at
50,000, because the probe scan is cache-hostile: every scored vector is an
indirection into a different part of a large array. (That figure describes a
superseded implementation, so it is not reproducible from the committed harness;
the qualitative point is what matters.)

Rewriting so that each cell owns its members' vectors **contiguously in its own
block** took the same algorithm to about 14x. Do not reintroduce an indirection
here.

### The probe count must not be a fixed fraction of the partitions

Total query cost is `C + probes * (n / C)`. With `C = sqrt(n)`, a `probes` term
proportional to `C` puts `n` straight back into the second term, and the index
becomes linear in the corpus again while still looking like an approximate index.

The default is therefore `clamp(2 * ceil(sqrt(partitionCount)), 8, partitionCount)`,
which makes the fraction of the corpus scanned *fall* as the corpus grows: about
25% at 5,000 vectors, about 6% at 1,000,000. The recall harness reports that
scanned fraction at each corpus size, so the property is visible in the committed
evidence. A future "probe more for better recall" change must not turn this back
into a fraction.

## Determinism

The same corpus and configuration always produce the same result set, so
downstream suites are not flaky. Three mechanisms deliver that:

- Results are totally ordered by descending score, then ascending key.
- Training collects the live set **sorted by key** before sampling, so sampling
  and centroid seeding depend on contents rather than on insertion history.
- The k-means mean recomputation is deliberately **serial in ascending order**
  (only the pure per-vector nearest-centroid search is parallelised), so
  floating-point addition order is fixed.

Randomness is explicit: `VectorIndexOptions.Seed` is public, is surfaced again on
the index and in every snapshot header, and the generator is a hand-rolled seeded
xorshift128+ rather than `System.Random`, whose algorithm is an implementation
detail that may change between runtimes. An index must reproduce bit-for-bit from
its seed on any machine and any release.

## The durable layer

`DurableVectorIndex` persists the core on a Lattice tree. Everything lives under a
caller-chosen key prefix.

### Two counters

- A **generation** covers a whole partitioning. Training and rebuilding change
  every cell's membership, so they write a fresh generation and flip the manifest
  to it rather than editing the live one.
- An **epoch** covers one flush inside a generation. A dirty partition's chunks
  are written under a new epoch and committed by rewriting that partition's state
  record, so an interrupted flush leaves an uncommitted epoch the loader ignores
  and the next flush sweeps.

Both are zero-padded so ordinal key order is numeric order, and neither is ever
reused.

### Write order is the durability mechanism

No multi-key atomicity is required from Lattice. Records are written in a fixed
order - content records first, then the per-partition commit record, then the
manifest last - and a loader reads **exactly the chunk count a partition's commit
record claims**, addressed by key rather than discovered by scanning. A chunk
written but not committed is therefore simply never read.

Every record - manifest, chunk, partition state, build state, identifier mapping -
is wrapped in one 24-byte envelope carrying a marker, the layout version, the
payload length, and a checksum. Truncation, a flipped bit, a wrong key and a
future version all collapse to the same answer: the unwrap fails. There is exactly
one place that decides whether a persisted byte sequence may be believed.

No record grows with the corpus: chunk size is bounded by configuration, and a
test asserts every persisted record stays under the bound that implies.

### Lazy partial load

Opening an index applies only the centroid chunks, which are small
(`partitionCount * dimensions` floats). A query then selects the partitions it
would probe and fetches only those vector chunks. The answer is identical to the
fully resident index - asserted across a query sweep - because a query is scored
against exactly the cells it selects, and because a chunk is a slice of one
contiguous cell rather than a gather across the corpus.

At 250,000 vectors this is about 0.52 s to open and about 75 ms for the first
query, after which roughly 12% of the corpus is resident and repeated queries over
the same cells touch the store not at all. The box warms as it serves.

### Incremental persistence

Each partition carries a version stamp that advances whenever a vector enters or
leaves it, and the index carries an overall version that advances on every
mutation. A flush persists only the partitions whose stamp moved. Rendering a
chunk fails if the index has moved since the snapshot was planned, so a torn
snapshot cannot be written.

The unit of persistence is one cell. A flush with nothing dirty costs a single
write (the manifest); a flush after one update costs a handful. But 100 updates
landing in 100 different cells rewrite 100 cells, so a maintenance loop that
batches before flushing pays for the distinct cells it touched rather than for the
updates it applied.

## Coherence with the store of record

The index is a **derived projection**, never authoritative. That asymmetry is what
makes its recovery story simple: every inconsistency is settled by discarding index
state and recomputing, and nothing in the durable layer ever writes to a store of
record.

The rules it enforces:

- **No ghosts.** A retired vector never appears again, including across a restart
  that interrupted the deletion. A durable tombstone is written *before* the
  in-memory removal and dropped only once that removal is durable, and the journal
  is replayed against every cell a lazy reader fetches later, not only at load.
- **Lag only in the missing direction.** The index is never ahead of the source,
  and outstanding work is reported rather than hidden.
- **Verified load, no middle path.** The manifest, every checksum, every
  partition's chunk set and the declared count must all agree, or the index is
  rebuilt and never partly served.
- **A reader must not repair what it cannot maintain.** A lazily loaded handle
  that finds unverifiable state refuses to serve it and leaves the store alone,
  rather than discarding an index a writer elsewhere may be building.

A source-side deletion the index was never told about is explicitly *not* covered
by these rules; a bounded reconciliation sweep exists for that, and always settles
in the source's favour.
