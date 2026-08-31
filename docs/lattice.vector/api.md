# API

The public surface of `Orleans.Lattice.Vector`, in the order you meet it.

Namespaces: `Orleans.Lattice.Vector` (the in-memory core, no Orleans dependency)
and `Orleans.Lattice.Vector.Persistence` (the durable layer).

## The in-memory core

### `VectorIndex`

The index itself. Constructed from `VectorIndexOptions`.

**Shape and state**

| Member | Purpose |
|---|---|
| `Dimensions`, `Metric`, `Seed` | The immutable configuration the index was built with. |
| `Count`, `Capacity` | Live vectors, and reserved slots. |
| `PartitionCount`, `Probes` | The partitioning, and how many partitions a query probes. |
| `Version`, `PartitionVersion(int)` | Mutation stamps. The per-partition stamp advances whenever a vector enters or leaves that partition, so a durable layer can persist only what moved. |
| `PartitionSize(int)` | How many vectors a partition currently holds. |
| `State`, `IsReady`, `Status` | `Empty`, `Building` or `Ready`, and a `VectorIndexStatus` snapshot. |
| `CentroidsComplete` | Whether a restore has finished streaming its centroids. |

**Mutation**

| Member | Purpose |
|---|---|
| `Add(long, ReadOnlySpan<float>)` | Insert a vector under a key that is not already present. |
| `Upsert(long, ReadOnlySpan<float>)` | Insert or replace. |
| `Remove(long)` | Retire a vector. Constant time in the corpus size (one backfill, whatever the corpus holds); never a tombstone. |
| `Contains(long)`, `TryGetVector(long, Span<float>)` | Presence and retrieval. |
| `Clear()` | Drop everything. |
| `EnsureCapacity(int)` | Reserve ahead of a bulk load, which makes the insert run allocation-free. |
| `Train()` | Partition the corpus. Synchronous and expensive; keep it off the request path. |

**Query**

| Member | Purpose |
|---|---|
| `Search(query, Span<VectorSearchResult>)` | Top-k into a caller-owned span. |
| `Search(query, results, out VectorSearchMode)` | The same, plus how the answer was produced. **Prefer this overload**: it is the per-response honesty signal. |
| `SelectPartitions(query, Span<int>)` | Which partitions this query would probe, without scoring anything. The durable layer uses it to fetch only the chunks a query needs. |

**Persistence seam**

| Member | Purpose |
|---|---|
| `CreateSnapshot(int maxItemsPerChunk)` | Plan a chunked snapshot. Nothing is copied yet. |
| `Restore(VectorIndexHeader, VectorIndexOptions)` | Static. Create an index shaped by a persisted header. |
| `ApplyChunk(ReadOnlySpan<byte>)` | Apply one chunk. Order-independent and idempotent. |

### Value types and enums

| Type | Purpose |
|---|---|
| `VectorSearchResult(long Key, float Score)` | One hit. |
| `VectorIndexStatus` | A snapshot of state, counts, partitioning and `BytesPerVector`. |
| `VectorIndexSnapshot` | A snapshot plan: `Header`, `ChunkCount`, `Describe(i)`, `MeasureChunk(i)`, `WriteChunk(i, Span<byte>)`. |
| `VectorIndexHeader` | The 56-byte durable header. `Write`, `Read`, and `TryRead` (which returns `false` rather than throwing on an unreadable version). |
| `VectorIndexChunkDescriptor` | Kind, partition, sequence, item count and byte count for one chunk, without rendering it. |
| `VectorDistanceMetric` | `Cosine` or `DotProduct`. |
| `VectorIndexState` | `Empty`, `Building`, `Ready`. |
| `VectorSearchMode` | `Exhaustive` or `Approximate`. |
| `VectorIndexChunkKind` | `Centroids` or `Vectors`. |
| `VectorIndexFormat` | Format constants and `IsSupported`. |
| `VectorIndexFormatException` | Thrown when a persisted form cannot be believed. |
| `VectorSimilarity` | `Dot`, `Norm`, `Cosine`, `Scale`, `Normalize`, vectorised. |
| `VectorIndexMemory` | Per-slot and total byte accounting. |

## The durable layer

### `DurableVectorIndex`

Orchestrates persistence and incremental maintenance over a `VectorIndex`.

There is no public constructor. `DurableVectorIndex.OpenAsync(store, source, options, loadMode, cancellationToken)`
is the only entry point; `loadMode` selects a full load or a lazy partial one.

| Member | Purpose |
|---|---|
| `OpenAsync` | Static. Opens (and, for a full load, restores) an index over a store and a source. |
| `KeyPrefix`, `Generation`, `LoadMode` | Where the index lives, which partitioning is live, and how it was opened. |
| `Status`, `Count`, `UpdatesSinceTraining` | The core's status, the live vector count, and the drift signal that tells you when to retrain. |
| `Progress` | A `VectorIndexBuildProgress`: phase, generation, vectors indexed and expected, partitions persisted and total, whether the state was restored rather than recomputed, plus `IsReady` and `IngestedFraction`. `IngestedFraction` reports `1` when the build is ready *or* when the expected count is unknown, deliberately, so a caller never renders a progress bar implying knowledge the index does not have. |
| `BuildStepAsync` | Does one bounded slice of build work and returns progress. |
| `RunBuildAsync` | Loops `BuildStepAsync` to completion. |
| `UpsertAsync`, `RemoveAsync` | Incremental maintenance. |
| `TryGetId`, `TryGetKey` | Resolve between an external string identifier and the index's `long` key. |
| `FlushAsync` | Persist the partitions whose stamps moved. |
| `Search` | Synchronous and allocation-free, into a caller-owned span. Returns the number of hits written and reports the path through an `out` parameter. Under a lazy load it answers from whatever cells are already resident. |
| `SearchAsync` | Query, returning a `VectorSearchOutcome`. Under a lazy load it fetches any cell the query would probe. |
| `ReconcileAsync` | Bounded sweep against the store of record, always settling in the source's favour. |
| `RebuildAsync`, `RetrainAsync` | Full rebuild, and re-partition in place after distribution drift. |

**The build is a caller-driven pump, not a thread.** `BuildStepAsync` does one
bounded slice and returns; `RunBuildAsync` loops it. This honours the core's
single-writer constraint without fencing, lets the host decide when it can afford
the work, and makes resumability deterministic.

### Two readiness signals, deliberately distinct

Do not conflate these:

- `Progress.Phase == Ready` - the **build** has finished.
- `Status.State == Ready` - a usable **partitioning** exists.

A corpus below the training minimum legitimately finishes its build with no
partitioning and answers exactly by exhaustive scan. Reporting that as not-ready
would be wrong; reporting it as approximate would also be wrong.

### Supporting types

| Type | Purpose |
|---|---|
| `IVectorIndexStore` | The narrow async store seam: read, read-many, write, delete, scan by prefix, delete by prefix. |
| `LatticeVectorIndexStore` | The `ILattice` adapter. The only type in the package that binds to Orleans. |
| `IVectorSource`, `VectorSourceEntry` | The store-of-record seam the background build streams from. |
| `VectorKeyDictionary` | The durable string-to-`long` identifier mapping. A monotonic allocator, never a hash. |
| `VectorIndexBuildPhase` | `NotStarted`, `Ingesting`, `Training`, `Persisting`, `Ready`. Monotonic. |
| `VectorIndexBuildState`, `VectorIndexManifest`, `VectorIndexPartitionState` | The durable build checkpoint, the commit record, and per-partition commit state. |
| `VectorIndexStorageKeys`, `VectorIndexPersistenceFormat`, `VectorIndexRecord` | The key layout, the framing constants, and the checksummed record envelope. |
| `VectorIndexLoadMode` | Full load versus lazy partial load. |
| `VectorSearchOutcome` | A search result set plus the mode that produced it. |
| `DurableVectorIndexOptions` | Durable-layer configuration; see [Configuration](configuration.md). |

## Identifier mapping

Index keys are `long`; most consumers have string identifiers.
`VectorKeyDictionary` owns that mapping and is **not** a hash at any width,
because a collision returns the wrong record silently and undiagnosably. It is a
durable monotonic allocator: the watermark is made durable before any identifier
in a block is handed out, so a crash burns the remainder of a block rather than
reissuing. Keys are never recycled, and a rebuild does not rewind the counter.

Only the forward direction is persisted; the reverse map is rebuilt in memory from
the same scan, so resolving a result costs no round trip and no allocation.
