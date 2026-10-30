# The guarantees - brief

- **Path:** Evaluate, second. It follows *When Lattice fits, and when it
  doesn't* and leads to *The evidence* and the deep dive *Clocks and version
  vectors*.
- **Audience:** architects and tech leads evaluating the caller-visible
  contract of `ILattice`, including what a caller can rely on during topology
  changes, concurrent operations and silo recovery.
- **Length:** two to five minutes.
- **The one idea:** consistency is operation-specific: point reads, writes,
  batches, scans and snapshots each have a stated guarantee, and recovery
  across a crash depends on the durable storage provider.
- **Page it introduces:** [Consistency guarantees](../../../docs/lattice/consistency.md).
- **Afterwards the viewer can** distinguish linearizable, strongly consistent,
  snapshot and eventually consistent operations, and explain the limits of
  atomic batches and the WAL's role in recovery.

## Beats

1. Recap that *When Lattice fits* set the boundaries; this episode makes the
   caller-visible promises precise.
2. Define the four consistency levels in the consistency contract, and note
   that it classifies listed operations rather than every `ILattice` method.
3. Compare point reads under the default and a positive cache TTL, then place
   a single-key write at the WAL commit boundary.
4. Contrast a non-atomic batch with `SetManyAtomicAsync`: per-key ordering
   versus all-or-nothing, atomic visibility.
5. Distinguish an uninterrupted strongly consistent scan from an online
   snapshot and a strict snapshot cursor.
6. Show what survives a silo restart: the WAL provider determines storage
   durability, while an atomic saga resumes to a terminal outcome.
7. End with the plan's next Evaluate episode and its deep dive.

## Sources

- [Consistency guarantees](../../../docs/lattice/consistency.md): consistency
  levels, single-key and batch operations, enumeration, snapshot cursors,
  cache staleness and cancellation.
- [Atomic writes](../../../docs/lattice/atomic-writes.md): all-or-nothing
  semantics, atomic visibility and crash recovery.
- [Write-ahead log](../../../docs/lattice/wal.md): append as the foreground
  commit point and recovery from the log.
- [WAL storage providers](../../../docs/lattice/wal-storage-providers.md):
  the in-memory default and durable provider options.
- [Series plan](../../series.md): E2's idea and ending.

## Not in this episode

- The implementation details of how each guarantee is delivered.
- A complete inventory of every `ILattice` method; the consistency guide
  explicitly limits its classifications to the operations it lists.
- Performance comparisons, release versions or guarantees not stated in the
  cited documentation.
