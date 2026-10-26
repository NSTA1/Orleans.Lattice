# Atomic writes - brief

- **Path:** Build, fourth episode (B4), after Scans, filters and cursors.
- **Audience:** a developer making related changes to keys in one or more `ILattice` trees.
- **Length:** two to five minutes, with the preceding episode's reduced narration pace.
- **The one idea:** commit related key changes all-or-nothing, within one tree or across several trees.
- **It introduces:** [Atomic writes](../../../docs/lattice/atomic-writes.md).
- **Afterwards the viewer can** distinguish the non-atomic batch from an all-or-nothing write, choose the single-tree or cross-tree surface, and avoid treating atomic writes as a cross-tree read snapshot.

## Beats

1. Recap that Scans, filters and cursors chose what to read; this episode asks what happens when one change touches several keys.
2. Show the partial-write risk of separate writes and distinguish the throughput-oriented non-atomic batch.
3. Explain the single-tree atomic batch: all entries become durable together, or every key retains its pre-batch value.
4. Show the cross-tree builder applying one shared decision to each participating tree's slice, with a stable operation identifier for retry.
5. State the boundary: atomicity is a write guarantee, not a snapshot spanning separate reads across trees.
6. End at TTL and soft delete, and the deep dive Atomic commit without consensus.

## Code on screen

One compiled cross-tree builder example: delete an order from one tree and set it in another in the same atomic write.

## Sources

- [Atomic writes](../../../docs/lattice/atomic-writes.md): opening contract, "Atomicity Guarantees", "What `SetManyAtomicAsync` does not guarantee", and "Cross-tree (multi-tree) atomic writes".
- [Series plan](../../series.md): B4's idea, introduced page, and ending printed by `npm run series -- ending B4`.

## Not in this episode

- The saga's prepare, commit, abort, replication, and crash-recovery protocol; that is the subject of Atomic commit without consensus.
- A cross-tree read snapshot or global ordering across distinct sagas.
- Guard predicates, staged CRDT mutations, backups, performance figures, or implementation internals beyond the single decision that explains the visibility boundary.
