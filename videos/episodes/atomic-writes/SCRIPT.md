# Atomic writes - script

Status: revised cues fact-checked on 2026-10-26 by the Docs agent against
`docs/lattice/atomic-writes.md` and `docs/lattice/snapshot-cursors.md`. The
timeout cue specifies caller-side uncertainty, and the boundary cue names a
snapshot entry cursor for a consistent single-tree view.

Written form, in the series voice (Emma, British English). The code is on
screen, so the narration explains the promise rather than reading method names.
This episode keeps the preceding episode's reduced pace of 0.92.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds silence before the next cue or after the
last one.

## Narration

<!-- pace 0.92 -->

### Atomic writes

Scans, filters and cursors showed how to choose what to read. Here, we ask what happens when one change touches several keys.

This is Atomic writes: related changes become visible as a whole, or not at all.

### The partial-write problem

Suppose an order changes two keys: its status and its tracking reference. They belong together; once one is live, the other must be too.

Two ordinary writes are two chances to stop. If the second fails, the first may already be visible.

A non-atomic batch remains useful when throughput matters and partial application is acceptable. It does not make those writes all-or-nothing.

### One batch, one decision

For a batch in one tree, use the atomic write surface.

On success, the whole batch commits. If the saga aborts, none of its writes become visible. A caller-side timeout leaves the outcome unknown.

Readers do not see half of this batch. Its prepared writes stay hidden until one commit decision makes them visible together.

An abort drops those hidden writes; it does not need to write the old values back.

### Across trees

Sometimes one change spans distinct trees: an order leaves one tree and appears in another.

The single-tree call cannot cover both. The cross-tree builder stages each tree's part under one stable operation identifier.

One shared decision applies to every participating tree: commit them all, or commit none.

### The boundary

That is atomicity for the write, not a snapshot read across trees.

Two separate reads at different moments can straddle the decision: one may return the old state, and the next the new state. For a consistent view of one tree, use a snapshot entry cursor; every page reflects the tree state captured at open.

<!-- pause 0.8 -->

### Where next

Next on Build: TTL and soft delete. The deep dive on how this works is Atomic commit without consensus. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

| Narration | Source |
| --- | --- |
| Scans, filters and cursors showed how to choose what to read; this episode asks what happens when one change touches several keys | `videos/series.md`, B3 and B4 ideas |
| This is Atomic writes: related changes become visible as a whole, or not at all | `videos/series.md`, B4 title and idea; `docs/lattice/atomic-writes.md`, "Atomicity Guarantees" |
| Two ordinary writes may leave the first visible if the second fails; a non-atomic batch remains available when partial application is acceptable | `docs/lattice/atomic-writes.md`, opening contract; `src/lattice/BPlusTree/ILattice.cs`, `SetManyAsync` |
| A successful return means the whole batch commits; an abort exposes none of its prepared writes; a caller-side timeout leaves the outcome unknown | `docs/lattice/atomic-writes.md`, "Atomicity Guarantees" and "Batches whose caller was answered" |
| Prepared writes stay hidden until one commit decision makes them visible; abort drops them without per-key rollback writes | `docs/lattice/atomic-writes.md`, opening contract and "Atomicity Guarantees", items 1 and 2 |
| The single-tree call is bound to one tree; the cross-tree builder stages a slice per tree and requires a stable operation identifier | `docs/lattice/atomic-writes.md`, "What `SetManyAtomicAsync` does not guarantee" and "Cross-tree (multi-tree) atomic writes", "Public surface" |
| One cross-tree decision commits every participating tree or none | `docs/lattice/atomic-writes.md`, "Cross-tree (multi-tree) atomic writes" and "Guarantees and non-guarantees" |
| Atomicity is not a snapshot read across trees; separate reads at different moments can straddle the decision; a snapshot entry cursor gives a consistent view of one tree at open time | `docs/lattice/atomic-writes.md`, "Guarantees and non-guarantees", "Atomicity, not cross-tree read isolation"; `docs/lattice/snapshot-cursors.md`, "When to use a snapshot cursor" and opening contract |
| Next on Build: TTL and soft delete; the deep dive is Atomic commit without consensus; the links are on the episode page | `npm run series -- ending B4`, word for word |
