# Conflict-free merges in 90 seconds - brief

- **Path:** How it works, a standalone deep dive reached from Values that merge.
- **Audience:** developers who know that a typed CRDT chooses how concurrent
  writes merge, and want to see one merge in detail.
- **Length:** ninety seconds, one idea.
- **The one idea:** a G-Set merges concurrent additions with set union, so both
  clusters converge on the same set regardless of delivery order.
- **Introduces:** [CRDT primitives](../../../docs/crdt/readme.md) and
  [state primitives](../../../docs/lattice/state-primitives.md).
- **Afterwards the viewer can** explain why union keeps both concurrent adds,
  why repeating an add is harmless, and when a grow-only set fits.

## Beats

1. Recap Values that merge: a typed primitive gives concurrent writes a merge
   rule.
2. Contrast a plain last-writer-wins write, which silently replaces the other
   concurrent value.
3. Start with an empty G-Set, which only grows and has no remove operation.
4. Cluster A adds `alice` while cluster B concurrently adds `bob`.
5. Their states are incomparable; merging takes the union and keeps both adds.
6. Deliver `bob`'s add again; the set does not change.
7. Name the algebraic properties and the append-only use case, then hand back
   to Build.

## Sources

- [CRDT primitives](../../../docs/crdt/readme.md): conflict-free merge rules,
  convergence, and choosing a primitive whose resolution matches the data.
- [G-Set](../../../docs/crdt/gset.md): grow-only semantics, set union,
  duplicate delivery, and the append-only membership use case.
- [State primitives](../../../docs/lattice/state-primitives.md): plain writes
  use last-writer-wins; G-Set has no remove operation.
- [G-Set join figure](../../../docs-site/figures/join-figures.json): the
  canonical empty, concurrent and union states, including the re-delivery.

## Not in this episode

- Other CRDT merge rules or a comparison of every set primitive.
- How replication transports deltas, or any implementation detail not needed
  to explain the G-Set merge.
- Removals: the G-Set has no remove operation; the episode only points viewers
  to the alternatives.
