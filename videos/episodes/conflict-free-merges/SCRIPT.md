# Conflict-free merges in 90 seconds - script

Status: all eight narration cues fact-checked by Docs; all claims are supported
with no wording corrections. Narration is re-ordered to match the join-diagram's
writes, merge, re-delivery and rule beats. The opening recaps Values that merge;
the ending follows the H1 plan.

Written form, in the series voice (Emma, British English). The narration is
plain ASCII; captions keep the written form.

## Narration

### Opening

Values that merge showed how a chosen data type can resolve concurrent writes; now watch one merge in a set.

### G-Set merge

A plain write uses last-writer-wins: if two clusters write the same key, the later timestamp keeps its value and silently replaces the other.

A G-Set has a different rule. It only grows: you can add an element, but cannot remove one. Start with an empty set. Cluster A adds alice while cluster B, without seeing that change, adds bob.

Their sets are {alice} and {bob}; neither contains the other. When they merge, each takes the union, keeping every element from both sets.

Now deliver bob's add again. Both clusters hold {alice, bob}; the union gives the same result regardless of delivery order, and repeating an add does not duplicate an element.

Set union is commutative, associative and idempotent: swapping, regrouping or repeating a merge leaves the result unchanged. That is why both sides converge without choosing a winner.

Use a G-Set for append-only membership, such as seen identifiers or tags. If your data needs removals, choose an OR-Set or RW-Set instead; those define removal semantics.

Back on Build, next: Scans, filters and cursors. The links are on this episode's page in the documentation.

## Sources

| Narration | Source |
| --- | --- |
| A typed data type gives concurrent writes a merge rule | [CRDT primitives](../../../docs/crdt/readme.md), "What is a CRDT" and "The primitives"; [Values that merge](values-that-merge.md), the preceding episode. |
| A plain write uses last-writer-wins and can replace a concurrent value | [CRDT primitives](../../../docs/crdt/readme.md), warning on plain `SetAsync`; [state primitives](../../../docs/lattice/state-primitives.md), "Opt-in CRDT values". |
| G-Set only grows, has no remove operation, and merges by union | [G-Set](../../../docs/crdt/gset.md), "Semantics"; [state primitives](../../../docs/lattice/state-primitives.md), "Grow-Only Set". |
| `alice` and `bob` are concurrent; neither set contains the other; union produces both | [G-Set join figure](../../../docs-site/figures/join-figures.json), scenario `gset`; [G-Set](../../../docs/crdt/gset.md), "Behaviour". |
| Union is commutative, associative and idempotent; a re-delivered add is harmless | [G-Set](../../../docs/crdt/gset.md), "Semantics"; [CRDT primitives](../../../docs/crdt/readme.md), "What is a CRDT". |
| G-Set fits append-only membership; OR-Set and RW-Set support removals | [G-Set](../../../docs/crdt/gset.md), "Example use case"; [CRDT primitives](../../../docs/crdt/readme.md), "The primitives". |
| The ending names the next Build episode | `videos/series.json`, H1 ending. |
