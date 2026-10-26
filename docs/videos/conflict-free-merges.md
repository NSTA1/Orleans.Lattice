---
agent_spec: "docs/agents/concepts.yaml"
---

# Conflict-free merges in 90 seconds

A G-Set merges concurrent additions by taking their union, so both clusters converge on the same set.

<!-- The video block is written by `npm run companions` in videos/; the documentation site replaces it with the player. -->
<!-- video:begin episode="conflict-free-merges" path="how-it-works" order="1" length="1:18" cut="1238894bf1b5" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/conflict-free-merges.html),
> or [download it](../../docs-site/media/conflict-free-merges-1238894bf1b5.mp4)
> (MP4, 1:18, 3.4 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/. -->
<!-- transcript:begin -->

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

<!-- transcript:end -->

## Where next

<!-- The list is written from the series plan by `npm run companions` in videos/. -->
<!-- where-next:begin -->

- **Back to Build:** [Scans, filters and cursors](scans-filters-and-cursors.md)
- **The pages it introduces:** [CRDT primitives](../crdt/readme.md) and [state primitives](../lattice/state-primitives.md)

<!-- where-next:end -->
