---
agent_spec: "docs/agents/concepts.yaml"
---

# Atomic writes

An atomic write makes related key changes visible together, or not at all.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="atomic-writes" path="build" order="4" length="2:06" cut="fe47bdc3df26" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/atomic-writes.html),
> or [download it](../../docs-site/media/atomic-writes-fe47bdc3df26.mp4)
> (MP4, 2:06, 6.4 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

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

### Where next

Next on Build: TTL and soft delete. The deep dive on how this works is Atomic commit without consensus. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## The code on screen

The cross-tree example moves an order from one tree to another in one atomic write. The snippet is compiled with the rest of the documentation.

<!-- video-snippet: atomic-writes/cross-tree -->
```csharp verify
// Move a row between trees in one cross-tree saga.
await grainFactory
    .BeginAtomicWrite("rekey:order-42")
    .ForTree("orders-east")
        .Delete("order:42")
    .ForTree("orders-west")
        .Set("order:42", new Order("42", 250m))
    .CommitAsync(cancellationToken);
```

## Where next

<!-- where-next:begin -->

- **Next on Build:** [TTL and soft delete](ttl-and-soft-delete.md)
- **The deep dive:** [Atomic commit without consensus](atomic-commit-without-consensus.md)
- **The pages it introduces:** [Atomic writes](../lattice/atomic-writes.md)

<!-- where-next:end -->
