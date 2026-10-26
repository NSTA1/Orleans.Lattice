---
agent_spec: "docs/agents/concepts.yaml"
---

# Values that merge

A plain value keeps the last write, while a counter, register, set or map
merges concurrent writes by construction; choose the rule that matches your
data. This is B2 on the Build path, after Hello, Lattice, covering the
Orleans.Lattice 9.9 core package.

<!-- video:begin episode="values-that-merge" path="build" order="2" length="2:56" cut="29a5c4148870" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/values-that-merge.html),
> or [download it](../../docs-site/media/values-that-merge-29a5c4148870.mp4)
> (MP4, 2:56, 8.0 MB).

<!-- video:end -->

## Transcript

<!-- transcript:begin -->

### Values that merge

In Hello, Lattice, you registered Lattice on a silo and wrote and read typed values.

Now choose what should happen when two writers change the same key concurrently.

### Replace or merge

A plain set keeps the last write. It does not combine the contents of two records. If both updates matter, replacing one is the wrong rule.

A conflict-free replicated data type makes that rule part of the value. Choose the rule that matches your data, not just the shape of its fields.

### A counter keeps both

For a total that only grows, choose a grow-only counter. Each writer advances its own component.

Here, cluster A adds three, while cluster B adds five. Neither has seen the other's change.

Merge takes the larger component for each writer. Both components survive, and the value is their sum: eight.

Deliver the same state again and the value stays eight. For a count that can also decrease, choose a positive-negative counter: separate totals for increments and decrements.

### Choose a set

For a collection that only gains members, a grow-only set combines the members from both sides.

If members can be removed, choose what a concurrent add and remove should mean. An observed-remove set keeps the add.

A remove-wins set makes the opposite choice. That choice is part of the data's meaning, not a setting to leave until a conflict happens.

### Keep the conflict visible

Sometimes neither write should disappear. A multi-value register keeps values written concurrently.

Both clusters started from todo. One writes in progress, and the other writes resolved, without seeing the other's write.

The merged register holds both. Your application reads the candidates and decides how to resolve them.

A later write, made after observing both values, replaces both. Two writes in a row on one cluster are not this conflict: the second sees the first.

### Registers and maps

For a value that should only move upwards, choose a maximum register; for one that only moves downwards, a minimum register.

For a dictionary whose values also need to merge, choose an observed-remove map. Its keys are add-wins, and each value merges by its own conflict-free rule.

### Use the typed accessor

The typed accessor chooses the merge mode and encodes the delta. You do not hand-encode the CRDT delta. Give independent writers stable, distinct identities.

A local tree can mix these types on different keys. A replicated tree must use the single merge mode declared in its replication map.

Maps also need their key and value shape registered on the host, once per tree. The CRDT guide has the accessor examples and the details for each choice.

### Where next

Next on Build: Scans, filters and cursors. The deep dive on how this works is Conflict-free merges in 90 seconds. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## Choosing a primitive

This episode shows state diagrams rather than code. The
[CRDT primitives guide](../crdt/readme.md) holds the choice table and compiled
typed-accessor examples. Start with the conflict rule, then choose the family:
counters keep numeric contributions, sets decide concurrent membership,
registers choose or surface values, and maps recursively merge their values.

A local tree can mix modes on different keys. A tree enrolled for replication
must keep to its declared merge mode. An OR-Map also needs its key/value shape
registered on the host for its tree.

## Where next

<!-- where-next:begin -->

- **Next on Build:** Scans, filters and cursors
- **The deep dive:** Conflict-free merges in 90 seconds
- **The pages it introduces:** [CRDT primitives](../crdt/readme.md)

<!-- where-next:end -->
