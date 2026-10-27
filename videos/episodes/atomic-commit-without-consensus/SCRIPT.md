# Atomic commit without consensus - script

Status: fact-checked by the Docs agent on 2026-10-27, cue by cue. The
coordinator wording distinguishes the shard fan-out result from separate
leaf votes. The visibility-timing addition is scoped to the originating
tree, and the closing cues name the plan's next episode and the companion
page's generated Verified atomic commit link. All other claims were verified
against the current corpus.

Written form, in the series voice (Emma, British English). This standalone
episode follows the Build path's Atomic writes episode into the protocol
behind its all-or-nothing guarantee.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds that many seconds of silence before the
next cue, or after the last one.

## Narration

### One atomic decision

Atomic writes make a batch all-or-nothing. This episode follows the single
decision that lets readers see it as one change.

### Prepare and decide

A batch can update several keys in one tree, such as an order's status and
its tracking value.

A coordinator groups the keys by their owning shards and stages each change
as a prepared write.

Each leaf keeps its prepared values in a pending bucket for this transaction.
They are not visible to concurrent readers.

If every prepare succeeds, the saga records commit in the tree's transaction
registry.

That registry write is the tree-wide visibility flip.

A read that finds a pending key resolves it through the registry's recorded
outcome, rather than guessing from which leaf has finished.

A multi-key reader uses one decision snapshot. It sees the batch's full
effect or none of it, never one prepared key without the others.

After the decision, terminal marks travel to the touched leaves and drain
their pending buckets. On the originating tree, a read of a key still in a
prepared bucket can follow the recorded outcome while terminal marks are in
flight. The batch's logical visibility does not wait for every leaf to apply
its mark.

The leaves finish applying the outcome; they do not make a second visibility
decision.

### Commit or abort

If a prepare fails, the coordinator retries. If the batch still cannot
complete, the saga records abort instead.

The prepared values never became visible, so abort drops their buckets. It
does not write a rollback value for every key.

That matters when another writer changed a key during the saga: rollback
writes could overwrite that concurrent change.

There is coordination: the saga coordinator drives the work and learns
whether the shard fan-out staged the batch.

But the leaves do not reach a commit by consensus. The registry records one
outcome, and readers use that outcome for the whole batch.

For the protocol's verification and design mapping, read Verified atomic
commit.

Back on Build, next: TTL and soft delete. The links are on this episode's
page in the documentation.

## Sources

| Narration | Source |
| --- | --- |
| a batch is all-or-nothing | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Atomicity Guarantees" |
| the coordinator groups keys by shard and stages prepared writes in hidden pending buckets | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Phase 1 - Prepare" and "Phase 2 - Execute" |
| the transaction registry records the commit; that write is the visibility flip | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Tree-wide visibility flip" |
| reads resolve pending keys against the decision; multi-key readers use one decision snapshot and see the batch atomically | [Consistency guarantees](../../../docs/lattice/consistency.md), "Batch operations" and "Atomic visibility"; [Atomic writes](../../../docs/lattice/atomic-writes.md), "Tree-wide visibility flip" |
| terminal marks drain pending buckets after the decision | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Tree-wide visibility flip" |
| failed prepares retry and then abort without per-key rollback writes | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Phase 2 - Execute" and "Phase 3 - Compensate" |
| the decision is coordinated, but is not a consensus among leaves | [Atomic writes](../../../docs/lattice/atomic-writes.md), "Tree-wide visibility flip"; [Atomic-commit specification overview](../../../spec/atomic-commit/README.md), "What is modelled" |
| the ending names TTL and soft delete and Verified atomic commit | [Production order](../../series.md#production-order), H4 ending |
