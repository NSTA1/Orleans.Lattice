---
agent_spec: "docs/agents/concepts.yaml"
---

# Atomic commit without consensus

Prepared writes stay hidden until one tree-wide registry decision makes the
whole batch visible or aborts it.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="atomic-commit-without-consensus" path="how-it-works" order="4" length="2:05" cut="5526ab682e8e" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/atomic-commit-without-consensus.html),
> or [download it](../../docs-site/media/atomic-commit-without-consensus-5526ab682e8e.mp4)
> (MP4, 2:05, 5.3 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### One atomic decision

Atomic writes make a batch all-or-nothing. This episode follows the single decision that lets readers see it as one change.

### Prepare and decide

A batch can update several keys in one tree, such as an order's status and its tracking value.

A coordinator groups the keys by their owning shards and stages each change as a prepared write.

Each leaf keeps its prepared values in a pending bucket for this transaction. They are not visible to concurrent readers.

If every prepare succeeds, the saga records commit in the tree's transaction registry.

That registry write is the tree-wide visibility flip.

A read that finds a pending key resolves it through the registry's recorded outcome, rather than guessing from which leaf has finished.

A multi-key reader uses one decision snapshot. It sees the batch's full effect or none of it, never one prepared key without the others.

After the decision, terminal marks travel to the touched leaves and drain their pending buckets. On the originating tree, a read of a key still in a prepared bucket can follow the recorded outcome while terminal marks are in flight. The batch's logical visibility does not wait for every leaf to apply its mark.

The leaves finish applying the outcome; they do not make a second visibility decision.

### Commit or abort

If a prepare fails, the coordinator retries. If the batch still cannot complete, the saga records abort instead.

The prepared values never became visible, so abort drops their buckets. It does not write a rollback value for every key.

That matters when another writer changed a key during the saga: rollback writes could overwrite that concurrent change.

There is coordination: the saga coordinator drives the work and learns whether the shard fan-out staged the batch.

But the leaves do not reach a commit by consensus. The registry records one outcome, and readers use that outcome for the whole batch.

For the protocol's verification and design mapping, read Verified atomic commit.

Back on Build, next: TTL and soft delete. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## Where next

<!-- The list is written from the series plan (videos/series.json) by `npm run companions` in videos/; edit the plan, not this block. -->
<!-- where-next:begin -->

- **Back to Build:** [TTL and soft delete](ttl-and-soft-delete.md)
- **The pages it introduces:** [Verified atomic commit](../lattice/verified-atomic-commit.md)

<!-- where-next:end -->
