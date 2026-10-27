# TTL and soft delete - script

Status: fact-checked on 2026-10-27 by the Docs agent against
`docs/lattice/ttl.md` and `docs/lattice/tree-deletion.md`. Eleven of twelve
checked cues were accurate; the expiry-replacement cue now says a winning
*plain* write clears the expiry, since a CRDT write without a time to live
leaves it unchanged.

Written form, in the series voice (Emma, British English). The code is on
screen, so the narration explains the promise rather than reading method names.
This episode keeps the preceding episode's reduced pace of 0.92.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds silence before the next cue or after the
last one.

## Narration

<!-- pace 0.92 -->

### TTL and soft delete

Atomic writes showed how related changes become visible as a whole. Here, we ask how data goes away: on its own, or by mistake.

This is TTL and soft delete: entries can expire on their own, and a deleted tree can be recovered inside its retention window.

### Entries that expire

A session should not live for ever. Write it with a time to live, here thirty minutes.

The silo turns that duration into an absolute expiry time, so a client with a skewed clock does not move it.

Once that time passes, the entry is gone from every read. A get returns nothing, an existence check says false, and scans and counts skip it.

### Refresh and reaping

Expiry follows the write that wins. A later write replaces the entry as usual, and a winning plain write without a time to live clears the expiry.

On a CRDT entry, expiries merge by taking the later one, so a refresh only ever extends it.

An expired entry is not deleted straight away. Compaction reaps it after the tombstone grace period, so a lagging peer cannot bring it back.

### Soft delete

Deleting a whole tree is a soft delete. The tree becomes inaccessible at once: its reads and writes throw.

But its data is not gone. It is kept for a retention window, seventy-two hours by default, before it is purged.

### Recovery

Inside that window, recovering the tree makes it accessible again, with all the data it held before the delete.

Once the purge has started, it is too late; and once the purge is complete, the data is gone.

<!-- pause 0.8 -->

### Where next

Next on Build: Going durable. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

| Narration | Source |
| --- | --- |
| Atomic writes showed how related changes become visible as a whole; this episode asks how data goes away | `videos/series.md`, B4 and B5 ideas |
| Entries can expire on their own, and a deleted tree can be recovered inside its retention window | `videos/series.md`, B5 title and idea |
| Write an entry with a time to live, here thirty minutes | `docs/lattice/ttl.md`, "TTL on `SetAsync`" |
| The silo computes the absolute expiry, so client clock skew does not move it | `docs/lattice/ttl.md`, "TTL on `SetAsync`" |
| An expired entry is invisible to reads: get returns null, exists returns false, scans and counts skip it | `docs/lattice/ttl.md`, "Read paths" and "Scans and counts" |
| A later write wins, and a winning plain write without a time to live clears the expiry | `docs/lattice/ttl.md`, "TTL on `SetAsync`" |
| CRDT expiries merge by maximum, so a refresh only ever extends it | `docs/lattice/ttl.md`, "Per-entry TTL on CRDT writes" |
| Compaction reaps an expired entry after the tombstone grace period, preventing a lagging peer from resurrecting it | `docs/lattice/ttl.md`, "Tombstone compaction" |
| Deleting a tree is a soft delete; it is inaccessible at once and its reads and writes throw | `docs/lattice/tree-deletion.md`, opening contract |
| Data is retained for a window, 72 hours by default, before purge | `docs/lattice/tree-deletion.md`, opening contract and "Configuration" |
| Recovery inside the window restores the tree with all its data | `docs/lattice/tree-deletion.md`, "Recovering a Deleted Tree" |
| Once a purge has started it is too late; once complete the data is gone | `docs/lattice/tree-deletion.md`, "Recovering a Deleted Tree" |
| Next on Build: Going durable; the links are on the episode page | `npm run series -- ending B5`, word for word |
