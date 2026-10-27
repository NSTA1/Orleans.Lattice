# TTL and soft delete - brief

- **Path:** Build, fifth episode (B5), after Atomic writes.
- **Audience:** a developer whose trees hold data that should not live for ever, such as sessions or carts, or who may one day delete a whole tree by mistake.
- **Length:** two to five minutes, with the preceding episode's reduced narration pace.
- **The one idea:** entries can expire on their own, and a deleted tree can be recovered inside its retention window.
- **It introduces:** [TTL](../../../docs/lattice/ttl.md).
- **Afterwards the viewer can** write an entry with a time to live, predict what reads return once it expires and what a later write does to its expiry, and delete a tree knowing how long it stays recoverable.

## Beats

1. Recap that Atomic writes made related changes visible as a whole; this episode asks how data goes away.
2. Write an entry with a time to live; the silo fixes the absolute expiry, and an expired entry is invisible to every read.
3. A later write wins as usual, and a winning write without a time to live clears the expiry. On a CRDT entry, a refresh only ever extends it.
4. Compaction reaps expired entries after the tombstone grace period.
5. Deleting a tree is a soft delete: it is inaccessible at once, and its data is kept for a window, 72 hours by default.
6. Recovering inside the window restores the data; once the purge has started, it is too late.
7. End at Going durable.

## Code on screen

- One compiled typed write with a 30-minute time to live.
- One compiled tree deletion.
- One compiled tree recovery followed by a read.

## Sources

- [TTL](../../../docs/lattice/ttl.md): "TTL on `SetAsync`", "Per-entry TTL on CRDT writes", "Read paths", "Scans and counts" and "Tombstone compaction".
- [Tree deletion](../../../docs/lattice/tree-deletion.md): "Overview", "Configuration" and "Recovering a Deleted Tree".
- [Series plan](../../series.md): B5's idea, introduced page, and ending printed by `npm run series -- ending B5`.

## Not in this episode

- Compaction internals, the purge phases, or reminder mechanics.
- `PurgeTreeAsync`, beyond noting that a purge ends the window.
- The result cache's own `CacheTtl`, which is unrelated to entry expiry.
- Replication, backup, or performance figures.
