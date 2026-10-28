# Moving in from another store - script

Status: fact-checked on 2026-10-28 by the Docs agent, cue by cue, against the
migration guidance, relevant API source and series plan. The order caveats,
chunk-resume boundary, bulk-load size limits, source-specific cut and delta
deletions have been made explicit. Timing and delivery still require narration
and review.

Written form, in the series voice (Emma, British English). Each paragraph
under "Narration" is one cue, and each cue is a beat its scene can move on.

## Narration

<!-- pace 0.92 -->

### Moving in from another store

Going durable gave the tree durable grain storage and a durable write-ahead log. Now we can bring an existing dataset into it.

A move from Redis, a relational database or Cosmos DB is a one-way import. Bulk load seeds a tree; it does not keep the source and target in sync.

### Decide before loading

Before the first entry, settle three things: the ingest path, the key you compose from each source record, and the encoding that turns its value into bytes. Changing keys later is another migration; values stay opaque to the core.

### Choose an ingest path

If the dataset fits in memory, the list-based BulkLoadAsync sorts entries for you. For larger sets, stream them in strictly ascending ordinal key order from inside the cluster. It does not check order: out-of-order input is accepted and leaves the tree out of order. Redis scans, database collations and Cosmos ordering may not match the tree's order.

If the loader runs outside the cluster, use the tree-administration session. It acknowledges chunks and lets you resume the last unacknowledged chunk with the same operation id and index. It checks order within each chunk, not across chunks, so keep the whole stream ascending. When the source is paged, persist its cursor with the chunk index and resume both together.

### Move, then verify

Design keys for the tree's order: compare ordinally, and zero-pad numeric segments so ten follows nine. Choose the value encoding before loading. Bulk load does not enforce configured key or value size bounds, so check them in the producer.

Use the source store's own rules to take a consistent cut, then load those entries into a fresh tree.

Compare the loaded count with the cut and spot-check range boundaries.

Apply changes since that cut with SetManyAsync; handle deletions separately. That delta is not atomic. Move readers only when the checks and changes are complete, and keep the source readable until then.

### Where next

That is the end of the Build path. Next, the Samples page in the documentation. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

Each narration cue traces to the corpus or the series plan.

| Narration | Source |
| --- | --- |
| Going durable gave the tree durable grain storage and a durable write-ahead log | `videos/series.md`, B6 idea; `docs/lattice/wal-storage-providers.md`, "Registering a provider" |
| A move from Redis, relational database or Cosmos DB is a one-way import; bulk load does not keep source and target in sync | `docs/lattice/external-store-migration.md`, opening and "Then apply the delta and cut over" |
| Decide ingest path, key composition and value encoding before the first entry; keys are costly to change and values are opaque bytes | `docs/lattice/external-store-migration.md`, opening, "Designing the Lattice key" and "Encoding the value" |
| List-based BulkLoadAsync sorts the list; streaming needs strictly ascending ordinal keys, does not validate order, and source-store order may differ | `docs/lattice/external-store-migration.md`, "Choosing an ingest path", "Streaming into the cluster" and "Source-store playbooks" |
| Tree-administration sessions acknowledge chunks, validate order only within each chunk and resume the last unacknowledged chunk with the same operation id and index; persist a source continuation token with the chunk index | `docs/lattice/external-store-migration.md`, "Driving a resumable session", "Keys are compared ordinally" and "Cosmos DB" |
| Ordinal key comparison; zero-pad numeric segments; bulk load does not enforce configured key/value bounds, so validate them in the producer | `docs/lattice/external-store-migration.md`, "Keys are compared ordinally", "Zero-pad numeric segments", "What the bulk-load path deliberately does not do" and "Size" |
| Take a source-specific consistent cut, compare counts, spot-check boundaries, apply updates with SetManyAsync, handle deletions separately and move readers afterwards | `docs/lattice/external-store-migration.md`, relational database playbook, "Verifying the migration", and "Then apply the delta and cut over"; `src/lattice/BPlusTree/ILattice.cs`, `SetManyAsync` and `DeleteAsync` |
| Ending | `npm run series -- ending B7`, word for word |
