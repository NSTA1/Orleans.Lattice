---
agent_spec: "docs/agents/api/lattice.json"
---

# Moving in from another store

A safe migration is a planned bulk load into a new tree, verified before readers move.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="moving-in-from-another-store" path="build" order="7" length="2:16" cut="f25d838d6f21" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/moving-in-from-another-store.html),
> or [download it](../../docs-site/media/moving-in-from-another-store-f25d838d6f21.mp4)
> (MP4, 2:16, 6.6 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

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

<!-- transcript:end -->

## The code on screen

This compiled example shows the one-shot load of prepared keys and byte values
into the tree.

<!-- video-snippet: moving-in-from-another-store/one-shot -->
```csharp verify
var entries = new List<KeyValuePair<string, byte[]>>
{
    KeyValuePair.Create("order/acme/0000000000000000001", "{}"u8.ToArray()),
};

await tree.BulkLoadAsync(entries, cancellationToken);
```

## Where next

<!-- where-next:begin -->

- **Next:** the documentation site's [Samples](https://nsta1.github.io/Orleans.Lattice/samples/index.html) page
- **The pages it introduces:** [External store migration](../lattice/external-store-migration.md)

<!-- where-next:end -->
