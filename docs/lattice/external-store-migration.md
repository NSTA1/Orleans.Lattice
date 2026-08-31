# Migrating from an External Store

This page is the operational playbook for a one-way import: taking a dataset
that lives in Redis, a relational database, or Cosmos DB today and landing it in
a Lattice tree over the bulk-load path. [Bulk Loading](bulk-loading.md) compares
the bulk-load primitives against `SetManyAsync` and explains why the bulk path is
structurally cheaper; this page assumes you have read it and concentrates on the
parts a migration gets wrong.

The decisions that determine whether the migration succeeds are all made before
the first entry is written:

1. **Which ingest path you drive**, and therefore what ordering guarantee you owe
   the tree.
2. **How you compose the Lattice key** from the source store's identity. The tree
   is ordinally sorted, so the key shape is what makes later range scans
   expressible at all.
3. **How you encode the value**. The tree stores an opaque `byte[]` and nothing in
   the core will tell you, a year later, that the payload was the wrong shape.

Enumerating the source, chunking, and restarting are mechanical once those are
settled.

## Choosing an ingest path

| Path | Ordering you owe | Order enforced | Restart story |
|---|---|---|---|
| `ILattice.BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>>, ...)` | none; the entries are sorted for you | not applicable | re-drive against a fresh tree; a re-drive in place fails once any shard holds data |
| `LatticeExtensions.BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, chunkSize, ...)` | globally ascending, ordinal | **no** | re-drive the stream from the start against a fresh tree |
| `ILatticeTreeAdmin.BeginBulkLoadAsync` / `AppendBulkLoadAsync` / `CommitBulkLoadAsync` | strictly ascending, ordinal, within each chunk and across chunks | within a chunk only, as `BulkLoadOrderException` | re-drive the last unacknowledged chunk under the same operation id |

**One-shot** is the right answer whenever the dataset fits in the loader's
memory. It sorts internally, so the source enumeration order does not matter, and
it removes the entire class of ordering bugs the other two paths expose.

**Streaming** is for a dataset that does not fit in memory and a loader that runs
inside the cluster. It addresses the internal shard grains directly rather than
going through the `ILattice` facade, which has two consequences worth knowing
before you pick it:

- It performs no order validation. Out-of-order input is accepted and produces a
  tree whose entries are not in key order, and no exception is raised at load
  time to tell you so.
- On a cluster that registered the authorization layer, a direct call to an
  internal shard grain arriving from an external Orleans client is refused with
  `LatticeAuthorizationDeniedException`. Run the streaming loader inside the
  cluster (a grain or a silo-hosted background service), or use the
  tree-administration session instead.

**The tree-administration session** is for a loader that runs out of process,
including a non-.NET one over the gRPC binding. It goes through the `ILattice`
facade and its access gate, validates order, and is the only path with a
server-acknowledged chunk protocol you can resume against. See the
[tree-administration control API](../lattice.api.treeadmin/README.md) for the
rest of that facade.

### Streaming into the cluster

```csharp verify
// The external reader is whatever your source store gives you. All the
// bulk-load path asks for is an ascending-ordinal stream of key/value pairs.
static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ReadSourceAscendingAsync(
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
{
    for (var i = 0; i < 1_000; i++)
    {
        ct.ThrowIfCancellationRequested();
        yield return KeyValuePair.Create(
            $"order/acme/{i:D19}",
            Encoding.UTF8.GetBytes($"payload-{i}"));
    }

    await Task.CompletedTask;
}

await tree.BulkLoadAsync(
    ReadSourceAscendingAsync(cancellationToken),
    grainFactory,
    chunkSize: 5_000,
    cancellationToken);
```

### Driving a resumable session

```csharp verify
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;

// Resolved from DI on a host that registers the tree-administration facade. An
// out-of-process .NET loader drives the same three verbs through the separate
// typed client in Orleans.Lattice.Api.TreeAdmin.Grpc, which mirrors this
// contract rather than implementing it.
ILatticeTreeAdmin admin = default!;

const string treeId = "orders";

// The operation id must not contain '/': the facade composes the per-chunk id
// as "{operationId}/{chunkIndex}". Keep it stable across restarts so a replayed
// chunk is recognised as one already applied.
const string operationId = "orders-import-2026-01";

await admin.BeginBulkLoadAsync(treeId, operationId, cancellationToken);

long chunkIndex = 0;
var chunk = new List<DataEntry>(1_000);
await foreach (var (key, value) in ReadAscendingAsync())
{
    chunk.Add(new DataEntry { Key = key, Value = value });
    if (chunk.Count < 1_000)
    {
        continue;
    }

    var ack = await admin.AppendBulkLoadAsync(
        treeId, operationId, chunkIndex, chunk, cancellationToken);

    // A short count means an installed schema policy rejected or diverted
    // entries in this chunk. Silence here is how a migration loses rows.
    if (ack.AcceptedEntryCount != chunk.Count)
    {
        Console.WriteLine(
            $"chunk {ack.ChunkIndex}: accepted {ack.AcceptedEntryCount} of {chunk.Count}");
    }

    chunkIndex = ack.NextChunkIndex;
    chunk = new List<DataEntry>(1_000);
}

if (chunk.Count > 0)
{
    await admin.AppendBulkLoadAsync(treeId, operationId, chunkIndex, chunk, cancellationToken);
}

var result = await admin.CommitBulkLoadAsync(treeId, operationId, cancellationToken);
Console.WriteLine($"{result.TotalLiveKeys} live keys after the load.");

static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ReadAscendingAsync()
{
    yield return KeyValuePair.Create("order/acme/0000000000000000001", "{}"u8.ToArray());
    await Task.CompletedTask;
}
```

## What the bulk-load path requires

### The target tree must be empty

The one-shot path fans out to *every* physical shard, including shards this load
has no entries for, so data sitting on an untouched shard is still detected. A
shard that already holds a root node rejects the load with
`InvalidOperationException`.

`BeginBulkLoadAsync` probes the tree first with a shallow diagnostic and rejects
a populated one with `TreeNotEmptyException`. The shallow probe counts live keys
only; it does not walk the leaves to count tombstones. A tree you emptied by
deleting every key therefore reports zero live keys and **passes the probe**,
even though the tombstoned rows are still in the leaves and the append grafts
onto the existing tree rather than building a fresh one. Create a fresh tree
rather than emptying an existing one.

The streaming extension performs no emptiness check at all - neither a probe of
its own nor the one-shot path's per-shard rejection. It appends to whatever is
already there, which is why its restart story is a fresh tree.

### Keys are compared ordinally

Every comparison the tree makes - routing, in-leaf search, range bounds, and the
bulk-load order check - is `StringComparison.Ordinal`. There is no culture-aware
or case-insensitive mode, and two consequences follow directly:

- Every uppercase ASCII letter sorts before every lowercase one, so `"Z"`
  precedes `"a"`.
- `List<string>.Sort()` with no comparer, and `OrderBy(x => x)`, both use the
  current culture. Neither matches the tree's order. Sort with
  `StringComparer.Ordinal` or `string.CompareOrdinal`.

The append-based paths graft each chunk onto the right edge of the tree, which is
why they require ascending input rather than merely sorted-per-chunk input. The
tree-administration facade validates this and throws `BulkLoadOrderException`,
carrying the tree id, the chunk index, the offending key, and the key that
preceded it, before any grain call is made, so no partial data is grafted. The
limits on that check are worth internalising:

- It rejects an equal key as well as a descending one, so **de-duplicate in the
  source enumeration**.
- It compares within a single chunk. The first key of a chunk is not compared
  against the last key of the previous chunk, so cross-chunk order remains the
  caller's responsibility on every path.

### The load is idempotent, not transactional

Each shard records the identifier of the last bulk operation it completed, and
re-driving that same operation short-circuits. The facade derives a deterministic
per-shard identifier from the one you supply, so replaying a chunk under the same
operation id and chunk index replays the same per-shard identifiers and is
absorbed. A shard remembers only its most recent completed operation, so resume
from the last unacknowledged chunk and never replay a chunk that a later one has
already superseded.

A graft persists its intent before it completes, so a shard that crashes
mid-graft finds the recorded intent and completes it before it serves its next
operation, rather than leaving a half-linked right edge behind.

What it is not is atomic across shards. A chunk that fails partway leaves the
shards that already committed holding their slice. That is safe because the retry
is idempotent, but it means a failed migration is resumed, not rolled back.

### What the bulk-load path deliberately does not do

- **It does not enforce the write-size bounds.** `LatticeOptions.MaxKeyLength`
  and `LatticeOptions.MaxValueSizeBytes` are opt-in and unset by default, and
  when set they are checked on the point and batch write boundary, not on the
  bulk-load path. A key or value you successfully bulk-load can therefore be
  rejected by a later `SetAsync` against the same tree. Check the bounds in the
  producer if you have configured them.
- **It does not merge.** `DataEntry` carries a `MergeMode` and a `Raw` flag, but
  both are read-side fields that the write path ignores: a chunk entry is
  projected down to its key and its value before it reaches the tree. Setting
  either one on a migration entry has no effect.
- **It does not carry an expiry.** The source store's remaining time-to-live is
  not part of a bulk-load entry. If expiry has to survive the migration, re-apply
  it after the load with the tree's [TTL](ttl.md) surface.

### Write interception still runs

The facade-hosted paths run the write interceptor when interception is active, so
an installed [schema policy](../lattice.schema/README.md) validates bulk-loaded
values and can divert them to the dead-letter store. The chunk acknowledgement's
accepted-entry count is the count *after* interception, so comparing it against
the number of entries you sent is how you detect that a schema policy quietly
filtered your migration. The streaming extension uses the facade only to resolve
routing and then writes to the shards directly, so it runs neither the access
gate nor interception.

### The write-ahead log and cross-cluster replication

Bulk-load writes are classified as user mutations, not maintenance, so on a
replicated tree they ship to peer clusters exactly like any other user write. A
migration into a replicated tree therefore pushes the whole dataset across every
replication link; budget for that, or complete the load before the tree
participates in replication.

Underneath, entries are batched per leaf into a single write-ahead-log append
rather than one append per key. See
[the batched leaf write path](wal.md#batched-leaf-write-path) for the bound, and
[Bulk Loading](bulk-loading.md#write-round-trip-budget-for-a-large-import) for
what that means for a large import's round-trip budget.

## Designing the Lattice key

The key is the only thing about a migrated dataset that is genuinely hard to
change later: rewriting values is a scan and a batch of writes, but rewriting
keys is another whole migration.

### Zero-pad numeric segments

Ordinal comparison is lexicographic, so `"10"` sorts before `"9"`. Pad every
numeric segment to a fixed width and the two orders agree. Nineteen digits covers
the whole `long` range.

```csharp verify
// Compose the Lattice key from the source store's identity. Numeric segments
// are zero-padded to a fixed width so lexicographic order matches numeric
// order.
static string OrderKey(string tenantId, long orderId) =>
    $"order/{tenantId}/{orderId:D19}";

var keys = new List<string>
{
    OrderKey("acme", 100),
    OrderKey("acme", 9),
    OrderKey("acme", 10),
};

// The tree compares keys with StringComparison.Ordinal. Sort with the same
// comparer: the parameterless List<string>.Sort() and OrderBy(x => x) both use
// the current culture, which is a different order.
keys.Sort(StringComparer.Ordinal);
```

### Choose a separator that sorts below your segment alphabet

With variable-width segments the separator's code point decides whether a
prefix's children stay contiguous. `/` is `0x2F`, below every digit and letter,
so `order/10/x` sorts before `order/100`, which is what a hierarchical key
wants. `:` is `0x3A`, above every digit, so `order:10:x` sorts *after*
`order:100` and the sub-tree under `order:10` is no longer a contiguous range.

Fixed-width segments remove the hazard entirely, which is another reason to
zero-pad. If your segments are variable-width, pick a separator below the
alphabet they can contain, and reject source values that contain the separator
rather than letting them forge a level of the hierarchy.

### A prefix is a logical range, not a placement hint

Entries are assigned to physical shards by hashing the whole key, not by
splitting the keyspace into ranges. Two things follow:

- A shared prefix does **not** co-locate entries on one shard, and a prefix scan
  is not a single-shard operation. What a prefix buys you is that the range is
  expressible and bounded, not that it is cheap to place.
- Monotonically increasing keys - a timestamp or an auto-increment id at the
  front of the key - do **not** create a hot shard, because placement does not
  follow key order. This is the one classic range-partitioned-store hazard that
  does not apply here.

The end-exclusive bound of a prefix scan is the prefix with its final character
incremented: the range `["order:", "order;")` is exactly the keys beginning
`order:`.

### Keep keys short

Every key is stored in the leaf that holds it and participates in the separators
in the levels above it, so key length trades directly against how many entries
fit within a node's size budget. See [Tree Storage](tree-storage.md) for the
per-provider row limits and node size estimation, and [Tree Sizing](tree-sizing.md)
for the sizing arithmetic.

## Encoding the value

### The tree stores bytes

A Lattice value is a `byte[]` and the core never inspects it. Everything below is
about the encoder you put in front of it.

### `ILatticeSerializer<T>`

`ILatticeSerializer<T>` declares `Serialize` and `Deserialize` over `byte[]`.
`JsonLatticeSerializer<T>` is the built-in implementation over
`System.Text.Json`, and `JsonLatticeSerializer<T>.Default` is the instance the
typed overloads use when you do not supply one.

```csharp verify
// A migration-time serializer. Whatever shape you choose here is what every
// later read has to understand, so decide it before the load, not after.
public sealed class CompactOrderSerializer : ILatticeSerializer<Order>
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public byte[] Serialize(Order value) =>
        JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public Order Deserialize(byte[] bytes) =>
        JsonSerializer.Deserialize<Order>(bytes, Options)
            ?? throw new InvalidOperationException("Stored value decoded to null.");
}
```

The typed bulk-load overloads are one-shot only, so a streaming migration
serializes to `byte[]` in the producer and drives the untyped stream:

```csharp verify
var orders = new List<KeyValuePair<string, Order>>
{
    KeyValuePair.Create("order/acme/0000000000000000001", new Order("1", 19.99m)),
    KeyValuePair.Create("order/acme/0000000000000000002", new Order("2", 24.50m)),
};

// Uses JsonLatticeSerializer<Order>.Default when no serializer is supplied.
await tree.BulkLoadAsync(orders, cancellationToken);

// Or pass your own encoder.
await tree.BulkLoadAsync(orders, JsonLatticeSerializer<Order>.Default, cancellationToken);
```

### Decide on schema evolution before the load, not after

The core hands back exactly the bytes you wrote, for as long as the tree lives.
If the payload shape can change - and over the lifetime of a migrated dataset it
will - adopt the [schema package](../lattice.schema/README.md) at load time
rather than retrofitting it across a populated tree. It gives you per-tree write
validation with dead-letter diversion, and self-describing value versioning with
read-time upcasting, over the opaque-`byte[]` core.

Because the facade-hosted bulk-load paths run write interception, a schema policy
installed before the migration applies *during* it. That is usually what you
want, since it catches malformed source rows at the door, but it does mean the
accepted-entry count is the number that matters, not the number you sent.

### Size

Check the encoded size in the producer. The bulk-load path will not do it for
you, and an oversized value surfaces later as a storage-provider failure rather
than an argument error at the call site.

```csharp verify
const int maxValueBytes = 512 * 1024;

static byte[] Encode(Order order) => JsonSerializer.SerializeToUtf8Bytes(order);

var candidate = new Order("00000001", 19.99m);
var encoded = Encode(candidate);
if (encoded.Length > maxValueBytes)
{
    throw new InvalidOperationException(
        $"Source row {candidate.Id} encodes to {encoded.Length} bytes, above the migration budget.");
}
```

[Tree Storage](tree-storage.md) carries the per-provider row limits that bound
what a value can actually be. Choose a migration budget below the tightest limit
in your deployment, and split or externalise the outliers before the load rather
than discovering them at chunk 4,000.

### Compression

`ILatticeCompressor` is a transport and storage-layer seam - replication framing
and the durable write-ahead-log row payload - not a per-value hook. If you want
stored values compressed, compress inside your `ILatticeSerializer<T>` and accept
that every read pays the decompression. See [Compression](compression.md) for the
seam that does exist and where it applies.

## Source-store playbooks

All three stores reduce to the same two steps: produce an ordinal-ascending
stream of key/value pairs, then hand it to one of the ingest paths above. The
differences are entirely in how the source is enumerated stably and resumably.

One rule cuts across all of them: **do not assume the source store's ordering is
the tree's ordering.** A relational `ORDER BY` uses the column's collation, and
Redis and Cosmos DB have their own rules. Byte-wise orderings agree with .NET
ordinal comparison for ASCII keys, but can diverge once non-ASCII characters are
involved, because ordinal comparison works on UTF-16 code units. Keep migration
keys ASCII, enumerate under a binary collation, or re-sort the composed key in
the loader.

### Redis

`SCAN` is the only safe way to walk a production keyspace - `KEYS` blocks the
server for the length of the scan - but it gives you no ordering at all, and it
can return the same key more than once. Sorting is therefore not optional:

- **If the key set fits in memory**, scan the whole keyspace into an
  ordinal-sorted, de-duplicated key list first, then stream values in that order.
  Only the keys are held; the values stream.
- **If the key set does not fit**, spill the keys, external-sort them with an
  ordinal comparer, then stream.
- **If you already maintain a sorted-set index**, walk it lexicographically
  instead of scanning, subject to the ASCII caveat above.

```csharp verify
// The Redis client is not a dependency of this repository, so the cursor is
// represented by this seam. A real adapter implements it over your client.
public interface IRedisScanCursor
{
    // One SCAN page plus the cursor to resume from; zero when the scan is done.
    Task<(IReadOnlyList<string> Keys, long NextCursor)> ScanAsync(
        long cursor, int count, CancellationToken cancellationToken);

    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken);
}

public static class RedisMigration
{
    // SCAN order is undefined and a key can repeat, so the key set is collected
    // and ordinally sorted before a single value is fetched.
    public static async Task<List<string>> ReadSortedKeysAsync(
        IRedisScanCursor source, CancellationToken cancellationToken)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        long cursor = 0;
        do
        {
            var (page, next) = await source.ScanAsync(cursor, 1_000, cancellationToken);
            foreach (var key in page)
            {
                keys.Add(key);
            }

            cursor = next;
        }
        while (cursor != 0);

        var sorted = new List<string>(keys);
        sorted.Sort(StringComparer.Ordinal);
        return sorted;
    }

    public static async Task MigrateAsync(
        ILattice tree,
        IGrainFactory grainFactory,
        IRedisScanCursor source,
        CancellationToken cancellationToken)
    {
        var sortedKeys = await ReadSortedKeysAsync(source, cancellationToken);
        await tree.BulkLoadAsync(
            StreamAsync(source, sortedKeys, cancellationToken),
            grainFactory,
            chunkSize: 5_000,
            cancellationToken);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> StreamAsync(
        IRedisScanCursor source,
        IReadOnlyList<string> sortedKeys,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var key in sortedKeys)
        {
            // A key can expire between the scan and the fetch; skip it rather
            // than writing an empty value.
            var value = await source.GetAsync(key, cancellationToken);
            if (value is not null)
            {
                yield return KeyValuePair.Create(key, value);
            }
        }
    }
}
```

Redis-specific things that bite:

- **Expiry.** A key can expire between the scan and the fetch, so the fetch has
  to tolerate a miss. If the remaining time-to-live matters, read it alongside
  the value and re-apply it after the load; a bulk-load entry does not carry one.
- **Value types.** Hashes, lists, and sets are not opaque strings. Decide how each
  source type maps onto a single Lattice value - or onto several keys under a
  shared prefix - before you start, not per-key during the load.
- **Key shape.** Redis keys are byte strings with no imposed structure. The
  migration is the moment to impose one: re-compose them into the padded,
  separator-disciplined shape described above instead of copying them verbatim.

### Relational databases

Enumerate with **keyset pagination**, not `OFFSET`. An `OFFSET` page re-scans
everything before it, so the cost grows with the page number and the enumeration
is not stable under concurrent inserts. Seeking on the last key you emitted stays
flat and stays stable.

```csharp verify
// ADO.NET is not exercised here, so the paged query is represented by this
// seam. A real adapter runs the keyset query over your DbConnection.
public interface ISqlKeysetReader
{
    // SELECT tenant_id, order_id, payload FROM orders
    // WHERE (tenant_id, order_id) > (@lastTenantId, @lastOrderId)
    // ORDER BY tenant_id, order_id
    // FETCH FIRST @pageSize ROWS ONLY
    Task<IReadOnlyList<(string TenantId, long OrderId, byte[] Payload)>> ReadPageAsync(
        string lastTenantId, long lastOrderId, int pageSize, CancellationToken cancellationToken);
}

public static class SqlMigration
{
    public static async IAsyncEnumerable<KeyValuePair<string, byte[]>> StreamAsync(
        ISqlKeysetReader source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lastTenantId = string.Empty;
        var lastOrderId = 0L;

        while (true)
        {
            var page = await source.ReadPageAsync(lastTenantId, lastOrderId, 1_000, cancellationToken);
            if (page.Count == 0)
            {
                yield break;
            }

            foreach (var (tenantId, orderId, payload) in page)
            {
                // The zero-padded segment makes the string order agree with the
                // numeric ORDER BY the query used.
                yield return KeyValuePair.Create($"order/{tenantId}/{orderId:D19}", payload);
                lastTenantId = tenantId;
                lastOrderId = orderId;
            }
        }
    }
}
```

Relational-specific things that bite:

- **Collation.** The default collations on most engines are case-insensitive and
  accent-insensitive, which is not ordinal comparison. Either order by columns
  whose ordering you can prove equivalent to the composed key's ordering - which
  fixed-width zero-padded numeric segments give you - or order under a binary
  collation, or re-sort in the loader.
- **A consistent cut.** Enumerate against a stable read - a snapshot isolation
  level or a point-in-time - so a row updated after the cursor has passed it is
  not silently missed. Record the cut, because it is the starting point of the
  delta pass.
- **Composite keys with nullable columns.** A `NULL` segment has no ordinal
  representation. Choose a sentinel before composing the key, and make sure the
  sentinel sorts where you want relative to real values.

### Cosmos DB

Enumerate with a cross-partition `ORDER BY` query and carry the continuation
token. Persist the token *with* the chunk index so a restart resumes both the
source cursor and the target chunk index together; resuming one without the other
either skips or duplicates a chunk.

```csharp verify
// The Cosmos SDK is not a dependency of this repository, so the paged query is
// represented by this seam. A real adapter runs it over CosmosClient.
public interface ICosmosPageReader
{
    // SELECT c.id, c.pk, c.payload FROM c ORDER BY c.pk, c.id
    Task<(IReadOnlyList<(string PartitionKey, string Id, byte[] Payload)> Items, string? ContinuationToken)>
        ReadPageAsync(string? continuationToken, int pageSize, CancellationToken cancellationToken);
}

public static class CosmosMigration
{
    public static async IAsyncEnumerable<KeyValuePair<string, byte[]>> StreamAsync(
        ICosmosPageReader source,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        string? continuation = null;
        do
        {
            var (items, next) = await source.ReadPageAsync(continuation, 1_000, cancellationToken);
            foreach (var (partitionKey, id, payload) in items)
            {
                yield return KeyValuePair.Create($"order/{partitionKey}/{id}", payload);
            }

            continuation = next;
        }
        while (continuation is not null);
    }
}
```

Cosmos-specific things that bite:

- **Indexing.** A cross-partition `ORDER BY` needs the ordered paths covered by
  the container's indexing policy, and a multi-property ordering needs a
  composite index. Check the policy before the migration window opens, because
  adding an index to a large container is itself a long operation.
- **Request-unit throttling.** A throttled request is normal at migration
  throughput. Honour the retry-after the service returns, and align chunk
  boundaries with page boundaries so a retried page replays a whole chunk under
  the same operation id rather than half of one.
- **Ordering.** Cosmos DB's string ordering is its own; do not assume it equals
  .NET ordinal comparison. Keep the composed key ASCII, or re-sort in the loader.
- **Key composition.** The partition key and item id together are the natural
  source of the Lattice key. Compose them as
  `{partitionKey}/{id}` so items sharing a partition stay a contiguous range in
  the tree - a logical range, not a placement guarantee.

## Verifying the migration

### Count what landed

`CommitBulkLoadAsync` returns the tree's observed live-key count, which is the
cheapest end-to-end check there is. Compare it against the source count you
recorded at the cut. For a subset, `CountAsync` takes a range.

```csharp verify
// Whole tree.
var total = await tree.CountAsync(cancellationToken);

// One prefix: the end bound is the prefix with its last character incremented.
var acmeOrders = await tree.CountAsync("order/acme/", "order/acme0", cancellationToken);

Console.WriteLine($"{total} entries loaded, {acmeOrders} of them under order/acme/.");
```

### Spot-check the boundaries

Range boundaries are where an off-by-one in key composition shows up. Walk the
first entries of each prefix and confirm they are the rows you expect.
`ScanEntriesAsync` is the recommended client API for long-running exports: it
recovers from an aborted enumeration and resumes deterministically.

```csharp verify
var sampled = 0;
await foreach (var entry in tree.ScanEntriesAsync(
    "order/acme/", "order/acme0", cancellationToken: cancellationToken))
{
    Console.WriteLine($"{entry.Key} -> {entry.Value.Length} bytes");
    if (++sampled == 10)
    {
        break;
    }
}
```

### Check the resulting shape

`DiagnoseAsync` reports the live-key and tombstone totals, the shard count, and a
per-shard breakdown, which is how you confirm the load spread across shards as
expected and that no shard is still mid-graft.

```csharp verify
var report = await tree.DiagnoseAsync(deep: true, cancellationToken);
Console.WriteLine(
    $"{report.TotalLiveKeys} live keys, {report.TotalTombstones} tombstones, {report.ShardCount} shards");

foreach (var shard in report.Shards)
{
    Console.WriteLine(
        $"  shard {shard.ShardIndex}: {shard.LiveKeys} keys, depth {shard.Depth}, "
        + $"bulk pending {shard.BulkOperationPending}");
}
```

The report is cached per mode for `LatticeOptions.DiagnosticsCacheTtl` (five
seconds by default), so a diagnose issued immediately after the last chunk can
return a pre-load snapshot. That caching is exactly why `CommitBulkLoadAsync`
counts rather than diagnoses. Wait out the cache window, or count. See
[Diagnostics](diagnostics.md) for the rest of the report.

### Then apply the delta and cut over

Bulk load seeds an empty tree; it is not a continuous replication mechanism. The
usual shape is: pick a cut in the source, bulk-load everything up to it, apply the
changes since the cut with `SetManyAsync` - which tolerates a non-empty tree -
and only then move readers across. Keep the source readable until the spot checks
pass.

## Related documents

- [Bulk Loading](bulk-loading.md) - the bulk-load primitives, how they compare
  with `SetManyAsync`, and the write round-trip budget for a large import.
- [BulkLoading sample](../../samples/BulkLoading/README.md) - a runnable one-shot
  and streaming load.
- [Tree-administration control API](../lattice.api.treeadmin/README.md) - the
  resumable session and the rest of the whole-tree administration surface.
- [Schema enforcement and versioning](../lattice.schema/README.md) - write
  validation, dead-letter diversion, and value versioning with read-time
  upcasting.
- [Tree Storage](tree-storage.md) and [Tree Sizing](tree-sizing.md) - per-provider
  row limits and node size estimation.
- [Compression](compression.md) - the `ILatticeCompressor` seam and where it
  applies.
- [TTL](ttl.md) - re-applying expiry after a migration.
- [Diagnostics](diagnostics.md) - the report `DiagnoseAsync` returns.
