# Tree Registry

Lattice maintains an internal **tree registry** - a Lattice tree (`_lattice_trees`) that tracks all user trees and their per-tree configuration overrides.

## How It Works

The registry is itself a Lattice tree with the reserved ID `_lattice_trees`. Each key in the registry is a user tree ID, and each value is that tree's JSON-serialized registry entry: its structural sizing pins, an optional physical-tree alias and shard map, optional per-tree runtime overrides of `LatticeOptions` settings, an optional per-tree durable-history retention policy, and bookkeeping - the pinned WAL partition count and WAL placement, the highest physical shard index adaptive splits have allocated, the projection-digest permanent-disable latch, and two provenance markers: on a physical copy created to back a logical tree, the logical tree it backs (see [Provenance](#provenance-derivedfrom)), and on a restore's shadow tree, the tree it was restored for.

### Automatic registration

Trees are automatically registered on first use - not only on the first write. The first time anything resolves the options of a tree that has no registry entry, it registers the tree with the default structural pins, and the first operation of any kind (a read included) that reaches one of the tree's shard roots creates that shard's root leaf. When a shard root creates its first root leaf node, it registers the tree in the registry **before** persisting the root pointer. This ensures:

1. The tree is discoverable before any data exists.
2. The registry write must succeed before the data write proceeds - registration is **not** best-effort.

### System trees

Tree IDs starting with `_lattice_` are reserved for internal use and are excluded from self-registration to avoid circular bootstrap. The system trees are the registry itself (`_lattice_trees`), the replication package's write-ahead-log trees (`_lattice_replog_*`), and the trees backing cluster-internal queues (`_lattice_queue_*`).

### Reserved tree-ID namespaces

Three tree-ID prefixes, and one literal tree ID, are reserved and cannot be created by application code through the public `ILattice` surface. Each guard throws `LatticeReservedTreeNamespaceException`, which derives from `InvalidOperationException`:

| Prefix | Purpose | Guard |
| --- | --- | --- |
| `_lattice_` | Internal library trees (the registry `_lattice_trees`, the replication WAL `_lattice_replog_*`, the queue trees `_lattice_queue_*`). Never user-addressable. | Every public `ILattice` call (read or write) throws; internal code reaches these trees through a separate, silo-internal grain interface that bypasses the guard. |
| `sys-` | Dogfooded **system-data** trees owned by first-party add-ons: authorization (`sys-auth-*`), backup (`sys-backup-*`), membership (`sys-membership-*`), schema (`sys-schema-*`), tenancy (`sys-tenant-*`), installable apps (`sys-app-*`), and replication configuration (`sys-replication-config`). These are real, individually inspectable trees. | A user-origin **write** (create/mutate) to a `sys-`-prefixed tree throws. Reads are allowed, and first-party add-ons create and mutate their own `sys-` trees under an internal system-origin scope. |
| `t/` | The structural tenant namespace: a tenant's trees are named `t/{tenantId}/{name}` and composed by the [tenancy](../lattice.tenancy/README.md) layer, never named by a user directly. | A user-origin **write** to a `t/`-prefixed tree throws unless it names a tree of the caller's active tenant (the id the tenancy layer composes); with no tenancy layer registered the namespace is wholly uncreatable. |
| `*` (literal ID) | The all-trees authorization sentinel, which the authorization layer promotes to a cluster-wide grant tier. | A user-origin **write** to a tree named exactly `*` throws. |

Materialised-view trees (`view-{name}`) are guarded separately and more tightly: a direct public write to a `view-` tree that does not come from the view maintainer, and a direct content read that does not come through an `ILatticeView` handle, both throw `InvalidOperationException`, because a view is derived state whose active generation a rebuild can swap underneath a raw bind. See [Materialised Views](materialised-views.md).

The `sys-`, `t/`, and `*` guards are enforced only on the data-mutation surface (writes, deletes, CRDT apply, bulk load) and only outside a system-origin scope, so a user cannot accidentally seed a tree that collides with a first-party add-on's namespace, while operators can still read those trees (for example through the State API, which hides `sys-` trees from the default catalog listing but exposes them when `IncludeSystemTrees` is set).

## Configuration Priority

Structural sizing and runtime settings resolve differently:

- **Structural sizing** (`MaxLeafKeys`, `MaxInternalChildren`, `ShardCount`) comes only from the registry entry. A new tree's entry is seeded with the hardcoded defaults - `MaxLeafKeys = 128`, `MaxInternalChildren = 128`, `ShardCount = 64` - for any pin its registration does not supply; an existing entry is never rewritten, so a pin it lacks resolves to the default. `IOptionsMonitor` plays no part (`LatticeOptions` does not expose these properties), and the pins change only through `ResizeAsync` and `ReshardAsync` (see [Tree Sizing](tree-sizing.md)).
- **Runtime settings** resolve in priority order:
  1. **Registry override** - a per-tree override on the registry entry, for the settings that have one (for example the WAL partition pin, publish-events, projection-digest maintenance, `MaxCacheValueBytes`, and `WalMaxRetainedBytes`).
  2. **`IOptionsMonitor` named options** - per-tree overrides registered via `ConfigureLattice("tree-name", ...)` at silo startup.
  3. **`IOptionsMonitor` global defaults** - defaults registered via `ConfigureLattice(...)`.

Registry overrides only apply to the properties that are set (non-null). All other properties fall back to the `IOptionsMonitor` chain.

`ShardRootGrain` reads the registry once on activation and caches the effective options for the grain's lifetime. This adds one async call per grain activation but zero overhead on subsequent operations.

Option resolutions reach the singleton registry through a bounded, coalescing path. Concurrent resolves of the same tree on a silo share one in-flight read, and at most 16 registry reads are in flight at once across the whole cluster - each silo takes an equal share of that ceiling, and never less than one, so a cluster of more than 16 silos has one read in flight per silo - with whatever queues behind the bound read together in batches of up to 64 trees. A cold start that activates many trees' background services therefore does not stampede the registry, and a quiet silo, whose reads never queue, pays no added latency.

## Tree Enumeration

Use `GetAllTreeIdsAsync` to list the registered trees - every one except the reserved `_lattice_` system trees, pruned to the active tenant's trees when tenancy is on. The call authorizes a whole-tree read of the tree it is issued through:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("any-tree-id");
var allIds = await tree.GetAllTreeIdsAsync();
```

## Tree Existence Check

Use `TreeExistsAsync` to check whether a specific tree is registered. A caller the access gate denies any read of the tree gets `false`, exactly as for an unregistered tree; a caller allowed to read even part of it gets the real answer:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
bool exists = await tree.TreeExistsAsync();
```

## Lifecycle Integration

| Operation | Registry effect |
|---|---|
| First use of a new tree (anything that resolves its options, or any operation that reaches a shard root) | Tree registered (key added), with its structural pins seeded |
| `ResizeAsync` snapshot phase | New physical tree registered via snapshot (visible in `GetAllTreeIdsAsync`), with `DerivedFrom` set to the logical tree id, carrying the logical tree's shard map and split allocation mark, and filled from every source shard that map routes to, each into the shard with the same index - see [Tree Sizing](tree-sizing.md#how-it-works) |
| `ResizeAsync` swap phase | Registry entry rewritten with the new sizing and the pinned `ShardCount`, keeping the tree's configuration overrides, taking the shard map (under a fresh `Version`) and split allocation mark from the new physical tree's entry, and dropping the old physical tree's WAL layout; the alias is then pointed at the new physical tree |
| `ResizeAsync` cleanup phase | Old physical tree retired: its shards are soft-deleted as physical maintenance and it is removed from the registry on purge. A previous resize's copy first has the logical tree's shard map and split allocation mark written to its own entry, so the retirement reaches every shard a split added. On a tree's first resize, the old physical tree's ID is the logical tree ID, so the purge keeps the logical tree's entry. Retirement publishes no tree lifecycle events and does not make the logical tree read as deleted |
| `UndoResizeAsync` | After the swap: alias removed, original entry restored, and the old tree recovered if the resize had already soft-deleted it. Either side of the swap, the new tree is discarded (its WAL retention released at once, removed from registry on purge) |
| `SnapshotAsync` initiation | Destination tree registered (visible in `GetAllTreeIdsAsync` with optional sizing overrides); the source's alias is resolved and the physical tree it points at is the one copied, every shard the source's shard map routes to included; the destination carries that shard map and split allocation mark (see [Snapshots](snapshots.md#requirements)) |
| Adaptive shard split | Shard map rewritten under a fresh `Version`; the next physical shard index to allocate advanced |
| Shard consolidation (automatic over-split healing) | Shard map rewritten under a fresh `Version`, reassigning the donor's slots to the survivor |
| `ReshardAsync` | Shard map grown by the splits it drives; `ShardCount` pin updated when it completes (or at once on an empty tree) |
| [`ILatticeTreeAdmin.CreateTreeAsync`](../lattice.api.treeadmin/README.md) | Tree registered with the supplied sizing pins (honoured only on first creation) |
| Shadow-cutover restore (`ILatticeTreeAdmin.RestoreTreeAsync`) | Shadow tree registered with `DerivedFrom` set to the target tree; alias pointed at the restored shadow tree; a revert points it back |
| Schema remediation cut-over | Remediated copy registered with `DerivedFrom` set to the remediated tree; alias pointed at it |
| `DeleteTreeAsync` / `RecoverTreeAsync` | On an aliased tree, the delete resolves the live backing tree the alias targets, checks that it is owned by this logical tree, pins it and marks it deleted; a recover unmarks the pinned tree. See [Logical lifecycle across an alias](#logical-lifecycle-across-an-alias) |
| `DeleteTreeAsync` + purge completion | Tree unregistered (key removed). On an aliased tree the pinned backing tree's state is purged too, and both the backing tree and the logical tree are unregistered |
| `BulkLoadAsync` | Tree registered on first shard write |

> **Note:** Physical trees created by `ResizeAsync` (e.g. `my-tree/resized/abc123`) and `SnapshotAsync` are regular registered trees and appear in `GetAllTreeIdsAsync` results. This is by design - it allows monitoring and manual intervention. When the old physical tree is purged after the `SoftDeleteDuration` window, it is automatically unregistered - unless it is a first resize's retired copy, whose ID is the logical tree ID: only its shards are purged, and the logical tree stays registered.

## Tree Aliasing

A tree's registry entry can carry a physical-tree alias that redirects a logical tree ID to a different physical tree. `ResizeAsync` uses it to switch a tree atomically onto a copy of its data at a different sizing (see [Tree Sizing](tree-sizing.md#how-it-works) for what the copy does not carry); a shadow-cutover restore uses it to switch a tree onto its restored copy (and back, on revert); and the [schema package](../lattice.schema/README.md)'s background remediation uses it to cut a tree over to its remediated copy.

### How aliasing works

1. `LatticeGrain` resolves the alias once per activation via `ILatticeRegistry.ResolveAsync(treeId)`.
2. If `PhysicalTreeId` is set, all shard routing uses the physical tree ID instead of the logical tree ID.
3. Only a single level of indirection is allowed - the physical tree must not itself be aliased. `SetAliasAsync` enforces this constraint.

Tree deletion, recovery and purge resolve the alias too, but only to a target the logical tree owns; see [Logical lifecycle across an alias](#logical-lifecycle-across-an-alias) and [Deleting an aliased tree](tree-deletion.md#deleting-an-aliased-tree).

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after an alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### API

Resize (and its undo), restore, and schema remediation drive the alias from inside the silo; the registry itself is internal infrastructure. For operators, the [tree-administration facade](../lattice.api.treeadmin/README.md) exposes `ILatticeTreeAdmin.SetTreeAliasAsync`, which points a logical tree at a physical tree after authorizing whole-tree administration on both the logical tree and its target, and `ILatticeTreeAdmin.ResolveTreeAliasAsync`, which returns the physical id (or the logical id when no alias is set); it offers no verb that removes an alias. Underneath, the registry exposes four operations:

- **Set** - points a logical tree id at a physical tree id, after verifying that the target differs from the logical id, is not itself aliased, and would not widen the caller's effective privilege (for example a `_lattice_` system tree, or a `sys-` tree aliased from outside the `sys-` namespace). It is also refused while either tree - or the logical tree the target was derived from - is logically deleted or has a logical delete in flight, and when the registered [ownership guard](#ownership-bounded-aliasing) denies it.
- **Resolve** - returns the physical id, or the logical id unchanged when no alias is registered.
- **Remove** - clears the alias, reverting to the logical id. It is refused while the logical tree is logically deleted or has a logical delete in flight.
- **Aliases targeting** - returns, in ordinal order, every logical tree id whose alias currently targets a given physical tree. It scans the authoritative registry entries rather than a separately maintained index, so it cannot go stale. Logical deletion uses it to refuse a physical tree that another tree still aliases.

### Provenance: `DerivedFrom`

A physical tree created to back a logical tree records that logical tree's id in its registry entry's `DerivedFrom` field, stamped once at creation: a resize's new copy, a shadow-cutover restore's shadow tree, and a schema remediation's remediated copy all carry it. A tree registered any other way - an ordinary tree, a standalone `SnapshotAsync` destination, or an entry written before the field existed - has no `DerivedFrom` and is an independent tree. Registration is first-writer-wins, so the field is never backfilled or rewritten, and every consumer treats a missing value as "not derived" and fails closed.

### Ownership-bounded aliasing

An alias must not let one owner's logical tree read or write another owner's data. Core carries no notion of ownership itself, so the registry consults an optional `ITreeOwnershipGuard` on every alias assignment - including system-origin maintenance such as resize, restore and remediation, which are not exempt - after the namespace, target-control and lifecycle checks and before anything is written or published. The guard receives the logical tree id, the physical target id, and the target's `DerivedFrom` read from the registry (never supplied by the caller), and returns a `TreeOwnershipDecision`: `TreeOwnershipDecision.Allow()` lets the alias proceed, and `TreeOwnershipDecision.Deny(reason)` refuses it. The default value of `TreeOwnershipDecision` denies. A refusal surfaces as `LatticeTreeOwnershipDeniedException` carrying the guard's reason, and the gRPC tree-administration binding maps it to `PermissionDenied`. A failure thrown by the guard propagates without writing the alias.

`AddLattice` registers an allow-all guard, so a host without an ownership provider behaves exactly as before. An add-on replaces it; the [installable apps package](../lattice.apps/README.md) registers a guard backed by its tree ownership ledger. The guard runs inside the registry's mutation turn, so an implementation must not call registry mutations or range scans (point reads are fine), and its denial reasons must be safe to show to the caller.

```csharp verify
public sealed class SingleOwnerGuard : ITreeOwnershipGuard
{
    public ValueTask<TreeOwnershipDecision> AuthorizeAliasAsync(
        string logicalTreeId, string physicalTreeId, string? derivedFrom,
        CancellationToken cancellationToken = default)
        => new(derivedFrom is null || derivedFrom == logicalTreeId
            ? TreeOwnershipDecision.Allow()
            : TreeOwnershipDecision.Deny("The target was created for a different tree."));
}
```

### Logical lifecycle across an alias

`DeleteTreeAsync`, `RecoverTreeAsync` and `PurgeTreeAsync` act on the **logical** tree. On an aliased tree they resolve the live backing tree the alias targets and apply the operation to it, after checking that the target is owned by this logical tree: its `DerivedFrom` must equal the logical tree id, and no other logical tree may alias it. An administrative alias to an independent tree, or to a tree another logical tree also aliases, is refused rather than deleted through the wrong name. The resolved target is pinned in durable state before any physical effect, so a retry after an ambiguous failure continues on the same target instead of re-resolving.

A logical purge removes the backing tree's state and unregisters both the backing tree and the logical tree, so `TreeExistsAsync` then reports the tree gone. A resize's retirement of its old copy is physical maintenance: it is not a logical delete, the live tree does not read as deleted during the retirement window, and a public `RecoverTreeAsync` on a live resized tree is refused.

Alias-changing operations and logical deletion never overlap. Resize (and its undo), shadow-cutover restore and revert, and schema remediation each hold a durable reservation, keyed by an operation id, on the tree while they run - a restore from before it writes its restore copy; a delete, and any of these operations under a different operation id, refuses while one is held, and an alias change refuses while the tree is logically deleted or a delete is pending. An administrative alias change takes no reservation and is not refused by one. Reservations never expire by time: each is released by the owning operation, by the matching id, and releasing an absent or different id is a no-op. A shadow-cutover restore or revert releases its reservation only when it completes, so one that fails part-way leaves the tree reserved until it is retried to completion or, for a restore, its unfinished restore copy is deleted - see [Deleting an aliased tree](tree-deletion.md#deleting-an-aliased-tree).

### Identity seen by observers and metrics

Routing through an alias does not change the tree identity reported to observers. [Mutation observers](api.md#mutation-observers) receive the logical tree id in `LatticeMutation.TreeId` for every write routed through the logical tree, across resize, restore and remediation; a write that reaches a physical copy by another path - such as a write an online resize mirrors into the new copy while it fills it - is reported under that copy's own id. The per-tree `tree` dimension on [metrics](metrics.md#tag-conventions) stays the logical id too. The WAL itself records the physical tree it belongs to.

## Shard Map

A tree's registry entry can also carry a per-tree `ShardMap` that maps virtual shard slots to physical shard indices. The shard map decouples logical key routing from the physical shard count: keys hash into a virtual space of as many slots as the tree's shard map holds, and the `ShardMap.Slots` array collapses ranges of virtual slots onto physical shards. That is 4096 slots unless the tree was created by an installed app whose manifest declares a `virtualShardCount` (`AppTreeDeclaration.VirtualShardCount`): the tree's first registration persists a map with the declared slot count, which a resize carries over to the resized copy and a reshard keeps (an empty tree's map is rebuilt over the same slot count).

When no shard map is persisted (the default state for newly created trees), the router materialises an identity map (`slot[i] = i % shardCount`), which routes exactly as the legacy `XxHash32(key) % shardCount` did whenever the virtual slot count is a whole multiple of the shard count, as it is for the default 64 shards. Custom shard maps are written by topology-changing operations - adaptive shard splits (including those an online reshard drives), shard consolidation, and an empty-tree reshard's re-pin - and by an installed app's tree registration when its declaration pins the virtual slot count; they are cached by the router, which drops its copy when a shard reports stale shard routing (a split or consolidation has moved slots) and, together with the physical-tree-ID cache, when a shard signals a stale alias.

### API

The registry grain and its tree are internal infrastructure; they are not part of the public Orleans.Lattice API. The router reads the shard map through them, and topology-changing coordinators write it. Conceptually the registry offers three shard-map operations:

- **Get** - returns the persisted `ShardMap` for a tree, or `null` if none has been written. A never-persisted tree behaves as an identity map with `Version = 0`.
- **Set** - replaces the map wholesale and atomically stamps a fresh monotonic `Version` on it (the caller-supplied `Version` is ignored).
- **Reassign slots** - re-reads the live map, points the given virtual slots at a target physical shard, and persists the result under a fresh `Version`, all in one call. Adaptive splits and shard consolidation use it, so their concurrent swaps compose instead of one erasing the other.

For operators, the [tree-administration facade](../lattice.api.treeadmin/README.md)'s `ILatticeTreeAdmin.GetShardMapAsync` reads the persisted map; it has no verb that writes one.

### Monotonic `ShardMap.Version`

Every shard-map write - a set or a slot reassignment - increments `ShardMap.Version` by one, so the first such write stamps `Version = 1`. The default identity map materialised in memory for never-persisted trees has `Version = 0`, and so does the map an installed app's tree registration persists for a declared `virtualShardCount`, until its first set or reassignment. A resize's alias swap drops the persisted map along with the rest of the retired copy's layout, so the resized tree starts again from the identity map at `Version = 0` and the first map it persists is `Version = 1`; an undo restores the original entry, map and version included.

`LatticeGrain` uses this version as a stability hint for scans (`CountAsync`, `ScanKeysAsync`, `ScanEntriesAsync`): a scan records the version when it starts and re-reads it before returning. If the version moved, the scan retries up to `LatticeOptions.MaxScanRetries` times. Because the registry grain is non-reentrant, the version increment is atomic with the shard-map write, so concurrent splits cannot produce torn maps or out-of-order version stamps. See [Shard Splitting](shard-splitting.md#scan-semantics-during-a-split) for the algorithm and [Consistency](consistency.md) for the resulting per-operation guarantees.
