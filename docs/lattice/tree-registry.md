# Tree Registry

Lattice maintains an internal **tree registry** - a Lattice tree (`_lattice_trees`) that tracks all user trees and their per-tree configuration overrides.

## How It Works

The registry is itself a Lattice tree with the reserved ID `_lattice_trees`. Each key in the registry is a user tree ID, and each value is that tree's JSON-serialized registry entry: its structural sizing pins, an optional physical-tree alias and shard map, and optional per-tree runtime overrides of `LatticeOptions` settings.

### Automatic registration

Trees are automatically registered on first use - not only on the first write. The first time anything resolves a tree's options it seeds any missing structural pin, which registers the tree, and the first operation of any kind (a read included) that reaches one of the tree's shard roots creates that shard's root leaf. When a shard root creates its first root leaf node, it registers the tree in the registry **before** persisting the root pointer. This ensures:

1. The tree is discoverable before any data exists.
2. The registry write must succeed before the data write proceeds - registration is **not** best-effort.

### System trees

Tree IDs starting with `_lattice_` are reserved for internal use and are excluded from self-registration to avoid circular bootstrap. The system trees are the registry itself (`_lattice_trees`), the replication package's write-ahead-log trees (`_lattice_replog_*`), and the trees backing cluster-internal queues (`_lattice_queue_*`).

### Reserved tree-ID namespaces

Three tree-ID prefixes, and one literal tree ID, are reserved and cannot be created by application code through the public `ILattice` surface. Each guard throws `LatticeReservedTreeNamespaceException`, which derives from `InvalidOperationException`:

| Prefix | Purpose | Guard |
| --- | --- | --- |
| `_lattice_` | Internal library trees (the registry `_lattice_trees`, the replication WAL `_lattice_replog_*`, the queue trees `_lattice_queue_*`). Never user-addressable. | Every public `ILattice` call (read or write) throws; internal code reaches these trees through a separate, silo-internal grain interface that bypasses the guard. |
| `sys-` | Dogfooded **system-data** trees owned by first-party add-ons: authorization (`sys-auth-*`), backup (`sys-backup-*`), membership (`sys-membership-*`), schema (`sys-schema-*`), tenancy (`sys-tenant-*`), and replication configuration (`sys-replication-config`). These are real, individually inspectable trees. | A user-origin **write** (create/mutate) to a `sys-`-prefixed tree throws. Reads are allowed, and first-party add-ons create and mutate their own `sys-` trees under an internal system-origin scope. |
| `t/` | The structural tenant namespace: a tenant's trees are named `t/{tenantId}/{name}` and composed by the [tenancy](../lattice.tenancy/README.md) layer, never named by a user directly. | A user-origin **write** to a `t/`-prefixed tree throws unless it names a tree of the caller's active tenant (the id the tenancy layer composes); with no tenancy layer registered the namespace is wholly uncreatable. |
| `*` (literal ID) | The all-trees authorization sentinel, which the authorization layer promotes to a cluster-wide grant tier. | A user-origin **write** to a tree named exactly `*` throws. |

The `sys-`, `t/`, and `*` guards are enforced only on the data-mutation surface (writes, deletes, CRDT apply, bulk load) and only outside a system-origin scope, so a user cannot accidentally seed a tree that collides with a first-party add-on's namespace, while operators can still read those trees (for example through the State API, which hides `sys-` trees from the default catalog listing but exposes them when `IncludeSystemTrees` is set).

## Configuration Priority

Structural sizing and runtime settings resolve differently:

- **Structural sizing** (`MaxLeafKeys`, `MaxInternalChildren`, `ShardCount`) comes only from the registry entry. A missing pin is seeded from the hardcoded defaults - `MaxLeafKeys = 128`, `MaxInternalChildren = 128`, `ShardCount = 64` - the first time the tree's options are resolved. `IOptionsMonitor` plays no part (`LatticeOptions` does not expose these properties), and the pins change only through `ResizeAsync` and `ReshardAsync` (see [Tree Sizing](tree-sizing.md)).
- **Runtime settings** resolve in priority order:
  1. **Registry override** - a per-tree override on the registry entry, for the settings that have one (for example the WAL partition pin, publish-events, projection-digest maintenance, `MaxCacheValueBytes`, and `WalMaxRetainedBytes`).
  2. **`IOptionsMonitor` named options** - per-tree overrides registered via `ConfigureLattice("tree-name", ...)` at silo startup.
  3. **`IOptionsMonitor` global defaults** - defaults registered via `ConfigureLattice(...)`.

Registry overrides only apply to the properties that are set (non-null). All other properties fall back to the `IOptionsMonitor` chain.

`ShardRootGrain` reads the registry once on activation and caches the effective options for the grain's lifetime. This adds one async call per grain activation but zero overhead on subsequent operations.

Option resolutions reach the singleton registry through a bounded, coalescing path. Concurrent resolves of the same tree on a silo share one in-flight read, and across the whole cluster at most 16 registry reads are in flight at once - each silo takes a share of that ceiling, and never less than one - with whatever queues behind the bound read together in batches of up to 64 trees. A cold start that activates many trees' background services therefore does not stampede the registry, and a quiet silo, whose reads never queue, pays no added latency.

## Tree Enumeration

Use `GetAllTreeIdsAsync` to list all registered trees:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("any-tree-id");
var allIds = await tree.GetAllTreeIdsAsync();
```

## Tree Existence Check

Use `TreeExistsAsync` to check whether a specific tree is registered:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
bool exists = await tree.TreeExistsAsync();
```

## Lifecycle Integration

| Operation | Registry effect |
|---|---|
| First use of a new tree (anything that resolves its options, or any operation that reaches a shard root) | Tree registered (key added), with its structural pins seeded |
| `ResizeAsync` snapshot phase | New physical tree registered via snapshot (visible in `GetAllTreeIdsAsync`) |
| `ResizeAsync` swap phase | Registry entry updated with new sizing + `PhysicalTreeId` alias set |
| `ResizeAsync` cleanup phase | Old physical tree soft-deleted; removed from registry on purge |
| `UndoResizeAsync` | Alias removed, old tree recovered, new tree deleted (removed from registry on purge) |
| `SnapshotAsync` initiation | Destination tree registered (visible in `GetAllTreeIdsAsync` with optional sizing overrides) |
| Adaptive shard split | Shard map rewritten under a fresh `Version`; the next physical shard index to allocate advanced |
| `ReshardAsync` | Shard map grown by the splits it drives; `ShardCount` pin updated when it completes (or at once on an empty tree) |
| [`ILatticeTreeAdmin.CreateTreeAsync`](../lattice.api.treeadmin/README.md) | Tree registered with the supplied sizing pins (honoured only on first creation) |
| Shadow-cutover restore (`ILatticeTreeAdmin.RestoreTreeAsync`) | Alias pointed at the restored shadow tree; a revert points it back |
| `DeleteTreeAsync` + purge completion | Tree unregistered (key removed) |
| `BulkLoadAsync` | Tree registered on first shard write |

> **Note:** Physical trees created by `ResizeAsync` (e.g. `my-tree/resized/abc123`) and `SnapshotAsync` are regular registered trees and appear in `GetAllTreeIdsAsync` results. This is by design - it allows monitoring and manual intervention. When the old physical tree is purged after the `SoftDeleteDuration` window, it is automatically unregistered.

## Tree Aliasing

A tree's registry entry can carry a physical-tree alias that redirects a logical tree ID to a different physical tree. `ResizeAsync` uses it to atomically swap a tree's data onto a new physical tree with different sizing; a shadow-cutover restore uses it to switch a tree onto its restored copy (and back, on revert); and the [schema package](../lattice.schema/README.md)'s background remediation uses it to cut a tree over to its remediated copy.

### How aliasing works

1. `LatticeGrain` resolves the alias once per activation via `ILatticeRegistry.ResolveAsync(treeId)`.
2. If `PhysicalTreeId` is set, all shard routing uses the physical tree ID instead of the logical tree ID.
3. Only a single level of indirection is allowed - the physical tree must not itself be aliased. `SetAliasAsync` enforces this constraint.

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after an alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### API

Resize (and its undo), restore, and schema remediation drive the alias from inside the silo; the registry itself is internal infrastructure. For operators, the [tree-administration facade](../lattice.api.treeadmin/README.md) exposes `ILatticeTreeAdmin.SetTreeAliasAsync`, which points a logical tree at a physical tree after authorizing whole-tree administration on both the logical tree and its target, and `ILatticeTreeAdmin.ResolveTreeAliasAsync`, which returns the physical id (or the logical id when no alias is set); it offers no verb that removes an alias. Underneath, the registry exposes three operations:

- **Set** - points a logical tree id at a physical tree id, after verifying that the target differs from the logical id, is not itself aliased, and would not widen the caller's effective privilege (for example a `_lattice_` system tree, or a `sys-` tree aliased from outside the `sys-` namespace).
- **Resolve** - returns the physical id, or the logical id unchanged when no alias is registered.
- **Remove** - clears the alias, reverting to the logical id.

## Shard Map

A `TreeRegistryEntry` can also carry a per-tree `ShardMap` that maps virtual shard slots to physical shard indices. The shard map decouples logical key routing from the physical shard count: keys hash into a large fixed virtual space (`LatticeConstants.DefaultVirtualShardCount`, fixed at 4096), and the `ShardMap.Slots` array collapses ranges of virtual slots onto physical shards.

When no shard map is persisted (the default state for newly created trees), the router materialises an identity map (`slot[i] = i % shardCount`) which preserves the legacy `XxHash32(key) % shardCount` routing bit-for-bit. Custom shard maps are written by topology-changing operations - adaptive shard splits (including those an online reshard drives), shard consolidation, and an empty-tree reshard's re-pin - and are cached by the router for the activation's lifetime, invalidated together with the physical-tree-ID cache when a shard signals a stale alias.

### API

The registry grain and its tree are internal infrastructure; they are not part of the public Orleans.Lattice API. The router reads the shard map through them, and topology-changing coordinators write it. Conceptually the registry offers three shard-map operations:

- **Get** - returns the persisted `ShardMap` for a tree, or `null` if none has been written. A never-persisted tree behaves as an identity map with `Version = 0`.
- **Set** - replaces the map wholesale and atomically stamps a fresh monotonic `Version` on it (the caller-supplied `Version` is ignored).
- **Reassign slots** - re-reads the live map, points the given virtual slots at a target physical shard, and persists the result under a fresh `Version`, all in one call. Adaptive splits and shard consolidation use it, so their concurrent swaps compose instead of one erasing the other.

For operators, the [tree-administration facade](../lattice.api.treeadmin/README.md)'s `ILatticeTreeAdmin.GetShardMapAsync` reads the persisted map; it has no verb that writes one.

### Monotonic `ShardMap.Version`

Every shard-map write - a set or a slot reassignment - increments `ShardMap.Version` by one, starting from 1 on the first persist. The default identity map materialised in memory for never-persisted trees has `Version = 0`.

`LatticeGrain` uses this version as a stability hint for scans (`CountAsync`, `ScanKeysAsync`, `ScanEntriesAsync`): a scan records the version when it starts and re-reads it before returning. If the version moved, the scan retries up to `LatticeOptions.MaxScanRetries` times. Because the registry grain is non-reentrant, the version increment is atomic with the shard-map write, so concurrent splits cannot produce torn maps or out-of-order version stamps. See [Shard Splitting](shard-splitting.md#scan-semantics-during-a-split) for the algorithm and [Consistency](consistency.md) for the resulting per-operation guarantees.
