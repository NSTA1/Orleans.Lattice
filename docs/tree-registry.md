# Tree Registry

Lattice maintains an internal **tree registry** — a Lattice tree (`_lattice_trees`) that tracks all user trees and their per-tree configuration overrides.

## How It Works

The registry is itself a Lattice tree with the reserved ID `_lattice_trees`. Each key in the registry is a user tree ID, and each value is a JSON-serialized `TreeRegistryEntry` containing optional `LatticeOptions` overrides.

### Automatic registration

Trees are automatically registered on first write. When a `ShardRootGrain` creates its first root leaf node (the initial `EnsureRootAsync` call), it registers the tree in the registry **before** persisting the root pointer. This ensures:

1. The tree is discoverable before any data exists.
2. The registry write must succeed before the data write proceeds — registration is **not** best-effort.

### System trees

Tree IDs starting with `_lattice_` are reserved for internal use and are excluded from self-registration to avoid circular bootstrap. Currently the only system tree is `_lattice_trees` (the registry itself).

## Configuration Priority

Options are resolved in the following priority order:

1. **Registry override** — per-tree `TreeRegistryEntry` stored in the registry tree (set by `ResizeAsync` or `SnapshotAsync`).
2. **`IOptionsMonitor` named options** — per-tree overrides registered via `ConfigureLattice("tree-name", ...)` at silo startup.
3. **`IOptionsMonitor` global defaults** — defaults registered via `ConfigureLattice(...)`.
4. **Hardcoded defaults** — `MaxLeafKeys = 128`, `MaxInternalChildren = 128`, `ShardCount = 64`.

Registry overrides only apply to the properties that are set (non-null). All other properties fall back to the `IOptionsMonitor` chain.

`ShardRootGrain` reads the registry once on activation and caches the effective options for the grain's lifetime. This adds one async call per grain activation but zero overhead on subsequent operations.

## Tree Enumeration

Use `GetAllTreeIdsAsync` to list all registered trees:

```csharp
var tree = grainFactory.GetGrain<ILattice>("any-tree-id");
var allIds = await tree.GetAllTreeIdsAsync();
```

## Tree Existence Check

Use `TreeExistsAsync` to check whether a specific tree is registered:

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");
bool exists = await tree.TreeExistsAsync();
```

## Lifecycle Integration

| Operation | Registry effect |
|---|---|
| First write to a new tree | Tree registered (key added) |
| `ResizeAsync` snapshot phase | New physical tree registered via snapshot (visible in `GetAllTreeIdsAsync`) |
| `ResizeAsync` swap phase | Registry entry updated with new sizing + `PhysicalTreeId` alias set |
| `ResizeAsync` cleanup phase | Old physical tree soft-deleted; removed from registry on purge |
| `UndoResizeAsync` | Alias removed, old tree recovered, new tree deleted (removed from registry on purge) |
| `SnapshotAsync` initiation | Destination tree registered (visible in `GetAllTreeIdsAsync` with optional sizing overrides) |
| `DeleteTreeAsync` + purge completion | Tree unregistered (key removed) |
| `BulkLoadAsync` | Tree registered on first shard write |

> **Note:** Physical trees created by `ResizeAsync` (e.g. `my-tree/resized/abc123`) and `SnapshotAsync` are regular registered trees and appear in `GetAllTreeIdsAsync` results. This is by design — it allows monitoring and manual intervention. When the old physical tree is purged after the `SoftDeleteDuration` window, it is automatically unregistered.

## Tree Aliasing

A `TreeRegistryEntry` can contain a `PhysicalTreeId` field that redirects a logical tree ID to a different physical tree. This is used internally by `ResizeAsync` to atomically swap a tree's data to a new physical tree with different sizing.

### How aliasing works

1. `LatticeGrain` resolves the alias once per activation via `ILatticeRegistry.ResolveAsync(treeId)`.
2. If `PhysicalTreeId` is set, all shard routing uses the physical tree ID instead of the logical tree ID.
3. Only a single level of indirection is allowed — the physical tree must not itself be aliased. `SetAliasAsync` enforces this constraint.

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after an alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### API

```csharp
var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

// Set alias: logical "my-tree" → physical "my-tree/resized/abc123"
await registry.SetAliasAsync("my-tree", "my-tree/resized/abc123");

// Resolve: returns "my-tree/resized/abc123" (or "my-tree" if no alias)
string physicalId = await registry.ResolveAsync("my-tree");

// Remove alias: reverts to using "my-tree" as the physical ID
await registry.RemoveAliasAsync("my-tree");
```

## Shard Map

A `TreeRegistryEntry` can also carry a per-tree `ShardMap` that maps virtual shard slots to physical shard indices. The shard map decouples logical key routing from the physical shard count: keys hash into a large fixed virtual space (`VirtualShardCount`, default 4096), and the `ShardMap.Slots` array collapses ranges of virtual slots onto physical shards.

When no shard map is persisted (the default state for newly created trees), `LatticeGrain` materialises an identity map (`slot[i] = i % shardCount`) which preserves the legacy `XxHash32(key) % shardCount` routing bit-for-bit. Custom shard maps are written by topology-changing operations (e.g. future adaptive shard splits) and are cached by `LatticeGrain` for the activation's lifetime — invalidated together with the physical-tree-ID cache when a shard signals a stale alias.

### API

```csharp
var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

// Read the persisted shard map for a tree (null if none).
ShardMap? map = await registry.GetShardMapAsync("my-tree");

// Persist a new shard map for a tree.
await registry.SetShardMapAsync("my-tree", new ShardMap { Slots = [0, 0, 1, 1] });
```

## Accessing the Registry Grain Directly

For advanced scenarios, the `ILatticeRegistry` grain can be accessed directly:

```csharp
var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
bool exists = await registry.ExistsAsync("my-tree");
var entry = await registry.GetEntryAsync("my-tree");
var allIds = await registry.GetAllTreeIdsAsync();
