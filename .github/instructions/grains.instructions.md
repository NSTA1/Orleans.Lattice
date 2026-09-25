---
applyTo: "src/lattice/BPlusTree/Grains/**"
---

# Grain Implementation Patterns

## Grain Structure

All grain implementations are **`internal sealed`** (exposed only via interfaces) and use **primary constructors** for DI:

```csharp
internal sealed partial class MyGrain(
    IGrainContext context,
    [PersistentState("stateName", LatticeOptions.StorageProviderName)] IPersistentState<MyState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IMyGrain
```

- Grains are `internal`; the test project has `InternalsVisibleTo` access.
- Use `partial class` to split large grains across multiple files by concern (e.g. `.Lifecycle.cs`, `.Traversal.cs`, `.BulkLoad.cs`).

## Grain Key Conventions

Grain identity is embedded in the string key with `/` as separator:

| Grain | Key format | Example |
|---|---|---|
| `LatticeGrain` | `{treeId}` | `"my-tree"` |
| `ShardRootGrain` | `{treeId}/{shardIndex}` | `"my-tree/0"` |
| `BPlusLeafGrain` | Opaque grain-assigned ID | - |
| `BPlusInternalGrain` | Opaque grain-assigned ID | - |
| `LeafCacheGrain` | `{leafGrainId}` | `"leaf/abc"` |
| `LeafSnapshotStorageGrain` | Guid key matching the source leaf's grain id | - |
| `LatticeRegistryGrain` | Singleton (`_lattice_trees`) | `"_lattice_trees"` |
| `LatticeQueueGrain` | `{queueName}` | `"work-items"` |
| `TombstoneCompactionGrain` | `{treeId}` | `"my-tree"` |
| `TreeDeletionGrain` | `{treeId}` | `"my-tree"` |
| `TreeResizeGrain` | `{treeId}` | `"my-tree"` |
| `TreeReshardGrain` | `{treeId}` | `"my-tree"` |
| `TreeSnapshotGrain` | `{sourceTreeId}` | `"my-tree"` |
| `TreeMergeGrain` | `{targetTreeId}` | `"my-tree"` |
| `AtomicWriteGrain` | `{treeId}/{operationId}` | `"my-tree/ab12…"` |
| `TxRegistryGrain` | `_lattice_txshard_{n}_{treeId}` via `TxRegistryRouting.ShardKey` (sharded; the shard leads inside the reserved `_lattice_` namespace so no tree id can collide with it), or `{treeId}` for the legacy registry that version-4 txids route to | `"_lattice_txshard_3_my-tree"` |
| `TxRegistryHighWaterGrain` | `{treeId}` (one durable shard high-water mark per tree) | `"my-tree"` |
| `LatticeCrossTreeReceiverGrain` | Length-prefixed `{originClusterId}`+`{operationId}` via `ComputeKey` (storage-safe; see below) | `"16_cluster-eus2op-1op-42"` |
| `ViewMaintainerGrain` | `{viewName}`, tenant-scoped to `t/{tenant}/{viewName}` when tenancy is on | `"orders-open"` |
| `WalMaterialiserPinGrain` | `{treeId}` (single shard) or `{treeId}~s{shard}` via `ShardKey` | `"my-tree~s3"` |
| `LatticeCursorGrain` | `{treeId}/{cursorId}` | `"my-tree/ab12…"` |
| `TagIndexReconcileGrain` | `{indexName}` | `"by-color"` |
| `LatticeLockGrain` | `{lockName}` (any non-empty string) | `"inventory/sku-42"` |
| `AtomicActionGrain` | `{operationId}` (caller-supplied idempotency key, any non-empty string) | `"order-4711"` |

Parse the tree ID from the key using `key[..key.LastIndexOf('/')]` when needed.

### Storage-safe compound keys

A grain's string key is its identity, and a **persistent** grain's identity is
carried by keyed storage backends into places that reject certain characters:
Azure Table grain storage puts the key into both the Partition/Row key columns
and the request URL, which forbid the control characters `0x00-0x1F` and
`0x7F-0x9F` and the characters `/`, `\`, `#` and `?`. A persistent grain whose
composite key contains one of these cannot activate on that backend - an opaque
HTTP 400 "Invalid URL" on `ReadStateAsync`/`WriteStateAsync` that no in-memory
test storage reproduces, so the whole suite stays green while a real Azure
deployment fails (issue [#1529](https://github.com/NSTA1/Orleans.Lattice/issues/1529):
`LatticeCrossTreeReceiverGrain` once joined its `(originClusterId, operationId)`
halves with an ASCII Unit Separator `0x1F` and stalled every replicated
cross-tree atomic write on a live Azure estate).

So when a grain both persists via `[PersistentState]` and is addressed by a
**composite** key:

- Never join key parts with a control character or with `/`, `\`, `#`, `?`.
  Prefer a length prefix (as `ComputeKey` now does) or another deterministic,
  unambiguous, control-char-free encoding. `BackupScopeKey` shows the
  percent-encoding approach when a part may itself carry unsafe characters.
- Compose the key in a single `static` method marked `[GrainKeyBuilder]`, never
  inline. The reflection-driven `GrainKeyStorageSafetyContractTestsBase` guard
  (shared testing library) discovers every marked composer per package and fails
  `build-and-test` if its output can contain a storage-unsafe character, so a new
  composer is covered automatically rather than surfacing as a production
  activation failure.
- The rule reaches any **caller-supplied string that ends up inside** a persistent
  grain's key, not only the delimiter. A materialised view's name is interpolated
  into its tree id, which keys `LatticeGrain` and is carried into
  `ShardRootGrain`'s `{treeId}/{shardIndex}`, so the name itself is validated at
  creation (`ViewNameValidator`) rather than trusted. When a composed key needs a
  delimiter that a name could also contain, reserve the delimiter in that
  validation so the composition stays unambiguous - two different inputs must
  never produce one key, or two grain identities collapse onto one state row.
- **Changing a key shape strands durable state**, so a change needs a self-healing
  migration rather than a cutover: keep reading the old shape until the state
  written under it has aged out. `WalMaterialiserPinRouting.EnumerateReadKeys`
  shows the pattern - the WAL GC reads every shard under both the current and the
  legacy separator, so a pin written by an earlier build keeps holding the trim
  floor with no operator action and no stranded segments.
- **Parse a suffix from the end, and validate its shape.** A key composed as
  `{name}{separator}{n}` must be parsed with `LastIndexOf` and only accepted when
  the trailing part actually looks like the suffix (all digits, say); a leading
  `IndexOf` truncates a name that legitimately contains the separator.

## State Management

- Each grain owns a single `IPersistentState<T>` injected via `[PersistentState]`.
- All state classes live in `BPlusTree/State/` and carry `[GenerateSerializer]` + `[Alias]`.
- Always call `state.WriteStateAsync()` after mutations - group writes when possible.

## Options Access

Resolve per-tree options via `IOptionsMonitor<LatticeOptions>`:

```csharp
private LatticeOptions Options => optionsMonitor.Get(TreeId);
```

## Error Handling

- Throw `InvalidOperationException` for invalid state transitions (e.g. writing to a deleted tree).
- Use `ArgumentNullException.ThrowIfNull` at public API boundaries.
- Grains that support split recovery must check `SplitState.SplitInProgress` before performing writes.

## StatelessWorker

`LatticeGrain` is annotated `[StatelessWorker]` - it holds no persistent state and routes requests to the correct `IShardRootGrain` via `LatticeSharding`.

## Placement

Orleans defaults to resource-optimised placement, which prefers the calling silo when silo scores are close and never migrates a long-lived grain afterwards. A grain burst-activated from one silo - every shard root and WAL shard is first reached through the `[StatelessWorker]` `LatticeGrain` on the gateway silo - therefore lands entirely on that silo and stays there (#3348). Hot per-tree singletons that are fanned out to (`ShardRootGrain`, `WalShardGrain`) carry `[Orleans.Placement.RandomPlacement]`; write it fully qualified, because the bare `[RandomPlacement]` resolves to the strategy class and fails with CS0616. `HotGrainPlacementSpreadTests` guards the spread.
