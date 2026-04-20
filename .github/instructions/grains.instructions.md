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
| `BPlusLeafGrain` | Opaque grain-assigned ID | — |
| `BPlusInternalGrain` | Opaque grain-assigned ID | — |
| `LeafCacheGrain` | `{leafGrainId}` | `"leaf/abc"` |
| `LatticeRegistryGrain` | Singleton (`_lattice_trees`) | `"_lattice_trees"` |
| `TombstoneCompactionGrain` | `{treeId}` | `"my-tree"` |
| `TreeDeletionGrain` | `{treeId}` | `"my-tree"` |
| `TreeResizeGrain` | `{treeId}` | `"my-tree"` |
| `TreeReshardGrain` | `{treeId}` | `"my-tree"` |
| `TreeSnapshotGrain` | `{sourceTreeId}` | `"my-tree"` |
| `TreeMergeGrain` | `{targetTreeId}` | `"my-tree"` |
| `AtomicWriteGrain` | `{treeId}/{operationId}` | `"my-tree/ab12…"` |
| `LatticeCursorGrain` | `{treeId}/{cursorId}` | `"my-tree/ab12…"` |

Parse the tree ID from the key using `key[..key.LastIndexOf('/')]` when needed.

## State Management

- Each grain owns a single `IPersistentState<T>` injected via `[PersistentState]`.
- All state classes live in `BPlusTree/State/` and carry `[GenerateSerializer]` + `[Alias]`.
- Always call `state.WriteStateAsync()` after mutations — group writes when possible.

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

`LatticeGrain` is annotated `[StatelessWorker]` — it holds no persistent state and routes requests to the correct `IShardRootGrain` via `LatticeSharding`.
