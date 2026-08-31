# API Reference

The public surface of `Orleans.Lattice.GrainIndex`. Types live in the
`Orleans.Lattice.GrainIndex` namespace.

## Silo setup

### `GrainIndexServiceCollectionExtensions`

| Method | Purpose |
|---|---|
| `AddGrainIndex<TGrain, TState>(this ISiloBuilder, Action<GrainIndexBuilder<TGrain, TState>>)` | Declares an index over `TGrain`'s `TState`. |
| `AddGrainIndexKeySource<TSource>(this ISiloBuilder, string indexName)` | Registers a keyed `IGrainKeySource` for an index's backfill. |
| `AddGrainIndexKeySource(this ISiloBuilder, string indexName, IGrainKeySource)` | The same, from an instance. |
| `AddGrainIndexKeySource(this ISiloBuilder, string indexName, Func<IServiceProvider, IGrainKeySource>)` | The same, from a factory. |
| `ConfigureGrainIndex(this ISiloBuilder, string indexName, Action<GrainIndexOptions>)` | Overrides a declared index's options by name. |
| `ConfigureGrainIndexOutbox(this ISiloBuilder, Action<GrainIndexOutboxOptions>)` | Configures the silo-wide outbox drain. |

### `GrainIndexBuilder<TGrain, TState>`

| Member | Purpose |
|---|---|
| `WithName(string)` | Sets the index name. Defaults to the `TGrain` interface name. |
| `WithTreeName(string)` | Overrides the backing tree. Must stay under `__grainindex/`. |
| `WithKeyCodec(IGrainKeyCodec<TGrain>)` | Sets how grain identities are encoded into entries. |
| `AllowReplication(bool allow = true)` | Permits the index tree to replicate across clusters. |
| `WithBackfillBatchSize(int)` | Grains visited per backfill pass. |
| `WithBackfillInterval(TimeSpan)` | Pause between backfill passes. |
| `Include<TProperty>(Expression<Func<TState, TProperty>>)` | Adds a property to the projection. At least one required. |

See [Configuration](configuration.md) for defaults and semantics.

## Grain enrolment

| Type | Purpose |
|---|---|
| `IndexedAttribute` | Orleans facet attribute standing in for `[PersistentState]`, marking a grain's state as indexed. Takes an optional state name and storage name. |
| `IndexedGrain<TState>` | Base class exposing `State`, `RecordExists`, `Etag`, `PersistentState`, and `WriteStateAsync`/`ReadStateAsync`/`ClearStateAsync`, each of which re-projects the grain's entries. |

## Querying

| Type | Purpose |
|---|---|
| `IGrainIndexProvider` | Resolves a declared index. `GetIndex<TGrain, TState>(string? name = null)` and `DeclaredIndexes`. |
| `IGrainIndex<TGrain, TState>` | One index. `Name`, `IndexedProperties`, and `Where(Expression<Func<TState, bool>>)`. |
| `GrainIndex<TGrain, TState>` | The concrete index implementing `IGrainIndex<TGrain, TState>`, resolved through `IGrainIndexProvider` rather than constructed directly. |
| `IGrainIndexQuery<TGrain>` | A planned, immutable query. See below. |
| `GrainIndexQueryExecution` | `DurableCursor` (default), `Stream`, `SnapshotCursor`. |
| `GrainIndexMatch` | A matched grain paired with the entry that matched it. |
| `GrainIndexQueryDefaults` | The default page size and execution mode. |

### `IGrainIndexQuery<TGrain>`

| Member | Purpose |
|---|---|
| `PageSize` | Entries fetched per round trip. |
| `Execution` | How the query walks the tree. |
| `WithPageSize(int)` | Returns a new query with the given page size. |
| `WithExecution(GrainIndexQueryExecution)` | Returns a new query with the given execution mode. |
| `ToGrainsAsync(CancellationToken)` | Streams matching grain references, each once. |
| `ToKeysAsync(CancellationToken)` | Streams matching encoded grain keys, each once. The cheapest shape. |
| `ToMatchesAsync(CancellationToken)` | Streams matches with the entry that matched. |
| `ToGrainListAsync(CancellationToken)` | Drains `ToGrainsAsync` into a list. |
| `ToKeyListAsync(CancellationToken)` | Drains `ToKeysAsync` into a list. |
| `AnyAsync(CancellationToken)` | Whether any grain matches. |

See [Queries](queries.md).

## Backfill

| Type | Purpose |
|---|---|
| `IGrainKeySource` | The application-supplied key population. `EnumerateKeysAsync(string? resumeAfterExclusive, CancellationToken)` and the optional `TryGetApproximateCountAsync(CancellationToken)`. |
| `IGrainIndexBackfillActivator` | Resolves an index's backfill grain. |
| `GrainIndexBackfillState` | `NotStarted`, `Running`, `Paused`, `Completed`, `Failed`. |
| `GrainIndexBackfillStatus` | A crawl's state, checkpoint, and progress. |
| `GrainIndexBackfillBatchResult` | The outcome of one pass. |
| `GrainIndexProgress` | Processed count, optional total, and percentage. |

See [Backfill](backfill.md).

## Administration

| Type | Purpose |
|---|---|
| `IGrainIndexAdmin` | `DeclaredIndexes`, `GetStatusAsync`, `ListStatusAsync`, `PauseBackfillAsync`, `ResumeBackfillAsync`, `RebuildAsync`, `RunBackfillPassAsync`. |
| `GrainIndexStatus` | `IndexName`, `Definition`, `Registered`, `Fingerprint`, `KeyCodecId`, `NeedsBackfill`, `Drift`, `Backfill`, `Progress`, `EntryCount`. |
| `GrainIndexDriftStatus` | Whether the declaration drifted, and on which fields. |
| `GrainIndexMetrics` | Instrument names, tag names, and tag values. |

See [Observability](observability.md).

## Options

| Type | Purpose |
|---|---|
| `GrainIndexOptions` | Per-index settings, resolved by name through `IOptionsMonitor<GrainIndexOptions>.Get(indexName)`. |
| `GrainIndexOutboxOptions` | Silo-wide outbox drain settings. |
| `GrainIndexDeclarationOptions` | The declaration captured at `AddGrainIndex` time. |
| `GrainIndexDriftPolicy` | `Reject` (default) or `Rebuild`. |
| `GrainIndexProjectionMode` | When entries are published relative to the state write. |

## Definition model

| Type | Purpose |
|---|---|
| `IGrainIndexDefinition` | The non-generic view of a declared index. |
| `GrainIndexDefinition<TGrain, TState>` | The typed declaration. |
| `GrainIndexDescriptor` | The serializable description of an index. |
| `GrainIndexProperty<TState>` / `TypedGrainIndexProperty<TState, TProperty>` | One projected property. |
| `GrainIndexPropertyDescriptor` | The serializable description of a property. |
| `GrainIndexDefinitionField` | `Name`, `TreeName`, `GrainInterfaceType`, `StateType`, `KeyCodec`, `Properties`, `AllowReplication`. |
| `GrainIndexDriftClassification` | `BreakingFields`, `SafeFields`, and the per-field rule. |
| `GrainIndexFingerprint` | The declaration's content digest. |

## Key encoding

| Type | Purpose |
|---|---|
| `IGrainKeyCodec` / `IGrainKeyCodec<TGrain>` | Encodes a `GrainId` into, and decodes it out of, an index entry. |
| `GrainKeyCodec` | Factory for the built-in codecs. |
| `StringGrainKeyCodec<TGrain>`, `GuidGrainKeyCodec<TGrain>`, `IntegerGrainKeyCodec<TGrain>` | The built-in codecs. |
| `GrainIndexKeyCodecIdentity` | The stable codec identity recorded in the registry. |
| `GrainIndexKeyEncoder` | The on-tree key encoding and range construction. |
| `GrainIndexTreeNames` | `ReservedPrefix`, `ForIndex(name)`, `IsIndexOwned(treeName)`. |

See [Architecture](architecture.md#key-encoding).

## Projection

| Type | Purpose |
|---|---|
| `GrainIndexProjection` | A grain's projected entry set. |
| `GrainIndexProjector<TGrain, TState>` | Projects state into entries. |
| `GrainIndexUpdatePlan` | The diff between an intended and a stored projection. |
| `GrainIndexMaintainer<TGrain, TState>` | Applies an update plan to the tree. |
| `GrainIndexEntry` | One index entry. |
| `GrainIndexEntryValue` | The entry payload encoding. |

## Exceptions

| Exception | Thrown when |
|---|---|
| `GrainIndexNotDeclaredException` | An administrative call names an index this silo does not declare. Resolving an unknown index through `IGrainIndexProvider` throws `InvalidOperationException` instead. |
| `GrainIndexPropertyNotIndexedException` | A predicate names a property that is not `Include`d. Reports the index, the path, and the indexed properties. |
| `GrainIndexConfigurationDriftException` | A drift-breaking declaration change is rejected at startup. Names the index and the drifted fields. |
| `GrainIndexReplicationNotAllowedException` | An index tree is configured to replicate while `AllowReplication` is `false`. |
| `GrainIndexKeyEncodingException` | A grain key cannot be encoded or decoded by the index's codec. |
| `NotSupportedException` | A predicate uses a construct the planner cannot route. See [Unsupported constructs](queries.md#unsupported-constructs). |
