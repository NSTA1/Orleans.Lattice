# Orleans.Lattice.Backup API reference

Every public type and member of `Orleans.Lattice.Backup`, grouped by role. Types not listed here are internal and are described by behaviour in [Architecture](architecture.md).

All serializable types are Orleans-serialized (`[GenerateSerializer]`) with stable aliases; the constructor parameter validation noted below is enforced at construction.

## Registration

### `LatticeBackupServiceCollectionExtensions`

Static extension methods on `ISiloBuilder`.

| Method | Signature | Purpose |
|---|---|---|
| `AddLatticeBackup` | `ISiloBuilder AddLatticeBackup(this ISiloBuilder builder, Action<LatticeBackupOptions>? configure = null)` | Adds the backup storage and engine surface: the default in-cluster sink, the catalog store, the capture / incremental / restore engines, the scheduler, options, and the once-per-silo history bootstrap. Ensures the view infrastructure is present so the catalog tree gets durable per-key history. Must be called after `AddLattice(...)`; throws `InvalidOperationException` when called first. Idempotent. |
| `ConfigureLatticeBackup` | `ISiloBuilder ConfigureLatticeBackup(this ISiloBuilder builder, Action<LatticeBackupOptions> configure)` | Layers an additional `LatticeBackupOptions` configuration delegate. |
| `ConfigureLatticeBackupSchedule` | `ISiloBuilder ConfigureLatticeBackupSchedule(this ISiloBuilder builder, Action<LatticeBackupScheduleOptions> configure)` | Configures the global (default) `LatticeBackupScheduleOptions` applied to every scope without a per-scope override. |
| `ConfigureLatticeBackupSchedule` | `ISiloBuilder ConfigureLatticeBackupSchedule(this ISiloBuilder builder, string scopeKey, Action<LatticeBackupScheduleOptions> configure)` | Configures `LatticeBackupScheduleOptions` for a specific scope keyed by `scopeKey` (the value from `BackupScopeKey.For`). Throws `ArgumentException` when `scopeKey` is null or empty. |

## Services

### `ILatticeBackupCaptureService`

The full-capture engine.

- `Task<LatticeBackupCaptureResult> CaptureAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)` - captures a full backup of the request's scope and returns its content-addressed id and manifest. Throws `ArgumentNullException` (`request` null), `LatticeAuthorizationDeniedException` (unauthorized), `LatticeSnapshotReplayBudgetExceededException` (scope exceeds the replay budget), `LatticeSaturatedException` (snapshot open shed under saturation), and `LatticeCursorSnapshotExpiredException` (pinned snapshot expired mid-capture).
- `Task<LatticeBackupSetCaptureResult> CaptureSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)` - captures a backup set: one full backup per scope grouped under a single set manifest. When `CrossTreeConsistent` is set and the set spans more than one tree, every tree is captured as of a single causal fence selected after in-flight cross-tree atomic sagas drain. Throws the same exceptions as `CaptureAsync`, plus `LatticeBackupCrossTreeFenceException` when a stable fence cannot be established within the configured attempts or drain timeout.

### `ILatticeBackupIncrementalCaptureService`

The incremental-capture engine.

- `Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)` - captures an incremental backup layered on a base backup and records the base id as the manifest's `BaseBackupId`. Throws `ArgumentNullException` when `request` is null.

### `ILatticeBackupRestoreService`

The causally-faithful restore engine.

- `Task<LatticeRestoreResult> RestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - walks the backup's base chain, validates every artifact against its recorded digest, then applies the entries per `LatticeRestoreRequest.Mode` (in-place bulk-load / merge, or atomic shadow-cutover). Idempotent under retry. Throws `ArgumentNullException` (`request` null), `LatticeRestoreValidationException` (pre-apply validation failure), and `LatticeAuthorizationDeniedException` (unauthorized).
- `Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)` - reverts a `ShadowCutover` restore by swapping the target tree's registry alias back to `PreviousPhysicalTreeId`. Idempotent. Throws `ArgumentNullException` (`restore` null), `ArgumentException` (not a shadow-cutover result), and `LatticeAuthorizationDeniedException` (unauthorized).

### `ILatticeBackupScheduler`

The public entry point for on-demand triggers, schedule registration, and retention. Each operation targets a `BackupScopeSelector` and is coordinated per scope so triggers, scheduled captures, and retention for the same scope never overlap.

- `Task<string?> TriggerFullBackupAsync(BackupScopeSelector scope)` - triggers a full backup; returns the backup id, or `null` when a capture for the scope is already in flight.
- `Task<string?> TriggerIncrementalBackupAsync(BackupScopeSelector scope)` - triggers an incremental backup layered on the most recent backup for the scope (or a full baseline when none exists); returns the backup id, or `null` when skipped by the overlap guard.
- `Task EnsureScheduleAsync(BackupScopeSelector scope)` - registers or updates the recurring full and incremental schedule reminders for the scope per its `LatticeBackupScheduleOptions`. Idempotent.
- `Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)` - prunes the scope's backup chain per its retention policy, preserving the base chain of every retained increment; a no-op that retains everything when retention is disabled.

All four throw `ArgumentNullException` when `scope` is null.

### `ILatticeBackupCatalogStore`

The durable, introspectable index of manifests, persisted into the reserved `sys-backup-catalog` tree keyed by backup id.

- `Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)` - registers or replaces a manifest, keyed by `manifest.Id`. Idempotent. Throws `ArgumentNullException` when `manifest` is null.
- `Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default)` - reads a manifest, or `null`. Throws `ArgumentException` when `backupId` is null or empty.
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)` - removes a manifest; returns `true` when one was removed. Throws `ArgumentException` when `backupId` is null or empty.
- `IAsyncEnumerable<BackupManifest> ListAsync(CancellationToken cancellationToken = default)` - enumerates every catalogued manifest in backup-id order.

### `ILatticeBackupSink`

The pluggable storage sink a backup is written to and restored from. Stores streamed, content-addressed artifacts and self-describing manifests; the artifact surface moves the payload as an ordered chunk sequence so a large tree streams without being materialized whole. Artifact ids are expected to be content-addressed (see `BackupContentHash`) so identical retries are no-ops.

Artifact members:

- `Task WriteArtifactAsync(string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default)` - writes an artifact as an ordered chunk stream. Idempotent for the same id and content. Throws `ArgumentException` (`artifactId` null/empty) and `ArgumentNullException` (`content` null).
- `IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(string artifactId, CancellationToken cancellationToken = default)` - reads an artifact back as an ordered chunk stream; yields nothing when absent. Throws `ArgumentException` when `artifactId` is null or empty.
- `Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)` - removes an artifact; returns `true` when one was removed. Throws `ArgumentException` when `artifactId` is null or empty.
- `IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default)` - enumerates every artifact id in id order.

Manifest members:

- `Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)` - creates or replaces a manifest keyed by `manifest.Id`. Idempotent. Throws `ArgumentNullException` when `manifest` is null.
- `Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)` - reads a manifest, or `null`. Throws `ArgumentException` when `backupId` is null or empty.
- `IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default)` - enumerates every manifest in backup-id order.
- `Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)` - removes a manifest; returns `true` when one was removed. Does not remove referenced artifacts. Throws `ArgumentException` when `backupId` is null or empty.

## Requests and results

### `LatticeBackupCaptureRequest`

Full-capture request.

- `const int DefaultPageSize = 1024`.
- Constructor: `LatticeBackupCaptureRequest(string name, BackupScopeSelector scope, int pageSize = DefaultPageSize)`. Throws `ArgumentException` (`name` null/empty), `ArgumentNullException` (`scope` null), `ArgumentOutOfRangeException` (`pageSize` not positive).
- Properties: `string Name`, `BackupScopeSelector Scope`, `int PageSize`.

### `LatticeBackupIncrementalCaptureRequest`

Incremental-capture request.

- Constructor: `LatticeBackupIncrementalCaptureRequest(string name, BackupScopeSelector scope, string baseBackupId, int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)`. Throws `ArgumentException` (`name` or `baseBackupId` null/empty), `ArgumentNullException` (`scope` null), `ArgumentOutOfRangeException` (`pageSize` not positive).
- Properties: `string Name`, `BackupScopeSelector Scope`, `string BaseBackupId`, `int PageSize`.

### `LatticeBackupCaptureResult`

- Constructor: `LatticeBackupCaptureResult(string backupId, BackupManifest manifest)`.
- Properties: `string BackupId`, `BackupManifest Manifest`.

### `LatticeBackupSetCaptureRequest`

Backup-set request.

- Constructor: `LatticeBackupSetCaptureRequest(string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent = false, int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)`. Throws `ArgumentException` when `name` is null/empty, `scopes` is empty, or two scopes name the same tree; `ArgumentNullException` when `scopes` or a member is null; `ArgumentOutOfRangeException` when `pageSize` is not positive.
- Properties: `string Name`, `IReadOnlyList<BackupScopeSelector> Scopes`, `bool CrossTreeConsistent`, `int PageSize`.

### `LatticeBackupSetCaptureResult`

- Constructor: `LatticeBackupSetCaptureResult(BackupSetManifest setManifest, IReadOnlyList<LatticeBackupCaptureResult> members)`. Throws `ArgumentNullException` (either null) and `ArgumentException` (`members` empty).
- Properties: `BackupSetManifest SetManifest`, `IReadOnlyList<LatticeBackupCaptureResult> Members`.

### `LatticeRestoreRequest`

Restore request.

- `const int DefaultApplyBatchSize = 1024`.
- Constructor: `LatticeRestoreRequest(string backupId, string? targetTreeId = null, BackupScopeSelector? scope = null, LatticeRestoreMode mode = LatticeRestoreMode.InPlace, string? operationId = null, int applyBatchSize = DefaultApplyBatchSize)`. Throws `ArgumentException` (`backupId` null/empty, or `targetTreeId` / `operationId` supplied but empty) and `ArgumentOutOfRangeException` (`applyBatchSize` not positive).
- Properties: `string BackupId`, `string? TargetTreeId`, `BackupScopeSelector? Scope`, `LatticeRestoreMode Mode`, `string? OperationId`, `int ApplyBatchSize`.

### `LatticeRestoreResult`

- Constructor: `LatticeRestoreResult(string backupId, string targetTreeId, LatticeRestoreMode mode, string operationId, IReadOnlyList<string> manifestChain, long entriesApplied, string? shadowPhysicalTreeId = null, string? previousPhysicalTreeId = null)`. Throws `ArgumentException` (`backupId`, `targetTreeId`, or `operationId` null/empty), `ArgumentNullException` (`manifestChain` null), `ArgumentOutOfRangeException` (`entriesApplied` negative).
- Properties: `string BackupId`, `string TargetTreeId`, `LatticeRestoreMode Mode`, `string OperationId`, `IReadOnlyList<string> ManifestChain`, `long EntriesApplied`, `string? ShadowPhysicalTreeId`, `string? PreviousPhysicalTreeId`.

## Scope

### `BackupScopeSelector`

Names a region of a tree to back up.

- Constructor: `BackupScopeSelector(BackupScopeKind kind, string treeId, string? keyOrPrefix = null)`. Throws `ArgumentException` when `treeId` is null/empty, a `WholeTree` scope carries a key/prefix, or a `Key` / `Prefix` scope omits its key/prefix.
- Properties: `BackupScopeKind Kind`, `string TreeId`, `string? KeyOrPrefix`.
- Factories: `static BackupScopeSelector WholeTree(string treeId)`, `static BackupScopeSelector Prefix(string treeId, string prefix)`, `static BackupScopeSelector Key(string treeId, string key)`.

### `BackupScopeKey`

- `static string For(BackupScopeSelector scope)` - the deterministic scope key used as the per-scope scheduler grain key and the named-options key. Two selectors covering the same region produce the same key. Throws `ArgumentNullException` when `scope` is null.

## Manifests and descriptors

### `BackupManifest`

The self-describing record of one backup.

- Constructor: `BackupManifest(string id, string name, DateTimeOffset createdAtUtc, BackupKind kind, BackupScopeSelector scope, BackupConsistencyCut consistencyCut, BackupTopologySnapshot topology, string structuralDigest, IReadOnlyList<BackupKeyDescriptor> keyDescriptors, IReadOnlyList<BackupContentDescriptor> contentDescriptors, IReadOnlyList<BackupOriginProvenance> provenance, string? baseBackupId = null, BackupCompressionDictionaryRef? compressionDictionary = null)`. Validates that `id` is non-empty and free of the reserved unit-separator (U+001F); that an `Incremental` backup carries a non-empty `baseBackupId` and a `Full` backup carries none; and null-checks the reference-type members.
- Properties: `string Id`, `string Name`, `DateTimeOffset CreatedAtUtc`, `BackupKind Kind`, `BackupScopeSelector Scope`, `BackupConsistencyCut ConsistencyCut`, `BackupTopologySnapshot Topology`, `string StructuralDigest`, `IReadOnlyList<BackupKeyDescriptor> KeyDescriptors`, `IReadOnlyList<BackupContentDescriptor> ContentDescriptors`, `IReadOnlyList<BackupOriginProvenance> Provenance`, `string? BaseBackupId`, `BackupCompressionDictionaryRef? CompressionDictionary`.

### `BackupConsistencyCut`

The causal cut a backup was taken as of.

- Constructor: `BackupConsistencyCut(long walSequence, long hlcTimestamp, IReadOnlyDictionary<string, long>? perOriginFrontier = null, IReadOnlyDictionary<int, long>? walPartitionOffsets = null)`. Throws `ArgumentOutOfRangeException` when `walSequence` or `hlcTimestamp` is negative.
- Properties: `long WalSequence`, `long HlcTimestamp`, `IReadOnlyDictionary<string, long>? PerOriginFrontier`, `IReadOnlyDictionary<int, long>? WalPartitionOffsets` (the per-partition resume offsets an incremental layers on).

### `BackupTopologySnapshot`

- Constructor: `BackupTopologySnapshot(int shardCount, int virtualShardCount, IReadOnlyList<string> shardRootDigests)`. Throws `ArgumentOutOfRangeException` when `shardCount` or `virtualShardCount` is not positive, `ArgumentNullException` when `shardRootDigests` is null.
- Properties: `int ShardCount`, `int VirtualShardCount`, `IReadOnlyList<string> ShardRootDigests`.

### `BackupKeyDescriptor`

Per-key shape and merge mode.

- Constructor: `BackupKeyDescriptor(string key, BackupKeyMergeMode mergeMode, string? originId = null)`. Throws `ArgumentException` when `key` is null/empty.
- Properties: `string Key`, `BackupKeyMergeMode MergeMode`, `string? OriginId`.

### `BackupContentDescriptor`

Describes one stored artifact.

- Constructor: `BackupContentDescriptor(string artifactId, string contentHash, long byteLength, int chunkCount, BackupScopeSelector scope)`. Throws `ArgumentException` (`artifactId` or `contentHash` null/empty), `ArgumentOutOfRangeException` (`byteLength` or `chunkCount` negative), `ArgumentNullException` (`scope` null).
- Properties: `string ArtifactId`, `string ContentHash`, `long ByteLength`, `int ChunkCount`, `BackupScopeSelector Scope`.

### `BackupOriginProvenance`

Per-origin high-water mark.

- Constructor: `BackupOriginProvenance(string originId, long highWaterSequence)`. Throws `ArgumentException` (`originId` null/empty), `ArgumentOutOfRangeException` (`highWaterSequence` negative).
- Properties: `string OriginId`, `long HighWaterSequence`.

### `BackupCompressionDictionaryRef`

Reference to the compression dictionary a backup's artifacts were encoded against.

- Constructor: `BackupCompressionDictionaryRef(string dictionaryId, string digest)`. Throws `ArgumentException` when either is null/empty.
- Properties: `string DictionaryId`, `string Digest`.

### `BackupSetManifest`

The record grouping a backup set's members.

- Constructor: `BackupSetManifest(string setId, string name, DateTimeOffset createdAtUtc, bool crossTreeConsistent, BackupSetFence? fence, IReadOnlyList<string> memberBackupIds)`. Throws `ArgumentException` (`setId` or `name` null/empty, `memberBackupIds` empty), `ArgumentNullException` (`memberBackupIds` null).
- Properties: `string SetId`, `string Name`, `DateTimeOffset CreatedAtUtc`, `bool CrossTreeConsistent`, `BackupSetFence? Fence`, `IReadOnlyList<string> MemberBackupIds`.

### `BackupSetFence`

The selected cross-tree causal fence of a cross-tree-consistent set.

- Constructor: `BackupSetFence(long hlcTimestamp, int drainedInFlightCount, double drainWaitMilliseconds, int attempts)`. Throws `ArgumentOutOfRangeException` when `hlcTimestamp`, `drainedInFlightCount`, or `drainWaitMilliseconds` is negative, or `attempts` is not positive.
- Properties: `long HlcTimestamp`, `int DrainedInFlightCount`, `double DrainWaitMilliseconds`, `int Attempts`.

## Reports and status

### `BackupRetentionReport`

- Constructor: `BackupRetentionReport(int retainedCount, IReadOnlyList<string> prunedBackupIds)`. Throws `ArgumentOutOfRangeException` (`retainedCount` negative), `ArgumentNullException` (`prunedBackupIds` null).
- Properties: `int RetainedCount`, `IReadOnlyList<string> PrunedBackupIds`, `int PrunedCount` (equals `PrunedBackupIds.Count`).
- `static BackupRetentionReport Empty` - a report that retained nothing and pruned nothing.

### `BackupSchedulerRuntimeStatus`

A scope's schedule registration and last-run status.

- Constructor: `BackupSchedulerRuntimeStatus(bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome)`.
- Properties mirror the constructor parameters: `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`.

## Enums

### `BackupKind`

`Full = 0`, `Incremental = 1`.

### `BackupScopeKind`

`WholeTree = 0`, `Prefix = 1`, `Key = 2`.

### `BackupKeyMergeMode`

`LastWriterWins = 0`, `Crdt = 1`.

### `LatticeRestoreMode`

`InPlace = 0` (empty-tree bulk-load fast path, or last-writer-wins merge into existing data), `ShadowCutover = 1` (build a fresh physical tree and atomically swap the registry alias).

### `BackupScopeRunOutcome`

`None = 0`, `Success = 1`, `Failure = 2`.

## Options

`LatticeBackupOptions` and `LatticeBackupScheduleOptions` are documented in full in [Configuration](configuration.md).

## Reserved-namespace guard

### `LatticeBackupReservedTrees`

- `static string Prefix` - the reserved tree-name prefix owned by the backup package (`sys-backup-`).
- `static bool IsReserved(string treeId)` - `true` when `treeId` collides with the reserved namespace. Throws `ArgumentNullException` when `treeId` is null.
- `static void ThrowIfReserved(string treeId, string? paramName = null)` - throws `ArgumentException` when `treeId` is null, empty, or reserved.

## Content addressing

### `BackupContentHash`

- `static string Compute(ReadOnlySpan<byte> content)` - the 64-character lowercase hexadecimal SHA-256 of the bytes.
- `static string Compute(IEnumerable<ReadOnlyMemory<byte>> chunks)` - the SHA-256 of an ordered chunk sequence, as if concatenated, without buffering the payload whole. Throws `ArgumentNullException` when `chunks` is null.

## Metrics

`BackupMetrics` and `LatticeBackupMetrics` expose the meter, its instruments, tag/phase/reason constants, and the emission helpers. They are documented in full in [Observability](observability.md).

## Exceptions

### `LatticeBackupCrossTreeFenceException` : `Exception`

Thrown by `CaptureSetAsync` when a stable cross-tree fence cannot be established within the configured attempts or drain timeout. Constructors: `(string message)` and `(string message, Exception innerException)`.

### `LatticeRestoreValidationException` : `InvalidOperationException`

Thrown by `RestoreAsync` when a backup fails pre-apply validation (for example an artifact whose bytes do not match its recorded content digest). Constructors: `(string message)` and `(string message, Exception innerException)`.
