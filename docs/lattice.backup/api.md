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
- `Task<LatticeBackupSetCaptureResult> CaptureSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)` - captures a backup set: one full backup per scope grouped under a single set manifest. When `CrossTreeConsistent` is set and the set spans more than one tree, every tree is captured as of a single causal fence selected after in-flight cross-tree atomic sagas drain. When the set spans more than one tree, every member manifest is stamped with the set's `SetId` and name so the catalogued per-tree backups can be grouped back into one logical set entry. Throws the same exceptions as `CaptureAsync`, plus `LatticeBackupCrossTreeFenceException` when a stable fence cannot be established within the configured attempts or drain timeout.

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
- `Task ScheduleRecurringBackupAsync(LatticeBackupScheduleRequest request)` - registers or updates a recurring backup of the request's scope that fires every `Interval`, capturing a full or incremental backup per the request. The interval is clamped up to the reminder minimum; a runtime schedule registered this way overrides the configured `LatticeBackupScheduleOptions` cadence for the chosen kind. Idempotent. Throws `ArgumentNullException` when `request` is null.
- `Task EnsureScheduleAsync(BackupScopeSelector scope)` - registers or updates the recurring full and incremental schedule reminders for the scope per its `LatticeBackupScheduleOptions`. Idempotent.
- `Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)` - prunes the scope's backup chain per its retention policy, preserving the base chain of every retained increment; a no-op that retains everything when retention is disabled.

All four scope-typed methods throw `ArgumentNullException` when `scope` is null.

### `ILatticeBackupCatalogStore`

The durable, introspectable index of manifests, persisted into the reserved `sys-backup-catalog` tree keyed by backup id.

- `Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)` - registers or replaces a manifest, keyed by `manifest.Id`. Idempotent. Throws `ArgumentNullException` when `manifest` is null.
- `Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default)` - reads a manifest, or `null`. Throws `ArgumentException` when `backupId` is null or empty.
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)` - removes a manifest; returns `true` when one was removed. Throws `ArgumentException` when `backupId` is null or empty.
- `IAsyncEnumerable<BackupManifest> ListAsync(CancellationToken cancellationToken = default)` - enumerates every catalogued manifest in backup-id order.

### `ILatticeBackupSink`

The pluggable storage sink a backup is written to and restored from. Stores streamed, content-addressed artifacts and self-describing manifests; the artifact surface moves the payload as an ordered chunk sequence so a large tree streams without being materialized whole. Artifact ids are expected to be content-addressed (see `BackupContentHash`) so identical retries are no-ops.

Capability members:

- `bool IsDurable` - whether the sink stores payload outside the cluster it protects (an external, durable store such as Azure Blob or the filesystem sample sink), as opposed to the ephemeral in-cluster sink whose payload shares the fate of the cluster. Disaster-recovery features that only make sense against an off-cluster store - notably the periodic backup-health monitor - gate themselves on this flag: `false` keeps the monitor inert and hides the Explorer health column.

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

Sink-existence probe members (read-only, cheap - existence and committed-metadata only, never downloading or hashing payload):

- `Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default)` - the cheap liveness check used at selection time: `true` when the manifest is present in the sink. Throws `ArgumentException` when `backupId` is null or empty.
- `Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)` - the richer resolvability probe used by reconcile / scrub: reports whether the manifest is present and which referenced artifacts are missing (absent, or - for sinks that mark commit - present but not committed). Throws `ArgumentException` when `backupId` is null or empty.

### `ILatticeBackupCatalogScrubService`

Reconciles the in-cluster catalog against the durable sink in the opposite direction to the rebuild service: it finds catalog rows the sink can no longer resolve (orphans) rather than sink manifests the catalog is missing.

- `Task<BackupCatalogScrubReport> ScrubAsync(bool pruneOrphans = false, CancellationToken cancellationToken = default)` - enumerates every catalog row and probes the sink (`ILatticeBackupSink.ProbeAsync`) for its resolvability, collecting the orphans - rows whose sink payload (manifest, or a referenced artifact) is gone. Non-destructive by default: it only flags orphans. When `pruneOrphans` is `true` it removes each orphan catalog row under system origin, which is idempotent on re-run (a pruned orphan is no longer scanned). Returns a `BackupCatalogScrubReport` summarizing counts scanned, orphaned, and removed, whether pruning ran, and the orphan backup ids.

### `ILatticeBackupCatalogRebuildService`

Rebuilds the in-cluster catalog from the durable sink, treating the sink as the single source of truth and the catalog as a rebuildable, self-healing projection over it.

- `Task<BackupCatalogRebuildReport> RebuildFromSinkAsync(CancellationToken cancellationToken = default)` - scans every manifest the sink holds (via `ILatticeBackupSink.ListManifestsAsync`) and re-registers each into the catalog under system-origin. Idempotent and safe to re-run: a manifest already catalogued is reconciled in place (keeping its immutable capture timestamp) rather than duplicated, and a catalog missing rows the sink has is repopulated. Returns a `BackupCatalogRebuildReport` summarizing counts scanned, freshly added, and reconciled.

### `ILatticeBackupColdRestoreService`

Restores a backup into a **fresh** cluster from the durable sink alone, with zero dependency on any surviving `sys-backup-catalog` tree. This is the disaster-recovery entry point: a cluster that lost its grain storage (so its catalog is gone) but still has the external sink can enumerate, resolve, chain-walk, and restore its backups from the sink.

- `Task<LatticeRestoreResult> ColdRestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - bootstraps the reserved `sys-` trees if they are absent, resolves the target manifest and its `BaseBackupId` chain directly from the sink (never the catalog), verifies every referenced artifact is present and intact, replays the chain through the HLC-preserving restore engine, then re-projects the catalog from the sink so the recovered cluster is left with a correct catalog. Reuses `LatticeRestoreRequest` / `LatticeRestoreResult`. Idempotent. Throws `LatticeRestoreValidationException` when the backup is absent from the sink, the base chain is broken, or an artifact is missing or tampered; throws `ArgumentNullException` when `request` is null.

### `ILatticeBackupHealthService`

Verifies that a backup's durable sink payload is present and intact, layering content-hash verification on top of the cheap presence probe. Registered by `AddLatticeBackup`.

- `Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default)` - resolves the backup's manifest, checks presence and committed-metadata of every referenced artifact (reusing `ILatticeBackupSink.ProbeAsync`), then downloads each present artifact and re-hashes it against its recorded `BackupContentDescriptor.ContentHash` to catch silent corruption. Returns a fresh point-in-time `BackupHealthReport`; does not persist it. Throws `ArgumentException` when `backupId` is null or empty.

### `ILatticeBackupHealthStore`

Persists per-backup health state - the latest `BackupHealthReport` and the per-backup `BackupHealthConfig` - in the reserved `sys-backup-health` `ILattice` tree keyed by backup id, so the periodic monitor that writes reports and the UI that reads them share one durable projection (no second external store). Registered by `AddLatticeBackup`.

- `Task SetReportAsync(BackupHealthReport report, CancellationToken cancellationToken = default)` - persists (or replaces) the latest report for its `BackupId`. Throws `ArgumentNullException` when `report` is null.
- `Task<BackupHealthReport?> GetReportAsync(string backupId, CancellationToken cancellationToken = default)` - reads the latest report, or `null`. Throws `ArgumentException` when `backupId` is null or empty.
- `IAsyncEnumerable<BackupHealthReport> ListReportsAsync(CancellationToken cancellationToken = default)` - enumerates every stored report in backup-id order.
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)` - removes the stored report and configuration for a backup; returns `true` when anything was removed. Throws `ArgumentException` when `backupId` is null or empty.
- `Task SetConfigAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)` - persists (or replaces) the per-backup monitor configuration. Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`config` null).
- `Task<BackupHealthConfig?> GetConfigAsync(string backupId, CancellationToken cancellationToken = default)` - reads the per-backup configuration, or `null` when the backup uses the configured defaults. Throws `ArgumentException` when `backupId` is null or empty.

The periodic monitor itself is an internal reminder-driven grain (mirroring the backup scheduler): once per sweep it enumerates the catalog and re-verifies each enrolled backup whose configured interval has elapsed, writing the result through `ILatticeBackupHealthStore`. It is inert unless the registered sink reports `IsDurable`.

## Extension seams (coordinated restore)

These backup-package-local seams let the replication package layer an atomic multi-tree, multi-cluster restore on top of the backup engine without the backup package taking a dependency on replication. Each has a default no-op registration installed by `AddLatticeBackup`, so a single-cluster host always takes the plain local restore path; the replication package (or the host) supplies the real implementation.

### `IRestoreSagaDispatcher`

The seam the restore path consults so a restore into a replicated tree can be promoted to an all-or-nothing coordinated restore across every cluster that replicates the target. The decision is a function of the target tree's current replication status, never of the backup's origin. The default registration never dispatches.

- `Task<LatticeRestoreResult?> TryDispatchAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - offers a single-tree restore to the coordinated path; returns the local cluster's result when the coordinated path handled it, or `null` to signal the caller should run the plain local restore. Throws `ArgumentNullException` when `request` is null.
- `Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync(string setId, LatticeRestoreMode mode, CancellationToken cancellationToken = default)` - offers a backup-set restore as one atomic unit over the union of the replicated members' peer sets; returns this cluster's per-member results, or `null` when no member is replicated (or the id is not a set id). Throws `ArgumentException` when `setId` is null or empty.

### `IReplicatedTreeMembership`

The seam the fail-fast sink guard consults to learn whether a tree participates in the cross-cluster replication set (a replicated tree must be backed by a shared external sink, not the default in-cluster sink). The default registration reports nothing replicated.

- `bool IsReplicated(string treeId)` - reports whether the tree participates in the replication set. Throws `ArgumentNullException` when `treeId` is null.
- `IReadOnlyCollection<string> ReplicatedTrees { get; }` - the ids of every replicated tree.

### `ILatticeCoordinatedRestoreEngine`

Decomposes the atomic `ShadowCutover` restore into the separate phases a coordinated restore saga drives independently. The single `ILatticeBackupRestoreService.RestoreAsync` entry point composes these same phases for the local path, so both paths share one alias swap. Saga-unaware: it exposes the mechanism without any knowledge of the coordinator, write fence, or participant model.

- `Task<RestoreAdmissionReport> ProbeAdmissionAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - resolves the target's manifest chain and reports its size and topology without validating, fencing, or building, so an infeasible target is refused up front. Throws `ArgumentNullException` (`request` null) and `LatticeRestoreValidationException` (a chain member is missing).
- `Task<LatticeRestoreResult> BuildShadowAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - builds the shadow tree from the manifest chain into a fresh physical tree without swapping the alias or fencing the live tree. Idempotent and resumable. Throws `ArgumentNullException` (`request` null), `ArgumentException` (not a shadow-cutover request), and `LatticeRestoreValidationException` (validation failure).
- `Task CommitShadowAsync(LatticeRestoreResult shadow, CancellationToken cancellationToken = default)` - commits a built shadow by atomically swapping the registry alias, then refreshing routing and converging any covering tag index. The caller engages the write fence around this call. Idempotent. Throws `ArgumentNullException` (`shadow` null) and `ArgumentException` (not a shadow-cutover build result).
- `Task DeleteShadowAsync(string shadowPhysicalTreeId, CancellationToken cancellationToken = default)` - reliably garbage-collects an orphaned shadow so an aborted restore leaks no storage. Idempotent. Throws `ArgumentException` when `shadowPhysicalTreeId` is null or empty.
- `string ResolveShadowTreeId(LatticeRestoreRequest request)` - deterministically resolves the shadow tree id a build of `request` would produce, without I/O, so an aborting participant can garbage-collect by id after losing its in-memory state. Throws `ArgumentNullException` (`request` null) and `ArgumentException` (no explicit target tree).

### `ILatticeBackupSetResolver`

The saga-unaware read seam that expands a captured backup-set id into the per-tree member backups it references, so the restore path can restore every tree in a set as one unit.

- `Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync(string setId, CancellationToken cancellationToken = default)` - resolves the set's member backups in tree-id order, or an empty list when the id is not a set id (for example a single-tree backup id). Throws `ArgumentException` when `setId` is null or empty.

### `BackupSetMember`

One resolved member of a captured backup set: the member backup id and the tree it restores. Returned by `ILatticeBackupSetResolver`. An in-process value only (no serializer surface).

- `string BackupId` - the content-addressed id of the member backup.
- `string TreeId` - the tree the member backup restores.

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

### `LatticeBackupScheduleRequest`

A request to register a recurring backup schedule for a scope.

- Constructor: `LatticeBackupScheduleRequest(BackupScopeSelector scope, bool incremental, TimeSpan interval)`. Throws `ArgumentNullException` when `scope` is null; `ArgumentOutOfRangeException` when `interval` is not strictly positive.
- Properties: `BackupScopeSelector Scope`, `bool Incremental`, `TimeSpan Interval`.

A runtime schedule registered from this request overrides the configured `LatticeBackupScheduleOptions` cadence for the chosen kind; the interval is clamped up to the scheduler minimum when smaller.

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
- Properties: `string Id`, `string Name`, `DateTimeOffset CreatedAtUtc`, `BackupKind Kind`, `BackupScopeSelector Scope`, `BackupConsistencyCut ConsistencyCut`, `BackupTopologySnapshot Topology`, `string StructuralDigest`, `IReadOnlyList<BackupKeyDescriptor> KeyDescriptors`, `IReadOnlyList<BackupContentDescriptor> ContentDescriptors`, `IReadOnlyList<BackupOriginProvenance> Provenance`, `string? BaseBackupId`, `BackupCompressionDictionaryRef? CompressionDictionary`, `string? SetId`, `string? SetName`. `SetId` and `SetName` are non-null only on a backup captured as a member of a multi-tree set: every member of one set shares the same `SetId`, so a catalog consumer can group the per-tree members into a single logical entry from a first-class fact rather than inferring it from the backup name. They are stamped once at set capture and never mutated; a single-tree set leaves both null, so it lists as an ordinary backup.

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

### `RestoreAdmissionReport`

The self-describing size and topology of a restore, resolved from the target backup's manifest chain before any fence is engaged or shadow tree is built, so a coordinated restore can hard-refuse an infeasible target up front. Returned by `ILatticeCoordinatedRestoreEngine.ProbeAdmissionAsync`. An in-process value only (no serializer surface).

- Constructor: `RestoreAdmissionReport(string backupId, string targetTreeId, long totalByteLength, long totalChunkCount, int shardCount, IReadOnlyList<string> manifestChain)`. Throws `ArgumentException` (a required string null/empty), `ArgumentNullException` (`manifestChain` null), and `ArgumentOutOfRangeException` (`totalByteLength`/`totalChunkCount` negative, `shardCount` not positive).
- Properties: `string BackupId`, `string TargetTreeId`, `long TotalByteLength`, `long TotalChunkCount`, `int ShardCount`, `IReadOnlyList<string> ManifestChain` (base-first order).

### `BackupSchedulerRuntimeStatus`

A scope's schedule registration and last-run status.

- Constructor: `BackupSchedulerRuntimeStatus(bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome)`.
- Properties mirror the constructor parameters: `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`.

### `BackupCatalogRebuildReport`

The outcome summary of `ILatticeBackupCatalogRebuildService.RebuildFromSinkAsync`. `ScannedCount` always equals `RegisteredCount + ReconciledCount`.

- Constructor: `BackupCatalogRebuildReport(long scannedCount, long registeredCount, long reconciledCount)`.
- Properties: `long ScannedCount` (manifests enumerated from the sink), `long RegisteredCount` (absent from the catalog and freshly added), `long ReconciledCount` (already catalogued and reconciled in place).

### `BackupCatalogScrubReport`

The outcome summary of `ILatticeBackupCatalogScrubService.ScrubAsync`. Non-destructive by default, so `RemovedCount` is zero and `Pruned` is `false` unless the caller opts in to pruning; a flag-only pass still reports every orphan.

- Constructor: `BackupCatalogScrubReport(long scannedCount, long orphanCount, long removedCount, bool pruned, IReadOnlyList<string> orphanBackupIds)`.
- Properties: `long ScannedCount` (catalog rows cross-checked against the sink), `long OrphanCount` (rows with no resolvable sink payload), `long RemovedCount` (orphan rows removed, zero on a non-destructive pass), `bool Pruned` (whether destructive pruning was requested and applied), `IReadOnlyList<string> OrphanBackupIds` (the ids of the orphans found).

### `BackupSinkResolution`

The read-only outcome of `ILatticeBackupSink.ProbeAsync`: whether a backup is resolvable from the sink alone.

- Constructor: `BackupSinkResolution(string backupId, bool manifestPresent, IReadOnlyList<string> missingArtifactIds)`. Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`missingArtifactIds` null).
- Properties: `string BackupId`, `bool ManifestPresent`, `IReadOnlyList<string> MissingArtifactIds` (referenced artifacts absent, or present but not committed), and the computed `bool IsResolvable` (`true` only when the manifest is present and no artifact is missing).

### `BackupHealthReport`

The result of verifying one backup's durable sink payload - presence plus content-hash consistency - precise enough to drive a diagnostics dialog. Persisted per backup by `ILatticeBackupHealthStore`.

- Constructor: `BackupHealthReport(string backupId, BackupHealthStatus status, bool manifestPresent, IReadOnlyList<string> missingArtifactIds, IReadOnlyList<string> hashMismatchArtifactIds, DateTimeOffset checkedAtUtc, string explanation)`. Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`missingArtifactIds`, `hashMismatchArtifactIds`, or `explanation` null).
- Properties: `string BackupId`, `BackupHealthStatus Status`, `bool ManifestPresent`, `IReadOnlyList<string> MissingArtifactIds` (referenced artifacts absent or uncommitted), `IReadOnlyList<string> HashMismatchArtifactIds` (present artifacts whose content no longer matches the manifest's recorded hash), `DateTimeOffset CheckedAtUtc`, `string Explanation` (a precise human-readable summary naming the missing / mismatched artifacts), and the computed `bool IsHealthy` (`true` only when `Status` is `Healthy`).

### `BackupHealthConfig`

The per-backup health-monitoring override: whether the periodic monitor verifies this backup, and how often. Every backup is auto-enrolled with the configured defaults; this record overrides that for a single backup. Persisted by `ILatticeBackupHealthStore`.

- Constructor: `BackupHealthConfig(bool monitoringEnabled, TimeSpan interval)`. Throws `ArgumentOutOfRangeException` when `interval` is not strictly positive.
- Properties: `bool MonitoringEnabled`, `TimeSpan Interval`.

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

### `BackupHealthStatus`

`Unknown = 0` (never verified), `Healthy = 1` (manifest and every artifact present, committed, and hash-matched), `Warning = 2` (manifest present but at least one artifact missing, uncommitted, or hash-mismatched), `Missing = 3` (manifest itself absent - the catalog row is an orphan).

## Options

`LatticeBackupOptions` and `LatticeBackupScheduleOptions` are documented in full in [Configuration](configuration.md). `LatticeBackupHealthOptions` configures the periodic health monitor cluster-wide: `bool Enabled` (default `true` - health monitoring is auto-enrolled) and `TimeSpan DefaultInterval` (default six hours), plus the static `MinimumInterval` (one minute) the sweep reminder clamps up to. The monitor stays inert against a non-durable sink regardless of these options.

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
