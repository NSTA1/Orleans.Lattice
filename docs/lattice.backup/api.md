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
| `ConfigureLatticeBackupSchedule` | `ISiloBuilder ConfigureLatticeBackupSchedule(this ISiloBuilder builder, Action<LatticeBackupScheduleOptions> configure)` | Configures the global `LatticeBackupScheduleOptions` applied to every scope. The delegate is registered for every named instance, so it also applies to a scope that has a per-scope delegate; the delegates run in registration order, so register the global defaults first for a per-scope override to win. |
| `ConfigureLatticeBackupSchedule` | `ISiloBuilder ConfigureLatticeBackupSchedule(this ISiloBuilder builder, string scopeKey, Action<LatticeBackupScheduleOptions> configure)` | Configures `LatticeBackupScheduleOptions` for a specific scope keyed by `scopeKey` (the value from `BackupScopeKey.For`). Throws `ArgumentException` when `scopeKey` is null or empty. |
| `ConfigureLatticeBackupHealth` | `ISiloBuilder ConfigureLatticeBackupHealth(this ISiloBuilder builder, Action<LatticeBackupHealthOptions> configure)` | Configures the cluster-wide `LatticeBackupHealthOptions` governing the periodic backup-health monitor: whether it runs and the default re-verification cadence. Health monitoring is auto-enrolled and on by default, so this is only needed to change the cadence or disable the monitor. The monitor stays inert against a non-durable sink regardless of these options. Throws `ArgumentNullException` when `builder` or `configure` is null. |

## Services

### `ILatticeBackupCaptureService`

The full-capture engine.

- `Task<LatticeBackupCaptureResult> CaptureAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)` - captures a full backup of the request's scope and returns its content-addressed id and manifest. Throws `ArgumentNullException` (`request` null), `LatticeAuthorizationDeniedException` (unauthorized), `LatticeSnapshotReplayBudgetExceededException` (scope exceeds the replay budget), `LatticeSaturatedException` (snapshot open shed under saturation), and `LatticeCursorSnapshotExpiredException` (pinned snapshot expired mid-capture). With a tenancy add-on active it also throws `LatticeBackupTenantIsolationException` (the tree is outside the caller's tenant) and `LatticeTenantAccessDeniedException` (the capture is not admitted under the calling tenant's request-rate budget).
- `Task<LatticeBackupSetCaptureResult> CaptureSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)` - captures a backup set: one full backup per scope grouped under a single set manifest. When `CrossTreeConsistent` is set and the set spans more than one tree, every tree is captured inside a single causal fence, selected after in-flight cross-tree atomic sagas drain, so a cross-tree atomic write is never torn across the set (each tree is still captured by its own snapshot, one after another). When the set spans more than one tree, every member manifest is stamped with the set's `SetId`, name, and capture time (`SetCreatedAtUtc`) so the catalogued per-tree backups can be grouped back into one logical set entry, and the returned `BackupSetManifest.SetId` is that same id. A set of a **single** scope is deliberately left unstamped - it is indistinguishable from a plain backup and lists as one - so its returned `SetId` is `null`: the create response agrees with the catalog row, and there is no id that resolves to nothing. Restore such a backup with `RestoreAsync(backupId)`, not `RestoreSetAsync`. Throws the same exceptions as `CaptureAsync`, plus `LatticeBackupCrossTreeFenceException` when a stable fence cannot be established within the configured attempts or drain timeout.

### `ILatticeBackupIncrementalCaptureService`

The incremental-capture engine.

- `Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)` - captures an incremental backup layered on a base backup and records the base id as the manifest's `BaseBackupId`. The base manifest is read from the sink, and the increment inherits the base's scope (the request scope is advisory). When a sound delta cannot be produced - the base resume point was trimmed off the WAL, a range delete surfaced in the delta window, the base chain was captured on a different cluster, or the base predates the recording of the atomic batches it held back as undecided - it falls back to a fresh full capture and returns that result instead. Throws `ArgumentNullException` when `request` is null, `KeyNotFoundException` when the sink holds no manifest for `BaseBackupId`, and `LatticeAuthorizationDeniedException` when the caller is not authorized over the base's scope, plus the tenancy exceptions `CaptureAsync` documents. A fallback full capture can also throw the replay-budget, saturation, and snapshot-expiry exceptions `CaptureAsync` documents.

### `ILatticeBackupRestoreService`

The causally-faithful restore engine.

- `Task<LatticeRestoreResult> RestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - resolves the backup's manifest (catalog first, then the sink) and authorizes the restore; only then, when the target tree is replicated, is the request handed to the coordinated restore path through `IRestoreSagaDispatcher`, whose result it returns. Otherwise it walks the base chain, validates every artifact against its recorded digest, then applies the entries per `LatticeRestoreRequest.Mode` (in-place bulk-load / merge, or atomic shadow-cutover). Idempotent under retry. Authorization covers both trees the restore names: `Restore` over the target tree, and - only when `TargetTreeId` retargets the backup onto a different tree than the one captured - `Backup` over the source tree recorded in each chain manifest's own scope, at the range actually replayed. A same-tree restore is unaffected. Throws `ArgumentNullException` (`request` null), `ArgumentException` (the target is a reserved `sys-backup-*` tree), `LatticeRestoreValidationException` (pre-apply validation failure, including a backup absent from both the catalog and the sink, a broken chain, or a requested sub-scope outside the captured scope; for a replicated target, also a coordinated restore that was refused before it started or aborted), and `LatticeAuthorizationDeniedException` (unauthorized). With a tenancy add-on active it also throws `LatticeBackupTenantIsolationException` when the target tree, or a retargeted source tree, is outside the caller's tenant. A `ShadowCutover` restore also throws `InvalidOperationException` when the target tree is deleted, a delete of it is pending, or another alias change of it (a resize, a schema remediation, or a different restore or revert) holds its alias reservation, and `LatticeTreeOwnershipDeniedException` when the registered tree ownership guard refuses its alias swap; see [Architecture](architecture.md#restore-pipeline).
- `Task<IReadOnlyList<LatticeRestoreResult>> RestoreSetAsync(string setId, CancellationToken cancellationToken = default)` - expands a captured backup set into its member trees (by scanning the catalog for the manifests stamped with `setId`), authorizes `Restore` over every member tree before anything is dispatched, and restores each one via shadow-cutover, promoting to a single all-or-nothing coordinated saga when any member is replicated. A set naming one tree the caller may not restore is therefore refused whole with `LatticeAuthorizationDeniedException` (or, with a tenancy add-on active, `LatticeBackupTenantIsolationException`). Throws `ArgumentException` when `setId` is null or empty, or when it resolves to no member trees. That last failure distinguishes its two causes: an id that equals the set id a single catalogued backup would have been given (the content address of that one member id) names a single-tree set, which is captured as a plain backup and is never stamped as a set member, so the message names the exact `backupId` to pass to `RestoreAsync` instead; any other unresolved id is reported as absent from the catalog. A local per-member restore can also fail with the exceptions `RestoreAsync` documents for a `ShadowCutover` restore.
- `Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)` - reverts a `ShadowCutover` restore by swapping the target tree's registry alias back to `PreviousPhysicalTreeId`. Idempotent. Authorization gates the logical target tree, so the physical tree ids on `restore` are separately re-validated against registry provenance: each must be the target itself, a shadow the engine built for that target, or the target's current physical tree. Throws `ArgumentNullException` (`restore` null), `ArgumentException` (not a shadow-cutover result), `LatticeRestoreValidationException` (a named physical tree does not belong to the target), and `LatticeAuthorizationDeniedException` (unauthorized). Also throws `InvalidOperationException` while the tree is deleted, a delete of it is pending, or another alias change holds its alias reservation, and `LatticeTreeOwnershipDeniedException` when the registered tree ownership guard refuses re-pointing the alias.

### `ILatticeBackupScheduler`

The public entry point for on-demand triggers, schedule registration, and retention. Each operation targets a `BackupScopeSelector` and is coordinated per scope so triggers, scheduled captures, and retention for the same scope never overlap.

- `Task<string?> TriggerFullBackupAsync(BackupScopeSelector scope)` - triggers a full backup; returns the backup id, or `null` when a capture for the scope is already in flight.
- `Task<string?> TriggerIncrementalBackupAsync(BackupScopeSelector scope)` - triggers an incremental backup layered on the most recent backup for the scope (or a full baseline when none exists); returns the backup id, or `null` when skipped by the overlap guard.
- `Task ScheduleRecurringBackupAsync(LatticeBackupScheduleRequest request)` - registers or updates a recurring backup of the request's scope that fires every `Interval`, capturing a full or incremental backup per the request. The interval is clamped up to the reminder minimum; a runtime schedule registered this way overrides the configured `LatticeBackupScheduleOptions` cadence for the chosen kind. Idempotent. Authorizes the caller's `Backup` capability over the scope at registration (the reminder-fired cycle later runs system-origin, because a reminder carries no caller). Throws `ArgumentNullException` when `request` is null and `LatticeAuthorizationDeniedException` when the caller is not authorized.
- `Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental)` - removes the scope's recurring schedule for one kind (`incremental` selects the incremental schedule, otherwise the full one): unregisters that kind's schedule reminder and clears any runtime interval override. Idempotent: a missing schedule is a no-op.
- `Task EnsureScheduleAsync(BackupScopeSelector scope)` - registers or updates the recurring full and incremental schedule reminders for the scope per its `LatticeBackupScheduleOptions`: an enabled kind is (re-)registered at its configured cadence and a disabled kind's reminder is unregistered. It consults only the configured options, so running it after `ScheduleRecurringBackupAsync` replaces (or, for a kind disabled in options, removes) that kind's runtime schedule reminder; the runtime interval that call recorded is left in place, so `BackupSchedulerRuntimeStatus` keeps reporting it until `CancelScheduleAsync` clears it. Idempotent. Authorizes the caller's `Backup` capability over the scope and throws `LatticeAuthorizationDeniedException` when it is not granted.
- `Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)` - prunes the scope's backup chain per its retention policy, preserving the base chain of every retained increment; a no-op that retains everything when retention is disabled.

All five scope-typed methods throw `ArgumentNullException` when `scope` is null. The two triggers are authorized by the capture engine they run, and `ScheduleRecurringBackupAsync` and `EnsureScheduleAsync` authorize at registration; `CancelScheduleAsync` and `PruneAsync` perform no authorization check of their own.

### `ILatticeBackupCatalogStore`

The durable, introspectable index of manifests, persisted into the reserved `sys-backup-catalog` tree keyed by backup id.

- `Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)` - registers or replaces a manifest, keyed by `manifest.Id`. Re-registering an id that is already catalogued carries the existing row's `CreatedAtUtc` forward (a backup id is a content address, so the capture time is immutable); every other field, set membership included, takes the incoming manifest's value. Idempotent. Throws `ArgumentNullException` when `manifest` is null.
- `Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default)` - reads a manifest, or `null`. Throws `ArgumentException` when `backupId` is null or empty.
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)` - removes a manifest; returns `true` when one was removed. Throws `ArgumentException` when `backupId` is null or empty.
- `IAsyncEnumerable<BackupManifest> ListAsync(CancellationToken cancellationToken = default)` - enumerates every catalogued manifest in backup-id order.

### `ILatticeBackupSink`

The pluggable storage sink a backup is written to and restored from. Stores streamed artifacts and self-describing manifests; the artifact surface moves the payload as an ordered chunk sequence so a large tree streams without being materialized whole. Re-writing the same artifact id with the same content is idempotent. The capture engine names each artifact with a per-capture id (scope tree id, scope kind, capture ticks, and a GUID) and records the artifact's SHA-256 digest (`BackupContentHash`) in the manifest's content descriptor; the content-addressed id is the backup (manifest) id, not the artifact id.

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

- `Task<LatticeRestoreResult> ColdRestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - bootstraps the reserved `sys-` trees if they are absent, resolves the target (tip) manifest from the sink alone (never the catalog), then hands the request to the ordinary restore engine (`ILatticeBackupRestoreService.RestoreAsync`), which walks the `BaseBackupId` chain catalog-first with a sink fallback - so on a cluster that lost its catalog the whole chain resolves from the sink - validates every referenced artifact, read from the sink, against its recorded digest, and replays the chain through the HLC-preserving seams; it then re-projects the catalog from the sink so the recovered cluster is left with a correct catalog. Reuses `LatticeRestoreRequest` / `LatticeRestoreResult`. Idempotent. Throws `LatticeRestoreValidationException` when the backup is absent from the sink, the base chain is broken, or an artifact is missing or tampered; `LatticeAuthorizationDeniedException` when the caller is not authorized (the delegated restore authorizes exactly as `RestoreAsync` does); and `ArgumentNullException` when `request` is null. The delegated restore can also throw the other exceptions `RestoreAsync` documents.

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

The periodic monitor itself is an internal reminder-driven grain (mirroring the backup scheduler): once per sweep it enumerates the catalog and re-verifies each enrolled backup whose configured interval has elapsed, writing the result through `ILatticeBackupHealthStore`. It is inert unless the registered sink reports `IsDurable` and `LatticeBackupHealthOptions.Enabled` is `true`.

## Extension seams (replication-aware backup)

These backup-package-local seams let the replication package layer replication awareness on top of the backup engine - an atomic multi-tree, multi-cluster restore, and a capture-side check that the sink is actually shared - without the backup package taking a dependency on replication. The dispatch, membership, and probe seams (`IRestoreSagaDispatcher`, `IReplicatedTreeMembership`, `IBackupSinkSharingProbe`) each have a default no-op registration installed by `AddLatticeBackup`, so a single-cluster host always takes the plain local restore path and never runs a cross-cluster probe; the replication package (or the host) supplies the real implementation. `ILatticeCoordinatedRestoreEngine` and `ILatticeBackupSetResolver` run the other way: `AddLatticeBackup` registers their real implementations (the restore engine itself, and a resolver that scans the catalog for set-stamped manifests), and the replication package consumes them.

### `IRestoreSagaDispatcher`

The seam the restore path consults so a restore into a replicated tree can be promoted to an all-or-nothing coordinated restore across every cluster that replicates the target. The decision is a function of the target tree's current replication status, never of the backup's origin. `RestoreAsync` and `RestoreSetAsync` consult it only after they have authorized the restore, so the coordinated path never runs for a caller the restore gate refuses. The default registration never dispatches.

- `Task<LatticeRestoreResult?> TryDispatchAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - offers a single-tree restore to the coordinated path; returns the local cluster's result when the coordinated path handled it, or `null` to signal the caller should run the plain local restore. Throws `ArgumentNullException` when `request` is null.
- `Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync(string setId, LatticeRestoreMode mode, CancellationToken cancellationToken = default)` - offers a backup-set restore as one atomic unit over the union of the replicated members' peer sets; returns this cluster's per-member results, or `null` when no member is replicated (or the id is not a set id). Throws `ArgumentException` when `setId` is null or empty.

### `IReplicatedTreeMembership`

The seam the startup sink guard consults to learn whether a tree participates in the cross-cluster replication set (a replicated tree must be backed by a shared external sink, not the default in-cluster sink). The default registration reports nothing replicated.

- `bool IsReplicated(string treeId)` - reports whether the tree participates in the replication set. Throws `ArgumentNullException` when `treeId` is null.
- `IReadOnlyCollection<string> ReplicatedTrees { get; }` - the ids of every replicated tree.

### `IBackupSinkSharingProbe`

The capture-side analogue of `IRestoreSagaDispatcher`: the seam the startup sink guard and the periodic health monitor consult to learn whether the sink this cluster captures into is demonstrably the **same** store every peer cluster reads from. That is a deployment fact rather than a locally provable configuration property - two regions can hold identical-looking connection strings that resolve to different accounts - so the real (replication-supplied) implementation writes a tiny marker naming this cluster into its own sink and reads every peer's marker back out of that same sink. The default registration never probes and always reports `NotApplicable`.

- `BackupSinkSharingReport? LastReport { get; }` - the most recent verdict, or `null` when the probe has never run. Read by the per-backup health path so annotating a report costs no I/O.
- `Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)` - runs the probe now and publishes the fresh verdict to `LastReport`. Inert (no sink or network I/O, verdict `NotApplicable`) when no tree is replicated or the deployment has no peers.

### `BackupSinkSharingReport`

The outcome of one cross-cluster sharing probe. Serialized (alias `olb.sh`), immutable.

- Constructor: `BackupSinkSharingReport(BackupSinkSharingStatus status, string clusterId, int peerCount, IReadOnlyList<string> unconfirmedPeerClusterIds, DateTimeOffset probedAtUtc, string explanation)`. Throws `ArgumentNullException` (`clusterId`, `unconfirmedPeerClusterIds`, or `explanation` null) and `ArgumentOutOfRangeException` (`peerCount` negative).
- `BackupSinkSharingStatus Status` - the verdict.
- `string ClusterId` - the local cluster's id, as attested by the marker it wrote.
- `int PeerCount` - the number of peer clusters considered.
- `IReadOnlyList<string> UnconfirmedPeerClusterIds` - the peers whose marker could not be read back from this cluster's sink.
- `DateTimeOffset ProbedAtUtc` - when the probe ran.
- `string Explanation` - an operator-facing sentence naming the unconfirmed peers and the remediation.
- `bool IsRefuted` - `true` only when `Status` is `NotShared`.

### `BackupSinkSharingStatus`

| Value | Meaning |
|-------|---------|
| `NotApplicable` | Nothing was measured: no replicated tree, no peers, no replication package, or the probe is disabled. The value a single-cluster deployment always reports. |
| `Shared` | Every peer's marker was read back from this cluster's sink, so a coordinated restore can resolve the same backup fleet-wide. |
| `Unverified` | At least one peer left no marker and was not reachable, so it may simply not be running yet. Undecided, not a fault. |
| `NotShared` | At least one peer is reachable yet its marker is absent, so the sink is **not** shared and backups of a replicated tree are not restorable fleet-wide. |

### `ILatticeCoordinatedRestoreEngine`

Decomposes the atomic `ShadowCutover` restore into the separate phases a coordinated restore saga drives independently. The single `ILatticeBackupRestoreService.RestoreAsync` entry point composes these same phases for the local path, so both paths share one alias swap. Saga-unaware: it exposes the mechanism without any knowledge of the coordinator, write fence, or participant model.

- `Task<RestoreAdmissionReport> ProbeAdmissionAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - resolves the target's manifest, authorizes `Restore` over the effective restore scope (the same target-side check `BuildShadowAsync` makes), then walks the manifest chain and reports its size and topology without validating artifacts, fencing, or building, so an infeasible target is refused up front. Throws `ArgumentNullException` (`request` null), `LatticeRestoreValidationException` (the backup or a chain member is missing, or a requested sub-scope falls outside the captured scope), and `LatticeAuthorizationDeniedException` (unauthorized).
- `Task<LatticeRestoreResult> BuildShadowAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)` - builds the shadow tree from the manifest chain into a fresh physical tree without swapping the alias or fencing the live tree. Idempotent and resumable. Authorizes both trees exactly as `RestoreAsync` does. Registering the shadow takes the target tree's alias reservation, which a commit or a garbage-collect of the shadow releases. Throws `ArgumentNullException` (`request` null), `ArgumentException` (not a shadow-cutover request, or a reserved `sys-backup-*` target), `LatticeRestoreValidationException` (validation failure), `LatticeAuthorizationDeniedException` (unauthorized), and `InvalidOperationException` (the target tree is deleted, a delete of it is pending, or another alias change holds its reservation).
- `Task CommitShadowAsync(LatticeRestoreResult shadow, CancellationToken cancellationToken = default)` - commits a built shadow by atomically swapping the registry alias, then refreshing routing and converging any covering tag index. The caller engages the write fence around this call. Idempotent. Authorizes `Restore` over the whole target tree. The physical tree ids carried on `shadow` are re-validated against registry provenance, so a result whose `ShadowPhysicalTreeId` or `PreviousPhysicalTreeId` names a tree that is neither the target, nor a shadow the engine built for that target, nor the target's current physical tree is refused. Releases the target tree's alias reservation once the cutover completes. Throws `ArgumentNullException` (`shadow` null), `ArgumentException` (not a shadow-cutover build result), `LatticeRestoreValidationException` (a named physical tree does not belong to the target), `LatticeAuthorizationDeniedException` (unauthorized), `InvalidOperationException` (the target tree is deleted, a delete of it is pending, or a different alias change holds its reservation), and `LatticeTreeOwnershipDeniedException` (the registered tree ownership guard refuses the alias swap).
- `Task DeleteShadowAsync(string shadowPhysicalTreeId, CancellationToken cancellationToken = default)` - reliably garbage-collects an orphaned shadow so an aborted restore leaks no storage. Idempotent: a tree that was never registered is a no-op. Because it purges every shard of the named tree, it deletes only a tree the engine itself stamped as a restore shadow, and authorizes `Restore` over the logical tree that shadow was built for - taken from the shadow's own registry provenance, never from the argument. It also releases the alias reservation the shadow build took on that logical tree, so an aborted restore does not go on refusing the tree's delete or later alias changes. Throws `ArgumentException` when `shadowPhysicalTreeId` is null or empty, `LatticeRestoreValidationException` when the named tree is registered but is not a restore shadow, and `LatticeAuthorizationDeniedException` (unauthorized).
- `string ResolveShadowTreeId(LatticeRestoreRequest request)` - deterministically resolves the shadow tree id a build of `request` would produce, without I/O, so an aborting participant can garbage-collect by id after losing its in-memory state. Throws `ArgumentNullException` (`request` null) and `ArgumentException` (no explicit target tree).

### `ILatticeBackupSetResolver`

The saga-unaware read seam that expands a captured backup-set id into the per-tree member backups it references, so the restore path can restore every tree in a set as one unit.

- `Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync(string setId, CancellationToken cancellationToken = default)` - resolves the set's member backups in tree-id order, or an empty list when the id is not a set id (for example a single-tree backup id). Throws `ArgumentException` when `setId` is null or empty.

### `BackupSetMember`

One resolved member of a captured backup set: the member backup id and the tree it restores. Returned by `ILatticeBackupSetResolver`. An in-process value only (no serializer surface).

- `string BackupId` - the content-addressed id of the member backup.
- `string TreeId` - the tree the member backup restores.

## Tenancy seam

The seam the backup engine consults to keep every capture and restore inside the active tenant's `t/{tenantId}/{name}` namespace and within the tenant's quota. It follows the same null-default pattern as the data-plane tenant gate: `AddLatticeBackup` installs an inert internal implementation (`IsActive` is `false`, every check a no-op), so a host without a tenancy add-on is unchanged; the tenancy add-on registers the active implementation in its place. The tenant is the ambient active tenant, not a parameter.

### `ILatticeBackupTenantScope`

- `bool IsActive { get; }` - `true` when a tenancy add-on has replaced the null default; every tenant check is gated on it.
- `void AuthorizeCapture(string treeId)` - verifies the active tenant may capture `treeId`: a platform tree is left to the authorization gate, and a tenant-owned tree may be captured only by its owning tenant (a flow with no active tenant is likewise left to the authorization gate). Throws `LatticeBackupTenantIsolationException` when refused.
- `void AuthorizeRestoreTarget(string treeId)` - applies the same ownership rule to a restore target, before any record is streamed. Throws `LatticeBackupTenantIsolationException` when refused.
- `ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(string targetTreeId, CancellationToken cancellationToken = default)` - opens a per-record admission controller for a restore into `targetTreeId`, resolving the active tenant's quota once.

### `IBackupRestoreAdmission`

The per-restore admission controller the restore stream consults once per record. A refused record is dead-lettered (skipped), never silently written. Not required to be thread-safe.

- `long AdmittedCount { get; }` - records admitted (written) so far.
- `long DeadLetteredCrossTenant { get; }` - records dead-lettered because they were addressed outside the active tenant's namespace.
- `long DeadLetteredOverQuota { get; }` - records dead-lettered because admitting them would exceed the active tenant's key quota.
- `BackupRestoreRecordDisposition Admit(string key)` - decides whether the record may be written, updating the counters. Throws `ArgumentNullException` when `key` is null.

### `BackupRestoreRecordDisposition`

| Value | Meaning |
|-------|---------|
| `Admit` | Inside the active tenant's namespace and within quota; the record is written. |
| `CrossTenant` | Addressed outside the active tenant's namespace; the record is dead-lettered. |
| `OverQuota` | Would take the active tenant past its key quota; the record is dead-lettered. |

An in-process control value only (no serializer surface).

## Operation constants and helpers

### `BackupOperationKinds`

Public constants for tracked backup operation kinds. `Prefix` is `backup.`, and the concrete kinds are `Capture` (`backup.capture`), `IncrementalCapture` (`backup.incremental-capture`), `SetCapture` (`backup.set-capture`), `Restore` (`backup.restore`), `ColdRestore` (`backup.cold-restore`), `HealthCheck` (`backup.health-check`), `CatalogRebuild` (`backup.catalog-rebuild`), and `CatalogScrub` (`backup.catalog-scrub`).

### `BackupOperationPhases`

Public constants for progress phases reported by tracked operations: `Capturing`, `CapturingMembers`, `Cataloguing`, `Bootstrapping`, `Validating`, `Applying`, `Replaying`, `Verifying`, `RebuildingCatalog`, `ScrubbingCatalog`, and `PruningOrphans`. A kind reports only the phases that apply to the work in hand.

### `BackupOperationUnits`

Public constants for progress unit names: `Entries` (`entries`), `Shards` (`shards`), `Members` (`members`), `Manifests` (`manifests`), and `Artifacts` (`artifacts`).

### `BackupOperationResultKeys`

Public constants for the string result map carried by a succeeded tracked operation: `backupId`, `setId`, `memberBackupIds`, `targetTreeId`, `mode`, `restoreOperationId`, `manifestChain`, `entriesApplied`, `shadowPhysicalTreeId`, `previousPhysicalTreeId`, `deadLetteredCrossTenant`, `deadLetteredOverQuota`, `healthStatus`, `missingArtifactCount`, `hashMismatchArtifactCount`, `scannedCount`, `registeredCount`, `reconciledCount`, `orphanCount`, `removedCount`, `pruned`, and `orphanBackupIds`.

### `BackupOperationResults`

Helpers for reading operation result maps. `TryReadRestoreResult(IReadOnlyDictionary<string, string> result, out LatticeRestoreResult? restore)` reconstructs the full restore result when the map has the restore keys, `ReadMemberBackupIds(IReadOnlyDictionary<string, string> result)` parses the comma-separated set-member backup ids, `TryReadCatalogRebuildReport(IReadOnlyDictionary<string, string> result, out BackupCatalogRebuildReport? report)` reconstructs a catalog rebuild report, and `TryReadCatalogScrubReport(IReadOnlyDictionary<string, string> result, out BackupCatalogScrubReport? report)` reconstructs a catalog scrub report.

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

- Constructor: `LatticeBackupCaptureResult(string backupId, BackupManifest manifest)`. Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`manifest` null).
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

- Constructor: `LatticeRestoreResult(string backupId, string targetTreeId, LatticeRestoreMode mode, string operationId, IReadOnlyList<string> manifestChain, long entriesApplied, string? shadowPhysicalTreeId = null, string? previousPhysicalTreeId = null, long deadLetteredCrossTenant = 0, long deadLetteredOverQuota = 0)`. Throws `ArgumentException` (`backupId`, `targetTreeId`, or `operationId` null/empty), `ArgumentNullException` (`manifestChain` null), `ArgumentOutOfRangeException` (`entriesApplied`, `deadLetteredCrossTenant`, or `deadLetteredOverQuota` negative).
- Properties: `string BackupId`, `string TargetTreeId`, `LatticeRestoreMode Mode`, `string OperationId`, `IReadOnlyList<string> ManifestChain`, `long EntriesApplied`, `string? ShadowPhysicalTreeId`, `string? PreviousPhysicalTreeId`, `long DeadLetteredCrossTenant` (records dead-lettered because they were addressed outside the active tenant's namespace), and `long DeadLetteredOverQuota` (records dead-lettered because admitting them would exceed the active tenant's key quota); both are zero when no tenancy add-on is active.

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

- Constructor: `BackupManifest(string id, string name, DateTimeOffset createdAtUtc, BackupKind kind, BackupScopeSelector scope, BackupConsistencyCut consistencyCut, BackupTopologySnapshot topology, string structuralDigest, IReadOnlyList<BackupKeyDescriptor> keyDescriptors, IReadOnlyList<BackupContentDescriptor> contentDescriptors, IReadOnlyList<BackupOriginProvenance> provenance, string? baseBackupId = null, BackupCompressionDictionaryRef? compressionDictionary = null, string? capturingClusterId = null)`. Validates that `id` is non-empty and free of the reserved unit-separator (U+001F); that `structuralDigest` is non-empty; that an `Incremental` backup carries a non-empty `baseBackupId` and a `Full` backup carries none; and null-checks the reference-type members.
- Properties: `string Id`, `string Name`, `DateTimeOffset CreatedAtUtc`, `BackupKind Kind`, `BackupScopeSelector Scope`, `BackupConsistencyCut ConsistencyCut`, `BackupTopologySnapshot Topology`, `string StructuralDigest`, `IReadOnlyList<BackupKeyDescriptor> KeyDescriptors`, `IReadOnlyList<BackupContentDescriptor> ContentDescriptors`, `IReadOnlyList<BackupOriginProvenance> Provenance`, `string? BaseBackupId`, `BackupCompressionDictionaryRef? CompressionDictionary`, `string? SetId`, `string? SetName`, `DateTimeOffset? SetCreatedAtUtc`, `string? CapturingClusterId`. `SetId`, `SetName`, and `SetCreatedAtUtc` are non-null only on a backup captured as a member of a multi-tree set: every member of one set shares the same values, so a catalog consumer can group the per-tree members into a single logical entry from a first-class fact rather than inferring it from the backup name, and the catalog index orders the members to one shared position. They are stamped when the set is captured; because re-registering a backup id replaces every field except the capture time (see `ILatticeBackupCatalogStore.RegisterAsync`), a later capture that reproduces a member's bytes - a standalone capture of the unchanged tree, or its capture into another set - re-registers that member with the later capture's set fields. A single-tree set leaves them null, so it lists as an ordinary backup. `CapturingClusterId` names the cluster that authored the capture (distinct from the per-entry origins in `Provenance`); it is stamped on every capture, an incremental inherits its base's value so a whole chain shares one stamp, and `null` marks a manifest captured before the stamp existed (read as the local cluster).

### `BackupConsistencyCut`

The causal cut a backup was taken as of.

- Constructor: `BackupConsistencyCut(long walSequence, long hlcTimestamp, IReadOnlyDictionary<string, long>? perOriginFrontier = null, IReadOnlyDictionary<int, long>? walPartitionOffsets = null, IReadOnlyList<Guid>? undecidedSagaIds = null)`. Throws `ArgumentOutOfRangeException` when `walSequence` or `hlcTimestamp` is negative.
- Properties: `long WalSequence`, `long HlcTimestamp`, `IReadOnlyDictionary<string, long>? PerOriginFrontier`, `IReadOnlyDictionary<int, long>? WalPartitionOffsets` (the per-partition resume offsets an incremental layers on), `IReadOnlyList<Guid>? UndecidedSagaIds` (the atomic writes the capture held pre-saga because they were undecided at its decision gate; `null` for a manifest captured before the field existed). `HlcTimestamp` is the wall-clock component of the highest hybrid-logical-clock stamp the capture read - over the captured entries for a full backup, over the drained delta for an incremental (never below its base's) - and `0` only when the capture (with its whole chain) read nothing, or for a full backup captured by a build that predates this frontier. It is also the frontier an incremental pins the WAL at while it drains forward from its base.
- What a capture records besides `HlcTimestamp`: a full capture sets `WalSequence` to the highest next-to-assign WAL offset across the shards and partitions its snapshot covered, and `WalPartitionOffsets` to the per-partition WAL heads read just before the snapshot opened. An incremental sets `WalPartitionOffsets` to the per-partition frontier its drain reached - held back to the first record of any atomic write it left out because the write was still undecided, so the next incremental reads that write again - and `WalSequence` to the highest of those offsets. Both kinds set `UndecidedSagaIds`: a full capture to the atomic writes pending and undecided in its snapshot, an incremental to those of its base's that are still undecided, so the next incremental looks each one up (#4589). `PerOriginFrontier` is `null` when the captured entries name no origin.

### `BackupTopologySnapshot`

- Constructor: `BackupTopologySnapshot(int shardCount, int virtualShardCount, IReadOnlyList<string> shardRootDigests)`. Throws `ArgumentOutOfRangeException` when `shardCount` or `virtualShardCount` is not positive, `ArgumentNullException` when `shardRootDigests` is null.
- Properties: `int ShardCount`, `int VirtualShardCount`, `IReadOnlyList<string> ShardRootDigests`.
- What a capture records: all three come from the tree's routing map at the capture. `ShardCount` is the number of physical shards the map names, `VirtualShardCount` is the map's slot count (4096 unless the tree was created with a declared virtual shard count), and `ShardRootDigests` holds one digest per physical shard in ascending shard-index order - the digest of the captured range on that shard, or `nodigest-{index}` when the tree does not maintain projection digests. The indices need not be contiguous: a shard consolidation that folds a shard away leaves a gap.

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

- Constructor: `BackupSetManifest(string? setId, string name, DateTimeOffset createdAtUtc, bool crossTreeConsistent, BackupSetFence? fence, IReadOnlyList<string> memberBackupIds)`. Throws `ArgumentException` (`setId` empty, `name` null/empty, `memberBackupIds` empty), `ArgumentNullException` (`memberBackupIds` null). A `null` `setId` is the absence of an id and is accepted; an empty one is a malformed id and is rejected.
- Properties: `string? SetId`, `string Name`, `DateTimeOffset CreatedAtUtc`, `bool CrossTreeConsistent`, `BackupSetFence? Fence`, `IReadOnlyList<string> MemberBackupIds`.
- `SetId` is non-null only for a set spanning two or more trees. Membership is durable only as the per-member `BackupManifest.SetId` stamp, and a single-member set is deliberately left unstamped, so it reports no id rather than one that matches no catalog row. A non-null `SetId` here therefore equals the `SetId` stamped on every member's catalogued manifest when the set was captured.

### `BackupSetFence`

The selected cross-tree causal fence of a cross-tree-consistent set.

- Constructor: `BackupSetFence(long hlcTimestamp, int drainedInFlightCount, double drainWaitMilliseconds, int attempts)`. Throws `ArgumentOutOfRangeException` when `hlcTimestamp`, `drainedInFlightCount`, or `drainWaitMilliseconds` is negative, or `attempts` is not positive.
- Properties: `long HlcTimestamp` (the wall-clock tick count at which the fence was selected), `int DrainedInFlightCount`, `double DrainWaitMilliseconds`, `int Attempts`.

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

- Constructor: `BackupSchedulerRuntimeStatus(bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, TimeSpan? runtimeFullBackupInterval = null, TimeSpan? runtimeIncrementalBackupInterval = null)`.
- Properties mirror the constructor parameters: `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`, `TimeSpan? RuntimeFullBackupInterval`, `TimeSpan? RuntimeIncrementalBackupInterval` (the clamped interval of a runtime schedule registered through `ScheduleRecurringBackupAsync`, or `null` when none is recorded for that kind).

### `BackupCatalogRebuildReport`

The outcome summary of `ILatticeBackupCatalogRebuildService.RebuildFromSinkAsync`. `ScannedCount` always equals `RegisteredCount + ReconciledCount`.

- Constructor: `BackupCatalogRebuildReport(long scannedCount, long registeredCount, long reconciledCount)`.
- Properties: `long ScannedCount` (manifests enumerated from the sink), `long RegisteredCount` (absent from the catalog and freshly added), `long ReconciledCount` (already catalogued and reconciled in place).

### `BackupCatalogScrubReport`

The outcome summary of `ILatticeBackupCatalogScrubService.ScrubAsync`. Non-destructive by default, so `RemovedCount` is zero and `Pruned` is `false` unless the caller opts in to pruning; a flag-only pass still reports every orphan.

- Constructor: `BackupCatalogScrubReport(long scannedCount, long orphanCount, long removedCount, bool pruned, IReadOnlyList<string> orphanBackupIds)`. Throws `ArgumentNullException` when `orphanBackupIds` is null.
- Properties: `long ScannedCount` (catalog rows cross-checked against the sink), `long OrphanCount` (rows with no resolvable sink payload), `long RemovedCount` (orphan rows removed, zero on a non-destructive pass), `bool Pruned` (whether destructive pruning was requested for the pass - `true` even when no orphan was found), `IReadOnlyList<string> OrphanBackupIds` (the ids of the orphans found).

### `BackupSinkResolution`

The read-only outcome of `ILatticeBackupSink.ProbeAsync`: whether a backup is resolvable from the sink alone.

- Constructor: `BackupSinkResolution(string backupId, bool manifestPresent, IReadOnlyList<string> missingArtifactIds)`. Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`missingArtifactIds` null).
- Properties: `string BackupId`, `bool ManifestPresent`, `IReadOnlyList<string> MissingArtifactIds` (referenced artifacts absent, or present but not committed), and the computed `bool IsResolvable` (`true` only when the manifest is present and no artifact is missing).

### `BackupHealthReport`

The result of verifying one backup's durable sink payload - presence plus content-hash consistency, and for a replicated tree whether every peer cluster can read the sink holding it - precise enough to drive a diagnostics dialog. Persisted per backup by `ILatticeBackupHealthStore`.

- Constructor: `BackupHealthReport(string backupId, BackupHealthStatus status, bool manifestPresent, IReadOnlyList<string> missingArtifactIds, IReadOnlyList<string> hashMismatchArtifactIds, DateTimeOffset checkedAtUtc, string explanation, BackupSinkSharingStatus peerVisibility = BackupSinkSharingStatus.NotApplicable, IReadOnlyList<string>? peerUnconfirmedClusterIds = null)`. The two sharing parameters are trailing and defaulted, so every pre-existing call site and every report persisted before the probe existed still means "no cross-cluster claim made". Throws `ArgumentException` (`backupId` null/empty) and `ArgumentNullException` (`missingArtifactIds`, `hashMismatchArtifactIds`, or `explanation` null).
- Properties: `string BackupId`, `BackupHealthStatus Status`, `bool ManifestPresent`, `IReadOnlyList<string> MissingArtifactIds` (referenced artifacts absent or uncommitted), `IReadOnlyList<string> HashMismatchArtifactIds` (present artifacts whose content no longer matches the manifest's recorded hash), `DateTimeOffset CheckedAtUtc`, `string Explanation` (a precise human-readable summary naming the missing / mismatched artifacts and any peer that cannot see the sink), `BackupSinkSharingStatus PeerVisibility`, `IReadOnlyList<string> PeerUnconfirmedClusterIds`, and the computed `bool IsHealthy` (`true` only when `Status` is `Healthy`).

A backup of a **replicated** tree whose sink is positively refuted (`PeerVisibility` is `NotShared`) is reported as `Warning` even when it is locally intact, because a coordinated restore resolves the same manifest chain from every cluster's own sink and would abort. A non-replicated tree's report is unaffected.

### `BackupHealthConfig`

The per-backup health-monitoring override: whether the periodic monitor verifies this backup, and how often. Every backup is auto-enrolled with the configured defaults; this record overrides that for a single backup. Persisted by `ILatticeBackupHealthStore`.

- Constructor: `BackupHealthConfig(bool monitoringEnabled, TimeSpan interval)`. Throws `ArgumentOutOfRangeException` when `interval` is not strictly positive.
- Properties: `bool MonitoringEnabled`, `TimeSpan Interval`.

## Catalog index

### `BackupCatalogIndexProjection`

The `ILatticeViewProjection` behind the backup-catalog index materialised view (created when `LatticeBackupOptions.EnableBackupCatalogIndexView` is `true`). It lowers each catalog registration - a `Set` carrying a `BackupManifest` - into one compact `BackupCatalogIndexRow`, re-keyed so the index scans newest-first with the members of a backup set contiguous; deletes and range deletes project nothing, and the listing drops any index row whose backup no longer exists in the catalog.

- `const string Version` - the projection's code-identity version (`backup-catalog-index-v3`).
- `string ProjectionVersion { get; }` - returns `Version`.
- `IEnumerable<ViewWrite> Project(LatticeMutation mutation)` - maps one catalog mutation to its index-row upsert.

### `BackupCatalogIndexRow`

The compact row the index stores per catalogued backup - exactly the fields the listing filters and sorts on - so a filtered, created-descending, paged query is answered from the index and only the rows that land on the page read their full manifest. Serialized, immutable.

- `string BackupId`, `string Name`, `BackupKind Kind`, `string TreeId`, `DateTimeOffset CreatedAtUtc` - the indexed backup's id, name, kind, scope tree, and capture time.
- `string? SetId`, `string? SetName` - the backup set the backup belongs to, or `null` when it was captured standalone.
- `string? BaseBackupId` - the base an incremental is layered on, or `null` for a full backup.
- `string DisplayName` - `SetName` when the backup belongs to a set, otherwise `Name`.

## Enums

### `BackupKind`

`Full = 0`, `Incremental = 1`.

### `BackupScopeKind`

`WholeTree = 0`, `Prefix = 1`, `Key = 2`.

### `BackupKeyMergeMode`

`LastWriterWins = 0`, `Crdt = 1`.

### `LatticeRestoreMode`

`InPlace = 0` (a bottom-up bulk-load fast path when the target tree has never been registered and a single full whole-tree backup is restored with no narrower sub-scope, otherwise a last-writer-wins merge that converges with whatever the target already holds), `ShadowCutover = 1` (build a fresh physical tree and atomically swap the registry alias).

### `BackupScopeRunOutcome`

`None = 0`, `Success = 1`, `Failure = 2`, `Denied = 3` (the last cycle was refused by the access gate, recorded apart from a generic `Failure`).

### `BackupHealthStatus`

`Unknown = 0` (never verified), `Healthy = 1` (manifest and every artifact present, committed, and hash-matched), `Warning = 2` (manifest present but at least one artifact missing, uncommitted, or hash-mismatched - or, for a replicated tree, the sink is not readable from a peer cluster), `Missing = 3` (manifest itself absent - the catalog row is an orphan).

### `BackupSinkSharingEnforcement`

`Disabled = 0` (never probe), `Warn = 1` (the default - probe, log loudly, annotate health, but start), `FailFast = 2` (a positively refuted sink blocks silo start). See [Configuration](configuration.md).

## Options

`LatticeBackupOptions`, `LatticeBackupScheduleOptions`, and `LatticeBackupHealthOptions` are documented in full in [Configuration](configuration.md). `LatticeBackupHealthOptions` configures the periodic health monitor cluster-wide: `bool Enabled` (default `true` - health monitoring is auto-enrolled) and `TimeSpan DefaultInterval` (default six hours), plus the static `MinimumInterval` (one minute) the sweep reminder clamps up to and the static `DefaultSweepInterval` (six hours) that `DefaultInterval` defaults to. The monitor stays inert against a non-durable sink regardless of these options.

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

### `LatticeBackupTenantIsolationException` : `InvalidOperationException`

Thrown by `ILatticeBackupTenantScope.AuthorizeCapture` / `AuthorizeRestoreTarget` when a capture or restore would cross the active tenant's isolation boundary. The operation is refused before any data is read or written. Constructors: `(string message)` and `(string message, Exception innerException)`.

### `LatticeRestoreValidationException` : `InvalidOperationException`

Thrown by `RestoreAsync` when a backup fails pre-apply validation (for example an artifact whose bytes do not match its recorded content digest), and by the other restore entry points - `ColdRestoreAsync`, `RestoreSetAsync`'s per-member restores, `RevertRestoreAsync` (a named physical tree that does not belong to the target), and the `ILatticeCoordinatedRestoreEngine` seams. The replication package's coordinated restore path also throws it when a replicated restore is refused before it starts or aborts. Constructors: `(string message)` and `(string message, Exception innerException)`.

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Backup.BackupCatalogIndexProjection`

[Source](../../src/lattice.backup/BackupCatalogIndexProjection.cs) (line 33).

`public sealed class BackupCatalogIndexProjection : ILatticeViewProjection`

- `public const string Version`
- `public string ProjectionVersion`
- `public IEnumerable<ViewWrite> Project(LatticeMutation mutation)`

### `Orleans.Lattice.Backup.BackupCatalogIndexRow`

[Source](../../src/lattice.backup/BackupCatalogIndexRow.cs) (line 17).

`public sealed record BackupCatalogIndexRow`

- `public string BackupId { get; init; }`
- `public string Name { get; init; }`
- `public BackupKind Kind { get; init; }`
- `public string TreeId { get; init; }`
- `public DateTimeOffset CreatedAtUtc { get; init; }`
- `public string? SetId { get; init; }`
- `public string? SetName { get; init; }`
- `public string? BaseBackupId { get; init; }`
- `public string DisplayName`

### `Orleans.Lattice.Backup.BackupCatalogRebuildReport`

[Source](../../src/lattice.backup/BackupCatalogRebuildReport.cs) (line 13).

`public sealed record BackupCatalogRebuildReport`

- `public BackupCatalogRebuildReport(long scannedCount, long registeredCount, long reconciledCount)`
- `public long ScannedCount { get; init; }`
- `public long RegisteredCount { get; init; }`
- `public long ReconciledCount { get; init; }`

### `Orleans.Lattice.Backup.BackupCatalogScrubReport`

[Source](../../src/lattice.backup/BackupCatalogScrubReport.cs) (line 15).

`public sealed record BackupCatalogScrubReport`

- `public BackupCatalogScrubReport( long scannedCount, long orphanCount, long removedCount, bool pruned, IReadOnlyList<string> orphanBackupIds)`
- `public long ScannedCount { get; init; }`
- `public long OrphanCount { get; init; }`
- `public long RemovedCount { get; init; }`
- `public bool Pruned { get; init; }`
- `public IReadOnlyList<string> OrphanBackupIds { get; init; }`

### `Orleans.Lattice.Backup.BackupCompressionDictionaryRef`

[Source](../../src/lattice.backup/BackupCompressionDictionaryRef.cs) (line 9).

`public sealed record BackupCompressionDictionaryRef`

- `public BackupCompressionDictionaryRef(string dictionaryId, string digest)`
- `public string DictionaryId { get; init; }`
- `public string Digest { get; init; }`

### `Orleans.Lattice.Backup.BackupConsistencyCut`

[Source](../../src/lattice.backup/BackupConsistencyCut.cs) (line 11).

`public sealed record BackupConsistencyCut`

- `public BackupConsistencyCut( long walSequence, long hlcTimestamp, IReadOnlyDictionary<string, long>? perOriginFrontier = null, IReadOnlyDictionary<int, long>? walPartitionOffsets = null, IReadOnlyList<Guid>? undecidedSagaIds = null)`
- `public long WalSequence { get; init; }`
- `public long HlcTimestamp { get; init; }`
- `public IReadOnlyDictionary<string, long>? PerOriginFrontier { get; init; }`
- `public IReadOnlyDictionary<int, long>? WalPartitionOffsets { get; init; }`
- `public IReadOnlyList<Guid>? UndecidedSagaIds { get; init; }`

### `Orleans.Lattice.Backup.BackupContentDescriptor`

[Source](../../src/lattice.backup/BackupContentDescriptor.cs) (line 18).

`public sealed record BackupContentDescriptor`

- `public BackupContentDescriptor( string artifactId, string contentHash, long byteLength, int chunkCount, BackupScopeSelector scope)`
- `public string ArtifactId { get; init; }`
- `public string ContentHash { get; init; }`
- `public long ByteLength { get; init; }`
- `public int ChunkCount { get; init; }`
- `public BackupScopeSelector Scope { get; init; }`

### `Orleans.Lattice.Backup.BackupContentHash`

[Source](../../src/lattice.backup/BackupContentHash.cs) (line 14).

`public static class BackupContentHash`

- `public static string Compute(ReadOnlySpan<byte> content)`
- `public static string Compute(IEnumerable<ReadOnlyMemory<byte>> chunks)`

### `Orleans.Lattice.Backup.BackupHealthConfig`

[Source](../../src/lattice.backup/BackupHealthConfig.cs) (line 16).

`public sealed record BackupHealthConfig`

- `public BackupHealthConfig(bool monitoringEnabled, TimeSpan interval)`
- `public bool MonitoringEnabled { get; init; }`
- `public TimeSpan Interval { get; init; }`

### `Orleans.Lattice.Backup.BackupHealthReport`

[Source](../../src/lattice.backup/BackupHealthReport.cs) (line 20).

`public sealed record BackupHealthReport`

- `public BackupHealthReport( string backupId, BackupHealthStatus status, bool manifestPresent, IReadOnlyList<string> missingArtifactIds, IReadOnlyList<string> hashMismatchArtifactIds, DateTimeOffset checkedAtUtc, string explanation, BackupSinkSharingStatus peerVisibility = BackupSinkSharingStatus.NotApplicable, IReadOnlyList<string>? peerUnconfirmedClusterIds = null)`
- `public string BackupId { get; init; }`
- `public BackupHealthStatus Status { get; init; }`
- `public bool ManifestPresent { get; init; }`
- `public IReadOnlyList<string> MissingArtifactIds { get; init; }`
- `public IReadOnlyList<string> HashMismatchArtifactIds { get; init; }`
- `public DateTimeOffset CheckedAtUtc { get; init; }`
- `public string Explanation { get; init; }`
- `public BackupSinkSharingStatus PeerVisibility { get; init; }`
- `public IReadOnlyList<string> PeerUnconfirmedClusterIds { get; init; }`
- `public bool IsHealthy`

### `Orleans.Lattice.Backup.BackupHealthStatus`

[Source](../../src/lattice.backup/BackupHealthStatus.cs) (line 11).

`public enum BackupHealthStatus`

- `Unknown = 0`
- `Healthy = 1`
- `Warning = 2`
- `Missing = 3`

### `Orleans.Lattice.Backup.BackupKeyDescriptor`

[Source](../../src/lattice.backup/BackupKeyDescriptor.cs) (line 9).

`public sealed record BackupKeyDescriptor`

- `public BackupKeyDescriptor(string key, BackupKeyMergeMode mergeMode, string? originId = null)`
- `public string Key { get; init; }`
- `public BackupKeyMergeMode MergeMode { get; init; }`
- `public string? OriginId { get; init; }`

### `Orleans.Lattice.Backup.BackupKeyMergeMode`

[Source](../../src/lattice.backup/BackupKeyMergeMode.cs) (line 9).

`public enum BackupKeyMergeMode`

- `LastWriterWins = 0`
- `Crdt = 1`

### `Orleans.Lattice.Backup.BackupKind`

[Source](../../src/lattice.backup/BackupKind.cs) (line 7).

`public enum BackupKind`

- `Full = 0`
- `Incremental = 1`

### `Orleans.Lattice.Backup.BackupManifest`

[Source](../../src/lattice.backup/BackupManifest.cs) (line 14).

`public sealed record BackupManifest`

- `public BackupManifest( string id, string name, DateTimeOffset createdAtUtc, BackupKind kind, BackupScopeSelector scope, BackupConsistencyCut consistencyCut, BackupTopologySnapshot topology, string structuralDigest, IReadOnlyList<BackupKeyDescriptor> keyDescriptors, IReadOnlyList<BackupContentDescriptor> contentDescriptors, IReadOnlyList<BackupOriginProvenance> provenance, string? baseBackupId = null, BackupCompressionDictionaryRef? compressionDictionary = null, string? capturingClusterId = null)`
- `public string Id { get; init; }`
- `public string Name { get; init; }`
- `public DateTimeOffset CreatedAtUtc { get; init; }`
- `public BackupKind Kind { get; init; }`
- `public BackupScopeSelector Scope { get; init; }`
- `public BackupConsistencyCut ConsistencyCut { get; init; }`
- `public BackupTopologySnapshot Topology { get; init; }`
- `public string StructuralDigest { get; init; }`
- `public IReadOnlyList<BackupKeyDescriptor> KeyDescriptors { get; init; }`
- `public IReadOnlyList<BackupContentDescriptor> ContentDescriptors { get; init; }`
- `public IReadOnlyList<BackupOriginProvenance> Provenance { get; init; }`
- `public string? BaseBackupId { get; init; }`
- `public BackupCompressionDictionaryRef? CompressionDictionary { get; init; }`
- `public string? SetId { get; init; }`
- `public string? SetName { get; init; }`
- `public DateTimeOffset? SetCreatedAtUtc { get; init; }`
- `public string? CapturingClusterId { get; init; }`

### `Orleans.Lattice.Backup.BackupMetrics`

[Source](../../src/lattice.backup/BackupMetrics.cs) (line 13).

`public static class BackupMetrics`

- `public const string MeterName`
- `public const string TagTreeCount`
- `public static readonly Meter Meter`
- `public static readonly Counter<long> CrossTreeFenceSelections`
- `public static readonly Counter<long> CrossTreeFenceDrainedInFlight`
- `public static readonly Counter<long> CrossTreeFenceRetries`
- `public static readonly Histogram<double> CrossTreeFenceDrainWaitMilliseconds`

### `Orleans.Lattice.Backup.BackupOperationKinds`

[Source](../../src/lattice.backup/BackupOperationKinds.cs) (line 8).

`public static class BackupOperationKinds`

- `public const string Prefix`
- `public const string Capture`
- `public const string IncrementalCapture`
- `public const string SetCapture`
- `public const string Restore`
- `public const string ColdRestore`
- `public const string HealthCheck`
- `public const string CatalogRebuild`
- `public const string CatalogScrub`

### `Orleans.Lattice.Backup.BackupOperationPhases`

[Source](../../src/lattice.backup/BackupOperationPhases.cs) (line 8).

`public static class BackupOperationPhases`

- `public const string Capturing`
- `public const string CapturingMembers`
- `public const string Cataloguing`
- `public const string Bootstrapping`
- `public const string Validating`
- `public const string Applying`
- `public const string Replaying`
- `public const string Verifying`
- `public const string RebuildingCatalog`
- `public const string ScrubbingCatalog`
- `public const string PruningOrphans`

### `Orleans.Lattice.Backup.BackupOperationResultKeys`

[Source](../../src/lattice.backup/BackupOperationResultKeys.cs) (line 7).

`public static class BackupOperationResultKeys`

- `public const string BackupId`
- `public const string SetId`
- `public const string MemberBackupIds`
- `public const string TargetTreeId`
- `public const string Mode`
- `public const string RestoreOperationId`
- `public const string ManifestChain`
- `public const string EntriesApplied`
- `public const string ShadowPhysicalTreeId`
- `public const string PreviousPhysicalTreeId`
- `public const string DeadLetteredCrossTenant`
- `public const string DeadLetteredOverQuota`
- `public const string HealthStatus`
- `public const string MissingArtifactCount`
- `public const string HashMismatchArtifactCount`
- `public const string ScannedCount`
- `public const string RegisteredCount`
- `public const string ReconciledCount`
- `public const string OrphanCount`
- `public const string RemovedCount`
- `public const string Pruned`
- `public const string OrphanBackupIds`

### `Orleans.Lattice.Backup.BackupOperationResults`

[Source](../../src/lattice.backup/BackupOperationResults.cs) (line 10).

`public static class BackupOperationResults`

- `public static bool TryReadRestoreResult( IReadOnlyDictionary<string, string> result, out LatticeRestoreResult? restore)`
- `public static IReadOnlyList<string> ReadMemberBackupIds(IReadOnlyDictionary<string, string> result)`
- `public static bool TryReadCatalogRebuildReport( IReadOnlyDictionary<string, string> result, out BackupCatalogRebuildReport? report)`
- `public static bool TryReadCatalogScrubReport( IReadOnlyDictionary<string, string> result, out BackupCatalogScrubReport? report)`

### `Orleans.Lattice.Backup.BackupOperationUnits`

[Source](../../src/lattice.backup/BackupOperationUnits.cs) (line 6).

`public static class BackupOperationUnits`

- `public const string Entries`
- `public const string Shards`
- `public const string Members`
- `public const string Manifests`
- `public const string Artifacts`

### `Orleans.Lattice.Backup.BackupOriginProvenance`

[Source](../../src/lattice.backup/BackupOriginProvenance.cs) (line 9).

`public sealed record BackupOriginProvenance`

- `public BackupOriginProvenance(string originId, long highWaterSequence)`
- `public string OriginId { get; init; }`
- `public long HighWaterSequence { get; init; }`

### `Orleans.Lattice.Backup.BackupRestoreRecordDisposition`

[Source](../../src/lattice.backup/BackupRestoreRecordDisposition.cs) (line 10).

`public enum BackupRestoreRecordDisposition`

- `Admit = 0`
- `CrossTenant = 1`
- `OverQuota = 2`

### `Orleans.Lattice.Backup.BackupRetentionReport`

[Source](../../src/lattice.backup/BackupRetentionReport.cs) (line 11).

`public sealed record BackupRetentionReport`

- `public BackupRetentionReport(int retainedCount, IReadOnlyList<string> prunedBackupIds)`
- `public int RetainedCount { get; init; }`
- `public IReadOnlyList<string> PrunedBackupIds { get; init; }`
- `public int PrunedCount`
- `public static BackupRetentionReport Empty { get; }`

### `Orleans.Lattice.Backup.BackupSchedulerRuntimeStatus`

[Source](../../src/lattice.backup/BackupSchedulerRuntimeStatus.cs) (line 11).

`public sealed record BackupSchedulerRuntimeStatus`

- `public BackupSchedulerRuntimeStatus( bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, TimeSpan? runtimeFullBackupInterval = null, TimeSpan? runtimeIncrementalBackupInterval = null)`
- `public bool FullScheduleRegistered { get; init; }`
- `public bool IncrementalScheduleRegistered { get; init; }`
- `public DateTimeOffset? LastFullRunUtc { get; init; }`
- `public DateTimeOffset? LastFullSuccessUtc { get; init; }`
- `public DateTimeOffset? LastIncrementalRunUtc { get; init; }`
- `public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }`
- `public BackupScopeRunOutcome LastRunOutcome { get; init; }`
- `public TimeSpan? RuntimeFullBackupInterval { get; init; }`
- `public TimeSpan? RuntimeIncrementalBackupInterval { get; init; }`

### `Orleans.Lattice.Backup.BackupScopeKey`

[Source](../../src/lattice.backup/BackupScopeKey.cs) (line 13).

`public static class BackupScopeKey`

- `public static string For(BackupScopeSelector scope)`

### `Orleans.Lattice.Backup.BackupScopeKind`

[Source](../../src/lattice.backup/BackupScopeKind.cs) (line 8).

`public enum BackupScopeKind`

- `WholeTree = 0`
- `Prefix = 1`
- `Key = 2`

### `Orleans.Lattice.Backup.BackupScopeRunOutcome`

[Source](../../src/lattice.backup/BackupScopeRunOutcome.cs) (line 9).

`public enum BackupScopeRunOutcome`

- `None = 0`
- `Success = 1`
- `Failure = 2`
- `Denied = 3`

### `Orleans.Lattice.Backup.BackupScopeSelector`

[Source](../../src/lattice.backup/BackupScopeSelector.cs) (line 14).

`public sealed record BackupScopeSelector`

- `public BackupScopeSelector(BackupScopeKind kind, string treeId, string? keyOrPrefix = null)`
- `public BackupScopeKind Kind { get; init; }`
- `public string TreeId { get; init; }`
- `public string? KeyOrPrefix { get; init; }`
- `public static BackupScopeSelector WholeTree(string treeId)`
- `public static BackupScopeSelector Prefix(string treeId, string prefix)`
- `public static BackupScopeSelector Key(string treeId, string key)`

### `Orleans.Lattice.Backup.BackupSetFence`

[Source](../../src/lattice.backup/BackupSetFence.cs) (line 20).

`public sealed record BackupSetFence`

- `public BackupSetFence( long hlcTimestamp, int drainedInFlightCount, double drainWaitMilliseconds, int attempts)`
- `public long HlcTimestamp { get; init; }`
- `public int DrainedInFlightCount { get; init; }`
- `public double DrainWaitMilliseconds { get; init; }`
- `public int Attempts { get; init; }`

### `Orleans.Lattice.Backup.BackupSetManifest`

[Source](../../src/lattice.backup/BackupSetManifest.cs) (line 12).

`public sealed record BackupSetManifest`

- `public BackupSetManifest( string? setId, string name, DateTimeOffset createdAtUtc, bool crossTreeConsistent, BackupSetFence? fence, IReadOnlyList<string> memberBackupIds)`
- `public string? SetId { get; init; }`
- `public string Name { get; init; }`
- `public DateTimeOffset CreatedAtUtc { get; init; }`
- `public bool CrossTreeConsistent { get; init; }`
- `public BackupSetFence? Fence { get; init; }`
- `public IReadOnlyList<string> MemberBackupIds { get; init; }`

### `Orleans.Lattice.Backup.BackupSetMember`

[Source](../../src/lattice.backup/BackupSetMember.cs) (line 12).

`public readonly record struct BackupSetMember(string BackupId, string TreeId)`

- `Primary constructor / positional members: (string BackupId, string TreeId)`

### `Orleans.Lattice.Backup.BackupSinkResolution`

[Source](../../src/lattice.backup/BackupSinkResolution.cs) (line 19).

`public sealed record BackupSinkResolution`

- `public BackupSinkResolution(string backupId, bool manifestPresent, IReadOnlyList<string> missingArtifactIds)`
- `public string BackupId { get; init; }`
- `public bool ManifestPresent { get; init; }`
- `public IReadOnlyList<string> MissingArtifactIds { get; init; }`
- `public bool IsResolvable`

### `Orleans.Lattice.Backup.BackupSinkSharingEnforcement`

[Source](../../src/lattice.backup/BackupSinkSharingEnforcement.cs) (line 9).

`public enum BackupSinkSharingEnforcement`

- `Disabled = 0`
- `Warn = 1`
- `FailFast = 2`

### `Orleans.Lattice.Backup.BackupSinkSharingReport`

[Source](../../src/lattice.backup/BackupSinkSharingReport.cs) (line 19).

`public sealed record BackupSinkSharingReport`

- `public BackupSinkSharingReport( BackupSinkSharingStatus status, string clusterId, int peerCount, IReadOnlyList<string> unconfirmedPeerClusterIds, DateTimeOffset probedAtUtc, string explanation)`
- `public BackupSinkSharingStatus Status { get; init; }`
- `public string ClusterId { get; init; }`
- `public int PeerCount { get; init; }`
- `public IReadOnlyList<string> UnconfirmedPeerClusterIds { get; init; }`
- `public DateTimeOffset ProbedAtUtc { get; init; }`
- `public string Explanation { get; init; }`
- `public bool IsRefuted`

### `Orleans.Lattice.Backup.BackupSinkSharingStatus`

[Source](../../src/lattice.backup/BackupSinkSharingStatus.cs) (line 17).

`public enum BackupSinkSharingStatus`

- `NotApplicable = 0`
- `Shared = 1`
- `Unverified = 2`
- `NotShared = 3`

### `Orleans.Lattice.Backup.BackupTopologySnapshot`

[Source](../../src/lattice.backup/BackupTopologySnapshot.cs) (line 10).

`public sealed record BackupTopologySnapshot`

- `public BackupTopologySnapshot( int shardCount, int virtualShardCount, IReadOnlyList<string> shardRootDigests)`
- `public int ShardCount { get; init; }`
- `public int VirtualShardCount { get; init; }`
- `public IReadOnlyList<string> ShardRootDigests { get; init; }`

### `Orleans.Lattice.Backup.IBackupRestoreAdmission`

[Source](../../src/lattice.backup/IBackupRestoreAdmission.cs) (line 19).

`public interface IBackupRestoreAdmission`

- `long AdmittedCount { get; }`
- `long DeadLetteredCrossTenant { get; }`
- `long DeadLetteredOverQuota { get; }`
- `BackupRestoreRecordDisposition Admit(string key)`

### `Orleans.Lattice.Backup.IBackupSinkSharingProbe`

[Source](../../src/lattice.backup/IBackupSinkSharingProbe.cs) (line 31).

`public interface IBackupSinkSharingProbe`

- `BackupSinkSharingReport? LastReport { get; }`
- `Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupCaptureService`

[Source](../../src/lattice.backup/ILatticeBackupCaptureService.cs) (line 18).

`public interface ILatticeBackupCaptureService`

- `Task<LatticeBackupCaptureResult> CaptureAsync( LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)`
- `Task<LatticeBackupSetCaptureResult> CaptureSetAsync( LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupCatalogRebuildService`

[Source](../../src/lattice.backup/ILatticeBackupCatalogRebuildService.cs) (line 20).

`public interface ILatticeBackupCatalogRebuildService`

- `Task<BackupCatalogRebuildReport> RebuildFromSinkAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupCatalogScrubService`

[Source](../../src/lattice.backup/ILatticeBackupCatalogScrubService.cs) (line 20).

`public interface ILatticeBackupCatalogScrubService`

- `Task<BackupCatalogScrubReport> ScrubAsync( bool pruneOrphans = false, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupCatalogStore`

[Source](../../src/lattice.backup/ILatticeBackupCatalogStore.cs) (line 17).

`public interface ILatticeBackupCatalogStore`

- `Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)`
- `Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<BackupManifest> ListAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupColdRestoreService`

[Source](../../src/lattice.backup/ILatticeBackupColdRestoreService.cs) (line 25).

`public interface ILatticeBackupColdRestoreService`

- `Task<LatticeRestoreResult> ColdRestoreAsync( LatticeRestoreRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupHealthService`

[Source](../../src/lattice.backup/ILatticeBackupHealthService.cs) (line 11).

`public interface ILatticeBackupHealthService`

- `Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupHealthStore`

[Source](../../src/lattice.backup/ILatticeBackupHealthStore.cs) (line 12).

`public interface ILatticeBackupHealthStore`

- `Task SetReportAsync(BackupHealthReport report, CancellationToken cancellationToken = default)`
- `Task<BackupHealthReport?> GetReportAsync(string backupId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<BackupHealthReport> ListReportsAsync(CancellationToken cancellationToken = default)`
- `Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task SetConfigAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)`
- `Task<BackupHealthConfig?> GetConfigAsync(string backupId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupIncrementalCaptureService`

[Source](../../src/lattice.backup/ILatticeBackupIncrementalCaptureService.cs) (line 22).

`public interface ILatticeBackupIncrementalCaptureService`

- `Task<LatticeBackupCaptureResult> CaptureIncrementalAsync( LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupRestoreService`

[Source](../../src/lattice.backup/ILatticeBackupRestoreService.cs) (line 19).

`public interface ILatticeBackupRestoreService`

- `Task<LatticeRestoreResult> RestoreAsync( LatticeRestoreRequest request, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<LatticeRestoreResult>> RestoreSetAsync( string setId, CancellationToken cancellationToken = default)`
- `Task RevertRestoreAsync( LatticeRestoreResult restore, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupScheduler`

[Source](../../src/lattice.backup/ILatticeBackupScheduler.cs) (line 16).

`public interface ILatticeBackupScheduler`

- `Task<string?> TriggerFullBackupAsync(BackupScopeSelector scope)`
- `Task<string?> TriggerIncrementalBackupAsync(BackupScopeSelector scope)`
- `Task ScheduleRecurringBackupAsync(LatticeBackupScheduleRequest request)`
- `Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental)`
- `Task EnsureScheduleAsync(BackupScopeSelector scope)`
- `Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)`

### `Orleans.Lattice.Backup.ILatticeBackupSetResolver`

[Source](../../src/lattice.backup/ILatticeBackupSetResolver.cs) (line 19).

`public interface ILatticeBackupSetResolver`

- `Task<IReadOnlyList<BackupSetMember>> ResolveMembersAsync( string setId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupSink`

[Source](../../src/lattice.backup/ILatticeBackupSink.cs) (line 19).

`public interface ILatticeBackupSink`

- `bool IsDurable { get; }`
- `Task WriteArtifactAsync( string artifactId, IAsyncEnumerable<ReadOnlyMemory<byte>> content, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync( string artifactId, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default)`
- `Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default)`
- `Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default)`
- `Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeBackupTenantScope`

[Source](../../src/lattice.backup/ILatticeBackupTenantScope.cs) (line 29).

`public interface ILatticeBackupTenantScope`

- `bool IsActive { get; }`
- `void AuthorizeCapture(string treeId)`
- `void AuthorizeRestoreTarget(string treeId)`
- `ValueTask<IBackupRestoreAdmission> BeginRestoreAsync( string targetTreeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.ILatticeCoordinatedRestoreEngine`

[Source](../../src/lattice.backup/ILatticeCoordinatedRestoreEngine.cs) (line 20).

`public interface ILatticeCoordinatedRestoreEngine`

- `Task<RestoreAdmissionReport> ProbeAdmissionAsync( LatticeRestoreRequest request, CancellationToken cancellationToken = default)`
- `Task<LatticeRestoreResult> BuildShadowAsync( LatticeRestoreRequest request, CancellationToken cancellationToken = default)`
- `Task CommitShadowAsync( LatticeRestoreResult shadow, CancellationToken cancellationToken = default)`
- `Task DeleteShadowAsync( string shadowPhysicalTreeId, CancellationToken cancellationToken = default)`
- `string ResolveShadowTreeId(LatticeRestoreRequest request)`

### `Orleans.Lattice.Backup.IReplicatedTreeMembership`

[Source](../../src/lattice.backup/IReplicatedTreeMembership.cs) (line 18).

`public interface IReplicatedTreeMembership`

- `bool IsReplicated(string treeId)`
- `IReadOnlyCollection<string> ReplicatedTrees { get; }`

### `Orleans.Lattice.Backup.IRestoreSagaDispatcher`

[Source](../../src/lattice.backup/IRestoreSagaDispatcher.cs) (line 23).

`public interface IRestoreSagaDispatcher`

- `Task<LatticeRestoreResult?> TryDispatchAsync( LatticeRestoreRequest request, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync( string setId, LatticeRestoreMode mode, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Backup.LatticeBackupCaptureRequest`

[Source](../../src/lattice.backup/LatticeBackupCaptureRequest.cs) (line 12).

`public sealed record LatticeBackupCaptureRequest`

- `public const int DefaultPageSize`
- `public LatticeBackupCaptureRequest(string name, BackupScopeSelector scope, int pageSize = DefaultPageSize)`
- `public string Name { get; init; }`
- `public BackupScopeSelector Scope { get; init; }`
- `public int PageSize { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupCaptureResult`

[Source](../../src/lattice.backup/LatticeBackupCaptureResult.cs) (line 9).

`public sealed record LatticeBackupCaptureResult`

- `public LatticeBackupCaptureResult(string backupId, BackupManifest manifest)`
- `public string BackupId { get; init; }`
- `public BackupManifest Manifest { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupCrossTreeFenceException`

[Source](../../src/lattice.backup/LatticeBackupCrossTreeFenceException.cs) (line 14).

`public sealed class LatticeBackupCrossTreeFenceException : Exception`

- `public LatticeBackupCrossTreeFenceException(string message)`
- `public LatticeBackupCrossTreeFenceException(string message, Exception innerException)`

### `Orleans.Lattice.Backup.LatticeBackupHealthOptions`

[Source](../../src/lattice.backup/LatticeBackupHealthOptions.cs) (line 17).

`public sealed class LatticeBackupHealthOptions`

- `public static readonly TimeSpan MinimumInterval`
- `public static readonly TimeSpan DefaultSweepInterval`
- `public bool Enabled { get; set; }`
- `public TimeSpan DefaultInterval { get; set; }`

### `Orleans.Lattice.Backup.LatticeBackupIncrementalCaptureRequest`

[Source](../../src/lattice.backup/LatticeBackupIncrementalCaptureRequest.cs) (line 11).

`public sealed record LatticeBackupIncrementalCaptureRequest`

- `public LatticeBackupIncrementalCaptureRequest( string name, BackupScopeSelector scope, string baseBackupId, int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)`
- `public string Name { get; init; }`
- `public BackupScopeSelector Scope { get; init; }`
- `public string BaseBackupId { get; init; }`
- `public int PageSize { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupMetrics`

[Source](../../src/lattice.backup/LatticeBackupMetrics.cs) (line 33).

`public static class LatticeBackupMetrics`

- `public const string TagScope`
- `public const string TagPhase`
- `public const string TagReason`
- `public const string TagKind`
- `public const string PhaseSnapshotOpen`
- `public const string PhaseExport`
- `public const string PhaseSinkWrite`
- `public const string PhaseManifestCommit`
- `public const string PhaseRead`
- `public const string PhaseMerge`
- `public const string PhaseVerify`
- `public const string ReasonPermissionDenied`
- `public const string ReasonSaturation`
- `public const string ReasonSinkIoError`
- `public const string ReasonIntegrityMismatch`
- `public const string ReasonCancellation`
- `public const string ReasonUnknown`
- `public const string ReasonIncrementalFallback`
- `public static readonly Counter<long> Captures`
- `public static readonly Histogram<long> BackupBytes`
- `public static readonly Histogram<long> BackupArtifacts`
- `public static readonly Histogram<long> BackupEntries`
- `public static readonly Counter<long> EntriesProcessed`
- `public static readonly Counter<long> BytesProcessed`
- `public static readonly Counter<long> RetentionBytesReclaimed`
- `public static readonly Counter<long> RetentionPruned`
- `public static readonly Histogram<double> CaptureDuration`
- `public static readonly Histogram<double> RestoreDuration`
- `public static readonly Counter<long> RestoreEntriesApplied`
- `public static readonly Histogram<long> IncrementalLagEntries`
- `public static readonly Histogram<double> IncrementalLagAge`
- `public static readonly Counter<long> CaptureFailures`
- `public static readonly Counter<long> RestoreFailures`
- `public static readonly Counter<long> CaptureRetries`
- `public static readonly Counter<long> SchedulerSkipped`
- `public static readonly Counter<long> SchedulerOverruns`
- `public static readonly Counter<long> SchedulerFailures`
- `public static KeyValuePair<string, object?> KindTag(BackupKind kind)`
- `public static void RecordCaptureSuccess( BackupManifest manifest, double durationMs, long byteLength, int artifactCount, int entryCount)`
- `public static void RecordIncrementalLag(long deltaEntries, double baseCutAgeMs)`
- `public static void RecordRestoreSuccess(double durationMs, long entriesApplied)`
- `public static void RecordRetention(string scopeKey, long bytesReclaimed, int prunedCount)`
- `public static void RecordSchedulerSkipped(string scopeKey)`
- `public static void RecordSchedulerOverrun(string scopeKey)`
- `public static void RecordSchedulerFailure(string scopeKey, string reason)`
- `public static void RecordCaptureRetry(string reason)`
- `public static bool EmitCaptureFailure(BackupKind kind, string phase, Exception exception)`
- `public static bool EmitRestoreFailure(string phase, Exception exception)`
- `public static string MapReason(Exception exception)`

### `Orleans.Lattice.Backup.LatticeBackupOptions`

[Source](../../src/lattice.backup/LatticeBackupOptions.cs) (line 9).

`public sealed class LatticeBackupOptions`

- `public HistoryRetentionMode HistoryRetentionMode { get; set; }`
- `public TimeSpan? HistoryRetentionWindow { get; set; }`
- `public bool EnableDurableHistoryView { get; set; }`
- `public bool EnableBackupCatalogIndexView { get; set; }`
- `public TimeSpan CrossTreeFenceDrainTimeout { get; set; }`
- `public TimeSpan CrossTreeFencePollInterval { get; set; }`
- `public int MaxCrossTreeFenceAttempts { get; set; }`
- `public BackupSinkSharingEnforcement SinkSharingEnforcement { get; set; }`
- `public TimeSpan SinkSharingProbeTimeout { get; set; }`

### `Orleans.Lattice.Backup.LatticeBackupReservedTrees`

[Source](../../src/lattice.backup/LatticeBackupReservedTrees.cs) (line 11).

`public static class LatticeBackupReservedTrees`

- `public static string Prefix`
- `public static bool IsReserved(string treeId)`
- `public static void ThrowIfReserved(string treeId, string? paramName = null)`

### `Orleans.Lattice.Backup.LatticeBackupScheduleOptions`

[Source](../../src/lattice.backup/LatticeBackupScheduleOptions.cs) (line 22).

`public sealed class LatticeBackupScheduleOptions`

- `public static readonly TimeSpan MinimumInterval`
- `public static readonly TimeSpan DefaultFullBackupInterval`
- `public static readonly TimeSpan DefaultIncrementalBackupInterval`
- `public bool FullBackupScheduleEnabled { get; set; }`
- `public TimeSpan FullBackupInterval { get; set; }`
- `public bool IncrementalBackupScheduleEnabled { get; set; }`
- `public TimeSpan IncrementalBackupInterval { get; set; }`
- `public bool RetentionEnabled { get; set; }`
- `public int? RetentionKeepLast { get; set; }`
- `public TimeSpan? RetentionMaxAge { get; set; }`

### `Orleans.Lattice.Backup.LatticeBackupScheduleRequest`

[Source](../../src/lattice.backup/LatticeBackupScheduleRequest.cs) (line 12).

`public sealed record LatticeBackupScheduleRequest`

- `public LatticeBackupScheduleRequest(BackupScopeSelector scope, bool incremental, TimeSpan interval)`
- `public BackupScopeSelector Scope { get; init; }`
- `public bool Incremental { get; init; }`
- `public TimeSpan Interval { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupServiceCollectionExtensions`

[Source](../../src/lattice.backup/LatticeBackupServiceCollectionExtensions.cs) (line 13).

`public static class LatticeBackupServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeBackup( this ISiloBuilder builder, Action<LatticeBackupOptions>? configure = null)`
- `public static ISiloBuilder ConfigureLatticeBackup( this ISiloBuilder builder, Action<LatticeBackupOptions> configure)`
- `public static ISiloBuilder ConfigureLatticeBackupSchedule( this ISiloBuilder builder, Action<LatticeBackupScheduleOptions> configure)`
- `public static ISiloBuilder ConfigureLatticeBackupSchedule( this ISiloBuilder builder, string scopeKey, Action<LatticeBackupScheduleOptions> configure)`
- `public static ISiloBuilder ConfigureLatticeBackupHealth( this ISiloBuilder builder, Action<LatticeBackupHealthOptions> configure)`

### `Orleans.Lattice.Backup.LatticeBackupSetCaptureRequest`

[Source](../../src/lattice.backup/LatticeBackupSetCaptureRequest.cs) (line 15).

`public sealed record LatticeBackupSetCaptureRequest`

- `public LatticeBackupSetCaptureRequest( string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent = false, int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)`
- `public string Name { get; init; }`
- `public IReadOnlyList<BackupScopeSelector> Scopes { get; init; }`
- `public bool CrossTreeConsistent { get; init; }`
- `public int PageSize { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupSetCaptureResult`

[Source](../../src/lattice.backup/LatticeBackupSetCaptureResult.cs) (line 9).

`public sealed record LatticeBackupSetCaptureResult`

- `public LatticeBackupSetCaptureResult( BackupSetManifest setManifest, IReadOnlyList<LatticeBackupCaptureResult> members)`
- `public BackupSetManifest SetManifest { get; init; }`
- `public IReadOnlyList<LatticeBackupCaptureResult> Members { get; init; }`

### `Orleans.Lattice.Backup.LatticeBackupTenantIsolationException`

[Source](../../src/lattice.backup/LatticeBackupTenantIsolationException.cs) (line 17).

`public sealed class LatticeBackupTenantIsolationException : InvalidOperationException`

- `public LatticeBackupTenantIsolationException(string message)`
- `public LatticeBackupTenantIsolationException(string message, Exception innerException)`

### `Orleans.Lattice.Backup.LatticeRestoreMode`

[Source](../../src/lattice.backup/LatticeRestoreMode.cs) (line 6).

`public enum LatticeRestoreMode`

- `InPlace = 0`
- `ShadowCutover = 1`

### `Orleans.Lattice.Backup.LatticeRestoreRequest`

[Source](../../src/lattice.backup/LatticeRestoreRequest.cs) (line 13).

`public sealed record LatticeRestoreRequest`

- `public const int DefaultApplyBatchSize`
- `public LatticeRestoreRequest( string backupId, string? targetTreeId = null, BackupScopeSelector? scope = null, LatticeRestoreMode mode = LatticeRestoreMode.InPlace, string? operationId = null, int applyBatchSize = DefaultApplyBatchSize)`
- `public string BackupId { get; init; }`
- `public string? TargetTreeId { get; init; }`
- `public BackupScopeSelector? Scope { get; init; }`
- `public LatticeRestoreMode Mode { get; init; }`
- `public string? OperationId { get; init; }`
- `public int ApplyBatchSize { get; init; }`

### `Orleans.Lattice.Backup.LatticeRestoreResult`

[Source](../../src/lattice.backup/LatticeRestoreResult.cs) (line 11).

`public sealed record LatticeRestoreResult`

- `public LatticeRestoreResult( string backupId, string targetTreeId, LatticeRestoreMode mode, string operationId, IReadOnlyList<string> manifestChain, long entriesApplied, string? shadowPhysicalTreeId = null, string? previousPhysicalTreeId = null, long deadLetteredCrossTenant = 0, long deadLetteredOverQuota = 0)`
- `public string BackupId { get; init; }`
- `public string TargetTreeId { get; init; }`
- `public LatticeRestoreMode Mode { get; init; }`
- `public string OperationId { get; init; }`
- `public IReadOnlyList<string> ManifestChain { get; init; }`
- `public long EntriesApplied { get; init; }`
- `public string? ShadowPhysicalTreeId { get; init; }`
- `public string? PreviousPhysicalTreeId { get; init; }`
- `public long DeadLetteredCrossTenant { get; init; }`
- `public long DeadLetteredOverQuota { get; init; }`

### `Orleans.Lattice.Backup.LatticeRestoreValidationException`

[Source](../../src/lattice.backup/LatticeRestoreValidationException.cs) (line 12).

`public sealed class LatticeRestoreValidationException : InvalidOperationException`

- `public LatticeRestoreValidationException(string message)`
- `public LatticeRestoreValidationException(string message, Exception innerException)`

### `Orleans.Lattice.Backup.RestoreAdmissionReport`

[Source](../../src/lattice.backup/RestoreAdmissionReport.cs) (line 11).

`public sealed class RestoreAdmissionReport`

- `public RestoreAdmissionReport( string backupId, string targetTreeId, long totalByteLength, long totalChunkCount, int shardCount, IReadOnlyList<string> manifestChain)`
- `public string BackupId { get; }`
- `public string TargetTreeId { get; }`
- `public long TotalByteLength { get; }`
- `public long TotalChunkCount { get; }`
- `public int ShardCount { get; }`
- `public IReadOnlyList<string> ManifestChain { get; }`
