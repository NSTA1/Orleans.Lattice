# Orleans.Lattice.Api.Backup API reference

This package's own public surface is the registration extension and the options type. The control facade interface (`ILatticeBackupControl`), the model records it returns and accepts, and their `ApiBackupTypeAliases` constant class are published in the shared `Orleans.Lattice.Api.Abstractions` package under the same `Orleans.Lattice.Api.Backup` namespace; this package implements the interface and registers it. The facade interface is the contract the gRPC binding adapts over, and is described by its operations below and in [Architecture](architecture.md).

The model records are Orleans-serialized (`[GenerateSerializer]`, `[Immutable]`) with stable aliases held in the public `ApiBackupTypeAliases` constant class.

## Registration

### `LatticeApiBackupServiceCollectionExtensions`

Static extension method on `ISiloBuilder`.

- `ISiloBuilder AddLatticeBackupApi(this ISiloBuilder builder, Action<LatticeApiBackupOptions>? configure = null)`

  Adds the transport-agnostic backup / restore control facade: binds `LatticeApiBackupOptions`, registers the control-facade singleton every transport binding adapts over, and an idempotency marker. Adds no transport behaviour of its own. Must be called after `AddLatticeBackup(...)`; throws `InvalidOperationException` when called first. Throws `ArgumentNullException` when `builder` is null. Idempotent.

## Options

### `LatticeApiBackupOptions`

The read-bounding knobs the control facade honours for its paged catalog listing. See [Configuration](configuration.md) for defaults.

- `int DefaultListPageSize` - page size used when a listing request leaves its page size unset.
- `int MaxListPageSize` - the largest listing page size honoured; larger requests are clamped down.

## Facade operations

Every tree name these operations accept is a **tenant-local name**: the facade resolves it to its effective, tenant-scoped id through `ITenantContextResolver.ResolveEffectiveTreeIdAsync` at the entry point and uses that one id for **both** the authorization check and the operation, so a verb can never authorize one tree and act on another. This covers a `BackupScopeSelector`'s tree, a catalog listing's `TreeId` filter, and a restore's `TargetTreeId`. A `BackupId` is **not** composed - it is already a recorded, effective id, so scoping it again would double-scope it or re-attribute another tenant's backup to the caller. With the tenancy add-on absent - or registered, but with no active tenant asserted, which resolves the default tenant - the bare name is returned unchanged, so behaviour is byte-for-byte as before. Under an asserted active tenant an unqualified name is scoped into that tenant's `t/{tenant}/{name}` namespace, and an already-qualified `t/` id or a `_lattice_` system-tree name passes through unchanged (a well-formed foreign `t/{other}/{name}` is left to the tenancy access gate, which refuses it unless the owning tenant has issued a matching cross-tenant grant). The call fails closed with a `LatticeTenantAccessDeniedException` when the asserted tenant fails validation against the caller's own membership (an anonymous caller can never act as a tenant), or - outside a system-origin scope - when it names a `sys-` tree or a malformed `t/` id that belongs to no tenant. See [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md).

The control facade exposes these operations. The [gRPC binding](../lattice.api.backup.grpc/api.md) projects the remote-safe subset as RPCs; inventory, catalog rebuild / scrub, and cold restore are in-process-only today. Operations that touch backup data authorize their scope fail-closed before touching data; advisory capability and availability probes report state without mutating data.

| Operation | Shape | Returns |
|---|---|---|
| Create backup (`CreateBackupAsync`) | takes a `LatticeBackupCaptureRequest` | `LatticeBackupCaptureResult` |
| Create incremental backup (`CreateIncrementalBackupAsync`) | takes a `LatticeBackupIncrementalCaptureRequest` | `LatticeBackupCaptureResult` |
| Create backup set (`CreateBackupSetAsync`) | takes a `LatticeBackupSetCaptureRequest` | `LatticeBackupSetCaptureResult` |
| List backups (`ListBackupsAsync`) | takes a `BackupCatalogRequest` | `BackupCatalogPage` |
| Stream backups (`StreamBackupsAsync`) | streams | `IAsyncEnumerable<BackupManifest>` in backup-id order |
| Describe backup (`DescribeBackupAsync`) | takes a backup id | `BackupChainDescription?` (null when absent) |
| Delete backup (`DeleteBackupAsync`) | takes a backup id | `bool` (true when one was deleted) |
| Restore backup (`RestoreBackupAsync`) | takes a `LatticeRestoreRequest` | `LatticeRestoreResult` |
| Cold restore (`ColdRestoreAsync`) | takes a `LatticeRestoreRequest` | `LatticeRestoreResult` |
| Revert restore (`RevertRestoreAsync`) | takes a `LatticeRestoreResult` | (void) |
| Export artifact (`ExportArtifactAsync`) | takes a backup id and artifact id | `IAsyncEnumerable<ReadOnlyMemory<byte>>` |
| Get inventory (`GetInventoryAsync`) | (none) | `BackupInventoryReport` |
| Rebuild catalog from sink (`RebuildCatalogFromSinkAsync`) | (none) | `BackupCatalogRebuildReport` |
| Scrub catalog against sink (`ScrubCatalogAgainstSinkAsync`) | takes a `bool pruneOrphans` | `BackupCatalogScrubReport` |
| Is health monitoring available (`IsHealthMonitoringAvailableAsync`) | (none) | `bool` (true when the sink is durable) |
| Check backup health (`CheckBackupHealthAsync`) | takes a backup id | `BackupHealthReport` (verifies and persists) |
| Get backup health (`GetBackupHealthAsync`) | takes a backup id | `BackupHealthReport?` (last stored, null when none/absent) |
| Configure backup health (`ConfigureBackupHealthAsync`) | takes a backup id and a `BackupHealthConfig` | (void) |
| Get scope status (`GetScopeStatusAsync`) | takes a `BackupScopeSelector` | `BackupScopeStatus?` (null when unknown) |
| Probe capabilities (`ProbeCapabilitiesAsync`) | takes a `BackupScopeSelector` | `BackupScopeCapabilities` |
| Schedule backup (`ScheduleBackupAsync`) | takes a `LatticeBackupScheduleRequest` | (void) |
| Cancel schedule (`CancelScheduleAsync`) | takes a `BackupScopeSelector` and `bool incremental` | (void) |

Create backup set captures one full backup per distinct tree scope under a single set manifest, so an operator can back up several trees as one unit; it authorizes every member scope fail-closed before any capture, so a set that names one forbidden scope is rejected whole. When cross-tree consistency is requested the members share one consistency fence. The returned `BackupSetManifest.SetId` is the value each member's `BackupCatalogIndexRow.SetId` carries, so a consumer can group the `ListBackupsAsync` rows of one set by it; a **single-scope** capture stamps no membership and so reports `SetId` as `null`, matching the `null` its catalog row reports. The `LatticeBackupSetCaptureRequest` / `LatticeBackupSetCaptureResult` and `BackupSetManifest` types are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Schedule backup registers (or updates) a recurring backup of one scope: the scheduler grain persists the scope and registers an Orleans reminder that fires every interval, capturing a full or an incremental backup per the request. It authorizes the scope with the same grant as a capture, and clamps a sub-minimum interval up to the scheduler minimum. A runtime schedule registered this way overrides the startup-configured cadence for the chosen kind. Cancel schedule removes that runtime full or incremental schedule for the scope and is idempotent. The `LatticeBackupScheduleRequest` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Backup-set restore is exposed by the backup engine restore service, not by `ILatticeBackupControl` or the gRPC binding. Its public shape is `Task<IReadOnlyList<LatticeRestoreResult>> RestoreSetAsync(string setId, CancellationToken cancellationToken = default)`: it restores every member tree in the captured set as one unit, using shadow-cutover restores for the members and a coordinated all-or-nothing saga when replicated members require it. Only a set spanning two or more trees has a set id to pass here; a single-scope capture reports a `null` `SetId` and is restored as an ordinary backup with `RestoreAsync(backupId)`.

The request / result types prefixed `LatticeBackup*` / `LatticeRestore*` and `BackupManifest` / `BackupScopeSelector` are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md); the facade's own model records (shipped in `Orleans.Lattice.Api.Abstractions`) are documented below. `BackupManifest` includes `string? CapturingClusterId`, the id of the cluster that authored the capture and owns the WAL cursor lineage; legacy manifests can carry `null`, which readers treat as the local cluster.

Rebuild catalog from sink re-registers every self-describing manifest the durable sink holds into the reserved `sys-backup-catalog` tree, so the sink is the single source of truth and the catalog a rebuildable, self-healing projection over it. It is a high-privilege administrative action authorized fail-closed with the Restore (author / bulk-load) grant over the catalog tree, and is idempotent: a manifest already catalogued is reconciled in place (keeping its immutable capture timestamp) rather than duplicated, and a catalog missing rows the sink has is repopulated. It returns a `BackupCatalogRebuildReport` summarizing how many manifests were scanned, freshly added, and reconciled. The `BackupCatalogRebuildReport` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Scrub catalog against sink is the reconcile pass in the other direction: it enumerates every catalog row and probes the durable sink for its resolvability, reporting the orphans - catalog rows whose sink payload (manifest, or a referenced artifact) is gone, so the backup can no longer be resolved or restored. It shares the rebuild op's high-privilege, fail-closed Restore grant over the catalog tree. It is non-destructive by default: it only flags orphans and leaves the catalog untouched. Removal of orphan rows is an explicit opt-in (`pruneOrphans: true`), which deletes each orphan under system origin and is idempotent on re-run (a pruned orphan is no longer scanned). It returns a `BackupCatalogScrubReport` summarizing how many rows were scanned, how many were orphans, how many were removed, whether pruning ran, and the orphan backup ids. The `BackupCatalogScrubReport` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Cold restore is the disaster-recovery entry point: it restores a backup into a **fresh** cluster from the durable sink alone, with zero dependency on any surviving `sys-backup-catalog`. It bootstraps the reserved `sys-` trees if they are absent, resolves the target (tip) manifest from the durable sink alone (never the catalog), and hands the request to the HLC-preserving restore engine, whose `BaseBackupId` chain walk and artifact verification read catalog-first with a sink fallback - so on a cluster that lost its catalog the whole chain resolves from the sink; it then re-projects the catalog from the sink so the recovered cluster is left with a correct catalog. It is authorized fail-closed against the target scope's Restore grant - derived from the request's target tree or, when absent, the sink-held manifest - and, when it retargets the backup onto a different tree, the restore engine additionally requires the Backup grant over the tree the backup was captured from. It is idempotent. It reuses `LatticeRestoreRequest` / `LatticeRestoreResult` (defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md)) and throws `LatticeRestoreValidationException` when the backup is absent from the sink, the base chain is broken, or an artifact is missing or tampered.

The backup-health operations surface the periodic health monitor to an operator. Is health monitoring available reports whether the registered sink is durable (`ILatticeBackupSink.IsDurable`), so a UI can hide the health column when payload lives in the ephemeral in-cluster sink. It reflects the sink alone: `LatticeBackupHealthOptions.Enabled` is not consulted, so it reports `true` against a durable sink even when the periodic monitor is disabled, and on-demand checks still work there. Check backup health runs an on-demand verification of one backup - resolving its manifest, checking every referenced artifact's presence, and re-hashing each present artifact against its recorded digest - then persists and returns the `BackupHealthReport`; it authorizes the backup's scope fail-closed and throws `KeyNotFoundException` for an unknown backup id. Get backup health returns the last stored report for a backup (or `null` when none has been stored or the backup is absent) under the same read grant. Configure backup health stores a per-backup `BackupHealthConfig` (enable / disable plus interval) overriding the cluster default cadence; it authorizes fail-closed and throws `KeyNotFoundException` for an unknown backup id. The `BackupHealthReport` and `BackupHealthConfig` types are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

## Model records

### `BackupCatalogRequest`

Paging request for the catalog listing. By default the catalog is enumerated ascending by backup id.

- `int PageSize` - maximum manifests per page (in the newest-first mode, the maximum logical rows per page, where the adjacent members of one backup set count as a single row). Values below 1 fall back to `LatticeApiBackupOptions.DefaultListPageSize`; values above `MaxListPageSize` are clamped to it.
- `string? PageToken` - the exclusive continuation cursor. In the default order this is the backup id of the last manifest on the previous page; in the newest-first mode it is the opaque `BackupCatalogPage.NextPageToken`. `null` (the default) starts from the beginning.
- `bool OrderByCreatedDescending` - when set, returns the catalog newest-first (by capture time) with backup-set members kept adjacent, and enables the filter predicates below. In this mode an incremental chain is listed once, as its tip: a backup that another backup names as its base is folded out of the listing. This mode is served efficiently from a maintained backup-catalog index; when that index is not hosted it degrades to a full catalog scan with the same ordering, filtering, and cursor semantics. When `false` (the default) the listing keeps the ascending-by-backup-id order and ignores the filters.
- `BackupKind? Kind` - optional exact kind filter (full or incremental). Applied only in newest-first mode.
- `string? NamePrefix` - optional case-insensitive starts-with filter on the row's display name. Applied only in newest-first mode.
- `string? TreeId` - optional exact scope tree-id filter. Applied only in newest-first mode.
- `string? CreatedPrefix` - optional starts-with filter on the created timestamp rendered as the invariant UTC string `yyyy-MM-dd HH:mm:ss`. Applied only in newest-first mode.

### `BackupCatalogPage`

One page of the catalog.

- `IReadOnlyList<BackupManifest> Entries` - the manifests on this page (defaults to empty), in the request's order: ascending by backup id by default, or newest-first with backup-set members adjacent when `OrderByCreatedDescending` is set.
- `string? NextPageToken` - the cursor to pass back in the next request, or `null` on the final page.

### `BackupChainDescription`

A backup and its restore chain.

- Constructor: `BackupChainDescription(BackupManifest manifest, IReadOnlyList<string> chainBackupIds)`. Throws `ArgumentNullException` when either is null.
- `BackupManifest Manifest` - the described backup's manifest.
- `IReadOnlyList<string> ChainBackupIds` - the ordered ancestor chain (base first) needed to restore it, as far as the catalog holds it: the walk reads the catalog only and ends at the first ancestor it cannot find there.

### `BackupInventoryReport`

A catalog-wide inventory summary.

- Constructor: `BackupInventoryReport(long totalBackupCount, long totalCatalogBytes, long fullBackupCount, long incrementalBackupCount, DateTimeOffset? oldestBackupUtc, DateTimeOffset? newestBackupUtc, long captureFailureCount, long restoreFailureCount, long bytesReclaimed)`.
- Properties: `long TotalBackupCount`, `long TotalCatalogBytes`, `long FullBackupCount`, `long IncrementalBackupCount`, `DateTimeOffset? OldestBackupUtc`, `DateTimeOffset? NewestBackupUtc`, `long CaptureFailureCount`, `long RestoreFailureCount`, `long BytesReclaimed`.

The counts and byte totals are computed from the durable catalog (excluding manifests the caller may not read); the failure and bytes-reclaimed tallies are the process-lifetime figures from the in-memory metric registry of the silo that serves the call, and `BytesReclaimed` counts retention passes only, not a backup deleted through the facade.

### `BackupScopeCapabilities`

The allowed-operation set the read-only capability probe reports for one scope. Every flag is default-deny (`false` means "not known to be permitted"), and the flags are advisory: the server still authorizes each real operation fail-closed. The probe distinguishes the two authorization grants the access gate models - one covering list / read / capture / delete, the other covering restore - so the capture, incremental, list, and delete flags move together and the restore flag is separate.

- `required BackupScopeSelector Scope` - the probed scope.
- `bool CanList` - whether the caller may list / read / describe backups in the scope.
- `bool CanCapture` - whether the caller may capture a full backup of the scope.
- `bool CanCaptureIncremental` - whether the caller may capture an incremental backup of the scope.
- `bool CanRestore` - whether the caller may restore a backup into the scope.
- `bool CanDelete` - whether the caller may delete a backup in the scope.

### `BackupScopeStatus`

A single scope's schedule and last-run status.

- Constructor: `BackupScopeStatus(BackupScopeSelector scope, bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, int chainDepth, TimeSpan? runtimeFullBackupInterval = null, TimeSpan? runtimeIncrementalBackupInterval = null)`. Throws `ArgumentNullException` when `scope` is null.
- Properties: `BackupScopeSelector Scope`, `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`, `int ChainDepth`, `TimeSpan? RuntimeFullBackupInterval`, `TimeSpan? RuntimeIncrementalBackupInterval` (the runtime-registered full / incremental cadence, `null` when none is registered).

## Serialization aliases

### `ApiBackupTypeAliases`

A public static class holding the stable Orleans serialization alias constants for the facade's model records: `AliasPrefix` (`oib.`), `BackupCatalogRequest` (`oib.cr`), `BackupCatalogPage` (`oib.cp`), `BackupChainDescription` (`oib.cd`), `BackupInventoryReport` (`oib.ir`), `BackupScopeStatus` (`oib.st`), and `BackupScopeCapabilities` (`oib.ca`). Referenced by the `[Alias(...)]` attributes on those records so the wire contract stays stable across renames; a consumer does not normally reference it directly.
