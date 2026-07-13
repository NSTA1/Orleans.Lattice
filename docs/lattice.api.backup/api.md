# Orleans.Lattice.Api.Backup API reference

The public surface is the registration extension, the options type, and the model records the facade returns and accepts. The control facade interface itself is internal to the package - it is the contract the gRPC binding adapts over - and is described by its operations below and in [Architecture](architecture.md).

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

The control facade (internal) exposes these operations; each is projected as one RPC by the [gRPC binding](../lattice.api.backup.grpc/api.md). Every operation authorizes its scope fail-closed before touching data.

| Operation | Shape | Returns |
|---|---|---|
| Create backup | takes a `LatticeBackupCaptureRequest` | `LatticeBackupCaptureResult` |
| Create incremental backup | takes a `LatticeBackupIncrementalCaptureRequest` | `LatticeBackupCaptureResult` |
| Create backup set | takes a `LatticeBackupSetCaptureRequest` | `LatticeBackupSetCaptureResult` |
| List backups | takes a `BackupCatalogRequest` | `BackupCatalogPage` |
| Stream backups | streams | `IAsyncEnumerable<BackupManifest>` in backup-id order |
| Describe backup | takes a backup id | `BackupChainDescription?` (null when absent) |
| Delete backup | takes a backup id | `bool` (true when one was deleted) |
| Restore backup | takes a `LatticeRestoreRequest` | `LatticeRestoreResult` |
| Revert restore | takes a `LatticeRestoreResult` | (void) |
| Export artifact | takes a backup id and artifact id | `IAsyncEnumerable<ReadOnlyMemory<byte>>` |
| Get inventory | (none) | `BackupInventoryReport` |
| Rebuild catalog from sink | (none) | `BackupCatalogRebuildReport` |
| Scrub catalog against sink | takes a `bool pruneOrphans` | `BackupCatalogScrubReport` |
| Get scope status | takes a `BackupScopeSelector` | `BackupScopeStatus?` (null when unknown) |
| Probe capabilities | takes a `BackupScopeSelector` | `BackupScopeCapabilities` |
| Schedule backup | takes a `LatticeBackupScheduleRequest` | (void) |

Create backup set captures one full backup per distinct tree scope under a single set manifest, so an operator can back up several trees as one unit; it authorizes every member scope fail-closed before any capture, so a set that names one forbidden scope is rejected whole. When cross-tree consistency is requested the members share one consistency fence. The `LatticeBackupSetCaptureRequest` / `LatticeBackupSetCaptureResult` and `BackupSetManifest` types are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Schedule backup registers (or updates) a recurring backup of one scope: the scheduler grain persists the scope and registers an Orleans reminder that fires every interval, capturing a full or an incremental backup per the request. It authorizes the scope with the same grant as a capture, and clamps a sub-minimum interval up to the scheduler minimum. A runtime schedule registered this way overrides the startup-configured cadence for the chosen kind. The `LatticeBackupScheduleRequest` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

The request / result types prefixed `LatticeBackup*` / `LatticeRestore*` and `BackupManifest` / `BackupScopeSelector` are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md); the package's own model records are documented below.

Rebuild catalog from sink re-registers every self-describing manifest the durable sink holds into the reserved `sys-backup-catalog` tree, so the sink is the single source of truth and the catalog a rebuildable, self-healing projection over it. It is a high-privilege administrative action authorized fail-closed with the Restore (author / bulk-load) grant over the catalog tree, and is idempotent: a manifest already catalogued is reconciled in place (keeping its immutable capture timestamp) rather than duplicated, and a catalog missing rows the sink has is repopulated. It returns a `BackupCatalogRebuildReport` summarizing how many manifests were scanned, freshly added, and reconciled. The `BackupCatalogRebuildReport` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

Scrub catalog against sink is the reconcile pass in the other direction: it enumerates every catalog row and probes the durable sink for its resolvability, reporting the orphans - catalog rows whose sink payload (manifest, or a referenced artifact) is gone, so the backup can no longer be resolved or restored. It shares the rebuild op's high-privilege, fail-closed Restore grant over the catalog tree. It is non-destructive by default: it only flags orphans and leaves the catalog untouched. Removal of orphan rows is an explicit opt-in (`pruneOrphans: true`), which deletes each orphan under system origin and is idempotent on re-run (a pruned orphan is no longer scanned). It returns a `BackupCatalogScrubReport` summarizing how many rows were scanned, how many were orphans, how many were removed, whether pruning ran, and the orphan backup ids. The `BackupCatalogScrubReport` type is defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md).

## Model records

### `BackupCatalogRequest`

Paging request for the catalog listing. By default the catalog is enumerated ascending by backup id.

- `int PageSize` - maximum manifests per page. Values below 1 fall back to `LatticeApiBackupOptions.DefaultListPageSize`; values above `MaxListPageSize` are clamped to it.
- `string? PageToken` - the exclusive continuation cursor. In the default order this is the backup id of the last manifest on the previous page; in the newest-first mode it is the opaque `BackupCatalogPage.NextPageToken`. `null` (the default) starts from the beginning.
- `bool OrderByCreatedDescending` - when set, returns the catalog newest-first (by capture time) with backup-set members kept adjacent, and enables the filter predicates below. This mode is served efficiently from a maintained backup-catalog index. When `false` (the default) the listing keeps the ascending-by-backup-id order and ignores the filters.
- `BackupKind? Kind` - optional exact kind filter (full or incremental). Applied only in newest-first mode.
- `string? NamePrefix` - optional case-insensitive starts-with filter on the row's display name. Applied only in newest-first mode.
- `string? TreeId` - optional exact scope tree-id filter. Applied only in newest-first mode.
- `string? CreatedPrefix` - optional starts-with filter on the created timestamp rendered as the invariant UTC string `yyyy-MM-dd HH:mm:ss`. Applied only in newest-first mode.

### `BackupCatalogPage`

One page of the catalog.

- `IReadOnlyList<BackupManifest> Entries` - the manifests on this page, ordered by backup id (defaults to empty).
- `string? NextPageToken` - the cursor to pass back in the next request, or `null` on the final page.

### `BackupChainDescription`

A backup and its restore chain.

- Constructor: `BackupChainDescription(BackupManifest manifest, IReadOnlyList<string> chainBackupIds)`. Throws `ArgumentNullException` when either is null.
- `BackupManifest Manifest` - the described backup's manifest.
- `IReadOnlyList<string> ChainBackupIds` - the ordered ancestor chain (base first) needed to restore it.

### `BackupInventoryReport`

A catalog-wide inventory summary.

- Constructor: `BackupInventoryReport(long totalBackupCount, long totalCatalogBytes, long fullBackupCount, long incrementalBackupCount, DateTimeOffset? oldestBackupUtc, DateTimeOffset? newestBackupUtc, long captureFailureCount, long restoreFailureCount, long bytesReclaimed)`.
- Properties: `long TotalBackupCount`, `long TotalCatalogBytes`, `long FullBackupCount`, `long IncrementalBackupCount`, `DateTimeOffset? OldestBackupUtc`, `DateTimeOffset? NewestBackupUtc`, `long CaptureFailureCount`, `long RestoreFailureCount`, `long BytesReclaimed`.

The counts and byte totals are computed from the durable catalog (excluding manifests the caller may not read); the failure and bytes-reclaimed tallies are the process-lifetime figures from the in-memory metric registry.

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

- Constructor: `BackupScopeStatus(BackupScopeSelector scope, bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, int chainDepth)`. Throws `ArgumentNullException` when `scope` is null.
- Properties: `BackupScopeSelector Scope`, `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`, `int ChainDepth`.

## Serialization aliases

### `ApiBackupTypeAliases`

A public static class holding the stable Orleans serialization alias constants for the package's model records. Referenced by the `[Alias(...)]` attributes on those records so the wire contract stays stable across renames; a consumer does not normally reference it directly.
