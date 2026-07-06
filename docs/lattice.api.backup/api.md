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
| List backups | takes a `BackupCatalogRequest` | `BackupCatalogPage` |
| Stream backups | streams | `IAsyncEnumerable<BackupManifest>` in backup-id order |
| Describe backup | takes a backup id | `BackupChainDescription?` (null when absent) |
| Delete backup | takes a backup id | `bool` (true when one was deleted) |
| Restore backup | takes a `LatticeRestoreRequest` | `LatticeRestoreResult` |
| Revert restore | takes a `LatticeRestoreResult` | (void) |
| Export artifact | takes a backup id and artifact id | `IAsyncEnumerable<ReadOnlyMemory<byte>>` |
| Get inventory | (none) | `BackupInventoryReport` |
| Get scope status | takes a `BackupScopeSelector` | `BackupScopeStatus?` (null when unknown) |

The request / result types prefixed `LatticeBackup*` / `LatticeRestore*` and `BackupManifest` / `BackupScopeSelector` are defined in [`Orleans.Lattice.Backup`](../lattice.backup/api.md); the package's own model records are documented below.

## Model records

### `BackupCatalogRequest`

Paging request for the catalog listing. The catalog is enumerated ascending by backup id.

- `int PageSize` - maximum manifests per page. Values below 1 fall back to `LatticeApiBackupOptions.DefaultListPageSize`; values above `MaxListPageSize` are clamped to it.
- `string? PageToken` - the exclusive continuation cursor (the backup id of the last manifest on the previous page). `null` (the default) starts from the beginning.

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

### `BackupScopeStatus`

A single scope's schedule and last-run status.

- Constructor: `BackupScopeStatus(BackupScopeSelector scope, bool fullScheduleRegistered, bool incrementalScheduleRegistered, DateTimeOffset? lastFullRunUtc, DateTimeOffset? lastFullSuccessUtc, DateTimeOffset? lastIncrementalRunUtc, DateTimeOffset? lastIncrementalSuccessUtc, BackupScopeRunOutcome lastRunOutcome, int chainDepth)`. Throws `ArgumentNullException` when `scope` is null.
- Properties: `BackupScopeSelector Scope`, `bool FullScheduleRegistered`, `bool IncrementalScheduleRegistered`, `DateTimeOffset? LastFullRunUtc`, `DateTimeOffset? LastFullSuccessUtc`, `DateTimeOffset? LastIncrementalRunUtc`, `DateTimeOffset? LastIncrementalSuccessUtc`, `BackupScopeRunOutcome LastRunOutcome`, `int ChainDepth`.

## Serialization aliases

### `ApiBackupTypeAliases`

A public static class holding the stable Orleans serialization alias constants for the package's model records. Referenced by the `[Alias(...)]` attributes on those records so the wire contract stays stable across renames; a consumer does not normally reference it directly.
