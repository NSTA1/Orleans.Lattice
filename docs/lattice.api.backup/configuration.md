# Orleans.Lattice.Api.Backup configuration

The package has one public options type, `LatticeApiBackupOptions`, bound through `AddLatticeBackupApi(configure)` and resolvable via `IOptions<LatticeApiBackupOptions>`.

## `LatticeApiBackupOptions`

The read-bounding knobs the control facade honours for its paged, cursor-resumable catalog listing.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultListPageSize` | `int` | `100` | Page size used for a catalog listing when the request leaves its page size unset (`0` or negative). |
| `MaxListPageSize` | `int` | `1000` | The largest catalog listing page size honoured; larger requested page sizes are clamped down to it. |

Both bounds apply to the `BackupCatalogRequest.PageSize` a caller supplies: a value below 1 falls back to `DefaultListPageSize`, and a value above `MaxListPageSize` is clamped to it.

## What is configured elsewhere

This facade drives the backup engine but does not re-expose its configuration. The engine's catalog-history, cross-tree-fence, scheduling, and retention behaviour is configured on [`Orleans.Lattice.Backup`](../lattice.backup/configuration.md); the sink backend is configured by whichever sink package is registered (for example [`Orleans.Lattice.Backup.AzureBlob`](../lattice.backup.azureblob/configuration.md)). Transport concerns - authorization, credentials, TLS, deadlines - live on the [gRPC binding](../lattice.api.backup.grpc/configuration.md), not here.
