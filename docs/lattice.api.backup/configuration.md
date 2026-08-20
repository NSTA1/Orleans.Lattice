# Orleans.Lattice.Api.Backup configuration

The facade package has one public options type, `LatticeApiBackupOptions`, bound through `AddLatticeBackupApi(configure)` and resolvable via `IOptions<LatticeApiBackupOptions>`. The sibling gRPC package exposes `LatticeBackupApiGrpcOptions`, and the underlying backup engine exposes `LatticeBackupHealthOptions` for periodic health monitoring.

## `LatticeApiBackupOptions`

The read-bounding knobs the control facade honours for its paged, cursor-resumable catalog listing.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultListPageSize` | `int` | `100` | Page size used for a catalog listing when the request leaves its page size unset (`0` or negative). |
| `MaxListPageSize` | `int` | `1000` | The largest catalog listing page size honoured; larger requested page sizes are clamped down to it. |

Both bounds apply to the `BackupCatalogRequest.PageSize` a caller supplies: a value below 1 falls back to `DefaultListPageSize`, and a value above `MaxListPageSize` is clamped to it.

## `LatticeBackupHealthOptions`

Engine-level cluster-wide options for the periodic backup-health monitor. Configure them on the underlying `Orleans.Lattice.Backup` package.

| Member | Type | Default | Meaning |
|---|---|---|---|
| `MinimumInterval` | `TimeSpan` | `TimeSpan.FromMinutes(1)` | The smallest sweep cadence the monitor reminder honours. |
| `DefaultSweepInterval` | `TimeSpan` | `TimeSpan.FromHours(6)` | The default value used by `DefaultInterval`. |
| `Enabled` | `bool` | `true` | Whether the periodic monitor runs at all. A non-durable sink keeps the monitor inert even when this is `true`. |
| `DefaultInterval` | `TimeSpan` | `DefaultSweepInterval` (six hours) | Default catalog sweep cadence and default per-backup re-verification interval. Values below `MinimumInterval` are clamped up when the reminder is registered. |

## What is configured elsewhere

This facade drives the backup engine but does not re-expose all engine configuration. The engine's catalog-history, cross-tree-fence, scheduling, retention, and health-monitor behaviour is configured on [`Orleans.Lattice.Backup`](../lattice.backup/configuration.md); the sink backend is configured by whichever sink package is registered (for example [`Orleans.Lattice.Backup.AzureBlob`](../lattice.backup.azureblob/configuration.md)). Transport concerns - authorization, credentials, TLS, deadlines - live on the [gRPC binding](../lattice.api.backup.grpc/configuration.md), not here.
