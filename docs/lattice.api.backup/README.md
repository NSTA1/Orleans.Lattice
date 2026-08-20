# Orleans.Lattice.Api.Backup

A transport-agnostic backup / restore control facade for [Orleans.Lattice.Backup](../lattice.backup/README.md).

## What is it?

`Orleans.Lattice.Api.Backup` is the **control plane** of a cluster's backup system. The [`Orleans.Lattice.Backup`](../lattice.backup/README.md) package adds the capture, restore, catalog, sink, and authorization engine reached through .NET service interfaces; this package adds the administrative surface an operator dashboard, a CLI, or an internal admin service needs to drive backup and restore, list and describe the catalog, delete backups safely, and inspect scope status - over a single surface with no wire dependency.

It is built the same way as the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and read-write [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) data-plane facades:

- **A transport-agnostic facade.** A single control surface (`ILatticeBackupControl`, a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) exposes capture, incremental, backup-set capture, list, stream, describe, delete, restore, revert, artifact export, inventory, catalog repair, scheduling, health, capability, and scope-status operations over plain request / response records. It has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding** (the sibling [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) package) that projects this facade onto a remotely callable service and typed client. This package ships no transport of its own; it is the contract every binding adapts over.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeBackupApi()` on the silo, and once added the facade does no background work until a method is called.
- **Fail-closed by construction.** Operations that touch backup data authorize their scope through the same backup access gate the engine uses before touching data. A capture / incremental / restore authorizes its target scope; a list / describe / delete authorizes the scope carried by each manifest, and a manifest whose scope the caller may not read is hidden from list and inventory results. Capability and availability probes are advisory and do not mutate data.
- **Bounded-memory enumeration.** Catalog listing is cursor-resumable and page-bounded; whole-catalog draining and artifact export are streamed, so a large catalog or artifact enumerates with bounded memory rather than being materialized whole.
- **Safe deletion.** Deleting a backup removes its manifest and only the artifacts it owns that no other retained manifest still references, so a shared base artifact is never orphaned out from under a retained increment.
- **Read-only capability probe.** A caller can ask, with no side effects, which backup and restore operations it may perform over a given scope. The probe runs the same fail-closed access gate every operation uses and reports the result as an allowed-operation set, so a UI can grey out actions the caller cannot perform without ever mutating state. The probe is advisory only: it never replaces the per-operation authorization each real call still performs.

## Ordering

`AddLatticeBackupApi()` must be called **after** `AddLatticeBackup(...)`: the backup engine is the source of truth for the capture, restore, catalog, sink, and authorization seams this facade drives. Calling it first fails fast at registration with an actionable message.

## Surface

The facade operations. The gRPC binding exposes the remote-safe subset as RPCs; inventory, catalog rebuild / scrub, and cold restore are in-process-only today.

| Operation | Purpose |
|---|---|
| Create backup | Capture a full backup of a scope. |
| Create incremental backup | Capture an incremental layered on a base backup. |
| Create backup set | Capture one full backup per scope under a shared set manifest. |
| List backups | One deterministic, cursor-resumable, read-filtered catalog page. |
| Stream backups | Drain the whole readable catalog with bounded memory. |
| Describe backup | A manifest and its base-first restore chain, or absent. |
| Delete backup | Remove a manifest and its unshared artifacts. |
| Restore backup | Restore a backup into its target tree. |
| Cold restore | Restore a backup into a fresh cluster from the durable sink alone (in-process only). |
| Revert restore | Undo a shadow-cutover restore. |
| Export artifact | Stream one of a backup's artifacts back chunk-wise. |
| Get inventory | A catalog-wide inventory summary of every readable backup (in-process only). |
| Rebuild catalog from sink | Re-project every sink manifest into the in-cluster catalog (in-process only). |
| Scrub catalog against sink | Report or prune catalog rows whose sink payload is gone (in-process only). |
| Get scope status | A single scope's schedule and last-run status. |
| Probe capabilities | Report, with no side effects, which backup and restore operations the caller may perform over a scope. |
| Schedule backup | Register or update a runtime recurring full or incremental backup schedule. |
| Cancel schedule | Remove a runtime full or incremental backup schedule. |
| Is health monitoring available | Report whether backup-health monitoring applies for the configured sink. |
| Check backup health | Verify one backup now and persist the report. |
| Get backup health | Read the latest stored health report. |
| Configure backup health | Override one backup's periodic health-monitor settings. |

## Reference

- [API reference](api.md) - the public options and model types, and the facade operations by name.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - how the facade authorizes, walks chains, deletes safely, and pages.

## See also

- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the capture, restore, catalog, sink, and authorization engine this facade drives.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the code-first gRPC binding and typed client.
- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) - the data-plane facades this control facade is modelled on.
