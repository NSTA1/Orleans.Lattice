# Orleans.Lattice.Api.Backup architecture

This page describes how the control facade drives the backup engine. The facade (`ILatticeBackupControl`, a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) is a single silo singleton that every transport binding adapts over, so it is described here by behaviour. The public model records it returns and accepts are named.

## Position in the stack

```
transport binding (gRPC now, other bindings later)
        |
        v
control facade  (this package - transport-agnostic)
        |
        v
backup engine   (Orleans.Lattice.Backup - capture / restore / catalog / sink / authorization)
        |
        v
core data plane (Orleans.Lattice)
```

The facade adds no transport of its own and no bespoke, un-authorized write path. It composes the engine's public service seams - the capture, incremental, restore, catalog, sink, and authorization surfaces - into the operations a control consumer needs, and enforces the read-bounding and safe-deletion policy that a raw engine caller would otherwise have to implement itself. Registering it (`AddLatticeBackupApi`) requires the engine to be registered first; the ordering guard fails fast at registration otherwise.

## Fail-closed authorization on every operation

Every facade operation authorizes before it touches data, through the same backup access gate the engine uses:

- A **capture**, **incremental**, **backup-set capture** (every member scope), **schedule**, or **cancel schedule** authorizes the request's target scope with the backup (capture / read) grant; a **restore**, **cold restore**, or **revert** authorizes the target scope with the restore grant (when a restore or cold restore retargets a backup onto a different tree, the backup engine additionally requires the backup grant over the tree the backup was captured from).
- A **describe**, **delete**, **export**, or **health check / read / configure** authorizes the scope carried by the target manifest.
- A **scope status** read authorizes the scope's read grant.
- A **catalog rebuild** or **catalog scrub** authorizes the restore grant over the reserved backup catalog tree, because it acts on the rows of every scope.
- **List**, **stream**, and **inventory** authorize per manifest and silently exclude any manifest whose scope the caller may not read, so existence of a backup a caller cannot read is never leaked through a count, a page, or the inventory totals.

Because the gate is the same one the data path consults, the facade inherits the engine's fail-closed posture, the zero-cost short-circuit when no authorization add-on is registered, and the bootstrap-administrator break-glass, with no bespoke authorization logic of its own.

## Deterministic, bounded-memory enumeration

By default the catalog is enumerated in a stable order (ascending by backup id); a listing request can instead ask for the filtered, newest-first order (`BackupCatalogRequest.OrderByCreatedDescending` - see the [API reference](api.md)). Two enumeration shapes are offered:

- **Paged listing** returns one `BackupCatalogPage` at a time. The request's page size is bounded by the facade's `DefaultListPageSize` / `MaxListPageSize` options, and the page's `NextPageToken` is the exclusive cursor to pass back for the next page (the last backup id on the page in the default order; an opaque index cursor in the newest-first order). Paging is resumable and stateless on the server.
- **Whole-catalog streaming** drains every readable manifest as an async stream in backup-id order, for a consumer that wants the whole catalog without managing a cursor.

Both shapes also skip a catalog row whose manifest the sink no longer holds, so an unresolvable backup is never offered as a restore point.

Artifact export is likewise streamed chunk-wise, so exporting a large artifact - like listing a large catalog - runs with bounded memory rather than materializing the payload whole.

## Chain walking

`Describe` returns a manifest together with its base-first restore chain: for a full backup the chain is the backup itself; for an increment it is the base backup followed by the ordered increments up to that point. The chain is walked through the catalog alone and ends silently at the first ancestor the catalog does not hold. With a complete catalog it is exactly the set of manifests a restore of that backup would read, so a consumer can present or validate a restore before running it; a restore instead falls back to the sink for a missing ancestor, and fails with `LatticeRestoreValidationException` when neither holds it. `Describe` returns absent (null) when no backup with the id exists; when it finds the manifest it authorizes that manifest's scope before walking the chain.

## Safe deletion

Deleting a backup removes its manifest from both the catalog and the sink, and deletes only the artifacts it owns that are **not** shared with any other retained manifest. Before deleting, the facade collects every artifact id still referenced by the content descriptors of any other retained manifest and deletes only this backup's artifacts outside that set, so an artifact another retained manifest points at is never deleted out from under it. Deletion authorizes the backup's scope first, discards the deleted backup's stored health state, and reports whether a backup was actually removed.

The check is by artifact id, not by chain. An increment references its base by `BaseBackupId` and records only its own delta artifact, so deleting a base backup that a retained increment is layered on removes the base's manifest and artifacts and breaks that increment's restore chain. Unlike the retention pass, which always preserves the base chain of a retained increment, deletion does not consult `BaseBackupId`.

## Inventory

`GetInventory` combines two sources: the durable catalog (absolute counts, byte totals, per-kind counts, and oldest / newest timestamps, computed only over manifests the caller may read) and the in-memory metric registry of the silo that serves the call (the process-lifetime capture-failure, restore-failure, and retention bytes-reclaimed tallies). The result is a `BackupInventoryReport` - a point-in-time summary suitable for a dashboard header or a health probe.

## Scope status

`GetScopeStatus` reports a single scope's schedule registration and last-run health - whether full and incremental schedules are registered, the last run and last success timestamps for each, the last-run outcome, the current chain depth, and any runtime-registered full / incremental schedule interval - or absent when the scope has no registered schedule, no runtime schedule interval, no recorded run, and no catalogued backup. It authorizes the scope's read grant before returning anything.
