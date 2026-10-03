# Orleans.Lattice.Api.Backup

Optional, opt-in **backup / restore control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Exposes a single
transport-agnostic admin surface that drives the
[`Orleans.Lattice.Backup`](https://www.nuget.org/packages/Orleans.Lattice.Backup)
engine - capture, incremental, backup-set capture, list, stream, describe,
delete, restore, cold restore, revert, artifact export, inventory, catalog
rebuild / scrub, scheduling, backup health, capability probe, and scope status -
from one place. A sibling package projects this facade onto a code-first gRPC
surface.

## Design

The facade mirrors the read-only `Orleans.Lattice.Api.State` and the read-write
`Orleans.Lattice.Api.Data` data-plane facades: the facade is the contract,
transports bind over it, and it costs nothing until it is registered.

- **Bounded-memory enumeration.** Catalog listing is cursor-resumable and
  page-bounded; whole-catalog draining and artifact export are streamed, so a
  large catalog or artifact enumerates with bounded memory.
- **Safe deletion, by artifact.** Deleting a backup removes its manifest and only
  the artifacts it owns that no other retained manifest still references. The
  check is by artifact id, not by chain: an increment references its base by
  `BaseBackupId`, so deleting a base that a retained increment is layered on
  breaks that increment's restore chain - delete an incremental chain tip-first.
- **Accept-then-poll operations.** `ILatticeBackupOperations` starts backup and
  restore work, returns a handle, and lets callers poll progress or cancel. The
  older blocking verbs are deprecated and will be removed in the next major
  version; migrate at
  https://nsta1.github.io/Orleans.Lattice/docs/lattice.api.backup/operations.html#migrating-from-the-blocking-verbs.

## Security

Every operation that touches backup data authorizes its scope through the same
backup access gate the engine uses, before touching data (the capability probe
reports each grant as an allowed / denied flag, and the health-monitoring
availability flag checks no grant). A
capture / incremental / restore authorizes its target scope (a prefix scope is a
range over every key under that prefix, so a grant on the prefix string alone is
not enough, and any deny below the prefix refuses it; a restore whose
target cannot be resolved authorizes the reserved backup catalog tree instead,
so the check is never skipped); a list / describe /
delete authorizes the scope carried by each manifest, and a manifest whose scope
the caller may not read is hidden from list and inventory results. A listing
with `BackupCatalogRequest.ActiveTenantOnly` set is further narrowed to the
backups of the caller's active tenant's own trees, taken from the caller's
validated assertion and never from the request, and names that tenant in
`BackupCatalogPage.Tenant`.

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeBackupApi()`, and once added the facade does no background work until
  a method is called.
- **Must be registered after `AddLatticeBackup(...)`.** The call fails fast with
  an actionable message otherwise.

## Usage

```csharp
siloBuilder
    .AddLattice(/* ... */)
    .AddLatticeBackup()
    .AddLatticeBackupApi();
```

Bind a transport over the facade to drive backup and restore remotely: the
sibling
[`Orleans.Lattice.Api.Backup.Grpc`](https://www.nuget.org/packages/Orleans.Lattice.Api.Backup.Grpc)
package projects it onto a code-first gRPC surface.

See the
[Backup API documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.backup/README.md)
for the full guide.
