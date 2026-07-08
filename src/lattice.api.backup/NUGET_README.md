# Orleans.Lattice.Api.Backup

Optional, opt-in **backup / restore control facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Exposes a single
transport-agnostic admin surface that drives the
[`Orleans.Lattice.Backup`](https://www.nuget.org/packages/Orleans.Lattice.Backup)
engine - capture, incremental, list, stream, describe, delete, restore, revert,
artifact export, inventory, and scope status - from one place. A sibling package
projects this facade onto a code-first gRPC surface.

## Design

The facade mirrors the read-only `Orleans.Lattice.Api.State` and the read-write
`Orleans.Lattice.Api.Data` data-plane facades: the facade is the contract,
transports bind over it, and it costs nothing until it is registered.

- **Bounded-memory enumeration.** Catalog listing is cursor-resumable and
  page-bounded; whole-catalog draining and artifact export are streamed, so a
  large catalog or artifact enumerates with bounded memory.
- **Safe deletion.** Deleting a backup removes its manifest and only the
  artifacts it owns that no other retained manifest still references, so a shared
  base artifact is never orphaned out from under a retained increment.

## Security

Every operation authorizes its scope through the same backup access gate the
engine uses, before touching data. A capture / incremental / restore authorizes
its target scope; a list / describe / delete authorizes the scope carried by each
manifest, and a manifest whose scope the caller may not read is hidden from list
and inventory results.

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
