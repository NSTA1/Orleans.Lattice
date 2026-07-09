# Orleans.Lattice.Backup

Optional, opt-in **causally-consistent backup and restore** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Captures a named,
point-in-time backup of a selected scope of a lattice tree and restores it later
without losing a single bit of causal metadata - **zero runtime cost until
`AddLatticeBackup` is registered**.

## Design

`AddLatticeBackup()` installs the storage and engine surface on top of the core
snapshot, write-ahead-log, and merge machinery:

- **Full capture** - a scoped snapshot exported through the core
  zero-observable-writes cursor, with a self-describing manifest that records the
  consistency cut, shard topology, per-key shape and merge-mode map, per-origin
  provenance high-water marks, and an optional compression-dictionary reference.
- **Incremental capture** - a forward write-ahead-log differential layered on a
  base backup, resuming from the base's per-partition WAL offsets and falling
  back to a full capture when the resume point has been trimmed or a range delete
  surfaces in the delta window.
- **Restore** - a mode-faithful replay that reinstalls every entry's
  hybrid-logical-clock, version vector, origin cluster id, expiry, and tombstone
  flag exactly as captured, either in place (bulk-load or last-writer-wins merge)
  or via an atomic shadow-cutover to a fresh tree.
- **Scheduling and retention** - opt-in recurring full and incremental schedules
  per scope, and a chain-aware retention policy that never prunes the base chain
  of a retained increment.

Every artifact is content-addressed (SHA-256), so a retried capture that produces
identical bytes is stored once and a restore is idempotent under retry. Backups
are written to a pluggable `ILatticeBackupSink`, defaulting to an in-cluster
dogfooded tree, with a durable
[Azure Blob Storage sink](https://www.nuget.org/packages/Orleans.Lattice.Backup.AzureBlob)
shipped as a sibling package.

## Security

Every capture, restore, list, describe, and delete authorizes its scope through
the same access gate the data path uses, against a dedicated `Backup` (capture)
or `Restore` (author / bulk-load) capability, before touching data. The catalog
and store live in reserved `sys-backup-*` trees that inherit the core `sys-`
catalog-hiding filter.

## Registration

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeBackup();
```

Must be registered after `AddLattice(...)`. For a remotely-drivable control
plane, add the
[`Orleans.Lattice.Api.Backup`](https://www.nuget.org/packages/Orleans.Lattice.Api.Backup)
facade and its
[gRPC binding](https://www.nuget.org/packages/Orleans.Lattice.Api.Backup.Grpc).

See the
[Backup documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.backup/README.md)
for the full guide.
