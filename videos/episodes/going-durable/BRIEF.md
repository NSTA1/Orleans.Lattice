# Going durable - brief

- **Path:** Build, sixth episode (B6), after TTL and soft delete.
- **Audience:** a developer whose silo has so far run on the in-memory defaults, and who now wants a tree to survive a restart.
- **Length:** two to five minutes, with the preceding episodes' reduced narration pace.
- **The one idea:** the registration code only: durable grain storage and a durable write-ahead log in place of the in-memory defaults, on the file or Azure Table backend.
- **It introduces:** [Configuration](../../../docs/lattice/configuration.md), [WAL storage providers](../../../docs/lattice/wal-storage-providers.md), [File WAL](../../../docs/lattice.storage.file/README.md) and [Azure Table storage](../../../docs/lattice.storage.azuretable/README.md).
- **Afterwards the viewer can** replace both in-memory defaults on a silo: register a durable grain storage provider that enforces ETags through `AddLattice`, and a durable write-ahead log on local disk or in Azure Table storage, knowing that one without the other is not a durable tree.

## Beats

1. Recap that TTL and soft delete showed how data goes away on purpose; this episode stops it going away when a silo restarts.
2. The registration so far keeps both of a tree's storage surfaces in memory: grain storage, which holds leaves and snapshots, and the write-ahead log, which holds every committed write. Neither survives a restart.
3. Durable grain storage: the callback passed to `AddLattice` registers it, so swap the memory provider for a durable Orleans provider. It must enforce ETags, and each silo checks that at start-up; by default a provider that accepts a stale write fails silo start.
4. A durable write-ahead log on local disk: the file provider, one call and a root directory, for a single node or a container. Each batch is all or nothing.
5. Or in Azure Table storage, for production deployments replicated across regions. Either call replaces the in-memory log, before or after `AddLattice`.
6. The log is only half of a durable tree: it is trimmed once a snapshot in grain storage covers it, so pair it with durable grain storage.
7. End at Moving in from another store.

## Code on screen

- One compiled registration of the in-memory defaults.
- One compiled registration of Azure Table grain storage through `AddLattice`.
- One compiled file write-ahead log registration.
- One compiled Azure Table write-ahead log registration.

## Sources

- [Configuration](../../../docs/lattice/configuration.md): "Registering Lattice" and "The grain storage provider must enforce ETags".
- [WAL storage providers](../../../docs/lattice/wal-storage-providers.md): the opening, "Registering a provider" and "Provider catalogue".
- [File WAL](../../../docs/lattice.storage.file/README.md): "What is it?" and "Quick Start".
- [Azure Table storage](../../../docs/lattice.storage.azuretable/README.md): "What is it?" and "Quick Start", which ends on "The WAL is only half of a durable tree".
- [API reference](../../../docs/lattice/api.md): the Azure Table grain storage registration.
- [Series plan](../../series.md): B6's idea, introduced pages, and ending printed by `npm run series -- ending B6`.

## Not in this episode

- What durability guarantees, where the durability boundary lies, and how the backend choice changes it: that is Operate's first episode.
- `LatticeOptions`, global defaults and per-tree overrides.
- Fencing modes other than the default, the probe's mechanics, or why ETags matter internally.
- File WAL framing, compaction and recovery; Azure Table row layout, compression and authentication modes beyond a connection string.
- Multi-account placement, WAL moves, custom providers, or performance figures.
