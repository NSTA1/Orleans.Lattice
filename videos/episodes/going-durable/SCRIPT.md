# Going durable - script

Status: fact-checked by the Docs agent against the corpus, cue by cue. Eight of ten cues were accurate as written; two were corrected: the file provider now "suits a single container with a mounted volume" (the corpus says single-container, not single node), and the closing pairing advice now reads "durable grain storage too, not the memory provider" rather than an absolute ban. The fencing default it states (Reject: a provider that accepts a stale write stops the silo) matches configuration.md and source; wal-storage-providers.md still says Warn and is stale.

Written form, in the series voice (Emma, British English). The code is on
screen, so the narration explains the change rather than reading method names.
This episode keeps the preceding episodes' reduced pace of 0.92.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds silence before the next cue or after the
last one.

## Narration

<!-- pace 0.92 -->

### Going durable

TTL and soft delete showed how data goes away when you ask it to. Here, we stop it going away when a silo restarts.

This is Going durable: durable grain storage and a durable write-ahead log, in place of the in-memory defaults.

### Two halves, both in memory

Every episode so far has run on the defaults, and the defaults keep everything in memory.

A tree lives in two places. Grain storage holds its leaves and their snapshots, and the write-ahead log holds every committed write, in order. In memory, neither survives a restart, so going durable means replacing both.

### Durable grain storage

Start with grain storage. The callback you pass when you register Lattice chooses it, so swap the memory provider for a durable Orleans provider, here Azure Table storage.

That provider must enforce ETags, so that a write presenting a stale one fails. Orleans' own durable providers do, and each silo checks as it starts: by default, a provider that accepts a stale write stops the silo from starting.

### A durable log on disk

Then the write-ahead log. With no cloud dependency, the file provider keeps it on local disk: one call, and a root directory.

It suits a single container with a mounted volume. Each batch is all or nothing, so a crash before a batch is committed rolls it back.

### Or in Azure Table storage

For production deployments replicated across regions, the Azure Table provider keeps the log in a storage table instead. Either call replaces the in-memory log, whether it comes before or after Lattice is registered.

Either way, the log is only half of a durable tree. It is trimmed once a snapshot in grain storage covers it, so pair it with durable grain storage too, not the memory provider.

<!-- pause 0.8 -->

### Where next

Next on Build: Moving in from another store. The links are on this episode's page in the documentation.

<!-- pause 2.0 -->

## Sources

| Narration | Source |
| --- | --- |
| TTL and soft delete showed how data goes away; this episode stops it going away on a restart | `videos/series.md`, B5 and B6 ideas |
| Durable grain storage and a durable write-ahead log in place of the in-memory defaults | `videos/series.md`, B6 title and idea |
| The defaults keep everything in memory | `docs/lattice/configuration.md`, "Registering Lattice"; `docs/lattice/wal-storage-providers.md`, "Registering a provider" |
| Grain storage holds leaves and their snapshots; the write-ahead log holds every committed write, in order | `docs/lattice.storage.file/README.md`, "What is it?"; `docs/lattice/wal-storage-providers.md`, opening |
| In memory, neither survives a restart, so going durable means replacing both | `docs/lattice/wal-storage-providers.md`, "InMemoryWalStorageProvider"; `docs/lattice/configuration.md`, "Registering Lattice" |
| The callback passed to `AddLattice` chooses grain storage; swap the memory provider for a durable Orleans provider such as Azure Table | `docs/lattice/configuration.md`, "Registering Lattice"; `docs/lattice/api.md`, the Azure Table registration |
| The provider must enforce ETags, so a write with a stale ETag fails; Orleans' providers do | `docs/lattice/configuration.md`, "The grain storage provider must enforce ETags" |
| Each silo checks at start-up; by default a provider that accepts a stale write fails silo start | `docs/lattice/configuration.md`, the fencing probe and `Reject` default |
| The file provider keeps the write-ahead log on local disk, with no cloud dependency, given a root directory | `docs/lattice/wal-storage-providers.md`, opening; `docs/lattice.storage.file/README.md`, "Quick Start" |
| It suits a single container with a mounted volume; each batch is all or nothing, and a crash before commit rolls it back | `docs/lattice.storage.file/README.md`, "What is it?"; `docs/lattice.storage.file/configuration.md`, "Choosing a data root" |
| Azure Table storage is for production deployments replicated across regions | `docs/lattice/wal-storage-providers.md`, opening |
| Either call replaces the in-memory log, before or after `AddLattice` | `docs/lattice/wal-storage-providers.md`, "Registering a provider"; `docs/lattice.storage.file/README.md`, "Quick Start" |
| The log is only half of a durable tree; it is trimmed once a snapshot in grain storage covers it, so pair it with durable grain storage too | `docs/lattice.storage.azuretable/README.md`, "Quick Start" |
| Next on Build: Moving in from another store; the links are on the episode page | `npm run series -- ending B6`, word for word |
