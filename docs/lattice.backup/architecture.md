# Orleans.Lattice.Backup architecture

This page describes the end-to-end capture, incremental, restore, scheduling, and sink pipelines by behaviour, and the core Lattice seams they attach to. Public types are named; the engine, coordination grains, collectors, authorizer, and inventory registry are internal and are described by their effect.

## Where it attaches

The backup engine is a set of silo-singleton services and per-scope coordination grains layered strictly above the core data plane. It reuses the core primitives unchanged rather than reaching around them:

- **The snapshot cursor** gives a capture its point-in-time isolation. A capture opens through the public snapshot-cursor surface, so it inherits the core zero-observable-writes read, the saturation shedding, and the replay-budget guard: a capture whose in-scope size would exceed the replay budget fails fast before any data is read.
- **The write-ahead log** gives an incremental capture its resume points. The base backup's manifest records the per-partition WAL offsets of its consistency cut; an incremental reads forward from those offsets.
- **The last-writer-wins merge and bulk-load shard seams** give a restore its causal fidelity. Entries replay through the same HLC-preserving merge and bulk-load entry points the data plane uses, so every entry's hybrid-logical-clock, version vector, origin cluster id, expiry, and tombstone flag land bit-identical to the capture.
- **The access gate** gives every operation its authorization. Backup and restore are dedicated capabilities evaluated against the same gate the data path consults.
- **The tree registry** gives the sink, catalog, and shadow-cutover restore their storage. The reserved `sys-backup-*` trees are ordinary dogfooded `ILattice` trees that self-register and carry the core `sys-` catalog-hiding prefix.
- **The view infrastructure** gives the catalog its audit trail. `AddLatticeBackup` ensures views are present so a durable per-key history view over the catalog tree records every manifest catalogued and removed.

## Full capture pipeline

`ILatticeBackupCaptureService.CaptureAsync` runs a capture in ordered phases (the phase names are the values of the `phase` metric tag; see [Observability](observability.md)):

1. **Authorize.** The scope is authorized fail-closed at its root against the `Backup` capability before any data is touched. A whole-tree scope is a whole-tree check; a prefix or key scope is a point check at the prefix or key.
2. **Snapshot open.** A point-in-time cursor is opened over the scope. This is the phase that sheds under saturation or is rejected by the replay-budget guard.
3. **Export.** In-scope entries are streamed out of the pinned snapshot, page by page, with their full last-writer-wins / CRDT metadata. The stream is content-addressed as it flows, so a large tree is never materialized whole.
4. **Sink write.** The streamed bytes are written to the configured sink as one or more content-addressed artifacts, and the self-describing manifest is written alongside them.
5. **Manifest commit.** The manifest is registered in the catalog, keyed by the backup id. Committing the manifest is the point at which the backup becomes enumerable and restorable.

The manifest that results records the consistency cut (WAL sequence, HLC timestamp, per-origin frontier, and per-partition WAL offsets), the shard topology, a per-key shape and merge-mode map, per-origin provenance high-water marks, and an optional compression-dictionary reference. The id is the content address of the backup, so an identical retry derives the same id and stores once.

## Backup set and the cross-tree fence

`CaptureSetAsync` captures one full backup per scope under a single `BackupSetManifest`. A single-tree set, or a set with `CrossTreeConsistent` left `false`, issues no extra coordination and captures each member with the cheap per-tree cut.

When the flag is set over more than one tree, the set is captured as of a single shared-HLC causal fence. The fence is selected only after every in-flight cross-tree atomic saga touching the set has drained to a terminal decision, so a cross-tree atomic write is never torn across the set boundary: for each such batch, either all members are present at or under the fence or none are. The drain waits are bounded by `LatticeBackupOptions.CrossTreeFenceDrainTimeout`, polled at `CrossTreeFencePollInterval`, and retried up to `MaxCrossTreeFenceAttempts` times (an attempt is retried when a new cross-tree saga registers during the capture window). A fence that cannot stabilize within those bounds fails the capture with `LatticeBackupCrossTreeFenceException`. The selected fence and its drain statistics are recorded on the set manifest as a `BackupSetFence`.

## Incremental capture pipeline

`ILatticeBackupIncrementalCaptureService.CaptureIncrementalAsync` emits a forward-WAL differential layered on a base backup. It resumes from the base backup's per-partition WAL offsets and folds the entries that changed since the base cut into a delta artifact. The delta is written as the same uniform entry-array artifact shape a full backup uses, so the restore chain decodes a base and its increments through one path. The manifest records the base id as `BaseBackupId`.

The incremental **falls back to a full capture** when it cannot produce a sound delta: when the base resume point has been trimmed off the WAL (WAL fall-off), or when a range delete surfaces in the delta window (a range delete cannot be expressed as a forward-entry delta without risking a missed removal). A fallback is recorded as a capture retry with an incremental-fallback reason.

## Restore pipeline

`ILatticeBackupRestoreService.RestoreAsync` replays a manifest chain into a target tree:

1. **Read.** The manifest chain is read - a full backup, or a base plus ordered increments up to the chosen point.
2. **Verify.** Every referenced artifact is validated against its recorded content digest. Any mismatch aborts the restore with `LatticeRestoreValidationException` before anything is installed.
3. **Merge.** The entries are applied through the HLC-preserving seams, either **in place** (an empty-tree bulk-load fast path, or a last-writer-wins merge into existing data) or via an **atomic shadow-cutover** that builds a fresh physical tree and swaps the target's registry alias to it in one step.

A restore is idempotent: re-running the same request converges to the same state. A shadow-cutover restore records the physical tree it built (`ShadowPhysicalTreeId`) and the physical tree the alias resolved to beforehand (`PreviousPhysicalTreeId`).

`RevertRestoreAsync` undoes a shadow-cutover by swapping the registry alias back to `PreviousPhysicalTreeId`, restoring the pre-restore state. It is idempotent and rejects a result that did not come from a shadow-cutover restore.

Because a restore preserves each entry's origin cluster id and provenance, a restored tree re-synchronizes per origin faithfully under replication rather than presenting as a single new origin.

## Scheduling and retention

`ILatticeBackupScheduler` is the public front door; the actual coordination is a single per-scope grain keyed by `BackupScopeKey.For(scope)`. Keying the grain by the scope key means on-demand triggers, scheduled captures, and retention for the same scope are serialized through one coordinator and never overlap - a trigger issued while a capture is in flight returns `null` rather than starting a second one.

`EnsureScheduleAsync` registers Orleans reminders for the scope's configured full and incremental cadences (clamped up to the one-minute reminder minimum). Each scheduled cycle runs the capture and, when retention is enabled, a retention pass afterwards. `PruneAsync` evaluates the chain against `RetentionKeepLast` and `RetentionMaxAge` and prunes only backups that fail every enabled rule, always preserving the base chain of a retained increment; it returns a `BackupRetentionReport`.

Per-scope schedule registration and last-run status are tracked so the control facade and the observable gauges can report a scope's health (see `BackupSchedulerRuntimeStatus`).

## The sink seam

`ILatticeBackupSink` is the storage boundary. It stores two kinds of content - streamed, content-addressed artifacts and self-describing manifests - and its artifact surface is chunk-streaming on both write and read so a large payload never buffers whole. `AddLatticeBackup` installs the default in-cluster sink, which dogfoods the reserved `sys-backup-store` tree, storing manifests and streamed artifact chunks as ordinary rows. A durable external sink (for example [Azure Blob Storage](../lattice.backup.azureblob/architecture.md)) implements the same interface and replaces the registration; because the engine talks only to the seam, it stays unaware of the sink's backend.

Content addressing (`BackupContentHash`, lowercase hex SHA-256) is what makes the whole pipeline idempotent: identical artifact bytes derive an identical id, so a retried write is a no-op rather than a duplicate, and re-registering a manifest is harmless.

## The catalog

`ILatticeBackupCatalogStore` is the in-cluster index of manifests, persisted into the reserved `sys-backup-catalog` tree keyed by backup id. Every mutation runs through the standard write path, so it is captured by the durable per-key history view enabled by default - the record of what was catalogued and removed stays auditable beyond the source WAL window. Because the catalog tree carries the `sys-` prefix it is hidden from the default state-catalog surface, so the backup control API is the sole enumeration point for backups.

## Authorization

Backup and restore are dedicated capabilities (`Backup` for capture, `Restore` for author / bulk-load) evaluated against the registered core access gate through the shared enforcement helper the data plane already uses. This means the backup path inherits the gate's behaviour exactly: the system-origin bypass for infrastructure-authored turns, the zero-cost short-circuit when only the no-op core gate is registered (a cluster with no authorization add-on pays nothing), caller-subject resolution through the membership seam, and the bootstrap-administrator break-glass. A scope is authorized at its root: a partial or filtered allow is refused fail-closed, exactly as a bulk-load or admin operation is.

## Reserved namespace

The `sys-backup-store` and `sys-backup-catalog` trees are reserved under the `sys-backup-` prefix. An application tree that shadowed that namespace could corrupt the catalog, so `LatticeBackupReservedTrees` lets an application validate its own tree ids against the reserved prefix (mirroring the guards the membership and authorization packages enforce on their own reserved namespaces).
