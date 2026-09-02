# Disaster recovery

How to recover Orleans.Lattice backups after catastrophic loss of the cluster
that took them - and how the backup surface keeps itself recoverable so that
recovery is possible in the first place.

This guide covers the operator-facing model and runbook. For the per-member API,
see the [API reference](api.md) and the
[`Orleans.Lattice.Api.Backup` API reference](../lattice.api.backup/api.md).

## The problem: two stores that can drift

A backup has two distinct pieces of state:

- **Payload** - the self-describing `BackupManifest` and its content-addressed
  artifacts, written to the external [sink](../lattice.backup.azureblob/README.md).
  This is the backup itself.
- **Discovery index** - the per-cluster catalog in the reserved
  `sys-backup-catalog` tree, which lists the manifests a cluster knows about so
  they can be enumerated, described, and selected as restore points.

The catalog lives inside the very cluster whose data the backup protects. If that
cluster is lost (corrupted grain storage, a wiped or rebuilt silo, a lost storage
account), a catalog-only view of the world would report *no backups* even though
the payload is sitting intact in the durable sink. Conversely, a catalog can
retain a row whose sink payload has been deleted out from under it, so it offers a
restore point that will fail the moment it is used.

## The model: the sink is the single source of truth

Orleans.Lattice resolves this by treating the **durable sink as authoritative**
and the catalog as a **rebuildable projection** over it. A `BackupManifest` is
fully self-describing: it records the consistency cut, shard topology, per-key
shape and merge-mode map, per-origin provenance, content descriptors (with
SHA-256 digests), and - for an increment - its `BaseBackupId`. Nothing the catalog
holds is unique to the catalog; every row can be re-derived from the sink.

Four capabilities follow from that model, each a fail-closed administrative
operation on the `Orleans.Lattice.Api.Backup` control facade
(`ILatticeBackupControl`):

| Capability | Operation | What it does |
|---|---|---|
| Rebuild catalog | `RebuildCatalogFromSinkAsync` | Re-derives the catalog by enumerating every manifest in the sink and re-registering it. |
| Scrub catalog | `ScrubCatalogAgainstSinkAsync` | Flags (and optionally prunes) catalog rows whose sink payload is gone. |
| Cold restore | `ColdRestoreAsync` | Restores a backup into a fresh cluster from the sink alone, with no surviving catalog. |
| Health monitoring | `IsHealthMonitoringAvailableAsync`, `CheckBackupHealthAsync`, `GetBackupHealthAsync`, `ConfigureBackupHealthAsync` | Periodically re-verifies that each backup's sink payload is present and intact. |

### Rebuild the catalog from the sink

`RebuildCatalogFromSinkAsync` enumerates every manifest the sink holds (via
`ILatticeBackupSink.ListManifestsAsync`, which returns manifests in backup-id
order) and re-registers each into `sys-backup-catalog` under system origin. It is
idempotent: a manifest already catalogued is reconciled in place, keeping its
immutable capture timestamp, rather than duplicated; a catalog missing rows the
sink has is repopulated. It returns a `BackupCatalogRebuildReport` whose invariant
is `ScannedCount == RegisteredCount + ReconciledCount`.

Use it whenever the catalog has drifted from the sink - after a non-clean cluster
restart, a partial storage loss, or any time the enumerated backups look
incomplete.

### Scrub the catalog against the sink

`ScrubCatalogAgainstSinkAsync` is the reconcile pass in the other direction: it
enumerates every catalog row and probes the sink for its resolvability (manifest
present, and every referenced artifact present and committed), reporting the
**orphans** - rows whose payload is gone. It is **non-destructive by default**:
with `pruneOrphans: false` it only flags orphans in the returned
`BackupCatalogScrubReport`; with `pruneOrphans: true` it removes each orphan row.
It is idempotent, and shares the same high-privilege, fail-closed Restore grant as
the rebuild. Scrubbing keeps a dead backup from ever being listed or offered as an
incremental base or restore point only to fail later.

### Cold restore into a fresh cluster

`ColdRestoreAsync` is the acid test that a backup is genuinely useful after cluster
loss. It restores a backup into a **brand-new, independent cluster** whose only
shared state with the original is the sink:

1. It bootstraps the reserved `sys-` trees if they are absent, so a cluster whose
   catalog has never existed can proceed.
2. It resolves the target manifest and walks its `BaseBackupId` chain **directly
   from the sink**, never the catalog.
3. It verifies every referenced artifact against its recorded digest before
   applying anything.
4. It replays the chain through the existing HLC-preserving restore engine, so the
   recovered tree keeps every entry's hybrid-logical-clock, version vector, origin
   cluster id, expiry, and tombstone flag exactly as captured.
5. It re-projects the catalog from the sink, so the recovered cluster ends up with
   a correct catalog.

It reuses `LatticeRestoreRequest` / `LatticeRestoreResult`, authorizes fail-closed
against the target scope, and is idempotent. It throws
`LatticeRestoreValidationException` when the backup is absent from the sink, the
base chain is broken, or an artifact is missing or tampered.

Because a cold restore depends on nothing but the sink, the same call recovers a
single tree, an incremental chain, or a whole backup set - as long as the sink is
reachable.

## Recovery runbook

When a cluster is lost and you are standing up a replacement that points at the
same durable sink:

1. **Stand up the replacement cluster** with the backup package registered and the
   same durable sink configured (for example the
   [Azure Blob sink](../lattice.backup.azureblob/README.md) pointed at the
   surviving storage account). The reserved `sys-` trees start empty.
2. **Cold-restore each tree** you need with `ColdRestoreAsync`, targeting the tree
   id you want and the backup id (or the tip of an incremental chain). Each call
   bootstraps the `sys-` trees on first use and re-projects the catalog as it goes.
3. **Verify the catalog** by listing backups through the control facade; after the
   cold restores the catalog reflects everything the sink holds. If you restored
   only some trees, run `RebuildCatalogFromSinkAsync` to re-project the full
   catalog for discovery without restoring the remaining payload.
4. **Scrub** with `ScrubCatalogAgainstSinkAsync` if you suspect the sink itself
   lost some payload, so the catalog only advertises resolvable restore points.

The recovered cluster is causally faithful to the source: entries replay through
the HLC-preserving merge and bulk-load seams, so a restored tree converges
identically to the original.

## Keeping backups recoverable: health monitoring

Recovery only works if the sink payload is actually present and intact when you
need it. A blob can be deleted out of band, a lifecycle policy can expire it, or an
artifact can bit-rot. Health monitoring surfaces these faults **before** a disaster
rather than at restore time.

Every catalogued backup is **auto-enrolled** in a periodic health check. Each check
resolves the backup's manifest, confirms every referenced artifact is present and
committed, and **re-hashes each artifact against the digest the manifest recorded at
capture time**, so silent corruption is caught, not just deletion. The result is a
`BackupHealthReport` (status `Healthy` / `Warning` / `Missing` / `Unknown`, the
missing artifact ids, the hash-mismatched artifact ids, when it was last checked,
and a human-readable explanation) persisted in the reserved `sys-backup-health`
tree.

Key properties:

- **Gated on a durable sink.** The monitor is inert, and the Explorer health column
  hidden, when the registered sink is not durable (`ILatticeBackupSink.IsDurable`
  is `false`). Verifying payload that shares the fate of the cluster it protects -
  the ephemeral in-cluster sink - proves nothing about disaster recovery, so there
  is no point running it there.
- **Periodic and configurable.** The monitor runs as a reminder-driven grain that
  mirrors the backup scheduler. It sweeps on a cluster-wide cadence (default every
  6 hours) and re-verifies each enrolled backup whose interval has elapsed. The
  `MultiSiteManufacturing` sample runs it every 5 minutes.
- **Per-backup overrides.** An operator can enable or disable monitoring and set a
  custom interval per backup with `ConfigureBackupHealthAsync`, trigger an on-demand
  check with `CheckBackupHealthAsync`, and read the last stored report with
  `GetBackupHealthAsync`.
- **Peer visibility for replicated trees.** For a backup of a replicated tree, each
  sweep also refreshes the cross-cluster sink-sharing verdict, and the report carries
  it as `PeerVisibility` plus the `PeerUnconfirmedClusterIds` that could not see the
  sink. A backup that is locally intact but whose sink is **provably not readable
  from a peer cluster** is reported `Warning`, not `Healthy`, with an explanation
  naming the peers - because a coordinated restore would abort on it. See
  [Un-restorable backups: a sink that is not shared](#un-restorable-backups-a-sink-that-is-not-shared).

### Un-restorable backups: a sink that is not shared

A coordinated restore of a replicated tree is all-or-nothing across every cluster,
and each cluster resolves the manifest chain from **its own** configured sink. Point
each region at an isolated sink and every capture succeeds, every local health check
passes, and the restore aborts - at the worst possible moment.

That failure mode is now caught at capture time instead. When at least one tree is
replicated and the deployment has at least one peer, each cluster writes a tiny
self-naming marker into its own sink at start and reads every peer's marker back out
of that same sink. A marker that is missing while its peer is reachable proves the
sinks are separate. The verdict is logged loudly at start, annotated onto every
affected backup's health report, and - if `SinkSharingEnforcement` is set to
`FailFast` - blocks the silo from starting at all. A missing marker from a peer that
is itself unreachable is reported `Unverified` and never fails anything; it is
re-probed on the next sweep.

A replicated tree backed by the default **in-cluster** sink is rejected outright at
start regardless of that setting, since a per-cluster reserved tree is provably
invisible to a peer.

Nothing runs, and nothing changes, for a non-replicated tree, a single-cluster
deployment, or a host that does not wire the replication package. See
[Configuration](configuration.md#cross-cluster-sink-sharing) for the enforcement
modes and the probe timeout.

### Health in the Explorer

The [`Orleans.Lattice.Explorer`](../lattice.explorer/managing-backups.md) **Existing Backups**
tab renders a per-row health indicator (an OK marker when healthy, a warning marker
when a backup is unresolvable, has a missing blob, has a hash mismatch, or - for a
replicated tree - sits in a sink a peer cluster cannot read). Clicking
the warning opens a diagnostics dialog that names exactly which artifact is missing,
which hash mismatched, or which peer cluster could not see the sink, and when the
backup was last checked. The New Backup form
and the schedule dialog expose the per-backup health schedule. When no durable sink
is configured the health column and its controls are hidden.

## Sink durability posture

Because the sink is the single source of truth for disaster recovery, its
durability is the durability of your backups. Recommendations:

- Use a **durable, external** sink (the Azure Blob sink, or another
  `ILatticeBackupSink` whose `IsDurable` is `true`) for any backup you expect to
  survive cluster loss. The in-cluster sink is a convenience for development and
  dogfooding; it shares the fate of the cluster and is not a disaster-recovery
  target.
- Give the sink storage account its own **redundancy and geo-replication** posture
  appropriate to your recovery objectives; the backup surface treats whatever the
  sink returns as truth.
- Keep **health monitoring enabled** so payload loss is detected on a cadence rather
  than discovered during an outage, and act on any `Warning` or `Missing` report.
- Guard the sink against **out-of-band deletion** (lifecycle policies, retention
  locks) so a backup you still depend on is not expired underneath the catalog.

## See also

- [API reference](api.md) - the health service, store, and cold-restore engine seams.
- [`Orleans.Lattice.Api.Backup` API reference](../lattice.api.backup/api.md) - the
  rebuild, scrub, cold-restore, and health control-facade operations.
- [Architecture](architecture.md) - the capture, incremental, restore, and sink
  pipelines.
- [`Orleans.Lattice.Backup.AzureBlob`](../lattice.backup.azureblob/README.md) - the
  durable Azure Blob Storage sink.
