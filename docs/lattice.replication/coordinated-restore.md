# Coordinated multi-cluster restore

Restoring a backup into a tree that is **replicated** across clusters is not a
local operation. If one cluster swapped in the restored data on its own, its
peers would still be shipping their pre-restore writes, and the restored cut
would be re-advanced (partially overwritten) the moment replication resumed.
Worse, a reader on another cluster could observe a torn mix of restored and
pre-restore state while the swap propagated. This is the failure mode tracked by
[issue #1169](https://github.com/NSTA1/Orleans.Lattice/issues/1169).

Coordinated restore closes that gap. When the restore target is currently
replicated, the restore is promoted into an **all-or-nothing cross-cluster
saga**: every participating cluster prepares the restored data, then a single
global decision either cuts every cluster over together or rolls every cluster
back. No peer re-advances the restored cut, and no reader ever observes a torn
or half-restored tree.

## When a restore becomes a saga

The decision is a function of the **target tree's current replication
membership**, never of where the backup was originally captured:

- **Target tree is replicated** - the restore runs as a coordinated saga across
  the union of the target tree's current peer set. A backup captured on a
  single cluster and restored into a replicated tree still runs the saga,
  because it is the target that must stay consistent across clusters.
- **Target tree is not replicated** (or the backup package is deployed
  single-cluster) - the restore runs as a plain local restore with no saga.
  There is nothing to coordinate.

A **backup set** (multiple trees captured together) restores as one unit: if any
member tree is replicated, the whole set restores under a single saga spanning
the union of the replicated members' peer sets, so every member tree flips
together on every participating cluster or none does.

## The saga phases

The coordinator drives every enlisted participant on every cluster through three
phases:

1. **Prepare** - each participant builds the restored data into a shadow
   alongside the live tree. Prepare is **unfenced and resumable**: the live tree
   keeps serving reads and accepting writes throughout the build, and a
   participant that restarts mid-build resumes from its checkpoint rather than
   restarting from zero. Each participant returns a vote.
2. **Commit** - reached only if **every** participant on every cluster voted to
   commit. Each participant engages a short per-tree **write fence**, atomically
   swaps the tree's alias to the restored shadow, then unblocks local writes.
   Local writes resume as soon as the cutover completes, but cross-cluster
   shipping and receiving stay paused until the saga completes globally, so an
   early-flipping cluster cannot re-advance the restored cut. The write fence is
   held only for the cutover, not for the whole build, so healthy clusters are
   not write-starved while a large tree builds.
3. **Abort** - reached if any participant voted to abort. Every participant that
   prepared is compensated: its shadow is reverted and garbage collected and the
   pre-restore tree is left untouched.

Two guarantees make this safe under failure:

- **Single global decision.** The coordinator reaches exactly one
  commit-or-abort decision after collecting every vote, and delivers that one
  decision to every participant. A participant never observes a mixed outcome.
- **Bounded fence-timer auto-compensation.** A prepared participant holds its
  cutover fence under a bounded timer. If the coordinator is lost before it
  delivers a decision, the fence expires and the participant auto-compensates
  (aborts), so a prepared restore can never leak after a coordinator loss.

## Reliability under duress

Restoring a large tree onto a small cluster is admitted only when it can
actually succeed. An infeasible target (a tree that cannot fit the target
cluster) is **refused at admission**, before any shadow build starts, so the
saga fails fast with a clear vote rather than exhausting capacity mid-build. A
participant whose build exhausts its bounded retry budget votes to abort and
garbage collects its partial shadow, leaving no orphaned shadow state; the whole
saga then rolls back all-or-nothing.

## The sink must be shared, and that is checked at capture time

Every cluster in the saga resolves the backup's manifest chain from **its own**
configured `ILatticeBackupSink`. A coordinated restore therefore only works if
every cluster's sink is the *same* storage. Point each region at an isolated
sink and the misconfiguration is invisible: each capture succeeds locally, each
local health check passes, and the fault only surfaces as an all-or-nothing saga
abort at restore time - after the operator has spent weeks relying on backups
that were never restorable.

That check now runs at **capture/startup** time instead. The replication package
registers a real cross-cluster sink-sharing probe over the backup package's
no-op default (the same layering trick the saga dispatcher itself uses - see
[Enlisting your own resource in the saga](#enlisting-your-own-resource-in-the-saga)),
so the backup package never takes a dependency on replication. When a tree is
replicated and the deployment has peers, each cluster writes a tiny self-naming
marker into its own sink and reads every peer's marker back out of that same
sink. A marker that is missing while its peer answers the saga control channel
proves the sinks are separate; a marker missing from an unreachable peer is
merely undecided and is re-probed on the next backup-health sweep.

The verdict is logged at start, annotated onto every affected backup's health
report (so it shows as a `Warning` in the Explorer Backups tab), and can be made
to block silo start outright. Nothing is probed at all - no sink write and no
network call - when no tree is replicated or the deployment has no peers.

See [backup configuration](../lattice.backup/configuration.md#cross-cluster-sink-sharing)
for the enforcement modes and their defaults, and
[disaster recovery](../lattice.backup/disaster-recovery.md#un-restorable-backups-a-sink-that-is-not-shared)
for how the verdict reaches the health surface.

## Triggering a restore

Use the ordinary backup restore surface. Restoring into a replicated target
transparently runs the saga; the caller does not opt in.

```csharp verify
using Orleans.Lattice.Backup;

// Restore an entire captured backup set as one coordinated unit. When any member
// tree is replicated this runs as a single all-or-nothing cross-cluster saga
// across the union of the replicated members' peer sets; otherwise it runs as a
// plain local per-member restore.
var restoreService = client.ServiceProvider.GetRequiredService<ILatticeBackupRestoreService>();
IReadOnlyList<LatticeRestoreResult> results =
    await restoreService.RestoreSetAsync("your-backup-set-id", cancellationToken);
```

A single-tree restore uses `RestoreAsync(LatticeRestoreRequest, CancellationToken)`
in the same way; see the [backup package restore docs](../lattice.backup/api.md)
for the request shape and result fields.

## Enlisting your own resource in the saga

The built-in restore participant is one participant among many. An application
that holds a resource which must flip atomically alongside a replicated restore
(for example an external projection or a downstream index) can enlist its own
participant so it runs in the **same** saga, under the same unanimous prepare and
single global decision.

Implement the public `ISagaParticipant` interface (in `Orleans.Lattice.Replication`)
and register it with `AddLatticeSagaParticipant<TParticipant>(name)` on the silo
builder. The interface has four methods:

- `PrepareAsync` - prepare the resource set this participant hosts for the saga
  and return a `SagaParticipantPrepareResult` carrying the vote. The work may be
  long-running and must be idempotent and resumable.
- `CommitAsync` - make the prepared mutation durable. Idempotent.
- `AbortAsync` - compensate (roll back) the prepared resource set. Compensation
  must be **total**: once a participant votes to commit it must always be able to
  undo that prepare.
- `GetStatusAsync` - report the phase the participant currently holds, without
  changing state.

The optional `name` argument passed to `AddLatticeSagaParticipant` is used for
diagnostics and logging only; it never affects the saga wire contract. A
participant that hosts nothing for a given saga prepares vacuously (votes to
commit) rather than blocking the saga. Registration is idempotent per participant
type.

**Guardrails.** Every method must be idempotent, and a participant that cannot
guarantee total compensation must vote to abort from `PrepareAsync` rather than
preparing. These match the intra-cluster cross-tree saga contract.

## Observability

The saga emits OpenTelemetry instruments on the replication meter
(`orleans.lattice.replication`): saga phase durations, participant vote / commit
/ abort counts, per-tree write-fence window durations, and compensation counts by
cause. See [observability](observability.md#coordinated-restore-saga) for the
full instrument list and tags.
