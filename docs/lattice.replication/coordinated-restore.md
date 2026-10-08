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
  the local cluster and every current replication peer. A backup captured on a
  single cluster and restored into a replicated tree still runs the saga,
  because it is the target that must stay consistent across clusters.
- **Target tree is not replicated** (or the backup package is deployed
  single-cluster) - the restore runs as a plain local restore with no saga.
  There is nothing to coordinate.

A **backup set** (multiple trees captured together) restores as one unit: if any
member tree is replicated, the whole set restores under a single saga spanning
the local cluster and every current replication peer, so every member tree flips
together on every participating cluster or none does.

The initiating cluster authorizes the restore with the caller's identity before
it decides whether to dispatch a saga, so an unauthorized caller never reaches a
peer: it checks the `Restore` capability over the target tree (over every member
tree for a set) and, when a single-tree restore retargets onto a different tree,
the `Backup` capability over the tree the backup was captured from. A refused
capability check throws `LatticeAuthorizationDeniedException`. A single-tree
restore into a replicated target then runs the capacity check on the initiating
cluster and refuses an infeasible target with a
`LatticeRestoreValidationException` that names the refusal but not the target's
stored size or shard count.

Before any cluster prepares, the coordinator probes every current peer over the
saga control channel and refuses to start - with a
`LatticeRestoreValidationException` naming the unreachable peers - if any peer
cannot be reached, because a partial coordinated restore is never allowed. An
aborted saga likewise surfaces to the caller as a
`LatticeRestoreValidationException`, after every cluster has been compensated.

## The saga phases

The coordinator drives every enlisted participant on every cluster through its
phases:

1. **Prepare** - each participant builds the restored data into a shadow
   alongside the live tree. Prepare is **unfenced and resumable**: the live tree
   keeps serving reads and accepting writes throughout the build, and a
   participant that restarts mid-build resumes from its checkpoint rather than
   restarting from zero. Each participant returns a vote.
2. **Commit** - reached only if **every** participant on every cluster voted to
   commit. Each participant engages a short per-tree **write fence**, atomically
   swaps the tree's alias to the restored shadow, then unblocks local writes.
   The fence covers every shard the tree's routing reaches when it engages -
   including a shard an adaptive split added, on the physical copy an earlier
   resize or restore put behind the alias - and every release lifts exactly
   that set. The cutover carries the restored copy's shard map in the same
   registry write as the alias and records the replaced map on the shadow, so
   revert can move the previous copy's map back atomically and no reader can
   pair one physical copy with another copy's routing map.
   The alias swap is bounded by tree ownership like every alias change: a
   registered `ITreeOwnershipGuard` (the `Orleans.Lattice.Apps` package registers
   one) is consulted before the alias is written, and a denial throws
   `LatticeTreeOwnershipDeniedException` without swapping. The restored shadow
   records the target tree as its origin, so the guard admits a restore into the
   tree it was built for.
   Local writes resume as soon as the cutover completes, but cross-cluster
   shipping and receiving stay paused until the saga completes globally, so an
   early-flipping cluster cannot re-advance the restored cut. The write fence is
   held only for the cutover, not for the whole build, so healthy clusters are
   not write-starved while a large tree builds. The write fence also self-lifts
   on a bounded cutover deadline (five minutes after it engages), so a stalled
   cutover never fences local writes indefinitely; that release leaves shipping
   and receiving paused until the saga completes globally. When shipping
   resumes, each shipper re-resolves the tree's source identity before its first
   send, so it ships from the restored copy even if the alias-change push to it
   was lost; it never drains the retired log onto a peer's restored copy (#4490).
3. **Abort** - reached if any participant voted to abort. Every participant that
   prepared is compensated: its shadow is reverted and garbage collected and the
   pre-restore tree is left untouched.

These guarantees make this safe under failure:

- **Single global decision.** The coordinator reaches exactly one
  commit-or-abort decision after collecting every vote, and delivers that one
  decision to every cluster that voted to commit; a cluster that voted to abort
  has already compensated its own prepared work. A participant never observes a
  mixed outcome.
- **A fence timer asks; it never decides.** A prepared participant waits for
  the decision under a cutover-fence timer (five minutes; distinct from the
  per-tree write fence, which engages only at commit). The participant voted to
  commit, so by the time its timer fires the coordinator may already have
  committed every other cluster; compensating on the timer alone could leave
  this cluster on its pre-restore tree while the others serve the restored copy.
  So when the timer fires the participant asks the coordinator cluster for the
  saga's durable decision ([#4637](https://github.com/NSTA1/Orleans.Lattice/issues/4637))
  and applies it: it commits on a commit decision and compensates on an abort.
  A coordinator that never started the saga, or no longer holds it, answers
  abort, because it prepared nobody. A decision the participant applies itself
  reaches every member tree of a backup-set restore, exactly as the
  coordinator's delivery would.
- **An unreachable coordinator keeps the fence up.** While the coordinator is
  still collecting votes, or cannot be reached at all, the participant keeps its
  prepared state and asks again on every fence tick. It reports the fence's age
  past its window on the `orleans.lattice.replication.saga.participant.fence_held_age`
  gauge and logs an error while the coordinator is unreachable (see
  [Observability](observability.md#coordinated-restore-saga)). The coordinator
  bounds a pending decision itself: it aborts a saga whose prepare is still
  being retried an hour after the saga started. If the coordinator cluster is
  lost for good, an operator resolves the participant - see
  [Resolving a participant whose coordinator is lost](#resolving-a-participant-whose-coordinator-is-lost).
- **A refused decision is never a success.** A participant answers each
  commit or abort with its durable phase. If one reports the other terminal
  phase - it refused the decision - the coordinator does not complete the saga:
  it records the split, logs an error, fails the restore call, and keeps
  re-delivering, because the clusters disagree and need operator repair.

### Restored copies are born receive-closed

Pausing receiving is not enough on its own. The applier consults the tree's
receive fence once per entry, through a short per-silo cache, so an entry can
pass that check just before the pause and reach the tree just after the alias
swap - or, if it is slow enough, after the saga's lift
([#4593](https://github.com/NSTA1/Orleans.Lattice/issues/4593)). An entry the
receiver parked in its causal-apply buffer before the pause can likewise drain
after the lift. Three rules make the restored copy unreachable to all of them:

- **Admissions carry the fence epoch.** Every pause of a tree's receive fence
  bumps its epoch, and each answer the receive gate gives carries the epoch it
  was read under. The applier stamps every entry it admits with that epoch.
  A park re-reads the fence uncached and defers an entry the fence is in fact
  paused for, or whose admission a pause has since superseded, so a parked
  entry carries the epoch current when it was parked; the drain stamps it.
- **Closed before it is routable, at the pause's epoch.** The commit's fence
  engage pauses receiving first, then closes each restored physical copy with
  the epoch of that pause as the copy's minimum admission epoch, and only then
  does the alias swap run. Only the fence lift that resumes receiving - the
  abort or terminal lift, or the lift on observed global completion - opens the
  copy; the write-fence deadline self-lift and the local write unblock leave it
  closed. The closed set is recorded durably before the copy is closed, so a
  crash, a re-driven commit or an abort between the close and the lift still
  opens it. The minimum admission epoch survives the open.
- **Checked on every routing resolution.** Every replicated write path on the
  tree's apply seam - point and batched writes and deletes, range deletes, CRDT
  deltas, saga prepares, terminals and cross-tree finalizes, and the snapshot
  bootstrap import, which enters through the same applier - resolves its route
  under a replication-apply mark, and every resolution under that mark checks
  the resolved copy: it refuses a closed copy, and an open one whose minimum
  admission epoch is above the apply's stamp (an apply with no stamp fails
  closed). A write that was routed to the replaced copy is redirected by that
  copy and re-resolves onto the restored one, so it meets the check there. A
  copy only moves from closed to open and its epoch is fixed once open, so a
  routing activation caches an open answer and always re-reads a closed one.

A refused live entry is deferred, exactly as the receive fence defers it, and
the sender re-ships it. That cannot re-advance the cut: every pre-cutover entry
sits in a retired log, and a shipper re-resolves its source and rebinds to its
restored copy before it sends after the saga (#4490), so a retired log is never
shipped again. A post-cutover entry that was refused only because a stale cache
stamped it is re-admitted with a fresh epoch and lands, which is where it
belongs. A parked entry the copy refuses as admitted before the restore is
discarded instead: no peer ships a post-cutover write before the saga completes
globally, so an entry parked before the receiver's pause is a pre-cutover write,
and the restore excludes it.

A copy that stays closed defers every replicated write to its tree. Alarm on
`orleans.lattice.restore.copy_receive_closed_age` (see
[Metrics](../lattice/metrics.md#restored-copy-receive-fence-sourced-from-copy-receive-fence-owner-and-tree-router)):
it reports how long each closed copy has been closed, and
`orleans.lattice.restore.copy_receive_fenced` counts the applies it refused, by
reason.

## Reliability under duress

Every participant runs an **admission pre-flight** before it builds a shadow: it
probes the backup's self-describing size and topology and votes to abort if that
probe fails or its capacity check refuses the target, so the saga fails fast
with a clear vote rather than failing mid-build. The shipped capacity check
admits every target, so today the pre-flight catches an unprobeable backup
rather than a tree too large for the cluster. A participant whose build fails
permanently (a restore validation failure, such as an artifact absent from the
sink or failing its content-digest check) or exhausts its bounded retry budget
(a set restore does not retry: the first member build that fails aborts the
whole set) votes to abort and garbage collects its partial shadow, leaving no
orphaned shadow state; the whole saga then rolls back all-or-nothing.

### Resolving a participant whose coordinator is lost

A prepared participant whose coordinator can no longer be reached keeps its
cutover fence up and keeps asking. If the coordinator cluster is gone for good,
`ILatticeReplicationAdmin.ResolveCrossClusterSagaParticipantAsync(sagaId,
commit, reason)` resolves the participant on this cluster. The saga id is the
coordinated restore's operation id.

- **The coordinator's answer wins.** The participant still asks the coordinator
  first. If it answers with a decision, that decision is applied, and a request
  that contradicts it is refused with `InvalidOperationException`. If it answers
  that it is still deciding, the call is refused and nothing changes: let it
  decide.
- **Consequence.** Only when the coordinator cannot be reached is the requested
  decision applied, and then nothing checks it against the other clusters.
  Resolve every cluster of the saga the same way, or the restore ends with some
  clusters on the restored copy and others on their pre-restore tree. A
  resolution to abort counts on the `coordinator-loss` cause of
  `orleans.lattice.replication.saga.compensations`.
- **Audit.** It requires a reason. Every call is audit-logged at `Warning` before
  it is dispatched. Like the other verbs on the same seam, it is available to
  host code only and is not exposed by any network API. It is never part of an
  automatic recovery path.

```csharp verify
ILatticeReplicationAdmin admin = client.ServiceProvider
    .GetRequiredService<ILatticeReplicationAdmin>();

// The coordinator cluster site-home was decommissioned mid-restore, and every
// other cluster of the saga was resolved to abort the same way.
bool resolved = await admin.ResolveCrossClusterSagaParticipantAsync(
    "restore-saga-id", commit: false, reason: "coordinator site-home decommissioned", cancellationToken);
_ = resolved;
```

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
merely undecided and is re-probed on the next backup-health sweep. Over the
shipped gRPC saga control channel, though, the peer refuses that reachability
call, because it carries an empty saga id and the receiving service rejects one
as an invalid argument, so a reachable peer still counts as unreachable and a
missing marker leaves the verdict undecided rather than refuting the sink.

The verdict is logged at start, annotated onto every affected backup's health
report (so it shows as a `Warning` in the Health column of the backup catalogue
in the Explorer's Backups area), and can be made to block silo start outright.
Nothing is probed at all - no sink write and no network call - when no tree is
replicated or the deployment has no peers.

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
// across the local cluster and every current replication peer; otherwise it
// runs as a plain local per-member restore.
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
builder. The interface methods are:

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
type and form: repeated unnamed calls, or repeated named calls (the first name is
kept), enlist it once, but mixing an unnamed and a named call for the same type
enlists it twice, so every saga drives it through each phase twice - use one form
per participant type.

**Guardrails.** Every method must be idempotent, and a participant that cannot
guarantee total compensation must vote to abort from `PrepareAsync` rather than
preparing. These match the intra-cluster cross-tree saga contract.

## Observability

The saga emits OpenTelemetry instruments on the replication meter
(`orleans.lattice.replication`): saga phase durations, participant vote / commit
/ abort counts, per-tree write-fence window durations, and compensation counts by
cause. See [observability](observability.md#coordinated-restore-saga) for the
full instrument list and tags.
