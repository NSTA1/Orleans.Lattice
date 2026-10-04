# Verified backup and restore

Backup and restore meet concurrent atomic writes, replication and failures at
several points, and the argument that they stay consistent there was until now
made only in prose and chaos tests. This page states what is now checked
formally, by four TLA+ specifications under
[`spec/backup/`](../../spec/backup/README.md) and by Coyote models driving
extracted production cores, and - just as importantly - what is not.

## What the specifications check

| Area | The guarantee checked | Specification |
|------|-----------------------|---------------|
| Capture | A backup never holds part of an atomic batch: not across two shards of one tree, and, for a cross-tree-consistent backup set, not across its member trees. A capture never holds an uncommitted batch's writes. Every set capture is accepted or fails explicitly. | `BackupCapture` |
| Provenance | A manifest never names an empty origin, never drops a real one, and a backup chain's HLC frontier covers every write it captured and never regresses. | `BackupProvenance` |
| Coordinated restore | A restore of a replicated tree is all-or-nothing across regions; no pre-restore write ever reaches a restored copy; a restore never installs another tenant's records; replication resumed after a restore converges. | `BackupRestore` |
| Local cutover and revert | A reader never pairs one physical copy with another copy's shard map; once a restore returns every reader sees the restored copy, and once a revert returns none does, however stale its routing; a tree is never deleted while its copies are in motion; a restore that crashes part-way still completes on retry. | `BackupCutover` |

Every property is paired with a deliberate defect (a mutation) that must make
TLC report it, and every protocol step of each specification is perturbed by at
least one, so a specification that could not notice a broken protocol fails the
build. TLC runs these checks on every pull request.

## Two guarantees above are not yet true of production

The specifications check the **intended** design. Two of the guarantees above
were found not to hold in production, and are tracked as defects:

- **A backup can hold an atomic batch torn (#4485).** A capture reads each shard
  at its own moment and serves a write still pending at that moment as if the
  batch had not committed, so a backup taken while a multi-shard
  `SetManyAtomicAsync` is being applied can hold one of its keys new and
  another old. A cross-tree-consistent backup set is affected the same way.
  The specification checks the approved fix, a short decision gate held for
  the capture; until it ships, the capture guarantees above describe the fix.
- **A restore's resumed replication could re-advance a peer (#4490, fixed).**
  If a shipper missed the notification that a restore moved the tree's alias,
  it could resume shipping from the retired copy for up to
  `ShipSourceIdentityBackstopInterval`, and a write made just before the
  restore could reach a peer's restored copy. The fix re-binds the shipper
  before its first send after every resume, which is the design the
  specification checks.

## Which parts run in production code

The checks reach production through three extracted cores, each executed by the
code that runs and by a test that drives it:

- the cross-tree set's drain gate and post-capture re-observation
  (`CrossTreeFenceWindow`), also driven by a Coyote model;
- the origin normalisation, per-origin high-water and consistency-cut frontier
  rules of every capture (`BackupChainFrontier`);
- the coordinated restore's single commit-or-abort decision
  (`CrossClusterSagaDecisionCore`), also driven by a Coyote model.

Everything else the specifications describe - registry leases, routing,
redirects, the alias reservation, the write and receive fences - is mapped to
its production code and to the tests that pin it in each specification's
refinement note, not executed by the model itself.

## What is not covered

Coverage of one half must not be read as coverage of the other. Not covered:

- the participant fence timer: a coordinated restore whose commit reaches a
  cluster more than five minutes after it prepared leaves that cluster
  compensated, which the documented all-or-nothing guarantee already scopes
  out;
- a replication batch delayed in flight across a whole restore cutover;
- reader-level atomicity across the members of a backup set while their aliases
  are swapped, which is not claimed anywhere;
- in-place and cold restores, which have no cutover;
- the restore-side behaviour of a resharded or resized tree, which belongs to
  the shard-ownership specification;
- the receiver side of cross-cluster atomic batches, which depends on #4480.

Each refinement note under [`spec/backup/`](../../spec/backup/README.md) lists its
abstraction gaps in full.
