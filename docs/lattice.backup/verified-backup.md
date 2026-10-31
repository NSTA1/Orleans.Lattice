---
agent_spec: "docs/agents/api/backup.json"
---

# Verified backup and restore

Backup and restore meet concurrent atomic writes, replication and failures at
several points, and the argument that they stay consistent there was until now
made only in prose and chaos tests. This page states what is now checked
formally, by six TLA+ specifications under
[`spec/backup/`](../../spec/backup/README.md) and by Coyote models driving
extracted production cores, and - just as importantly - what is not.

## What the specifications check

| Area | The guarantee checked | Specification |
|------|-----------------------|---------------|
| Capture | A backup never holds part of an atomic batch: not across two shards of one tree, and, for a cross-tree-consistent backup set, not across its member trees. A capture never holds an uncommitted batch's writes. Every set capture is accepted or fails explicitly. | `BackupCapture` |
| Incremental backups | Restoring an incremental backup chain never yields part of an atomic batch, or the writes of a batch that did not commit; an increment taken after a batch commits restores it whole; and an increment falls back to a full backup only for a batch it could not otherwise hold whole. | `BackupIncremental` |
| Provenance | A manifest never names an empty origin, never drops a real one, and a backup chain's HLC frontier covers every write it captured and never regresses. | `BackupProvenance` |
| Coordinated restore | A restore of a replicated tree is all-or-nothing across regions; no pre-restore write ever reaches a restored copy; a restore never installs another tenant's records; replication resumed after a restore converges. | `BackupRestore` |
| Local cutover and revert | A reader never pairs one physical copy with another copy's shard map; once a restore returns every reader sees the restored copy, and once a revert returns none does, however stale its routing; a tree is never deleted while its copies are in motion; a restore, or a revert, that crashes part-way still completes on retry. | `BackupCutover` |
| Retention pruning | Every backup the catalog lists, and in particular every backup the retention policy keeps, with its whole base chain, stays restorable while full and incremental captures of the same scope run alongside the pruning and either side crashes at any step; an incremental backup is published only while its base is restorable; nothing the policy keeps is pruned; and a backup outside the policy that nothing builds on is eventually pruned. | `BackupRetention` |

Every property is paired with a deliberate defect (a mutation) that must make
TLC report it, and every protocol step of each specification is perturbed by at
least one, so a specification that could not notice a broken protocol fails the
build. TLC runs these checks on every pull request that changes a TLC input (a
specification, its configuration, the Formal harness that builds, launches or
judges TLC cases, the TLA+ toolchain, or a build file), and on every release-line build, integration-branch push and nightly
coverage run; a pull request that changes none of those inputs cannot alter
their verdict.

## Defects the specifications found

The specifications check the **intended** design. These guarantees were found
not to hold in production:

- **A backup could hold an atomic batch torn (#4485, fixed).** A capture read
  each shard at its own moment and served a write still pending at that moment
  as if the batch had not committed, so a backup taken while a multi-shard
  `SetManyAtomicAsync` was being applied could hold one of its keys new and
  another old. A cross-tree-consistent backup set was affected the same way.
  The fix holds a short decision gate for the capture and resolves every
  still-pending write against the decisions recorded when the gate was taken;
  the specification checks that design.
- **A restore's resumed replication could re-advance a peer (#4490, fixed).**
  If a shipper missed the notification that a restore moved the tree's alias,
  it could resume shipping from the retired copy for up to
  `ShipSourceIdentityBackstopInterval`, and a write made just before the
  restore could reach a peer's restored copy. The fix re-binds the shipper
  before its first send after every resume, which is the design the
  specification checks.
- **An incremental backup could hold an aborted or undecided batch (#4589,
  fixed).** An increment copied an atomic batch's prepare-phase writes as
  ordinary data, so restoring it could install the writes of a batch that later
  aborted, or of one still undecided, or only some keys of a batch. The fix
  resolves each batch in the increment's window against the same decision gate a
  full backup uses: a committed batch is included whole, an aborted one never,
  and an undecided one is left for the next increment. A batch that straddles a
  full backup's frontier makes the increment fall back to a full backup.
- **An increment on an older base could omit a committed batch (#4686,
  fixed).** A base captured before the #4589 fix does not record the batches it
  held back as undecided, so an increment layered on it could not look them up
  and could leave out a batch that committed after the base. An incremental
  capture on such a base now falls back to a full backup, which starts a new
  chain.

- **A capture could hold a committed batch with one key absent (#4619, fixed).**
  A decision whose tombstone had expired but was still stored was read at the
  gate as undecided and hidden, so a shard holding the batch's terminal kept
  its key while a shard still pending dropped the other. The specification
  checks the fixed design, which captures such a decision at its recorded
  verdict.
- **A coordinated restore could end with one cluster restored and another not
  (#4637, fixed).** A participant's fence timer compensated on its own once it
  outlived its window, even after the restore's coordinator decided to commit,
  and the coordinator counted the participant's refusal of the late commit as
  success, so the restore could finish mixed. A prepared participant now asks
  the coordinator for the decision when its fence expires and keeps the fence
  up while the coordinator is undecided or unreachable, and the coordinator
  never completes a saga a participant refused. The specification checks this
  design; the one exception it does not cover is an operator resolving a
  participant whose coordinator is lost, which is audited.

- **Retention could prune the base of an incremental backup still being
  captured (#5357, fixed).** A retention pass planned from a catalog listing
  that cannot hold an increment still in flight, and an increment read its base
  once, before it drained, so a pass could delete the base the increment had
  chosen and the increment was then published without one: listed, and not
  restorable. Two retention passes could also run at once on one scope. The fix
  is a fence on both sides - a pass removes each backup's catalog row before it
  looks in the sink for an increment built on it, and keeps any such backup; an
  increment re-reads its base's catalog row just before it publishes, and takes
  a fresh full backup instead if the row has gone - and one retention pass per
  scope at a time. The specification checks that design.

- **A retention pass could plan from a stale listing (#5412, fixed).** The fix
  above, as first written, read a pass's catalog listing before it took the
  one-pass guard, so a pass could wait out another and then plan from a listing
  holding backups that pass had already pruned, and restore the catalog row of
  one whose data was gone. A pass now reads its listing only once it holds the
  guard. Found by the specification's independent review.
- **A capture of an unchanged tree could be deleted by a retention pass
  (#5418, fixed).** A full backup's id was the hash of its bytes alone, so a
  capture of a tree that had not changed republished an older backup's id. A
  retention pass that had already decided to prune the older backup then deleted
  what the new capture had just written, so the catalog could list a backup the
  storage no longer held, or lose the new backup outright. Every capture now gets
  its own backup id, derived from its own artifact, so no capture ever reuses
  another backup's id. Found by the specification's independent review.

## Which parts run in production code

The checks reach production through five extracted cores, each executed by the
code that runs and by a test that drives it:

- the cross-tree set's drain gate, re-check under the decision gate and
  post-capture re-observation, also driven by a Coyote
  model;
- the origin normalisation, per-origin high-water and consistency-cut frontier
  rules of every capture;
- how an incremental backup resolves the atomic batches in its window against
  the decision gate;
- the coordinated restore's single commit-or-abort decision
  (shared production/test decision logic), also driven by a Coyote model;
- retention pruning's keep set, base closure, dependent scan, tip-first delete
  order and an increment's publish check, also driven by a Coyote model.

Everything else the specifications describe - the decision gate's registry leases, routing,
redirects, the alias reservation, the write and receive fences - is mapped to
its production code and to the tests that pin it in each specification's
refinement note, not executed by the model itself.

## What is not covered

Coverage of one half must not be read as coverage of the other. Not covered:

- a replication batch delayed in flight across a whole restore cutover;
- reader-level atomicity across the members of a backup set while their aliases
  are swapped, which is not claimed anywhere;
- in-place and cold restores, which have no cutover;
- the restore-side behaviour of a resharded or resized tree, which belongs to
  the shard-ownership specification;
- an atomic batch in flight across a local cutover and its revert, which the
  shard-ownership specification's `ShardOwnershipCutover` module checks, and
  which found #4689 (fixed);
- the receiver side of cross-cluster atomic batches, which the atomic-commit
  cross-cluster module checks ([formal coverage](../lattice/formal-coverage.md));
- an operator's explicit backup delete, which removes the named backup whatever
  builds on it and is not coordinated with a retention pass running beside it,
  and catalog rebuild and scrub;
- retention over a sink shared between clusters.

Each refinement note under [`spec/backup/`](../../spec/backup/README.md) lists in
full what lies outside its scope, and any gap open against production.
