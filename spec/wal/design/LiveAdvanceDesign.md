# Live checkpoint advance: fenced redesign

Design for issues #5572 and #5593, epic #5348. This is a design draft for review: no
production code implements it yet. `WalLiveAdvance.tla` beside this note models it,
with its mutations in `mutations/`. Because a module under `spec/` must cite a
production detector on every behaviour row, this draft sits outside module
discovery. It becomes the companion module `spec/wal/WalLiveAdvance`, with a
refinement note, when the production members land.

**Gate.** No production member starts until the independent reviewer has accepted
this design.

## Why the first attempt was withdrawn

The first attempt (#5578, withdrawn from the bucket at `9a6c10813`) let a leaf
advance its projection checkpoint over the appends it had issued and applied itself,
without reading the records in between. Replay ownership is by key range and shard
map, not by author, so other writers append records a leaf owns. The advance passed
such records unread:

- **A duplicate activation.** A leaf's writes go to the WAL and never persist its
  row, so a stale duplicate keeps appending acknowledged writes without moving the
  row's ETag. The other activation passed them, persisted its checkpoint, captured
  into the shared per-leaf snapshot store and published a pin. The GC trimmed them.
  The born-at-admission rule (#5593) did not close this.
- **Foreign authors.** A split donor, a fold neighbour, an adaptive split's donor
  shard, a range delete (whose record carries the whole requested range and is
  replayed by every leaf), and the leaf a write reached before a bulk switch. An
  unacknowledged record from one of them was skipped by its owner but still applied
  by a WAL reader such as the replication shipper or a view.
- **The bulk interleave.** A batch write that landed on the old leaf during a bulk
  load or append became invisible on the bulk leaf that took over its range.

`mutations/NoTrimPastAckedLiveAdvance5578.mutation` reproduces that design in the
model below and fails "no trim past an acknowledged append".

## The design

A leaf may advance a partition's checkpoint over its own applied appends only when
it can prove exclusive authorship of every entry it owns in the window it passes.
Whenever it cannot, it falls back to the replay-based advance, which reads and
applies every entry it owns. Two fences supply the proof: an epoch fence against
other activations of the same leaf, and a hand-off fence against other writers.

### 1. The epoch fence: other activations of the leaf

**Claim eagerly, never lazily.** Before an activation's first append, it does two
things, in order:

1. It persists a new epoch to the leaf's row: the epoch it read plus one, with the
   ETag it read. A conflict means another activation claimed since this one read the
   row. This activation is stale: the write fails closed and the activation
   deactivates. The caller's retry reaches the live activation.
2. It claims that epoch at every WAL partition it writes (`ClaimAppendEpochAsync`
   on the WAL shard). The shard records the epoch in its per-leaf epoch table and
   returns its head. From then on it rejects any append for the leaf from an older
   epoch, as a failed write that is never acknowledged.

The activation is **armed** for the live advance on a partition only after it has
replayed to the head that partition's claim returned. Everything another activation
appended before the claim lies below that head and has been read; everything an
older activation tries to append after it is rejected; and a newer activation's
appends all lie above this activation's own.

A lazy fence, where the shard rejects an older epoch only once the newer one has
appended, is not enough. A stale duplicate can append between the newer
activation's claim and its first append, above the head it replays to.
`mutations/NoTrimPastAckedLazyFence.mutation` shows that.

**Durable state.** The leaf row gains an append epoch, a new `[Id]` on
`LeafNodeState`. The WAL shard's epoch table is durable per partition (leaf id to
epoch), written only by a claim: once per activation per partition it writes, never
per append. It is stored with the shard's other durable metadata, beside the move
fence (#4525).

**Moves.** A WAL shard move carries the epoch table with the stream; the
destination has the table before it accepts its first append. Model:
`StaleEpochNeverAppends`, broken by `mutations/StaleEpochNeverAppendsMoveDropsTable.mutation`.

**Trimming the table.** An entry is removed only when its leaf is retired:
- by the same fenced step that retires the leaf's materialiser pins, under the
  retirement token (#5358);
- or by a purge tail, under the lineage check (#5417).

An append from a leaf with no entry is rejected, so an activation that finds its
entry gone re-claims; a stale one cannot re-claim, because its row write conflicts.

A re-created leaf (a purge re-creation, or an id reused by a create) starts a new
row. Its first claim must therefore exceed any epoch still in the table. The claim
returns the table's epoch when the request is lower, and the leaf persists one above
it before claiming again. This is the step the reviewer should check first (open
question 1).

**Wire shape and cost.** The append request carries the leaf's identity and epoch
(16 bytes). The stored record does not change. The shard checks the epoch with one
dictionary lookup per append, under the append gate it already holds. Per
activation the fence costs one conditional row write, plus one claim call per
partition it writes. There is no new grain call and no allocation per write.

### 2. The hand-off fence: other writers

Every foreign-author path follows one protocol:

1. The initiator sets a **durable open marker** on the owner leaf before its first
   append of a record the owner will own.
2. It **closes** the marker only after its appends have drained: every issued
   append has returned, and it will append no more for that range. It records the
   WAL heads it captured after the drain.
3. The owner holds the live advance while any marker is open. It re-arms only after
   **replaying to the recorded heads**.

If the initiator crashes, the marker stays open until the initiator's recovery
re-drives the close. The cost is latency (the owner advances by replay), never data.

The marker is a set of open tokens plus per-partition closed heads, new `[Id]`
fields on `LeafNodeState`. Concurrent windows open distinct tokens; the closed heads
merge by maximum.

| Path | Open | Close | Drain |
|------|------|-------|-------|
| Split | In the sibling's birth persist (`InitializeSiblingAsync`) | By the donor at split completion, after it narrows its range and finishes its straggler sweep | The donor's split gate already serialises the narrowing against its own writes |
| Fold / reclaim | On the survivor before its range widens | After the neighbour's authoritative drain | The serial-turn guard (`ShardRootGrain.PointWriteQuiesce`) already drains point writes |
| Adaptive split | A shard-level marker on the target shard root, which the target's leaves read on their tick | After the donor shard is frozen and its shadow forwarding has drained | The donor's reject phase |
| Range delete | No fence: the record is clipped to the authoring leaf's range | - | - |
| Bulk load / append | At the leaf's birth (`SiblingInitialization`), with the birth hint below | By the shard root after the switch | Point writes are excluded by the serial-turn guard; batch writes admitted before the switch are drained with a generation counter |

Notes on the table:
- **Adaptive split.** The marker is shard-level because the moved slots scatter
  across every target leaf, so marking each one would cost a call per leaf.
- **Range delete.** A clipped record applies on the authoring leaf exactly as
  before, and the union of the clips is the requested range. Replay keeps applying
  older, unclipped records unconditionally.
- **Bulk drain.** The generation counter extends `_interleavedBatchWritesInFlight`
  in `ShardRootGrain.PointWriteQuiesce`, so the drain waits only for batches
  admitted before the switch.
- **Saga terminals** keep the leaf-wide prepare clamp (#5572): the advance holds on
  every partition while any prepare is pending.

### 3. The bulk birth hint

A bulk load or append captures the per-partition WAL heads when it begins, before
any write it can race. The leaves it creates take those heads as their birth hint:
the checkpoint they start from, as a split sibling starts from its donor's heads
(`WalHeadsAtBirth`). With the open marker, re-arming then means a short, bounded
replay from the hint to the close heads. That replay imports any write that reached
the old leaf in the window, before the advance proceeds as for any leaf.

Bulk leaves also get the key ranges `PlanBulkLoadLeaves` already computes. A
rangeless bulk leaf owns every key of its shard during replay, which duplicates
sibling bulk leaves' rows into it.

`mutations/ReadPositionHonestBulkWithoutHint.mutation` starts the leaf at the head
at its creation instead, and passes the interleaved entries.

### 4. Fail safe everywhere

The leaf takes the replay-based advance whenever it cannot prove exclusive
authorship:
- no claim yet on the partition, or the claim failed or conflicted;
- a hand-off marker is open, or it has not replayed to the claim head or the last
  close's heads;
- a saga prepare is pending;
- the partition is latched stale;
- an own append failed (the tracker is off for the activation);
- the cluster has not enabled fencing.

**Rollout.** Fencing and the live advance are enabled only once every silo
supports the claim, the epoch-checked append and the marker: a cluster capability
gate, the same shape as other wire-format rollouts. Until then nothing claims, the
shards keep no table and every advance is replay-based. A mixed-version cluster
therefore never runs the advance.

## The model

`WalLiveAdvance.tla` covers:
- one WAL partition of three offsets, and one owner leaf;
- its born activation `a1`, created by a bulk load, and a duplicate `a2` that also
  stands for the next activation after a stop (both read the row and must claim);
- a foreign author with two hand-off windows: the bulk switch and one more;
- a WAL shard move;
- a GC trimming below the shared snapshot's coverage.

Only entries the leaf owns are modelled; entries other leaves own are invisible to
its replay. It reuses `WalDurability`'s abstractions: the read position as the
checkpoint, a snapshot that keeps the highest coverage, and a pin at that coverage.

**Properties:**
- `ReadPositionHonest`: no running activation's read position passes a durable entry
  it does not hold. Unacknowledged entries count, because a WAL reader applies them.
- `SnapshotHonest`.
- `NoTrimPastAcked`: no trim past an acknowledged append, whoever appended it.
- `StaleEpochNeverAppends`: no append from an epoch below the highest claimed is
  ever accepted, across a move too.

**Checked locally** (tla2tools v1.7.4, 2 workers): 2,991,924 distinct states,
depth 33, no error, in about 45 seconds.

The first instance, four offsets and a third activation for the reactivation,
passed 5.6 million distinct states in its first minute with 2.3 million queued, so it
was cut. The longest counterexample below needs two offsets, and a reactivation
behaves as a duplicate: it reads the row and must claim.

### Mutations: every guard must break

Each mutation was checked against its target alone. The clean arm (the unmutated
module with the same configuration) held in every case.

| Mutation | Removes | Target | Result |
|----------|---------|--------|--------|
| `NoTrimPastAckedLiveAdvance5578` | Everything: #5578's arming rule (born, or within one of the first own append), no claim, no epoch check, no hand-off fence | `NoTrimPastAcked` | Violated (9 states) |
| `NoTrimPastAckedNoEpochCheck` | The shard's epoch check | `NoTrimPastAcked` | Violated |
| `NoTrimPastAckedLazyFence` | The claim raising the fence; the first append raises it instead | `NoTrimPastAcked` | Violated |
| `ReadPositionHonestCloseBeforeDrain` | The drain before close | `ReadPositionHonest` | Violated |
| `ReadPositionHonestRearmWithoutReplay` | The replay to the close heads | `ReadPositionHonest` | Violated |
| `ReadPositionHonestBulkWithoutHint` | The birth hint | `ReadPositionHonest` | Violated |
| `StaleEpochNeverAppendsMoveDropsTable` | The epoch table on a move | `StaleEpochNeverAppends` | Violated |

**#5578's counterexample.** `NoTrimPastAckedLiveAdvance5578` reaches it in nine
states:

1. `Init`
2. `Begin`
3. `Create`: `a1` is born.
4. `Duplicate`: `a2` activates beside it.
5. `OwnAppend(a2)`: offset 0, acknowledged.
6. `OwnAppend(a1)`: offset 1.
7. `LiveAdvance(a1)`: born, so the read position moves to 1, past offset 0, which
   `a1` never read.
8. `Capture(a1)`: rows `{1}`, coverage 1.
9. `GcTrim`: offset 0, acknowledged, is trimmed, and no snapshot holds it.

### Reachability: the clean run is not vacuous

Each witness is an invariant that must be violated, proving its step is reachable
in this instance:

| Witness | Shortest trace |
|---------|----------------|
| A stale epoch's append is reached and rejected | `ShardMove`, `Begin`, `Create`, `Claim(a1)`, `Duplicate`, `Claim(a2)`, `StaleAppend(a1)`: after a move too |
| A duplicate activation appends | `Begin`, `Create`, `Duplicate`, `Claim(a2)`, `OwnAppend(a2)` |
| A foreign append during an open hand-off | `Begin`, `ForeignIssue`, `ForeignAlloc` |
| The live advance happens at all | `Begin`, `Create`, `Claim(a1)`, `Close`, `OwnAppend(a1)`, `LiveAdvance(a1)` |
| The live advance resumes past a foreign entry once its hand-off closed and was replayed | `Begin`, `ForeignIssue`, `Create`, `ForeignAlloc`, `Claim(a1)`, `Land`, `Close`, `OwnAppend(a1)`, `Replay(a1)`, `LiveAdvance(a1)` |

Run a witness with a configuration that checks `TypeOK` and the witness alone, for
example `INVARIANTS TypeOK WitnessStaleAppendRejected`.

## Formal impact

Every existing module, how the design affects it, and why:

| Module(s) | Effect | Reason |
|-----------|--------|--------|
| `WalDurability` | Unchanged; gains a gap entry | Its subject is the replay-based advance, which the design keeps as the fallback. Its single owner activation is always the current epoch, so `Append` never meets a rejection. Extending it would add an activation dimension to every leaf: `SnapshotLoss` is already 17,428,938 states and `TwoFaults` 2,958,083, so the fences go in the companion instead. |
| `WalMove` | Gains the epoch table | The table moves with the stream. Its invariant "a stale epoch never appends after a move" and the mutation that drops the table are `StaleEpochNeverAppends` and `StaleEpochNeverAppendsMoveDropsTable` here, and move into `WalMove` with the production member. |
| `WalPinRetirement` | New rows | A re-created leaf takes a new epoch, above the table's. The table entry is removed by the fenced retirement step under the retirement token, so a removal cannot race a re-created leaf's claim. |
| `TreeLifecycle`, `TreeLifecycleAdmission` | New rows | The purge tail removes the table entries of the leaves it clears, under the lineage check (#5417). |
| `ShardOwnership`, `ShardOwnershipCutover`, `ShardOwnershipFold`, `ShardOwnershipCrdt`, `ShardOwnershipRetention` | New rows or gaps | The adaptive split's shard-level marker opens and closes around the shadow window, and the fold's around its drain. A marker only holds the live advance and never blocks a move, a cutover or a fold, so ownership safety is unchanged. |
| `SplitLink`, `SplitLinkTurns`, `BPlusTopology`, `BPlusReclaimRecovery`, `SplitGateCycles`, `BPlusCascade` | New rows or gaps | The split opens the sibling's marker in its birth persist and the donor closes it at completion; the reclaim opens the survivor's before widening. No new gate or wait-for edge: the close is one more call from the donor's completion. |
| `BackupRestore`, the bulk-load paths | Rows | A restore and an offline snapshot load through `BulkLoadRawAsync`, so their leaves take the birth hint and the open marker. |
| `ReplayPermits`, `WalGcStarvationDrive`, `WalGcStarvationDriveWitness`, `MaterialiserDrain` | Demand abstraction only | The design lowers replay demand. These modules bound permits and drives for any demand, so their properties are unaffected; their notes gain a line that live-advanced leaves no longer drive. |
| `WalAdmission`, `WalBackPressure` | Gap entry | A stale-epoch rejection is a new append failure, distinct from admission refusal under load. It fails one stale writer, never a saturated partition, so the admission and back-pressure properties are unaffected. |
| `AtomicCommit`, `AtomicCommitCrossCluster` | Unchanged | A prepare whose append is rejected as stale is a nack, which `PrepareTx` already permits (it chooses ack or nack per key). A terminal applied through a stale activation fails and the broadcast retries; `BroadcastStep` assumes only eventual delivery. Saga terminals are appended by coordinators, which hold no leaf epoch. |
| `SagaReadProgress`, `TurnOccupancy`, `TagIndexReconcile`, `ReplicationShipping` | Unaffected | They consume accepted WAL records or schedule turns; a rejected append never becomes a record. |
| `AntiEntropy`; `Replication`, `ReplicationCausalDelivery`, `ReplicationLowWatermark`, `ReplicationReBootstrap` | Unaffected | They consume accepted WAL records only. |
| `MaterialisedView*`, `view-*` modules | Unaffected | Views read accepted WAL records only. |
| `AliasRevert`, `AliasRouting`; `Schema*`, `SchemaViewComposition`; `TenantIsolation`, `TenantFootprint`, `TenantQuotaEvaluation`, `TenantTreeCount`, `TenantRateBudget` | Unaffected | Routing, schema and tenancy decisions are made before a write reaches a leaf. |
| `BackupCapture`, `BackupCutover`, `BackupIncremental`, `BackupProvenance`, `BackupRetention` | Unaffected | They read accepted records and leaf state. |

## Production members

Each production member carries detectors proven red, and they land in this order:

1. **Epoch claim and the epoch-checked append.**
   - Leaf row epoch, `ClaimAppendEpochAsync`, the durable shard table and its
     trimming, carried by a move.
   - Model: `WalLiveAdvance` and the `WalMove` rows become real.
2. **Hand-off markers.** Split, fold and reclaim, the adaptive split's shard-level
   marker, and range-delete clipping.
3. **Bulk.** The birth hint, bulk-leaf ranges, the shard-root batch drain and the
   marker.
4. **The live advance itself.** Behind the capability gate, armed only by the rules
   above. It reuses #5578's tracker and the core extraction from the abandoned #5596.

## Open questions for the reviewer

1. **Re-created leaves.** Should a re-created leaf's first claim exceed the table's
   epoch through a returned floor, as proposed? Or should the purge tail's removal of
   the entry be the only path, leaving no table entry to exceed?
2. **Adaptive split marker.** A shard-level marker costs every target leaf its
   advance for the window. A per-leaf marker costs one call per target leaf. The
   shard-level marker is proposed.
3. **Range-delete clipping.** Clipping changes the shipped record. A peer then
   receives the union of the clips; under a partial fan-out that union is exactly
   what was applied locally, which is the agreement this design wants. Is the shape
   change acceptable for the replication wire contract?
