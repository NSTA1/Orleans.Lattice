# Live checkpoint advance: fenced redesign

Design for issues #5572 and #5593, epic #5605. This is a design draft for review:
no production code implements it yet. `WalLiveAdvance.tla` beside this note models
it, with variant configurations and its mutations in `mutations/`. A module under
`spec/` must cite a production detector on every behaviour row, so this draft sits
outside module discovery. It becomes the companion module `spec/wal/WalLiveAdvance`,
with a refinement note, when the production members land.

**Gate.** No production member starts until the independent reviewer has accepted
this design.

**Revision.** This is the second revision. It answers the
[design review](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6558718267)
(2 blocking, 9 should-fix, 5 nits). The last section maps every finding to its
resolution.

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
- **Foreign authors.** A split donor, a reclaim neighbour, an adaptive split's or a
  shard fold's donor shard, a range delete (whose record carries the whole requested
  range and is replayed by every leaf), and the leaf a write reached before a bulk
  switch. An unacknowledged record from one of them was skipped by its owner but
  still applied by a WAL reader such as the replication shipper or a view.
- **The bulk interleave.** A batch write that landed on the old leaf during a bulk
  load or append became invisible on the bulk leaf that took over its range.

`mutations/NoTrimPastAckedLiveAdvance5578.mutation` reproduces that design in the
model and fails "no trim past an acknowledged append" (trace below).

## The design

A leaf may advance a partition's checkpoint over its own applied appends only when
it can prove exclusive authorship of every entry it owns in the window it passes.
Whenever it cannot, it falls back to the replay-based advance, which reads and
applies every entry it owns. Two fences supply the proof: an epoch fence against
other activations of the same leaf, and a hand-off fence against other writers.
Both are enforced at one seam: the WAL shard's offset assignment, under the lock
(`_stateGate`) that already makes the move fence (#4525) and the clock floor (#4586)
atomic with assigning an offset.

### 1. The epoch fence: other activations of the leaf

**Claim eagerly, never lazily.** Before an activation's first append, it claims an
epoch:

1. **Persist.** It writes the epoch it read plus one to its row as a strict
   conditional write on the ETag it read. The write never goes through
   `TopologySeedPersist.WriteAdoptingBenignFirstCreateRaceAsync`, which adopts the
   winner's row on an empty-ETag conflict, the shape of #5593: every conflict,
   empty-ETag included, is a conflict. On a conflict it re-reads the row. If the
   epoch is unchanged, the conflicting write was an ordinary row write, and it
   retries. If the epoch moved, another activation claimed: this one is stale, so
   the write fails closed and the activation deactivates. The caller's retry
   reaches the live activation.
2. **Claim.** It claims that epoch at every WAL partition it writes
   (`ClaimAppendEpochAsync`). The shard first writes the leaf's durable entry, then,
   under `_stateGate`, raises the in-memory entry and reads `_nextOffset`, the
   allocator's next offset, not the readable head (`DurableContiguousTailOffset`). It
   returns that offset as the claim head. Anything allocated before the raise lies
   below the claim head; anything after it is checked against the raised entry.
   Persisting before raising means a restart can never lose a raise whose head was
   already returned.
3. **Refusal.** A claim at or below the entry is refused as stale. A claim into an
   absent entry must exceed the partition's retired floor (below); otherwise the
   shard refuses it and returns the floor, and the leaf persists the floor plus one,
   conditionally, and claims again.

**The append check.** Under the same lock section that assigns the offset, the shard
accepts an append only if the leaf's entry is loaded and the append's epoch is not
below it. A stale epoch is rejected with `WalAppendEpochStaleException`. An entry not
yet loaded rejects the append as unloaded (below); the leaf re-presents its claim
and retries once.

**Arming.** The activation is armed for the live advance on a partition only when
it has replayed to that partition's claim head and every append it has issued there
has been applied (the applied-prefix tracker). The hand-off conditions in section 2
also apply. Everything another activation appended before the claim lies below the
claim head and has been read. Everything an older activation tries to append after
it is rejected. A newer activation's appends all lie above this activation's own.

A lazy fence, where the shard rejects an older epoch only once the newer one has
appended, is not enough: a stale duplicate can append between the newer
activation's claim and its first append (`NoTrimPastAckedLazyFence`).

**Durable state and storage.**
- **Leaf row:** an append epoch, a new `[Id]` on `LeafNodeState`.
- **Entries:** one row per (partition, leaf), written through the tree's grain
  storage provider under a reserved grain type `_lattice_wal-append-epoch`, keyed by
  the WAL shard key and the leaf id. `GrainStorageFencingProbe` is the precedent for
  rows under a reserved type. Entries are not kept in the shard's own state row
  (`WalShardFloorState`), because a claim would then rewrite every leaf's entry.
- **Retired floor:** one scalar per partition, a new `[Id]` on `WalShardFloorState`,
  loaded with the clock floor at activation.
- **In memory:** a dictionary of the entries this shard activation has loaded.
  Entries load lazily: an append from a leaf whose entry is not loaded is rejected as
  unloaded, and the leaf's re-presented claim (same epoch) loads it. No storage read
  ever happens on the append path or under the lock.

**Retiring entries: a monotone floor.** An entry is retired only when its leaf is
retired: by the step that retires the leaf's materialiser pins, under the retirement
token (#5358), or by a purge tail, under the lineage check (#5417). Retiring raises
the partition's retired floor to at least the entry's epoch, durably, before the
entry row is deleted. No removal ever lowers the floor, so the entries stay bounded
by live leaves.

A re-created leaf (a purge re-creation, or an id reused by a create) starts a new
row at epoch 0. Its first claim lands in an absent entry, so it must exceed the
floor, and therefore every epoch a surviving duplicate of the old incarnation holds.
`ReadPositionHonestRecreateDropsFloor` removes the floor raise and fails.

**Moves and restarts.** Entries and the floor live in grain storage, not in the WAL
stream, so a WAL move, which switches the stream's provider, does not touch them. A
restart of the shard grain, including the reactivation at a move's cutover, loses
only the in-memory entries. Those reload through re-presented claims, and nothing is
accepted from an unloaded entry. The model's `Restart` and `Reload` actions cover
this. `StaleEpochNeverAppendsUnloadedEntryAccepted` accepts an append from an
unloaded entry and fails. The first revision's `StaleEpochNeverAppendsMoveDropsTable`
is retired: it modelled a hazard that exists only if the table were stored with the
stream.

**Cost.**
- **Per append:** one dictionary lookup and one comparison under the lock the append
  already holds. No allocation and no grain call. The request carries the leaf
  identity and epoch, plus the shard routing generation (section 2): 24 bytes. The
  stored record is unchanged.
- **Per activation:** one conditional row write, plus one claim call and one entry-row
  write per partition it writes. A fresh estate of N leaves writing P partitions
  costs at most N x P entry-row writes once, not O(N^2).
- **Per shard restart:** one extra claim round trip for each leaf that appends
  afterwards.
- **Storage:** one entry row per (partition, live leaf that wrote it), and one scalar
  per partition.
- **Memory:** about 100 bytes per leaf that has appended to the partition since the
  shard activated.

### 2. The hand-off fence: other writers

Every foreign-author path follows one protocol:

1. **Open.** The initiator sets a durable open marker on the owner before its first
   admission of a write the owner will own.
2. **Close by fencing.** In order, the initiator:
   - (a) stops admitting writes for the range;
   - (b) may wait a bounded time for writes it admitted earlier to return. This only
     reduces spurious rejections in (c);
   - (c) re-claims its own epoch (a leaf initiator) or its routing generation (a
     shard-level initiator) at every partition it may have written. The re-claim
     returns allocator heads, exactly as a claim does;
   - (d) closes the marker, recording those heads.
3. **Re-arm by replay.** The owner holds the live advance while any marker is open.
   It re-arms only after replaying to the recorded heads.

**Why the drain is a fence and not a wait.** Every append carries the epoch or
generation its write was admitted under. So after (c), a write admitted before the
close either was allocated already, and lies below the recorded heads, or will be
rejected when it arrives.

The review proposed bumping an admission generation and waiting for every write
admitted under the old one to return. That is unsound on its own: a write's call can
return by timeout while its append is still delivered and allocated later. In the
model, waiting for returns is therefore equivalent to not waiting
(`ReadPositionHonestDrainWithoutFence`).

The design no longer relies on any gate draining anything. The review found three
such assumptions false: split commits never take `_splitGate`; the adaptive split's
reject phase only flips a phase; and the fold's serial-turn guard excludes point
writes and only counts batch writes.

**Stamped at admission.** The stamp is taken where the write is admitted, not where
the append is built. A leaf captures its epoch in the same synchronous step as span
admission. A shard root captures its routing generation in the same step as its
phase check (`ThrowIfRejectedForKey`) and passes it to the leaf call. If the append
read the current epoch instead, a write admitted before the close would carry the
re-claimed epoch and be accepted (`ReadPositionHonestStampAtAppend`).

**Every append path is stamped.** Point, batch, CRDT apply, range delete, merge,
relocation and saga prepare all carry the stamps. A path that bypasses the stamp is
exactly the gap the fold's point-only guard has today (`ReadPositionHonestDrainPointOnly`).
Production member 2 carries a census of every append path into a leaf, with a
detector per path.

**Crash and cost.** If the initiator crashes, the marker stays open until its
recovery re-drives the close. The cost is latency (the owner advances by replay),
never data. A write admitted before the close and not yet allocated at (c) fails
unacknowledged, and the caller retries through the same path as a stale-epoch
rejection. That happens only during a close.

**Marker storage.** The leaf marker is a set of open tokens plus per-partition closed
heads, new `[Id]` fields on `LeafNodeState`. Concurrent windows open distinct tokens,
and closed heads merge by maximum. The shard-level marker is the same shape on
`ShardRootState`.

| Path | Initiator | Open | Admission stops at | Fence at close |
|------|-----------|------|--------------------|----------------|
| Split | Donor leaf | In the sibling's birth persist (`InitializeSiblingAsync`) | The donor's narrow of `HighKeyExclusive`; span admission forwards moved keys afterwards | The donor re-claims its epoch at every partition it writes; the heads close the sibling's marker |
| Leaf reclaim | Absorbed neighbour leaf | On the survivor, before its range widens | The neighbour's unlink | The neighbour's entry is retired (the floor rises), so its later appends are rejected; the heads are captured in the retire step |
| Adaptive split | Donor shard root | Shard-level marker on the target shard root | The reject phase | The donor shard root re-claims its routing generation at every partition; the heads close the marker at completion |
| Shard fold | Folded shard root | Shard-level marker on the survivor shard root | The fold's quiesce, point and batch | The same routing-generation re-claim |
| Range delete | - | No fence: the record is clipped to the authoring leaf's range | - | - |
| Bulk load / append | The shard root, for writes admitted before the switch | At the bulk leaf's birth (`SiblingInitialization`), with the birth hint | The switch, in the shard root's serial turn | The shard root re-claims its routing generation at every partition; the heads close the marker |

**Shard-level marker.** The moved slots of an adaptive split or a shard fold scatter
across every target leaf, so a per-leaf marker would cost one call per leaf. A target
leaf reads the shard-level marker **fresh**, after the last own append it would
advance over, never from an earlier tick's cache. It reads it through an interleaving,
read-only shard-root call, `GetHandOffMarkerAsync`, which returns the marker's
generation and close heads. Read-only is load-bearing: this is a new leaf-to-shard-root
wait-for edge, and a serial shard-root turn that calls the leaf must not be able to
deadlock against it. The read costs one call per advance tick, not per write.

**Range delete: clipping (accepted in review).** The authoring leaf clips the record
to its own range, and the receiver applies each record's range (`ReplicationApplier`
bypasses dedupe for `DeleteRange`), so N clipped records reproduce their union. An
older peer applies a narrower range correctly. Under a partial fan-out, the union is
what was applied locally. The record's shape changes for every consumer, so it is a
documented contract change for:
- the state-change feed (`LatticeStateObserver`);
- history views (one range-tombstone row per clip);
- aggregation and predicate views (narrower `RangeReconcile` signals);
- `EntryHistoryReader`.

`MatchedKeys` and the bounds are clipped consistently. The fan-out walks the leaf
chain, so a split racing the delete cannot leave a narrowed leaf's clip uncovered.
Replay keeps applying older, unclipped records unconditionally.

**Saga terminals** keep the leaf-wide prepare clamp (#5572): the advance holds on
every partition while any prepare is pending.

### 3. The bulk birth hint

**Prerequisite: #5610.** Bulk append today grafts leaves without checking the keys
the rightmost leaf already holds. The hint below assumes the bulk range starts
empty, so #5610's fix lands first, in production member 3.

A bulk load or append captures the per-partition WAL heads when it begins, before any
write it can race. It does so in the same serial shard-root turn as the emptiness
check: `EnsureEmptyForBulkLoadAsync` for a load, and #5610's check for an append. The
leaves it creates take those heads as their birth hint. The hint is the read position
they start from, as a split sibling starts from its donor's heads (`WalHeadsAtBirth`).

With the open marker, re-arming then means a short, bounded replay from the hint to
the close heads of the switch's fence. That replay imports any write that reached the
old leaf in the window, before the advance proceeds as for any leaf. The hint is
needed by every arming, not only by the live advance: it is the leaf's birth read
position. Starting the leaf at the head when it is created instead fails without any
live advance (`ReadPositionHonestBulkWithoutHint`), and so does skipping the
emptiness check (`ReadPositionHonestBulkAppendNoEmptinessCheck`).

Bulk leaves also get the key ranges `PlanBulkLoadLeaves` already computes. A rangeless
bulk leaf owns every key of its shard during replay, which duplicates sibling bulk
leaves' rows into it.

### 4. Fail safe everywhere

The leaf takes the replay-based advance on a partition whenever it cannot prove
exclusive authorship:
- **Its own claim.** No claim yet on the partition, or the claim failed, conflicted
  or was refused.
- **Replay.** It has not replayed to the claim head or to the last close's heads.
- **Hand-offs.** A leaf marker is open, or the shard-level marker read is open,
  failed, or not fresh.
- **Its own writes.** An own append is unapplied, or an own append failed (the
  tracker is off for the activation).
- **Sagas.** A saga prepare is pending.
- **Partition state.** The partition is latched stale.
- **Overlap cannot be excluded.** Another live leaf may own the same keys:
  - `ShardIndex` is null (a slot-less legacy leaf owns every shard's entries in
    replay);
  - the leaf is rangeless but has siblings (a legacy bulk leaf: "Bulk-load leaves
    carry no ownership range");
  - the leaf is suspected to be an orphan: the scan chain flagged it, orphan repair
    is pending for it, or it is not reachable by descent. An orphan sharing a live
    leaf's bounds materialises a full copy of that leaf's range.
- **Storage.** The grain storage provider does not enforce ETags
  (`GrainStorageFencingProbe` reports so). The claim's conditional write proves
  nothing without them.
- **Rollout.** The cluster's fencing gate is not open.

**Rollout and downgrade.** The claim, the stamped append and the markers are gated by
a cluster capability gate, the same shape as other wire-format rollouts. The gate
opens only once every silo supports them, and it is ratcheted:
- **Open.** Once open, it stays open. A WAL shard then rejects an unstamped append to
  a fenced partition, so an old or rolled-back silo's writes fail closed rather than
  slipping past an armed leaf.
- **Downgrade.** A downgrade requires closing the gate first, and the runbook says
  so. Closing it disarms every leaf, and every advance becomes replay-based.
- **Re-enable.** Re-opening it forces every activation to claim afresh and replay to
  the new claim heads before it arms.

Until the gate opens nothing claims, the shards keep no entries, and every advance is
replay-based. A mixed-version cluster therefore never runs the advance.

**Where a rejection surfaces.** `WalAppendEpochStaleException` derives directly from
`System.Exception` and carries a stable alias, so it needs no same-silo copier. It
follows the precedent of `WalStampBelowFloorException`, the clock floor's append
refusal (#4586). The WAL shard throws it under the lock, before any offset is
assigned. Every caller treats it as not acknowledged and retryable:
- the leaf maps it in its append paths, beside `AbsorbClockFloorRefusalAndRethrow`:
  it deactivates the stale activation and rethrows;
- `LatticeGrain` retries as it does for the clock floor, reaching the live
  activation.

The unloaded-entry rejection is absorbed inside the leaf by re-presenting the claim
and retrying once.

## The model

`WalLiveAdvance.tla` covers:
- one WAL partition of three offsets, and one owner leaf;
- its born activation `a1`, created by a bulk load, and a duplicate `a2` that also
  stands for the next activation after a stop (both read the row and must claim);
- a foreign author `f` with point and batch writes and two hand-off windows: the bulk
  switch and one more. `f`'s epoch stands for either a leaf initiator's epoch or a
  shard-level initiator's routing generation; their fences have the same algebra;
- a pre-existing writer `p` for the bulk append's emptiness check (#5610);
- a GC trimming below the shared snapshot's coverage.

Only entries the leaf owns are modelled; entries other leaves own are invisible to
its replay. It reuses `WalDurability`'s abstractions: the read position as the
checkpoint, a snapshot that keeps the highest coverage, and a pin at that coverage.

**Own appends are two steps.** `OwnAlloc` is the shard's epoch check and offset
assignment under its lock. `Land` makes the append durable and, if its author is
still running, applies and acknowledges it. A foreign write is three: `ForeignAdmit`
(stamped with the epoch it is admitted under), then `ForeignAlloc` or `ForeignReject`
at the shard, then `Land`.

**Abstractions in the claim.**
- `Claim` folds the persist, the shard claim and the floor-refusal retry into one
  step. Claims at or below the entry are refused, so the order in which two
  activations' claims reach the shard does not matter.
- The model compares epochs where production compares ETags. A production conflict
  whose re-read finds the epoch unchanged is retried, so the two agree.
- The claim persists the epoch it read plus one, raised above the floor, so dropping
  the conditional write is observable (`ReadPositionHonestClaimAdoptsNoCas`).

The redundant `epoch > 0` arming guard of the first revision is removed: `mine[a]` is
non-empty only after a claim.

**Feature switches** keep each check small:
- `WithRecreate`: a purge and re-creation of the leaf that retires its entry.
- `WithRestart`: a WAL shard restart that loses the in-memory entries.
- `FrozenView`: a superseded activation keeps the marker state it last saw. This
  matches production's stale in-memory row and is weaker than one global marker.
- `Ghosts`: enables the witness ghosts. Safety runs keep it off.

**Properties:**
- `ReadPositionHonest`: no running activation's read position passes a durable entry
  it does not hold. Unacknowledged entries count, because a WAL reader applies them.
- `SnapshotHonest`.
- `NoTrimPastAcked`: no trim past an acknowledged append, whoever appended it.
- `StaleEpochNeverAppends`: no append from an epoch below the highest claimed is ever
  accepted, across a shard restart too.

**Checked locally** (tla2tools v1.7.4, `-workers 2`, safety only, all four
properties):

| Configuration | Distinct states | Depth | Time |
|---------------|-----------------|-------|------|
| `WalLiveAdvance.cfg` (base) | 2,314,036 | 31 | 43 s |
| `WalLiveAdvanceRecreate.cfg` | 8,907,861 | 32 | 161 s |
| `WalLiveAdvanceRestart.cfg` | 6,942,108 | 32 | 129 s |
| `WalLiveAdvanceFrozenView.cfg` | 3,704,430 | 31 | 60 s |

### Mutations: every guard must break

Each mutation was checked against its target alone, and the clean arm (the unmutated
module with the same constants) held every time. Trace lengths are the shortest
counterexample, from a single-worker run.

| Mutation | Removes | Target | Trace |
|----------|---------|--------|-------|
| `NoTrimPastAckedLiveAdvance5578` | Everything: #5578's arming rule (born, or within one of the first own append), no claim, no epoch check, no hand-off fence | `NoTrimPastAcked` | 11 states |
| `NoTrimPastAckedNoEpochCheck` | The shard's epoch check | `NoTrimPastAcked` | 14 |
| `NoTrimPastAckedLazyFence` | The claim raising the entry; the first append raises it instead | `NoTrimPastAcked` | 14 |
| `ReadPositionHonestClaimAdoptsNoCas` | The claim's conditional write (#5593's adopt) | `ReadPositionHonest` | 12 |
| `ReadPositionHonestClaimReadableHead` | The allocator head: the claim returns the readable head | `ReadPositionHonest` | 12 |
| `ReadPositionHonestNoClaimHeadReplay` | The replay to the claim head | `ReadPositionHonest` | 12 |
| `SnapshotHonestNoOwnTracker` | "Every own append applied" | `SnapshotHonest` | 11 |
| `ReadPositionHonestNoOpenCheck` | The open-marker guard | `ReadPositionHonest` | 10 |
| `ReadPositionHonestRearmWithoutReplay` | The replay to the close heads | `ReadPositionHonest` | 11 |
| `ReadPositionHonestDrainWithoutFence` | The close's fence; waiting for returns is equivalent | `ReadPositionHonest` | 11 |
| `ReadPositionHonestDrainPointOnly` | The stamp on batch appends, so the fence covers point writes only | `ReadPositionHonest` | 11 |
| `ReadPositionHonestStampAtAppend` | The stamp at admission; the append carries the current epoch | `ReadPositionHonest` | 11 |
| `ReadPositionHonestBulkWithoutHint` | The birth hint | `ReadPositionHonest` | 6 |
| `ReadPositionHonestBulkAppendNoEmptinessCheck` | The bulk append's emptiness check (#5610) | `ReadPositionHonest` | 4 |
| `ReadPositionHonestRecreateDropsFloor` | The retired floor's raise (`WithRecreate`) | `ReadPositionHonest` | 13 |
| `StaleEpochNeverAppendsUnloadedEntryAccepted` | The unloaded-entry rejection (`WithRestart`) | `StaleEpochNeverAppends` | 8 |

**#5578's counterexample.** `NoTrimPastAckedLiveAdvance5578` reaches it in eleven
states:

1. `Init`
2. `Begin`
3. `Create`: `a1` is born.
4. `Duplicate`: `a2` activates beside it.
5. `OwnAlloc(a2)`: offset 0 is assigned to `a2`.
6. `OwnAlloc(a1)`: offset 1 is assigned to `a1`.
7. `Land(0)`: offset 0 is durable; `a2` applies and acknowledges it.
8. `Land(1)`: offset 1 is durable; `a1` applies and acknowledges it.
9. `LiveAdvance(a1)`: `a1` is born, so its read position moves to 1, past offset 0,
   which it never read.
10. `Capture(a1)`: the snapshot's rows are `{1}`, with coverage 1.
11. `GcTrim`: offset 0, acknowledged, is trimmed, and no snapshot holds it.

### Reachability: the clean run is not vacuous

Each witness is an invariant that must be violated, with `Ghosts = TRUE`, proving its
step is reachable in the instance:

| Witness | Shortest trace |
|---------|----------------|
| A stale epoch's append is reached and rejected | `Begin`, `Create`, `Claim(a1)`, `Duplicate`, `Claim(a2)`, `StaleAppend(a1)` |
| A duplicate activation appends | `Begin`, `Create`, `Duplicate`, `Claim(a2)`, `OwnAlloc(a2)` |
| A foreign append during an open hand-off | `Begin`, `ForeignAdmit`, `ForeignAlloc` |
| A write admitted before a close is fenced out | `Begin`, `Create`, `ForeignAdmit`, `Close`, `ForeignReject` |
| The live advance happens at all | `Begin`, `Create`, `Claim(a1)`, `Close`, `OwnAlloc`, `Land`, `LiveAdvance(a1)` |
| The live advance resumes past a foreign entry once its hand-off closed and was replayed | `Begin`, `Create`, `Claim(a1)`, `ForeignAdmit`, `ForeignAlloc`, `Land`, `Close`, `OwnAlloc`, `Land`, `Replay(a1)`, `LiveAdvance(a1)` |
| The duplicate advances live | `Begin`, `Create`, `Close`, `Duplicate`, `Claim(a2)`, `OwnAlloc`, `Land`, `LiveAdvance(a2)` |
| The GC trims after a live advance | `Begin`, `Create`, `Claim(a1)`, `Close`, `OwnAlloc`, `Land`, `LiveAdvance(a1)`, `Capture`, `GcTrim` |
| A claimed entry is unloaded and reloaded (`WithRestart`) | `Begin`, `Create`, `Claim(a1)`, `Restart`, `Reload` |
| A claim is raised by the retired floor (`WithRecreate`) | `Begin`, `Create`, `Claim(a1)`, `Recreate`, `Duplicate`, `Claim(a2)` |

To run a witness, use a configuration with `Ghosts = TRUE` that checks `TypeOK` and
the witness alone, for example `INVARIANTS TypeOK WitnessStaleAppendRejected`.

## Formal impact

Every existing module, how the design affects it, and why:

| Module(s) | Effect | Reason |
|-----------|--------|--------|
| `WalDurability` | Unchanged; gains a gap entry | Its subject is the replay-based advance, which the design keeps as the fallback. Its single owner activation is always the current epoch, so `Append` never meets a rejection. Extending it would add an activation dimension to every leaf: `SnapshotLoss` is already 17,428,938 states and `TwoFaults` 2,958,083, so the fences go in the companion instead. |
| `WalMove` | Refinement row only | The entries and the retired floor live in grain storage, not in the stream a move switches, so a move carries nothing new. The cutover's reactivation of the shard grain is `Restart` here, and "no stale epoch appends across it" is `StaleEpochNeverAppends`, broken by `StaleEpochNeverAppendsUnloadedEntryAccepted`. |
| `WalPinRetirement` | New rows | Retiring a leaf's pins also retires its epoch entries under the same retirement token, raising the floor first. A re-created leaf's claim exceeds the floor, so a removal cannot let a surviving duplicate outrank it. |
| `TreeLifecycle`, `TreeLifecycleAdmission` | New rows | The purge tail retires the entries of the leaves it clears, under the lineage check (#5417), raising the floor first. |
| `ShardOwnership`, `ShardOwnershipCutover`, `ShardOwnershipFold`, `ShardOwnershipCrdt`, `ShardOwnershipRetention` | New rows or gaps | The adaptive split's and the shard fold's shard-level markers open around their windows and close with a routing-generation re-claim. A marker only holds the live advance and never blocks a move, a cutover or a fold, so ownership safety is unchanged. The re-claim can reject an in-flight write admitted before the close; that write fails unacknowledged. |
| `SplitLink`, `SplitLinkTurns`, `BPlusTopology`, `BPlusReclaimRecovery`, `BPlusCascade` | New rows or gaps | The split opens the sibling's marker in its birth persist, and the donor closes it at completion after re-claiming its epoch. That is one more call, made from the donor's completion; it waits on the WAL shard, not on another leaf. The reclaim opens the survivor's marker before widening, and retires the neighbour's entry in the retire step. |
| `SplitGateCycles`, `TurnOccupancy` | New rows | A target leaf's fresh read of the shard-level marker is a new leaf-to-shard-root wait-for edge. It is an interleaving, read-only call, so a serial shard-root turn that calls the leaf cannot deadlock against it. Both modules gain a row stating that the edge does not take the shard root's serial turn. |
| `BackupRestore`, the bulk-load paths | Rows | A restore and an offline snapshot load through `BulkLoadRawAsync`, so their leaves take the birth hint, the open marker and the switch's fence. |
| `ReplayPermits`, `WalGcStarvationDrive`, `WalGcStarvationDriveWitness`, `MaterialiserDrain` | Demand abstraction only | The design lowers replay demand. These modules bound permits and drives for any demand, so their properties are unaffected. Their notes gain a line that live-advanced leaves no longer drive. |
| `WalAdmission`, `WalBackPressure` | Gap entry | A stale-epoch or fenced-out rejection is a new append failure, distinct from admission refusal under load. It fails one stale or pre-close writer, never a saturated partition, so the admission and back-pressure properties are unaffected. |
| `AtomicCommit`, `AtomicCommitCrossCluster` | Unchanged; rows restated | A prepare whose append is rejected as stale is a nack, which `PrepareTx` already permits (it chooses ack or nack per key). A terminal is different. `ApplyTxTerminalAsync` flips the pending bucket in memory and appends only on its backstop path, so a stale activation accepts a terminal without appending. Safety still holds, through the leaf-wide prepare clamp: the current activation does not advance while the prepare is pending, and it learns the terminal by replaying the coordinator's record. `BroadcastStep` assumes only eventual delivery. Coordinators append terminals and hold no leaf epoch. |
| `SagaReadProgress`, `TagIndexReconcile`, `ReplicationShipping` | Unaffected | They consume accepted WAL records; a rejected append never becomes a record. |
| `AntiEntropy`; `Replication`, `ReplicationCausalDelivery`, `ReplicationLowWatermark`, `ReplicationReBootstrap` | Unaffected | They consume accepted WAL records only. Range-delete clipping changes the shape of a shipped record, not its semantics: the receiver applies each record's range, so the union applies. |
| `MaterialisedView*`, `view-*` modules | Unaffected; contract note | Views read accepted WAL records only. Clipped range deletes produce narrower range signals, documented as a contract change. |
| `AliasRevert`, `AliasRouting`; `Schema*`, `SchemaViewComposition`; `TenantIsolation`, `TenantFootprint`, `TenantQuotaEvaluation`, `TenantTreeCount`, `TenantRateBudget` | Unaffected | Routing, schema and tenancy decisions are made before a write reaches a leaf. |
| `BackupCapture`, `BackupCutover`, `BackupIncremental`, `BackupProvenance`, `BackupRetention` | Unaffected | They read accepted records and leaf state. |

## Production members

Each production member carries detectors proven red. They land in this order:

1. **Epoch claim and the stamped append.**
   - The leaf row epoch, `ClaimAppendEpochAsync`, entry rows under the reserved grain
     type, the retired floor on `WalShardFloorState`, unloaded-entry reload,
     `WalAppendEpochStaleException` and its mapping.
   - Entry retirement in pin retirement and the purge tail.
   - The ratcheted capability gate.
2. **Hand-off markers and fences.**
   - Split, leaf reclaim, adaptive split and shard fold.
   - The shard routing generation, stamped at admission.
   - The append-path stamp census.
   - Range-delete clipping, with its contract note.
3. **Bulk.** #5610's fix first, then the birth hint captured with the emptiness check,
   bulk-leaf ranges, and the switch's marker and fence.
4. **The live advance itself.**
   - Behind the gate, armed only by the rules above.
   - It reuses #5578's applied-prefix tracker and the core extraction from the
     abandoned #5596.
   - `WalLiveAdvance` then becomes a module with its refinement note.

## Review resolution

| Finding | Resolution |
|---------|------------|
| B1 drains | Section 2: the close is a fence (a re-claim of the initiator's epoch or routing generation), stamped at admission, on every append path. Waiting for returns is shown unsound. The model splits admission from allocation, and `Close` assumes nothing. Mutations: `DrainWithoutFence`, `DrainPointOnly`, `StampAtAppend`. |
| B2 epoch floor | Section 1, "Retiring entries": a monotone per-partition retired floor, raised before an entry is deleted. Modelled with `Recreate` (`WalLiveAdvanceRecreate.cfg`, clean at 8,907,861) and broken by `RecreateDropsFloor`. |
| S1 claim head and tracker | The claim reads `_nextOffset` in the lock section that assigns offsets; arming requires every own append applied. The model has two-step own appends. Mutations: `ClaimReadableHead`, `SnapshotHonestNoOwnTracker`. |
| S2 CAS | A strict conditional write that never adopts; ordinary-write conflicts are retried (N3). The model claims read-plus-one. Mutation: `ClaimAdoptsNoCas`. |
| S3 guard mutations | `NoClaimHeadReplay` and `NoOpenCheck` are added; the redundant `epoch > 0` guard is removed; `PERTURBS` fields name actions. |
| S4 overlap | Section 4 adds slot-less, rangeless-with-siblings and suspected-orphan leaves to the fail-safe list; the refinement gaps will name them. |
| S5 fold vs reclaim | Separate rows: leaf reclaim (per-leaf marker, neighbour retirement) and shard fold (shard-level marker, routing-generation fence). |
| S6 #5610 | Prerequisite of member 3; the hint and the emptiness check share one serial turn. Modelled by `PreWrite` and `Begin`'s check. Mutation: `BulkAppendNoEmptinessCheck`. |
| S7 storage, cost, moves | Entry rows under a reserved grain type, the floor scalar on `WalShardFloorState`, lazy loading, costs quantified. A move does not touch the table; restarts are modelled. The move mutation is replaced by `UnloadedEntryAccepted`. |
| S8 rollout | Ratcheted gate, downgrade only with the gate closed, fresh claims on re-enable, and unstamped appends rejected once open. The rejection's exception and mapping are named. |
| S9 impact rows | `AtomicCommit` restated (terminals and the prepare clamp); `TurnOccupancy` and `SplitGateCycles` gain the read-only edge; `WalMove` is a refinement row only. |
| N1 frozen view | `FrozenView` switch, clean at 3,704,430. |
| N2 README | Rewritten. |
| N3 conflict retry | Section 1, persist step. |
| N4 bulk hint | Section 3 says the hint is needed by every arming. |
| N5 witnesses | The duplicate's live advance and a trim after a live advance are witnesses. |
| Q1 re-created leaves | The floor, per the recommendation. |
| Q2 adaptive split marker | Shard-level, read fresh through an interleaving read-only call; the shard fold uses the same marker. |
| Q3 range-delete clipping | Accepted, with the consumer contract change listed. |

## Open questions for re-review

1. **Stamp census.** The shard-level fence relies on every write into a donor shard's
   leaves carrying the routing generation captured at that shard's admission. Point
   and batch writes are admitted at the shard root. Saga prepares, relocation and
   forwarding are candidates for paths that reach a leaf without that admission;
   member 2's census settles each path with a detector. Is any path known to bypass
   shard admission that the census should start from?
2. **Rejecting in-flight writes at a close.** The fence fails writes admitted before a
   close that have not been allocated yet; their callers retry. The bounded wait in
   step (b) keeps that rare. Is a retryable failure during a split, a fold or a bulk
   switch acceptable, or should the wait be mandatory before the fence?
