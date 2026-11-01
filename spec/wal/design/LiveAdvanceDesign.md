# Live checkpoint advance: fenced redesign

Design for issues #5572 and #5593, epic #5605. This is a design draft for review:
no production code implements it yet. `WalLiveAdvance.tla` beside this note models
it, with variant configurations and its mutations in `mutations/`. A module under
`spec/` must cite a production detector on every behaviour row, so this draft sits
outside module discovery. It becomes the companion module `spec/wal/WalLiveAdvance`,
with a refinement note, when the production members land.

**Gate.** No production member starts until the independent reviewer has accepted
this design.

**Revision.** This is the third revision. It answers the
[re-review of revision 2](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6563596741)
(4 blocking, 8 should-fix, 3 nits), and keeps the answers to the
[first review](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6558718267).
The hard requirement still applies: no unauthorised breaking change against the
shipped `lattice` 10.2.0 (tag `lattice-v10.2.0`).
[Backward compatibility](#backward-compatibility) lists every observable behaviour
change.
- Revision 2's behaviour change 3 is **withdrawn**. Fence rejections are now retried
  where they are raised, until the operation's existing deadline
  ([section 5](#5-rejections-are-retried-where-they-are-raised)). No caller sees a new
  exception cause.
- One change still needs the user's authorisation: which value wins when a bulk append
  meets a key the range already holds (change 4). It is pending before the bulk
  production member (member 3), not before the others.

The last sections map every finding of both reviews to its resolution.

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
  shard fold's donor shard, a range delete, and the leaf a write reached before a bulk
  switch. A range delete's record is not key-scoped: it carries the whole requested
  range, and every leaf of **every** shard whose range overlaps it replays it
  (`WalKeyFilter.IsKeyScoped` filters only Set, Delete and Tombstone, and the replay
  judge tests key overlap only). An unacknowledged record from one of these authors
  was skipped by its owner but still applied by a WAL reader such as the replication
  shipper or a view.
- **The bulk interleave.** A batch write that landed on the old leaf during a bulk
  load or append became invisible on the bulk leaf that took over its range.

`mutations/NoTrimPastAckedLiveAdvance5578.mutation` reproduces that design in the
model and fails "no trim past an acknowledged append" (trace below).

## The design

A leaf may advance a partition's checkpoint over its own applied appends only when
it can prove exclusive authorship of every entry it owns in the window it passes.
Whenever it cannot, it falls back to the replay-based advance, which reads and
applies every entry it owns. Three mechanisms supply the proof:
- an epoch fence against other activations of the same leaf;
- a hand-off fence against other writers;
- an unstamped watermark against writers that predate the fences.

All three are enforced at one seam: the WAL shard's offset assignment, under the
lock (`_stateGate`) that already makes the move fence (#4525) and the clock floor
(#4586) atomic with assigning an offset. A rejection there is retried where it is
raised, so no caller's contract changes (section 5).

**New state only in new rows.** Every piece of durable state the design adds lives
in its own row, under a reserved grain type, written through the tree's grain
storage provider. `GrainStorageFencingProbe` is the precedent. No existing state
type (`LeafNodeState`, `ShardRootState`, `WalShardFloorState`) gains a field. A
10.2.0 silo that rewrites one of those rows therefore cannot drop anything the
design relies on. See [Backward compatibility](#backward-compatibility).

| Row (reserved grain type) | Key | Holds | Written by |
|---------------------------|-----|-------|------------|
| `_lattice_leaf-append-epoch` | Leaf id | The leaf's append epoch | Claims only, by conditional write |
| `_lattice_wal-append-epoch` | WAL shard key and leaf id | The partition's epoch entry for the leaf | The claim at the WAL shard (conditional max-merge), and entry retirement |
| `_lattice_wal-append-epoch-floor` | WAL shard key | The partition's retired-epoch floor | Entry retirement only (max-merge) |
| `_lattice_wal-routing-generation` | WAL shard key and shard root id | The partition's routing-generation entry for a shard-level initiator | The initiator's re-claim (conditional max-merge) |
| `_lattice_leaf-handoff` | Leaf id | The leaf's hand-off marker: initialised flag, open tokens and per-partition close heads | Hand-off initiators, and the leaf's birth |
| `_lattice_shard-handoff` | Shard root id | The shard-level marker (initialised flag, open tokens with their key ranges, close heads), the shard's routing generation, and each in-flight bulk operation's birth hint | The shard root, and the range-delete fence |
| `_lattice_tree-range-delete` | Tree id and operation id | A range delete's durable intent: the shards it opened, and whether its fan-out has ended | The range-delete fence |

Each row's state type is new, carries a new stable alias, and is marked
`ILatticeBinaryPersistedState`. Every member is `[Id(n)]` and is a public property
of a concrete type: arrays and concrete dictionaries, never a collection expression
typed as an interface (#5579). The rows therefore also round-trip through a JSON
grain-storage serializer when a host bypasses Lattice's binary serializer.

### 1. The epoch fence: other activations of the leaf

**Claim eagerly, never lazily.** Before an activation's first append, it claims an
epoch. The claim is one serialised sequence per leaf and partition, in three steps:

1. **Persist (`ClaimRow`).** The activation writes the epoch it read plus one, raised
   above the partition's retired floor, to its `_lattice_leaf-append-epoch` row, as a
   strict conditional write on the ETag it read. Only claims write that row. So every
   conflict, empty-ETag included, means another activation claimed: this one is stale,
   the write fails closed and the activation deactivates. The caller's retry reaches
   the live activation (section 5).

   The write never goes through
   `TopologySeedPersist.WriteAdoptingBenignFirstCreateRaceAsync`, which adopts the
   winner's row on an empty-ETag conflict, the shape of #5593. Keeping the epoch out
   of `LeafNodeState` also means the leaf's ordinary persists never conflict with a
   claim.
2. **Write the entry (`ClaimWrite`).** At every WAL partition the activation writes,
   `ClaimAppendEpochAsync` reads the durable entry row and refuses the claim at or
   below the higher of the loaded entry, the durable entry and the retired floor.
   Otherwise it writes the entry row as a **conditional (ETag) max-merge**: the row
   never moves down. Two overlapping claims for one leaf can reach the shard in either
   order (claim calls interleave like appends, and a delayed write can land late), so
   a plain write could leave the durable entry below the in-memory one, and a restart
   would reload the lower value and readmit the superseded activation
   (`StaleEpochNeverAppendsClaimEntryOverwrite`, 12 states). An ETag conflict re-reads
   and re-decides.
3. **Raise (`ClaimRaise`).** Under `_stateGate`, the shard raises the in-memory entry
   to the epoch and reads `_nextOffset`, the allocator's next offset, not the readable
   head (`DurableContiguousTailOffset`). It returns that offset as the claim head. The
   raise is refused if another claim has raised the entry above this epoch since the
   write, or if a retirement since the write has raised the floor to it. Anything
   allocated before the raise lies below the claim head; anything after it is checked
   against the raised entry. The entry row was written before the raise, so a restart
   can never lose a raise whose head was already returned.

A refusal returns the higher of the entry and the floor. The activation then persists
that value plus one to its own epoch row, conditionally on the ETag it last wrote:
- **The row has not moved.** The refusal came from the floor alone (a re-created leaf
  whose epoch row is older than a retirement, cost N3), and the activation claims again
  above it.
- **The row moved.** Another activation has claimed since, so this one is stale: it
  stops, and the caller's retry reaches the live one.

The model folds the first case into `ClaimRow`, which reads the floor directly, and
models the second as `ClaimRefuse`, which stops the activation (`WitnessClaimRefused`).

**The stamped append.** Once the cluster's fence gate is open (section 4), an
activation appends through new stamped methods; the existing methods are unchanged:
- `AppendStampedAsync`, for a single entry;
- `AppendBatchStampedAsync`, for a batch. This is the dominant path:
  `WalCommitLogWriter.AppendManyAsync` reaches `IWalShardGrain.AppendBatchAsync`
  today, and `WalBatchedSingleEntryAppends` routes even single entries there. Each
  entry of the stamped batch carries its own stamp (leaf identity, epoch, and the
  shard routing generation of section 2). Under the one `_stateGate` section that
  assigns the batch's offsets, the shard checks every entry's stamp before it assigns
  any offset. Entries whose stamp fails are rejected and the rest are allocated; the
  response carries the per-entry outcome and the unstamped watermark (section 4). The
  coalescing writer fails only the callers whose entries were rejected, and their
  seam retries them (section 5).

The shard accepts a stamped entry only if the leaf's entry is loaded, the epoch is not
below it, and the epoch is above the retired floor. A stale epoch is rejected with
`WalAppendEpochStaleException`, an internal exception (section 5). An entry not yet
loaded rejects the append as unloaded (below); the leaf re-presents its claim and
retries. The stored record does not change: the stamp is a call argument, never part
of the record.

**The retired floor at every step.** A purge and re-creation of the leaf resets its
entry, so a claim written before the retirement and raised after it would pass an
entry-only check. The floor is therefore checked at the entry write, at the raise and
at the append. Dropping the raise and append checks together fails in 8 states
(`StaleEpochNeverAppendsIgnoresRetiredFloor`). Either check alone suffices in the model
(see [Redundancy checks](#redundancy-checks)); both are kept, so a stale claimant
stops at its raise rather than on its first append.

**Arming.** The activation is armed for the live advance on a partition only when:
- its claim on that partition has been raised (an explicit guard; see the redundancy
  checks for why it is kept);
- its marker state is initialised (section 2);
- it has replayed to that partition's claim head;
- every append it has issued there has been applied (the applied-prefix tracker);
- it has read past every unstamped append reported to it, its own included
  (section 4);
- the hand-off conditions in section 2 hold.

Everything another activation appended before the claim lies below the claim head
and has been read. Everything an older activation tries to append after it is
rejected. A newer activation's appends all lie above this activation's own.

A lazy fence, where the shard rejects an older epoch only once the newer one has
appended, is not enough: a stale duplicate can append between the newer
activation's claim and its first append (`NoTrimPastAckedLazyFence`).

**Loading entries.** The shard keeps the entries it has loaded in memory, in a
dictionary keyed by leaf. Entries load lazily: an append from a leaf whose entry is
not loaded is rejected as unloaded, and the leaf's re-presented claim (same epoch)
loads it. No storage read ever happens on the append path or under the lock.

**Retiring entries: a monotone floor.** An entry is retired only when its leaf is
retired: by the step that retires the leaf's materialiser pins, under the retirement
token (#5358), or by a purge tail, under the lineage check (#5417). Retiring raises
the partition's floor row to at least the entry's epoch, durably, before the entry
row is deleted. No removal ever lowers the floor, so the entries stay bounded by live
leaves.

A re-created leaf (a purge re-creation, or an id reused by a create) starts a new
epoch row. Its first claim lands in an absent entry, so it must exceed the floor,
and therefore every epoch a surviving duplicate of the old incarnation holds.
`ReadPositionHonestRecreateDropsFloor` removes the floor raise and fails.

Retirement code runs whether the gate is open or closed, so rows are retired on every
path a new silo runs. A retirement on a 10.2.0 silo deletes none of these rows; the
leftover rows are safe (the entry stays above any re-claim, the epoch row survives),
and a periodic sweep removes rows whose leaf no longer exists (review N1).

**Moves and restarts.** The entry, floor and generation rows live in grain storage,
not in the WAL stream, so a WAL move, which switches the stream's provider, does not
touch them. A restart of the shard grain, including the reactivation at a move's
cutover, loses only the in-memory entries, which reload through re-presented claims;
nothing is accepted from an unloaded entry
(`StaleEpochNeverAppendsUnloadedEntryAccepted`). It also loses the in-memory unstamped
watermark, which restarts at the recovered allocator head minus one (section 4).

**Abandoned flushes (#4621).** The restarted watermark assumes nothing is allocated
below the recovered head after the restart. That holds: the reactivated allocator
recovers from the highest stored offset (`WalShardGrain.cs:440`) and ignores abandoned
flush windows. A flush abandoned before the restart that lands late conflicts
create-only with the reissued offset, and one side fails. So no entry lands below the
restarted watermark unreported.

**Cost.**
- **Per append:** one dictionary lookup and two comparisons under the lock the append
  already holds, per stamp. No allocation and no grain call. A stamped entry carries 24
  more bytes on the request: leaf identity, epoch and routing generation. The response
  carries the 8-byte unstamped watermark, and a batch response the per-entry outcome.
  The stored record is unchanged.
- **Per activation:** one conditional row write, plus one claim call, one entry-row
  read and one entry-row write per partition it writes. A fresh estate of N leaves
  writing P partitions costs at most N x P entry-row writes once, not O(N^2).
- **After a retirement (N3):** the floor is one scalar per partition, so after any
  leaf's retirement every new leaf whose epoch row is older than the floor pays one
  refused claim per partition it writes, then claims above the floor.
- **Per shard restart:** one extra claim round trip for each leaf that appends
  afterwards.
- **Storage:** one entry row per (partition, live leaf that wrote it), one floor row
  per partition, one generation row per (partition, shard root that re-claimed
  there), one epoch row per leaf, and the marker rows.
- **Memory:** about 100 bytes per leaf that has appended to the partition since the
  shard activated.

### 2. The hand-off fence: other writers

Every foreign-author path follows one protocol:

1. **Open.** The initiator sets a durable open marker on the owner before its first
   admission of a write the owner will own.
2. **Close by fencing.** In order, the initiator:
   - (a) stops admitting writes for the range;
   - (b) may wait a bounded time for writes it admitted earlier to return. This is an
     optional latency optimisation: no bound makes a rejection in (c) impossible;
   - (c) re-claims its own epoch (a leaf initiator) or its routing generation (a
     shard-level initiator) at every partition it may have written. The re-claim
     returns allocator heads, exactly as a claim does;
   - (d) closes the marker, recording those heads.
3. **Re-arm by replay.** The owner holds the live advance while any marker is open.
   It re-arms only after replaying to the recorded heads.

**Why the drain is a fence and not a wait.** Every stamped append carries the epoch
or generation its write was admitted under. So after (c), a write admitted before the
close either was allocated already, and lies below the recorded heads, or will be
rejected when it arrives. A wait cannot replace this: a write's call can return by
timeout while its append is still delivered and allocated later
(`ReadPositionHonestDrainWithoutFence`). The design relies on no gate draining
anything: split commits never take `_splitGate`, the adaptive split's reject phase
only flips a phase, and the fold's serial-turn guard excludes point writes.

**The routing generation is durable (S1).** A shard-level initiator's generation entry
at a partition lives in `_lattice_wal-routing-generation`, with the same semantics as
a leaf's entry: a conditional max-merge write, an in-memory copy raised under the
lock, an unloaded entry that rejects until a re-presented re-claim reloads it, and no
loss across a restart or a move. If a restart lost it, a write admitted before a close
would pass the reset entry after the close (`ReadPositionHonestGenerationNotDurable`,
14 states).

**Stamped at admission.** The stamp is taken where the write is admitted, not where
the append is built. A leaf captures its epoch in the same synchronous step as span
admission. A shard root captures its routing generation in the same step as its phase
check (`ThrowIfRejectedForKey`) and passes it to the leaf through a new stamped
leaf-write method. If the append read the current epoch instead, a write admitted
before the close would carry the re-claimed epoch and be accepted
(`ReadPositionHonestStampAtAppend`).

**The unstamped leaf methods stay unstamped (S3).** A write that reaches a leaf
through an existing leaf-write method carries no routing generation: a 10.2.0 shard
root admitted it, or a path the census missed. The leaf appends it through the
**unstamped** WAL method, never stamped with its own epoch alone. A leaf-epoch stamp
would pass the leaf's own check and escape every shard-level fence unreported; an
unstamped append is admitted and reported (section 4), which is safe and disarms the
leaves it reaches.

**The stamp census.** Production member 2 carries a census of every path that appends
an entry a leaf owns, with a detector per path. Each path is stamped, classified, or
stated "unstamped, disarms". A path that bypasses the stamp is the gap the fold's
point-only guard has today (`ReadPositionHonestDrainPointOnly`). The census starts
from the paths that reach a leaf without shard-root admission:

| Path | Where | Treatment |
|------|-------|-----------|
| Point, batch, CRDT apply, merge, delete | Shard root admission, then the stamped leaf-write method | Stamped with the shard's routing generation and the leaf's epoch |
| Saga prepare | `BPlusLeafGrain.PendingTx` | Stamped at the participant's admission |
| Stranded relocation, leaf to leaf | `BPlusLeafGrain.SpanAdmission.cs:431`, `MergeManyAsync` | Stamped with the donor leaf's epoch, under the donor's open marker on the receiver |
| Re-routing a terminal's stranded prepared keys, and the backstop appends | `BPlusLeafGrain.PendingTx.cs:2534`, `:2719ff` | Stamped with the re-routing leaf's epoch, under its marker on the sibling |
| Split transfers | `BPlusLeafGrain.Split.cs`, `MergeEntriesAsync` | Under the split's marker on the sibling; the donor's epoch |
| Adaptive split coordinator's drain | `TreeShardSplitGrain.cs:1023`, `:1037` | Under the adaptive split's shard-level marker; the donor shard's generation |
| Bulk seeding and the resize mirror | `ShardRootGrain.BulkLoad.cs`; the resize mirror | Under the bulk switch's marker; the shard's generation |
| Shadow forwards, plain and CRDT | `ShardRootGrain.Split.cs` | Under the adaptive split's marker; the forwarding shard's generation |
| Compaction reap, replayed terminals | `BPlusLeafGrain.cs:3434`; `BPlusLeafGrain.ReplayedTerminals.cs:147` | Leaf-internal: stamped with the leaf's own epoch. They are the leaf's own appends, tracked like any other |
| Coordinator saga terminals | `AtomicWriteGrain.FlushPendingTerminalsAsync` (`:2356`); `ShardRootGrain.TxTerminal` (`:352`) | The **terminal** stamp class (below) |

**Coordinator terminals (S6).** A saga terminal appended by a coordinator, not by a
leaf, carries a new stamp class, "terminal", which the shard admits like an unstamped
append but does **not** report in the unstamped watermark. Excluding it is safe
because of two guards the leaf already has: the leaf-wide prepare clamp (#5572), which
holds the advance on every partition while any prepare is pending; and
`_recentlyTerminal`, which makes a terminal applied after its prepare resolved
idempotent. A terminal only ever resolves a prepare the leaf itself recorded, so the
advance never passes a terminal whose prepare is still pending, and passing one whose
prepare has resolved changes nothing. Without the class, every saga would raise its
partitions' watermark and send every leaf writing them back to replay, the demand
#5569 and #5570 are about. The model has no sagas, so this rests on the clamp; it is a
gap entry in the refinement note, and member 2's detector proves a terminal-class
append never raises the watermark and never passes a pending prepare.

**Crash and cost.** If the initiator crashes, the marker stays open until its
recovery re-drives the close. The cost is latency (the owner advances by replay),
never data. A write admitted before the close and not yet allocated at (c) is
rejected unacknowledged and retried at its seam (section 5).

| Path | Initiator | Open | Admission stops at | Fence at close |
|------|-----------|------|--------------------|----------------|
| Split | Donor leaf | In the sibling's marker row, written in the sibling's birth (`InitializeSiblingAsync`) | The donor's narrow of `HighKeyExclusive`; span admission forwards moved keys afterwards | The donor re-claims its epoch at every partition it writes; the heads close the sibling's marker |
| Leaf reclaim | Absorbed neighbour leaf | On the survivor, before its range widens | The neighbour's unlink | The neighbour's entry is retired (the floor rises), so its later stamped appends are rejected; the heads are captured in the retire step |
| Adaptive split | Donor shard root | Shard-level marker on the target shard root | The reject phase | The donor shard root re-claims its routing generation at every partition; the heads close the marker at completion |
| Shard fold | Folded shard root | Shard-level marker on the survivor shard root | The fold's quiesce, point and batch | The same routing-generation re-claim |
| Range delete | The tree-level range-delete fence (below) | A range-scoped marker on **every** shard root of the tree, before any shard appends | The end of every shard's walk, or its abandonment | Every shard root re-claims its routing generation at every partition; the heads close each shard's marker |
| Bulk load / append | The shard root, for writes admitted before the switch | At the bulk leaf's birth, with the birth hint | The switch, in the shard root's serial turn | The shard root re-claims its routing generation at every partition; the heads close the marker |

**Range delete: fenced at the tree (B1).** A range delete's record is replayed by every
overlapping leaf of every shard, not only by the authoring shard's leaves. A
shard-scoped fence therefore fails after a partial fan-out: shard A's walk appends,
shard B's walk fails, and B's armed leaves pass A's record and never delete those keys,
a permanent divergence from replicas and views that 10.2.0 does not have (it converges
at B's next replay). So the fence is tree-level:
1. **Intent.** Before any shard appends, the fence writes a durable intent row
   (`_lattice_tree-range-delete`, keyed by tree and operation id) naming the delete's
   range.
2. **Open.** It opens a range-scoped marker, idempotently, in the shard-level marker
   row of **every** shard root of the tree, and records each in the intent. Every
   shard's B+ tree spans the whole key space, so every shard can hold an overlapping
   leaf.
3. **Fan out.** `LatticeGrain.DeleteRangeAsync` runs the shard walks as today, each
   stamped with its shard's routing generation at admission, with one issue HLC for
   all shards.
4. **Close.** Only once every shard's walk has finished or been abandoned (the
   operation's deadline passed, or the cursor step gave up), each shard root re-claims
   its routing generation at every partition, records the heads, and closes its
   marker. The intent is then deleted.
5. **Re-drive.** A per-tree reminder re-drives the close of any intent whose fan-out
   has ended, so a crash at any step leaves the markers open, costing latency only,
   until it closes them.

The record is unchanged (it is not clipped; see
[Backward compatibility](#backward-compatibility)). A leaf whose range is disjoint
from an open range-delete marker ignores it, by the same overlap test the replay judge
uses (`IsDisjointRangeDelete`). **Cost:** one intent write and delete, two marker writes
per shard and one re-claim per shard per partition per range delete; and **every
overlapping leaf of the tree is disarmed for the delete's duration**, advancing by
replay. The model's foreign author stands for every shard root's walk: its marker is
open on the owner's shard before any of them is admitted, which step 2 guarantees
whichever shard authors the record.

**Hand-offs in flight across the gate (B2).** A split (`LeafNodeState.SplitInFlight`
and `SplitSiblingId`, resumed by `CompleteSplitAsync`), an adaptive split
(`ShardRootState.SplitInProgress`), a bulk graft (`PendingBulkGraft`), a shard fold, or
a cursor range delete can be started by a 10.2.0 silo, or by any silo while the gate
is closed, and so never open a marker. Two rules close that window:
1. **Resume opens the marker.** Every resume path of an in-flight hand-off opens its
   marker idempotently before its next admission, before it admits a stamped write.
2. **Absence never means closed.** A leaf or shard with no marker row is held, not
   armed, until its marker state is initialised: a shard root writes the initialised
   flag only in a serial turn in which it has no hand-off in flight; a leaf born under
   the gate writes it at its birth; an existing leaf takes its shard's flag once that
   is set and the leaf itself has no split in flight.

Dropping rule 1 fails in 17 states (`ReadPositionHonestResumeWithoutMarker`). Rule 2 is
implied by rule 1 in the model (see [Redundancy checks](#redundancy-checks)); it is
kept because a resume path the census misses would otherwise read as closed.

**Shard-level marker.** The moved slots of an adaptive split or a shard fold, and the
leaves a range delete reaches, scatter across a shard's leaves, so a per-leaf marker
would cost one call per leaf. A leaf reads its shard's marker **fresh**, after the
last own append it would advance over, never from an earlier tick's cache. It reads
it through a new interleaving, read-only shard-root call, `GetHandOffMarkerAsync`,
which returns the marker's generation, open ranges and close heads. Read-only is
load-bearing: this is a new leaf-to-shard-root wait-for edge, and a serial shard-root
turn that calls the leaf must not be able to deadlock against it. The read costs one
call per advance tick, not per write.

**Saga terminals** keep the leaf-wide prepare clamp (#5572): the advance holds on
every partition while any prepare is pending.

### 3. The bulk birth hint

A bulk load or append captures the per-partition WAL heads when it begins, before any
write it can race. The leaves it creates take those heads as their birth hint. The
hint is the read position they start from, as a split sibling starts from its donor's
heads (`WalHeadsAtBirth`). With the open marker, re-arming then means a short, bounded
replay from the hint to the close heads of the switch's fence. That replay imports any
write that reached the old leaf in the window, before the advance proceeds as for any
leaf.

The hint is needed by every arming, not only by the live advance: it is the leaf's
birth read position. Starting the leaf at the head when it is created instead fails
without any live advance (`ReadPositionHonestBulkWithoutHint`).

**The hint survives a crash (S7).** The hint is written to the shard root's
`_lattice_shard-handoff` row, keyed by the bulk operation id, in the serial turn that
captures it and before any leaf is created. A crash between the capture and the switch
resumes the same operation id with deterministic leaf ids, and the resume reuses the
stored hint; a later one would skip the window the hint exists to cover.
`PendingBulkGraft` stays in `ShardRootState` unchanged. The hint row is deleted when the
switch's marker closes.

**Existing rows: relocate, never refuse (#5610).** Today a bulk append grafts its
leaves without checking the keys the rightmost leaf already holds, so a point write
into the bulk range between chunks becomes unroutable. #5614's inflated counts are
probably the same graft. In the same serial shard-root turn that captures the hint,
the graft finds the rows the bulk range already holds and relocates them into the new
bulk leaf at its birth, exactly as a split relocates its moved rows, narrowing the old
leaf. It never refuses the chunk: a chunk that succeeds today keeps succeeding.
Skipping the relocation fails (`ReadPositionHonestBulkAppendDropsExistingRows`).

**Which value wins (S5): last-writer-wins.** `BulkAppendAsync` stamps its entries from
`HybridLogicalClock.Zero` (`ShardRootGrain.BulkLoad.cs:440`, `:460`, `:493`), so a
relocated pre-existing row wins the last-writer-wins merge for any key both hold.
- **Today:** a read of such a key routes to the new bulk leaf and returns the bulk
  value; the pre-existing row is stranded, unroutable, and still counted (#5614).
- **With the fix:** the acknowledged point write wins and is returned; keys only the
  pre-existing rows hold become readable again; counts and scans become exact.

This is the design's choice, and it is a **documented-behaviour change for a bulk
append into a non-empty range**, pending the user's authorisation before the bulk
member (change 4). The alternative is **"bulk value wins"**: the graft drops each
relocated row whose key the bulk chunk also holds, keeping today's read result for
those keys while still making the other relocated keys readable. #5610 and #5614 are
fixed together in the bulk member, which confirms whether they share a cause.

Bulk leaves also get the key ranges `PlanBulkLoadLeaves` already computes. A rangeless
bulk leaf owns every key of its shard during replay, which duplicates sibling bulk
leaves' rows into it.

### 4. Fail safe everywhere, and mixed versions

**The fence gate** works like the clock floor's gate. Each silo whose build supports
the claim, the stamped methods and the markers advertises a marker grain interface,
`IWalAppendFenceCapable`, in its grain manifest. A `ClusterManifestWalAppendFenceGate`
is open only while every active silo advertises it. A member whose manifest has not
arrived counts as not capable, so the gate fails closed during a rolling upgrade and
opens by itself once the last old silo has left. The precedent is
`IWalClockFloorCapable` and `ClusterManifestWalClockFloorGate` (#4586). The gate is
not ratcheted: if a 10.2.0 silo joins, or rejoins in a rollback, it closes again by
itself.

**Unstamped appends are always admitted.** An append through an existing, unstamped
method comes from a 10.2.0 silo, from any silo while its view of the gate is closed,
or from a path the census classifies as unstamped. The WAL shard never rejects it.
Instead it records the append's offset as the partition's highest unstamped offset,
and returns that value in the response of every stamped append. An activation arms
only once it has read past every unstamped offset reported to it:
- An unstamped append allocated before one of the activation's own appends is
  reported by that append's response.
- One allocated after the activation's last own append lies above the advance's
  target, so the advance cannot pass it.

The watermark is in memory. A shard restart sets it to the recovered allocator head
minus one, treating everything before the restart as possibly unstamped.

So an unstamped append is never rejected (`OldNeverRejected`, broken by
`OldNeverRejectedRejectedOnceFenced`) and never skipped
(`ReadPositionHonestUnstampedNotReported`, `ReadPositionHonestRestartForgetsUnstamped`).

**An own unstamped append reports itself (S2).** When an activation's view of the gate
closes, its own appends go unstamped, and no stamped response reports them. If its view
reopens while it is armed, its next advance would cover them. So an activation raises
its reported watermark to the offset of each of its own unstamped appends, as if the
shard had reported it. Dropping the self-report fails in 13 states
(`ReadPositionHonestNoSelfReport`); the configuration `WalLiveAdvanceGateFlip.cfg`
lets an activation's appends go stamped or unstamped at any time.

**Racing the gate (S4).** A new silo may call a stamped method on a grain activated on
a 10.2.0 silo before its gate has observed that silo join. What a 10.2.0 silo does with
an unknown invokable, and what the caller sees, has no precedent:
`ClusterManifestWalClockFloorGate` is consulted locally by the WAL shard and never calls
a new method on an older silo. So:
- Member 1 pins the failure with a mixed-version test (a 10.2.0 silo's grain receiving
  the new method), and records the exact exception type and shape.
- The caller re-issues the write through the unstamped method **only** on that definite
  "method unknown" failure, and closes its own view of the gate.
- Any other failure, above all a timeout, is ambiguous (the append may have been
  allocated) and follows today's retry semantics unchanged.

**The leaf takes the replay-based advance** on a partition whenever it cannot prove
exclusive authorship:
- **Its own claim.** No claim yet on the partition, or the claim failed, conflicted
  or was refused.
- **Replay.** It has not replayed to the claim head, to the last close's heads, or
  past the highest unstamped offset reported to it or appended by it.
- **Hand-offs.** Its marker state is not initialised; a leaf marker is open; or the
  shard-level marker read has an open range overlapping the leaf, failed, or is not
  fresh.
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
  (`GrainStorageFencingProbe` reports so). The claim's conditional writes prove
  nothing without them.
- **Rollout.** The fence gate is closed. Nothing claims, the shards keep no entries,
  and every advance is replay-based.

### 5. Rejections are retried where they are raised

A stale-epoch or fence rejection is not a fault that repeats by itself. A re-issued
write is admitted under the **current** epoch and generation, and only another claim
or another close can reject it again. Both are finite within any interval: a claim
needs a new activation, and a close needs a hand-off. So the design retries a
rejection at the seam that raised it, for as long as the operation's **existing
deadline** allows (its cancellation token, or the response timeout of the call that
carries it), not for a fixed number of attempts. When that deadline expires, the caller
receives exactly what it receives today when the deadline expires. Nothing new is
surfaced: `WalAppendEpochStaleException` and the fence rejection are internal, never
cross a public API boundary, and are never an inner exception of anything public.

Convergence: each retry either succeeds or meets a claim or close that happened after
its admission. Under unbounded claim or close churn on one key, the retry runs to the
deadline, which is the same outcome as today's timeout under unbounded churn. In the model a
retried foreign write is a fresh `ForeignAdmit` under the current epoch, and a stale
activation's retry is the live activation's own append.

Every caller of the leaf write surface, and where its retry runs:

| Caller | Exposure without the seam retry | Seam retry |
|--------|---------------------------------|------------|
| `LatticeGrain` writes through `ShardActivationRetry` (34 call sites) | None; the envelope already retries transient faults, but only 3 times | The rejection is retried below the envelope, at the shard root's leaf-call step, until the deadline; the envelope never sees it |
| `DeleteAsync`, single-key `ApplyCrdtDeltaAsync` (`RunMutationAsync`, `RetryOnStaleRoutingAsync`) | The internal exception would reach the caller | Same leaf-call step; a stale generation re-routes through the existing stale-routing path |
| Saga prepares (`AtomicWriteGrain`, cross-tree transactions) | A rejected prepare becomes a nack, and the saga rolls back with `InvalidOperationException` where 10.2.0 commits | The participant retries the prepare under its current stamp until the prepare's deadline; a fence rejection is never a nack |
| Replication apply (`DeadLetterTrackingReplicationApplier.ApplyTrackedAsync`) | Counted toward the dead-letter budget, a failed peer contact recorded | Retried below the applier, so a rejection never reaches its accounting |
| Views, tag index, resize mirror, backup restore, schema remediation | A transient failure in their own retry loops | Same leaf-call step; their loops see success or today's deadline outcome |
| Leaf-to-leaf paths (relocation, prepared-key re-routing, split transfers), shadow forwards, the adaptive split drain, bulk seeding | A failed hand-off step, re-driven by recovery | The initiating step retries under its current stamp; recovery is unchanged |
| A leaf's own appends (compaction reap, replayed terminals) | A failed internal append | A stale activation stops (it is superseded); the live activation redoes the work |

For a stale epoch, the rejected activation deactivates, and the retry is a fresh call to
the same grain id, which reaches the live activation. For a fence rejection at a close,
the leaf re-admits the write under its current epoch and generation (span admission
forwards a key the close moved), or the shard root re-routes it once, as it already
does for stale routing.

## Backward compatibility

`lattice` 10.2.0 shipped (tag `lattice-v10.2.0`). This section shows that the design
breaks nothing that release relies on, in storage, on the wire, or across a
mixed-version cluster, and lists every observable behaviour change.

### Persisted formats

| Format | Change | 10.2.0 data under the new build | New data under 10.2.0 |
|--------|--------|---------------------------------|-----------------------|
| WAL records | None. Stamps, the routing generation and the watermark travel as call arguments and response fields, never in a stored record. Range deletes are fenced, not clipped, so their records keep their shape. | Read unchanged | Read unchanged |
| `LeafNodeState`, `ShardRootState`, `WalShardFloorState` | None: no field is added. The bulk hint lives in a new row (S7). | Read unchanged | Read unchanged. A 10.2.0 silo that rewrites one drops nothing the design needs, because nothing new lives there. |
| New rows (the table at the top of [The design](#the-design)) | New state types, new aliases, reserved grain types | Not present: an absent row reads as "no claim", "floor 0", and "marker not initialised", which **holds** the advance; the leaf falls back to replay. An absent marker never reads as closed (B2). | Never read by 10.2.0, which knows none of these types. A rollback leaves them in place, harmless; re-upgrading resumes from them. |

**JSON-backed providers (#5579).** A host's grain storage normally serializes the new
state types through `LatticeGrainStorageSerializer`, in binary, because they are
marked `ILatticeBinaryPersistedState`. A host that bypasses it falls back to the JSON
serializer, which persists public properties only and must be able to construct every
collection on read. The new state types are written for both: every member is a public
property of a concrete type. Each type gets a round-trip test through the JSON
grain-storage serializer as well as the binary one. The WAL's JSON-backed providers see
no change, because no WAL record changes.

### Wire formats

- **Grain interfaces, 10.2.0 caller to a new silo.**
  - No existing grain interface method changes its name, parameters or return type.
  - New behaviour arrives only through new methods: `ClaimAppendEpochAsync`,
    `AppendStampedAsync`, `AppendBatchStampedAsync`, the stamped leaf-write methods and
    `GetHandOffMarkerAsync`.
  - A 10.2.0 silo calls only the existing methods, and a new silo serves them exactly
    as 10.2.0 does. Its appends are unstamped, so they are admitted and reported, and a
    write reaching a leaf through an existing leaf method is appended unstamped (S3).
- **Grain interfaces, new silo to a 10.2.0 silo.** A new silo calls a new method only
  while its gate is open. The race where a 10.2.0 silo joins before the gate observes it
  is pinned by a mixed-version test, and only its definite "method unknown" failure is
  re-issued unstamped ([Racing the gate](#4-fail-safe-everywhere-and-mixed-versions)).
- **Serializable types and exceptions.**
  - The new request and response types have new aliases. Existing aliases and `[Id]`s
    are untouched. `LatticeWalQuiescingException` is untouched (revision 2's `Reason`
    field is withdrawn).
  - `WalAppendEpochStaleException` derives directly from `System.Exception` and carries
    a new alias. It is internal to the seam retry and reaches no public caller.
- **Cross-cluster replication.** Shipped records are WAL records, which are unchanged,
  so a 10.2.0 peer and a new peer exchange exactly what two 10.2.0 peers exchange, in
  both directions. A replicated write applied on the receiving cluster goes through
  the receiving leaf's own append path: stamped on a new cluster with the gate open,
  unstamped otherwise; a rejection is retried below the applier (section 5).

### Mixed-version rollout

Fencing and the live advance turn on only once every active silo supports them, and
off again if a 10.2.0 silo joins. At every moment:
- a 10.2.0 silo's unstamped append is never rejected;
- it is never skipped by an advance, because it is reported and must be read before
  any activation arms past it;
- an own unstamped append, made while an activation's view of the gate was closed,
  reports itself;
- a hand-off begun before the gate opened holds every owner it reaches until it
  resumes, opens its marker and closes it by fence.

The model checks these with four configurations:
- `WalLiveAdvanceMixed.cfg` and `WalLiveAdvanceMixedRestart.cfg`: an environment
  author `o` appends unstamped. `OldNeverRejected` (broken by
  `OldNeverRejectedRejectedOnceFenced`) and `ReadPositionHonest` (broken by
  `ReadPositionHonestUnstampedNotReported` and `ReadPositionHonestRestartForgetsUnstamped`).
- `WalLiveAdvanceGateFlip.cfg`: own appends may go unstamped at any time
  (`ReadPositionHonestNoSelfReport`).
- `WalLiveAdvanceLegacyHandOff.cfg`: a hand-off with no marker writes unstamped before
  the gate opens (`LegacyWrite`), the gate opens (`OpenGate`), the hand-off resumes and
  admits stamped writes (`Resume`, which opens the marker), and the owner's marker state
  is initialised only once no such hand-off is in flight (`MarkInit`)
  (`ReadPositionHonestResumeWithoutMarker`).

A downgrade needs no operator step: the gate closes when the first 10.2.0 silo
appears, and leaves fall back to replay as soon as an unstamped append is reported.

### Observable behaviour changes

Each change is classed as a **compatible bug fix** (no documented behaviour changes)
or a **documented-behaviour change** (needs the user's explicit authorisation before
production).

| # | Change | Class |
|---|--------|-------|
| 1 | **A stale duplicate activation's write is retried, not accepted.** With the gate open, the WAL shard rejects a stale epoch's append; the stale activation deactivates, and the seam retry (section 5) reaches the live one. Every caller in the section 5 table sees one write that succeeds, with added latency, or today's deadline outcome. Atomic writes, cross-tree transactions, `DeleteAsync`, single-key CRDT deltas and replication apply are covered by the seam retry, so none of them fails, rolls back, or counts toward a dead-letter budget where 10.2.0 succeeds. Orleans already promises a single activation, so no caller relies on duplicates. | Compatible bug fix |
| 2 | **A write admitted before a hand-off close, and not yet allocated, is retried.** The close's fence rejects it, and its seam re-admits it under the current stamp; span admission or the shard map routes it to the new owner. The callers and their outcomes are those of change 1. | Compatible bug fix |
| 3 | *Withdrawn.* Revision 2 surfaced an exhausted retry as `LatticeWalQuiescingException` with a new `Reason`. The seam retry runs to the operation's existing deadline instead, so there is no exhaustion to surface, no new exception cause, and no change to `LatticeWalQuiescingException`. | - |
| 4 | **Bulk append over existing rows (#5610, #5614).** The graft relocates the rows the bulk range already holds into the new bulk leaf, instead of stranding them. No new exception: a chunk that succeeds today still succeeds. For a key both the bulk chunk and a pre-existing row hold, the pre-existing (acknowledged) value now wins last-writer-wins, where today the bulk value is read and the pre-existing row is stranded; keys only the pre-existing rows hold become readable; counts and scans become exact. The alternative, "bulk value wins", keeps today's read for keys in both. | **Documented-behaviour change for bulk append into a non-empty range: pending user authorisation before the bulk member (member 3).** |
| 5 | **The live advance itself (#5572).** A leaf's checkpoint advances without a replay from -1. The WAL GC trims sooner, replay demand falls, and #5570's "carries no usable offset" warnings stop for live leaves. Existing metrics report different values; none changes name, unit or tags. | Compatible bug fix |
| 6 | **Range deletes cost more.** Each range delete writes an intent, opens and closes a range-scoped marker on every shard of the tree, re-claims every shard's generation at every partition, and disarms every overlapping leaf of the tree for its duration. The record, its result and its feed and history output are unchanged. | Compatible (cost only) |
| 7 | **New metrics.** New instruments only, on the existing meter: claims, refusals and rejections by reason, seam retries by caller class, live advances by arming outcome, open markers, and unstamped reports. No existing instrument gains a tag, a tag value, or a new unit. | Compatible (additive) |
| 8 | **New log lines.** A deactivation warning for a stale activation, and a debug line when a partition disarms on an unstamped report. #5571's blocked-pin warning keeps its categories. | Compatible (additive) |
| 9 | **New storage rows** under the reserved grain types listed above appear in the host's grain storage, and a sweep removes orphaned ones. They are documented in the tree's storage notes. | Compatible (additive) |
| 10 | **New public configuration.** A `LatticeOptions` switch to turn the live advance off is additive, and defaults to on once the gate opens. | Compatible (additive) |

**Change 4 is the only one that needs the user's authorisation,** and only before the
bulk member. The others change no documented behaviour, and none adds an exception a
caller can observe.

## The model

`WalLiveAdvance.tla` covers:
- one WAL partition of three offsets, and one owner leaf;
- its born activation `a1`, created by a bulk load, and a duplicate `a2` that also
  stands for the next activation after a stop (both read the row and must claim);
- a foreign author `f` with point and batch writes and up to two hand-off windows: the
  bulk switch and one more. `f`'s epoch stands for either a leaf initiator's epoch or a
  shard-level initiator's routing generation, including every shard's walk of a
  tree-level range delete; their fences have the same algebra;
- a pre-existing writer `p`, whose rows the bulk range already holds and the graft
  relocates (#5610);
- an unstamped author `o`: a 10.2.0 silo, or any silo while the gate is closed;
- a legacy hand-off begun before the gate opened, with no marker;
- a GC trimming below the shared snapshot's coverage.

Only entries the leaf owns are modelled; entries other leaves own are invisible to its
replay. It reuses `WalDurability`'s abstractions: the read position as the checkpoint,
a snapshot that keeps the highest coverage, and a pin at that coverage.

**Steps.**
- **The claim is three steps** (`ClaimRow`, `ClaimWrite`, `ClaimRaise`), with
  `ClaimConflict` (the epoch row moved) and `ClaimRefuse` (refused at the write or the
  raise). The model compares epochs where production compares ETags; only claims write
  the epoch row, so the two agree. `fence` is the durable entry and `memFence` the
  in-memory one.
- **Own appends are two steps.** `OwnAlloc` is the shard's checks and offset assignment
  under its lock, and records the watermark it reports. `Land` makes the append durable
  and, if its author is still running, applies and acknowledges it.
  `OwnAllocUnstamped` is an own append made while the activation's view of the gate is
  closed. `StaleAppend` is a rejected stale append (a witness step).
- **A foreign write is three steps:** `ForeignAdmit` (stamped with the epoch it is
  admitted under), then `ForeignAlloc` or `ForeignReject` at the shard, then `Land`.
  `ForeignOpen` and `Close` open a window and close it by fence.
- **`OldAlloc`** is the unstamped append, admitted unless `UnstampedAdmitted` says
  otherwise.
- **Rollout:** `LegacyWrite`, `OpenGate`, `Resume`, `LegacyComplete`, `MarkInit`.
- **Environment:** `Duplicate`, `Restart` (loses the in-memory entry and watermark,
  keeps the durable entry and the generation entry), `Reload`, `Recreate` (resets the
  entry and epoch row, raises the floor), and `GcTrim`.

**Feature switches** keep each check small:
- `WithBatch`, `MaxWindows`: the instance. The base configuration has the batch kind
  and two windows; every variant runs the reduced instance (point writes, one window).
- `WithDuplicate`: admits the duplicate. On in every configuration.
- `WithRecreate`, `WithRestart`, `FrozenView` (a superseded activation keeps the marker
  state it last saw), `WithMixedVersion`, `WithGateFlip`, `WithLegacyHandOff`.
- `Ghosts`: enables the witness ghosts. Safety runs keep it off.

**Properties:**
- `ReadPositionHonest`: no running activation's read position passes a durable entry
  it does not hold. Unacknowledged entries count, because a WAL reader applies them.
- `SnapshotHonest`.
- `NoTrimPastAcked`: no trim past an acknowledged append, whoever appended it.
- `StaleEpochNeverAppends`: no append is ever accepted from an epoch below the highest
  raised one, or at or below the retired floor, across a shard restart too.
- `OldNeverRejected`: no unstamped append is ever rejected.

**Checked locally** (tla2tools v1.7.4, `-workers 2`, safety only, all five properties
plus `TypeOK`):

| Configuration | Switches | Distinct states | Depth | Time |
|---------------|----------|-----------------|-------|------|
| `WalLiveAdvance.cfg` (base) | batch, two windows | 4,822,222 | 35 | 118 s |
| `WalLiveAdvanceRecreate.cfg` | `WithRecreate` | 6,328,225 | 34 | 149 s |
| `WalLiveAdvanceRestart.cfg` | `WithRestart` | 7,803,467 | 35 | 180 s |
| `WalLiveAdvanceFrozenView.cfg` | `FrozenView` | 1,354,237 | 33 | 33 s |
| `WalLiveAdvanceMixed.cfg` | `WithMixedVersion` | 2,525,889 | 33 | 57 s |
| `WalLiveAdvanceMixedRestart.cfg` | `WithMixedVersion`, `WithRestart` | 13,948,705 | 35 | 344 s |
| `WalLiveAdvanceGateFlip.cfg` | `WithGateFlip` | 7,609,129 | 33 | 170 s |
| `WalLiveAdvanceLegacyHandOff.cfg` | `WithLegacyHandOff` | 20,828,314 | 37 | 499 s |

All eight hold every property. `MixedRestart` is the reviewer's Mixed+Restart+duplicate
combination (S8) on the reduced instance.

**Combinations no configuration covers.** The reviewer's clean runs on revision 2 stand
as evidence for now: Mixed+Restart+duplicate on the full instance (31,777,796 states),
own unstamped appends with the self-report (17,459,936), repeated unstamped appends
(8,467,580), and the split claim with a max-merge write under `Restart` (7,787,173).
**The promotion member must add every combined configuration the reviewer named to CI,
on a dedicated or measured TLC shard; nothing from phase 2 reaches `main` before that
member lands** (see [Production members](#production-members)).

### Mutations: every guard must break

Each mutation was checked against its target alone, with its `BOUNDS`. The clean arm
of each is the configuration with the same switches, which holds every property
(table above). Traces are the shortest a two-worker breadth-first run found, counted
in states including the initial one.

| Mutation | Removes | Target | Bounds | Trace |
|----------|---------|--------|--------|-------|
| `NoTrimPastAckedLiveAdvance5578` | Everything: #5578's arming rule (born, or within one of the first own append), no claim, no epoch check, no hand-off fence | `NoTrimPastAcked` | base | 11 |
| `NoTrimPastAckedNoEpochCheck` | The shard's epoch check against the entry | `NoTrimPastAcked` | base | 18 |
| `NoTrimPastAckedLazyFence` | The claim raising the entry; the first append raises it instead | `NoTrimPastAcked` | base | 18 |
| `ReadPositionHonestClaimReadableHead` | The allocator head: the claim returns the readable head | `ReadPositionHonest` | base | 16 |
| `ReadPositionHonestNoClaimHeadReplay` | The replay to the claim head | `ReadPositionHonest` | base | 16 |
| `SnapshotHonestNoOwnTracker` | "Every own append applied" | `SnapshotHonest` | base | 13 |
| `ReadPositionHonestNoOpenCheck` | The open-marker guard | `ReadPositionHonest` | base | 12 |
| `ReadPositionHonestRearmWithoutReplay` | The replay to the close heads | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestDrainWithoutFence` | The close's fence; waiting for returns is equivalent | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestDrainPointOnly` | The stamp on batch appends, so the fence covers point writes only | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestStampAtAppend` | The stamp at admission; the append carries the current epoch | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestBulkWithoutHint` | The birth hint | `ReadPositionHonest` | base | 6 |
| `ReadPositionHonestBulkAppendDropsExistingRows` | The relocation of the rows the bulk range already holds (#5610) | `ReadPositionHonest` | base | 4 |
| `ReadPositionHonestRecreateDropsFloor` | The retired floor's raise | `ReadPositionHonest` | Recreate | 17 |
| `StaleEpochNeverAppendsIgnoresRetiredFloor` | The floor check at the raise and at the append | `StaleEpochNeverAppends` | Recreate | 8 |
| `StaleEpochNeverAppendsUnloadedEntryAccepted` | The unloaded-entry rejection | `StaleEpochNeverAppends` | Restart | 12 |
| `StaleEpochNeverAppendsClaimEntryOverwrite` | The max-merge: the claim overwrites the durable entry, checked against the loaded entry only (B3) | `StaleEpochNeverAppends` | Restart | 12 |
| `ReadPositionHonestGenerationNotDurable` | The durable routing-generation entry: a restart resets it (S1) | `ReadPositionHonest` | Restart | 14 |
| `OldNeverRejectedRejectedOnceFenced` | Admitting every unstamped append: the first revision's "reject once fenced" | `OldNeverRejected` | Mixed | 6 |
| `ReadPositionHonestUnstampedNotReported` | Reading past reported unstamped appends before arming | `ReadPositionHonest` | Mixed | 12 |
| `ReadPositionHonestRestartForgetsUnstamped` | The watermark's reset on a shard restart | `ReadPositionHonest` | Mixed, Restart | 14 |
| `ReadPositionHonestNoSelfReport` | An own unstamped append reporting itself (S2) | `ReadPositionHonest` | GateFlip | 13 |
| `ReadPositionHonestResumeWithoutMarker` | The resume opening the marker (B2) | `ReadPositionHonest` | LegacyHandOff | 17 |

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

**The retired floor straddled (found in revision 3).** Splitting the claim opened a
window revision 2's atomic claim did not have. `StaleEpochNeverAppendsIgnoresRetiredFloor`
shows it in eight states: `Begin`, `Create`, `ClaimRow(a1)` (epoch 1), `ClaimWrite(a1)`,
`Recreate` (the entry resets to 0; the floor rises to 1), `ClaimRaise(a1)` (1 is not
below the reset entry), `OwnAlloc(a1)` (an epoch at the floor appends). The floor
checks at the raise and the append close it.

### Redundancy checks

Five guards are kept although the model, with every other guard in place, does not
need them. Each was removed alone and the model stayed clean, so each is classed
redundant in the model (as for #2321), with the reason it is kept:

| Check | Removes | Configuration | Result | Why it is kept |
|-------|---------|---------------|--------|----------------|
| `ReadPositionHonestClaimAdoptsNoCas` | The epoch row's conditional write (#5593's adopt) | base | clean, 158 s | With the three-step claim, the shard's refusal at or below the durable entry catches a same-epoch claimant at the entry write. The conditional write is kept so a stale claimant stops before touching any partition, and because the production claim spans several partitions in sequence, which the model's single partition cannot show. |
| `ReadPositionHonestArmsWithoutClaim` | The explicit claim guard in `Armed` | GateFlip | clean, 171 s | Implied by the self-report: an activation that never claimed has only unstamped own appends, each of which reports itself, so it cannot arm past them. Kept, as the review asked, because arming without a claim is meaningless and the implication depends on every unstamped path self-reporting. |
| `ReadPositionHonestNoMarkerInit` | The "marker initialised" guard in `Armed` (B2 rule 2) | LegacyHandOff | clean, 493 s | Implied by rule 1 in the model, whose only legacy hand-off cannot admit a stamped write before `Resume` opens the marker. Kept because a resume path the census misses would otherwise read as closed. |
| `StaleEpochNeverAppendsRaiseIgnoresRetiredFloor` | The floor check at the raise only | Recreate | clean, 158 s | The append's floor check catches the straddling claim's first append. Kept so the stale claimant stops at its raise. |
| `StaleEpochNeverAppendsAppendIgnoresRetiredFloor` | The floor check at the append only | Recreate | clean, 156 s | The raise's floor check refuses the straddling claim, and an old incarnation's raised claim is below the re-created leaf's entry. Kept as the check on the hot path that does not depend on the raise. |

Together, removing both floor checks fails (`StaleEpochNeverAppendsIgnoresRetiredFloor`,
above). The redundancy checks' mutation files are not committed with the draft; their
changes are as described, and they become committed redundancy rows when the draft is
promoted to a module.

### Reachability: the clean run is not vacuous

Each witness is an invariant that must be violated, with `Ghosts = TRUE` and the switch
shown, proving its step is reachable. Traces are from a single-worker breadth-first run.

| Witness | Switch | Shortest trace |
|---------|--------|----------------|
| `WitnessStaleAppendRejected` | - | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise` (`a1`), `Duplicate`, `ClaimRow`, `ClaimWrite`, `ClaimRaise` (`a2`), `StaleAppend(a1)` |
| `WitnessDuplicateAppends` | - | `Begin`, `Create`, `Duplicate`, `ClaimRow`, `ClaimWrite`, `ClaimRaise` (`a2`), `OwnAlloc(a2)` |
| `WitnessForeignAppendInOpenWindow` | - | `Begin`, `ForeignAdmit`, `ForeignAlloc` |
| `WitnessAdmittedForeignWriteFenced` | - | `Begin`, `Create`, `ForeignAdmit`, `Close`, `ForeignReject` |
| `WitnessLiveAdvance` | - | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `Close`, `OwnAlloc`, `Land`, `LiveAdvance(a1)` |
| `WitnessLiveAdvanceAfterHandOff` | - | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `ForeignAdmit`, `ForeignAlloc`, `Land`, `Close`, `OwnAlloc`, `Land`, `Replay(a1)`, `LiveAdvance(a1)` |
| `WitnessDuplicateLiveAdvance` | - | `Begin`, `Create`, `Close`, `Duplicate`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OwnAlloc`, `Land`, `LiveAdvance(a2)` |
| `WitnessTrimAfterLiveAdvance` | - | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `Close`, `OwnAlloc`, `Land`, `LiveAdvance`, `Capture`, `GcTrim` |
| `WitnessClaimRefused` | - | `Begin`, `Create`, `ClaimRow(a1)`, `Duplicate`, `ClaimRow(a2)`, `ClaimWrite(a2)`, `ClaimRefuse(a1)` |
| `WitnessUnloadedEntryReloaded` | `WithRestart` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `Restart`, `Reload` |
| `WitnessClaimAboveRetiredFloor` | `WithRecreate` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `Recreate`, `Duplicate`, `ClaimRow(a2)` |
| `WitnessUnstampedAppendReported` | `WithMixedVersion` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OldAlloc`, `OwnAlloc(a1)` |
| `WitnessOwnUnstampedReportsItself` | `WithGateFlip` | `Begin`, `Create`, `OwnAllocUnstamped(a1)` |
| `WitnessLegacyHandOffResumed` | `WithLegacyHandOff` | `Begin`, `Create`, `OpenGate`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `Resume`, `Close`, `OwnAlloc`, `Land`, `MarkInit`, `LiveAdvance(a1)` |

To run a witness, use a configuration with `Ghosts = TRUE` and the switch shown that
checks `TypeOK` and the witness alone, for example
`INVARIANTS TypeOK WitnessStaleAppendRejected`.

## Formal impact

Every existing module, how the design affects it, and why:

| Module(s) | Effect | Reason |
|-----------|--------|--------|
| `WalDurability` | Unchanged; gains a gap entry | Its subject is the replay-based advance, which the design keeps as the fallback. Its single owner activation is always the current epoch, so `Append` never meets a rejection. Extending it would add an activation dimension to every leaf: `SnapshotLoss` is already 17,428,938 states and `TwoFaults` 2,958,083, so the fences go in the companion instead. |
| `WalMove` | Refinement row only | The entry, floor and generation rows live in grain storage, not in the stream a move switches, so a move carries nothing new. The cutover's reactivation of the shard grain is `Restart` here, covered by `StaleEpochNeverAppends` (broken by `StaleEpochNeverAppendsUnloadedEntryAccepted` and `StaleEpochNeverAppendsClaimEntryOverwrite`), the durable generation (`ReadPositionHonestGenerationNotDurable`) and the restarted watermark (`ReadPositionHonestRestartForgetsUnstamped`). |
| `WalPinRetirement` | New rows | Retiring a leaf's pins also retires its epoch entries under the same retirement token, raising the floor first. A re-created leaf's claim exceeds the floor, and the floor is checked at the claim's raise and at every append, so a removal cannot let a surviving duplicate or a straddling claim outrank it. |
| `TreeLifecycle`, `TreeLifecycleAdmission` | New rows | The purge tail retires the entries of the leaves it clears, under the lineage check (#5417), raising the floor first, and deletes their epoch and marker rows. |
| `ShardOwnership`, `ShardOwnershipCutover`, `ShardOwnershipFold`, `ShardOwnershipCrdt`, `ShardOwnershipRetention` | New rows or gaps | The adaptive split's and the shard fold's shard-level markers open around their windows (or at their resume, for one in flight across the gate) and close with a routing-generation re-claim. A marker only holds the live advance and never blocks a move, a cutover or a fold, so ownership safety is unchanged. A write rejected at a close is re-admitted at its seam. |
| `SplitLink`, `SplitLinkTurns`, `BPlusTopology`, `BPlusReclaimRecovery`, `BPlusCascade` | New rows or gaps | The split opens the sibling's marker in its birth (or at `CompleteSplitAsync`'s resume), and the donor closes it at completion after re-claiming its epoch. That is one more call, made from the donor's completion; it waits on the WAL shard, not on another leaf. The reclaim opens the survivor's marker before widening, and retires the neighbour's entry in the retire step. |
| `SplitGateCycles`, `TurnOccupancy` | New rows | A leaf's fresh read of the shard-level marker is a new leaf-to-shard-root wait-for edge. It is an interleaving, read-only call, so a serial shard-root turn that calls the leaf cannot deadlock against it. Both modules gain a row stating that the edge does not take the shard root's serial turn. |
| `BackupRestore`, the bulk-load paths | Rows | A restore and an offline snapshot load through `BulkLoadRawAsync`, so their leaves take the birth hint (persisted in `_lattice_shard-handoff`), the open marker, the relocation of existing rows and the switch's fence. |
| `ReplayPermits`, `WalGcStarvationDrive`, `WalGcStarvationDriveWitness`, `MaterialiserDrain` | Demand abstraction only | The design lowers replay demand. These modules bound permits and drives for any demand, so their properties are unaffected. Their notes gain a line that live-advanced leaves no longer drive. |
| `WalAdmission`, `WalBackPressure` | Gap entry | A stale-epoch or fenced-out rejection is a new internal append outcome, distinct from admission refusal under load and retried at its seam. It fails one stale or pre-close writer, never a saturated partition, and an unstamped append is never refused. So the admission and back-pressure properties are unaffected. |
| `AtomicCommit`, `AtomicCommitCrossCluster` | Unchanged; rows restated | A prepare rejected by a fence is retried by the participant under its current stamp, never turned into a nack, so the saga's outcome is the one 10.2.0 reaches. A coordinator terminal carries the terminal stamp class, admitted and excluded from the watermark; safety holds through the leaf-wide prepare clamp: the current activation does not advance while the prepare is pending, and it learns the terminal by replaying the coordinator's record. `BroadcastStep` assumes only eventual delivery. |
| `SagaReadProgress`, `TagIndexReconcile`, `ReplicationShipping` | Unaffected | They consume accepted WAL records, which are unchanged; a rejected append never becomes a record. |
| `AntiEntropy`; `Replication`, `ReplicationCausalDelivery`, `ReplicationLowWatermark`, `ReplicationReBootstrap` | Unaffected | They consume accepted WAL records, whose shape is unchanged: range deletes are fenced, not clipped. A rejected apply is retried below the dead-letter accounting. |
| `MaterialisedView*`, `view-*` modules | Unaffected | Views read accepted WAL records only, unchanged. |
| `AliasRevert`, `AliasRouting`; `Schema*`, `SchemaViewComposition`; `TenantIsolation`, `TenantFootprint`, `TenantQuotaEvaluation`, `TenantTreeCount`, `TenantRateBudget` | Unaffected | Routing, schema and tenancy decisions are made before a write reaches a leaf. |
| `BackupCapture`, `BackupCutover`, `BackupIncremental`, `BackupProvenance`, `BackupRetention` | Unaffected | They read accepted records and leaf state. |

## Production members

Each production member carries detectors proven red. They land in this order:

1. **Epoch claim, stamped appends and the gate.**
   - The epoch, entry, floor and generation rows; the three-step claim with its
     conditional max-merge; `ClaimAppendEpochAsync`, `AppendStampedAsync` and
     `AppendBatchStampedAsync`; the unloaded-entry reload; the unstamped watermark and
     the own-append self-report; the floor checks at the write, the raise and the append.
   - `WalAppendEpochStaleException` (internal) and the seam retry of section 5 for every
     caller in its table, each with a detector that the caller's 10.2.0 outcome is
     unchanged: an atomic write commits, `DeleteAsync` and a CRDT delta succeed, and
     replication apply counts no failure.
   - Entry retirement in pin retirement and the purge tail, run regardless of the gate,
     and the orphaned-row sweep.
   - `IWalAppendFenceCapable` and its manifest gate; the mixed-version test pinning a
     10.2.0 silo's "method unknown" failure.
   - JSON and binary round-trip tests for every new state type. A test that every
     10.2.0 grain interface method still exists with its signature.
2. **Hand-off markers and fences.**
   - Split, leaf reclaim, adaptive split, shard fold, and the tree-level range-delete
     fence with its intent and re-drive.
   - Resume opens the marker, and the marker-initialised state.
   - The shard routing generation, stamped at admission; the unstamped leaf methods
     appending unstamped.
   - The stamp census, including the terminal stamp class.
3. **Bulk.** #5610's relocating graft, fixed with #5614; the birth hint captured with
   it and persisted in `_lattice_shard-handoff`; bulk-leaf ranges; and the switch's
   marker and fence. **Pending the user's authorisation of change 4.**
4. **The live advance itself.**
   - Armed only by the rules above.
   - It reuses #5578's applied-prefix tracker and the core extraction from the
     abandoned #5596.
5. **Promotion.** `WalLiveAdvance` becomes a module with its refinement note, its
   manifest, its redundancy rows, and **every combined configuration the reviewer named**
   (Mixed+Restart+duplicate on the full instance, own unstamped appends with the
   self-report, repeated unstamped appends, the split claim under `Restart`, and the
   eight configurations above) **in CI, on a dedicated or measured TLC shard.** Nothing
   from phase 2 reaches `main` before this member lands.

## Review resolution

### Re-review of revision 2

| Finding | Resolution |
|---------|------------|
| B1 range delete fenced per shard | Section 2, "Range delete: fenced at the tree": a durable intent, a range-scoped marker opened on every shard root before any shard appends, closed by fence only after every walk has finished or been abandoned, re-driven by a per-tree reminder. The cost (every overlapping leaf of the tree disarmed for the delete) is stated. The model's foreign author stands for every shard's walk. |
| B2 hand-off in flight across the gate | Section 2, "Hand-offs in flight across the gate": resume opens the marker; absence never means closed, a positive initialised state is required. Modelled by `WithLegacyHandOff` (`LegacyWrite`, `OpenGate`, `Resume`, `LegacyComplete`, `MarkInit`); the window opens with no marker before the gate and continues after it. Clean, 20,828,314 states. Mutation `ResumeWithoutMarker` (17 states); rule 2 is a redundancy row. |
| B3 claim entry write not monotone | Section 1: the claim is three steps, the entry write is a conditional max-merge refused against the higher of the loaded entry, the durable entry and the floor. The model splits `Claim` into `ClaimRow`, `ClaimWrite`, `ClaimRaise`. Mutation `ClaimEntryOverwrite` (12 states). Splitting the claim also exposed a retired-floor window, now closed (`IgnoresRetiredFloor`, 8 states). |
| B4 rejections escaping `ShardActivationRetry` | Section 5: retried at the seam that raises them, until the operation's existing deadline; a table of every caller of the leaf write surface, its exposure and its retry. Saga prepares retry rather than nack. Changes 1 and 2 restated. |
| S1 routing generation not durable | `_lattice_wal-routing-generation`, with the leaf entry's semantics. The model keeps `fFence` across `Restart`. Mutation `GenerationNotDurable` (14 states). |
| S2 gate flips | Own unstamped appends self-report; an explicit claim guard in `Armed`. `WithGateFlip` configuration, clean (7,609,129). Mutation `NoSelfReport` (13 states); the claim guard is a redundancy row. |
| S3 stamp contract | Unstamped leaf methods append unstamped. `AppendBatchStampedAsync` named, with per-entry stamps checked under `_stateGate` and a per-entry outcome. |
| S4 racing the gate | Pinned by a mixed-version test in member 1; re-issued unstamped only on the definite "method unknown" failure; anything else follows today's retry semantics. |
| S5 #5610 who wins | Last-writer-wins: the acknowledged point write wins, and keys only the pre-existing rows hold become readable. Change 4, a documented-behaviour change, pending the user's authorisation before the bulk member; "bulk value wins" is the alternative. |
| S6 coordinator terminals | The terminal stamp class, excluded from the watermark, justified by the prepare clamp and `_recentlyTerminal`; not modelled (no sagas), so a gap entry and a member 2 detector. |
| S7 bulk hint across a crash | Persisted in `_lattice_shard-handoff` by operation id; the resume reuses it. |
| S8 configurations | Eight configurations, including `MixedRestart`, `GateFlip` and `LegacyHandOff`; the split claim and the durable generation are in the base model. The reviewer's larger combinations are a promotion-member CI requirement. |
| N1 orphaned rows | Retirement runs regardless of the gate; a sweep removes orphaned rows. |
| N2 abandoned flushes | Section 1, "Abandoned flushes (#4621)". |
| N3 floor cost | Section 1, "Cost". |
| Q1 census start | The census table in section 2. |
| Q2 bounded wait | Optional, a latency optimisation only. |
| Q3 change 3 | Withdrawn; the seam retry runs to the existing deadline. |

### First review

| Finding | Resolution |
|---------|------------|
| B1 drains | Section 2: the close is a fence (a re-claim of the initiator's epoch or routing generation), stamped at admission, on every append path. Mutations: `DrainWithoutFence`, `DrainPointOnly`, `StampAtAppend`. |
| B2 epoch floor | Section 1, "Retiring entries": a monotone per-partition floor row, raised before an entry is deleted, checked at every claim step and append. Mutations: `RecreateDropsFloor`, `IgnoresRetiredFloor`. |
| S1 claim head and tracker | The claim reads `_nextOffset` in the lock section that assigns offsets; arming requires every own append applied. Mutations: `ClaimReadableHead`, `SnapshotHonestNoOwnTracker`. |
| S2 CAS | A strict conditional write on a row only claims write. Now a redundancy row (`ClaimAdoptsNoCas`), because the three-step claim's entry write refuses a same-epoch claimant. |
| S3 guard mutations | `NoClaimHeadReplay` and `NoOpenCheck`; `PERTURBS` fields name actions. |
| S4 overlap | Section 4's fail-safe list: slot-less, rangeless-with-siblings and suspected-orphan leaves. |
| S5 fold vs reclaim | Separate rows in the hand-off table. |
| S6 #5610 | Fixed by relocation in the bulk member. Mutation: `BulkAppendDropsExistingRows`. |
| S7 storage, cost, moves | Reserved-type rows, lazy loading, costs quantified; restarts modelled. |
| S8 rollout | The manifest gate, not ratcheted; unstamped appends admitted and reported. |
| S9 impact rows | `AtomicCommit`, `TurnOccupancy`, `SplitGateCycles`, `WalMove` rows. |
| N1-N5 | `FrozenView`; the README; conflicts are claims; the hint is needed by every arming; the duplicate's live advance and a trim after a live advance are witnesses. |
| Q1-Q3 | The floor; the shard-level marker read fresh; range deletes fenced, not clipped. |

## Open questions for re-review

1. **Tree-level range-delete fence.** The fence opens a marker on every shard of the
   tree, so a range delete disarms every overlapping leaf of the tree for its duration.
   A per-tree range-delete generation that every leaf reads fresh is the alternative
   the review offered; it trades the per-shard marker writes for one more fresh read per
   advance tick on every leaf. Is the marker form preferred?
2. **Terminal stamp class.** The exclusion rests on the leaf-wide prepare clamp and
   `_recentlyTerminal`, which the model does not cover. Is a gap entry with a member 2
   detector enough, or should the promoted module add a prepare and its terminal?
