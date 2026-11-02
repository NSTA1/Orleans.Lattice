# Live checkpoint advance: fenced redesign

Design for issues #5572 and #5593, epic #5605. This is a design draft for review:
no production code implements it yet. `WalLiveAdvance.tla` beside this note models
it, with variant configurations and its mutations in `mutations/`. A module under
`spec/` must cite a production detector on every behaviour row, so this draft sits
outside module discovery. It becomes the companion module `spec/wal/WalLiveAdvance`,
with a refinement note, when the production members land.

**Gate.** A production member starts only once the independent reviewer has accepted
the part of the design it builds. The re-review of revision 3 accepted member 1 (the
epoch claim, stamped appends, the gate, seam retries, the floor, and the generation and
epoch rows). The re-review of revision 4 accepted member 2, except the tree range-delete
fence's re-stamp path, which waits for the reviewer to confirm this revision's model
change. Members 3 and 4 follow member 2.

**Revision.** This is the fifth revision. It answers the
[re-review of revision 4](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6566046196)
(1 blocking, 3 should-fix, 2 nits):
- **The re-stamp is atomic with the close (B1).** The fence grain marks an intent closed
  before the close's first re-claim, and answers the open check and the generation from
  that one state, in turns that do not interleave with a close. The model gains
  `GRestamp` and `GUnstamped` and a two-window configuration, and the split close is a
  mutation. Section 2, "A walk outliving its intent".
- **The intent's deadline** takes the `SagaDecisionDeadline` form and affects liveness
  only (S1). The shard root asks the fence grain only after a rejection, never per page
  (S2). The cost of unstamped walks is stated, with a counter by source (S3).
- **The retired floor is checked at the claim only, never on an append (#5635).** The
  floor is one value per partition while epochs are per leaf, so an append check would
  supersede live leaves after another leaf's retirement. Member 1 found it. The model
  gains `RetireOther` and `CurrentNeverFenced`, with a mutation. Section 1.
- **Member plan:** #5634's epoch-row sweep is member 5; the terminal class (member 2)
  lands before member 4 arms any leaf.

Revision 4 answered the[re-review of revision 3](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6564904377)
(2 blocking, 4 should-fix, 3 nits), which accepted revision 3 for production member 1
and held member 2 for this revision:
- **The range-delete fence** stamps every shard's walk with one per-tree range-delete
  generation, owned by a new `TreeRangeDeleteFenceGrain`, so one re-claim per partition
  fences every walk (B1). A walk with no open intent is appended unstamped (B2). The
  intent records a deadline, and its reminder has a named owner (S3); the per-step cost
  of stepped range deletes is stated (S4). Section 2.
- **Generation rejections** are retried from the shard root through its routing check
  (S1, section 5). The terminal stamp class is accepted only for `TxCommit` and
  `TxAbort` (S2).
- **Change 4** (a bulk append over a key the range already holds) is now last-writer-wins,
  the point write wins, which is what 10.2.0 converges to on every reader: a compatible
  bug fix needing no authorisation (section 3, "Which value wins").
- **The model** gains `WithTreeWalk` (another shard's walk) and `ContestedKeyAgrees`
  (origin and replica agree on a contested key), with their mutations and witnesses;
  the redundancy checks are committed in `redundancy/`.

Revision 3 answered the
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
- No change needs the user's authorisation. Change 4 (bulk append over a key the range
  already holds) is last-writer-wins, which is what 10.2.0 converges to on every reader.

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
| `_lattice_wal-routing-generation` | WAL shard key and initiator id (a shard root, or a tree for its range-delete generation) | The partition's generation entry for a shard-level initiator, or for the tree's range-delete fence | The initiator's re-claim (conditional max-merge) |
| `_lattice_leaf-handoff` | Leaf id | The leaf's hand-off marker: initialised flag, open tokens and per-partition close heads | Hand-off initiators, and the leaf's birth |
| `_lattice_shard-handoff` | Shard root id | The shard-level marker (initialised flag, open tokens with their key ranges, close heads), the shard's routing generation, and each in-flight bulk operation's birth hint | The shard root, and the range-delete fence |
| `TreeRangeDeleteFenceGrain` state (a new grain type, not a reserved-type row) | Tree id | The tree's range-delete generation, and each open range-delete intent: operation id, range, recorded deadline, and the shards whose markers it opened | The range-delete fence grain only |

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

The shard accepts a stamped entry only if the leaf's entry is loaded and the epoch is
not below it. It does **not** check the retired floor on the append path (#5635). A
stale epoch is rejected with
`WalAppendEpochStaleException`, an internal exception (section 5). An entry not yet
loaded rejects the append as unloaded (below); the leaf re-presents its claim and
retries. The stored record does not change: the stamp is a call argument, never part
of the record.

**The retired floor at the claim, not the append (#5635).** A purge and re-creation of
the leaf resets its entry, so a claim written before the retirement and raised after it
would pass an entry-only check. The floor is therefore checked at the claim's entry
write and at its raise. Dropping the raise check fails in 8 states
(`StaleEpochNeverAppendsIgnoresRetiredFloor`).

The floor is **not** checked on appends. It is one value per partition, while epochs
are per leaf: checking it on the append path would reject every live leaf of the
partition whose epoch is at or below an epoch **another** leaf retired, superseding
leaves that nothing superseded. Revision 4 specified that check; production member 1
(#5633) found the over-retirement and removed it (#5635). A retired incarnation's own
appends do not need it: retiring the leaf deletes its entry, so its appends are rejected
as unloaded and its re-presented claim is refused by the floor.

**The condition that makes the append check unnecessary.** Retirement must remove the
WAL shard's **loaded** entry, not only its rows. It therefore goes through the WAL shard
activation (`RetireAppendEpochAsync` in #5633), which, in order: takes the per-leaf gate,
serialising it with that leaf's claims; raises the durable floor; deletes the entry row;
drains any in-flight stamped batch; and removes the entry from the loaded entries under
`_stateGate`. An unloaded entry rejects every stamped append (`UnloadedEntry`). Every
retirement path, the leaf's clear and the WAL GC's orphan-pin retirement, goes through
it. A retirement that only wrote rows would leave the loaded entry admitting a surviving
activation of the retired incarnation, and would need the append check back
(`StaleEpochNeverAppendsRecreateKeepsLoadedEntry`, 9 states). In the model,
`AppendAdmits` is the entry check alone, `RetireOther` (`WithOtherRetire`) raises the
shared floor above epochs L holds, and `CurrentNeverFenced` states that the
floor never rejects an append L's own entry admits.
`CurrentNeverFencedFloorAtAppend` puts the floor back on the append path and
fails.

**A floor-only refusal bumps and re-claims.** The claim-time checks are partition-wide
too: a claim between its row write and its raise when another leaf is retired with a
higher epoch is refused by the floor alone. That activation is not stale, so it does not
stop. It persists the higher of its row and the floor plus one to its epoch row,
conditionally on the row it wrote, and claims again, at most
`LeafAppendEpochFence.MaxClaimRounds` (8) rounds, after which it appends unstamped
(reported, section 4). Only a refusal by the leaf's own entry, or a moved row, means
another activation claimed, and stops it. The model has `ClaimBump` for the floor-only
refusal (`WitnessClaimBumped`); it bounds the rounds by `MaxEpoch` and stops a bump that
has no room, standing for the unstamped fallback, which `WalLiveAdvanceGateFlip.cfg`
covers. Cost: one extra claim round per claim in flight at another leaf's retirement in
the same partition (N3, below).

**One epoch per activation, across partitions.** An activation has one epoch, stamped on
every partition it writes. A floor-only bump forced on partition P2 therefore changes the
epoch it stamps on P1, where that epoch was never claimed. P1 admits it, because it is
above P1's entry. That is safe: any other activation must first win the leaf's epoch row,
and its claim on P1 then exceeds the bumped epoch. `OtherPartitionBump`
(`WithSecondPartition`, `WalLiveAdvanceTwoPartition.cfg`) bumps a raised activation's
epoch from another partition at any time, and every property holds
(`WitnessOtherPartitionEpoch`).
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
path a new silo runs: every leaf clear (purge, reclaim, orphan repair) and the WAL GC's
orphan-pin retirement, which also re-drives an interrupted clear. A retirement on a
10.2.0 silo, during a rollback or a rejoin after the gate opened, deletes none of these
rows, and it also removes the leaf's materialiser pins, so the orphan-pin pass never
finds the leaf again; `IGrainStorage` cannot enumerate rows, so the rows would leak for
good (#5634). The leftover rows are safe (a leaked entry only ever sits above a
re-claim, and a leaked epoch row makes a re-created leaf continue from a higher epoch),
so the cost is storage only. A **per-partition epoch directory row** makes a
partition's entries enumerable, and a **sweep** walks it and retires the entries of
leaves that no longer exist (review N1). The directory row is bucketed or paged to
stay within the provider's row limits (Azure Table: 64 KiB per property, 1 MiB per
entity). It is built in its own production member (#5634), which lands before #5605's
bucket reaches `main`.

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
- **Per append:** one dictionary lookup and one comparison under the lock the append
  already holds, per stamp. No allocation and no grain call. A stamped entry carries 24
  more bytes on the request: leaf identity, epoch and routing generation. The response
  carries the 8-byte unstamped watermark, and a batch response the per-entry outcome.
  The stored record is unchanged.
- **Per activation:** one conditional row write, plus one claim call, one entry-row
  read and one entry-row write per partition it writes. A fresh estate of N leaves
  writing P partitions costs at most N x P entry-row writes once, not O(N^2).
- **After a retirement (N3):** the floor is one scalar per partition, so after any
  leaf's retirement every new leaf whose epoch row is older than the floor pays one
  refused claim per partition it writes, then bumps and claims above the floor; and every
  claim in flight in that partition when another leaf is retired with a higher epoch pays
  one extra round. Appends are never affected (#5635).
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
append but does **not** report in the unstamped watermark. **The WAL shard accepts the
terminal class only for records whose `Op` is `TxCommit` or `TxAbort`** (S2); a record
of any other kind sent with it is admitted unstamped and reported, so the class cannot
be used to bypass a fence or the watermark. Excluding it is safe
because of two guards the leaf already has: the leaf-wide prepare clamp (#5572), which
holds the advance on every partition while any prepare is pending; and
`_recentlyTerminal`, which makes a terminal applied after its prepare resolved
idempotent. A terminal only ever resolves a prepare the leaf itself recorded, so the
advance never passes a terminal whose prepare is still pending, and passing one whose
prepare has resolved changes nothing. Without the class, every saga would raise its
partitions' watermark and send every leaf writing them back to replay, the demand
#5569 and #5570 are about. The exclusion is sound because a `TxCommit` or `TxAbort`
record carries only its `TransactionId` and no row data (`WalRecord.cs`): replaying one
with no pending bucket is a no-op. The model has no sagas, so this rests on the clamp; it
is a gap entry in the refinement note, member 2's detector proves a terminal-class
append never raises the watermark and never passes a pending prepare, and the promoted
module adds one prepare and its terminal.

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
| Range delete | The tree's range-delete fence grain (below) | A range-scoped marker on **every** shard root of the tree, before any shard appends | The end of every shard's walk, or the intent's recorded deadline | The fence grain re-claims the tree's range-delete generation at every partition once; the heads close every shard's marker |
| Bulk load / append | The shard root, for writes admitted before the switch | At the bulk leaf's birth, with the birth hint | The switch, in the shard root's serial turn | The shard root re-claims its routing generation at every partition; the heads close the marker |

**Range delete: fenced at the tree.** A range delete's record is replayed by every
overlapping leaf of every shard, not only by the authoring shard's leaves. A
shard-scoped fence therefore fails after a partial fan-out: shard A's walk appends,
shard B's walk fails, and B's armed leaves pass A's record and never delete those keys,
a permanent divergence from replicas and views that 10.2.0 does not have (it converges
at B's next replay). So the fence is tree-level, and is owned by a new per-tree grain,
`TreeRangeDeleteFenceGrain`, keyed by tree id.

**One generation for every walk (revision 4, B1).** Every shard's walk of a fenced range
delete is stamped with the **tree's range-delete generation**, not with its own shard's
routing generation. Revision 3 stamped each walk with its shard's generation and closed
the shards one at a time. That is unsound when a walk is abandoned: shard A's walk,
still in flight, is stamped with A's generation, which B's close does not re-claim, so
A's record can be allocated above B's recorded heads, and B's leaves, which replay
every overlapping range-delete record, re-arm and pass it
(`ReadPositionHonestRangeDeletePerShardClose`, 13 states). With one generation, one
re-claim at each partition fences every shard's walk at once, and the heads it returns
are valid for every shard's marker. No two-phase close is needed.

1. **Intent.** `LatticeGrain.DeleteRangeAsync` asks the fence grain to open an intent
   before any shard appends. The intent records the operation id, the range, and a
   **deadline**, taken once when the call starts in the form of
   `LatticeGrain.SagaDecisionDeadline` (`LatticeGrain.cs:3448`): the silo's
   `SiloMessagingOptions.ResponseTimeout` less a margin. A `CancellationToken` carries no
   deadline to read. The fence grain persists the intent in its own state and returns
   the tree's current range-delete generation.
2. **Open.** The fence grain opens a range-scoped marker, idempotently, in the
   shard-level marker row of **every** shard root of the tree, and records each in the
   intent. Every shard's B+ tree spans the whole key space, so every shard can hold an
   overlapping leaf.
3. **Fan out.** `LatticeGrain` runs the shard walks as today, with one issue HLC for
   all shards, through a **new stamped shard-root method** that names the intent and
   carries the generation. The shard root **trusts the generation it is given** and
   stamps the walk's leaf appends with it at admission. It does not ask the fence grain
   per page: the WAL shard's generation check already rejects a walk whose intent has
   closed, and only that rejection's seam retry asks the fence grain.
4. **Close.** When every walk has returned, or the intent's deadline has passed, the
   fence grain re-claims the tree's range-delete generation at every partition (the
   re-claim returns allocator heads), records the heads in every shard's marker, closes
   the markers, and deletes the intent.
5. **Re-drive.** The fence grain owns one reminder per tree, registered while any
   intent is open and unregistered when none is. Each tick closes every intent whose
   deadline has passed. `LatticeGrain` is a stateless worker, so if it dies mid-fan-out
   nothing marks the fan-out ended; the recorded deadline is what lets the reminder
   tell "ended" from "still running".

**The deadline affects liveness only.** A close that comes before every walk has
finished is safe: the walk's next stamped append is rejected and, with the intent
closed, the walk goes on unstamped and reported. The cost is that a range delete
outlasting one response timeout, such as a paged drain
(`LatticeGrain.DrainShardRangeDeleteAsync`), runs its later pages unstamped, which
disarms the partitions they reach until their leaves replay past them.

**A walk with no open intent is appended unstamped (B2).** A shard root stamps a
range-delete walk only when the call comes through the stamped method with an intent's
generation (step 3). A
range-delete walk that reaches a shard root through the **existing** shard-root method
is appended **unstamped**, so it is admitted and reported (section 4), never stamped
under a fence that holds nothing. That covers, without any resume rule:
- **replication apply:** `LatticeGrain.ApplyDeleteRangeAsync` fans out with
  `shard.DeleteRangeAsync` to every shard (`LatticeGrain.ReplicationApply.cs:338`);
- **resize and reshard shadow forwards:** `ShardRootGrain.DeleteRangeBoundedCoreAsync`
  forwards to the destination tree (`ShardRootGrain.cs:2070`);
- **a fan-out spanning the gate opening:** a `LatticeGrain` fan-out begun while its view
  of the gate was closed opened no intent, so its later walks go through the existing
  method;
- **a 10.2.0 caller.**

These stay unstamped by design, rather than being routed through the fence. The cost is
bounded: an unstamped walk raises the unstamped watermark on each partition it reaches,
and every armed leaf writing those partitions then replays from its checkpoint past it.
A live-advanced leaf's checkpoint is near the head, so that is a tail replay, never a
replay from -1 and never more than 10.2.0 already does on every advance. The busiest
case is a replicated repocontext tree, whose `RepoContextStore` range-deletes in steps
(`:1187`, `:1587`), so each step reaches each peer as an unstamped walk. A
predicate-filtered replicated delete is not affected: it ships the matched keys, which
the receiver applies as point tombstones through `ApplyMergeManyAsync`, stamped as
usual. A counter of unstamped reports by source makes the cost observable (change 7).
Stamping a walk with no open intent fails (`ReadPositionHonestRangeDeleteWithoutIntent`,
13 states).

**A walk outliving its intent (B1).** A walk still running after its intent closed has
its next stamped append rejected by the re-claimed generation. The shard root's seam
retry then asks the fence grain whether an intent is open. If one is (another delete,
whose close re-claimed the shared generation), the walk is re-stamped with the
generation the fence grain returns; if not, it appends unstamped, so the walk completes
as it would in 10.2.0. Two concurrent range deletes on one tree share the generation,
so one's close rejects the other's in-flight appends once, and each is retried this
way.

That is sound **only if the open check is atomic with the close**. The close is not one
instant: it re-claims the generation at every partition (several awaits), records the
heads, and closes the markers. An open check answered between the first re-claim and
the marker close says "open", the walk re-stamps with the **post-close** generation, its
append is accepted above the heads the close already recorded, and the owner re-arms
after replaying to those heads and passes it: the divergence the fence exists to
prevent (`ReadPositionHonestRestampDuringClose`, 16 states). Two rules close it:
1. **The intent is closed before the close's first re-claim.** The fence grain marks
   the intent closing, durably, in the same turn that begins the close. From then on
   the open check answers "not open".
2. **One source for both answers.** The open check and the generation it hands out are
   answered by the fence grain from that single state, in turns that do not interleave
   with a close: `TreeRangeDeleteFenceGrain` is non-reentrant, and its open check is
   neither `[AlwaysInterleave]` nor `[ReadOnly]`. No shard root caches either answer.

In the model, `GReject` leaves a rejected walk to `GRestamp` (an intent is open and
not closing: re-stamp with the current generation) or `GUnstamped` (otherwise: append
unstamped and report it). `closing` marks a close between its re-claim and its marker
close; it is always false in the model, because `Close` marks the intent closed
atomically with the re-claim, which rule 1 makes true in production. The mutation splits
`Close` into the re-claim and a later `CloseMark` and lets the open check read the
marker alone. `WalLiveAdvanceTreeWalkConcurrent.cfg` (two windows) is the case where a
re-stamp under a second delete's intent is reachable (`WitnessTreeWalkRestamped`).
The record is unchanged (it is not clipped; see
[Backward compatibility](#backward-compatibility)). A leaf whose range is disjoint
from an open range-delete marker ignores it, by the same overlap test the replay judge
uses (`IsDisjointRangeDelete`).

**Cost per range delete call (S4):** one fence-grain call to open and one intent write;
one marker write per shard to open and one to close; one generation re-claim per WAL
partition, once, not once per shard; and the reminder only while an intent is open.
**Every overlapping leaf of the tree is disarmed for the call's duration**, advancing by
replay. Stepped range deletes pay that cost **per step**, because each step is its own
`DeleteRangeAsync` call with its own intent:
- cursor deletes (`LatticeCursorGrain.cs:569`, `:582`);
- `RepoContextStore` (`:1187`, `:1587`);
- the vector index (`LatticeVectorIndexStore.cs:291`);
- view maintenance (`ViewMaintainerGrain.cs:1587`).

Between steps the markers are closed, so overlapping leaves re-arm after a replay to
the step's close heads. They are not held across the whole stepped operation. A caller
that wants one fence for a whole stepped operation is a later optimisation, not part of
this design.

**In the model,** the foreign author `f` stands for the walk on the owner's own shard,
and `g` (`WithTreeWalk`, `WalLiveAdvanceTreeWalk.cfg`) for another shard's walk. Both
are stamped with `fEpoch`, the tree's generation, and are admitted only under the open
marker. `Close` re-claims it once and fences both.

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

**Which value wins (S5): last-writer-wins, the point write wins.** What 10.2.0 does
with a key both a bulk chunk and an earlier point write hold:
- **Bulk entries are WAL records at versions below every real write.**
  `BulkAppendAsync` ticks its entries' versions from `HybridLogicalClock.Zero`
  (`ShardRootGrain.BulkLoad.cs:440`, `:460`, `:493`) and writes them through
  `MergeEntriesAsync` (`BPlusLeafGrain.cs:3703`), which appends a merge record to the
  WAL and resolves last-writer-wins by timestamp. A point write is versioned by its
  leaf's real clock (`BPlusLeafGrain.cs:687`), so it outranks a bulk entry on every
  reader that applies both.
- **Keys that fill the existing rightmost leaf's free space** (`ShardRootGrain.BulkLoad.cs:454-465`)
  are merged into the leaf that already holds the point row: the **point value wins**,
  immediately and permanently.
- **Keys placed in new bulk leaves.** The origin returns the bulk value only while the
  point row is stranded on the old leaf. Replicas and views apply both records and
  return the **point value** now, so in 10.2.0 the origin and its replicas disagree on
  the key. The origin itself flips to the point value when a rangeless bulk leaf
  replays, because it owns every key of its shard during replay
  (`ShardRootGrain.BulkLoad.cs:290`).

So 10.2.0 converges to the point value on every reader and for every free-space key;
the one exception is the origin's read of a new-leaf key before a replay, which
already diverges from its replicas. The relocation keeps that convergence and removes
the exception: the acknowledged point write is returned on the origin as on every
replica and view, keys only the pre-existing rows hold become readable again, and
counts and scans become exact. This is a **compatible bug fix** (change 4): it fixes
the origin-versus-replica divergence and needs no authorisation. The model states it
as `ContestedKeyAgrees`: the owner resolves the contested key to the value a WAL
reader resolves at the same position. `ContestedKeyAgreesBulkWinsLocally`, where the
owner ranks its born bulk entry above a point write made before the switch (10.2.0's
stranded origin read), fails in four states.

**Rejected: a stable "bulk value wins".** Dropping the superseded point row on the
owner is unsound: replicas and views still apply the point record at its real version,
so origin and replicas would disagree for good. The only replay-consistent form is to
stamp the contested bulk entries above every earlier version, with the relocation
narrowing the old leaf and reading its clock in one step. That flips every free-space
key, and every replica's read of a new-leaf key, from the point value to the bulk value,
a documented-behaviour change on paths where 10.2.0 is already stable. The user may
still choose it later; it would need that authorisation and a value-version layer in
the model. #5610 and #5614 are fixed together in the bulk member, which confirms
whether they share a cause.

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

Where each kind of rejection is retried (S1):
- **A stale leaf epoch.** The rejected activation deactivates, and the retry is a fresh
  call to the same grain id, which reaches the live activation. A leaf-epoch rejection
  at a split close is retried at the leaf, whose span admission forwards a key the
  split moved.
- **A stale shard routing generation** (an adaptive split, a fold or a bulk switch
  closed). This is **always retried from the shard root, never re-stamped at the leaf.**
  The retry re-runs `ThrowIfRejectedForKey`, which raises `StaleShardRoutingException`
  for a slot that moved to another shard, and the existing stale-routing retry sends it
  there; otherwise it re-admits under the current generation. A leaf that re-stamped
  the write would append it on the old shard, where the shard map gives it to no leaf,
  while the new owner's marker is already closed: acknowledged data never applied.
- **A stale tree range-delete generation.** Retried by the shard root's walk step, as
  under "A walk outliving its intent" in section 2.

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
    `AppendStampedAsync`, `AppendBatchStampedAsync`, the stamped leaf-write methods, the
    stamped shard-root range-delete method, `GetHandOffMarkerAsync`, and the new
    `ITreeRangeDeleteFenceGrain`.
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
| 4 | **Bulk append over existing rows (#5610, #5614).** The graft relocates the rows the bulk range already holds into the new bulk leaf, instead of stranding them. No new exception: a chunk that succeeds today still succeeds. For a key both the bulk chunk and an earlier point write hold, the point value is returned last-writer-wins, as 10.2.0 already returns on every replica and view, for every free-space key, and on the origin after a replay; only the origin's pre-replay read of a new-leaf key changes, from a value its replicas never return. Keys only the pre-existing rows hold become readable; counts and scans become exact. | Compatible bug fix (fixes an origin-versus-replica divergence) |
| 5 | **The live advance itself (#5572).** A leaf's checkpoint advances without a replay from -1. The WAL GC trims sooner, replay demand falls, and #5570's "carries no usable offset" warnings stop for live leaves. Existing metrics report different values; none changes name, unit or tags. | Compatible bug fix |
| 6 | **Range deletes cost more.** Each `DeleteRangeAsync` call opens an intent at the tree's fence grain, opens and closes a range-scoped marker on every shard of the tree, re-claims the tree's range-delete generation once per WAL partition, and disarms every overlapping leaf of the tree for the call's duration. The shard root adds no per-page call: it trusts the generation it is given and asks the fence grain only after a rejection. Stepped range deletes pay this per step (section 2). A range-delete walk that stays unstamped (replication apply, a shadow forward, a gate-spanning fan-out, a 10.2.0 caller, or a page after the intent's deadline) costs every armed leaf on the partitions it reaches a tail replay past it. The record, its result and its feed and history output are unchanged. | Compatible (cost only) |
| 7 | **New metrics.** New instruments only, on the existing meter: claims, refusals and rejections by reason, seam retries by caller class, live advances by arming outcome, open markers, and unstamped reports by source (replication apply, shadow forward, gate-spanning fan-out, 10.2.0 caller, census-unstamped, and a range-delete page past its intent's deadline). No existing instrument gains a tag, a tag value, or a new unit. | Compatible (additive) |
| 8 | **New log lines.** A deactivation warning for a stale activation, and a debug line when a partition disarms on an unstamped report. #5571's blocked-pin warning keeps its categories. | Compatible (additive) |
| 9 | **New storage rows** under the reserved grain types listed above, and the `TreeRangeDeleteFenceGrain` state, appear in the host's grain storage, and a sweep removes orphaned ones. The fence grain's reminder adds one reminder-table row per tree while it has an open intent. They are documented in the tree's storage notes. | Compatible (additive) |
| 10 | **New public configuration.** A `LatticeOptions` switch to turn the live advance off is additive, and defaults to on once the gate opens. | Compatible (additive) |

**No change needs the user's authorisation.** None changes documented behaviour, and
none adds an exception a caller can observe.

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
- another shard's walk `g` of a tree-level range delete, which can be fenced out at a
  close and then re-stamped under another open intent or appended unstamped;
- a GC trimming below the shared snapshot's coverage.

Only entries the leaf owns are modelled; entries other leaves own are invisible to its
replay. It reuses `WalDurability`'s abstractions: the read position as the checkpoint,
a snapshot that keeps the highest coverage, and a pin at that coverage.

**Per-partition scopes in a one-leaf model (#5635).** With one leaf, a value shared by
every leaf of a partition and a value of L's alone are the same variable, so the model
cannot tell them apart unless something else moves the shared one. #5635 hid there: the
retired floor is per partition, while L's own retirements are per leaf. The model
therefore separates them (`retired` is the partition floor, `lRetired` L's own retired
epoch, which `StaleEpochNeverAppends` uses) and adds `RetireOther`
(`WithOtherRetire`, `WalLiveAdvanceOtherRetire.cfg`), another leaf's retirement raising the
partition floor to any epoch. Two further shared scopes are fine and are named for the
refinement note: the unstamped watermark is per partition, so any writer's unstamped
append holds every leaf writing that partition until it replays past it (over-reporting,
safe, cost only); and the routing-generation entry is keyed by partition and initiator.
The promoted module should put two leaves on one partition; `RetireOther` stands in until
then.
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
  state it last saw), `WithMixedVersion`, `WithGateFlip`, `WithLegacyHandOff`,
  `WithTreeWalk` (another shard's walk of a tree-level range delete, author `g`, with
  `GAdmit`, `GAlloc`, `GReject`, `GRestamp` and `GUnstamped`).
- `WithOtherRetire`: another leaf of the partition is retired (`RetireOther`), raising the
  shared floor (#5635). `WithSecondPartition`: a floor-only bump on another partition
  changes a raised activation's epoch (`OtherPartitionBump`).
- `Ghosts`: enables the witness ghosts. Safety runs keep it off.

**Properties:**
- `ReadPositionHonest`: no running activation's read position passes a durable entry
  it does not hold. Unacknowledged entries count, because a WAL reader applies them.
- `SnapshotHonest`.
- `NoTrimPastAcked`: no trim past an acknowledged append, whoever appended it.
- `StaleEpochNeverAppends`: no append is ever accepted from an epoch below the highest
  raised one, or at or below an epoch of L's own retired incarnations (`lRetired`),
  across a shard restart too.
- `CurrentNeverFenced`: the partition's retired floor never rejects an append
  L's own entry admits, so another leaf's retirement cannot supersede a live leaf (#5635).
- `OldNeverRejected`: no unstamped append is ever rejected.
- `ContestedKeyAgrees`: the owner resolves a contested key to the value a WAL reader
  (a replica or a view) resolves at the same read position. Every entry the leaf owns
  writes one key, the contention worst case; point and foreign entries carry real
  versions, ordered by offset, and the bulk entry a version below them all.

**Checked locally** (tla2tools v1.7.4, `-workers 2`, safety only, all six properties
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
| `WalLiveAdvanceTreeWalk.cfg` | `WithTreeWalk` | 4,250,341 | 35 | 115 s |
| `WalLiveAdvanceTreeWalkConcurrent.cfg` | `WithTreeWalk`, two windows | 23,951,016 | 38 | 711 s |
| `WalLiveAdvanceOtherRetire.cfg` | `WithRecreate`, `WithOtherRetire` | 46,444,519 | 37 | 1,340 s |
| `WalLiveAdvanceTwoPartition.cfg` | `WithSecondPartition` | 3,039,953 | 34 | 93 s |

All twelve hold every property, `ContestedKeyAgrees` and `CurrentNeverFenced` included.
Revision 5's new actions (`ClaimBump`, `OtherPartitionBump`, `RetireOther`, `FloorReject`)
are enabled only when the floor is raised or their switch is on, so base, Recreate,
OtherRetire and TwoPartition were re-run and the other eight are unchanged by
construction. `WalLiveAdvanceOtherRetire.cfg` is the largest; like `LegacyHandOff` it belongs
on a dedicated CI shard at promotion. `MixedRestart` is the reviewer's Mixed+Restart+duplicate
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
| `StaleEpochNeverAppendsIgnoresRetiredFloor` | The floor check at the claim's raise | `StaleEpochNeverAppends` | Recreate | 8 |
| `CurrentNeverFencedFloorAtAppend` | The entry-only append check: the floor is checked on appends too (revision 4's rule, #5635) | `CurrentNeverFenced` | OtherRetire | 8 |
| `StaleEpochNeverAppendsRecreateKeepsLoadedEntry` | Retirement removing the WAL shard's loaded entry: it deletes the rows only, with no append floor check (#5635's condition) | `StaleEpochNeverAppends` | Recreate | 9 |
| `StaleEpochNeverAppendsUnloadedEntryAccepted` | The unloaded-entry rejection | `StaleEpochNeverAppends` | Restart | 12 |
| `StaleEpochNeverAppendsClaimEntryOverwrite` | The max-merge: the claim overwrites the durable entry, checked against the loaded entry only (B3) | `StaleEpochNeverAppends` | Restart | 12 |
| `ReadPositionHonestGenerationNotDurable` | The durable routing-generation entry: a restart resets it (S1) | `ReadPositionHonest` | Restart | 14 |
| `OldNeverRejectedRejectedOnceFenced` | Admitting every unstamped append: the first revision's "reject once fenced" | `OldNeverRejected` | Mixed | 6 |
| `ReadPositionHonestUnstampedNotReported` | Reading past reported unstamped appends before arming | `ReadPositionHonest` | Mixed | 12 |
| `ReadPositionHonestRestartForgetsUnstamped` | The watermark's reset on a shard restart | `ReadPositionHonest` | Mixed, Restart | 14 |
| `ReadPositionHonestNoSelfReport` | An own unstamped append reporting itself (S2) | `ReadPositionHonest` | GateFlip | 13 |
| `ReadPositionHonestResumeWithoutMarker` | The resume opening the marker (B2) | `ReadPositionHonest` | LegacyHandOff | 17 |
| `ReadPositionHonestRangeDeletePerShardClose` | The per-tree range-delete generation: another shard's walk is fenced only by its own shard's close (revision 3's per-shard close) | `ReadPositionHonest` | TreeWalk | 13 |
| `ReadPositionHonestRangeDeleteWithoutIntent` | Stamping a walk only under an open intent: a walk with none is stamped anyway | `ReadPositionHonest` | TreeWalk | 13 |
| `ReadPositionHonestRestampDuringClose` | The intent closed atomically with the close: the close re-claims first and closes the intent and markers later, and the open check reads the marker | `ReadPositionHonest` | TreeWalk | 16 |
| `ContestedKeyAgreesBulkWinsLocally` | Last-writer-wins on the owner: it ranks its born bulk entry above a point write made before the switch, 10.2.0's stranded origin read | `ContestedKeyAgrees` | base | 4 |

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
check at the raise closes it.

### Redundancy checks

Three guards are kept although the model, with every other guard in place, does not
need them. Each was removed alone and the model stayed clean, so each is classed
redundant in the model (as for #2321), with the reason it is kept:

| Check | Removes | Configuration | Result | Why it is kept |
|-------|---------|---------------|--------|----------------|
| `ReadPositionHonestClaimAdoptsNoCas` | The epoch row's conditional write (#5593's adopt) | base | clean, 158 s | With the three-step claim, the shard's refusal at or below the durable entry catches a same-epoch claimant at the entry write. The conditional write is kept so a stale claimant stops before touching any partition, and because the production claim spans several partitions in sequence, which the model's single partition cannot show. |
| `ReadPositionHonestArmsWithoutClaim` | The explicit claim guard in `Armed` | GateFlip | clean, 171 s | Implied by the self-report: an activation that never claimed has only unstamped own appends, each of which reports itself, so it cannot arm past them. Kept, as the review asked, because arming without a claim is meaningless and the implication depends on every unstamped path self-reporting. |
| `ReadPositionHonestNoMarkerInit` | The "marker initialised" guard in `Armed` (B2 rule 2) | LegacyHandOff | clean, 493 s | Implied by rule 1 in the model, whose only legacy hand-off cannot admit a stamped write before `Resume` opens the marker. Kept because a resume path the census misses would otherwise read as closed. The model's `MarkInit` is stronger than production's rule: it waits until no hand-off is in flight anywhere, while production initialises an existing leaf once its shard's flag is set and it has no split in flight **as donor**, which is weaker for a split **sibling**. That is safe because rule 1 alone suffices, which is exactly what this clean result shows (review N3). |

Revisions 3 and 4 had two more rows, the floor check at the raise alone and at the append
alone. With the append check removed (#5635), the raise check is required, and is the
mutation above. The redundancy checks are committed in `redundancy/`, in the mutation format with
an `EXPECT: clean` line, so a re-run does not depend on this note; they are kept out of
`mutations/`, whose every file must fail.

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
| `WitnessContestedKeyResolvedToPoint` | - | `PreWrite`, `Begin`, `Create`: the owner resolves the contested key to the relocated point write, over its bulk entry |
| `WitnessTreeWalkAllocated` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `GAlloc`: another shard's walk is allocated under L's open marker |
| `WitnessTreeWalkFenced` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`: L's close re-claims the tree generation and fences another shard's admitted walk |
| `WitnessTreeWalkRestamped` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`, `ForeignOpen`, `GReject`, `GRestamp`: a walk fenced out by one delete's close re-stamps under a second delete's open intent |
| `WitnessTreeWalkUnstamped` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`, `GReject`, `GUnstamped`: a walk whose intent closed goes on unstamped and reported |
| `WitnessAppendAtOrBelowOtherFloor` | `WithOtherRetire` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OwnAlloc`, `RetireOther`: another leaf's retirement lifts the floor to an epoch a live activation holds, and it keeps appending |
| `WitnessClaimBumped` | `WithOtherRetire` | `Begin`, `Create`, `ClaimRow`, `RetireOther`, `ClaimBump`: a floor-only refusal bumps and re-claims |
| `WitnessOtherPartitionEpoch` | `WithSecondPartition` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OtherPartitionBump`: an activation stamps this partition with an epoch a bump elsewhere gave it |

To run a witness, use a configuration with `Ghosts = TRUE` and the switch shown that
checks `TypeOK` and the witness alone, for example
`INVARIANTS TypeOK WitnessStaleAppendRejected`.

## Formal impact

Every existing module, how the design affects it, and why:

| Module(s) | Effect | Reason |
|-----------|--------|--------|
| `WalDurability` | Unchanged; gains a gap entry | Its subject is the replay-based advance, which the design keeps as the fallback. Its single owner activation is always the current epoch, so `Append` never meets a rejection. Extending it would add an activation dimension to every leaf: `SnapshotLoss` is already 17,428,938 states and `TwoFaults` 2,958,083, so the fences go in the companion instead. |
| `WalMove` | Refinement row only | The entry, floor and generation rows live in grain storage, not in the stream a move switches, so a move carries nothing new. The cutover's reactivation of the shard grain is `Restart` here, covered by `StaleEpochNeverAppends` (broken by `StaleEpochNeverAppendsUnloadedEntryAccepted` and `StaleEpochNeverAppendsClaimEntryOverwrite`), the durable generation (`ReadPositionHonestGenerationNotDurable`) and the restarted watermark (`ReadPositionHonestRestartForgetsUnstamped`). |
| `WalPinRetirement` | New rows | Retiring a leaf's pins also retires its epoch entries under the same retirement token, raising the floor first. A re-created leaf's claim exceeds the floor, and the floor is checked at the claim's entry write and raise (never on an append, #5635), so a removal cannot let a surviving duplicate or a straddling claim outrank it. |
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
     the own-append self-report; the floor checks at the claim's write and raise, and none on
     the append path (#5635).
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
     fence: `TreeRangeDeleteFenceGrain` with its intents, deadlines, reminder and
     per-tree range-delete generation, and the stamped shard-root range-delete method
     (the existing method appends unstamped).
   - Generation rejections retried from the shard root through `ThrowIfRejectedForKey`.
   - The terminal class accepted only for `TxCommit` and `TxAbort`.
   - Resume opens the marker, and the marker-initialised state.
   - The shard routing generation, stamped at admission; the unstamped leaf methods
     appending unstamped.
   - The stamp census, including the terminal stamp class.
   - **Ordering dependency (found by member 1):** the coordinator-terminal stamp class
     must land before member 4 arms any leaf. Saga terminals from
     `ShardRootGrain.TxTerminal` and `AtomicWriteGrain` otherwise append unstamped and
     raise every partition's unstamped watermark, holding every leaf writing those
     partitions on the replay-based advance.
3. **Bulk.** #5610's relocating graft, fixed with #5614; the birth hint captured with
   it and persisted in `_lattice_shard-handoff`; bulk-leaf ranges; and the switch's
   marker and fence.
4. **The live advance itself.**
   - Armed only by the rules above.
   - It reuses #5578's applied-prefix tracker and the core extraction from the
     abandoned #5596.
5. **Epoch-row sweep (#5634).** The per-partition epoch directory row, bucketed or paged
   within the provider's row limits, and the sweep that retires the entries of leaves
   that no longer exist, which a 10.2.0 silo's retirement leaves behind. It lands
   before #5605's bucket reaches `main`, independently of members 2 to 4.
6. **Promotion.** `WalLiveAdvance` becomes a module with its refinement note, its
   manifest, its redundancy rows (with a redundancy class in the mutation catalogue, or
   kept out of the catalogue gate's glob, so a clean run is not reported as a missing
   violation), and **every combined configuration the reviewer named**
   (Mixed+Restart+duplicate on the full instance, own unstamped appends with the
   self-report, repeated unstamped appends, the split claim under `Restart`, and the
   twelve configurations above) **in CI, on a dedicated or measured TLC shard.** Nothing
   from phase 2 reaches `main` before this member lands.

## Review resolution

### Re-review of revision 4

| Finding | Resolution |
|---------|------------|
| B1 re-stamp not atomic with the close | Section 2, "A walk outliving its intent": the intent is marked closed, durably, in the turn that begins the close, before its first re-claim; the fence grain answers the open check and the generation from that one state, is non-reentrant, and its open check is neither `[AlwaysInterleave]` nor `[ReadOnly]`; no shard root caches either answer. The model gains `GRestamp` and `GUnstamped`; `TreeWalk` is clean at 4,250,341 states and the two-window `TreeWalkConcurrent` is clean at 23,951,016. The split close is mutation `RestampDuringClose` (16 states). |
| S1 deadline from a cancellation token | Step 1 takes the `SagaDecisionDeadline` form; "The deadline affects liveness only" states that an early close is safe and that a long paged drain runs its later pages unstamped. |
| S2 per-page intent check | Step 3: the stamped method trusts the generation it is given; only a rejection's seam retry asks the fence grain. Change 6 states it. |
| S3 cost of unstamped walks | Section 2 states the tail-replay cost and the repocontext case; change 7 adds unstamped reports by source. |
| N1 `MergeEntriesAsync` line | Corrected to `BPlusLeafGrain.cs:3703`. |
| N2 redundancy class at promotion | Member 6 (promotion) gives the catalogue a redundancy class, or keeps the files out of the gate's glob. |
| Q1-Q4 | Per-call fences stay the default; the re-stamp rule is B1; the deadline is S1; replication apply and the shadow forward stay unstamped (S3). |
| #5635 (from member 1), and the reviewer's [interim note](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6566514445) | Section 1: the floor is checked at the claim's entry write and raise only, `AppendAdmits` is the entry check. (1) The condition that makes that sound, retirement removing the WAL shard's loaded entry under `_stateGate` after the batch drain, is stated, with mutation `RecreateKeepsLoadedEntry` (9 states). (2) `RetireOther` and `CurrentNeverFenced`, broken by `CurrentNeverFencedFloorAtAppend` (8 states, #5635 itself). (3) `StaleEpochNeverAppends` is narrowed to L's own retirements (`lRetired`). (4) A floor-only refusal bumps and re-claims (`ClaimBump`, `WitnessClaimBumped`), up to `MaxClaimRounds` then unstamped. (5) One epoch across partitions: `OtherPartitionBump` (`WithSecondPartition`), clean at 3,039,953. `OtherRetire` is clean at 46,444,519. The two floor redundancy rows are retired: the raise check is now required (`IgnoresRetiredFloor`). |
| Coordinator additions | #5634: the per-partition epoch directory row and sweep, as member 5, before the bucket reaches `main` (section 1). The ordering found by member 1: the coordinator-terminal class (member 2) lands before member 4 arms any leaf. |

### Re-review of revision 3

| Finding | Resolution |
|---------|------------|
| B1 per-shard close with an abandoned walk | Section 2, "One generation for every walk": every shard's walk is stamped with the tree's range-delete generation, so one re-claim per partition fences them all; no two-phase close. Modelled by `WithTreeWalk` (another shard's walk `g`), clean at 3,915,069 states with the duplicate. The per-shard close is mutation `RangeDeletePerShardClose`. |
| B2 walks stamped with no tree fence | Section 2, "A walk with no open intent is appended unstamped": a shard root stamps a walk only through the new stamped method naming an open intent. Replication apply, shadow forwards, gate-spanning fan-outs and 10.2.0 callers use the existing method and stay unstamped, by design. Mutation `RangeDeleteWithoutIntent`. |
| S1 generation rejections re-stamped at the leaf | Section 5: a routing-generation rejection is always retried from the shard root, through `ThrowIfRejectedForKey` and the existing stale-routing retry; only a leaf-epoch rejection is retried at the leaf. |
| S2 terminal class unchecked | Section 2: the WAL shard accepts the class only for `TxCommit` and `TxAbort`; anything else is admitted unstamped and reported. The promoted module adds one prepare and its terminal. |
| S3 intent deadline and reminder owner | Section 2: the intent records the call's deadline; `TreeRangeDeleteFenceGrain` owns the per-tree reminder and closes every intent past its deadline. |
| S4 stepped range deletes | Section 2, "Cost per range delete call": per step, with markers closed between steps; the four stepped callers are named. Change 6 restated. |
| N1 redundancy files | Committed in `redundancy/` with `EXPECT: clean`. |
| N2 reminder storage | Change 9. |
| N3 `MarkInit` stronger than production | Stated in the `NoMarkerInit` redundancy row. |
| Q1, Q2 | Q1: the per-tree generation with per-shard range-scoped markers, as answered. Q2: gap entry, member 2 detector, the `Op` check, and a prepare-terminal pair in the promoted module. |
| Change 4 (coordinator decision after revision 3) | Last-writer-wins, the point write wins: what 10.2.0 converges to on every reader (section 3, with file and line). A compatible bug fix; no authorisation. Property `ContestedKeyAgrees`, mutation `BulkWinsLocally`, witness `ContestedKeyResolvedToPoint`. A stable "bulk value wins" is rejected, with reasons. |

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
| S5 #5610 who wins | Revision 4: last-writer-wins, the point write wins, as 10.2.0 converges on every reader (section 3, with file and line). A compatible bug fix that removes an origin-versus-replica divergence; no authorisation. Property `ContestedKeyAgrees`, mutation `BulkWinsLocally` (4 states), witness `ContestedKeyResolvedToPoint`. A stable "bulk value wins" is rejected, with reasons. |
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
| B2 epoch floor | Section 1, "Retiring entries": a monotone per-partition floor row, raised before an entry is deleted, checked at every claim step and append (revision 5: at the claim steps only, #5635). Mutations: `RecreateDropsFloor`, `IgnoresRetiredFloor`. |
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

None. Per the re-review of revision 4, the reviewer can confirm B1 from the model diff:
`GRestamp`, `GUnstamped`, `closing`, the two `TreeWalk` configurations and
`ReadPositionHonestRestampDuringClose`.
