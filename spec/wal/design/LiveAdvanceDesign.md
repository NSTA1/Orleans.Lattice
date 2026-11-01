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
(2 blocking, 9 should-fix, 5 nits), and it meets a hard requirement added since:
no unauthorised breaking change against the shipped `lattice` 10.2.0 (tag
`lattice-v10.2.0`). [Backward compatibility](#backward-compatibility) sets out how,
and lists every observable behaviour change. One of them (change 3, option (a)) is
pending the user's authorisation before the fencing production member. The last sections map every review finding to its
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
  range and is replayed by every leaf of the shard), and the leaf a write reached
  before a bulk switch. An unacknowledged record from one of them was skipped by its
  owner but still applied by a WAL reader such as the replication shipper or a view.
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
(#4586) atomic with assigning an offset.

**New state only in new rows.** Every piece of durable state the design adds lives
in its own row, under a reserved grain type, written through the tree's grain
storage provider. `GrainStorageFencingProbe` is the precedent. No existing state
type (`LeafNodeState`, `ShardRootState`, `WalShardFloorState`) gains a field. A
10.2.0 silo that rewrites one of those rows therefore cannot drop anything the
design relies on. See [Backward compatibility](#backward-compatibility).

| Row (reserved grain type) | Key | Holds | Written by |
|---------------------------|-----|-------|------------|
| `_lattice_leaf-append-epoch` | Leaf id | The leaf's append epoch | Claims only, by conditional write |
| `_lattice_wal-append-epoch` | WAL shard key and leaf id | The partition's epoch entry for the leaf | The claim at the WAL shard, and entry retirement |
| `_lattice_wal-append-epoch-floor` | WAL shard key | The partition's retired-epoch floor | Entry retirement only |
| `_lattice_leaf-handoff` | Leaf id | The leaf's hand-off marker: open tokens and per-partition close heads | Hand-off initiators |
| `_lattice_shard-handoff` | Shard root id | The shard-level marker and the shard's routing generation | The shard root |

Each row's state type is new, carries a new stable alias, and is marked
`ILatticeBinaryPersistedState`. Every member is `[Id(n)]` and is a public property
of a concrete type: arrays and concrete dictionaries, never a collection expression
typed as an interface (#5579). The rows therefore also round-trip through a JSON
grain-storage serializer when a host bypasses Lattice's binary serializer.

### 1. The epoch fence: other activations of the leaf

**Claim eagerly, never lazily.** Before an activation's first append, it claims an
epoch:

1. **Persist.** It writes the epoch it read plus one to its
   `_lattice_leaf-append-epoch` row, as a strict conditional write on the ETag it
   read. Only claims write that row. So every conflict, empty-ETag included, means
   another activation claimed: this one is stale, the write fails closed and the
   activation deactivates. The caller's retry reaches the live activation.

   The write never goes through
   `TopologySeedPersist.WriteAdoptingBenignFirstCreateRaceAsync`, which adopts the
   winner's row on an empty-ETag conflict, the shape of #5593. Keeping the epoch out
   of `LeafNodeState` also means the leaf's ordinary persists never conflict with a
   claim (review N3).
2. **Claim.** It claims that epoch at every WAL partition it writes
   (`ClaimAppendEpochAsync`). The shard first writes the leaf's entry row, then,
   under `_stateGate`, raises the in-memory entry and reads `_nextOffset`, the
   allocator's next offset, not the readable head (`DurableContiguousTailOffset`). It
   returns that offset as the claim head. Anything allocated before the raise lies
   below the claim head; anything after it is checked against the raised entry.
   Persisting before raising means a restart can never lose a raise whose head was
   already returned.
3. **Refusal.** A claim at or below the entry is refused. So is a claim into an
   absent entry that does not exceed the partition's retired floor (below). A refusal
   returns the higher of the entry and the floor. The leaf then persists that value
   plus one to its epoch row, conditionally, and claims again. The conditional write
   on the leaf's own row still serialises the claimants.

**The stamped append.** Once the cluster's fence gate is open (section 4), an
activation appends through `AppendStampedAsync`. That is a new method; the existing
append method is unchanged. It passes the leaf's identity and epoch, plus the shard
routing generation (section 2).

Under the same lock section that assigns the offset, the shard accepts a stamped
append only if the leaf's entry is loaded and the epoch is not below it. A stale
epoch is rejected with `WalAppendEpochStaleException`. An entry not yet loaded
rejects the append as unloaded (below); the leaf re-presents its claim and retries
once. The stored record does not change: the stamp is a call argument, never part of
the record.

**Arming.** The activation is armed for the live advance on a partition only when:
- it has replayed to that partition's claim head;
- every append it has issued there has been applied (the applied-prefix tracker);
- it has read past every unstamped append the shard reported to it (section 4);
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

**Moves and restarts.** The entry and floor rows live in grain storage, not in the
WAL stream, so a WAL move, which switches the stream's provider, does not touch them.
A restart of the shard grain, including the reactivation at a move's cutover, loses
only the in-memory entries, which reload through re-presented claims; nothing is
accepted from an unloaded entry (`StaleEpochNeverAppendsUnloadedEntryAccepted`). It
also loses the in-memory unstamped watermark, which restarts at the recovered
allocator head minus one (section 4).

**Cost.**
- **Per append:** one dictionary lookup and one comparison under the lock the append
  already holds. No allocation and no grain call. The stamped request carries 24 more
  bytes: leaf identity, epoch and routing generation. The response carries the 8-byte
  unstamped watermark in the struct it already returns. The stored record is
  unchanged.
- **Per activation:** one conditional row write, plus one claim call and one entry-row
  write per partition it writes. A fresh estate of N leaves writing P partitions costs
  at most N x P entry-row writes once, not O(N^2).
- **Per shard restart:** one extra claim round trip for each leaf that appends
  afterwards.
- **Storage:** one entry row per (partition, live leaf that wrote it), one floor row
  per partition, one epoch row per leaf, and the marker rows.
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

**Why the drain is a fence and not a wait.** Every stamped append carries the epoch
or generation its write was admitted under. So after (c), a write admitted before the
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
admission. A shard root captures its routing generation in the same step as its phase
check (`ThrowIfRejectedForKey`) and passes it to the leaf through a new stamped
leaf-write method (the existing method is unchanged). If the append read the current
epoch instead, a write admitted before the close would carry the re-claimed epoch and
be accepted (`ReadPositionHonestStampAtAppend`).

**Every append path is stamped.** Point, batch, CRDT apply, range delete, merge,
relocation and saga prepare all carry the stamps. A path that bypasses the stamp is
exactly the gap the fold's point-only guard has today (`ReadPositionHonestDrainPointOnly`).
Production member 2 carries a census of every append path into a leaf, with a
detector per path. A path the census finds unstamped appends through the unstamped
method, which is safe (section 4) but disarms the leaves it reaches.

**Crash and cost.** If the initiator crashes, the marker stays open until its
recovery re-drives the close. The cost is latency (the owner advances by replay),
never data. A write admitted before the close and not yet allocated at (c) is
rejected unacknowledged. The retry envelope re-routes and re-issues it (see
[Behaviour changes](#observable-behaviour-changes)). This happens only during a
close.

| Path | Initiator | Open | Admission stops at | Fence at close |
|------|-----------|------|--------------------|----------------|
| Split | Donor leaf | In the sibling's marker row, written in the sibling's birth (`InitializeSiblingAsync`) | The donor's narrow of `HighKeyExclusive`; span admission forwards moved keys afterwards | The donor re-claims its epoch at every partition it writes; the heads close the sibling's marker |
| Leaf reclaim | Absorbed neighbour leaf | On the survivor, before its range widens | The neighbour's unlink | The neighbour's entry is retired (the floor rises), so its later stamped appends are rejected; the heads are captured in the retire step |
| Adaptive split | Donor shard root | Shard-level marker on the target shard root | The reject phase | The donor shard root re-claims its routing generation at every partition; the heads close the marker at completion |
| Shard fold | Folded shard root | Shard-level marker on the survivor shard root | The fold's quiesce, point and batch | The same routing-generation re-claim |
| Range delete | The shard root running `DeleteRangeBoundedAsync` | Shard-level marker on that shard root, before the first leaf's append | The end of the shard's bounded walk | A routing-generation re-claim at every partition; the heads close the marker |
| Bulk load / append | The shard root, for writes admitted before the switch | At the bulk leaf's birth, with the birth hint | The switch, in the shard root's serial turn | The shard root re-claims its routing generation at every partition; the heads close the marker |

**Range delete: fenced, not clipped.** A range delete's record carries the whole
requested range, and every leaf of the shard that replays it applies it. The first
revision clipped each record to its authoring leaf's range. That changes the record's
shape for every consumer: the state-change feed, history views and predicate views.
So it is rejected here (see [Backward compatibility](#backward-compatibility)).
Instead, the shard root running the delete opens its shard-level marker before the
first append and closes it by fence after its bounded walk. The record is unchanged.
The cost is two shard-root row writes and one re-claim per WAL partition per range
delete, and the shard's leaves advance by replay while it runs.

**Shard-level marker.** The moved slots of an adaptive split or a shard fold, and the
leaves a range delete reaches, scatter across a shard's leaves, so a per-leaf marker
would cost one call per leaf. A leaf reads its shard's marker **fresh**, after the
last own append it would advance over, never from an earlier tick's cache. It reads
it through a new interleaving, read-only shard-root call, `GetHandOffMarkerAsync`,
which returns the marker's generation and close heads. Read-only is load-bearing:
this is a new leaf-to-shard-root wait-for edge, and a serial shard-root turn that
calls the leaf must not be able to deadlock against it. The read costs one call per
advance tick, not per write.

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

**Existing rows: relocate, never refuse (#5610).** Today a bulk append grafts its
leaves without checking the keys the rightmost leaf already holds, so a point write
into the bulk range between chunks becomes unroutable. #5614's inflated counts are
probably the same graft. The bulk member fixes #5610 compatibly. In the same serial
shard-root turn that captures the hint, the graft finds the rows the bulk range
already holds and relocates them into the new bulk leaf at its birth, exactly as a
split relocates its moved rows, narrowing the old leaf. It never refuses the chunk: a
chunk that succeeds today keeps succeeding (see
[Behaviour changes](#observable-behaviour-changes)). Skipping the relocation fails
(`ReadPositionHonestBulkAppendDropsExistingRows`). #5610 and #5614 are fixed together
in the bulk member, which confirms whether they share a cause.

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

**Unstamped appends are always admitted.** An append through the existing, unstamped
method comes from a 10.2.0 silo, from any silo while the gate is closed, or from a
path the census missed. The WAL shard never rejects it. Instead it records the
append's offset as the partition's highest unstamped offset, and returns that value in
the response of every stamped append. An activation arms only once it has read past
every unstamped offset reported to it:
- An unstamped append allocated before one of the activation's own appends is
  reported by that append's response.
- One allocated after the activation's last own append lies above the advance's
  target, so the advance cannot pass it.

The watermark is in memory. A shard restart sets it to the recovered allocator head
minus one, treating everything before the restart as possibly unstamped.

So an unstamped append is never rejected (`OldNeverRejected`, broken by
`OldNeverRejectedRejectedOnceFenced`) and never skipped
(`ReadPositionHonestUnstampedNotReported`, `ReadPositionHonestRestartForgetsUnstamped`).
Safety therefore does not depend on when the gate opens or closes: the gate only
decides which methods exist on every silo.

**Racing the gate.** A new silo may call a stamped method on a grain activated on a
10.2.0 silo before its gate has observed that silo join. The call fails because the
method does not exist there. The caller then re-issues the same write through the
unstamped method and closes its own view of the gate. Nothing is rejected or lost,
and the unstamped append is reported and read like any other.

**The leaf takes the replay-based advance** on a partition whenever it cannot prove
exclusive authorship:
- **Its own claim.** No claim yet on the partition, or the claim failed, conflicted
  or was refused.
- **Replay.** It has not replayed to the claim head, to the last close's heads, or
  past the highest unstamped offset reported to it.
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
- **Rollout.** The fence gate is closed. Nothing claims, the shards keep no entries,
  and every advance is replay-based.

## Backward compatibility

`lattice` 10.2.0 shipped (tag `lattice-v10.2.0`). This section shows that the design
breaks nothing that release relies on, in storage, on the wire, or across a
mixed-version cluster, and lists every observable behaviour change.

### Persisted formats

| Format | Change | 10.2.0 data under the new build | New data under 10.2.0 |
|--------|--------|---------------------------------|-----------------------|
| WAL records | None. The epoch, routing generation and watermark travel as call arguments and response fields, never in a stored record. Range deletes are fenced, not clipped, so their records keep their shape. | Read unchanged | Read unchanged |
| `LeafNodeState`, `ShardRootState`, `WalShardFloorState` | None: no field is added | Read unchanged | Read unchanged. A 10.2.0 silo that rewrites one drops nothing the design needs, because nothing new lives there. |
| New rows (the table at the top of [The design](#the-design)) | New state types, new aliases, reserved grain types | Not present: an absent row reads as "no claim", "no marker", "floor 0", and the leaf falls back to replay | Never read by 10.2.0, which knows none of these types. A rollback leaves them in place, harmless; re-upgrading resumes from them. |

**JSON-backed providers (#5579).** A host's grain storage normally serializes the new
state types through `LatticeGrainStorageSerializer`, in binary, because they are
marked `ILatticeBinaryPersistedState`. A host that bypasses it falls back to the JSON
serializer, which persists public properties only and must be able to construct every
collection on read. The new state types are written for both: every member is a public
property of a concrete type (arrays and concrete dictionaries, never an
interface-typed collection expression). Each type gets a round-trip test through the
JSON grain-storage serializer as well as the binary one. The WAL's JSON-backed
providers see no change, because no WAL record changes.

### Wire formats

- **Grain interfaces, 10.2.0 caller to a new silo.**
  - No existing grain interface method changes its name, parameters or return type.
    An Orleans method's identity covers its parameters, so changing one would break a
    10.2.0 caller.
  - New behaviour arrives only through new methods: `ClaimAppendEpochAsync`,
    `AppendStampedAsync`, the stamped leaf-write methods and `GetHandOffMarkerAsync`.
  - A 10.2.0 silo calls only the existing methods, and a new silo serves them exactly
    as 10.2.0 does. Its appends are unstamped, so they are admitted and reported.
- **Grain interfaces, new silo to a 10.2.0 silo.** A new silo calls a new method only
  while the fence gate is open, that is, while no 10.2.0 silo is active. The race
  where a 10.2.0 silo joins before the gate observes it is covered under
  [Racing the gate](#4-fail-safe-everywhere-and-mixed-versions): the call fails, and
  the caller re-issues the write through the unstamped method.
- **Serializable types and exceptions.**
  - The new request and response types have new aliases. Existing aliases and `[Id]`s
    are untouched.
  - `WalAppendEpochStaleException` derives directly from `System.Exception` and carries
    a new alias. Only the new stamped methods throw it, so it reaches only new
    callers.
- **Cross-cluster replication.** Shipped records are WAL records, which are unchanged,
  so a 10.2.0 peer and a new peer exchange exactly what two 10.2.0 peers exchange, in
  both directions. A replicated write applied on the receiving cluster goes through
  the receiving leaf's own append path: stamped on a new cluster with the gate open,
  unstamped otherwise.

### Mixed-version rollout

Fencing and the live advance turn on only once every active silo supports them, and
off again if a 10.2.0 silo joins. Until then, and at every moment:
- a 10.2.0 silo's unstamped append is never rejected;
- it is never skipped by an advance, because it is reported and must be read before
  any activation arms past it.

The model checks both with an environment author `o` that appends unstamped
(`WithMixedVersion`, `WalLiveAdvanceMixed.cfg`):
- **Never rejected:** `OldNeverRejected`, broken by `OldNeverRejectedRejectedOnceFenced`
  (the first revision's rule of rejecting unstamped appends once a partition is
  fenced).
- **Never skipped:** `ReadPositionHonest`, broken by
  `ReadPositionHonestUnstampedNotReported` and by
  `ReadPositionHonestRestartForgetsUnstamped`.

A downgrade needs no operator step: the gate closes when the first 10.2.0 silo
appears, and leaves fall back to replay as soon as an unstamped append is reported.

### Observable behaviour changes

Each change is classed as a **compatible bug fix** (no documented behaviour changes)
or a **documented-behaviour change** (needs the user's explicit authorisation before
production).

| # | Change | Class |
|---|--------|-------|
| 1 | **A stale duplicate activation's write is retried, not accepted.** With the gate open, the WAL shard rejects a stale epoch's append. `ShardActivationRetry` gains this rejection as a transient arm, beside silo churn: the stale activation deactivates, and the retry reaches the live one. The caller sees one write that succeeds, with added latency. Today the stale activation's append is accepted; under today's replay-based advance nothing is lost, but the fenced live advance relies on rejecting it. Orleans already promises a single activation, so no caller relies on duplicates. | Compatible bug fix |
| 2 | **A write admitted before a hand-off close, and not yet allocated, is retried.** The close's fence rejects it, and the same envelope re-issues it, which span admission or the shard map now routes to the new owner. Today such a write is accepted. The caller sees success with added latency. | Compatible bug fix |
| 3 | **The retry envelope is exhausted.** After its bounded attempts (3 today), the envelope would rethrow the last fault, which for changes 1 and 2 is `WalAppendEpochStaleException`, a type 10.2.0 never threw to a caller. That needs a pathological run of closes or claims on one key. **Chosen: option (a).** The exhausted retry surfaces as the existing retryable `LatticeWalQuiescingException`, with a distinguishing message ("an append was fenced by a newer activation or a hand-off close; retry"). It also carries a new optional `Reason` (`LatticeWalQuiescingReason.PlacementMove`, the default and today's only cause, or `AppendFence`) at `[Id(0)]`, which a 10.2.0 deserializer skips as an unknown field. The internal exception never crosses to the caller, not even as an inner exception. The type's documentation gains the cause. | **Documented-behaviour change: pending user authorisation before the fencing production member (member 1).** |
| 4 | **Bulk append over existing rows (#5610).** The graft relocates the rows the bulk range already holds into the new bulk leaf, instead of stranding them. Keys that were unroutable become readable, and counts and statistics become exact (#5614). No new exception: a chunk that succeeds today still succeeds. Refusing the chunk was considered and rejected, because it would fail a correct caller whose range another client wrote into. | Compatible bug fix |
| 5 | **The live advance itself (#5572).** A leaf's checkpoint advances without a replay from -1. The WAL GC trims sooner, replay demand falls, and #5570's "carries no usable offset" warnings stop for live leaves. Existing metrics report different values; none changes name, unit or tags. | Compatible bug fix |
| 6 | **Range deletes cost more.** Each shard's bounded walk now opens and closes a shard-level marker: two row writes and one re-claim per WAL partition. The record, its result and its feed and history output are unchanged. Clipping, the cheaper alternative, would change every consumer's view of the record, so it is not chosen. | Compatible (cost only) |
| 7 | **New metrics.** New instruments only, on the existing meter: claims, refusals and rejections by reason, live advances by arming outcome, open markers, and unstamped reports. No existing instrument gains a tag, a tag value, or a new unit. | Compatible (additive) |
| 8 | **New log lines.** A deactivation warning for a stale activation, and a debug line when a partition disarms on an unstamped report. #5571's blocked-pin warning keeps its categories. | Compatible (additive) |
| 9 | **New storage rows** under the reserved grain types listed above appear in the host's grain storage. They are documented in the tree's storage notes. | Compatible (additive) |
| 10 | **New public configuration.** A `LatticeOptions` switch to turn the live advance off is additive, and defaults to on once the gate opens. | Compatible (additive) |

**Change 3 is the only one that needs the user's authorisation.** Its design is chosen (option (a)), and it is pending that authorisation before member 1 lands. The others change no documented behaviour.

## The model

`WalLiveAdvance.tla` covers:
- one WAL partition of three offsets, and one owner leaf;
- its born activation `a1`, created by a bulk load, and a duplicate `a2` that also
  stands for the next activation after a stop (both read the row and must claim);
- a foreign author `f` with point and batch writes and two hand-off windows: the bulk
  switch and one more. `f`'s epoch stands for either a leaf initiator's epoch or a
  shard-level initiator's routing generation; their fences have the same algebra;
- a pre-existing writer `p`, whose rows the bulk range already holds and the graft
  relocates (#5610);
- an unstamped author `o`: a 10.2.0 silo, or any silo while the gate is closed;
- a GC trimming below the shared snapshot's coverage.

Only entries the leaf owns are modelled; entries other leaves own are invisible to its
replay. It reuses `WalDurability`'s abstractions: the read position as the checkpoint,
a snapshot that keeps the highest coverage, and a pin at that coverage.

**Own appends are two steps.** `OwnAlloc` is the shard's epoch check and offset
assignment under its lock, and records the unstamped watermark it reports. `Land`
makes the append durable and, if its author is still running, applies and
acknowledges it. A foreign write is three steps: `ForeignAdmit` (stamped with the
epoch it is admitted under), then `ForeignAlloc` or `ForeignReject` at the shard, then
`Land`. `OldAlloc` is the unstamped append, admitted unless `UnstampedAdmitted` says
otherwise.

**Abstractions in the claim.**
- `Claim` folds the persist, the shard claim and the refusal retry into one step.
  Claims at or below the entry are refused, so the order in which two activations'
  claims reach the shard does not matter.
- The model compares epochs where production compares ETags. Only claims write the
  epoch row, so the two agree.
- The claim persists the epoch it read plus one, raised above the floor, so dropping
  the conditional write is observable (`ReadPositionHonestClaimAdoptsNoCas`).

The redundant `epoch > 0` arming guard of the first revision is removed: `mine[a]` is
non-empty only after a claim.

**Feature switches** keep each check small:
- `WithDuplicate`: admits the duplicate. It is on everywhere except one combined
  mutation check.
- `WithRecreate`: a purge and re-creation of the leaf that retires its entry.
- `WithRestart`: a WAL shard restart that loses the in-memory entries and watermark.
- `FrozenView`: a superseded activation keeps the marker state it last saw. This
  matches production's stale in-memory view and is weaker than one global marker.
- `WithMixedVersion`: the unstamped author.
- `Ghosts`: enables the witness ghosts. Safety runs keep it off.

**Properties:**
- `ReadPositionHonest`: no running activation's read position passes a durable entry
  it does not hold. Unacknowledged entries count, because a WAL reader applies them.
- `SnapshotHonest`.
- `NoTrimPastAcked`: no trim past an acknowledged append, whoever appended it.
- `StaleEpochNeverAppends`: no append from an epoch below the highest claimed is ever
  accepted, across a shard restart too.
- `OldNeverRejected`: no unstamped append is ever rejected.

**Checked locally** (tla2tools v1.7.4, `-workers 2`, safety only, all five
properties):

| Configuration | Distinct states | Depth | Time |
|---------------|-----------------|-------|------|
| `WalLiveAdvance.cfg` (base) | 2,688,244 | 31 | 46 s |
| `WalLiveAdvanceMixed.cfg` | 6,427,886 | 31 | 105 s |
| `WalLiveAdvanceRecreate.cfg` | 10,293,766 | 32 | 199 s |
| `WalLiveAdvanceRestart.cfg` | 17,074,939 | 33 | 390 s |
| `WalLiveAdvanceFrozenView.cfg` | 4,381,732 | 31 | 101 s |

`WalLiveAdvanceRestart.cfg` is large for a local check. When the draft becomes a
module, it belongs in a CI shard with the other large variants.

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
| `ReadPositionHonestBulkAppendDropsExistingRows` | The relocation of the rows the bulk range already holds (#5610) | `ReadPositionHonest` | 4 |
| `ReadPositionHonestRecreateDropsFloor` | The retired floor's raise (`WithRecreate`) | `ReadPositionHonest` | 13 |
| `StaleEpochNeverAppendsUnloadedEntryAccepted` | The unloaded-entry rejection (`WithRestart`) | `StaleEpochNeverAppends` | 8 |
| `OldNeverRejectedRejectedOnceFenced` | Admitting every unstamped append: the first revision's "reject once fenced" (`WithMixedVersion`) | `OldNeverRejected` | 5 |
| `ReadPositionHonestUnstampedNotReported` | Reading past reported unstamped appends before arming (`WithMixedVersion`) | `ReadPositionHonest` | 10 |
| `ReadPositionHonestRestartForgetsUnstamped` | The watermark's reset on a shard restart (`WithMixedVersion`, `WithRestart`, no duplicate) | `ReadPositionHonest` | 12 |

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
| An unstamped append is admitted and reported to a stamped one (`WithMixedVersion`) | `Begin`, `Create`, `Claim(a1)`, `OldAlloc`, `OwnAlloc(a1)` |

To run a witness, use a configuration with `Ghosts = TRUE` that checks `TypeOK` and
the witness alone, for example `INVARIANTS TypeOK WitnessStaleAppendRejected`.

## Formal impact

Every existing module, how the design affects it, and why:

| Module(s) | Effect | Reason |
|-----------|--------|--------|
| `WalDurability` | Unchanged; gains a gap entry | Its subject is the replay-based advance, which the design keeps as the fallback. Its single owner activation is always the current epoch, so `Append` never meets a rejection. Extending it would add an activation dimension to every leaf: `SnapshotLoss` is already 17,428,938 states and `TwoFaults` 2,958,083, so the fences go in the companion instead. |
| `WalMove` | Refinement row only | The entry and floor rows live in grain storage, not in the stream a move switches, so a move carries nothing new. The cutover's reactivation of the shard grain is `Restart` here, covered by `StaleEpochNeverAppends` (broken by `StaleEpochNeverAppendsUnloadedEntryAccepted`) and by the restarted watermark (`ReadPositionHonestRestartForgetsUnstamped`). |
| `WalPinRetirement` | New rows | Retiring a leaf's pins also retires its epoch entries under the same retirement token, raising the floor first. A re-created leaf's claim exceeds the floor, so a removal cannot let a surviving duplicate outrank it. |
| `TreeLifecycle`, `TreeLifecycleAdmission` | New rows | The purge tail retires the entries of the leaves it clears, under the lineage check (#5417), raising the floor first, and deletes their epoch and marker rows. |
| `ShardOwnership`, `ShardOwnershipCutover`, `ShardOwnershipFold`, `ShardOwnershipCrdt`, `ShardOwnershipRetention` | New rows or gaps | The adaptive split's and the shard fold's shard-level markers open around their windows and close with a routing-generation re-claim. A marker only holds the live advance and never blocks a move, a cutover or a fold, so ownership safety is unchanged. The re-claim can reject an in-flight write admitted before the close; the retry envelope re-issues it. |
| `SplitLink`, `SplitLinkTurns`, `BPlusTopology`, `BPlusReclaimRecovery`, `BPlusCascade` | New rows or gaps | The split opens the sibling's marker in its birth, and the donor closes it at completion after re-claiming its epoch. That is one more call, made from the donor's completion; it waits on the WAL shard, not on another leaf. The reclaim opens the survivor's marker before widening, and retires the neighbour's entry in the retire step. |
| `SplitGateCycles`, `TurnOccupancy` | New rows | A leaf's fresh read of the shard-level marker is a new leaf-to-shard-root wait-for edge. It is an interleaving, read-only call, so a serial shard-root turn that calls the leaf cannot deadlock against it. Both modules gain a row stating that the edge does not take the shard root's serial turn. |
| `BackupRestore`, the bulk-load paths | Rows | A restore and an offline snapshot load through `BulkLoadRawAsync`, so their leaves take the birth hint, the open marker, the relocation of existing rows and the switch's fence. |
| `ReplayPermits`, `WalGcStarvationDrive`, `WalGcStarvationDriveWitness`, `MaterialiserDrain` | Demand abstraction only | The design lowers replay demand. These modules bound permits and drives for any demand, so their properties are unaffected. Their notes gain a line that live-advanced leaves no longer drive. |
| `WalAdmission`, `WalBackPressure` | Gap entry | A stale-epoch or fenced-out rejection is a new append failure, distinct from admission refusal under load. It fails one stale or pre-close writer, never a saturated partition, and an unstamped append is never refused. So the admission and back-pressure properties are unaffected. |
| `AtomicCommit`, `AtomicCommitCrossCluster` | Unchanged; rows restated | A prepare whose append is rejected as stale is a nack, which `PrepareTx` already permits (it chooses ack or nack per key). A terminal is different. `ApplyTxTerminalAsync` flips the pending bucket in memory and appends only on its backstop path, so a stale activation accepts a terminal without appending. Safety still holds, through the leaf-wide prepare clamp: the current activation does not advance while the prepare is pending, and it learns the terminal by replaying the coordinator's record. `BroadcastStep` assumes only eventual delivery. Coordinators append terminals and hold no leaf epoch. |
| `SagaReadProgress`, `TagIndexReconcile`, `ReplicationShipping` | Unaffected | They consume accepted WAL records, which are unchanged; a rejected append never becomes a record. |
| `AntiEntropy`; `Replication`, `ReplicationCausalDelivery`, `ReplicationLowWatermark`, `ReplicationReBootstrap` | Unaffected | They consume accepted WAL records, whose shape is unchanged: range deletes are fenced, not clipped. |
| `MaterialisedView*`, `view-*` modules | Unaffected | Views read accepted WAL records only, unchanged. |
| `AliasRevert`, `AliasRouting`; `Schema*`, `SchemaViewComposition`; `TenantIsolation`, `TenantFootprint`, `TenantQuotaEvaluation`, `TenantTreeCount`, `TenantRateBudget` | Unaffected | Routing, schema and tenancy decisions are made before a write reaches a leaf. |
| `BackupCapture`, `BackupCutover`, `BackupIncremental`, `BackupProvenance`, `BackupRetention` | Unaffected | They read accepted records and leaf state. |

## Production members

Each production member carries detectors proven red. They land in this order:

1. **Epoch claim, stamped append and the gate.**
   - The epoch, entry and floor rows; `ClaimAppendEpochAsync` and `AppendStampedAsync`;
     the unloaded-entry reload; the unstamped watermark.
   - `WalAppendEpochStaleException`, and its arm in the retry envelope.
   - Entry retirement in pin retirement and the purge tail.
   - `IWalAppendFenceCapable` and its manifest gate.
   - JSON and binary round-trip tests for every new state type. A test that every
     10.2.0 grain interface method still exists with its signature.
2. **Hand-off markers and fences.**
   - Split, leaf reclaim, adaptive split, shard fold and range delete.
   - The shard routing generation, stamped at admission.
   - The append-path stamp census.
3. **Bulk.** #5610's relocating graft, fixed with #5614; the birth hint captured with
   it; bulk-leaf ranges; and the switch's marker and fence.
4. **The live advance itself.**
   - Armed only by the rules above.
   - It reuses #5578's applied-prefix tracker and the core extraction from the
     abandoned #5596.
   - `WalLiveAdvance` then becomes a module with its refinement note.

Change 3 above, option (a), is pending user authorisation; member 1 does not land without it. Member 1 also adds LatticeWalQuiescingReason, the Reason property and the documentation of the new cause.

## Review resolution

| Finding | Resolution |
|---------|------------|
| B1 drains | Section 2: the close is a fence (a re-claim of the initiator's epoch or routing generation), stamped at admission, on every append path. Waiting for returns is shown unsound. The model splits admission from allocation, and `Close` assumes nothing. Mutations: `DrainWithoutFence`, `DrainPointOnly`, `StampAtAppend`. |
| B2 epoch floor | Section 1, "Retiring entries": a monotone per-partition floor row, raised before an entry is deleted. Modelled with `Recreate` and broken by `RecreateDropsFloor`. |
| S1 claim head and tracker | The claim reads `_nextOffset` in the lock section that assigns offsets; arming requires every own append applied. The model has two-step own appends. Mutations: `ClaimReadableHead`, `SnapshotHonestNoOwnTracker`. |
| S2 CAS | A strict conditional write on a row only claims write, which never adopts. The model claims read-plus-one. Mutation: `ClaimAdoptsNoCas`. |
| S3 guard mutations | `NoClaimHeadReplay` and `NoOpenCheck` are added; the redundant `epoch > 0` guard is removed; `PERTURBS` fields name actions. |
| S4 overlap | Section 4 adds slot-less, rangeless-with-siblings and suspected-orphan leaves to the fail-safe list; the refinement gaps will name them. |
| S5 fold vs reclaim | Separate rows: leaf reclaim (per-leaf marker, neighbour retirement) and shard fold (shard-level marker, routing-generation fence). |
| S6 #5610 | Prerequisite of the bulk work, fixed by relocation in the same serial turn as the hint. Modelled by `PreWrite` and `Create`'s relocation. Mutation: `BulkAppendDropsExistingRows`. |
| S7 storage, cost, moves | Reserved-type rows, lazy loading, costs quantified. A move does not touch the rows; restarts are modelled. The move mutation is replaced by `UnloadedEntryAccepted` and `RestartForgetsUnstamped`. |
| S8 rollout | Superseded by the compatibility requirement. The gate is the manifest gate, not ratcheted. Unstamped appends are never rejected, only reported, so neither a downgrade nor a rejoining 10.2.0 silo needs an operator step. The rejection's exception and its retry are named; exhausting the retry is behaviour change 3. |
| S9 impact rows | `AtomicCommit` restated (terminals and the prepare clamp); `TurnOccupancy` and `SplitGateCycles` gain the read-only edge; `WalMove` is a refinement row only. |
| N1 frozen view | `FrozenView` switch. |
| N2 README | Rewritten. |
| N3 conflict retry | Moot: only claims write the epoch row, so every conflict is a claim. |
| N4 bulk hint | Section 3 says the hint is needed by every arming. |
| N5 witnesses | The duplicate's live advance and a trim after a live advance are witnesses. |
| Q1 re-created leaves | The floor, per the recommendation. |
| Q2 adaptive split marker | Shard-level, read fresh through an interleaving read-only call; the shard fold and the range delete use the same marker. |
| Q3 range-delete clipping | Not adopted after all. The record's shape is part of what 10.2.0 consumers see, so the range delete takes the shard-level fence instead (behaviour change 6). |

## Open questions for re-review

1. **Stamp census.** The shard-level fence relies on every write into a shard's leaves
   carrying the routing generation captured at that shard's admission. Point and batch
   writes are admitted at the shard root. Saga prepares, relocation and forwarding are
   candidates for paths that reach a leaf without that admission. A missed path is
   safe, because it appends unstamped, but it disarms the leaves it reaches. Is any
   path known to bypass shard admission that the census should start from?
2. **Rejecting in-flight writes at a close.** The fence fails writes admitted before a
   close that have not been allocated yet, and the retry envelope re-issues them. The
   bounded wait in step (b) keeps that rare. Should the wait be mandatory?
3. **Retry exhaustion (behaviour change 3).** Option (a) is chosen and pending user
   authorisation. Does the reviewer see a design that avoids surfacing anything at all?
