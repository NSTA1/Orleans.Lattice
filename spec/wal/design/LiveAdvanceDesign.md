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

**Revision.** This is the eighth revision. It answers the
[re-review of revision 7](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6570611826):
the epoch row's conditional write is load-bearing again, since `AlreadyDurable` relies on
it, so `ClaimAdoptsNoCas` is a mutation (16 states) and step 2 says so (S1); the purge
straddle text covers a duplicate's bump landing before the clear's conditional row delete
(N1); and TreeWalk cites 22,966,529. Two clarifications follow production members
2 and 3: section 2 states that a leaf initiator's close **bumps** its epoch, that its own
rejected appends are a self-fence, and that whoever appends a record stamps it with its
own epoch (the census rows for receiver-appended hand-offs are corrected); and section 3
corrects change 4's reading of 10.2.0. Bulk entries carry real HLC versions, so a
contested key resolves last-writer-wins by stamp, the later-stamped write winning, not
"the point write wins". The behaviour is unchanged (still last-writer-wins, still a
compatible bug fix); `ContestedKeyAgrees` now versions the bulk entry in time, and its
mutation is `StrandedValueKept`.

Revision 7 superseded revision 6 on one point,
**#5641**, which the independent review of member 1 found: production admits a claim
equal to the durable entry (`AlreadyDurable`), and a clear deletes the epoch row, so a
floor built from entries alone let a pre-purge duplicate and the re-created incarnation
hold the same epoch. Retirement now raises the floor to the higher of the entry and the
epoch row (section 1, "Retiring entries"), the model admits `AlreadyDurable` as production
does, and the entry-only floor is mutation `FloorFromEntryOnly`. It also folds in the
reviewer's partial findings on revision 6: `OtherIntentClose` makes rule 3's own case
reachable, `OnlySupersededRefused` also flags a conflict-free `ClaimConflict`, the
Recreate configuration runs with `Ghosts` on, and a cold leaf's first-write latency is
behaviour change 11.

Revision 6 answered the
[re-review of revision 5](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6568413136)
(1 blocking, 2 should-fix, 2 nits), which confirmed #5635 sound and let member 1 proceed:
- **A rejected range-delete walk re-stamps only under its own intent** (B1, rule 3 in
  section 2), with mutation `RestampUnderAnyIntent`.
- **Any refusal bumps while the activation's ETag holds** (S1), as production does: an
  entry refusal with the row unmoved is the purge straddle, now named in section 1. The
  model gains an ETag stand-in (`rowInc`) and the widened `ClaimBump`.
- **`OnlySupersededRefused` replaces `CurrentNeverFenced`** (S2): it is behavioural, where
  `CurrentNeverFenced` was definitional.
- Step 1 describes production's `ClaimRowAsync`, and production's re-claim after a bump
  and its cost are stated (N1, N2).

Revision 5 answered the[re-review of revision 4](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6566046196)
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
  last-writer-wins by HLC stamp, which is what 10.2.0 converges to on every reader: a
  compatible bug fix needing no authorisation (section 3, "Which value wins").
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
| `_lattice_wal-routing-generation` | WAL shard key and initiator id (a shard root, or a tree for its range-delete generation) | The partition's generation entry for a shard-level initiator, or for the tree's range-delete fence | The initiator's re-claim (conditional max-merge), and retirement at the tree's purge (#5668) |
| `_lattice_leaf-handoff` | Leaf id | The leaf's hand-off marker: initialised flag, open tokens and per-partition close heads | Hand-off initiators, and the leaf's birth |
| `_lattice_shard-handoff` | Shard root id | The shard-level marker (initialised flag, open tokens with their key ranges, close heads), the shard's routing generation, and each in-flight bulk operation's birth hint | The shard root, and the range-delete fence |
| `_lattice_tree-routing-floor` | Tree id | The highest routing generation the tree's purges retired (#5668) | The purge only (max-merge, before any fence row is deleted); read by every first-generation initialisation |
| `TreeRangeDeleteFenceGrain` state (a new grain type, not a reserved-type row) | Tree id | The tree's range-delete generation, and each open range-delete intent: operation id, range, recorded deadline, and the shards whose markers it opened | The range-delete fence grain only |

Each row's state type is new, carries a new stable alias, and is marked
`ILatticeBinaryPersistedState`. Every member is `[Id(n)]` and is a public property
of a concrete type: arrays and concrete dictionaries, never a collection expression
typed as an interface (#5579). The rows therefore also round-trip through a JSON
grain-storage serializer when a host bypasses Lattice's binary serializer.

### 1. The epoch fence: other activations of the leaf

**Claim eagerly, never lazily.** Before an activation's first append, it claims an
epoch. The claim is one serialised sequence per leaf and partition, in three steps:

1. **Persist (`ClaimRow`).** The activation writes the epoch it read plus one to its
   `_lattice_leaf-append-epoch` row (`LeafAppendEpochFence.ClaimRowAsync`), as a strict
   conditional write on the ETag it read. It learns the partition's retired floor only
   through a refusal and a bump (below). Only claims write that row. So every
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
   below the retired floor, or below the higher of the loaded and durable entries. A
   claim **equal** to the durable entry, and not below the loaded one, is admitted without
   a write (`AlreadyDurable`): it is a re-presentation after a WAL shard restart or move,
   which then costs no re-claim. It rests on two things: the epoch row's strict
   conditional write (step 1), which gives each epoch one holder within an incarnation,
   so an equal claim is the same claimant (`ReadPositionHonestClaimAdoptsNoCas`, 16 states,
   drops it); and retirement raising the floor to the leaf's epoch row as well as its
   entry (below, #5641), so epoch values never recur across incarnations. Otherwise it
   writes the entry row as a **conditional (ETag) max-merge**: the row
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

**Any refusal bumps while the row holds.** A refusal, by the entry at the write or the
raise, or by the floor, returns the higher of the entry and the floor. The activation
then persists the higher of that and its row, plus one, to its own epoch row,
conditionally on the ETag of its own last write (`LeafAppendEpochFence`, as #5633
implements it):
- **The ETag holds.** No activation has claimed since this one wrote the row, so it is
  not stale, whatever refused it, and it claims again above the refusal, at most
  `LeafAppendEpochFence.MaxClaimRounds` (8) rounds, after which it appends unstamped
  (reported, section 4).
- **The ETag conflicts.** Another activation has claimed since, so this one is stale: it
  stops, and the caller's retry reaches the live one (`WitnessClaimRefused`).

An entry refusal with the ETag holding would mean the entry above this activation's
epoch was left by an incarnation the row no longer records: the **purge straddle**
(below), which the row floor of #5641 now refuses first. The model has `ClaimBump` for the bump
(`WitnessClaimBumped`); it bounds the rounds by `MaxEpoch` and stops a bump that has no
room, standing for the unstamped fallback, which `WalLiveAdvanceGateFlip.cfg` covers.
Revision 5 stopped the activation on an entry refusal, which stopped a re-created
incarnation in the straddle (`OnlySupersededRefusedEntryRefusalStops`, red in 9 states at
revision 6); since #5641 closes the straddle it is a redundancy row.

**The ETag in the model.** Production compares the row's ETag; the model compares the
pair of the row's incarnation (`rowInc`, raised by `Recreate`, the purge's delete) and its
epoch. Within an incarnation every row write raises the epoch, so the pair identifies a
write, and an epoch value that recurs after a purge never matches an old activation's
write. Comparing the epoch alone would let a pre-purge activation's compare succeed
where production's ETag fails.

**The stamped append.** Once the cluster's fence gate is open (section 4), an
activation appends through new stamped methods; the existing methods are unchanged:
- `AppendStampedAsync`, for a single entry;
- `AppendBatchStampedAsync`, for a batch. This is the dominant path:
  `WalCommitLogWriter.AppendManyAsync` reaches `IWalShardGrain.AppendBatchAsync`
  today, and `WalBatchedSingleEntryAppends` routes even single entries there. Each
  entry of the stamped batch carries its own stamp (leaf identity, epoch, and the
  shard routing generation of section 2). Under the one `_stateGate` section that
  assigns the batch's offsets, the shard checks every entry's stamp before it assigns
  any offset. A batch is rejected at its first failing entry, so nothing of it is
  allocated (`WalAppendFenceOutcome`, as #5633 implements it); the response carries the
  outcome per entry and the unstamped watermark (section 4). The coalescing writer fails
  the callers of a rejected batch, and their seams retry them (section 5).

The shard accepts a stamped entry only if the leaf's entry is loaded and the epoch is
not below it. It does **not** check the retired floor on the append path (#5635). The
shard reports a refusal as an outcome, not an exception: `StaleEpoch`, `UnloadedEntry`,
`StaleGeneration` and so on (`WalAppendFenceOutcome`). The leaf turns `StaleEpoch` into the
existing `LeafRetiredException`, with `ActivationSuperseded` set and the superseded epoch,
which its callers' seams retry (section 5; the wire form is under
[Wire formats](#wire-formats)). An entry not yet loaded rejects the append as
unloaded (below); the leaf re-presents its claim and
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
shared floor above epochs L holds, and `OnlySupersededRefused` states that only a
superseded activation is ever refused, at a claim or at an append. It is behavioural:
putting the floor back in the admission predicate (`OnlySupersededRefusedFloorAtAppend`)
or in the append guards beside it (`OnlySupersededRefusedFloorInAppendGuards`) both fail.

**Another leaf's retirement at the claim.** The claim-time checks are partition-wide
too: a claim between its row write and its raise when another leaf is retired with a
higher epoch is refused by the floor. Its ETag holds, so it bumps and re-claims (above).
Cost: one extra claim round per claim in flight at another leaf's retirement in the same
partition (N3, below).
**One epoch per activation, across partitions.** An activation has one epoch, stamped on
every partition it writes. A floor-only bump forced on partition P2 therefore changes the
epoch it would stamp on P1. Production does not stamp P1 with an epoch it never claimed
there: `StampForAsync` stamps only when the partition's claimed epoch equals the
activation's, so the next append to P1 first re-claims P1 under the new epoch, and P1's
claim head is unset until then, so P1 is disarmed until the leaf replays to the new head.
The model checks a weaker discipline that over-approximates it: `OtherPartitionBump`
(`WithSecondPartition`, `WalLiveAdvanceTwoPartition.cfg`) bumps a raised activation's epoch
from another partition at any time and keeps stamping P1 without a re-claim, which P1
admits because the epoch is above its entry; every property still holds
(`WitnessOtherPartitionEpoch`), because any other activation must first win the leaf's
epoch row, and its claim on P1 then exceeds the bumped epoch.

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
the partition's floor row, durably and before the entry row is deleted, to at least
the **higher of the leaf's entry and its epoch row**: the clear passes the epoch-row value
it already reads into the WAL shard's retirement (#5641). No removal ever lowers the
floor, so the entries stay bounded by live leaves.

**Why the row, not only the entry (#5641).** A clear deletes the leaf's epoch row, so
epoch values recur in the re-created incarnation. A pre-purge duplicate D that wrote
the row (epoch e) but not yet its entry at a partition leaves no entry there, so a floor
built from entries alone stays below e. D's entry write then lands after the purge, D
raises and appends, and the re-created incarnation R, reading the row as absent, claims
the same e and is admitted as `AlreadyDurable`: both hold e, neither is ever found stale,
and R arms past D's acknowledged appends. Raising the floor to the row refuses D's
straddling entry write; D's bump then either meets the deleted row and stops, or lands
before the clear's conditional row delete, which then conflicts and keeps the row (see
"The purge straddle"). The model's `Recreate` is one atomic step; the reviewer's
three-step clear (read the row, retire, conditional delete) is clean at 20,374,851 states,
and the promoted module should model the clear in those three steps. Building the floor
from entries alone fails (`ReadPositionHonestFloorFromEntryOnly`, 17 states).

A re-created leaf (a purge re-creation, or an id reused by a create) starts a new
epoch row. Its first claim lands in an absent entry, so it must exceed the floor,
and therefore every epoch a surviving duplicate of the old incarnation holds.
`ReadPositionHonestRecreateDropsFloor` removes the floor raise and fails.

**The purge straddle.** A pre-purge duplicate D can write its row (epoch e) before the
purge and its entry after it. Raising the floor to the row (above) refuses that entry
write, since e is at or below the floor. D then bumps. The clear reads the row, retires
the partitions, then deletes the row conditionally on what it read, so D's bump meets
either the deleted row, and D stops, or, landing first, the row the clear read: the
clear's conditional delete then conflicts and the row is kept with D's bumped epoch, so
the next activation continues above it and no value recurs. Either way no entry is left
above a held row of the re-created incarnation. Production's
bump on any refusal while the row holds (section 1, "Any refusal bumps while the row
holds") is therefore redundant in the model since #5641; it is kept, because it is the
general recovery for an entry above a held row, however one arises, and it is what
#5633 implements (redundancy row `EntryRefusalStops`).

Retirement code runs whether the gate is open or closed, so rows are retired on every
path a new silo runs: every leaf clear (purge, reclaim, orphan repair) and the WAL GC's
orphan-pin retirement, which also re-drives an interrupted clear. A retirement on a
10.2.0 silo, during a rollback or a rejoin after the gate opened, deletes none of these
rows (so the epoch row survives, epoch values cannot recur, and the floor needs no row
value), and it also removes the leaf's materialiser pins, so the orphan-pin pass never
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
  writing P partitions costs at most N x P entry-row writes once, not O(N^2). The first
  write after every leaf activation waits for that claim: a leaf-row read and write, then
  per partition a grain call with an entry read and write, about four sequential storage
  round trips before the first append (change 11).
- **Per retirement:** a leaf clear reads the leaf's epoch row and, if it has one, calls the
  WAL shard's retirement at every partition, each a floor write and an entry delete, so a
  tree purge costs N x P grain calls with storage I/O for N leaves with rows. The WAL GC's
  orphan-pin retirement costs a grain call and a floor and entry read at the partition
  whose pin it removes.
- **After a retirement (N3):** the floor is one scalar per partition, so after any
  leaf's retirement every new leaf whose epoch row is older than the floor pays one
  refused claim per partition it writes, then bumps and claims above the floor; and every
  claim in flight in that partition when another leaf is retired with a higher epoch pays
  one extra round. Appends are never affected (#5635). A bump forced at one partition
  costs one extra claim round at **every** partition the activation writes, and a re-arm
  (a replay to the new claim head) at each.
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
   - (c) **bumps and re-claims** its own epoch (a leaf initiator: it persists its row
     plus one, conditionally on its own last ETag) or its routing generation (a
     shard-level initiator) at every partition it may have written. A same-value
     re-claim would fence nothing; the model's `Close` raises the initiator's epoch
     (`fEpoch' = fEpoch + 1`). The re-claim returns allocator heads, exactly as a claim
     does;
   - (d) closes the marker, recording those heads.

   **A leaf initiator's own rejected appends are a self-fence, not supersession.** A
   donor's appends admitted under its old epoch and not yet allocated at (c) are
   rejected as stale. With its row ETag still held and the stamp below its own current
   epoch, that rejection is a distinct internal outcome: the activation does not
   deactivate, and its seam retry re-admits the write under the current epoch, where span
   admission forwards a key the narrow moved and a key it still owns re-appends. On the
   wire it stays the existing `LeafRetiredException` with `ActivationSuperseded` set and no
   new field: a 10.2.0 shard root calling this leaf across a gate race deserializes it and
   retries until its deadline, and every new seam already retries it. A genuine
   supersession still meets the next current-epoch append and deactivates. The self-fence
   has its own counter (change 7). It also covers a floor bump's own in-flight appends
   (#5643). The
   order is narrow (a), then bump (c), then close (d); the in-memory epoch moves only
   after the row's conditional write succeeds. The initiator's own claim heads move too,
   so it is disarmed until it replays to them (N3).
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
14 states). A raise of any fenced identity, a leaf's epoch, a shard root's routing
generation (per initiator) or the tree's range-delete generation, drains the in-flight
stamped batches before it reads its heads, so no entry of a batch admitted under the old
value is allocated above the heads the raise returns.

**Stamped at admission.** The stamp is taken where the write is admitted, not where
the append is built. A leaf captures its epoch in the same synchronous step as span
admission. A shard root captures its routing generation in the same step as its phase
check (`ThrowIfRejectedForKey`) and passes it to the leaf through a new stamped
leaf-write method. If the append read the current epoch instead, a write admitted
before the close would carry the re-claimed epoch and be accepted
(`ReadPositionHonestStampAtAppend`).

An implementation may carry an **admission mark** instead of the epoch value: the
activation's count of hand-off closes plus one, captured in the same synchronous step
as span admission. At dispatch, a write whose mark predates a close is refused as a
self-fence (section 2) and re-admitted, where span admission forwards a moved key. The
raised epoch therefore never stamps a write admitted before the close, which is all
`ReadPositionHonestStampAtAppend` requires. It refuses fewer writes than a captured
epoch value, because a claim or a floor-refusal bump is not a close. Mark `0` means
"not captured" and is never carried by a stamped entry. The fence that counts the closes
must therefore exist at admission, not be created lazily at the activation's first append.
Otherwise a write admitted before that first append reads "no fence yet" as mark `0`, a
close lands before it is dispatched, and it is stamped with the raised epoch above the
heads. A path that appends with mark `0`
stamps at dispatch, so it must not append a key the initiator may have narrowed away:
it captures its own mark before its span check, as the tx-terminal backstop does (the
census, below).

**The unstamped leaf methods stay unstamped (S3).** A write that reaches a leaf
through an existing leaf-write method carries no routing generation: a 10.2.0 shard
root admitted it, or a path the census missed. The leaf appends it through the
**unstamped** WAL method, never stamped with its own epoch alone. A leaf-epoch stamp
would pass the leaf's own check and escape every shard-level fence unreported; an
unstamped append is admitted and reported (section 4), which is safe and disarms the
leaves it reaches.

**What the close heads must bound (S2, #5664).** The owner re-arms after replaying to the
close heads and may then live-advance past anything above them, so the heads must bound
**every** record the initiator appends that the owner holds, not only those stamped before
the bump. Three production shapes fall outside that bound unless guarded:
- **A write admitted before the narrow but stamped after the bump.** The stamp must be
  the epoch captured at **admission** ("Stamped at admission", above, which also admits
  an admission mark), carried to the
  append. Taken at dispatch, it is the raised epoch, and the write is accepted above the
  heads (`ReadPositionHonestStampAtAppend`, 13 states).
- **A partition whose claim was reset.** After a WAL shard restart or move resets a
  partition's loaded claim, the initiator has still stamped appends there. The close must
  re-claim **every partition it has claimed in this activation**, and record no head
  (`-1`) only for a partition it never claimed. A head recorded as `-1` where it did append
  lets the owner skip them (`ReadPositionHonestCloseHeadUnderstated`, 13 states).
- **A close while the initiator's gate view is closed.** It cannot bump. If it has claimed
  any partition, a stamped append may be in flight, so it must **leave the marker open**,
  and the owner keeps advancing by replay, until a later close can bump and re-claim. Only
  an initiator that never stamped may close on unfenced heads. A close that records heads
  without a fence is `ReadPositionHonestDrainWithoutFence`.

The span re-check at apply (`StoreAdmittedEntry`) and the receiver's own forward or
straggler transfer make each of these harmless while the initiator stays up: the moved
row reaches the owner as the owner's own append. That argument fails when the initiator's
record is durable but the forward never happens. It can crash between its append and its
apply, or its reactivated replay can no longer own the key. The record is then owned by
the owner and applied by every WAL reader, and a live advance past it diverges for good.
The model lets a hand-off never happen for exactly this reason (`HandOff` is optional),
so it does not rely on the forward; the three guards are what make the heads a bound.

**Who stamps.** Whoever calls the WAL append stamps it with **its own** epoch (and, for a
write admitted at a shard root, that shard's routing generation). A record appended by
the leaf that owns it is that leaf's own append, tracked by its applied-prefix tracker,
and needs no marker. A marker on the owner is needed only for records a **different**
author appends, such as a donor's appends of moved keys before its narrow, which the
donor's bump fences.

**The stamp census.** Production member 2 carries a census of every path that appends
an entry a leaf owns, with a detector per path. Each path is stamped, classified, or
stated "unstamped, disarms". A path that bypasses the stamp is the gap the fold's
point-only guard has today (`ReadPositionHonestDrainPointOnly`). The census starts
from the paths that reach a leaf without shard-root admission:

| Path | Where | Treatment |
|------|-------|-----------|
| Point, batch, CRDT apply, merge, delete | Shard root admission, then the stamped leaf-write method | Stamped with the shard's routing generation and the leaf's epoch |
| Saga prepare | `BPlusLeafGrain.PendingTx` | Stamped at the participant's admission |
| Stranded relocation, leaf to leaf | `BPlusLeafGrain.SpanAdmission.cs:431`, `MergeManyAsync` | Appended by the receiver: stamped with the receiver's own epoch, its own append. The donor's own earlier appends of those keys are fenced by its bump under its marker on the receiver |
| Re-routing a terminal's stranded prepared keys, and the backstop appends | `BPlusLeafGrain.PendingTx.cs:2534`, `:2719ff` | Whichever leaf calls the append stamps with its own epoch: the sibling's re-routed appends are its own; a backstop append by the re-routing leaf for a key it no longer owns stays stamped with that leaf's epoch, under its marker on the sibling. The backstop forwards keys outside its span to the owner, which appends them itself; it captures its admission mark before its span check, so a local key a split narrows away while the forwards are awaited is self-fenced and re-admitted, never appended under the raised epoch |
| Split transfers | `BPlusLeafGrain.Split.cs`, `MergeEntriesAsync` | Appended by the sibling: stamped with the sibling's own epoch (born under the gate, it claims eagerly). The donor's pre-narrow appends of moved keys are fenced by the donor's bump under the split's marker on the sibling |
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
| Split | Donor leaf | In the sibling's marker row, written in the sibling's birth (`InitializeSiblingAsync`) | The donor's narrow of `HighKeyExclusive`; span admission forwards moved keys afterwards | The donor bumps its epoch and re-claims it at every partition it writes; the heads close the sibling's marker; its own rejected appends are a self-fence |
| Leaf reclaim | Absorbed neighbour leaf | On the survivor, before its range widens | The neighbour's unlink | The neighbour's entry is retired (the floor rises), so its later stamped appends are rejected; the heads are captured in the retire step |
| Adaptive split | Donor shard root | Shard-level marker on the target shard root | The reject phase | The donor shard root re-claims its routing generation at every partition; the heads close the marker at completion |
| Shard fold | Folded shard root | Shard-level marker on the survivor shard root | The fold's quiesce, point and batch | The same routing-generation re-claim |
| Range delete | The tree's range-delete fence grain (below) | A range-scoped marker on **every** shard root of the tree, before any shard appends | The end of every shard's walk, or the intent's recorded deadline | The fence grain re-claims the tree's range-delete generation at every partition once; the heads close every shard's marker |
| Bulk load / append | The shard root, for writes admitted before the switch | At the bulk leaf's birth, with the birth hint (shared by a run of append chunks) | The switch, in the shard root's serial turn | The shard root re-claims its routing generation at every partition; the heads close the marker. Completed append tokens close in batches (section 3, #5702) |

**A resumed division applies no read position (#5706).** A forward division (a split,
or the bulk switch's narrow) drains the mutations admitted before it reads the heads it
gives the new leaf as its read position, so nothing admitted earlier lies above them
unapplied. A division resumed on the recovery path (`CompleteSplitAsync`) cannot drain
commits that were in flight when the original activation died, so it applies no read
position, and the sibling replays from its own start. The close heads do not cover this:
they gate arming only, not the sibling's replay start.

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
retry then asks the fence grain whether **its own** intent, named by id in the stamped
call, is still open. If it is (another delete's close re-claimed the shared
generation), the walk is re-stamped with the generation the fence grain returns; if
not, it appends unstamped, so the walk completes as it would in 10.2.0. Two concurrent
range deletes on one tree share the generation, so one's close rejects the other's
in-flight appends once, and each is retried this way.

**Rule 3: re-stamp only under the walk's own intent (revision 6).** Markers are
range-scoped, and a leaf disjoint from an open marker ignores it. So another open
intent, I2 on range R2, does not hold a leaf X that overlaps the walk's range R1 but not
R2: X re-armed after the walk's own intent I1 closed. A walk re-stamped under I2 would
be admitted above I1's close heads, and X would live-advance past a range-delete record
it must replay, and never apply the delete. Re-stamping only while I1 is open is safe:
I1's markers still hold every leaf overlapping R1. In the model, `OtherIntentClose`
(under `WithTreeWalk`) is that case: another delete's close re-claims the shared
generation while the walk's own intent, whose marker holds L, stays open, and `GRestamp`
re-stamps the walk (`WitnessTreeWalkRestampedOwnIntent`). `GRestamp`'s guard reads L's
marker (`hoOpen`), which is a **safe over-approximation** of rule 3: it includes re-stamps
under a newer intent covering L, which production forbids. Dropping the guard stands for
re-stamping under an intent disjoint from L (`ReadPositionHonestRestampUnderAnyIntent`,
15 states). Re-stamping under any intent whose range contains the walk's would also be
sound, but needs a containment test for no benefit.

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

**Purging a tree's fence rows (#5668).** The per-tree rows (each shard root's
`_lattice_shard-handoff` marker, every partition's `_lattice_wal-routing-generation`
entries, and the `TreeRangeDeleteFenceGrain` state) are retired by the purge, before
its registry removal and re-driven while `RegistryUnregisterPending`, in this order:
1. **Floor first.** The purge reads the highest routing generation over every shard
   root's marker, every partition's generation entries (each shard-root initiator and
   the range-delete initiator, durable and loaded) and the fence grain's generation,
   and max-merges it into the tree's `_lattice_tree-routing-floor` row **before any
   delete**. Written after the deletes, a crash between them leaves a re-drive reading
   rows that are already gone, and the floor is never raised
   (`ReadPositionHonestRoutingFloorAfterDeletes`, 15 states).
2. **Then retire.** It retires the generation entries (durable and loaded), deletes the
   marker rows and clears the fence state.
3. **Re-creation starts above the floor.** A re-created incarnation initialises every
   first routing generation (the shard root's and the range-delete fence's) at the
   floor plus one, on every initialisation path, including one taken while the gate is
   closed that opens later. Starting at 1 lets generation values recur, the #5641 shape
   applied to generations (`ReadPositionHonestRoutingRecreatedAtOne`, 15 states).

The WAL keeps no generation floor, and that makes two further rules load-bearing. A
retired entry is absent, and an absent entry admits any stamp: the leaf re-presents an
unloaded generation and the claim max-merges with no floor
(`WalAppendFenceCore.IsGenerationAdmitted`). So a **straddling shard root** of the
purged incarnation, which admitted a write before the purge and dispatches it after, is
refused only once a generation above its stamp has been claimed at that partition. New
generations exceed every old one, but only once they are claimed.
- **The purge leaves the marker state uninitialised.** A deleted marker is absent, and
  rule 2 holds the leaves until their marker state is initialised again. Read as
  closed, it releases a foreign write the open marker held
  (`ReadPositionHonestRoutingPurgeKeepsMarkerInit`, 13 states).
- **The re-created incarnation's first initialisation is a fence.** When the floor is
  above 0, the marker reads initialised only after every initiator the purge retired
  has claimed its seeded first generation at every WAL partition of the tree, with the
  heads recorded as a close records them: each shard root its own, and the range-delete
  fence its own, since a straddling walk carries the range-delete initiator's stamp. A
  shard root's initialisation can drive the fence grain's, idempotently, before it
  writes its own initialised flag. Seeding the value without claiming it admits the
  straddling write above the recorded heads (`ReadPositionHonestRoutingInitWithoutClaim`,
  15 states). A tree whose floor is 0 has no straddler, so its initialisation claims
  nothing extra. Today's `EnsureRoutingGenerationSlowAsync` sets the generation to 1 and
  claims nothing, so the #5668 member changes it on both counts.

A straddling shard root's own close re-claims at most the floor plus one, fencing only
its own writes, and its marker write meets a row the purge deleted, so its conditional
write conflicts. The model has one foreign author, so neither is modelled.

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

A bulk load or append captures per-partition WAL heads before its first leaf is born,
before any write it can race. The leaves it creates take those heads as their birth
hint. The hint is the read position they start from, as a split sibling starts from its
donor's heads (`WalHeadsAtBirth`). With the open marker, re-arming then means a short, bounded
replay from the hint to the close heads of the switch's fence. That replay imports any
write that reached the old leaf in the window, before the advance proceeds as for any
leaf.

The hint is needed by every arming, not only by the live advance: it is the leaf's
birth read position. Starting the leaf at the head when it is created instead fails
without any live advance (`ReadPositionHonestBulkWithoutHint`).

**A run shares a hint (#5702).** A chunk's hint may be any head read before its first
leaf is born: a lower hint is never above an unapplied record, it only lengthens the
replay. So consecutive append chunks on a shard reuse the previous chunk's hint, or the
heads of the fence they fold, and read the partitions' heads only at the start of a
run. A resumed operation's stored hint always wins (S7, below).

**The hint survives a crash (S7).** The hint is written to the shard root's
`_lattice_shard-handoff` row, keyed by the bulk operation id, in the serial turn that
captures it and before any leaf is created. A crash between the capture and the switch
resumes the same operation id with deterministic leaf ids, and the resume reuses the
stored hint; a later one would skip the window the hint exists to cover.
`PendingBulkGraft` stays in `ShardRootState` unchanged. The hint row is deleted when the
switch's token closes.

**Deferred, batched close (#5702).** A bulk append no longer closes its own `bulk:{op}`
token. A shard's completed bulk tokens (open, not the current operation's, not a pending
graft's) are closed together by one routing-generation fence and one marker write: when
eight have accumulated (folded into the next chunk's open write), after five seconds with
no chunk (a grain timer), or by a bulk load, which still closes at its end. A fresh
shard-root activation folds any stale tokens into its first chunk, which also closes an
abandoned switch. The open tokens hold the leaves they cover on the replay-based advance
for longer, which is safe, only slower. The token and range interface is unchanged.

**Existing rows: relocate, never refuse (#5610).** Today a bulk append grafts its
leaves without checking the keys the rightmost leaf already holds, so a point write
into the bulk range between chunks becomes unroutable. #5614's inflated counts are
probably the same graft. In the same serial shard-root turn that captures the hint,
the graft finds the rows the bulk range already holds and relocates them into the new
bulk leaf at its birth, exactly as a split relocates its moved rows, narrowing the old
leaf. It never refuses the chunk: a chunk that succeeds today keeps succeeding.
Skipping the relocation fails (`ReadPositionHonestBulkAppendDropsExistingRows`).

**Narrow and link in one persist (#5654).** The graft narrows the old rightmost leaf's
high bound and links it to the first appended leaf in **one** persist
(`NarrowHighKeyAsync(highKeyExclusive, nextSibling)`). Narrowing first and linking later
leaves a window in which the old leaf declares a narrower range but has no
`NextSibling`, because it was the shard's rightmost leaf. A point or batch write at or
above the new bound that still routes to the old leaf, the #5610 shape, then finds no
forward target in span admission (`SpanFailOpenReason.NoSibling`) and is stored on the
old leaf outside its declared range. Once the appended leaves are routable, the key
routes to a leaf that does not hold it: the write is acknowledged and its WAL record
reaches replicas and views, but the origin misses it until the appended leaf replays.
With the bound and the link set together, every key at or above the bound forwards to
the appended leaf from the moment the bound narrows. `SetNextSiblingAsync` stays, as an
idempotent re-assertion for a resumed graft. The model's `Create` relocates the rows and
re-points the owner in one step, so it has no such window; the bulk member's detector,
a write interleaved between narrow and link that must be readable on the origin with the
span fail-open counter at 0, pins it.

**Which value wins (S5): last-writer-wins by HLC stamp; the later-stamped write wins.**
What 10.2.0 does with a key both a bulk chunk and another write hold:
- **Bulk entries carry real versions.** `BulkAppendAsync` stamps its entries with
  `HybridLogicalClock.Tick` (`ShardRootGrain.BulkLoad.cs:440`, `:460`, `:493`), which
  reads the wall clock (`Primitives/HybridLogicalClock.cs:26-39`), so a bulk entry is
  versioned at the time its chunk was stamped, like any write. It is written through
  `MergeEntriesAsync` (`BPlusLeafGrain.cs:3703`), which appends a merge record to the WAL
  and resolves last-writer-wins by version. A point write is versioned by its leaf's
  clock (`BPlusLeafGrain.cs:687`). So on every reader that applies both, the
  later-stamped write wins.
- **Keys that fill the existing rightmost leaf's free space**
  (`ShardRootGrain.BulkLoad.cs:454-465`) are merged into the leaf that already holds the
  other row: the later-stamped value wins, immediately and permanently.
- **Keys placed in new bulk leaves.** Replicas and views apply both records and return
  the later-stamped value. The origin returns the bulk value while the other row is
  stranded on the old leaf, and flips when a rangeless bulk leaf replays, because it
  owns every key of its shard during replay (`ShardRootGrain.BulkLoad.cs:290`).

So 10.2.0 already resolves a contested key last-writer-wins by stamp on every reader
except the origin before a replay. The divergence the fix removes is that narrower
set: on the origin, a key only the pre-existing rows hold is unreadable, a stranded
write stamped later than the bulk entry is hidden behind the bulk value, and the
stranded rows are still counted (#5614's duplicates). The relocation makes the origin
resolve every key as its replicas and views do: the later-stamped value is returned,
keys only the pre-existing rows hold become readable again, and counts and scans
become exact. This is a **compatible bug fix** (change 4): it fixes the
origin-versus-replica divergence and needs no authorisation. The model states it as
`ContestedKeyAgrees`: the owner resolves the contested key to the value a WAL reader
resolves at the same position. A bulk entry is versioned on the shard root's clock
when its chunk is appended, so its order against a concurrent write is arbitrary; the
model fixes it between the writes before the bulk began and those after, and the
reviewer's runs with every other placement are clean too.
`ContestedKeyAgreesStrandedValueKept`, where the owner keeps a stranded or skipped value
over a later-stamped write made before the switch (10.2.0's stranded origin read),
fails in seven states.

**Re-delivery (#5474, member 3).** A run of a chunk below the right edge may be skipped as
a re-delivery only when **every** key it carries at or below the right edge is already
held **with an identical value and identical metadata** (value bytes, TTL, tombstone):
nothing the run would write differs from what every reader already resolves. Every other
below-edge run is relocated and merged last-writer-wins as above, the later stamp
winning; a run is never skipped in part.

Key presence alone cannot tell a re-delivery from a fresh chunk that overwrites existing
keys: a refresh that re-imports an updated dataset into a populated tree, an import
resumed into a tree that has since taken writes, or a writer that touched every
below-edge key of the next chunk. Skipping such a run by presence would write no WAL
record, return success, and leave every reader, origin, replicas and views, on the older
value, with a held tombstone keeping a re-inserted key deleted. In 10.2.0 the same chunk
was filled or grafted with fresh stamps, so replicas and views resolved it to the later
bulk value. That would be a replica-visible behaviour change (revision 8's rule, found in
the review of member 3's #5646). Under the value-and-metadata rule:
- a genuine re-delivery with nothing changed since is all-equal, and is skipped;
- a refresh with new values is merged, and the later stamp wins, as in 10.2.0;
- a re-delivery after a later overwrite re-stamps the bulk value over it, consistently on
  every reader, which is what 10.2.0 did on replicas.

A skipped run's visible values are the same everywhere, so skipping cannot make the
origin and its replicas disagree. No state and no wire format changes. The model has one
bulk entry per key and no re-delivery step, so it does not encode the skip rule; the bulk
member's detector does: a point writer holding every below-edge key of the next chunk
(the reviewer's widened `BulkAppend_after_point_writes_above_the_maximum_keeps_every_key_readable_last_writer_wins`)
must leave the later-stamped bulk value readable on every reader.

**Rejected: a stable "bulk value wins".** Dropping the other row on the owner is
unsound: replicas and views still apply it at its real version, so origin and replicas
would disagree for good. The only replay-consistent form is to stamp the contested
bulk entries above every earlier version, with the relocation narrowing the old leaf
and reading its clock in one step. That flips every key whose other write is stamped
later than the bulk entry, on every reader, from that write's value to the bulk value:
a documented-behaviour change on paths where 10.2.0 is already stable. The user may
still choose it later; it would need that authorisation. #5610 and #5614 are fixed
together in the bulk member, which confirms whether they share a cause.
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
  - the leaf is suspected to be an orphan: orphan repair is pending for it, or it is
    not reachable by descent. An orphan sharing a live leaf's bounds materialises a
    full copy of that leaf's range. The descent check is the orphan signal, and it
    subsumes a scan-chain flag: an orphan that shares a live leaf's bounds is never the
    leaf descent reaches (#5704, #5709).
- **Storage.** The grain storage provider does not enforce ETags
  (`GrainStorageFencingProbe` reports so). The claim's conditional writes prove
  nothing without them.
- **Rollout.** The fence gate is closed. Nothing claims, the shards keep no entries,
  and every advance is replay-based.

**An unreachable leaf never stamps (#5709).** The overlap rule above holds the
**orphan's** advance. The reverse direction needs its own rule. An unreachable orphan O
can share bounds with a reachable leaf L, and it is still doubly linked into the sibling
chain, so a span forward from its predecessor, or a stale route, can deliver a write for
L's keys into O. O's append of it would be stamped with O's own epoch under O's own
entry, so no fence rejects it, and L's live advance would pass it unread: the record
reaches replicas and views, but reads through L never see it. Orphan repair cannot be
relied on to close this. It runs only when an operator invokes it, and it clears an
orphan only after proving, key by key, that every row the orphan holds is readable from
the reachable owner; otherwise it refuses and leaves the orphan untouched. It never folds
rows into L. So the rule is at the source:
1. **Claim-time reachability.** Before an activation's first claim of its append epoch,
   the leaf proves it is reachable by descent, with the same interleaving shard-root
   check the live-advance view makes, once per activation. An unreachable leaf does not
   claim, and appends through the **unstamped** WAL method, as with a closed gate view.
   An unstamped append is always reported and raises the partition's unstamped
   watermark, so any leaf sharing the partition replays past it before it arms, and
   L's replay applies O's record by key range. That covers an orphan anywhere in the
   chain, with no persisted audit result.
2. **Born under an open hand-off marker.** A split sibling claims eagerly at its birth,
   before its parent learns of it, and is legitimately unreachable until then. It keeps
   its claim: it admits only keys the donor has already narrowed away, because the donor
   forwards a key only once it is outside the donor's span.
3. **Tick re-check.** If an advance tick's descent finds the leaf unreachable while no
   marker is open on it, the leaf stops stamping for the rest of the activation.

The residue is the window between a leaf becoming unreachable and its next tick. No
correct topology change opens it: a reclaim retires the leaf under the epoch floor, and a
split is under its marker. Only a defect that mints a live orphan would. In the model an
orphan's append is `OldAlloc` (unstamped, reported), and dropping the report is
`ReadPositionHonestUnstampedNotReported`, the same counterexample.

### 5. Rejections are retried where they are raised

A stale-epoch or fence rejection is not a fault that repeats by itself. A re-issued
write is admitted under the **current** epoch and generation, and only another claim
or another close can reject it again. Both are finite within any interval: a claim
needs a new activation, and a close needs a hand-off. So the design retries a
rejection at the seam that raised it, for as long as the operation's **existing
deadline** allows (its cancellation token, or the response timeout of the call that
carries it), not for a fixed number of attempts. When that deadline expires, the caller
receives exactly what it receives today when the deadline expires. Nothing new is
surfaced: the fence's outcomes and the `LeafRetiredException` that carries a supersession
are internal, never cross a public API boundary, and are never an inner exception of
anything public.

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
  - The new request and response types have new aliases. No existing alias changes, and
    no existing `[Id]` is renumbered or removed. `LatticeWalQuiescingException` is
    untouched (revision 2's `Reason` field is withdrawn).
  - **The one additive change to an existing serialized type.** A supersession is the
    existing internal `LeafRetiredException`, alias `ol.lrt` unchanged, with two trailing
    members appended after `[Id(0)] LeafId`: `[Id(1)] ActivationSuperseded` and
    `[Id(2)] SupersededEpoch`. A 10.2.0 silo's deserializer skips the two trailing fields, so
    a 10.2.0 shard root that reaches a newer leaf across a gate race sees exactly today's
    retired-leaf refusal and retries it until its deadline, as every `LeafRetiredException`
    handler does (a shard-root retry, never "skip this leaf"). A newer caller retries a
    supersession at its seam against the live activation. Member 2's self-fence uses the
    same form (section 2). `LeafRetiredExceptionWireCompatibilityTests` pins the trailing-field
    skip in both directions, and the alias has its own test. No new exception type crosses a
    silo boundary.
- **Cross-cluster replication.** Shipped records are WAL records, which are unchanged,
  so a 10.2.0 peer and a new peer exchange exactly what two 10.2.0 peers exchange, in
  both directions. A replicated write applied on the receiving cluster goes through
  the receiving leaf's own append path: stamped on a new cluster with the gate open,
  unstamped otherwise; a rejection is retried below the applier (section 5).

### Mixed-version rollout

Fencing and the live advance turn on only once every active silo supports them, and
off again if a 10.2.0 silo joins. At every moment:
- a 10.2.0 silo's unstamped append is never rejected;
- a retirement on a 10.2.0 silo keeps the leaf's epoch row, so epoch values do not recur
  after it, and the floor needs the row value (#5641) only from new-build clears, which
  pass it;
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
| 4 | **Bulk append over existing rows (#5610, #5614).** The graft relocates the rows the bulk range already holds into the new bulk leaf, instead of stranding them. No new exception: a chunk that succeeds today still succeeds. A key both the bulk chunk and another write hold resolves last-writer-wins by HLC stamp, the later-stamped write winning, as 10.2.0 already resolves it on every replica and view, for every free-space key, and on the origin after a replay. Only the origin's pre-replay reads change: keys only the pre-existing rows hold become readable, a later-stamped stranded write is no longer hidden, and counts and scans become exact (#5614). A run below the right edge is skipped as a re-delivery only when every key it carries there is already held with an identical value and metadata; any other run is relocated and merged last-writer-wins, so a refresh's later-stamped values reach every reader as in 10.2.0. The graft narrows and links the old leaf in one persist (#5654). | Compatible bug fix (fixes an origin-versus-replica divergence) |
| 5 | **The live advance itself (#5572).** A leaf's checkpoint advances without a replay from -1. The WAL GC trims sooner, replay demand falls, and #5570's "carries no usable offset" warnings stop for live leaves. Existing metrics report different values; none changes name, unit or tags. | Compatible bug fix |
| 6 | **Range deletes cost more.** Each `DeleteRangeAsync` call opens an intent at the tree's fence grain, opens and closes a range-scoped marker on every shard of the tree, re-claims the tree's range-delete generation once per WAL partition, and disarms every overlapping leaf of the tree for the call's duration. The shard root adds no per-page call: it trusts the generation it is given and asks the fence grain only after a rejection. Stepped range deletes pay this per step (section 2). A range-delete walk that stays unstamped (replication apply, a shadow forward, a gate-spanning fan-out, a 10.2.0 caller, or a page after the intent's deadline) costs every armed leaf on the partitions it reaches a tail replay past it. The record, its result and its feed and history output are unchanged. | Compatible (cost only) |
| 7 | **New metrics.** New instruments only, on the existing meter: claims, refusals and rejections by reason, seam retries by caller class, self-fences (`orleans.lattice.wal.append_fence.self_fenced`), live advances by arming outcome, open markers, and unstamped reports by source (replication apply, shadow forward, gate-spanning fan-out, 10.2.0 caller, census-unstamped, and a range-delete page past its intent's deadline). No existing instrument gains a tag, a tag value, or a new unit. | Compatible (additive) |
| 8 | **New log lines.** A deactivation warning for a stale activation, and a debug line when a partition disarms on an unstamped report. #5571's blocked-pin warning keeps its categories. | Compatible (additive) |
| 9 | **New storage rows** under the reserved grain types listed above, and the `TreeRangeDeleteFenceGrain` state, appear in the host's grain storage, and a sweep removes orphaned ones. The fence grain's reminder adds one reminder-table row per tree while it has an open intent. They are documented in the tree's storage notes. | Compatible (additive) |
| 10 | **New public configuration.** A `LatticeOptions` switch to turn the live advance off is additive, and defaults to on once the gate opens. | Compatible (additive) |
| 11 | **The first write after a leaf activation is slower.** With the gate open, it waits for the activation's claim: a leaf-row read and write, then per partition a grain call with an entry read and write, about four sequential storage round trips. Later writes are unaffected. A cold leaf's first-write latency rises; no result, exception or record changes. | Compatible (latency) |

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
  raise), and `ClaimBump` (any refusal while the activation's ETag holds). The ETag is
  the pair (`rowInc`, `rowEpoch`) (section 1, "The ETag in the model"). `fence` is the
  durable entry and `memFence` the in-memory one.
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
- `OnlySupersededRefused`: only a superseded activation is ever refused, at a claim or at an
  append. Superseded means its ETag no longer holds, a higher epoch was raised, or it
  belongs to a retired incarnation of L. In particular, another leaf's retirement never
  refuses a live leaf (#5635), and the straddle never stops a re-created incarnation. The
  append half needs the witness-gated `StaleAppend`, so `WalLiveAdvanceOtherRetire.cfg` runs
  with `Ghosts` on.
- `OldNeverRejected`: no unstamped append is ever rejected.
- `ContestedKeyAgrees`: the owner resolves a contested key to the value a WAL reader
  (a replica or a view) resolves at the same read position, last-writer-wins by version.
  Every entry the leaf owns writes one key, the contention worst case; every write has a
  real version, ordered by time. The bulk entry is versioned when its chunk is appended,
  in arbitrary order against concurrent writes; the model places it between the writes
  allocated before the bulk began and those after, and every placement is clean.

**Checked locally** (tla2tools v1.7.4, `-workers 2`, safety only, all six properties
plus `TypeOK`):

| Configuration | Switches | Distinct states | Depth | Time |
|---------------|----------|-----------------|-------|------|
| `WalLiveAdvance.cfg` (base) | batch, two windows | 4,822,222 | 35 | 118 s |
| `WalLiveAdvanceRecreate.cfg` | `WithRecreate`, `Ghosts` on | 6,748,568 | 36 | 479 s |
| `WalLiveAdvanceRestart.cfg` | `WithRestart` | 7,803,467 | 35 | 180 s |
| `WalLiveAdvanceFrozenView.cfg` | `FrozenView` | 1,354,237 | 33 | 33 s |
| `WalLiveAdvanceMixed.cfg` | `WithMixedVersion` | 2,525,889 | 33 | 57 s |
| `WalLiveAdvanceMixedRestart.cfg` | `WithMixedVersion`, `WithRestart` | 13,948,705 | 35 | 344 s |
| `WalLiveAdvanceGateFlip.cfg` | `WithGateFlip` | 7,609,129 | 33 | 170 s |
| `WalLiveAdvanceLegacyHandOff.cfg` | `WithLegacyHandOff` | 20,828,314 | 37 | 499 s |
| `WalLiveAdvanceTreeWalk.cfg` | `WithTreeWalk` | 22,966,529 (the reviewer's run with `OtherIntentClose`; not re-run locally) | - | - |
| `WalLiveAdvanceTreeWalkConcurrent.cfg` | `WithTreeWalk`, two windows | larger than revision 6's 23,951,016 with `OtherIntentClose`; for the reviewer or CI | - | - |
| `WalLiveAdvanceOtherRetire.cfg` | `WithOtherRetire`, `Ghosts` on | 13,817,702 | 36 | 402 s |
| `WalLiveAdvanceTwoPartition.cfg` | `WithSecondPartition` | 3,039,953 | 34 | 93 s |

All twelve hold every property, `ContestedKeyAgrees` and `OnlySupersededRefused` included.
Revisions 6 to 8 changed only what a purge, a raised floor or a tree walk reaches:
`rowInc` and the row floor move only on `Recreate`; `AlreadyDurable` admits an equal claim,
which only a purge's recurring epoch value produces; the widened `ClaimBump` needs a purge
or a raised floor; `OtherIntentClose` needs `WithTreeWalk`; and `OnlySupersededRefused` can be
set only by `ClaimRefuse` and `ClaimConflict`, which the bump and the row compare pre-empt,
or by the witness-gated `StaleAppend`. So base and Recreate were re-run for revision 7
(Recreate, with `Ghosts` on, was clean at 4,859,576; without them at 3,322,487, the
reviewer's #5641 count), OtherRetire and TwoPartition at revision 6, and the other
configurations are unchanged by construction. The two TreeWalk configurations grow with
`OtherIntentClose` beyond the small instances run locally; the reviewer's run of TreeWalk
with it is clean at 22,966,529. `WalLiveAdvanceOtherRetire.cfg` runs without `Recreate` and
with `Ghosts` on (revision 5's Recreate-plus-OtherRetire combination, 46,444,519 states, is
a promotion-member CI configuration, with the other combinations). Mutation trace lengths are the shortest a two-worker run found and can
vary by one state between runs. `MixedRestart` is the reviewer's Mixed+Restart+duplicate
combination (S8) on the reduced instance. #5668 changed `Recreate` (the routing floor,
the retired generation entry, the deleted marker) and gave `MarkInit` a branch that
fires only after it; Recreate was re-run, clean at 6,748,568, and no other configuration
enables `Recreate`, so they are unchanged by construction.

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
| `ReadPositionHonestCloseHeadUnderstated` | The heads at every partition the initiator wrote: the close records `-1` at a partition whose claim a WAL shard restart reset (#5664) | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestDrainWithoutFence` | The close's fence; waiting for returns is equivalent | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestDrainPointOnly` | The stamp on batch appends, so the fence covers point writes only | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestStampAtAppend` | The stamp at admission; the append carries the current epoch | `ReadPositionHonest` | base | 13 |
| `ReadPositionHonestBulkWithoutHint` | The birth hint | `ReadPositionHonest` | base | 6 |
| `ReadPositionHonestBulkAppendDropsExistingRows` | The relocation of the rows the bulk range already holds (#5610) | `ReadPositionHonest` | base | 4 |
| `ReadPositionHonestRecreateDropsFloor` | The retired floor's raise | `ReadPositionHonest` | Recreate | 17 |
| `StaleEpochNeverAppendsIgnoresRetiredFloor` | The floor check at the claim's raise | `StaleEpochNeverAppends` | Recreate | 8 |
| `OnlySupersededRefusedFloorAtAppend` | The entry-only admission predicate: the floor is checked in it too (revision 4's rule, #5635) | `OnlySupersededRefused` | OtherRetire, Ghosts | 9 |
| `OnlySupersededRefusedFloorInAppendGuards` | The entry-only append guards: the floor is checked beside the predicate, which a definitional property would not see | `OnlySupersededRefused` | OtherRetire, Ghosts | 8 |
| `OnlySupersededRefusedConflictWithoutMove` | The conflict check: an activation stops at its claim although its row has not moved | `OnlySupersededRefused` | base | 4 |
| `ReadPositionHonestFloorFromEntryOnly` | The epoch row in the retirement's floor (#5641): the floor is raised from the entry alone, so a pre-purge duplicate and the re-created incarnation hold the same epoch | `ReadPositionHonest` | Recreate | 17 |
| `ReadPositionHonestClaimAdoptsNoCas` | The epoch row's conditional write (#5593's adopt): two activations write the same epoch, and the second's entry write is admitted as `AlreadyDurable` | `ReadPositionHonest` | base | 16 |
| `ReadPositionHonestRoutingRecreatedAtOne` | The routing floor in the first generation (#5668): the re-created incarnation starts at 1, and a straddling shard root's pre-purge write carries the generation it claims | `ReadPositionHonest` | Recreate | 15 |
| `ReadPositionHonestRoutingFloorAfterDeletes` | The floor written before the deletes (#5668): with a crash between, the re-drive reads deleted rows and never raises it | `ReadPositionHonest` | Recreate | 15 |
| `ReadPositionHonestRoutingInitWithoutClaim` | The fence at the re-created incarnation's first initialisation (#5668): the seeded generation is not claimed, and the absent entry admits the straddling write above the recorded heads | `ReadPositionHonest` | Recreate | 15 |
| `ReadPositionHonestRoutingPurgeKeepsMarkerInit` | Rule 2 at the purge (#5668): the deleted marker still reads initialised, releasing a foreign write the open marker held | `ReadPositionHonest` | Recreate | 13 |
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
| `ReadPositionHonestRestampUnderAnyIntent` | Rule 3: a rejected walk re-stamps under any open intent, not only its own | `ReadPositionHonest` | TreeWalk | 15 |
| `ContestedKeyAgreesStrandedValueKept` | The relocation's last-writer-wins on the owner: it keeps a stranded or skipped value over a later-stamped write made before the switch (10.2.0's stranded origin read) | `ContestedKeyAgrees` | base | 7 |

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

Four guards are kept although the model, with every other guard in place, does not
need them. Each was removed alone and the model stayed clean, so each is classed
redundant in the model (as for #2321), with the reason it is kept:

| Check | Removes | Configuration | Result | Why it is kept |
|-------|---------|---------------|--------|----------------|
| `ReadPositionHonestArmsWithoutClaim` | The explicit claim guard in `Armed` | GateFlip | clean, 171 s | Implied by the self-report: an activation that never claimed has only unstamped own appends, each of which reports itself, so it cannot arm past them. Kept, as the review asked, because arming without a claim is meaningless and the implication depends on every unstamped path self-reporting. |
| `ReadPositionHonestNoMarkerInit` | The "marker initialised" guard in `Armed` (B2 rule 2) | LegacyHandOff | clean, 493 s | Implied by rule 1 in the model, whose only legacy hand-off cannot admit a stamped write before `Resume` opens the marker. Kept because a resume path the census misses would otherwise read as closed. The model's `MarkInit` is stronger than production's rule: it waits until no hand-off is in flight anywhere, while production initialises an existing leaf once its shard's flag is set and it has no split in flight **as donor**, which is weaker for a split **sibling**. That is safe because rule 1 alone suffices, which is exactly what this clean result shows (review N3). |
| `OnlySupersededRefusedEtagByValue` | The incarnation in the row's ETag stand-in: the row is compared by epoch value | Recreate | clean against `OnlySupersededRefused`, `ReadPositionHonest` and `StaleEpochNeverAppends` | In this one-leaf instance no property separates a recurring epoch value from a new write. Kept because production compares the ETag, which a recurring value never matches, and the model should not let a pre-purge activation's compare succeed where production's fails (revision 5 review, S1). |
| `OnlySupersededRefusedEntryRefusalStops` | The bump on an entry refusal while the row holds (only a floor-only refusal bumps) | Recreate | clean | Since #5641 the floor includes the epoch row, so a pre-purge duplicate's straddling entry write is refused and no entry is left above a held row. Kept because it is the general recovery for such an entry however it arises, and it is production's rule (#5633). |

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
| `WitnessClaimAboveRetiredFloor` | `WithRecreate` | `Begin`, `Create`, `ClaimRow`, `Recreate`, `Duplicate`, `ClaimRow(a2)` (the floor now includes the retired row's epoch) |
| `WitnessUnstampedAppendReported` | `WithMixedVersion` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OldAlloc`, `OwnAlloc(a1)` |
| `WitnessOwnUnstampedReportsItself` | `WithGateFlip` | `Begin`, `Create`, `OwnAllocUnstamped(a1)` |
| `WitnessLegacyHandOffResumed` | `WithLegacyHandOff` | `Begin`, `Create`, `OpenGate`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `Resume`, `Close`, `OwnAlloc`, `Land`, `MarkInit`, `LiveAdvance(a1)` |
| `WitnessContestedKeyResolvedToPoint` | - | `Begin`, `Create`, `ForeignAdmit`, `ForeignAlloc`, `Land`, `Replay`: the owner resolves the contested key to a write stamped later than the bulk entry, over the bulk value |
| `WitnessTreeWalkAllocated` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `GAlloc`: another shard's walk is allocated under L's open marker |
| `WitnessTreeWalkFenced` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`: L's close re-claims the tree generation and fences another shard's admitted walk |
| `WitnessTreeWalkRestamped` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`, `ForeignOpen`, `GReject`, `GRestamp`: a walk fenced out by one delete's close re-stamps under a second delete's open intent |
| `WitnessTreeWalkUnstamped` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `Close`, `GReject`, `GUnstamped`: a walk whose intent closed goes on unstamped and reported |
| `WitnessTreeWalkRestampedOwnIntent` | `WithTreeWalk` | `Begin`, `Create`, `GAdmit`, `OtherIntentClose`, `GReject`, `GRestamp`: rule 3's own case, a walk re-stamped under its still-open own intent |
| `WitnessAppendAtOrBelowOtherFloor` | `WithOtherRetire` | `Begin`, `Create`, `ClaimRow`, `ClaimWrite`, `ClaimRaise`, `OwnAlloc`, `RetireOther`: another leaf's retirement lifts the floor to an epoch a live activation holds, and it keeps appending |
| `WitnessClaimBumped` | `WithOtherRetire` | `Begin`, `Create`, `ClaimRow`, `RetireOther`, `ClaimBump`: a floor-only refusal bumps and re-claims |
| `WitnessRecreatedStraddleFenced` | `WithRecreate` | `Begin`, `Create`, `ForeignAdmit`, `Recreate`, `MarkInit`: a straddling write of the purged incarnation, still pending, is below the generation the re-created incarnation's initialisation claimed (#5668) |
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
| `TreeLifecycle`, `TreeLifecycleAdmission` | New rows | The purge tail retires the entries of the leaves it clears, under the lineage check (#5417), raising the floor first, and deletes their epoch and marker rows. The tree's purge also retires its per-tree fence rows after max-merging their highest generation into the tree's routing floor (#5668). |
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
   - `WalAppendFenceOutcome`, the supersession as `LeafRetiredException` with its two
     trailing members, and the seam retry of section 5 for every caller in its table, each with a detector that the caller's 10.2.0 outcome is
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
   - Arms nothing before #5664 lands: the close heads must bound every record the
     initiator appends (section 2, "What the close heads must bound").
   - It reuses #5578's applied-prefix tracker and the core extraction from the
     abandoned #5596.
   - Formal work: production only. `WalDurability` stays unchanged (Formal impact) and
     gains a gap entry naming `WalLiveAdvance` as the live advance's safety argument and
     citing member 4's detectors; `PinRetirementRefinement.md` gains the entry-retirement
     rows. #5578's `LiveAdvance` and `NewbornPinEventuallyUsable` are not re-landed in
     `WalDurability`. The PR maps each production arming guard to a conjunct of
     `Armed(a)`, so the promoted module's refinement note can cite the detectors.
5. **Epoch-row sweep (#5634).** The per-partition epoch directory row, bucketed or paged
   within the provider's row limits, and the sweep that retires the entries of leaves
   that no longer exist, which a 10.2.0 silo's retirement leaves behind. It lands
   before #5605's bucket reaches `main`, independently of members 2 to 4. With it,
   #5668: the purge's retirement of the per-tree fence rows behind the routing floor,
   and the floor-seeded, fencing first initialisation (section 2, "Purging a tree's
   fence rows").
6. **Promotion.** `WalLiveAdvance` becomes a module with its refinement note, its
   manifest, its redundancy rows (with a redundancy class in the mutation catalogue, or
   kept out of the catalogue gate's glob, so a clean run is not reported as a missing
   violation), and **every combined configuration the reviewer named**
   (Mixed+Restart+duplicate on the full instance, own unstamped appends with the
   self-report, repeated unstamped appends, the split claim under `Restart`, and the
   twelve configurations above) **in CI, on a dedicated or measured TLC shard.** It also
   adds the liveness property #5572 asked for and the draft model lacks: under weak
   fairness of the claim, replay, apply and `LiveAdvance` steps, an armed activation's
   newborn pin eventually becomes usable without a replay from `-1` (the successor of
   #5578's `NewbornPinEventuallyUsable`), checked on a small configuration. Nothing
   from phase 2 reaches `main` before this member lands.

## Review resolution

### After acceptance: #5704's review S2 (#5709), and member 3's #5702 and #5706

| Finding | Resolution |
|---------|------------|
| #5709: a reachable leaf's live advance can pass an overlapping orphan's appends | Section 4, "An unreachable leaf never stamps": orphan repair never folds rows, so the rule is at the source. A leaf proves reachability by descent before its first claim; an unreachable one appends unstamped (reported), so every leaf sharing the partition replays past its records. A sibling born under an open marker keeps its eager claim, and a tick re-check stops stamping. The model's counterexample is `ReadPositionHonestUnstampedNotReported`. The descent check is named as the orphan signal, subsuming "scan chain flagged". |
| #5702: a run shares a hint; deferred, batched closes | Section 3: a chunk's hint may be any head read before its first leaf is born, and a run reads heads only at its start. Completed append tokens close in batches by one fence and one marker write. Hand-off table updated. |
| #5706: a resumed division cannot drain in-flight commits | Section 2: a resumed division applies no read position; the sibling replays from its own start. |

### After acceptance: per-tree fence rows at purge (#5668)

| Finding | Resolution |
|---------|------------|
| #5668: the per-tree fence rows are never deleted | Section 2, "Purging a tree's fence rows": the floor is max-merged into `_lattice_tree-routing-floor` before any delete, then the rows are retired, and a re-created incarnation starts at the floor plus one. Modelling it surfaced two further rules: the deleted marker reads uninitialised, and the first initialisation after a purge claims its seeded generation at every partition before reading initialised, because an absent generation entry admits any stamp. `Recreate` and `MarkInit` model all of it, with four mutations (`RoutingRecreatedAtOne`, `RoutingFloorAfterDeletes`, `RoutingInitWithoutClaim`, `RoutingPurgeKeepsMarkerInit`) and the witness `WitnessRecreatedStraddleFenced`; Recreate is clean at 6,748,568. |

### After acceptance: member 2's review (#5644), S2

| Finding | Resolution |
|---------|------------|
| S2 close heads bound only appends stamped before the bump | Section 2, "What the close heads must bound": three guards, each load-bearing in the model. Stamp at admission (`StampAtAppend`), re-claim every partition claimed in the activation (new `CloseHeadUnderstated`), and no unfenced close while a stamped append may be in flight (`DrainWithoutFence`). The span re-check and receiver-own transfer argument is stated, and why it fails when the initiator's forward never happens. Production departs on all three at #5644's head, so the fix is filed as #5664, a sub-issue of #5605, which blocks member 4. |
| #5664 (a) mechanism | Section 2, "Stamped at admission": member 2 carries an admission mark (close count plus one) and refuses at dispatch any write whose mark predates a close. Equivalent for `StampAtAppend`, with fewer spurious retries. Mark `0` is reserved for uncaptured paths, so the fence is resolved at admission, not lazily at the first append, which would leave a write admitted before the activation's first append unmarked (found by member 2, fixed in #5644). The tx-terminal backstop awaited its forwards between its span check and its local appends, so a moved local key could append under the raised epoch; it now captures its mark before the span check (census row). |

### After acceptance: member 3's review (#5646)

| Finding | Resolution |
|---------|------------|
| B2 the re-delivery rule drops a refresh | Section 3, "Re-delivery": a run is skipped only when every key is held with an identical value and metadata; any other run is relocated and merged last-writer-wins. With it, change 4's "only the origin's pre-replay reads change" holds: a refresh reaches replicas and views with its later stamp, as in 10.2.0. No state or wire change. The model has no re-delivery step, so the rule is pinned by the bulk member's widened detector, not a mutation. |
| #5654 narrow then link fails open | Section 3, "Narrow and link in one persist": the old leaf's bound and its `NextSibling` are persisted together; `SetNextSiblingAsync` stays as an idempotent re-assertion. |

### After acceptance: member 1's review (S-a)

| Finding | Resolution |
|---------|------------|
| S-a the note names a removed exception | Sections 1 and 5, the wire-format bullets and member 1's list now describe production: the shard reports `WalAppendFenceOutcome`, a supersession is the existing `LeafRetiredException` with two trailing members (alias unchanged), the one additive change to an existing serialized type, and a stamped batch is rejected whole at its first failing entry. |

### Re-review of revision 7

| Finding | Resolution |
|---------|------------|
| S1 `ClaimAdoptsNoCas` red | Moved to `mutations/` (16 states); step 2 states that `AlreadyDurable` rests on the row CAS (one holder per epoch within an incarnation) and on the row floor (no recurrence across incarnations). The redundancy table drops the row. |
| N1 clear ordering | "The purge straddle" and "Why the row, not only the entry" cover a duplicate's bump landing before the clear's conditional delete: the delete conflicts and the row is kept, so no value recurs. A three-step `Recreate` is a promotion-member item. |
| N2 #5633 head | Production, for member 1 (red-proof commit to revert). |
| TreeWalk count | 22,966,529, the count for this model's `OtherIntentClose`. |

### #5641 and the reviewer's partial findings on revision 6

| Finding | Resolution |
|---------|------------|
| #5641 `AlreadyDurable` with recurring epochs | Section 1, "Retiring entries" and "Why the row, not only the entry": retirement raises the floor to the higher of the entry and the epoch row, which the clear passes in. `ClaimWrite` and `EntryRefuses` admit `AlreadyDurable` as production does; `Recreate` raises the floor to `Max(fence, rowEpoch)`. Mutation `FloorFromEntryOnly` (17 states). Step 2 states `AlreadyDurable`; section 4 states that a 10.2.0 retirement keeps the row. |
| Rule 3's own case unreachable | `OtherIntentClose` (`WithTreeWalk`); `WitnessTreeWalkRestampedOwnIntent` (7 states). The note and the `GRestamp` comment now call `hoOpen` a safe over-approximation that includes re-stamps under a newer intent covering L. |
| `ClaimConflict` not flagged | `ClaimConflict` sets `wrongRefusal` when the row is unmoved; mutation `ConflictWithoutMove` (4 states). |
| Recreate with `Ghosts` | The Recreate configuration runs with `Ghosts` on: clean at 4,859,576. |
| Mutation count | Corrected: 33 mutations, 5 redundancy checks (revision 8: 34 and 4, `ClaimAdoptsNoCas` moved). `EntryRefusalStops` is now a redundancy row, because #5641 closes the straddle. |
| Note-table row (#5633 review) | Behaviour change 11: a cold leaf's first write waits for the claim; and the cost section counts retirement and a tree purge's N x P retirement calls. |

### Re-review of revision 5

| Finding | Resolution |
|---------|------------|
| B1 re-stamp under any open intent | Section 2, rule 3: a rejected walk re-stamps only while its own intent, named by id, is open; otherwise it appends unstamped. The model's `hoOpen` guard encodes that exactly (comment corrected). Mutation `RestampUnderAnyIntent` (15 states). |
| S1 refusal rule and the row compare | Section 1, "Any refusal bumps while the row holds": any refusal bumps while the activation's ETag holds, as production does; the purge straddle is named in "Retiring entries". `ClaimBump` widened to `Refused(a) /\ RowHolds(a)`, bumping to the higher of entry, floor and row, plus one. The ETag stand-in is (`rowInc`, epoch), `rowInc` raised by `Recreate`. Recreate clean at 5,641,927. Revision 5's rule is mutation `EntryRefusalStops` (9 states). The value compare is a redundancy row: no property separates it in this instance. |
| S2 `CurrentNeverFenced` definitional | Replaced by the behavioural `OnlySupersededRefused`; `FloorReject` removed. `WalLiveAdvanceOtherRetire.cfg` runs with `Ghosts` on so the witness-gated `StaleAppend` runs: clean at 13,817,702. `FloorAtAppend` (9 states) and the new `FloorInAppendGuards` (8 states) both fail. |
| N1 step 1 prose | Step 1 describes `ClaimRowAsync`: row plus one; the floor is learnt through a refusal and a bump. |
| N2 re-claim after a bump | Section 1, "One epoch per activation": production re-claims each partition under the new epoch before stamping it, and the partition is disarmed until it replays to the new head; `OtherPartitionBump` is a safe over-approximation. The cost is in N3. |

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
| #5635 (from member 1), and the reviewer's [interim note](https://github.com/NSTA1/Orleans.Lattice/issues/5605#issuecomment-6566514445) | Section 1: the floor is checked at the claim's entry write and raise only, `AppendAdmits` is the entry check. (1) The condition that makes that sound, retirement removing the WAL shard's loaded entry under `_stateGate` after the batch drain, is stated, with mutation `RecreateKeepsLoadedEntry` (9 states). (2) `RetireOther` and `CurrentNeverFenced`, broken by `CurrentNeverFencedFloorAtAppend` (8 states, #5635 itself). (3) `StaleEpochNeverAppends` is narrowed to L's own retirements (`lRetired`). (4) A floor-only refusal bumps and re-claims (`ClaimBump`, `WitnessClaimBumped`), up to `MaxClaimRounds` then unstamped. (5) One epoch across partitions: `OtherPartitionBump` (`WithSecondPartition`), clean at 3,039,953. `OtherRetire` is clean at 46,444,519. The two floor redundancy rows are retired: the raise check is now required (`IgnoresRetiredFloor`). Revision 6 replaces `CurrentNeverFenced` with `OnlySupersededRefused` and widens `ClaimBump` (re-review of revision 5, S1, S2). |
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
| Change 4 (coordinator decision after revision 3) | Last-writer-wins by HLC stamp, the later-stamped write winning: what 10.2.0 converges to on every reader (section 3, with file and line; revision 8 corrects revision 4's reading that bulk entries were versioned below every real write, found by member 3). A compatible bug fix; no authorisation. Property `ContestedKeyAgrees`, mutation `StrandedValueKept`, witness `ContestedKeyResolvedToPoint`. A stable "bulk value wins" is rejected, with reasons. |

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
| S5 #5610 who wins | Last-writer-wins by HLC stamp, the later-stamped write winning, as 10.2.0 converges on every reader (section 3, with file and line; revisions 4 to 7 read the bulk stamp wrongly as below every real write, corrected in revision 8). A compatible bug fix that removes an origin-versus-replica divergence; no authorisation. |
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
| S2 CAS | A strict conditional write on a row only claims write. A redundancy row in revisions 4 to 6; load-bearing again since revision 7's `AlreadyDurable` (mutation `ClaimAdoptsNoCas`, 16 states). |
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
