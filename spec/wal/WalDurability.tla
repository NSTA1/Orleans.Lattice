--------------------------- MODULE WalDurability ---------------------------
(***************************************************************************)
(* An abstract TLA+ specification of the Orleans.Lattice write-ahead-log    *)
(* durability lifecycle at the leaf, end to end and with crash-anywhere     *)
(* recovery.                                                               *)
(*                                                                         *)
(* One WAL partition is a stream shared by every leaf of a tree. Writes    *)
(* are appended at dense offsets, flushed (possibly out of order) and       *)
(* acknowledged once durable. Each leaf reads the stream through a per-leaf *)
(* READ position (its checkpoint), folds the entries it owns into an        *)
(* in-memory projection, persists the read position (a write that can      *)
(* fail), captures durable snapshots of its projection (which can fail),   *)
(* and publishes a durable materialiser pin. The WAL garbage collector     *)
(* trims the stream's prefix up to the minimum pin. The WAL shard and a   *)
(* leaf crash at any step between actions; a leaf recovers from its        *)
(* snapshot, or cold from the readable WAL, and either rebuilds every      *)
(* acknowledged write it owns or fails closed.                             *)
(*                                                                         *)
(* Shard moves are specified separately, in WalMove.tla: a move touches    *)
(* the stream and its allocator but none of this module's leaf state, and *)
(* keeping it out keeps this module small enough to check its liveness     *)
(* properties exhaustively in CI.                                          *)
(*                                                                         *)
(* This models the protocol DESIGN, not the code. Values, keys, HLCs,       *)
(* partitions beyond one, retention TTLs and replication are abstracted     *)
(* away: a write is identified by its offset and a projection is the set   *)
(* of offsets it holds. See Refinement.md for the mapping from every        *)
(* variable, action and property to production and to the tests that       *)
(* detect a regression, and for what this model does NOT cover.             *)
(*                                                                         *)
(* Epic #4430, issue #4432.                                                *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, TLC

(***************************************************************************)
(* Model instance. Two leaves share one partition of three offsets. Every  *)
(* write belongs to l1; l2 owns nothing in the partition - the common case *)
(* behind issue #3453, since keys hash across every partition - so l2 must *)
(* advance its read position over entries it never applies, which is why  *)
(* a checkpoint is a read position and not an applied high-water mark.     *)
(* MaxFaults bounds the environment's faults (a shard crash, a leaf stop, a *)
(* failed persist or capture): the fairness ceiling that lets the liveness *)
(* properties be checked. One is enough to reach every defect paired below.*)
(***************************************************************************)
CONSTANTS l1, l2

Leaves == {l1, l2}
MaxOff == 3
Offs == 0..(MaxOff - 1)
Owner == (0 :> l1) @@ (1 :> l1) @@ (2 :> l1)
MaxFaults == 1

\* The "no snapshot" sentinel for snapshot coverage; -1 is a real coverage
\* claim ("a snapshot exists and covers no offset").
NoSnap == -2

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

(***************************************************************************)
(* State.                                                                  *)
(*                                                                         *)
(* WAL shard (one partition):                                              *)
(*  next      the next offset the allocator hands out.                     *)
(*  inflight  offsets assigned to an append whose flush has not completed. *)
(*  durable   offsets the WAL store holds (trim removes them).             *)
(*  tail      the oldest readable offset; every offset below it is gone.   *)
(*  acked     offsets acknowledged to their writer (only once durable).    *)
(*                                                                         *)
(* Leaf l:                                                                 *)
(*  up[l]     an activation exists and has finished activating.            *)
(*  cache[l]  the in-memory projection: offsets of owned writes it holds.  *)
(*  rp[l]     the in-memory read position (persisted or pending), -1 none. *)
(*  stCp[l]   the activation's belief about its persisted checkpoint       *)
(*            (state.State).                                               *)
(*  durCp[l]  the checkpoint grain storage actually holds.                 *)
(*  anchor[l] the checkpoint this activation started from: the snapshot's   *)
(*            coverage it rehydrated, or the stored checkpoint on a cold   *)
(*            start. The activation may hold this belief before persisting.*)
(*  clk[l]    whether the leaf's persisted clock is past Zero (it has      *)
(*            durably recorded applying a write).                          *)
(*  cov[l]    the snapshot coverage this activation has recorded from a    *)
(*            kept capture or a successful load; NoSnap when none.         *)
(*  snapCov[l], snapRows[l]  the durable snapshot: its coverage claim and  *)
(*            the rows it holds; snapCov = NoSnap when none exists.        *)
(*  stale[l]  the leaf has latched LeafProjectionStaleException (fail      *)
(*            closed: operator rebuild required). Absorbing.               *)
(*                                                                         *)
(* Durable pin store (per leaf, merged by monotone max):                   *)
(*  pinOff[l] the highest offset the leaf has ever published, -1 none.     *)
(*  pinHlc[l] "zero" while only Zero-frontier pins were published (a       *)
(*            block pin), "clock" once a real frontier was.                *)
(*                                                                         *)
(*  faults    environment faults spent so far.                             *)
(***************************************************************************)
VARIABLES next, inflight, durable, tail, acked,
          up, cache, rp, stCp, durCp, anchor, clk, cov, snapCov, snapRows, stale,
          pinOff, pinHlc, faults

walVars  == <<next, inflight, durable, tail, acked>>
leafVars == <<up, cache, rp, stCp, durCp, anchor, clk, cov, snapCov, snapRows, stale>>
pinVars  == <<pinOff, pinHlc>>
vars == <<next, inflight, durable, tail, acked,
          up, cache, rp, stCp, durCp, anchor, clk, cov, snapCov, snapRows, stale,
          pinOff, pinHlc, faults>>

Pos == -1..(MaxOff - 1)

TypeOK ==
   /\ next \in 0..MaxOff
   /\ inflight \subseteq Offs
   /\ durable \subseteq Offs
   /\ tail \in 0..MaxOff
   /\ acked \subseteq Offs
   /\ up \in [Leaves -> BOOLEAN]
   /\ cache \in [Leaves -> SUBSET Offs]
   /\ rp \in [Leaves -> Pos]
   /\ stCp \in [Leaves -> Pos]
   /\ durCp \in [Leaves -> Pos]
   /\ anchor \in [Leaves -> Pos]
   /\ clk \in [Leaves -> BOOLEAN]
   /\ cov \in [Leaves -> Pos \cup {NoSnap}]
   /\ snapCov \in [Leaves -> Pos \cup {NoSnap}]
   /\ snapRows \in [Leaves -> SUBSET Offs]
   /\ stale \in [Leaves -> BOOLEAN]
   /\ pinOff \in [Leaves -> Pos]
   /\ pinHlc \in [Leaves -> {"zero", "clock"}]
   /\ faults \in 0..MaxFaults

(***************************************************************************)
(* Derived views.                                                          *)
(*                                                                         *)
(* Readable: what a reader of the stream can still get. Watermark: the     *)
(* durable-contiguous tail a cursor-advancing reader may be shown - the     *)
(* oldest in-flight offset, or the next offset when nothing is in flight   *)
(* (WalShippingWatermark). Owned(l): the acknowledged writes l owns. Cur:  *)
(* the checkpoint the leaf reports as current, the higher of its persisted *)
(* belief and its pending read position (GetCurrentCheckpointForPartition) *)
(* - a cold activation's read position restarts below the persisted slot,  *)
(* which it leaves untouched, so the two differ for a whole cold replay.   *)
(***************************************************************************)
Readable == {o \in durable : o >= tail}

Watermark == IF inflight = {} THEN next ELSE Min(inflight)

Owned(l) == {o \in acked : Owner[o] = l}

Cur(l) == IF rp[l] > stCp[l] THEN rp[l] ELSE stCp[l]

\* What a recovery of l from durable state alone rebuilds: the snapshot's
\* rows, then the readable WAL past the snapshot's coverage (or the whole
\* readable WAL when there is no snapshot).
Recoverable(l, o) ==
    IF snapCov[l] # NoSnap
    THEN o \in snapRows[l] \/ (o > snapCov[l] /\ o \in Readable)
    ELSE o \in Readable

\* Merge a published pin into the durable pin store (WalMaterialiserPinGrain:
\* offset and frontier both by monotone max, so nothing published can be
\* taken back).
MergePin(l, hlc, off) ==
    /\ pinOff' = [pinOff EXCEPT ![l] = IF off > @ THEN off ELSE @]
    /\ pinHlc' = [pinHlc EXCEPT ![l] = IF hlc = "clock" THEN "clock" ELSE @]

\* The GC's view of the pin store. A pin that has never left Zero with no
\* offset is a block pin and stops every trim; a real frontier with no
\* offset abstains from the offset floor; any offset joins the floor.
Blocking(l) == pinOff[l] < 0 /\ pinHlc[l] = "zero"
Floor == Min({pinOff[l] : l \in {k \in Leaves : pinOff[k] >= 0}} \cup {MaxOff})

Init ==
    /\ next = 0
    /\ inflight = {}
    /\ durable = {}
    /\ tail = 0
    /\ acked = {}
    /\ up = [l \in Leaves |-> TRUE]
    /\ cache = [l \in Leaves |-> {}]
    /\ rp = [l \in Leaves |-> -1]
    /\ stCp = [l \in Leaves |-> -1]
    /\ durCp = [l \in Leaves |-> -1]
    /\ anchor = [l \in Leaves |-> -1]
    /\ clk = [l \in Leaves |-> FALSE]
    /\ cov = [l \in Leaves |-> NoSnap]
    /\ snapCov = [l \in Leaves |-> NoSnap]
    /\ snapRows = [l \in Leaves |-> {}]
    /\ stale = [l \in Leaves |-> FALSE]
    \* Every leaf is born holding a durably seeded Zero block pin
    \* (SeedDurableMaterialiserBlockPinAsync), before any write reaches it.
    /\ pinOff = [l \in Leaves |-> -1]
    /\ pinHlc = [l \in Leaves |-> "zero"]
    /\ faults = 0

(***************************************************************************)
(* Append: the allocator hands the next offset to a new write              *)
(* (WalOffsetAllocationCore.Assign). The write is in flight until its      *)
(* flush completes.                                                        *)
(***************************************************************************)
Append ==
    /\ next < MaxOff
    /\ inflight' = inflight \cup {next}
    /\ next' = next + 1
    /\ UNCHANGED <<durable, tail, acked>>
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

(***************************************************************************)
(* FlushAck(o): one in-flight append's flush completes - flushes complete  *)
(* in any order - and the write is acknowledged. The owning leaf handles   *)
(* its own write, so when it is active it folds the write into its         *)
(* projection (the foreground apply); that never advances its read         *)
(* position. When the owner is not active the write is acknowledged        *)
(* without the apply, which over-approximates production (a write reaches  *)
(* an active leaf) and leaves the owner to replay it.                      *)
(***************************************************************************)
FlushAck(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ acked' = acked \cup {o}
    /\ cache' = IF up[Owner[o]] /\ ~stale[Owner[o]]
                THEN [cache EXCEPT ![Owner[o]] = @ \cup {o}]
                ELSE cache
    /\ UNCHANGED <<next, tail>>
    /\ UNCHANGED <<up, rp, stCp, durCp, anchor, clk, cov, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

(***************************************************************************)
(* ShardCrash: the WAL shard's activation is lost at any step. Every       *)
(* unflushed append is lost (none was acknowledged) and the allocator      *)
(* recovers from what the store holds: one past the highest stored offset, *)
(* never below the tail.                                                   *)
(***************************************************************************)
RecoveredNext == Max({tail} \cup {o + 1 : o \in durable})

ShardCrash ==
    /\ faults < MaxFaults
    /\ inflight' = {}
    /\ next' = RecoveredNext
    /\ faults' = faults + 1
    /\ UNCHANGED <<durable, tail, acked>>
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars

(***************************************************************************)
(* ReadStep(l): an active leaf reads the next entry of the shared stream   *)
(* past its read position (activation replay and the starvation drive      *)
(* both). It is shown only offsets below the watermark. A read starting    *)
(* below the tail returns the surviving suffix, so the step lands on the   *)
(* tail. The entry is folded into the projection only if l owns it, but    *)
(* the read position advances over it either way: a checkpoint is a READ  *)
(* position in a shared stream (#2270, #2692).                             *)
(***************************************************************************)
ReadFrom(l) == IF rp[l] + 1 < tail THEN tail ELSE rp[l] + 1

ReadStep(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ ReadFrom(l) < Watermark
    /\ LET o == ReadFrom(l)
       IN /\ rp' = [rp EXCEPT ![l] = o]
          /\ cache' = IF o \in durable /\ Owner[o] = l
                      THEN [cache EXCEPT ![l] = @ \cup {o}]
                      ELSE cache
    /\ UNCHANGED walVars
    /\ UNCHANGED <<up, stCp, durCp, anchor, clk, cov, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

(***************************************************************************)
(* PersistCheckpoint(l) / PersistFail(l): the leaf persists its pending    *)
(* read position. On success storage and the activation's belief both move. *)
(* On failure the commit is rolled back: the activation's belief stays at  *)
(* the last durably written checkpoint and the advance stays pending       *)
(* (#4017).                                                                *)
(***************************************************************************)
PersistCheckpoint(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ rp[l] > stCp[l]
    /\ durCp' = [durCp EXCEPT ![l] = rp[l]]
    /\ stCp' = [stCp EXCEPT ![l] = rp[l]]
    /\ clk' = [clk EXCEPT ![l] = @ \/ cache[l] # {}]
    /\ UNCHANGED walVars
    /\ UNCHANGED <<up, cache, rp, anchor, cov, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

PersistFail(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ rp[l] > stCp[l]
    /\ faults < MaxFaults
    /\ stCp' = stCp
    /\ faults' = faults + 1
    /\ UNCHANGED walVars
    /\ UNCHANGED <<up, cache, rp, durCp, anchor, clk, cov, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars

(***************************************************************************)
(* Capture(l) / CaptureFail(l): the leaf captures a durable snapshot of    *)
(* its projection. Production proceeds once any partition is proven        *)
(* checkpointed (Cur >= 0) and declines a claim that covers nothing        *)
(* (#2725). The claim is the leaf's read position - what the projection     *)
(* actually holds - and the store keeps only a claim that does not regress *)
(* the coverage it already holds. Coverage is recorded in memory only from *)
(* a kept capture (#3440): a failed or declined capture records nothing.   *)
(* Production matches it since #4451 was fixed: an unanchored cache claims *)
(* only what its cold rebuild has re-read (BuildUnanchoredCoverage), and   *)
(* an anchored one its checkpoint.                                         *)
(***************************************************************************)
Capture(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ Cur(l) >= 0
    /\ rp[l] >= 0
    /\ rp[l] >= snapCov[l]
    /\ snapCov' = [snapCov EXCEPT ![l] = rp[l]]
    /\ snapRows' = [snapRows EXCEPT ![l] = cache[l]]
    /\ cov' = [cov EXCEPT ![l] = rp[l]]
    /\ UNCHANGED walVars
    /\ UNCHANGED <<up, cache, rp, stCp, durCp, anchor, clk, stale>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

CaptureFail(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ Cur(l) >= 0
    /\ faults < MaxFaults
    /\ cov' = cov
    /\ faults' = faults + 1
    /\ UNCHANGED walVars
    /\ UNCHANGED <<up, cache, rp, stCp, durCp, anchor, clk, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars

(***************************************************************************)
(* PublishPin(l): the leaf resolves and publishes its durable materialiser *)
(* pin (LeafDurablePinCore.Resolve), which the pin store merges by          *)
(* monotone max.                                                           *)
(*                                                                         *)
(* A leaf whose clock is past Zero: an empty leaf with no checkpoint        *)
(* releases its block; otherwise the pin may authorise trimming only up to *)
(* min(persisted checkpoint, covered) - the PERSISTED checkpoint, never    *)
(* the pending one (#3476) - and a pin with nothing durably recoverable is *)
(* the Zero block pin.                                                     *)
(*                                                                         *)
(* A leaf whose clock is still Zero has never applied a write. The cursor  *)
(* mirror publishes nothing for it, but the flush paths (the starvation    *)
(* drive and the deactivation barrier) release its block once it has       *)
(* scanned through a persisted checkpoint holding no row (#3453), so a     *)
(* leaf that owns nothing cannot hold its seeded block pin forever. The    *)
(* release is bounded by the snapshot coverage the leaf holds, and with no *)
(* snapshot it does not fire at all: the block stands until a capture -    *)
(* which a checkpointed never-written leaf always reaches - records        *)
(* coverage. A release with nothing durable behind it cannot be lowered,   *)
(* and a later cold-rebuild capture can land below it; the GC then trims   *)
(* past that snapshot and the leaf latches stale on its next activation    *)
(* (#4523). Bounded this way, every published offset is at or below the    *)
(* leaf's durable coverage, which only grows (ReleaseBackedBySnapshot).    *)
(* Production matches it since #4523 was fixed (LeafDurablePinCore).       *)
(***************************************************************************)
ClockLive(l) == clk[l] \/ cache[l] # {}

NeverWrittenRelease(l) ==
    IF cov[l] < 0 THEN -1 ELSE IF cov[l] < stCp[l] THEN cov[l] ELSE stCp[l]

PublishPin(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ ClockLive(l) \/ (cache[l] = {} /\ stCp[l] >= 0)
    /\ LET safe == IF stCp[l] < cov[l] THEN stCp[l] ELSE cov[l]
       IN IF ~ClockLive(l)
          THEN MergePin(l, "zero", NeverWrittenRelease(l))
          ELSE IF cache[l] = {} /\ Cur(l) < 0
               THEN MergePin(l, "clock", -1)
               ELSE IF safe < 0
                    THEN MergePin(l, "zero", -1)
                    ELSE MergePin(l, "clock", safe)
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED faults

(***************************************************************************)
(* GcTrim: the WAL GC trims the stream's prefix. No trim at all while any  *)
(* block pin stands; otherwise it may trim every offset at or below the     *)
(* minimum published offset (the durable offset floor). A leaf abstaining *)
(* from the floor does not constrain it, which over-approximates the trim *)
(* production performs (production additionally bounds it by in-memory     *)
(* consumer cursors).                                                      *)
(***************************************************************************)
GcTrim ==
    /\ \A l \in Leaves : ~Blocking(l)
    /\ \E t \in (tail + 1)..next :
         /\ t - 1 <= Floor
         /\ tail' = t
         /\ durable' = {o \in durable : o >= t}
    /\ UNCHANGED <<next, inflight, acked>>
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

(***************************************************************************)
(* LeafStop(l): the leaf's activation ends at any step - a crash, a silo   *)
(* restart or a deactivation. Everything in memory is lost: the projection, *)
(* the pending read position and the recorded coverage. Storage survives.   *)
(***************************************************************************)
LeafStop(l) ==
    /\ up[l]
    /\ faults < MaxFaults
    /\ up' = [up EXCEPT ![l] = FALSE]
    /\ cache' = [cache EXCEPT ![l] = {}]
    /\ rp' = [rp EXCEPT ![l] = -1]
    /\ stCp' = [stCp EXCEPT ![l] = durCp[l]]
    /\ cov' = [cov EXCEPT ![l] = NoSnap]
    /\ faults' = faults + 1
    /\ UNCHANGED walVars
    /\ UNCHANGED <<durCp, anchor, clk, snapCov, snapRows, stale>>
    /\ UNCHANGED pinVars

(***************************************************************************)
(* Activate(l): a stopped leaf activates. If it has a durable snapshot it   *)
(* rehydrates from it, records its coverage and resumes reading past the   *)
(* coverage; the fall-off detector latches the leaf stale when the WAL has *)
(* been trimmed past the first offset it still needs. Without a snapshot it *)
(* starts cold, reading the whole readable WAL, and the cold-replay guard  *)
(* latches it stale when the WAL has been trimmed past its durable          *)
(* checkpoint. Both trim tests are WalFallOffCore.IsPrefixLost.            *)
(***************************************************************************)
FallsOff(cp) == cp > 0 /\ tail > cp + 1

Activate(l) ==
    /\ ~up[l]
    /\ ~stale[l]
    /\ IF snapCov[l] # NoSnap
       THEN /\ cache' = [cache EXCEPT ![l] = snapRows[l]]
            /\ rp' = [rp EXCEPT ![l] = snapCov[l]]
            /\ stCp' = [stCp EXCEPT ![l] = snapCov[l]]
            /\ anchor' = [anchor EXCEPT ![l] = snapCov[l]]
            /\ cov' = [cov EXCEPT ![l] = snapCov[l]]
            /\ IF FallsOff(snapCov[l])
               THEN /\ stale' = [stale EXCEPT ![l] = TRUE]
                    /\ up' = up
               ELSE /\ up' = [up EXCEPT ![l] = TRUE]
                    /\ stale' = stale
       ELSE /\ cache' = [cache EXCEPT ![l] = {}]
            /\ rp' = [rp EXCEPT ![l] = -1]
            /\ stCp' = [stCp EXCEPT ![l] = durCp[l]]
            /\ anchor' = [anchor EXCEPT ![l] = durCp[l]]
            /\ cov' = cov
            /\ IF FallsOff(durCp[l])
               THEN /\ stale' = [stale EXCEPT ![l] = TRUE]
                    /\ up' = up
               ELSE /\ up' = [up EXCEPT ![l] = TRUE]
                    /\ stale' = stale
    /\ UNCHANGED walVars
    /\ UNCHANGED <<durCp, clk, snapCov, snapRows>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults

(***************************************************************************)
(* ActivateLoadFail(l): a leaf that has a durable snapshot fails to load it *)
(* (a storage fault, a cancelled load, an unreadable payload or segment).  *)
(* A failed load is not the same as no snapshot: the snapshot may be the   *)
(* only durable copy of a prefix the GC trimmed under its coverage, and    *)
(* the leaf's pin - resolved against that snapshot and never lowerable -   *)
(* keeps the GC entitled to trim that prefix for as long as a cold rebuild *)
(* would run. So the activation fails closed and is retried later, however *)
(* the WAL's tail reads at the moment of the failure: nothing changes, and *)
(* the step is a stutter. It costs no fault budget because it changes no   *)
(* state; Activate's weak fairness still forces the retry to succeed.      *)
(* Production matches it since #4450 was fixed (LeafReplayStartPolicy).    *)
(***************************************************************************)
ActivateLoadFail(l) ==
    /\ ~up[l]
    /\ ~stale[l]
    /\ snapCov[l] # NoSnap
    /\ UNCHANGED vars

(***************************************************************************)
(* ReplayFaultRearm(l): an active leaf's cold rebuild faults part-way (a   *)
(* read or apply failure inside the replay barrier) and the barrier is     *)
(* re-armed within the same activation, with the partly rebuilt projection *)
(* still in memory. The re-armed replay stays cold - it resumes from what  *)
(* it has actually re-read - so the step changes nothing and is a stutter. *)
(* Production matches it since #4467 was fixed: a pending cold rebuild     *)
(* keeps the cache unanchored, so LeafReplayStartPolicy retries it cold.   *)
(***************************************************************************)
ReplayFaultRearm(l) ==
    /\ up[l]
    /\ ~stale[l]
    /\ rp[l] < anchor[l]
    /\ UNCHANGED vars

Next ==
    \/ Append
    \/ \E o \in Offs : FlushAck(o)
    \/ ShardCrash
    \/ \E l \in Leaves : ReadStep(l)
    \/ \E l \in Leaves : PersistCheckpoint(l)
    \/ \E l \in Leaves : PersistFail(l)
    \/ \E l \in Leaves : Capture(l)
    \/ \E l \in Leaves : CaptureFail(l)
    \/ \E l \in Leaves : PublishPin(l)
    \/ GcTrim
    \/ \E l \in Leaves : LeafStop(l)
    \/ \E l \in Leaves : Activate(l)
    \/ \E l \in Leaves : ActivateLoadFail(l)
    \/ \E l \in Leaves : ReplayFaultRearm(l)

(***************************************************************************)
(* Fairness: the protocol's own steps are weakly fair - appends complete,  *)
(* flushes land, leaves activate, read, persist, capture and publish, and  *)
(* the GC runs. The fault actions are environment events and deliberately *)
(* NOT fair; faults are bounded by MaxFaults, which is the assumption that *)
(* faults do not happen for ever.                                          *)
(***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Append)
    /\ \A o \in Offs : WF_vars(FlushAck(o))
    /\ WF_vars(GcTrim)
    /\ \A l \in Leaves : WF_vars(ReadStep(l))
    /\ \A l \in Leaves : WF_vars(PersistCheckpoint(l))
    /\ \A l \in Leaves : WF_vars(Capture(l))
    /\ \A l \in Leaves : WF_vars(PublishPin(l))
    /\ \A l \in Leaves : WF_vars(Activate(l))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* Every acknowledged write is recoverable from durable state: a recovery of
\* its owner from the owner's snapshot and the readable WAL rebuilds it.
AckedWriteDurable ==
    \A o \in acked : Recoverable(Owner[o], o)

\* The GC never trims an acknowledged write its owner's durable snapshot does
\* not cover (#4017's invariant).
TrimCoveredBySnapshot ==
    \A o \in acked :
        o < tail => /\ o \in snapRows[Owner[o]]
                    /\ o <= snapCov[Owner[o]]

\* An active leaf's read position never passes an acknowledged write it owns
\* that its projection does not hold: a leaf never serves a projection that
\* has silently lost a write it has read past.
ReadPositionHonest ==
    \A l \in Leaves : \A o \in Owned(l) :
        (up[l] /\ o <= rp[l]) => o \in cache[l]

\* No reader is ever shown an offset above a still-unfilled prefix hole.
ShippingNeverSkips ==
    \A l \in Leaves : \A o \in inflight : up[l] => rp[l] < o

\* No acknowledged offset is ever handed out again, and every assigned
\* offset is below the allocator.
OffsetContiguity ==
    /\ acked \cap inflight = {}
    /\ \A o \in acked \cup inflight : o < next

\* Recovery never falls off the log: no leaf ever latches stale.
RecoveryNeverFallsOffLog ==
    \A l \in Leaves : ~stale[l]

\* The activation's belief about its persisted checkpoint (state.State) is
\* either what storage holds or the anchor it started from. A failed persist
\* must not leave it claiming an advance storage never recorded (#4017).
PersistedBeliefHonest ==
    \A l \in Leaves : up[l] => stCp[l] \in {durCp[l], anchor[l]}

\* Every trim entitlement the pin store has published is backed by durable
\* snapshot coverage: a published offset is at or below the leaf's coverage,
\* which only grows. The root-cause form of RecoveryNeverFallsOffLog's #4523
\* violation, and reachable with no fault at all.
ReleaseBackedBySnapshot ==
    \A l \in Leaves : pinOff[l] >= 0 => snapCov[l] >= pinOff[l]

\* Durable snapshot coverage never regresses.
SnapshotCoverageMonotonic ==
    [][ \A l \in Leaves : snapCov'[l] >= snapCov[l] ]_vars

\* A newly published pin offset never exceeds the persisted checkpoint the
\* leaf believes at that moment: a pending advance never reaches the pin
\* store, which can never take an offset back (#3476).
PublishedPinWithinPersistedBelief ==
    [][ \A l \in Leaves : pinOff'[l] > pinOff[l] => pinOff'[l] <= stCp[l] ]_vars

(***************************************************************************)
(* Liveness.                                                               *)
(***************************************************************************)

\* Every acknowledged write is eventually materialised by its owner.
EveryAckedWriteMaterialised ==
    \A o \in Offs :
        (o \in acked) ~> (up[Owner[o]] /\ o \in cache[Owner[o]])

\* Reclamation eventually advances over the whole stream once every write
\* is in and every pin can be released.
ReclamationEventuallyAdvances ==
    <>(tail = MaxOff)

=============================================================================
