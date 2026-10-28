------------------------- MODULE ShardOwnershipFold -------------------------
(***************************************************************************)
(* Key ownership across a round trip: an adaptive shard split moves k2's   *)
(* slot from s1 to s2, and an online shard fold later moves it back.       *)
(* The fold's survivor is the split's source, so it still carries the      *)
(* slot in its permanent moved-away seal and still holds the rows it had   *)
(* when the split moved the slot away; the fold reclaims the slot before   *)
(* the map moves back. The fold's donor is retired once it has committed,  *)
(* and a fold may be cancelled before it freezes. An atomic-write saga     *)
(* over k1 and k2, stale routers, one later write or delete of k2, and     *)
(* tombstone compaction run alongside.                                     *)
(*                                                                         *)
(* One physical copy: the resize, the undo and the registry's retention    *)
(* are the companion modules' (ShardOwnership, ShardOwnershipRetention).   *)
(* See README.md for the instance and how to run TLC, and RefinementFold.md *)
(* for the mapping of every variable, action and property to production.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS s1, s2, k1, k2

Shards == {s1, s2}
Keys == {k1, k2}

\* A row holds a version of a write. Rank orders versions by the writes'
\* real-time order, which acknowledgement and reads are judged by; Stamp is the
\* HLC stamp last-writer-wins compares. Reaped is a tombstone that compaction
\* removed: it reads as deleted, but it no longer exists to win a merge, so it
\* carries no stamp at all.
Absent == 0
InitV == 1
SagaV == 2
LaterV == 3
DelV == 4
Reaped == 5
\* The fold's reconciliation tombstone over a live version v, stamped just above
\* it (TreeShardConsolidationGrain.JustAbove): it reads as deleted, beats the
\* version it covers, and loses to any later write.
ReconOf(v) == 10 + v
Live == {InitV, SagaV, LaterV}
Recons == {ReconOf(v) : v \in Live}
Vals == {Absent, InitV, SagaV, LaterV, DelV, Reaped} \cup Recons

Rank(v) == IF v = Reaped \/ v \in Recons THEN DelV ELSE v
Stamp(v) == IF v = Reaped THEN 0
            ELSE IF v \in Recons THEN 10 * (v - 10) + 1
            ELSE 10 * v

Max(a, b) == IF a > b THEN a ELSE b
\* Last-writer-wins. A missing row and a reaped tombstone both lose to any row;
\* between the two, the reaped tombstone stands for the delete that happened.
LWW(a, b) == IF Stamp(a) > Stamp(b) THEN a
             ELSE IF Stamp(b) > Stamp(a) THEN b
             ELSE Max(a, b)

\* A routing map is abstracted to the shard k2's virtual slot routes to; k1's
\* slot never moves.
MapOf(m, k) == IF k = k2 THEN m ELSE s1

VARIABLES
    rmap,       \* registry: where k2 routes
    published,  \* every map the registry has published; any stale router may still hold one
    row,        \* row[s][k]: committed projection value on shard s
    pend,       \* pend[s][k]: the saga's prepared bucket, "none" or "pending"
    term,       \* term[s]: the leaf activation's memory that the saga's terminal applied here
    sealed,     \* sealed[s]: k2's slot is in shard s's permanent moved-away seal
    retired,    \* retired[s]: shard s was retired by a committed fold
    mp,         \* migration phase
    mk,         \* migration kind: "none", "split" or "fold"
    src,        \* the migration's source (a fold's donor)
    dst,        \* the migration's destination (a fold's survivor)
    mdone,      \* the migrations that have committed
    cancels,    \* the environment's budget for cancelling a fold
    sg,         \* saga phase
    prepped,    \* keys the saga has dispatched
    told,       \* shards the terminal broadcast has visited
    dec,        \* the registry's recorded decision for the saga
    opDone,     \* the later write or delete of k2 has happened
    young,      \* the delete landed while the saga was deciding, which has not completed yet
    ackOn,      \* ghost: ackOn[k] is the rank of the newest version acknowledged for k
    vis         \* ghost: vis[k] is the highest rank a fresh reader has been served for k

vars == <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone,
          cancels, sg, prepped, told, dec, opDone, young, ackOn, vis>>

\* Every variable but the ghost vis, which the step relation sets alongside each
\* action (see VisStep).
svars == <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone,
           cancels, sg, prepped, told, dec, opDone, young, ackOn>>

-----------------------------------------------------------------------------
(* Derived state                                                           *)

\* The migration's source mirrors k2 to its destination while the window is
\* open, before the freeze.
Mirrors(s, k) == mp \in {"shadow", "swept"} /\ s = src /\ k = k2

\* The source refuses k2 from the freeze on, until the migration finishes and
\* the seal takes over.
Frozen(s, k) == mp \in {"frozen", "flipped"} /\ s = src /\ k = k2

\* A routed operation on key k refused at shard s: the shard's write and read
\* gates refuse a slot that is frozen there or in its permanent seal.
Refused(s, k) == k = k2 /\ (sealed[s] \/ Frozen(s, k))

Owner(k) == MapOf(rmap, k)

\* What the registry answers for the saga: InFlight before a decision.
RegistryView == IF dec = "none" THEN "inflight" ELSE dec

\* The leaf read gate: a bucket of a committed saga surfaces unless the
\* activation remembers applying its terminal (the orphan guard).
Surfaced(s, k) == pend[s][k] = "pending" /\ RegistryView = "committed" /\ ~term[s]

ValueAtRow(s, k, r) == IF Surfaced(s, k) THEN LWW(r, SagaV) ELSE r
ValueAt(s, k) == ValueAtRow(s, k, row[s][k])

Serves(m, k) == ~Refused(MapOf(m, k), k)
ReadVia(m, k) == Rank(ValueAt(MapOf(m, k), k))

\* The frozen donor still holds k2, as a row or as a prepared bucket.
DonorHolds == row[src][k2] \notin {Absent, Reaped} \/ pend[src][k2] # "none"

\* A fold's reconciliation (TreeShardConsolidationGrain.TombstoneRowsTheDonorReapedAsync):
\* a live row the survivor kept from before the split, whose key the donor no
\* longer holds, is tombstoned just above its stamp.
Reconciled(r) == IF mk = "fold" /\ ~DonorHolds /\ r \in Live THEN ReconOf(r) ELSE r

\* k2's row at the destination once the final drain and the reconciliation ran.
Drained == Reconciled(LWW(row[dst][k2], row[src][k2]))

\* What a fresh reader gets. A read the frozen source refuses is retried once
\* the map has moved, so the reader sees the destination after the final drain.
OwnerValue(k) ==
    Rank(IF Frozen(Owner(k), k)
         THEN ValueAtRow(dst, k, Drained)
         ELSE ValueAt(Owner(k), k))

\* Where a mutation accepted at shard s for key k lands: there, and at the
\* migration's destination while the window mirrors it.
Landing(s, k) == {s} \cup (IF Mirrors(s, k) THEN {dst} ELSE {})

\* A saga prepare's forward reaches the destination through its write gate
\* (ShardRootGrain.SetAsync), so a destination that refuses the slot fails the
\* whole call, which the saga retries. A plain write's forward is a migration
\* import (MergeManyAsync with isCrossShardMigration), which no gate refuses.
PrepareLandable(s, k) == ~Mirrors(s, k) \/ ~Refused(dst, k)

\* The shards the terminal broadcast must visit: the owners of the batch's keys,
\* every shard holding one of its buckets, and an open migration's destination.
TermTargets ==
    {Owner(k) : k \in Keys}
    \cup {s \in Shards : \E k \in Keys : pend[s][k] # "none"}
    \cup (IF mp # "idle" THEN {dst} ELSE {})

\* The terminal's effect on one key at one shard. An activation that already
\* applied it discards a bucket; otherwise a bucket drains, and the
\* committed-values backstop covers a key the shard owns.
TermRow(s, k) ==
    IF term[s] \/ dec = "aborted" THEN row[s][k]
    ELSE IF pend[s][k] # "none" \/ Owner(k) = s THEN LWW(row[s][k], SagaV)
    ELSE row[s][k]

-----------------------------------------------------------------------------
Init ==
    /\ rmap = s1
    /\ published = {s1}
    /\ row = [s \in Shards |-> [k \in Keys |-> IF s = s1 THEN InitV ELSE Absent]]
    /\ pend = [s \in Shards |-> [k \in Keys |-> "none"]]
    /\ term = [s \in Shards |-> FALSE]
    /\ sealed = [s \in Shards |-> FALSE]
    /\ retired = [s \in Shards |-> FALSE]
    /\ mp = "idle"
    /\ mk = "none"
    /\ src = s1
    /\ dst = s2
    /\ mdone = {}
    /\ cancels = 1
    /\ sg = "idle"
    /\ prepped = {}
    /\ told = {}
    /\ dec = "none"
    /\ opDone = FALSE
    /\ young = FALSE
    /\ ackOn = [k \in Keys |-> Rank(InitV)]
    /\ vis = [k \in Keys |-> Rank(InitV)]

-----------------------------------------------------------------------------
(* Migrations. The split (TreeShardSplitGrain) and the fold                *)
(* (TreeShardConsolidationGrain) open the source's window through the same *)
(* primitive and share every later step. They differ in admission, in the  *)
(* fold's reclaim of a survivor that once gave the slot away, and in the   *)
(* fold's retirement of its donor.                                         *)

SplitBegin ==
    /\ mp = "idle"
    /\ "split" \notin mdone
    /\ rmap = s1
    /\ mp' = "shadow"
    /\ mk' = "split"
    /\ src' = s1
    /\ dst' = s2
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

FoldBegin ==
    /\ mp = "idle"
    /\ "split" \in mdone
    /\ "fold" \notin mdone
    /\ rmap = s2
    /\ mp' = "shadow"
    /\ mk' = "fold"
    /\ src' = s2
    /\ dst' = s1
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* The retroactive sweep of a prepare that predates the window: a decided saga
\* is resolved at the destination by applying the saga's terminal there
\* (AppendTxTerminalAsync), which drains every bucket the destination holds for
\* it and backstops k2 for a commit; an undecided one is replayed through the
\* destination's write gate, so a destination that refuses the slot fails the
\* sweep, which the coordinator re-runs on its next pass. A fold's survivor can
\* hold the saga's other buckets; a split's fresh destination never does.
MigSweep ==
    /\ mp = "shadow"
    /\ LET b == pend[src][k2]
       IN IF b = "none"
          THEN UNCHANGED <<row, pend, term>>
          ELSE IF RegistryView \in {"committed", "aborted"}
               THEN /\ row' = IF RegistryView = "committed"
                              THEN [row EXCEPT ![dst] = [k \in Keys |->
                                       IF k = k2 \/ (pend[dst][k] # "none" /\ ~term[dst])
                                       THEN LWW(row[dst][k], SagaV) ELSE row[dst][k]]]
                              ELSE row
                    /\ pend' = [pend EXCEPT ![dst] = [k \in Keys |-> "none"]]
                    /\ term' = [term EXCEPT ![dst] = TRUE]
               ELSE /\ ~Refused(dst, k2)
                    /\ pend' = [pend EXCEPT ![dst][k2] = b]
                    /\ UNCHANGED <<row, term>>
    /\ mp' = "swept"
    /\ UNCHANGED <<rmap, published, sealed, retired, mk, src, dst, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* Seal the source's leaves and enter Reject: the source refuses k2.
MigFreeze ==
    /\ mp = "swept"
    /\ mp' = "frozen"
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mk, src, dst, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* The authoritative final drain over the frozen source, then, for a fold, the
\* survivor's reclaim of the slot, then the map moves. The drain is a migration
\* import, resolved by last-writer-wins even over a row the survivor still holds
\* behind its seal (BPlusLeafGrain.MergeIntoStateAsync, #5130). A fold then
\* tombstones, just above its stamp, a live row the survivor kept from before
\* the split whose key the frozen donor no longer holds, neither as a row nor as
\* a prepared bucket: compaction reaped the delete that superseded it (Drained).
MigCommit ==
    /\ mp = "frozen"
    /\ row' = [row EXCEPT ![dst][k2] = Drained]
    /\ sealed' = IF mk = "fold" THEN [sealed EXCEPT ![dst] = FALSE] ELSE sealed
    /\ rmap' = dst
    /\ published' = published \cup {dst}
    /\ mp' = "flipped"
    /\ UNCHANGED <<pend, term, retired, mk, src, dst, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* The migration finishes: the source records the slot in its permanent seal.
\* A fold then retires its donor, releasing its storage, unless a prepared
\* bucket still blocks retirement, which leaves a routing-only retirement.
MigFinish ==
    /\ mp = "flipped"
    /\ LET release == mk = "fold" /\ \A k \in Keys : pend[src][k] = "none"
       IN /\ row' = IF release THEN [row EXCEPT ![src] = [k \in Keys |-> Absent]] ELSE row
          /\ retired' = IF mk = "fold" THEN [retired EXCEPT ![src] = TRUE] ELSE retired
    /\ sealed' = [sealed EXCEPT ![src] = TRUE]
    /\ mdone' = mdone \cup {mk}
    /\ mp' = "idle"
    /\ mk' = "none"
    /\ UNCHANGED <<rmap, published, pend, term, src, dst, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* A fold cancelled before its freeze: the donor's window closes and the fold
\* may start again. The survivor keeps whatever the sweep gave it.
FoldCancel ==
    /\ mp \in {"shadow", "swept"}
    /\ mk = "fold"
    /\ cancels > 0
    /\ cancels' = cancels - 1
    /\ mp' = "idle"
    /\ mk' = "none"
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, src, dst, mdone, sg, prepped, told, dec, opDone, young, ackOn>>

-----------------------------------------------------------------------------
(* The atomic-write saga (AtomicWriteGrain), writing SagaV to k1 and k2.    *)

SagaStart ==
    /\ sg = "idle"
    /\ sg' = "exec"
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, prepped, told, dec, opDone, young, ackOn>>

\* One key's prepared write, dispatched through a router holding map m.
SagaPrepare(k, m) ==
    /\ sg = "exec"
    /\ k \notin prepped
    /\ m \in published
    /\ LET s == MapOf(m, k)
       IN /\ ~Refused(s, k)
          /\ PrepareLandable(s, k)
          /\ pend' = [x \in Shards |-> [j \in Keys |->
                         IF j = k /\ x \in Landing(s, k) THEN "pending" ELSE pend[x][j]]]
    /\ prepped' = prepped \cup {k}
    /\ UNCHANGED <<rmap, published, row, term, sealed, retired, mp, mk, src, dst, mdone, cancels, sg, told, dec, opDone, young, ackOn>>

SagaDecide ==
    /\ sg = "exec"
    /\ prepped = Keys
    /\ dec' = "committed"
    /\ sg' = "decided"
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, prepped, told, opDone, young, ackOn>>

\* The saga records an abort: a prepare failed past its retries, or the caller
\* went away.
SagaAbort ==
    /\ sg = "exec"
    /\ dec' = "aborted"
    /\ sg' = "decided"
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, prepped, told, opDone, young, ackOn>>

\* One shard of the terminal broadcast.
SagaTerminal(s) ==
    /\ sg = "decided"
    /\ s \in TermTargets \ told
    /\ told' = told \cup {s}
    /\ row' = [row EXCEPT ![s] = [k \in Keys |-> TermRow(s, k)]]
    /\ pend' = [pend EXCEPT ![s] = [k \in Keys |-> "none"]]
    /\ term' = [term EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<rmap, published, sealed, retired, mp, mk, src, dst, mdone, cancels, sg, prepped, dec, opDone, young, ackOn>>

\* The broadcast has visited every target: the saga completes, and a committed
\* saga's caller is acknowledged.
SagaComplete ==
    /\ sg = "decided"
    /\ TermTargets \subseteq told
    /\ sg' = "done"
    /\ ackOn' = IF dec = "committed" THEN [k \in Keys |-> Max(ackOn[k], Rank(SagaV))] ELSE ackOn
    /\ young' = FALSE
    /\ UNCHANGED <<rmap, published, row, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, prepped, told, dec, opDone>>

-----------------------------------------------------------------------------
(* Environment                                                              *)

\* A later plain write (LaterV) or delete (DelV) of k2, through a router
\* holding map m. A plain delete is not forwarded during a window: the final
\* drain carries its tombstone (ShardRootGrain.ForwardLocalDeleteToShadowIfNeededAsync
\* forwards only a saga's prepared delete).
LaterOp(m, v) ==
    /\ ~opDone
    /\ dec # "none"
    /\ m \in published
    /\ LET s == MapOf(m, k2)
           lands == IF v = DelV THEN {s} ELSE Landing(s, k2)
       IN /\ ~Refused(s, k2)
          /\ row' = [x \in Shards |-> [k \in Keys |->
                        IF k = k2 /\ x \in lands THEN LWW(row[x][k], v) ELSE row[x][k]]]
    /\ ackOn' = [ackOn EXCEPT ![k2] = Max(@, Rank(v))]
    /\ opDone' = TRUE
    /\ young' = (v = DelV /\ sg = "decided")
    /\ UNCHANGED <<rmap, published, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, sg, prepped, told, dec>>

\* Tombstone compaction (TombstoneCompactionGrain) reaps a tombstone once its
\* grace period has passed, on whichever shard holds it. A key whose saga bucket
\* is still undrained is left alone here: production does not check, but a
\* bucket that outlives the grace period (a day by default) is a stranded
\* bucket, which ShardOwnershipRetention's NoStrandedBucket rules out
\* (RefinementFold.md, deliberate gaps). For the same reason a tombstone is not
\* reaped while the saga that was deciding when it was written has not
\* completed. Compaction is taken between migrations: reaping a tombstone that
\* had already aged out when a window opened is the same as reaping it just
\* before, and one that ages out inside a window needs a migration open for
\* most of the grace period.
Compact(s) ==
    /\ mp = "idle"
    /\ row[s][k2] \in {DelV} \cup Recons
    /\ pend[s][k2] = "none"
    /\ ~young
    /\ row' = [row EXCEPT ![s][k2] = Reaped]
    /\ UNCHANGED <<rmap, published, pend, term, sealed, retired, mp, mk, src, dst, mdone, cancels, sg, prepped, told, dec, opDone, young, ackOn>>

\* Nothing is in flight.
Quiescent ==
    /\ sg \in {"idle", "done"}
    /\ mp = "idle"

Stutter ==
    /\ Quiescent
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ SplitBegin
    \/ FoldBegin
    \/ MigSweep
    \/ MigFreeze
    \/ MigCommit
    \/ MigFinish
    \/ FoldCancel
    \/ SagaStart
    \/ \E k \in Keys, m \in published : SagaPrepare(k, m)
    \/ SagaDecide
    \/ SagaAbort
    \/ \E s \in Shards : SagaTerminal(s)
    \/ SagaComplete
    \/ \E m \in published, v \in {LaterV, DelV} : LaterOp(m, v)
    \/ \E s \in Shards : Compact(s)
    \/ Stutter

\* The ghost vis tracks, per key, the highest rank a fresh reader has been served.
VisStep == vis' = [k \in Keys |-> Max(vis[k], OwnerValue(k)')]

\* Every step a running coordinator takes on its own is weakly fair. Starting a
\* migration, a cancel, an abort, a stale router's call and compaction are not.
Spec ==
    /\ Init /\ [][Next /\ VisStep]_vars
    /\ WF_svars(MigSweep) /\ WF_svars(MigFreeze) /\ WF_svars(MigCommit) /\ WF_svars(MigFinish)
    /\ WF_svars(\E k \in Keys : SagaPrepare(k, rmap))
    /\ WF_svars(SagaDecide)
    /\ WF_svars(\E s \in Shards : SagaTerminal(s)) /\ WF_svars(SagaComplete)

-----------------------------------------------------------------------------
(* Properties                                                               *)

TypeOK ==
    /\ rmap \in Shards
    /\ published \subseteq Shards
    /\ row \in [Shards -> [Keys -> Vals]]
    /\ pend \in [Shards -> [Keys -> {"none", "pending"}]]
    /\ term \in [Shards -> BOOLEAN]
    /\ sealed \in [Shards -> BOOLEAN]
    /\ retired \in [Shards -> BOOLEAN]
    /\ mp \in {"idle", "shadow", "swept", "frozen", "flipped"}
    /\ mk \in {"none", "split", "fold"}
    /\ src \in Shards
    /\ dst \in Shards
    /\ mdone \subseteq {"split", "fold"}
    /\ cancels \in 0..1
    /\ sg \in {"idle", "exec", "decided", "done"}
    /\ prepped \subseteq Keys
    /\ told \subseteq Shards
    /\ dec \in {"none", "committed", "aborted"}
    /\ opDone \in BOOLEAN
    /\ young \in BOOLEAN
    /\ ackOn \in [Keys -> 0..4]
    /\ vis \in [Keys -> 0..4]

\* Every map any router may hold, fresh or stale, either is refused for a key or
\* reaches that key's one owner.
UniqueOwner ==
    \A m \in published, k \in Keys : Serves(m, k) => MapOf(m, k) = Owner(k)

\* The owner holds every version acknowledged to a writer of the key, and a
\* deleted key stays deleted.
NoKeyLost ==
    \A k \in Keys : OwnerValue(k) >= ackOn[k]

\* No read served through any map returns a version older than one already
\* acknowledged.
NoResurrection ==
    \A m \in published, k \in Keys :
        Serves(m, k) => ReadVia(m, k) >= ackOn[k]

\* Until the later operation lands, a fresh reader sees the saga's batch on both
\* keys or on neither.
AtomicOnOwner ==
    ~opDone => ((OwnerValue(k1) >= Rank(SagaV)) = (OwnerValue(k2) >= Rank(SagaV)))

\* The version a fresh reader gets for a key never moves backwards.
OwnerMonotonic ==
    \A k \in Keys : OwnerValue(k) >= vis[k]

MigrationCompletes == mp # "idle" ~> mp = "idle"
SagaCompletes == sg \in {"exec", "decided"} ~> sg = "done"
RoutingConverges == <>[](\A k \in Keys : Serves(rmap, k))

=============================================================================
