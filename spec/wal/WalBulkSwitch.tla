--------------------------- MODULE WalBulkSwitch ---------------------------
(***************************************************************************)
(* The bulk switch of the fenced live checkpoint advance (epic #5605): how *)
(* a bulk append's leaf N is born from the old rightmost leaf O's range,   *)
(* with a birth read position (the hint) that no record it lacks lies      *)
(* below, and with every key the range already held resolved as every WAL *)
(* reader resolves it. It is module B of five focused modules promoted    *)
(* from the design model spec/wal/design/WalLiveAdvance.tla. It assumes   *)
(* nothing, and guarantees BulkHintBound and ContestedKeyAgrees to the    *)
(* core module; the switch's fence is the core module's, through the      *)
(* hand-off module (see BulkSwitchRefinement.md, "Interface").            *)
(* Epochs, claims, routing floors, arming and the live advance belong to   *)
(* the other modules and are not modelled here.                           *)
(*                                                                         *)
(* One WAL partition, one key K in the appended range (the contention     *)
(* worst case), the old leaf O that holds K's range before the switch,    *)
(* and the appended leaf N. The steps, in production's order              *)
(* (ShardRootGrain.BulkLoad.cs and ShardRootGrain.BulkSwitch.cs):          *)
(*  - Begin: in the shard root's serial turn, store the birth hint and    *)
(*    open the switch's marker token in one write. The hint is the stored *)
(*    one of an earlier attempt, else the heads of a fence that folds the *)
(*    run's completed switches, else the run's hint, else the heads now.  *)
(*  - Create: N is created holding the bulk entry, and the graft intent is *)
(*    persisted. N reads from the log's start until Finish.               *)
(*  - Narrow: O drains the commits it admitted (#5706), narrows its bound  *)
(*    and links N in one persist (#5654), and hands back a copy of its     *)
(*    rows in the range. A drain past its deadline narrows nothing and    *)
(*    the graft takes the 10.2.0 path (NarrowTimeout): no hint applied.   *)
(*  - Deliver, Drop: the copy is merged into N, and only then dropped from *)
(*    O (#5610: relocate, never refuse; copy, deliver, drop).             *)
(*  - Finish: N's read position becomes the hint capped at O's read      *)
(*    frontier at the narrow (#5752), only if every row was relocated. The switch stays open for the run's batched close.        *)
(*  - Close: one fence closes every completed switch, removing its hint.  *)
(* The environment: O's pre-switch write, a commit O admits and stores in *)
(* two steps, a foreign record of K's range and O's replay of it, a crash *)
(* of the shard root, and N's replay.                                      *)
(*                                                                         *)
(* See BulkSwitchRefinement.md for the mapping to production, the         *)
(* interface, and what this model does NOT cover.                         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

(***************************************************************************)
(* Instance. WithRun: the shard holds a completed switch of an earlier     *)
(* chunk of the run, and possibly the run's hint. WithCrash: the shard     *)
(* root may die once, before or during the graft. WithTimeout: O's drain   *)
(* may miss its deadline.                                                  *)
(***************************************************************************)
CONSTANTS WithRun, WithCrash, WithTimeout

MaxOff == 4
Offs == 0..(MaxOff - 1)
Null == -1
NoAuthor == "none"
\* p: O's write before the switch; c: a commit O admits; f: a foreign record.
Authors == {"p", "c", "f"}
Phases == {"pre", "begun", "retry", "created", "narrowed", "delivered", "dropped", "finished"}

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
    next,       \* the WAL allocator: the next offset it assigns
    author,     \* the author of each allocated offset; every record writes K
    rowsO,      \* the rows O holds, by the offset of the record that wrote them
    rpO,        \* O's read position: the last offset it has replayed; -1 at the start
    fO,         \* the frontier the narrow reported: O's read position when it narrowed
    narrowed,   \* O's bound is narrowed below K
    cst,        \* O's commit: "none", "flight" (allocated, not stored), "stored"
    cCounted,   \* the commit is counted among O's admitted mutations
    phase,      \* the current bulk operation's step
    hint,       \* the current operation's stored hint; Null when absent
    tokOpen,    \* the current operation's switch token is open on the marker
    prevOpen,   \* the run's earlier, completed switch is open (WithRun)
    prevHint,   \* its stored hint; Null when absent
    runHint,    \* the shard root activation's run hint; Null when absent
    bulkAt,     \* the head when the bulk entry was stamped; Null before Begin
    copied,     \* the rows the narrow handed back, in the shard root's memory
    relocOk,    \* the narrow moved every row of the range
    nUp,        \* N has been created
    heldN,      \* the rows N holds, by the offset of the record that wrote them
    rpN,        \* N's read position: the last offset it has applied; -1 at the start
    crashed     \* the shard root has died (WithCrash)

vars == <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, phase, hint, tokOpen, prevOpen,
          prevHint, runHint, bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

Allocated == {o \in Offs : author[o] # NoAuthor}

TypeOK ==
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Authors \cup {NoAuthor}]
    /\ rowsO \subseteq Offs
    /\ rpO \in -1..(MaxOff - 1)
    /\ fO \in -1..(MaxOff - 1)
    /\ narrowed \in BOOLEAN
    /\ cst \in {"none", "flight", "stored"}
    /\ cCounted \in BOOLEAN
    /\ phase \in Phases
    /\ hint \in {Null} \cup 0..MaxOff
    /\ tokOpen \in BOOLEAN
    /\ prevOpen \in BOOLEAN
    /\ prevHint \in {Null, 0}
    /\ runHint \in {Null} \cup 0..MaxOff
    /\ bulkAt \in {Null} \cup 0..MaxOff
    /\ copied \subseteq Offs
    /\ relocOk \in BOOLEAN
    /\ nUp \in BOOLEAN
    /\ heldN \subseteq Offs
    /\ rpN \in -1..(MaxOff - 1)
    /\ crashed \in BOOLEAN

(* WithRun: an earlier chunk's switch is open with its stored hint (heads  *)
(* read at the run's start, 0 here), and the activation may still hold the *)
(* run's hint or, after a restart, not.                                    *)
Init ==
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ rowsO = {}
    /\ rpO = -1
    /\ fO = -1
    /\ narrowed = FALSE
    /\ cst = "none"
    /\ cCounted = FALSE
    /\ phase = "pre"
    /\ hint = Null
    /\ tokOpen = FALSE
    /\ prevOpen = WithRun
    /\ prevHint = IF WithRun THEN 0 ELSE Null
    /\ runHint \in IF WithRun THEN {Null, 0} ELSE {Null}
    /\ bulkAt = Null
    /\ copied = {}
    /\ relocOk = FALSE
    /\ nUp = FALSE
    /\ heldN = {}
    /\ rpN = -1
    /\ crashed = FALSE

Min2(x, y) == IF x <= y THEN x ELSE y

Alloc(a) ==
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]

(***************************************************************************)
(* The environment's writes of K.                                          *)
(***************************************************************************)

(* PreWrite: before the bulk operation begins, O takes a write of K and    *)
(* stores it with its record.                                              *)
PreWrite ==
    /\ phase = "pre"
    /\ \A o \in Offs : author[o] # "p"
    /\ Alloc("p")
    /\ rowsO' = rowsO \cup {next}
    /\ UNCHANGED <<rpO, fO, narrowed, cst, cCounted, phase, hint, tokOpen, prevOpen, prevHint, runHint,
                   bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(* CommitAlloc: O admits a write of K inside its declared span, counts it  *)
(* among its admitted mutations, and its record is allocated; it is stored *)
(* on O later (CommitStore). At any time before the narrow.                *)
CommitAlloc ==
    /\ ~narrowed
    /\ cst = "none"
    /\ Alloc("c")
    /\ cst' = "flight"
    /\ cCounted' = TRUE
    /\ UNCHANGED <<rowsO, rpO, fO, narrowed, phase, hint, tokOpen, prevOpen, prevHint, runHint,
                   bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(* CommitStore: the admitted commit is stored on O. After a narrow it lies *)
(* outside O's declared span, where nothing reads it or relocates it.      *)
CommitStore ==
    /\ cst = "flight"
    /\ cst' = "stored"
    /\ rowsO' = rowsO \cup {CHOOSE o \in Offs : author[o] = "c"}
    /\ UNCHANGED <<next, author, rpO, fO, narrowed, cCounted, phase, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(* ReplayO: O replays the next record. While it declares K's range it    *)
(* holds it if another author wrote it (its own records it already        *)
(* holds); after the narrow its replay filters K out, so it passes the    *)
(* record without holding it.                                             *)
ReplayO ==
    /\ rpO + 1 < next
    /\ rpO' = rpO + 1
    /\ rowsO' = IF author[rpO + 1] = "f" /\ ~narrowed THEN rowsO \cup {rpO + 1} ELSE rowsO
    /\ UNCHANGED <<next, author, fO, narrowed, cst, cCounted, phase, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(* ForeignAlloc: a record of K's range by another author (a hand-off      *)
(* initiator), held only by replay: before the narrow O reaches it by its  *)
(* own replay (ReplayO), after it only N's replay does.                    *)
ForeignAlloc ==
    /\ \A o \in Offs : author[o] # "f"
    /\ Alloc("f")
    /\ UNCHANGED <<rowsO, rpO, fO, narrowed, cst, cCounted, phase, hint, tokOpen, prevOpen, prevHint, runHint,
                   bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(***************************************************************************)
(* The switch.                                                             *)
(***************************************************************************)

(* Begin (ShardRootGrain.BeginBulkSwitchAsync). The run's completed switch *)
(* is folded by one fence when the cadence is reached (any time, here) or  *)
(* when the activation holds neither a stored nor a run hint; the fence's  *)
(* heads are then the hint. The hint and the open token are one write.     *)
Begin ==
    /\ phase \in {"pre", "retry"}
    /\ \E cadence \in BOOLEAN :
        LET fold == prevOpen /\ (cadence \/ (hint = Null /\ runHint = Null))
            h == IF hint # Null THEN hint
                 ELSE IF fold THEN next
                 ELSE IF runHint # Null THEN runHint
                 ELSE next
        IN /\ hint' = h
           /\ tokOpen' = TRUE
           /\ runHint' = h
           /\ prevOpen' = (prevOpen /\ ~fold)
           /\ prevHint' = IF fold THEN Null ELSE prevHint
    /\ phase' = "begun"
    /\ bulkAt' = IF bulkAt = Null THEN next ELSE bulkAt
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, copied, relocOk, nUp, heldN, rpN,
                   crashed>>

(* Create: N is created holding the bulk entry, reading from the log's     *)
(* start, and the graft intent is persisted.                               *)
Create ==
    /\ phase = "begun"
    /\ phase' = "created"
    /\ nUp' = TRUE
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, heldN, rpN, crashed>>

(* Narrow (BPlusLeafGrain.NarrowHighKeyAsync): drain the counted commits   *)
(* (only when it would narrow), narrow and link, and hand back a copy of   *)
(* the rows in the range, with its read frontier, read in the same turn.  *)
(* A resumed narrow of a narrowed leaf drains nothing, copies what it      *)
(* still holds, and reports no frontier: its replay may have passed a     *)
(* record of the range since, unheld. With no frontier, no hint is applied. *)
Narrow ==
    /\ phase = "created"
    /\ narrowed \/ ~(cst = "flight" /\ cCounted)
    /\ narrowed' = TRUE
    /\ copied' = rowsO
    /\ relocOk' = ~narrowed
    /\ fO' = rpO
    /\ phase' = "narrowed"
    /\ UNCHANGED <<next, author, rowsO, rpO, cst, cCounted, hint, tokOpen, prevOpen, prevHint, runHint,
                   bulkAt, nUp, heldN, rpN, crashed>>

(* NarrowTimeout: the drain misses its deadline, nothing is narrowed, and  *)
(* the graft takes the 10.2.0 path: it links N and applies no hint.        *)
NarrowTimeout ==
    /\ WithTimeout
    /\ phase = "created"
    /\ ~narrowed
    /\ cst = "flight" /\ cCounted
    /\ relocOk' = FALSE
    /\ phase' = "dropped"
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, nUp, heldN, rpN, crashed>>

(* Deliver (ShardRootGrain.RelocateIntoGraftedLeavesAsync): the copy is    *)
(* merged into N with its stamps.                                          *)
Deliver ==
    /\ phase = "narrowed"
    /\ heldN' = heldN \cup copied
    /\ phase' = "delivered"
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, nUp, rpN, crashed>>

(* Drop (BPlusLeafGrain.DropRelocatedRowsAsync): only once delivered, O    *)
(* drops exactly the rows it handed back.                                  *)
Drop ==
    /\ phase = "delivered"
    /\ rowsO' = rowsO \ copied
    /\ phase' = "dropped"
    /\ UNCHANGED <<next, author, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, nUp, heldN, rpN, crashed>>

(* Finish (ShardRootGrain.FinishBulkSwitchAsync): N's read position        *)
(* becomes the stored hint only when every row was relocated, capped at   *)
(* the frontier the narrow reported (ShardRootGrain.CapAtReadFrontier,     *)
(* #5752): a record of the range O never applied is then above it, and N  *)
(* replays it. The switch stays open for the run's fence.                  *)
Finish ==
    /\ phase = "dropped"
    /\ phase' = "finished"
    /\ rpN' = IF relocOk /\ hint # Null THEN Min2(hint - 1, fO) ELSE rpN
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   runHint, bulkAt, copied, relocOk, nUp, heldN, crashed>>

(* Close (ShardRootGrain.CloseBulkSwitchesAsync): one fence closes every   *)
(* completed switch, every open token but a pending graft's (an abandoned  *)
(* one included), and removes their hints in the same write. Ends the run. *)
Pending == phase \in {"begun", "created", "narrowed", "delivered", "dropped"}
Close ==
    /\ prevOpen \/ (tokOpen /\ ~Pending)
    /\ prevOpen' = FALSE
    /\ prevHint' = Null
    /\ tokOpen' = (tokOpen /\ Pending)
    /\ hint' = IF Pending THEN hint ELSE Null
    /\ runHint' = Null
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, phase, bulkAt, copied, relocOk,
                   nUp, heldN, rpN, crashed>>

(* Crash (WithCrash): the shard root dies once. Its memory goes (the run   *)
(* hint, the copy). Before the graft intent the client retries the        *)
(* operation from Begin; after it the graft resumes from the narrow.       *)
Crash ==
    /\ WithCrash
    /\ ~crashed
    /\ phase \in {"begun", "narrowed", "delivered", "dropped"}
    /\ crashed' = TRUE
    /\ runHint' = Null
    /\ copied' = {}
    /\ phase' = IF phase = "begun" THEN "retry" ELSE "created"
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, hint, tokOpen, prevOpen, prevHint,
                   bulkAt, relocOk, nUp, heldN, rpN>>

(* Replay: once its switch is finished, N applies the next record.         *)
Replay ==
    /\ phase = "finished"
    /\ rpN + 1 < next
    /\ rpN' = rpN + 1
    /\ heldN' = heldN \cup {rpN + 1}
    /\ UNCHANGED <<next, author, rowsO, rpO, fO, narrowed, cst, cCounted, phase, hint, tokOpen, prevOpen,
                   prevHint, runHint, bulkAt, copied, relocOk, nUp, crashed>>

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ PreWrite
    \/ CommitAlloc
    \/ CommitStore
    \/ ForeignAlloc
    \/ ReplayO
    \/ Begin
    \/ Create
    \/ Narrow
    \/ NarrowTimeout
    \/ Deliver
    \/ Drop
    \/ Finish
    \/ Close
    \/ Crash
    \/ Replay
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The guarantees.                                                         *)
(***************************************************************************)

(* N's read position is at or below every record of its range it does not *)
(* hold: from its birth, when the read position becomes the hint and the  *)
(* relocated rows are held, through its replay.                           *)
BulkHintBound == nUp => \A o \in Allocated : o <= rpN => o \in heldN

(* The contested key (#5610). The entry at offset o has version Ver(o).    *)
(* The bulk entry is stamped on the shard root's clock when its chunk is   *)
(* appended, so its order against a concurrent write is arbitrary; the     *)
(* model fixes it between the records allocated before Begin and those     *)
(* after. N and every WAL reader hold the bulk entry, and each resolves K  *)
(* last-writer-wins by version up to N's read position: N over the rows it *)
(* holds, a reader over every record. Top is the winning offset, -1 the    *)
(* bulk value.                                                             *)
Ver(o) == 2 * o + 2
BulkVer == 2 * bulkAt + 1
Top(S) == IF S = {} \/ BulkVer > Ver(Max(S)) THEN -1 ELSE Max(S)
OwnerRank == Top({o \in heldN : o <= rpN})
ReaderRank == Top({o \in Allocated : o <= rpN})

(* The owner resolves the contested key to the value every WAL reader     *)
(* resolves: last writer wins by version, the later-stamped write wins.   *)
ContestedKeyAgrees == nUp => OwnerRank = ReaderRank

(* A stored hint belongs to an open switch: the hint and the token are     *)
(* stored in one write and removed in one write, so no closed switch       *)
(* leaves a hint a resume could adopt.                                     *)
HintOnlyWhileOpen ==
    /\ hint # Null => tokOpen
    /\ prevHint # Null => prevOpen
=============================================================================
