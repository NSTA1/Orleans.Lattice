--------------------------- MODULE WalAppendEpoch ---------------------------
(***************************************************************************)
(* The append-epoch fence of the fenced live checkpoint advance (epic      *)
(* #5605): how one leaf L's activations claim an epoch at a WAL partition, *)
(* and how the WAL shard refuses an append from a superseded activation.   *)
(* It is module E of five focused modules promoted from the design model   *)
(* spec/wal/design/WalLiveAdvance.tla, and the root of their acyclic       *)
(* assume-guarantee graph: it assumes nothing, and its four guarantees are *)
(* what the hand-off, routing-floor and core modules assume. Markers,      *)
(* foreign authors, routing generations, bulk loads, replay and read       *)
(* positions belong to those modules and are not modelled here.            *)
(*                                                                         *)
(* An activation claims in three steps (LiveAdvanceDesign.md, section 1):  *)
(*  - ClaimRow: it persists the epoch it read plus one to L's epoch row,   *)
(*    as a strict conditional write on the row's ETag. Only claims write   *)
(*    the row, so a conflict means another activation claimed.             *)
(*  - ClaimWrite: the WAL shard max-merges the epoch into L's durable      *)
(*    entry, refused at or below the partition's retired floor or below    *)
(*    the entries, and admitted without a write when it equals the durable *)
(*    entry (AlreadyDurable, a re-presentation).                           *)
(*  - ClaimRaise: under the lock that assigns offsets, the shard raises    *)
(*    its in-memory entry and returns the allocator head as the claim      *)
(*    head; refused if another claim or a retirement overtook the write.   *)
(* Any refusal while the row still holds the activation's own write bumps  *)
(* the epoch above the refusal and claims again; a moved row stops it. The *)
(* shard admits a stamped append only from a loaded entry and an epoch not *)
(* below it, and never checks the retired floor there (#5635).             *)
(*                                                                         *)
(* The environment: a duplicate activation, which also stands for the     *)
(* next activation after one stops or is cleared; a WAL shard restart,    *)
(* which unloads the in-memory entry; another leaf's retirement, which    *)
(* raises the shared floor; a bump forced on a second partition; and L's  *)
(* clear, in production's three steps: read the epoch row, retire the     *)
(* entry under a floor at least the higher of the entry and the row       *)
(* (#5641), then delete the row conditionally on the ETag read.           *)
(*                                                                         *)
(* See AppendEpochRefinement.md for the mapping to production, the        *)
(* interface the other modules assume, and what this model does NOT       *)
(* cover.                                                                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

(***************************************************************************)
(* Instance. WithRestart, WithRecreate (L's clear), WithOtherRetire and    *)
(* WithSecondPartition switch the environment steps on. MaxEpoch bounds    *)
(* the epochs; production bounds a claim's rounds instead                  *)
(* (LeafAppendEpochFence.MaxClaimRounds) and then appends unstamped.       *)
(***************************************************************************)
CONSTANTS WithRestart, WithRecreate, WithOtherRetire, WithSecondPartition

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
NoAuthor == "none"
MaxEpoch == 3
Epochs == 0..MaxEpoch
Unloaded == -1

Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    rowEpoch,      \* the epoch in L's epoch row; 0 when absent
    rowInc,        \* the row's incarnation: with the epoch, the row's ETag stand-in
    up,            \* L's running activations
    seen,          \* the epoch row each activation read
    seenInc,       \* the incarnation each activation read the row in
    epoch,         \* the epoch each activation persisted; 0 before its claim begins
    cstage,        \* each claim: 0 none, 1 row persisted, 2 entry written, 3 raised
    claimHead,     \* the allocator head each activation's raise returned
    prior,         \* ghost: the offsets already allocated when each claim was raised
    dupUsed,       \* the duplicate has activated
    fence,         \* the WAL shard's durable entry for L; 0 when absent
    memFence,      \* the WAL shard's in-memory entry for L; Unloaded when not loaded
    retired,       \* the partition's retired-epoch floor
    next,          \* the WAL shard's allocator: the next offset it assigns
    author,        \* the activation each allocated offset was allocated to
    restarted,     \* the WAL shard has restarted
    otherRetired,  \* another leaf of the partition has been retired
    clr,           \* L's clear: "idle", "read", "retired", "done"
    clrTag,        \* the epoch row's ETag the clear read: <<incarnation, epoch>>
    clrRow,        \* the epoch-row value the clear passes to the retirement
    lRetired,      \* ghost: the highest epoch L's clear retired
    maxClaimed,    \* ghost: the highest epoch ever raised for L
    staleAccepted, \* ghost: an append from a superseded or retired epoch was allocated
    wrongRefusal   \* ghost: an activation nothing superseded was refused

vars == <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed,
          fence, memFence, retired, next, author, restarted, otherRetired,
          clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

TypeOK ==
    /\ rowEpoch \in Epochs
    /\ rowInc \in 0..1
    /\ up \subseteq Acts
    /\ seen \in [Acts -> Epochs]
    /\ seenInc \in [Acts -> 0..1]
    /\ epoch \in [Acts -> Epochs]
    /\ cstage \in [Acts -> 0..3]
    /\ claimHead \in [Acts -> 0..MaxOff]
    /\ prior \in [Acts -> SUBSET Offs]
    /\ dupUsed \in BOOLEAN
    /\ fence \in Epochs
    /\ memFence \in {Unloaded} \cup Epochs
    /\ retired \in Epochs
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Acts \cup {NoAuthor}]
    /\ restarted \in BOOLEAN
    /\ otherRetired \in BOOLEAN
    /\ clr \in {"idle", "read", "retired", "done"}
    /\ clrTag \in (0..1) \X Epochs
    /\ clrRow \in Epochs
    /\ lRetired \in Epochs
    /\ maxClaimed \in Epochs
    /\ staleAccepted \in BOOLEAN
    /\ wrongRefusal \in BOOLEAN

(* The born activation a1 is running and has read the absent epoch row.   *)
Init ==
    /\ rowEpoch = 0
    /\ rowInc = 0
    /\ up = {"a1"}
    /\ seen = [a \in Acts |-> 0]
    /\ seenInc = [a \in Acts |-> 0]
    /\ epoch = [a \in Acts |-> 0]
    /\ cstage = [a \in Acts |-> 0]
    /\ claimHead = [a \in Acts |-> 0]
    /\ prior = [a \in Acts |-> {}]
    /\ dupUsed = FALSE
    /\ fence = 0
    /\ memFence = Unloaded
    /\ retired = 0
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ restarted = FALSE
    /\ otherRetired = FALSE
    /\ clr = "idle"
    /\ clrTag = <<0, 0>>
    /\ clrRow = 0
    /\ lRetired = 0
    /\ maxClaimed = 0
    /\ staleAccepted = FALSE
    /\ wrongRefusal = FALSE

(***************************************************************************)
(* The epoch row's ETag. Within an incarnation every row write raises the  *)
(* epoch, so (incarnation, epoch) identifies a write; the clear deletes    *)
(* the row and starts a new incarnation, so an epoch value that recurs     *)
(* never matches an old ETag. RowUnmoved(a): the row is as a read it.      *)
(* RowHolds(a): the row is as a last wrote it.                             *)
(***************************************************************************)
RowUnmoved(a) == seen[a] = rowEpoch /\ seenInc[a] = rowInc
RowHolds(a) == rowEpoch = epoch[a] /\ seenInc[a] = rowInc

(* AlreadyDurable(a): the claim equals the durable entry and is not below  *)
(* the loaded one, so it is admitted without a write                       *)
(* (WalAppendFenceCore.DecideClaimWrite).                                  *)
AlreadyDurable(a) == epoch[a] = fence /\ epoch[a] >= memFence

(* The refusals, and the bump a refusal makes while the row holds.         *)
EntryRefuses(a) ==
    \/ cstage[a] = 1 /\ epoch[a] <= Max2(fence, memFence) /\ ~AlreadyDurable(a)
    \/ cstage[a] = 2 /\ epoch[a] < Max2(fence, memFence)
FloorRefuses(a) == cstage[a] \in {1, 2} /\ ~EntryRefuses(a) /\ epoch[a] <= retired
Refused(a) == EntryRefuses(a) \/ FloorRefuses(a)
BumpEpoch == Max2(Max2(fence, memFence), Max2(retired, rowEpoch)) + 1
CanBump(a) == Refused(a) /\ RowHolds(a) /\ BumpEpoch <= MaxEpoch

(* Superseded, as production decides it: another activation has written   *)
(* the row since a did (its ETag no longer holds), a higher epoch was      *)
(* raised, or a's epoch was retired by L's clear. Anything else refused is *)
(* refused wrongly.                                                        *)
NotSuperseded(a) == RowHolds(a) /\ epoch[a] >= maxClaimed /\ epoch[a] > lRetired

(* The shard's check on a stamped append: the leaf's own loaded entry      *)
(* only, never the partition's retired floor (#5635).                      *)
AppendAdmits(a) == epoch[a] >= memFence

ClaimRow(a) ==
    /\ a \in up
    /\ cstage[a] = 0
    /\ RowUnmoved(a)
    /\ seen[a] + 1 <= MaxEpoch
    /\ rowEpoch' = seen[a] + 1
    /\ epoch' = [epoch EXCEPT ![a] = seen[a] + 1]
    /\ cstage' = [cstage EXCEPT ![a] = 1]
    /\ UNCHANGED <<rowInc, up, seen, seenInc, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* The row moved since a read it: another activation claimed, so a is      *)
(* stale and stops.                                                        *)
ClaimConflict(a) ==
    /\ a \in up
    /\ cstage[a] = 0
    /\ ~RowUnmoved(a)
    /\ up' = up \ {a}
    /\ wrongRefusal' = (wrongRefusal \/ RowUnmoved(a))
    /\ UNCHANGED <<rowEpoch, rowInc, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted>>

ClaimWrite(a) ==
    /\ a \in up
    /\ cstage[a] = 1
    /\ epoch[a] > retired
    /\ epoch[a] > Max2(fence, memFence) \/ AlreadyDurable(a)
    /\ fence' = Max2(fence, epoch[a])
    /\ cstage' = [cstage EXCEPT ![a] = 2]
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

ClaimRaise(a) ==
    /\ a \in up
    /\ cstage[a] = 2
    /\ epoch[a] >= Max2(fence, memFence)
    /\ epoch[a] > retired
    /\ memFence' = epoch[a]
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ prior' = [prior EXCEPT ![a] = {o \in Offs : author[o] # NoAuthor}]
    /\ maxClaimed' = Max2(maxClaimed, epoch[a])
    /\ cstage' = [cstage EXCEPT ![a] = 3]
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, dupUsed>>
    /\ UNCHANGED <<fence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, staleAccepted, wrongRefusal>>

(* Any refusal, by the entry or the floor, bumps while the row holds a's   *)
(* own write: persist the higher of the entry, the floor and the row, plus *)
(* one, and claim again. The model bounds the rounds by MaxEpoch.          *)
ClaimBump(a) ==
    /\ a \in up
    /\ Refused(a)
    /\ RowHolds(a)
    /\ BumpEpoch <= MaxEpoch
    /\ rowEpoch' = BumpEpoch
    /\ epoch' = [epoch EXCEPT ![a] = BumpEpoch]
    /\ cstage' = [cstage EXCEPT ![a] = 1]
    /\ UNCHANGED <<rowInc, up, seen, seenInc, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* A refusal with a moved row, or no room to bump, stops the activation.   *)
ClaimRefuse(a) ==
    /\ a \in up
    /\ Refused(a)
    /\ ~CanBump(a)
    /\ up' = up \ {a}
    /\ cstage' = [cstage EXCEPT ![a] = 0]
    /\ wrongRefusal' = (wrongRefusal \/ (NotSuperseded(a) /\ BumpEpoch <= MaxEpoch))
    /\ UNCHANGED <<rowEpoch, rowInc, seen, seenInc, epoch, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted>>

(* WithSecondPartition: a floor-only refusal on another partition bumps    *)
(* the activation's one epoch after it was raised here, conditionally on   *)
(* its row's ETag, and it keeps stamping this partition with the bumped    *)
(* epoch, which this partition never claimed. That over-approximates       *)
(* production, which re-claims here first (LeafAppendEpochFence           *)
(* StampForAsync).                                                         *)
OtherPartitionBump(a) ==
    /\ WithSecondPartition
    /\ a \in up
    /\ cstage[a] = 3
    /\ RowHolds(a)
    /\ \E e \in (rowEpoch + 1)..MaxEpoch :
        /\ rowEpoch' = e
        /\ epoch' = [epoch EXCEPT ![a] = e]
    /\ UNCHANGED <<rowInc, up, seen, seenInc, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* OwnAlloc(a): the shard admits a raised activation's stamped append from *)
(* a loaded entry and an epoch not below it, and assigns it an offset.     *)
OwnAlloc(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ memFence # Unloaded
    /\ AppendAdmits(a)
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed \/ epoch[a] <= lRetired)
    /\ UNCHANGED <<fence, memFence>>
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<retired, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, wrongRefusal>>

(* StaleAppend(a): the shard rejects a stamped append below its loaded     *)
(* entry (StaleEpoch), and the activation, superseded, stops.              *)
StaleAppend(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ memFence # Unloaded
    /\ ~AppendAdmits(a)
    /\ up' = up \ {a}
    /\ wrongRefusal' = (wrongRefusal \/ NotSuperseded(a))
    /\ UNCHANGED <<rowEpoch, rowInc, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted>>

(* Represent(a): an append meets an unloaded entry (UnloadedEntry); the    *)
(* activation re-presents its claim with the same epoch, from the entry    *)
(* write, which loads the entry (LeafAppendEpochFence.OnUnloaded).         *)
Represent(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ memFence = Unloaded
    /\ cstage' = [cstage EXCEPT ![a] = 1]
    /\ UNCHANGED memFence
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* Duplicate: a second activation, or the next one after a stop or a       *)
(* clear, reads the epoch row; it must claim before it appends.            *)
Duplicate ==
    /\ ~dupUsed
    /\ dupUsed' = TRUE
    /\ up' = up \cup {"a2"}
    /\ seen' = [seen EXCEPT !["a2"] = rowEpoch]
    /\ seenInc' = [seenInc EXCEPT !["a2"] = rowInc]
    /\ UNCHANGED <<epoch, cstage>>
    /\ UNCHANGED <<rowEpoch, rowInc, claimHead, prior>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* Restart (WithRestart): the WAL shard restarts, a reactivation or a      *)
(* move's cutover. It loses the in-memory entry; the durable entry and the *)
(* floor are rows in grain storage and survive.                            *)
Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ memFence' = Unloaded
    /\ UNCHANGED fence
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<retired, next, author, otherRetired>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* RetireOther (WithOtherRetire): another leaf of the partition is         *)
(* retired, raising the shared floor, possibly above every epoch L holds.  *)
(* L's own entry is untouched.                                             *)
RetireOther ==
    /\ WithOtherRetire
    /\ ~otherRetired
    /\ otherRetired' = TRUE
    /\ \E e \in (retired + 1)..MaxEpoch : retired' = e
    /\ UNCHANGED <<fence, memFence>>
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<next, author, restarted>>
    /\ UNCHANGED <<clr, clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(***************************************************************************)
(* L's clear (WithRecreate), a purge or a re-creation of the leaf, in      *)
(* production's three steps (WalAppendFenceRetirement.RetireLeafAsync).    *)
(* ClearRead reads the epoch row and its ETag; a clear that finds no row   *)
(* retires nothing and is a stutter here. ClearRetire raises the floor,    *)
(* durably and before the entry goes, to at least the higher of the entry  *)
(* and the row value read (#5641), deletes the durable entry and removes   *)
(* the loaded one. ClearDelete deletes the row conditionally on the ETag   *)
(* read: a row that moved since is kept. Running activations are           *)
(* untouched; the next activation (Duplicate) is the re-created            *)
(* incarnation. lRetired records, from the ETag read, every epoch the      *)
(* clear retired.                                                          *)
(***************************************************************************)
ClearRead ==
    /\ WithRecreate
    /\ clr = "idle"
    /\ rowEpoch > 0
    /\ clr' = "read"
    /\ clrTag' = <<rowInc, rowEpoch>>
    /\ clrRow' = rowEpoch
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<lRetired, maxClaimed, staleAccepted, wrongRefusal>>

ClearRetire ==
    /\ clr = "read"
    /\ clr' = "retired"
    /\ retired' = Max2(retired, Max2(Max2(fence, memFence), clrRow))
    /\ lRetired' = Max2(lRetired, Max2(Max2(fence, memFence), clrTag[2]))
    /\ fence' = 0
    /\ memFence' = Unloaded
    /\ UNCHANGED <<rowEpoch, rowInc, up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clrTag, clrRow, maxClaimed, staleAccepted, wrongRefusal>>

ClearDelete ==
    /\ clr = "retired"
    /\ clr' = "done"
    /\ IF <<rowInc, rowEpoch>> = clrTag
       THEN /\ rowEpoch' = 0
            /\ rowInc' = rowInc + 1
       ELSE UNCHANGED <<rowEpoch, rowInc>>
    /\ UNCHANGED <<up, seen, seenInc, epoch, cstage, claimHead, prior, dupUsed>>
    /\ UNCHANGED <<fence, memFence, retired, next, author, restarted, otherRetired>>
    /\ UNCHANGED <<clrTag, clrRow, lRetired, maxClaimed, staleAccepted, wrongRefusal>>

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ \E a \in Acts : ClaimRow(a)
    \/ \E a \in Acts : ClaimConflict(a)
    \/ \E a \in Acts : ClaimWrite(a)
    \/ \E a \in Acts : ClaimRaise(a)
    \/ \E a \in Acts : ClaimBump(a)
    \/ \E a \in Acts : ClaimRefuse(a)
    \/ \E a \in Acts : OtherPartitionBump(a)
    \/ \E a \in Acts : OwnAlloc(a)
    \/ \E a \in Acts : StaleAppend(a)
    \/ \E a \in Acts : Represent(a)
    \/ Duplicate
    \/ Restart
    \/ RetireOther
    \/ ClearRead
    \/ ClearRetire
    \/ ClearDelete
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The guarantees. Each is a checked invariant, and each is what the       *)
(* hand-off, routing-floor and core modules assume of the epoch fence.     *)
(***************************************************************************)

(* No append is allocated from an epoch below the highest raised one, or   *)
(* at or below an epoch L's clear retired: across a WAL shard restart, and *)
(* across the purge straddle, a claim written before the clear and         *)
(* completed after it.                                                     *)
StaleEpochNeverAppends == ~staleAccepted

(* When a claim is raised, its head is above every offset already          *)
(* allocated, so every append another activation made before the raise    *)
(* lies below the head the raised activation replays to.                   *)
ClaimHeadCoversPriorAppends ==
    \A a \in Acts : cstage[a] = 3 => \A o \in prior[a] : o < claimHead[a]

(* An activation that can still append: its claim is raised, and the shard *)
(* admits its epoch from the entry it holds, or would load on a            *)
(* re-presentation.                                                        *)
CanAppend(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ Max2(fence, memFence) > 0
    /\ epoch[a] >= Max2(fence, memFence)

(* No two activations that can still append hold the same raised epoch:    *)
(* within an incarnation, by the row's conditional write; across          *)
(* incarnations, by the retired floor and the clear's conditional delete.  *)
RaisedEpochUnique ==
    \A a, b \in Acts : a # b /\ CanAppend(a) /\ CanAppend(b) => epoch[a] # epoch[b]

(* Only a superseded activation is ever refused, at a claim or at an       *)
(* append: in particular, another leaf's retirement never refuses a live   *)
(* leaf (#5635), and a clear never stops the re-created incarnation.       *)
OnlySupersededRefused == ~wrongRefusal
=============================================================================
