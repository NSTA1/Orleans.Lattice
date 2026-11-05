--------------------------- MODULE WalHandOffFence ---------------------------
(***************************************************************************)
(* The hand-off fence of the fenced live checkpoint advance (epic #5605):  *)
(* how a foreign author - any writer of a record the owner leaf L owns     *)
(* that is not an activation of L - is bounded by L's durable hand-off     *)
(* marker. It is module H of five focused modules promoted from the design *)
(* model spec/wal/design/WalLiveAdvance.tla. It assumes only module E's    *)
(* entry check, as an environment constraint, and its two guarantees are   *)
(* what the bulk-switch and core modules assume. L's own appends, replay,  *)
(* read positions, routing floors and purges belong to the other modules   *)
(* and are not modelled here.                                              *)
(*                                                                         *)
(* Every foreign-author path follows one protocol (LiveAdvanceDesign.md,   *)
(* section 2):                                                             *)
(*  - ForeignOpen: the initiator opens L's marker before its first         *)
(*    admission of a write L will own.                                     *)
(*  - ForeignAdmit: a write is admitted and stamped, in the same step, with *)
(*    the initiator's current generation (an epoch, a routing generation,  *)
(*    or the tree's range-delete generation).                              *)
(*  - ForeignAlloc / ForeignReject: the WAL shard allocates a stamped      *)
(*    append at or above the initiator's entry and rejects one below it.   *)
(*  - Close: the initiator stops admitting, bumps and re-claims its        *)
(*    generation, which raises its entry and reads the allocator head in   *)
(*    one step, and closes the marker recording that head.                 *)
(* Another shard's walk of a tree-level range delete (WithTreeWalk) is     *)
(* stamped with the same tree generation, so one re-claim fences it too; a *)
(* rejected walk re-stamps only while its own intent's marker holds L      *)
(* (rule 3), and otherwise appends unstamped. A hand-off begun before the  *)
(* fence gate opened (WithLegacyHandOff) has no marker; it opens one when  *)
(* it resumes, and L's marker state is initialised only once no such       *)
(* hand-off is in flight.                                                  *)
(*                                                                         *)
(* See HandOffFenceRefinement.md for the mapping to production, the        *)
(* interface, and what this model does NOT cover.                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

(***************************************************************************)
(* Instance. WithBatch adds the batch write kind, MaxWindows bounds the    *)
(* hand-off windows, WithRestart a WAL shard restart, WithTreeWalk another *)
(* shard's range-delete walk, and WithLegacyHandOff a hand-off in flight   *)
(* across the fence gate's opening.                                        *)
(***************************************************************************)
CONSTANTS WithBatch, MaxWindows, WithRestart, WithTreeWalk, WithLegacyHandOff,
          WithPreArm, WithStaleDonor, WithRearmRace, WithDeferredClose, WithDonorFence

MaxOff == 3
Offs == 0..(MaxOff - 1)
Kinds == IF WithBatch THEN {"point", "batch"} ELSE {"point"}
\* WithRearmRace: one more epoch, so the live donor's re-arm race (claim,
\* fence, probed re-claim) is reachable rather than cut off by the bound.
MaxEpoch == IF WithRearmRace THEN 4 ELSE 3
Epochs == 0..MaxEpoch

(* The author of an allocated offset: a stamped foreign write ("f"), a     *)
(* stamped range-delete walk of another shard ("g"), or an unstamped walk *)
(* ("u"), which is admitted and reported, never fenced.                    *)
NoAuthor == "none"
Stamped == {"f", "g", "d"}
Authors == Stamped \cup {"u", "pre", NoAuthor}

VARIABLES
    next,       \* the WAL shard's allocator: the next offset it assigns
    author,     \* the author kind each allocated offset was allocated to
    hoOpen,     \* L's durable hand-off marker is open
    hoClosedAt, \* the per-partition maximum of the heads the marker's closes recorded
    windows,    \* hand-off windows opened so far
    fAdmit,     \* the initiator is admitting writes for L's range
    fpend,      \* a foreign write is admitted and not yet allocated or rejected
    fkind,      \* its kind
    fstamp,     \* the generation it was admitted under
    fEpoch,     \* the initiator's current generation
    fFence,     \* the WAL shard's durable entry for the initiator
    restarted,  \* the WAL shard has restarted
    gst,        \* another shard's walk: "idle", "pend", "repend", "rejected", "done"
    gstamp,     \* the tree generation the walk was stamped with
    closing,    \* a close is between its re-claim and its marker close; always
                \* FALSE here, because the close marks its intent closed atomically
    gate,       \* the cluster's fence gate is open
    hfly,       \* a hand-off begun before the gate opened, with no marker, is in flight
    resumed,    \* that hand-off has resumed under the gate
    mInit,      \* L's marker state is initialised
    eRow,       \* WithStaleDonor: the donor's durable epoch row; 0 while no activation has claimed
    liveArmed,  \* the splitting donor activation has claimed (always, outside pre-arm)
    sNarrowed,  \* the donor's narrowed leaf state is persisted
    dst,        \* a stale duplicate of the donor: "idle", "wrote", "claimed", "stale"
    dEpoch,     \* its claimed epoch
    dpend,      \* its epoch-only write for a moved key is admitted, not yet allocated
    dstamp,     \* the epoch that write carries
    bst,        \* the split's row bump: "idle", "read", "done"
    bRead,      \* the row value the bump read
    dRetried,   \* the claimant has re-run its claim after a StaleEpoch refusal
    lpend,      \* the live donor's write of a moved key, admitted before its narrow, not yet allocated
    lstamp,     \* the epoch that write carries
    lUsed,      \* the live donor has admitted that write
    relEpoch,   \* the epoch the live donor relinquished at its counted close; 0 none
    lEpoch,     \* the live donor's installed claim epoch; 0 none
    lCached,    \* the epoch row value the live donor's fence caches
    lClaim,     \* the epoch of the live donor's claim in flight; 0 none
    lAllow,     \* the live donor has used its one probed re-claim after a StaleEpoch
    lStamped,   \* ghost: the live donor has stamped an append after the split's bump
    lDead,      \* ghost: the live donor was superseded
    lStuck,     \* ghost: the live donor's current claim self-fences its own appends
    dupRaised,  \* ghost: a duplicate's claim has raised D's epoch row
    lBumped,    \* the live donor has taken its one floor-refusal bump
    lStaleLeaf, \* another activation has persisted D's state since the live donor loaded it
    lProbeFailed, \* ghost: a live donor's claim probe found its state stale
    dfStage,    \* WithDeferredClose: the shard root's lazy close, "none" or "raised"
    pend,       \* L's marker holds a deferred close head, not yet resolved
    pendGen     \* the routing generation that deferred head resolves at

lVars == <<lpend, lstamp, lUsed, relEpoch, lEpoch, lCached, lClaim, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
hdVars == <<dfStage, pend, pendGen>>
dVars == <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lVars, hdVars>>

vars == <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp,
          fEpoch, fFence, restarted, gst, gstamp, closing, gate, hfly, resumed, mInit, dVars>>

Max2(x, y) == IF x >= y THEN x ELSE y

TypeOK ==
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Authors]
    /\ hoOpen \in BOOLEAN
    /\ hoClosedAt \in 0..MaxOff
    /\ windows \in 0..MaxWindows
    /\ fAdmit \in BOOLEAN
    /\ fpend \in BOOLEAN
    /\ fkind \in Kinds
    /\ fstamp \in Epochs
    /\ fEpoch \in Epochs
    /\ fFence \in Epochs
    /\ restarted \in BOOLEAN
    /\ gst \in {"idle", "pend", "repend", "rejected", "done"}
    /\ gstamp \in Epochs
    /\ closing \in BOOLEAN
    /\ gate \in BOOLEAN
    /\ hfly \in BOOLEAN
    /\ resumed \in BOOLEAN
    /\ mInit \in BOOLEAN
    /\ eRow \in Epochs
    /\ liveArmed \in BOOLEAN
    /\ sNarrowed \in BOOLEAN
    /\ dst \in {"idle", "wrote", "claimed", "stale"}
    /\ dEpoch \in Epochs
    /\ dpend \in BOOLEAN
    /\ dstamp \in Epochs
    /\ bst \in {"idle", "read", "done"}
    /\ bRead \in Epochs
    /\ dRetried \in BOOLEAN
    /\ lpend \in BOOLEAN
    /\ lstamp \in Epochs
    /\ lUsed \in BOOLEAN
    /\ relEpoch \in Epochs
    /\ lEpoch \in Epochs
    /\ lCached \in Epochs
    /\ lClaim \in Epochs
    /\ lAllow \in BOOLEAN
    /\ lStamped \in BOOLEAN
    /\ lDead \in BOOLEAN
    /\ lStuck \in BOOLEAN
    /\ dupRaised \in BOOLEAN
    /\ lBumped \in BOOLEAN
    /\ lStaleLeaf \in BOOLEAN
    /\ lProbeFailed \in BOOLEAN
    /\ dfStage \in {"none", "raised"}
    /\ pend \in BOOLEAN
    /\ pendGen \in Epochs

(* A legacy hand-off, when switched on, may or may not be in flight at the *)
(* start; while it is, the gate is closed and L's marker is uninitialised. *)
Init ==
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ hoOpen = FALSE
    /\ hoClosedAt = 0
    /\ windows = 0
    /\ fAdmit = FALSE
    /\ fpend = FALSE
    /\ fkind = "point"
    /\ fstamp = 0
    /\ fEpoch = 1
    /\ fFence = 1
    /\ restarted = FALSE
    /\ gst = "idle"
    /\ gstamp = 0
    /\ closing = FALSE
    /\ hfly \in (IF WithLegacyHandOff THEN BOOLEAN ELSE {FALSE})
    /\ gate = ~hfly
    /\ resumed = FALSE
    /\ mInit = ~hfly
    /\ liveArmed \in (IF WithPreArm THEN BOOLEAN ELSE {TRUE})
    /\ eRow = IF liveArmed THEN 1 ELSE 0
    /\ sNarrowed = FALSE
    /\ dst = "idle"
    /\ dEpoch = 0
    /\ dpend = FALSE
    /\ dstamp = 0
    /\ bst = "idle"
    /\ bRead = 0
    /\ dRetried = FALSE
    /\ lpend = FALSE
    /\ lstamp = 0
    /\ lUsed = FALSE
    /\ relEpoch = 0
    /\ lEpoch = IF liveArmed THEN 1 ELSE 0
    /\ lCached = IF liveArmed THEN 1 ELSE 0
    /\ lClaim = 0
    /\ lAllow = FALSE
    /\ lStamped = FALSE
    /\ lDead = FALSE
    /\ lStuck = FALSE
    /\ dupRaised = FALSE
    /\ lBumped = FALSE
    /\ lStaleLeaf = FALSE
    /\ lProbeFailed = FALSE
    /\ dfStage = "none"
    /\ pend = FALSE
    /\ pendGen = 0

(***************************************************************************)
(* The foreign author.                                                     *)
(***************************************************************************)

(* ForeignOpen: under the gate, with no marker-less hand-off in flight,    *)
(* the initiator opens L's marker and starts admitting.                    *)
ForeignOpen ==
    /\ gate
    /\ ~hfly
    /\ ~hoOpen
    /\ ~fAdmit
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ fAdmit' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<next, author, hoClosedAt, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* ForeignAdmit(k): a write of kind k is admitted while the initiator      *)
(* admits, stamped with the generation current at admission.               *)
ForeignAdmit(k) ==
    /\ fAdmit
    /\ ~closing
    /\ ~fpend
    /\ next < MaxOff
    /\ fpend' = TRUE
    /\ fkind' = k
    /\ fstamp' = fEpoch
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* ForeignAlloc: the WAL shard allocates a stamped append at or above the *)
(* initiator's entry (E's entry check).                                    *)
ForeignAlloc ==
    /\ fpend
    /\ fstamp >= fFence
    /\ next < MaxOff
    /\ fpend' = FALSE
    /\ fkind' = "point"
    /\ fstamp' = 0
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = "f"]
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* ForeignReject: a stamped append below the entry is rejected, allocating *)
(* nothing; the write is retried at its seam as a fresh admission.         *)
ForeignReject ==
    /\ fpend
    /\ fstamp < fFence
    /\ fpend' = FALSE
    /\ fkind' = "point"
    /\ fstamp' = 0
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* The heads a close may record: any head up to the allocator's that lies  *)
(* above every stamped foreign append, the weakest E's claim head allows.  *)
CloseHeads == {h \in 0..next : \A o \in Offs : author[o] \in Stamped => o < h}

(* Close: the initiator stops admitting, bumps its generation and re-      *)
(* claims it, which raises its entry and reads the allocator head in one   *)
(* step (E's entry check), and closes L's marker recording that head. The  *)
(* marker max-merges it with the heads earlier closes recorded.            *)
Close ==
    /\ hoOpen
    /\ ~closing
    /\ fEpoch < MaxEpoch
    /\ fAdmit' = FALSE
    /\ fEpoch' = fEpoch + 1
    /\ fFence' = fEpoch + 1
    /\ \E h \in CloseHeads : hoClosedAt' = Max2(hoClosedAt, h)
    /\ hoOpen' = FALSE
    /\ UNCHANGED <<next, author, windows, fpend, fkind, fstamp, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ WithStaleDonor => bst = "done"
    /\ UNCHANGED dVars

(***************************************************************************)
(* The lazy key-moving close (#5833, #5839; WithDeferredClose, under       *)
(* WithPreArm). A split's or reclaim's close into L is two steps of the   *)
(* shard root's own turn (C6): CloseRaise stops admitting and raises the  *)
(* routing generation durably, with no WAL claim; CloseDefer then closes  *)
(* L's marker with a deferred head, the raised generation. The marker     *)
(* holds L while the head is pending. ResolveDeferred: L's arm re-claims  *)
(* the deferred generation at the partition, or keeps the loaded entry    *)
(* when a later raise was already re-claimed there (C5), and records the  *)
(* allocator head as the close head in the same WAL turn.                 *)
(***************************************************************************)
CloseRaise ==
    /\ WithDeferredClose
    /\ WithPreArm
    /\ hoOpen
    /\ ~closing
    /\ dfStage = "none"
    /\ ~pend
    /\ fEpoch < MaxEpoch
    /\ fAdmit' = FALSE
    /\ fEpoch' = fEpoch + 1
    /\ dfStage' = "raised"
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lVars, pend, pendGen>>

CloseDefer ==
    /\ dfStage = "raised"
    /\ hoOpen' = FALSE
    /\ pend' = TRUE
    /\ pendGen' = fEpoch
    /\ dfStage' = "none"
    /\ UNCHANGED <<next, author, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lVars>>

ResolveDeferred ==
    /\ pend
    /\ fFence' = Max2(fFence, pendGen)
    /\ hoClosedAt' = Max2(hoClosedAt, next)
    /\ pend' = FALSE
    /\ UNCHANGED <<next, author, hoOpen, windows, fAdmit, fpend, fkind, fstamp, fEpoch, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lVars, dfStage, pendGen>>

(* Restart (WithRestart): the WAL shard restarts, a reactivation or a      *)
(* move's cutover. The initiator's generation entry is a durable row and   *)
(* survives; the shard reloads it when the claim is re-presented.          *)
Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ UNCHANGED fFence
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(***************************************************************************)
(* Another shard's walk of a tree-level range delete (WithTreeWalk). It is *)
(* stamped only under an open intent, whose marker holds L, with the       *)
(* tree's range-delete generation that fEpoch stands for, so one re-claim  *)
(* (Close) fences every shard's walk at once.                              *)
(***************************************************************************)
GAdmit ==
    /\ WithTreeWalk
    /\ fAdmit
    /\ ~closing
    /\ gst = "idle"
    /\ gst' = "pend"
    /\ gstamp' = fEpoch
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

GAlloc ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp >= fFence
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = "g"]
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

GReject ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp < fFence
    /\ gst' = "rejected"
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* The seam retry of a rejected walk asks the fence grain, which answers   *)
(* from one state and marks an intent closed before its close's first      *)
(* re-claim. GRestamp: the walk's own intent, whose marker holds L, is     *)
(* open, so it re-stamps with the current generation. Reading L's marker   *)
(* over-approximates "its own intent" (rule 3). GUnstamped: otherwise it   *)
(* appends unstamped, admitted and reported.                               *)
GRestamp ==
    /\ gst = "rejected"
    /\ hoOpen
    /\ ~closing
    /\ gst' = "repend"
    /\ gstamp' = fEpoch
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

GUnstamped ==
    /\ gst = "rejected"
    /\ ~(hoOpen /\ ~closing)
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = "u"]
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(* OtherIntentClose (WithTreeWalk): another range delete's close re-claims *)
(* the tree's shared generation while the walk's own intent, whose marker  *)
(* holds L, stays open. It closes only its own intent's markers.           *)
OtherIntentClose ==
    /\ WithTreeWalk
    /\ hoOpen
    /\ ~closing
    /\ fEpoch < MaxEpoch
    /\ fEpoch' = fEpoch + 1
    /\ fFence' = fEpoch + 1
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

(***************************************************************************)
(* The rollout (WithLegacyHandOff). OpenGate: the last old silo has left.  *)
(* Resume: the marker-less hand-off resumes under the gate, opening its    *)
(* marker before its next admission. LegacyComplete: a resumed hand-off    *)
(* with no open marker and nothing pending completes; once it has closed   *)
(* by fence, that is its end. MarkInit: with no marker-less hand-off in    *)
(* flight, L's marker state is initialised; until then an absent marker    *)
(* holds L.                                                                *)
(***************************************************************************)
OpenGate ==
    /\ ~gate
    /\ gate' = TRUE
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, hfly, resumed, mInit>>
    /\ UNCHANGED dVars

Resume ==
    /\ hfly
    /\ gate
    /\ ~resumed
    /\ resumed' = TRUE
    /\ hoOpen' = TRUE
    /\ fAdmit' = TRUE
    /\ UNCHANGED <<next, author, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, mInit>>
    /\ UNCHANGED dVars

LegacyComplete ==
    /\ hfly
    /\ resumed
    /\ ~hoOpen
    /\ ~fpend
    /\ hfly' = FALSE
    /\ fAdmit' = FALSE
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, resumed, mInit>>
    /\ UNCHANGED dVars

MarkInit ==
    /\ ~hfly
    /\ ~mInit
    /\ mInit' = TRUE
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed>>
    /\ UNCHANGED dVars

(***************************************************************************)
(* The donor's epoch row across a split (#5841, WithStaleDonor). The       *)
(* foreign author here is a stale duplicate D' of the split's donor D, an  *)
(* activation that still holds the span from before the split. Its        *)
(* epoch-only writes (WalRoutingStamp.None: the PendingTx terminal         *)
(* backstop, the span-forward receive) of keys the split moved carry no    *)
(* routing generation, so only D's epoch fences them: they are allocated   *)
(* at or above D's durable epoch row (E's entry check) and rejected below. *)
(* The fix is a Dekker pair on two durable rows, with no new state:        *)
(*  - ClaimWrite / ClaimCheck: a claim writes row + 1, and only then reads *)
(*    D's leaf state; an ETag other than the one the activation loaded     *)
(*    marks it stale, and a stale activation makes no claim at any         *)
(*    partition and appends nothing epoch-stamped.                          *)
(*  - SplitPersist / SplitRowRead / SplitRowWrite: the split persists its  *)
(*    narrowed state, and only then reads D's epoch row; if the durable    *)
(*    row exists - whichever activation claimed - it writes row + 1,       *)
(*    conditionally on the value read (re-reading on a conflict), and      *)
(*    retires D's entries at every partition at that value.                *)
(* Either the claim's read sees the narrow, or the split's read sees the   *)
(* claim's row. Outside pre-arm the splitting activation has always        *)
(* claimed, since it wrote stamped before it split.                        *)
(***************************************************************************)
ClaimWrite ==
    /\ WithStaleDonor
    /\ dst = "idle"
    /\ eRow < MaxEpoch
    /\ dst' = "wrote"
    /\ dEpoch' = eRow + 1
    /\ eRow' = eRow + 1
    /\ dupRaised' = TRUE
    /\ UNCHANGED <<liveArmed, sNarrowed, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, relEpoch, lEpoch, lCached, lClaim, lAllow, lStamped, lDead, lStuck, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

ClaimCheck ==
    /\ dst = "wrote"
    /\ dst' = IF sNarrowed THEN "stale" ELSE "claimed"
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dEpoch, dpend, dstamp, bst, bRead, dRetried, lVars>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

DupAdmit ==
    /\ dst = "claimed"
    /\ ~dpend
    /\ next < MaxOff
    /\ dpend' = TRUE
    /\ dstamp' = dEpoch
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, bst, bRead, dRetried, lVars>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* An epoch-only write allocated before the split opened its window lies  *)
(* below the receiver's birth heads and is handed to it with the split's  *)
(* rows ("pre"); after, it is a foreign record of the receiver's ("d").   *)
DupAlloc ==
    /\ dpend
    /\ dstamp >= eRow
    /\ next < MaxOff
    /\ dpend' = FALSE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = IF windows > 0 THEN "d" ELSE "pre"]
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dstamp, bst, bRead, dRetried, lVars>>
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

DupReject ==
    /\ dpend
    /\ dstamp < eRow
    /\ dpend' = FALSE
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dstamp, bst, bRead, dRetried, lVars>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* The relinquish resets the live donor's cached row (R1): its next arm  *)
(* reads D's row fresh rather than writing on a stale ETag.               *)
RelinquishedCache == 0

SplitPersist ==
    /\ WithStaleDonor
    /\ hoOpen
    /\ ~sNarrowed
    /\ sNarrowed' = TRUE
    /\ IF WithPreArm /\ ~WithDonorFence /\ (liveArmed \/ lClaim > 0)
       THEN /\ liveArmed' = FALSE
            /\ relEpoch' = Max2(relEpoch, Max2(lEpoch, lClaim))
            /\ lEpoch' = 0
            /\ lCached' = RelinquishedCache
       ELSE UNCHANGED <<liveArmed, relEpoch, lEpoch, lCached>>
    /\ UNCHANGED <<eRow, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lpend, lstamp, lUsed, lClaim, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* DupRetry: an epoch-only write refused as StaleEpoch at the claimant's   *)
(* current epoch re-runs the probed claim, at most once per split close    *)
(* (#5840 S4, (a)): the live donor, superseded by the fence the shard root *)
(* ran at the link, re-claims instead of deactivating. The re-run is the   *)
(* same Dekker claim - a fresh row read and row + 1, then the leaf-state   *)
(* probe - so a stale duplicate it reaches is marked stale.                *)
DupRetry ==
    /\ WithStaleDonor
    /\ dst = "claimed"
    /\ dpend
    /\ dstamp < eRow
    /\ ~dRetried
    /\ dRetried' = TRUE
    /\ dpend' = FALSE
    /\ dst' = "idle"
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dEpoch, dstamp, bst, bRead, lVars>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* The live donor's write of a moved key, admitted before its narrow under *)
(* its claimed epoch (#5840 S4). LiveAlloc: allocated at or above D's      *)
(* entry. LiveRefuse: refused below it after the split's bump; a write at  *)
(* or below the epoch the donor relinquished at its counted close, or     *)
(* below its current epoch (R3, the <= form), is self-fenced and           *)
(* re-admitted at its seam, which finds the key moved and forwards it; any *)
(* other is foreign and supersedes it (MarkStale). Either way D allocates  *)
(* nothing for it. In the eager arm the split adopts the bumped row into   *)
(* the live activation (A3), so the stamp is below its current epoch.     *)
(* LiveBump: a floor refusal of the pending write, stamped under the live  *)
(* donor's current epoch, bumps D's row; the bump is in flight and the     *)
(* write is re-stamped at its epoch. A write stamped at or below the       *)
(* relinquished epoch is never bumped: its refusal self-fences.            *)
LiveBump ==
    /\ WithStaleDonor
    /\ WithPreArm
    /\ liveArmed
    /\ lpend
    /\ lstamp = lEpoch
    /\ lEpoch > relEpoch
    /\ lClaim = 0
    /\ ~lBumped
    /\ eRow < MaxEpoch
    /\ lBumped' = TRUE
    /\ lClaim' = eRow + 1
    /\ eRow' = eRow + 1
    /\ lstamp' = eRow + 1
    /\ UNCHANGED <<liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lUsed, relEpoch, lEpoch, lCached, lAllow, lStamped, lDead, lStuck, dupRaised, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveAdmit ==
    /\ WithStaleDonor
    /\ liveArmed
    /\ ~sNarrowed
    /\ ~lUsed
    /\ next < MaxOff
    /\ lUsed' = TRUE
    /\ lpend' = TRUE
    /\ lstamp' = lEpoch
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, relEpoch, lEpoch, lCached, lClaim, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveAlloc ==
    /\ lpend
    /\ lstamp >= eRow
    /\ next < MaxOff
    /\ lpend' = FALSE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = IF windows > 0 THEN "d" ELSE "pre"]
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lstamp, lUsed, relEpoch, lEpoch, lCached, lClaim, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveRefuse ==
    /\ lpend
    /\ lstamp < eRow
    /\ lpend' = FALSE
    /\ IF ~WithPreArm \/ lstamp <= relEpoch \/ lstamp < lEpoch
       THEN UNCHANGED lDead
       ELSE lDead' = TRUE
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried, lstamp, lUsed, relEpoch, lEpoch, lCached, lClaim, lAllow, lStamped, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* The live donor's claims across the split (#5840 S4, a5e2e2ef's R1-R4). *)
(* LiveClaimStart / LiveClaimDone: an arm's claim writes D's row before    *)
(* the count and installs on completion - unless the counted close has     *)
(* relinquished it meanwhile (R2), when it installs nothing and records  *)
(* its epoch as relinquished (R5, max-merged). The claim's probe runs on   *)
(* completion: after a relinquish another activation may have persisted D, *)
(* and a probe that finds the state stale marks the activation stale even  *)
(* when the claim is discarded (R6). LiveArm: after relinquishing, the live*)
(* donor arms again with a fresh row read, never its cached row (R1).      *)
(* LiveStamp: a stamped append under the installed claim; refused below    *)
(* D's entry at its current epoch, it re-runs the probed claim once,       *)
(* passing the probe (its state is current); a second refusal supersedes   *)
(* it. A refusal at or below the relinquished epoch self-fences. Epoch     *)
(* exhaustion disables the re-claim rather than superseding: a bound, not  *)
(* a behaviour.                                                            *)

LiveClaimStart ==
    /\ WithStaleDonor
    /\ WithPreArm
    /\ ~liveArmed
    /\ lClaim = 0
    /\ relEpoch = 0
    /\ eRow < MaxEpoch
    /\ lClaim' = eRow + 1
    /\ eRow' = eRow + 1
    /\ UNCHANGED <<liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, relEpoch, lEpoch, lCached, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveClaimDone ==
    /\ lClaim > 0
    /\ lClaim' = 0
    /\ \E stale \in IF relEpoch > 0 THEN BOOLEAN ELSE {FALSE} :
        /\ lStaleLeaf' = stale
        /\ CASE stale -> /\ lProbeFailed' = TRUE
                         /\ lDead' = TRUE
                         /\ relEpoch' = Max2(relEpoch, lClaim)
                         /\ UNCHANGED <<liveArmed, lEpoch, lCached>>
             [] relEpoch = 0 -> /\ liveArmed' = TRUE
                                /\ lEpoch' = lClaim
                                /\ lCached' = lClaim
                                /\ UNCHANGED <<relEpoch, lDead, lProbeFailed>>
             [] OTHER -> /\ relEpoch' = Max2(relEpoch, lClaim)
                         /\ UNCHANGED <<liveArmed, lEpoch, lCached, lDead, lProbeFailed>>
    /\ UNCHANGED <<eRow, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, lAllow, lStamped, lStuck, dupRaised, lBumped>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveArm ==
    /\ WithStaleDonor
    /\ WithPreArm
    /\ relEpoch > 0
    /\ ~liveArmed
    /\ lClaim = 0
    /\ ~lDead
    /\ eRow < MaxEpoch
    /\ CASE lStaleLeaf -> /\ lDead' = TRUE
                          /\ lProbeFailed' = TRUE
                          /\ eRow' = eRow + 1
                          /\ UNCHANGED <<liveArmed, lEpoch, lCached>>
         [] ~lStaleLeaf /\ (lCached = 0 \/ lCached = eRow) -> /\ eRow' = eRow + 1
                                             /\ liveArmed' = TRUE
                                             /\ lEpoch' = eRow + 1
                                             /\ lCached' = eRow + 1
                                             /\ UNCHANGED <<lDead, lProbeFailed>>
         [] OTHER -> /\ lDead' = TRUE
                     /\ UNCHANGED <<eRow, liveArmed, lEpoch, lCached, lProbeFailed>>
    /\ UNCHANGED <<sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, relEpoch, lClaim, lAllow, lStamped, lStuck, dupRaised, lBumped, lStaleLeaf>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

LiveStamp ==
    /\ WithStaleDonor
    /\ liveArmed
    /\ ~lDead
    /\ ~lStuck
    /\ ~lStamped
    /\ bst = "done"
    /\ CASE lEpoch >= eRow -> /\ lStamped' = TRUE
                              /\ UNCHANGED <<eRow, lEpoch, lCached, lAllow, lDead, lStuck>>
         [] lEpoch <= relEpoch -> /\ lStuck' = TRUE
                                  /\ UNCHANGED <<eRow, lEpoch, lCached, lAllow, lDead, lStamped>>
         [] ~lAllow -> /\ eRow < MaxEpoch
                       /\ lAllow' = TRUE
                       /\ eRow' = eRow + 1
                       /\ lEpoch' = eRow + 1
                       /\ lCached' = eRow + 1
                       /\ UNCHANGED <<lDead, lStuck, lStamped>>
         [] OTHER -> /\ lDead' = TRUE
                     /\ UNCHANGED <<eRow, lEpoch, lCached, lAllow, lStuck, lStamped>>
    /\ UNCHANGED <<liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bst, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, relEpoch, lClaim, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* The split's bump: whenever D's durable row exists. Under rule S        *)
(* (#5837, WithDonorFence) the pre-arm fence is the donor's own, run in   *)
(* its close (the adopting FenceSplitAsync): it keeps its claim, no       *)
(* relinquish, and adopts the bumped row into the live activation (A3).   *)
SplitBumps(read) == read > 0

SplitRowRead ==
    /\ WithStaleDonor
    /\ hoOpen
    /\ sNarrowed
    /\ bst = "idle"
    /\ bst' = "read"
    /\ bRead' = eRow
    /\ UNCHANGED <<eRow, liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, dRetried, lVars>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

SplitRowWrite ==
    /\ bst = "read"
    /\ IF eRow = bRead
       THEN /\ eRow' = IF SplitBumps(bRead) THEN bRead + 1 ELSE bRead
            /\ bst' = "done"
       ELSE /\ bst' = "idle"
            /\ UNCHANGED eRow
    /\ eRow' <= MaxEpoch
    /\ IF WithDonorFence /\ liveArmed /\ eRow = bRead /\ SplitBumps(bRead)
       THEN /\ lEpoch' = bRead + 1
            /\ lCached' = bRead + 1
       ELSE UNCHANGED <<lEpoch, lCached>>
    /\ UNCHANGED <<liveArmed, sNarrowed, dst, dEpoch, dpend, dstamp, bRead, dRetried>>
    /\ UNCHANGED <<lpend, lstamp, lUsed, relEpoch, lClaim, lAllow, lStamped, lDead, lStuck, dupRaised, lBumped, lStaleLeaf, lProbeFailed>>
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>
    /\ UNCHANGED hdVars

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ ForeignOpen
    \/ \E k \in Kinds : ForeignAdmit(k)
    \/ ForeignAlloc
    \/ ForeignReject
    \/ Close
    \/ CloseRaise
    \/ CloseDefer
    \/ ResolveDeferred
    \/ Restart
    \/ GAdmit
    \/ GAlloc
    \/ GReject
    \/ GRestamp
    \/ GUnstamped
    \/ OtherIntentClose
    \/ OpenGate
    \/ Resume
    \/ LegacyComplete
    \/ MarkInit
    \/ ClaimWrite
    \/ ClaimCheck
    \/ SplitPersist
    \/ DupAdmit
    \/ DupAlloc
    \/ DupReject
    \/ DupRetry
    \/ LiveAdmit
    \/ LiveAlloc
    \/ LiveRefuse
    \/ LiveBump
    \/ LiveClaimStart
    \/ LiveClaimDone
    \/ LiveArm
    \/ LiveStamp
    \/ SplitRowRead
    \/ SplitRowWrite
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The guarantees, each a checked invariant, and what the bulk-switch and  *)
(* core modules assume of the hand-off fence.                              *)
(***************************************************************************)

(* L's marker reads closed: its state is initialised, no window is open,  *)
(* and no deferred close head is pending. Only then may L arm past a       *)
(* foreign record.                                                         *)
MarkerReadsClosed == mInit /\ ~hoOpen /\ ~pend

(* Whenever L's marker reads closed, every stamped foreign append lies     *)
(* below the head its last close recorded: a stamped foreign offset is     *)
(* allocated only while the marker is open, or below that head, whatever   *)
(* its kind (point, batch, another shard's walk) or path (re-stamped, a    *)
(* restart in between).                                                    *)
HandOffBound ==
    MarkerReadsClosed => \A o \in Offs : author[o] \in Stamped => o < hoClosedAt

(* The live donor's progress across the split's fence (#5840 S4, R1-R7): *)
(* unless a stale duplicate's claim raised D's row, or another activation  *)
(* persisted D so that the live donor is itself stale, the live donor is   *)
(* never superseded and never left self-fencing its own appends.           *)
LiveDonorProgress == WithStaleDonor => ((lDead \/ lStuck) => (dupRaised \/ lStaleLeaf))

(* A probe that finds the live donor's state stale marks it stale, even   *)
(* when the relinquish discards the claim it ran in (#5840 S4, R6): a     *)
(* real #5841 detection is never swallowed.                               *)
StaleDetectionHonoured == lProbeFailed => lDead

(* The lazy close raises before it defers (#5839): a pending deferred head *)
(* never names a generation above the shard root's durable raised one, so *)
(* a crash between the two steps never leaves L to re-claim a generation   *)
(* the shard root has not raised and later admits under.                  *)
DeferredHeadAtRaisedGeneration == pend => pendGen <= fEpoch

(* L's marker state is initialised only while no hand-off begun without a  *)
(* marker is in flight, so an absent or uninitialised marker holds L.      *)
AbsentMarkerHolds == mInit => ~hfly

(* The heads L's marker requires never decrease: every close is max-merged *)
(* with the heads earlier closes recorded, so a late close with lower      *)
(* heads cannot release an owner an earlier close holds.                   *)
CloseHeadMonotone == [][hoClosedAt' >= hoClosedAt]_vars
=============================================================================
