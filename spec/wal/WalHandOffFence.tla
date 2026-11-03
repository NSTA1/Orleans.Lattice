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
CONSTANTS WithBatch, MaxWindows, WithRestart, WithTreeWalk, WithLegacyHandOff

MaxOff == 3
Offs == 0..(MaxOff - 1)
Kinds == IF WithBatch THEN {"point", "batch"} ELSE {"point"}
MaxEpoch == 3
Epochs == 0..MaxEpoch

(* The author of an allocated offset: a stamped foreign write ("f"), a     *)
(* stamped range-delete walk of another shard ("g"), or an unstamped walk *)
(* ("u"), which is admitted and reported, never fenced.                    *)
NoAuthor == "none"
Stamped == {"f", "g"}
Authors == Stamped \cup {"u", NoAuthor}

VARIABLES
    next,       \* the WAL shard's allocator: the next offset it assigns
    author,     \* the author kind each allocated offset was allocated to
    hoOpen,     \* L's durable hand-off marker is open
    hoClosedAt, \* the allocator head the marker's last close recorded
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
    mInit       \* L's marker state is initialised

vars == <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp,
          fEpoch, fFence, restarted, gst, gstamp, closing, gate, hfly, resumed, mInit>>

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

(* Close: the initiator stops admitting, bumps its generation and re-      *)
(* claims it, which raises its entry and reads the allocator head in one   *)
(* step (E's entry check), and closes L's marker recording that head.      *)
Close ==
    /\ hoOpen
    /\ ~closing
    /\ fEpoch < MaxEpoch
    /\ fAdmit' = FALSE
    /\ fEpoch' = fEpoch + 1
    /\ fFence' = fEpoch + 1
    /\ hoClosedAt' = next
    /\ hoOpen' = FALSE
    /\ UNCHANGED <<next, author, windows, fpend, fkind, fstamp, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed, mInit>>

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

GAlloc ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp >= fFence
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = "g"]
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>

GReject ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp < fFence
    /\ gst' = "rejected"
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>

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

GUnstamped ==
    /\ gst = "rejected"
    /\ ~(hoOpen /\ ~closing)
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = "u"]
    /\ UNCHANGED <<hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gstamp, closing, gate, hfly, resumed, mInit>>

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

Resume ==
    /\ hfly
    /\ gate
    /\ ~resumed
    /\ resumed' = TRUE
    /\ hoOpen' = TRUE
    /\ fAdmit' = TRUE
    /\ UNCHANGED <<next, author, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, mInit>>

LegacyComplete ==
    /\ hfly
    /\ resumed
    /\ ~hoOpen
    /\ ~fpend
    /\ hfly' = FALSE
    /\ fAdmit' = FALSE
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, resumed, mInit>>

MarkInit ==
    /\ ~hfly
    /\ ~mInit
    /\ mInit' = TRUE
    /\ UNCHANGED <<next, author, hoOpen, hoClosedAt, windows, fAdmit, fpend, fkind, fstamp, fEpoch, fFence, restarted>>
    /\ UNCHANGED <<gst, gstamp, closing, gate, hfly, resumed>>

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ ForeignOpen
    \/ \E k \in Kinds : ForeignAdmit(k)
    \/ ForeignAlloc
    \/ ForeignReject
    \/ Close
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
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The guarantees, each a checked invariant, and what the bulk-switch and  *)
(* core modules assume of the hand-off fence.                              *)
(***************************************************************************)

(* L's marker reads closed: its state is initialised and no window is      *)
(* open. Only then may L arm past a foreign record.                        *)
MarkerReadsClosed == mInit /\ ~hoOpen

(* Whenever L's marker reads closed, every stamped foreign append lies     *)
(* below the head its last close recorded: a stamped foreign offset is     *)
(* allocated only while the marker is open, or below that head, whatever   *)
(* its kind (point, batch, another shard's walk) or path (re-stamped, a    *)
(* restart in between).                                                    *)
HandOffBound ==
    MarkerReadsClosed => \A o \in Offs : author[o] \in Stamped => o < hoClosedAt

(* L's marker state is initialised only while no hand-off begun without a  *)
(* marker is in flight, so an absent or uninitialised marker holds L.      *)
AbsentMarkerHolds == mInit => ~hfly
=============================================================================
