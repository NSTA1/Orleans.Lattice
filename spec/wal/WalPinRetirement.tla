-------------------------- MODULE WalPinRetirement --------------------------
(***************************************************************************)
(* An abstract TLA+ specification of how the Orleans.Lattice WAL garbage   *)
(* collector retires a leaf's durable materialiser pin, and what that       *)
(* retirement lets the trim do.                                            *)
(*                                                                         *)
(* A leaf publishes a durable pin under its consumer id into a pin store   *)
(* spread over routing keys; the trim floor is a minimum over every pin it *)
(* finds. When the leaf dies (an empty-leaf fold, a purge, an undone       *)
(* resize's discarded copy), its pin must go, or it holds the WAL for good. *)
(* The leaf retires its own pins at its terminal seam, and the GC repairs  *)
(* the pins that seam missed: a bulk orphan sweep that enumerates the pin  *)
(* store key by key, reads the pinned leaf's durable row without           *)
(* activating it, and removes the pins of a leaf whose row is a husk; and  *)
(* a starvation drive that reports NotDriven for such a leaf. Both run as  *)
(* separate, non-atomic storage steps, two sweepers can race (each silo's *)
(* local loop and the tree's coordinator grain), and the same leaf grain   *)
(* id can be populated again after a purge or a fold cleared its row.     *)
(*                                                                         *)
(* The design checked here is the one that fences the removal: a sweeper  *)
(* takes a token from each pin-store key as it enumerates it, and a       *)
(* removal is refused at a key where the consumer's pin was merged again  *)
(* after that token was taken (issue #5358). Without the fence a purge-    *)
(* cleared leaf re-created between the sweeper's husk read and its removal *)
(* has the block pin its birth seeded deleted, and the trim passes writes  *)
(* it has not checkpointed (mutations NoTrimPastLiveNeedUnfencedRemoval    *)
(* and NoTrimPastLiveNeedDriveRemovesOnProbe reproduce it).                *)
(*                                                                         *)
(* The leaf's replay and checkpoint machinery, the snapshot, flush order   *)
(* and crash recovery are WalDurability's: a checkpoint here stands for   *)
(* the durable coverage that module proves honest, and the floor, block   *)
(* pin and trim are its abstraction over one partition. See                *)
(* PinRetirementRefinement.md for the mapping to production and for what   *)
(* this model does NOT cover.                                              *)
(*                                                                         *)
(* Epic #5348, issue #5349.                                                *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, TLC

(***************************************************************************)
(* Model instance. One leaf grain L of one tree publishes under consumer   *)
(* id cL; its pins may sit under two routing keys (a routing change        *)
(* between publications duplicates them). When Foreign holds, cX is a pin  *)
(* of a live consumer of the same tree whose id the removal gate cannot    *)
(* read as a leaf's own (issue #4238): parsed, it names L's grain, but L   *)
(* did not publish it; only the gate's mutation sets it. Process 0 is the  *)
(* starvation drive's retirement; processes 1..NSweepers are orphan        *)
(* sweepers. MaxInc bounds how many times L's grain id is populated again  *)
(* after its row was cleared. MaxFaults bounds the environment faults - a *)
(* routing change, an unreadable leaf read, an abandoned run - and is     *)
(* zero in the base: the OneFault variant checks the safety properties    *)
(* with one.                                                               *)
(***************************************************************************)
Keys == {"k1", "k2"}
\* The order the sweep enumerates them in (EnumerateReadKeys).
KeyOrder == <<"k1", "k2">>
\* Whether the foreign consumer cX is in the instance (see Cons).
Foreign == FALSE
Cons == IF Foreign THEN {"cL", "cX"} ELSE {"cL"}
MaxOff == 2
Offs == 0..(MaxOff - 1)
Pos == -1..(MaxOff - 1)
MaxInc == 1
MaxFaults == 0
NSweepers == 1
Drive == 0
Procs == 0..NSweepers

\* The pin store's "no entry" value; -1 is a real entry, the Zero block pin.
NoPin == -2

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* The removal gate's verdict on a consumer id under the tree's registry-pinned
\* partition count (WalFloorHolderReader.ClassifyConsumerId).
IdVerdict(c) == IF c = "cL" THEN "LeafPublished" ELSE "AmbiguousPartition"

(***************************************************************************)
(* State.                                                                  *)
(*                                                                         *)
(* WAL (one partition): next, the next offset; tail, the oldest readable   *)
(* offset. Every offset below next is an acknowledged write of L.          *)
(*                                                                         *)
(* Leaf grain L:                                                           *)
(*  phase     "live" (row bound to the tree), "deleted" (tree soft-deleted,*)
(*            row kept, recoverable), "marked" (a purge's durable marker   *)
(*            committed, row not yet cleared), "draining" (an empty-leaf   *)
(*            fold moved its rows away, row still bound), "husk" (row      *)
(*            cleared: no record or no tree id), "discarded" (an undone    *)
(*            resize's copy that can never be recovered).                  *)
(*  inc       the incarnation: how often L's id was populated again after  *)
(*            its row was cleared.                                         *)
(*  seeded    this incarnation's birth block pin has landed; L takes no    *)
(*            write before it.                                             *)
(*  cp        this incarnation's durable checkpoint: every write at or     *)
(*            below it is held by L's durable state (WalDurability proves *)
(*            the coverage honest).                                        *)
(*  need      the writes this incarnation must still replay from the WAL:  *)
(*            acknowledged above its checkpoint. A purge's clear (which    *)
(*            deletes them by design), a fold (which moved them to a       *)
(*            survivor) and a discard (which abandons the copy) end it.    *)
(*  purgeMark whether L's row record admits re-creating a rowless L: the   *)
(*            purge marker (PurgeCleared), or no record at all once a      *)
(*            fold's clear deleted it (AdmitCreateIntentAsync admits a     *)
(*            create intent when neither record nor snapshot survives).    *)
(*  owed      the pin-store keys L's terminal seam has still to retire.    *)
(*  route     the key L's pins are routed to now.                          *)
(*                                                                         *)
(* Tree-level purge tail: tailOwed, the completed purge still owes its     *)
(* registry removal, and with it the tail before it (the WAL trim and the  *)
(* pin-store clear); tailLin, the lineage of the registry entry the purge  *)
(* recorded with its completion. A registry entry's lineage is modelled by *)
(* the incarnation: every re-population registers a new one.               *)
(*                                                                         *)
(* Pin store: pinOff[k, c], the entry under key k for consumer c (NoPin,   *)
(* the Zero block pin -1, or an offset), merged by monotone max; pinInc[k], *)
(* a ghost: the incarnation of L that last merged into cL's entry at k.    *)
(*                                                                         *)
(* Retirement process p: pc[p]; seen[p], the keys enumerated (each yields *)
(* a token); found[p][c], the keys c's pin was found under and not yet     *)
(* decided; verdict[p], what the leaf read returned; touched[p], the keys *)
(* whose token a later merge of cL invalidated.                            *)
(***************************************************************************)
VARIABLES next, tail,
          phase, inc, seeded, cp, need, purgeMark, owed, route,
          pinOff, pinInc,
          pc, seen, found, verdict, touched,
          faults, tailOwed, tailLin

walVars  == <<next, tail>>
leafVars == <<phase, inc, seeded, cp, need, purgeMark, owed, route>>
pinVars  == <<pinOff, pinInc>>
procVars == <<pc, seen, found, verdict, touched>>
tailVars == <<tailOwed, tailLin>>
vars == <<next, tail,
          phase, inc, seeded, cp, need, purgeMark, owed, route,
          pinOff, pinInc,
          pc, seen, found, verdict, touched,
          faults, tailOwed, tailLin>>

Phases == {"live", "deleted", "marked", "draining", "husk", "discarded"}

TypeOK ==
   /\ next \in 0..MaxOff
   /\ tail \in 0..MaxOff
   /\ phase \in Phases
   /\ inc \in 0..MaxInc
   /\ seeded \in BOOLEAN
   /\ cp \in Pos
   /\ need \subseteq Offs
   /\ purgeMark \in BOOLEAN
   /\ owed \subseteq Keys
   /\ route \in Keys
   /\ pinOff \in [Keys \X Cons -> {NoPin} \cup Pos]
   /\ pinInc \in [Keys -> 0..MaxInc]
   /\ pc \in [Procs -> {"idle", "enum", "read", "decide"}]
   /\ seen \in [Procs -> SUBSET Keys]
   /\ found \in [Procs -> [Cons -> SUBSET Keys]]
   /\ verdict \in [Procs -> {"none", "husk", "live", "unreadable"}]
   /\ touched \in [Procs -> SUBSET Keys]
   /\ faults \in 0..MaxFaults
   /\ tailOwed \in BOOLEAN
   /\ tailLin \in 0..MaxInc

(***************************************************************************)
(* Derived views.                                                          *)
(*                                                                         *)
(* Present: a pin-store entry exists. LeafPinned: some key holds cL.      *)
(* Blocking and Floor are WalDurability's: a Zero block pin stops every    *)
(* trim, and otherwise the trim may reach the minimum published offset.   *)
(***************************************************************************)
Present(k, c) == pinOff[k, c] # NoPin

LeafPinned == \E k \in Keys : Present(k, "cL")

Blocking == \E kc \in Keys \X Cons : pinOff[kc] = -1

Floor == Min({pinOff[kc] : kc \in {x \in Keys \X Cons : pinOff[x] >= 0}} \cup {MaxOff})

\* The publisher of the entry at k is gone: an older incarnation that the
\* current one has not merged into, or the current one with its row cleared.
PublisherGone(k, c) == c = "cL" /\ (pinInc[k] < inc \/ phase = "husk")

\* Merge one report of cL into the entry at k (WalMaterialiserPinGrain.Merge:
\* monotone max on the offset, so a block seeded into a surviving entry
\* changes nothing there), and record the merge against every token already
\* taken from k (the fence, issue #5358).
MergeLeafPin(k, v) ==
    /\ pinOff' = [pinOff EXCEPT ![k, "cL"] = IF @ = NoPin THEN v ELSE Max({@, v})]
    /\ pinInc' = [pinInc EXCEPT ![k] = inc]
    /\ touched' = [p \in Procs |-> touched[p] \cup ({k} \cap found[p]["cL"])]

Init ==
    /\ next = 0
    /\ tail = 0
    \* L is born bound to the tree with its block pin seeded at its route.
    /\ phase = "live"
    /\ inc = 0
    /\ seeded = TRUE
    /\ cp = -1
    /\ need = {}
    /\ purgeMark = FALSE
    /\ owed = {}
    /\ route = "k1"
    /\ pinOff = [kc \in Keys \X Cons |->
                    IF kc = <<"k1", "cL">> THEN -1
                    ELSE IF kc = <<"k1", "cX">> THEN MaxOff - 1
                    ELSE NoPin]
    /\ pinInc = [k \in Keys |-> 0]
    /\ pc = [p \in Procs |-> "idle"]
    /\ seen = [p \in Procs |-> {}]
    /\ found = [p \in Procs |-> [c \in Cons |-> {}]]
    /\ verdict = [p \in Procs |-> "none"]
    /\ touched = [p \in Procs |-> {}]
    /\ faults = 0
    /\ tailOwed = FALSE
    /\ tailLin = 0

(***************************************************************************)
(* The leaf.                                                               *)
(*                                                                         *)
(* Write: a write to a live, seeded L is acknowledged at the next offset.  *)
(* Checkpoint: L durably covers everything written so far. Publish: L      *)
(* merges its checkpoint into its pin at its current route; a report that *)
(* would change nothing there is not modelled, which only drops fence      *)
(* taints and so lets more removals through. Seed: a newly (re-)created   *)
(* incarnation's birth seam merges a Zero block pin at its route before   *)
(* any write reaches it (SeedDurableMaterialiserBlockPinAsync, after       *)
(* SetTreeIdAsync persisted the tree id).                                  *)
(***************************************************************************)
Write ==
    /\ phase = "live"
    /\ seeded
    /\ next < MaxOff
    /\ need' = need \cup {next}
    /\ next' = next + 1
    /\ UNCHANGED tail
    /\ UNCHANGED <<phase, inc, seeded, cp, purgeMark, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Checkpoint ==
    /\ phase = "live"
    /\ cp < next - 1
    /\ cp' = next - 1
    /\ need' = {}
    /\ UNCHANGED walVars
    /\ UNCHANGED <<phase, inc, seeded, purgeMark, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Publish ==
    /\ phase = "live"
    /\ seeded
    /\ cp >= 0
    /\ pinOff[route, "cL"] < cp
    /\ MergeLeafPin(route, cp)
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED <<pc, seen, found, verdict>>
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Seed ==
    /\ phase = "live"
    /\ ~seeded
    /\ seeded' = TRUE
    /\ MergeLeafPin(route, -1)
    /\ UNCHANGED walVars
    /\ UNCHANGED <<phase, inc, cp, need, purgeMark, owed, route>>
    /\ UNCHANGED <<pc, seen, found, verdict>>
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

(***************************************************************************)
(* RouteChange: the pin store's routing for L's consumer id changes        *)
(* between publications (the shard count WalMaterialiserPinRouting resolves *)
(* changed), so later merges land under the other key and the old entry   *)
(* stays where it was: one logical pin duplicated across keys. An          *)
(* environment event, charged to the fault budget, with no guard on L's    *)
(* state: production constrains it no further.                             *)
(***************************************************************************)
RouteChange ==
    /\ faults < MaxFaults
    /\ route' \in Keys \ {route}
    /\ faults' = faults + 1
    /\ UNCHANGED walVars
    /\ UNCHANGED <<phase, inc, seeded, cp, need, purgeMark, owed>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED tailVars

(***************************************************************************)
(* Tree and leaf lifecycle.                                                *)
(*                                                                         *)
(* SoftDelete / RecoverTree: the tree is soft-deleted and recovered; L's   *)
(* row and data survive both. RecoverTree also hands back a leaf a purge   *)
(* marked but never cleared (RecoverBindingAsync resets the marker), so    *)
(* its writes are still needed. MarkPurge: the purge commits to L, making *)
(* its marker durable first. ClearRow: the purge clears L's row - its      *)
(* writes are deleted by design - and only then retires the pins it named *)
(* (issue #4700); a fold's clear runs after its pins are retired. Fold:    *)
(* an empty-leaf fold has moved L's rows to a survivor (outside this       *)
(* module, see spec/bplustree) and begins retiring L's pins.               *)
(***************************************************************************)
SoftDelete ==
    /\ phase = "live"
    /\ phase' = "deleted"
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, need, purgeMark, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

RecoverTree ==
    /\ phase \in {"deleted", "marked"}
    /\ phase' = "live"
    /\ purgeMark' = FALSE
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, need, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

MarkPurge ==
    /\ phase = "deleted"
    /\ phase' = "marked"
    /\ purgeMark' = TRUE
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, need, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Fold ==
    /\ phase = "live"
    /\ phase' = "draining"
    /\ need' = {}
    /\ owed' = Keys
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, purgeMark, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

ClearRow ==
    /\ \/ /\ phase = "marked"
          /\ need' = {}
          /\ owed' = Keys
          \* The purge's walk completes with this, its only leaf: the tail is
          \* owed, with the lineage of the entry it will remove.
          /\ tailOwed' = TRUE
          /\ tailLin' = inc
       \/ /\ phase = "draining"
          /\ owed = {}
          /\ UNCHANGED <<need, owed>>
          /\ UNCHANGED tailVars
    /\ phase' = "husk"
    \* A purge's clear keeps the record, marked; a fold's deletes it. Either
    \* admits re-creating the rowless leaf (AdmitCreateIntentAsync).
    /\ purgeMark' = TRUE
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults

(***************************************************************************)
(* SelfRetire(k): L's terminal seam removes its own pin under key k        *)
(* (ILeafCursorReporter.UnregisterAsync fans out to every read key). It is *)
(* the publisher retiring itself, inside the clear's own turn, so it is    *)
(* not fenced. SelfRetireFail(k): that removal faults, or the activation   *)
(* is lost part-way; the failure is swallowed and the pin is left to the  *)
(* GC. An environment event, not charged to the fault budget: each owed   *)
(* key can fail at most once, so it is finite without one, and it is what *)
(* leaves the orphans the GC exists to retire.                             *)
(***************************************************************************)
SelfRetire(k) ==
    /\ k \in owed
    /\ pinOff' = [pinOff EXCEPT ![k, "cL"] = NoPin]
    /\ owed' = owed \ {k}
    /\ UNCHANGED walVars
    /\ UNCHANGED <<phase, inc, seeded, cp, need, purgeMark, route>>
    /\ UNCHANGED pinInc
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

SelfRetireFail(k) ==
    /\ k \in owed
    /\ owed' = owed \ {k}
    /\ UNCHANGED walVars
    /\ UNCHANGED <<phase, inc, seeded, cp, need, purgeMark, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

(***************************************************************************)
(* Recreate: L's grain id is populated again after its row was cleared - a *)
(* recovery re-binding a purge-cleared leaf, a reused tree id re-seeding a *)
(* deterministic shard root leaf, a bulk load re-using a leaf id. A        *)
(* rowless leaf is admitted only when its row record shows the clear: the *)
(* purge marker (AdmitPurgeClearedAsync, AdmitCreateIntentAsync), or no    *)
(* record at all after a fold's clear deleted it (AdmitCreateIntentAsync,  *)
(* with no snapshot surviving). It binds the tree id before seeding its    *)
(* block pin. The dying turn's own retirement has ended first: the clear   *)
(* latches the activation against any later row write (_leafStateCleared) *)
(* and deactivates it, so the re-creation runs on a later activation; a    *)
(* duplicate activation or a late-delivered removal from a lost one is a   *)
(* stated abstraction gap. Nothing else gates it, and in particular        *)
(* nothing waits for an orphan sweep in flight.                            *)
(***************************************************************************)
Recreate ==
    /\ phase = "husk"
    /\ purgeMark
    /\ owed = {}
    /\ inc < MaxInc
    /\ phase' = "live"
    /\ inc' = inc + 1
    /\ seeded' = FALSE
    /\ cp' = -1
    /\ purgeMark' = FALSE
    /\ UNCHANGED walVars
    /\ UNCHANGED <<need, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

(***************************************************************************)
(* Discard: a soft-deleted tree that is an undone resize's retired copy    *)
(* which no resize can recover is discarded (TryDiscardAbandonedCopyAsync  *)
(* asks the deletion grain, DiscardIfAbandonedDerivedCopyAsync decides);   *)
(* recovery refuses a discarded copy, so its writes are abandoned.         *)
(* DiscardRetire: the GC's deleted-tree hold retires every pin of a        *)
(* discarded tree (UnregisterTreeAsync), whatever its consumer.            *)
(***************************************************************************)
Discard ==
    /\ phase = "deleted"
    /\ phase' = "discarded"
    /\ need' = {}
    /\ UNCHANGED walVars
    /\ UNCHANGED <<inc, seeded, cp, purgeMark, owed, route>>
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

DiscardRetire ==
    /\ phase = "discarded"
    /\ \E kc \in Keys \X Cons : pinOff[kc] # NoPin
    /\ pinOff' = [kc \in Keys \X Cons |-> NoPin]
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinInc
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

(***************************************************************************)
(* PurgeTailRemove / PurgeTailSettle: a completed purge's tree-level tail  *)
(* (TreeDeletionGrain.FinishCompletedPurgeAsync): it trims every WAL       *)
(* partition through its head, clears the tree's pin store (pins before   *)
(* the registry entry, issue #5416) and removes the registry entry, which *)
(* frees the id, then records the removal - or the write recording it, or *)
(* the loop stop before it, throws and the record stays owed. A re-drive  *)
(* then runs the tail again, so PurgeTailRemove may run any number of     *)
(* times until the record lands: an over-approximation, since production  *)
(* re-drives only after a fault. The tail acts only while the registry    *)
(* entry still carries the lineage the purge recorded (issue #5417): once *)
(* the id was populated again under a new lineage, the re-drive only      *)
(* records the removal (PurgeTailSettle). A re-drive that finds the entry *)
(* gone and not reused also only records it; acting again there is        *)
(* harmless, so it is folded into PurgeTailRemove.                         *)
(***************************************************************************)
PurgeTailRemove ==
    /\ tailOwed
    /\ tailLin = inc
    /\ tail' = IF next > tail THEN next ELSE tail
    /\ pinOff' = [kc \in Keys \X Cons |-> NoPin]
    /\ tailOwed' \in BOOLEAN
    /\ UNCHANGED tailLin
    /\ UNCHANGED next
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinInc
    /\ UNCHANGED procVars
    /\ UNCHANGED faults

PurgeTailSettle ==
    /\ tailOwed
    /\ tailLin # inc
    /\ tailOwed' = FALSE
    /\ UNCHANGED tailLin
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults

(***************************************************************************)
(* GcTrim: WalDurability's trim. No trim while any block pin stands;       *)
(* otherwise every offset at or below the floor may go. It ignores the     *)
(* in-memory cursors production also consults, which over-approximates    *)
(* the trim: that registry is per silo and lasts until an unregister or a *)
(* silo restart, and a GC pass on any silo not holding a leaf's cursor    *)
(* trims the shared WAL without seeing it.                                *)
(***************************************************************************)
GcTrim ==
    /\ ~Blocking
    /\ \E t \in (tail + 1)..next :
         /\ t - 1 <= Floor
         /\ tail' = t
    /\ UNCHANGED next
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED procVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

(***************************************************************************)
(* Retirement processes.                                                   *)
(*                                                                         *)
(* Start(p): a sweep begins (SweepOrphanedMaterialiserPinsCoreAsync, from  *)
(* the local loop or the tree's WalOrphanPinSweepGrain), or the            *)
(* starvation drive has activated L for its blocking pin and been told     *)
(* NotDriven - no tree id bound - which starts the drive's retirement of   *)
(* that one consumer (RetireOrphanedPinAsync). Every token and verdict of *)
(* an earlier run is dropped.                                              *)
(*                                                                         *)
(* Enum(p, k): read the entries under key k and take its token. The drive *)
(* looks only for its own consumer.                                        *)
(*                                                                         *)
(* Read(p): read L's durable row once, without activating it, after every *)
(* token was taken. A row with no record or no tree id is a husk.          *)
(* ReadFail(p): the read faults; it fails closed as "unreadable".          *)
(*                                                                         *)
(* Decide(p, c, k): decide one found pin. It is removed only when the read *)
(* was a husk, the id gate judges the id a leaf's own, and no merge of    *)
(* the consumer has reached k since its token (the fence, issue #5358).    *)
(* Otherwise it is left exactly where it is.                               *)
(*                                                                         *)
(* Done(p): every found pin is decided. Abandon(p): cancellation, a       *)
(* timeout or the loss of the coordinator ends the run part-way; anything *)
(* undecided stays put (an environment fault).                             *)
(***************************************************************************)
Start(p) ==
    /\ pc[p] = "idle"
    /\ p = Drive => (phase = "husk" /\ LeafPinned)
    /\ pc' = [pc EXCEPT ![p] = "enum"]
    /\ seen' = [seen EXCEPT ![p] = {}]
    /\ found' = [found EXCEPT ![p] = [c \in Cons |-> {}]]
    /\ verdict' = [verdict EXCEPT ![p] = "none"]
    /\ touched' = [touched EXCEPT ![p] = {}]
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Enum(p, k) ==
    /\ pc[p] = "enum"
    /\ k = KeyOrder[Cardinality(seen[p]) + 1]
    /\ seen' = [seen EXCEPT ![p] = @ \cup {k}]
    /\ found' = [found EXCEPT ![p] =
                    [c \in Cons |-> IF Present(k, c) /\ (p # Drive \/ c = "cL")
                                    THEN @[c] \cup {k}
                                    ELSE @[c]]]
    /\ pc' = [pc EXCEPT ![p] = IF seen[p] \cup {k} = Keys THEN "read" ELSE "enum"]
    /\ UNCHANGED <<verdict, touched>>
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Read(p) ==
    /\ pc[p] = "read"
    /\ verdict' = [verdict EXCEPT ![p] = IF phase = "husk" THEN "husk" ELSE "live"]
    /\ pc' = [pc EXCEPT ![p] = "decide"]
    /\ UNCHANGED <<seen, found, touched>>
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

ReadFail(p) ==
    /\ pc[p] = "read"
    /\ faults < MaxFaults
    /\ verdict' = [verdict EXCEPT ![p] = "unreadable"]
    /\ pc' = [pc EXCEPT ![p] = "decide"]
    /\ faults' = faults + 1
    /\ UNCHANGED <<seen, found, touched>>
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED tailVars

Decide(p, c, k) ==
    /\ pc[p] = "decide"
    /\ k \in found[p][c]
    /\ found' = [found EXCEPT ![p][c] = @ \ {k}]
    /\ pinOff' = IF /\ verdict[p] = "husk"
                    /\ IdVerdict(c) = "LeafPublished"
                    /\ k \notin touched[p]
                 THEN [pinOff EXCEPT ![k, c] = NoPin]
                 ELSE pinOff
    /\ touched' = IF c = "cL" THEN [touched EXCEPT ![p] = @ \ {k}] ELSE touched
    /\ UNCHANGED <<pc, seen, verdict>>
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinInc
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Done(p) ==
    /\ pc[p] = "decide"
    /\ \A c \in Cons : found[p][c] = {}
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ seen' = [seen EXCEPT ![p] = {}]
    /\ verdict' = [verdict EXCEPT ![p] = "none"]
    /\ touched' = [touched EXCEPT ![p] = {}]
    /\ UNCHANGED found
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED faults
    /\ UNCHANGED tailVars

Abandon(p) ==
    /\ pc[p] # "idle"
    /\ faults < MaxFaults
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ found' = [found EXCEPT ![p] = [c \in Cons |-> {}]]
    /\ seen' = [seen EXCEPT ![p] = {}]
    /\ verdict' = [verdict EXCEPT ![p] = "none"]
    /\ touched' = [touched EXCEPT ![p] = {}]
    /\ faults' = faults + 1
    /\ UNCHANGED walVars
    /\ UNCHANGED leafVars
    /\ UNCHANGED pinVars
    /\ UNCHANGED tailVars

Next ==
    \/ Write
    \/ Checkpoint
    \/ Publish
    \/ Seed
    \/ RouteChange
    \/ SoftDelete
    \/ RecoverTree
    \/ MarkPurge
    \/ Fold
    \/ ClearRow
    \/ \E k \in Keys : SelfRetire(k)
    \/ \E k \in Keys : SelfRetireFail(k)
    \/ Recreate
    \/ Discard
    \/ DiscardRetire
    \/ PurgeTailRemove
    \/ PurgeTailSettle
    \/ GcTrim
    \/ \E p \in Procs : Start(p)
    \/ \E p \in Procs, k \in Keys : Enum(p, k)
    \/ \E p \in Procs : Read(p)
    \/ \E p \in Procs : ReadFail(p)
    \/ \E p \in Procs, c \in Cons, k \in Keys : Decide(p, c, k)
    \/ \E p \in Procs : Done(p)
    \/ \E p \in Procs : Abandon(p)

(***************************************************************************)
(* Fairness sits on storage and turn completion only: a dying leaf's own   *)
(* retirement finishes, the deleted-tree hold retires a discarded tree's   *)
(* pins, and every retirement process, once started, completes its reads, *)
(* its leaf read and its per-pin decisions; a sweeper is also started      *)
(* again (the GC keeps passing a blocked tree). None is placed on what a   *)
(* decision decides: Decide's outcome is fixed by its inputs, and the      *)
(* drive starts only on its own NotDriven evidence. The lifecycle, writes, *)
(* publications, re-creation and the faults are not fair.                  *)
(***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A k \in Keys : WF_vars(SelfRetire(k))
    /\ WF_vars(DiscardRetire)
    /\ \A p \in Procs \ {Drive} : WF_vars(Start(p))
    /\ \A p \in Procs, k \in Keys : WF_vars(Enum(p, k))
    /\ \A p \in Procs : WF_vars(Read(p))
    /\ \A p \in Procs, c \in Cons, k \in Keys : WF_vars(Decide(p, c, k))
    /\ \A p \in Procs : WF_vars(Done(p))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* The WAL is never trimmed past a write the leaf still needs to replay: one
\* its current incarnation wrote above its durable checkpoint, while its row is
\* live or can be handed back by recovery.
NoTrimPastLiveNeed ==
    \A o \in need : o >= tail

\* A seeded incarnation whose row is bound (live, soft-deleted or marked but
\* not cleared) always has a pin somewhere in the store. The root-cause form of
\* NoTrimPastLiveNeed: violated as soon as a retirement erases the only pin of
\* a publisher that is not gone, whether or not a trim follows.
LivePublisherPinned ==
    (phase \in {"live", "deleted", "marked"} /\ seeded) => LeafPinned

\* A removal decided by a retirement process
RetiredBy(p, c, k) ==
    /\ k \in found[p][c]
    /\ k \notin found'[p][c]
    /\ Present(k, c)
    /\ pinOff'[k, c] = NoPin

\* A sweep or drive removes a pin only if its publisher is gone: the leaf's
\* state was cleared after the pin was published, and no later incarnation has
\* merged into the entry since.
RetireOnlyHusk ==
    [][ \A p \in Procs, c \in Cons, k \in Keys :
            RetiredBy(p, c, k) => PublisherGone(k, c) ]_vars

\* A leaf read that faulted never retires a pin: it is fail-closed.
UnreadableNeverRetired ==
    [][ \A p \in Procs, c \in Cons, k \in Keys :
            RetiredBy(p, c, k) => verdict[p] # "unreadable" ]_vars

(***************************************************************************)
(* Liveness.                                                               *)
(***************************************************************************)

\* A leaf that is gone - its row cleared, or its tree discarded - eventually
\* holds no pin anywhere, unless its grain id is populated again first.
OrphanEventuallyRetired ==
    (phase \in {"husk", "discarded"}) ~> (~LeafPinned \/ phase \notin {"husk", "discarded"})

=============================================================================
