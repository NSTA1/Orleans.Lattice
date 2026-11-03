---------------------- MODULE WalLiveAdvanceComposite ----------------------
(***************************************************************************)
(* The interface check of the five live-advance modules (epic #5605,      *)
(* member 6c). It composes the guarantees of the append-epoch (E),        *)
(* hand-off (H), routing-floor (R) and bulk-switch (B) modules with the   *)
(* assumptions the hand-off, routing-floor and core (C) modules make of   *)
(* them, over the vocabulary of LiveAdvanceInterface.md, and checks that  *)
(* no guarantee fails to meet the assumption that relies on it.           *)
(*                                                                         *)
(* The state is the vocabulary only: one partition's allocator and        *)
(* authors, the claims of L's activations and of two initiators (a        *)
(* foreign author F and the purged incarnation's shard root S), L's       *)
(* marker, the purge, and the bulk birth. Each step is generated freely   *)
(* within the vocabulary and kept only if every producer guarantee still  *)
(* holds after it: the guarantees are the step constraints, copied from   *)
(* their modules. Each consumer's environment guard is then an action     *)
(* property that every kept step must pass, copied from the consumer.     *)
(* A step a guarantee allows and an assumption forbids is an interface    *)
(* mismatch. LiveAdvanceCompositeTests pins every copy to its source.     *)
(*                                                                         *)
(* See LiveAdvanceCompositeRefinement.md for the edge table and the       *)
(* mapping of every copied formula to its source.                         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

MaxOff == 2
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
Inits == {"F", "S"}
Claimants == Acts \cup Inits
NoAct == "none"
Foreign == "f"
Straddler == "s"
PreAuthor == "p"
NoAuthor == "x"
MaxEpoch == 2
Epochs == 0..MaxEpoch
Unloaded == -1
MaxWindows == 1

Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    next,          \* the WAL shard's allocator: the next offset it assigns
    author,        \* the author of each allocated offset
    up,            \* L's running activations
    dupUsed,       \* the duplicate activation has activated
    epoch,         \* E: each claimant's epoch or routing generation
    cstage,        \* E: 3 once a claimant's claim is raised, else 0
    claimHead,     \* E: the allocator head each claimant's raise returned
    prior,         \* E: ghost, the offsets allocated when each claim was raised
    maxClaimed,    \* E: ghost, the highest epoch raised for L
    lRetired,      \* E: ghost, the highest epoch L's clear retired
    staleAccepted, \* E: ghost, a superseded or retired own append was allocated
    memFence,      \* E: the WAL shard's loaded entry per initiator; Unloaded
    fence,         \* E: the durable entry per initiator
    retired,       \* E: the floor per initiator
    mInit,         \* H: L's marker state is initialised
    hoOpen,        \* H: L's marker is open
    hoClosedAt,    \* H: the maximum of the heads the marker's closes recorded
    windows,       \* H: hand-off windows opened so far
    recreated,     \* R: the tree has been purged and re-created
    legacy,        \* R: the purge finished on 10.2.0
    caught,        \* R: the re-seed catch-up ran after a 10.2.0 purge
    reinit,        \* R: the re-created incarnation's first initialisation ran
    purgedUp,      \* the activations of the purged incarnation
    pGen,          \* the generation S held when the tree was purged
    phase,         \* B: "window" while a bulk switch creates L, then "live"
    hint,          \* B: the bulk-born leaf's birth hint
    born,          \* B: L was born by the bulk switch
    cache,         \* B: the records each activation holds; set only at a bulk birth
    rp,            \* B: each activation's read position; set only at a bulk birth
    restarted,     \* the WAL shard has restarted
    broken         \* the edges a kept step has failed: empty unless an assumption fails

eVars == <<epoch, cstage, claimHead, prior, maxClaimed, lRetired, staleAccepted, memFence, fence, retired>>
hVars == <<mInit, hoOpen, hoClosedAt, windows>>
rVars == <<recreated, legacy, caught, reinit, purgedUp, pGen>>
bVars == <<phase, hint, born, cache, rp>>
vars == <<next, author, up, dupUsed, restarted, eVars, hVars, rVars, bVars, broken>>

Allocated == {o \in Offs : author[o] # NoAuthor}

Edges ==
    {"EntryCheckMeetsHandOffAdmission",
     "ClaimHeadMeetsHandOffClose",
     "EntryCheckMeetsRoutingAdmission",
     "ClaimWriteMeetsRoutingClaim",
     "ClaimHeadMeetsRoutingInit",
     "EpochMeetsCoreClaim",
     "EpochMeetsCoreOwnAlloc",
     "HandOffMeetsCoreForeignAlloc",
     "HandOffMeetsCoreClose",
     "RoutingMeetsCoreStraddler",
     "RoutingMeetsCoreInit",
     "BulkMeetsCoreCreate"}

Fails(edge, ok) == IF ok THEN {} ELSE {edge}

TypeOK ==
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Claimants \cup {Foreign, Straddler, PreAuthor, NoAuthor}]
    /\ up \subseteq Acts
    /\ dupUsed \in BOOLEAN
    /\ restarted \in BOOLEAN
    /\ epoch \in [Claimants -> Epochs]
    /\ cstage \in [Claimants -> {0, 3}]
    /\ claimHead \in [Claimants -> 0..MaxOff]
    /\ prior \in [Claimants -> SUBSET Offs]
    /\ maxClaimed \in Epochs
    /\ lRetired \in Epochs
    /\ staleAccepted \in BOOLEAN
    /\ memFence \in [Inits -> {Unloaded} \cup Epochs]
    /\ fence \in [Inits -> Epochs]
    /\ retired \in [Inits -> Epochs]
    /\ mInit \in BOOLEAN
    /\ hoOpen \in BOOLEAN
    /\ hoClosedAt \in 0..MaxOff
    /\ windows \in 0..MaxWindows
    /\ recreated \in BOOLEAN
    /\ legacy \in BOOLEAN
    /\ caught \in BOOLEAN
    /\ reinit \in BOOLEAN
    /\ purgedUp \subseteq Acts
    /\ pGen \in Epochs
    /\ phase \in {"window", "live"}
    /\ hint \in 0..MaxOff
    /\ born \in BOOLEAN
    /\ cache \in [Acts -> SUBSET Offs]
    /\ rp \in [Acts -> -1..(MaxOff - 1)]
    /\ broken \subseteq Edges

(* L is a new tree's first leaf, or a bulk switch is creating it over a   *)
(* range already holding up to one record, with its marker open. F and S  *)
(* hold claimed generations at L's partition.                             *)
Init ==
    /\ \E n \in 0..1, bulk \in BOOLEAN :
        /\ bulk \/ n = 0
        /\ next = n
        /\ author = [o \in Offs |-> IF o < n THEN PreAuthor ELSE NoAuthor]
        /\ hint = n
        /\ phase = IF bulk THEN "window" ELSE "live"
        /\ up = IF bulk THEN {} ELSE {"a1"}
        /\ hoOpen = bulk
        /\ windows = IF bulk THEN 1 ELSE 0
    /\ dupUsed = FALSE
    /\ restarted = FALSE
    /\ epoch = [c \in Claimants |-> IF c \in Inits THEN 1 ELSE 0]
    /\ cstage = [c \in Claimants |-> IF c \in Inits THEN 3 ELSE 0]
    /\ claimHead = [c \in Claimants |-> 0]
    /\ prior = [c \in Claimants |-> {}]
    /\ maxClaimed = 0
    /\ lRetired = 0
    /\ staleAccepted = FALSE
    /\ memFence = [i \in Inits |-> 1]
    /\ fence = [i \in Inits |-> 1]
    /\ retired = [i \in Inits |-> 0]
    /\ mInit = TRUE
    /\ hoClosedAt = 0
    /\ recreated = FALSE
    /\ legacy = FALSE
    /\ caught = FALSE
    /\ reinit = FALSE
    /\ purgedUp = {}
    /\ pGen = 0
    /\ born = FALSE
    /\ cache = [a \in Acts |-> {}]
    /\ rp = [a \in Acts |-> -1]
    /\ broken = {}

(***************************************************************************)
(* The producer guarantees, copied from their modules. The copies rename  *)
(* only identifiers, each rename listed in LiveAdvanceCompositeTests, and *)
(* that test fails if a copy drifts from its source.                      *)
(***************************************************************************)

\* E (WalAppendEpoch): the entry check on a stamped append - a loaded entry
\* (OwnAlloc's guard) and a stamp not below it (AppendAdmits).
EAppendLoaded(a) == memFence[a] # Unloaded
EAppendAdmits(a, s) == s >= memFence[a]

\* E: the claim write's admission (ClaimWrite's guards and AlreadyDurable).
EAlreadyDurable(a, s) == s = fence[a] /\ s >= memFence[a]
EClaimAboveFloor(a, s) == s > retired[a]
EClaimAboveEntry(a, s) == s > Max2(fence[a], memFence[a]) \/ EAlreadyDurable(a, s)

\* E: ClaimHeadCoversPriorAppends, over every claimant.
ClaimHeadCoversPriorAppends ==
    \A a \in Claimants : cstage[a] = 3 => \A o \in prior[a] : o < claimHead[a]

\* E: RaisedEpochUnique, with CanAppend over the vocabulary: a raised
\* activation of L whose epoch L's clear has not retired.
CanAppend(a) == a \in up /\ cstage[a] = 3 /\ epoch[a] > lRetired
RaisedEpochUnique ==
    \A a, b \in Acts : a # b /\ CanAppend(a) /\ CanAppend(b) => epoch[a] # epoch[b]

\* E: StaleEpochNeverAppends, with the ghost OwnAlloc maintains.
StaleEpochNeverAppends == ~staleAccepted
EStaleUpdate(a) == staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed \/ epoch[a] <= lRetired)

\* H (WalHandOffFence): HandOffBound and CloseHeadMonotone.
MarkerReadsClosed == mInit /\ ~hoOpen
HandOffBound ==
    MarkerReadsClosed => \A o \in Offs : author[o] \in {Foreign} => o < hoClosedAt
CloseHeadMonotone == hoClosedAt' >= hoClosedAt

\* R (WalRoutingFloor): StraddlerBelowInit, over the straddler offsets.
rAuthor == [o \in Offs |-> IF author[o] = Straddler THEN Straddler ELSE NoAuthor]
MarkerReadsInit == mInit /\ ~(legacy /\ ~caught)
StraddlerBelowInit ==
    recreated /\ MarkerReadsInit => \A o \in Offs : rAuthor[o] # NoAuthor => o < hoClosedAt

\* B (WalBulkSwitch): BulkHintBound, for the bulk-born activation a1.
BulkHintBound == born => \A o \in Allocated : o <= rp["a1"] => o \in cache["a1"]

\* Every state guarantee, kept after every step. CloseHeadMonotone holds
\* within an incarnation of the marker; a purge discards the marker.
Guarantees ==
    /\ ClaimHeadCoversPriorAppends
    /\ RaisedEpochUnique
    /\ StaleEpochNeverAppends
    /\ HandOffBound
    /\ StraddlerBelowInit
    /\ BulkHintBound

(***************************************************************************)
(* The consumer assumptions, copied from their modules: H's and R's        *)
(* admission of E's entry check, and the core's environment steps.         *)
(***************************************************************************)

\* H: ForeignAlloc admits at or above the initiator's entry; Close records
\* a head above every stamped foreign append (CloseHeads).
HAllocGuard(fstamp, fFence) == fstamp >= fFence
CloseHeads == {h \in 0..next : \A o \in Offs : author[o] \in {Foreign} => o < h}

\* R: Alloc admits on a loaded entry not above the stamp; a re-presented
\* claim is admitted above the floor and not below the entry (ClaimAdmits).
RAllocGuard(i, loaded, pstamp) == loaded[i] # Unloaded /\ pstamp[i] >= loaded[i]
ClaimAdmits(i, pstamp, walFloor, entryRow) == pstamp[i] > walFloor[i] /\ pstamp[i] >= entryRow[i]

\* The core's view of E: the current activation and the stale ones.
claimed == [c \in Claimants |-> cstage[c] = 3]
cur == IF \E a \in up : CanAppend(a) /\ epoch[a] = maxClaimed
       THEN CHOOSE a \in up : CanAppend(a) /\ epoch[a] = maxClaimed
       ELSE NoAct
stale == {a \in Acts : claimed[a] /\ ~CanAppend(a)} \cup purgedUp

\* C (WalLiveAdvanceCore): the heads a close may record.
HeadsAbove(S) == {h \in 0..next : \A o \in Offs : o < next /\ author[o] \in S => o < h}

\* The entry H's guard reads: the loaded one, or the durable one it loads.
HEntry(i) == IF memFence[i] = Unloaded THEN fence[i] ELSE memFence[i]

\* C's environment steps.
CClaim(a) ==
    /\ a \in up
    /\ a \notin stale
    /\ ~claimed[a]
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ cur' = a
COwnAlloc(a) == a \in up /\ cur = a
CForeignAlloc == hoOpen
CClose == \E h \in HeadsAbove({Foreign}) : hoClosedAt' = Max2(hoClosedAt, h)
CStraddlerAlloc == recreated /\ ~reinit
CMarkInit == \E h \in HeadsAbove({Foreign, Straddler}) : hoClosedAt' = Max2(hoClosedAt, h)
CCreate ==
    /\ cache' = [cache EXCEPT !["a1"] = {o \in Offs : author[o] = PreAuthor}]
    /\ rp' = [rp EXCEPT !["a1"] = hint - 1]

(***************************************************************************)
(* The steps. Each is generated freely within the vocabulary; Next keeps  *)
(* it only if every guarantee holds after it.                             *)
(***************************************************************************)

(* Claim(a): one of L's activations raises a claim, with any epoch and    *)
(* any head the allocator has reached.                                    *)
Claim(a) ==
    /\ a \in up
    /\ cstage[a] = 0
    /\ \E e \in 1..MaxEpoch, h \in 0..next :
        /\ epoch' = [epoch EXCEPT ![a] = e]
        /\ claimHead' = [claimHead EXCEPT ![a] = h]
    /\ cstage' = [cstage EXCEPT ![a] = 3]
    /\ prior' = [prior EXCEPT ![a] = Allocated]
    /\ maxClaimed' = Max2(maxClaimed, epoch'[a])
    /\ UNCHANGED <<next, author, up, dupUsed, lRetired, staleAccepted, memFence, fence, retired>>
    /\ UNCHANGED <<hVars, rVars, bVars>>
    /\ broken' = broken \cup Fails("EpochMeetsCoreClaim", CClaim(a))
    /\ UNCHANGED restarted

(* OwnAlloc(a): a raised activation's stamped append is allocated.        *)
OwnAlloc(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ EStaleUpdate(a)
    /\ broken' = broken \cup Fails("EpochMeetsCoreOwnAlloc", COwnAlloc(a))
    /\ UNCHANGED <<up, dupUsed, epoch, cstage, claimHead, prior, maxClaimed, lRetired, memFence, fence, retired>>
    /\ UNCHANGED <<hVars, rVars, bVars>>
    /\ UNCHANGED restarted

(* Duplicate: a second activation of L, or the next one.                 *)
Duplicate ==
    /\ phase = "live"
    /\ ~dupUsed
    /\ dupUsed' = TRUE
    /\ up' = up \cup {"a2"}
    /\ UNCHANGED <<next, author, eVars, hVars, rVars, bVars, broken>>
    /\ UNCHANGED restarted

(* ForeignOpen: F opens L's marker for a hand-off window.                *)
ForeignOpen ==
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<next, author, up, dupUsed, eVars, mInit, hoClosedAt, rVars, bVars, broken>>
    /\ UNCHANGED restarted

(* ForeignAlloc: F's stamped append of a record L owns, under any stamp   *)
(* F has held, is allocated if E's entry check admits it.                *)
ForeignAlloc ==
    /\ next < MaxOff
    /\ \E s \in 1..epoch["F"] :
        /\ EAppendLoaded("F")
        /\ EAppendAdmits("F", s)
        /\ broken' = broken \cup Fails("EntryCheckMeetsHandOffAdmission", HAllocGuard(s, HEntry("F")))
                               \cup Fails("HandOffMeetsCoreForeignAlloc", CForeignAlloc)
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ UNCHANGED <<up, dupUsed, eVars, hVars, rVars, bVars>>
    /\ UNCHANGED restarted

(* Close: F re-claims a generation E's claim write admits, reading any   *)
(* head, and L's marker closes recording any head.                       *)
Close ==
    /\ hoOpen
    /\ \E g \in 1..MaxEpoch, h \in 0..next, v \in 0..next :
        /\ EClaimAboveFloor("F", g)
        /\ EClaimAboveEntry("F", g)
        /\ epoch' = [epoch EXCEPT !["F"] = g]
        /\ fence' = [fence EXCEPT !["F"] = Max2(@, g)]
        /\ memFence' = [memFence EXCEPT !["F"] = g]
        /\ claimHead' = [claimHead EXCEPT !["F"] = h]
        /\ hoClosedAt' = v
    /\ prior' = [prior EXCEPT !["F"] = Allocated]
    /\ hoOpen' = FALSE
    /\ broken' = broken \cup Fails("ClaimHeadMeetsHandOffClose", claimHead'["F"] \in CloseHeads)
                           \cup Fails("HandOffMeetsCoreClose", CClose)
    /\ UNCHANGED <<next, author, up, dupUsed, cstage, maxClaimed, lRetired, staleAccepted, retired>>
    /\ UNCHANGED <<mInit, windows, rVars, bVars>>
    /\ UNCHANGED restarted

(* Restart: the WAL shard restarts once and unloads every entry.         *)
Restart ==
    /\ ~restarted
    /\ restarted' = TRUE
    /\ memFence' = [i \in Inits |-> Unloaded]
    /\ UNCHANGED <<next, author, up, dupUsed, epoch, cstage, claimHead, prior, maxClaimed>>
    /\ UNCHANGED <<lRetired, staleAccepted, fence, retired, hVars, rVars, bVars, broken>>

(* Represent(i): an append met i's unloaded entry, and i re-presents a    *)
(* claim of a stamp it has held, which E's claim write admits.           *)
Represent(i) ==
    /\ memFence[i] = Unloaded
    /\ \E s \in 1..epoch[i] :
        /\ EClaimAboveFloor(i, s)
        /\ EClaimAboveEntry(i, s)
        /\ fence' = [fence EXCEPT ![i] = Max2(fence[i], s)]
        /\ memFence' = [memFence EXCEPT ![i] = s]
        /\ broken' = broken \cup Fails("ClaimWriteMeetsRoutingClaim",
                                         i # "S" \/ ClaimAdmits("S", [j \in Inits |-> s], retired, fence))
    /\ UNCHANGED <<next, author, up, dupUsed, epoch, cstage, claimHead, prior, maxClaimed>>
    /\ UNCHANGED <<lRetired, staleAccepted, retired, hVars, rVars, bVars>>
    /\ UNCHANGED restarted

(* Purge: the new build purges the tree. S's floor rises over its         *)
(* generation and entries and its entry is retired; L's clear retires    *)
(* every epoch L raised; L's marker is discarded.                         *)
Purge ==
    /\ phase = "live"
    /\ ~born
    /\ ~recreated
    /\ recreated' = TRUE
    /\ retired' = [retired EXCEPT !["S"] =
                     Max2(@, Max2(epoch["S"], Max2(fence["S"], memFence["S"])))]
    /\ fence' = [fence EXCEPT !["S"] = 0]
    /\ memFence' = [memFence EXCEPT !["S"] = Unloaded]
    /\ lRetired' = Max2(lRetired, maxClaimed)
    /\ purgedUp' = up
    /\ pGen' = epoch["S"]
    /\ mInit' = FALSE
    /\ hoOpen' = FALSE
    /\ hoClosedAt' = 0
    /\ UNCHANGED <<next, author, up, dupUsed, epoch, cstage, claimHead, prior, maxClaimed, staleAccepted>>
    /\ UNCHANGED <<windows, legacy, caught, reinit, bVars, broken>>
    /\ UNCHANGED restarted

(* LegacyPurge: the purge finished on 10.2.0, which keeps every row and   *)
(* L's marker. L's running activations are the purged incarnation's.     *)
LegacyPurge ==
    /\ phase = "live"
    /\ ~born
    /\ ~recreated
    /\ recreated' = TRUE
    /\ legacy' = TRUE
    /\ lRetired' = Max2(lRetired, maxClaimed)
    /\ purgedUp' = up
    /\ pGen' = epoch["S"]
    /\ UNCHANGED <<next, author, up, dupUsed, epoch, cstage, claimHead, prior, maxClaimed>>
    /\ UNCHANGED <<staleAccepted, memFence, fence, retired, hVars, caught, reinit, bVars, broken>>
    /\ UNCHANGED restarted

(* CatchUp: after a 10.2.0 purge, S re-seeds the tombstone and resets L's *)
(* marker to uninitialised.                                               *)
CatchUp ==
    /\ legacy
    /\ ~caught
    /\ caught' = TRUE
    /\ mInit' = FALSE
    /\ UNCHANGED <<next, author, up, dupUsed, eVars, hoOpen, hoClosedAt, windows>>
    /\ UNCHANGED <<recreated, legacy, reinit, purgedUp, pGen, bVars, broken>>
    /\ UNCHANGED restarted

(* StraddlerAlloc: a write S admitted before the purge, under a stamp it *)
(* held then, is allocated after it if E's entry check admits it.        *)
StraddlerAlloc ==
    /\ recreated
    /\ next < MaxOff
    /\ \E s \in 1..pGen :
        /\ EAppendLoaded("S")
        /\ EAppendAdmits("S", s)
        /\ broken' = broken \cup Fails("EntryCheckMeetsRoutingAdmission", RAllocGuard("S", memFence, [i \in Inits |-> s]))
                               \cup Fails("RoutingMeetsCoreStraddler", CStraddlerAlloc)
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Straddler]
    /\ UNCHANGED <<up, dupUsed, eVars, hVars, rVars, bVars>>
    /\ UNCHANGED restarted

(* FirstInit: the re-created incarnation of S claims a generation E's    *)
(* claim write admits, reading any head, and L's marker records any head *)
(* as a close and reads initialised.                                      *)
FirstInit ==
    /\ recreated
    /\ ~reinit
    /\ ~mInit
    /\ ~legacy \/ caught
    /\ \E g \in 1..MaxEpoch, h \in 0..next, v \in 0..next :
        /\ EClaimAboveFloor("S", g)
        /\ EClaimAboveEntry("S", g)
        /\ epoch' = [epoch EXCEPT !["S"] = g]
        /\ fence' = [fence EXCEPT !["S"] = Max2(@, g)]
        /\ memFence' = [memFence EXCEPT !["S"] = g]
        /\ claimHead' = [claimHead EXCEPT !["S"] = h]
        /\ hoClosedAt' = v
    /\ prior' = [prior EXCEPT !["S"] = Allocated]
    /\ mInit' = TRUE
    /\ reinit' = TRUE
    /\ broken' = broken \cup Fails("ClaimHeadMeetsRoutingInit", claimHead'["S"] = next)
                           \cup Fails("RoutingMeetsCoreInit", CMarkInit)
    /\ UNCHANGED <<next, author, up, dupUsed, cstage, maxClaimed, lRetired, staleAccepted, retired>>
    /\ UNCHANGED <<hoOpen, windows, recreated, legacy, caught, purgedUp, pGen, bVars>>
    /\ UNCHANGED restarted

(* Birth: the bulk switch creates L, its first activation born with any   *)
(* read position and any of the records allocated so far.                *)
Birth ==
    /\ phase = "window"
    /\ phase' = "live"
    /\ born' = TRUE
    /\ up' = {"a1"}
    /\ \E r \in -1..(next - 1), c \in SUBSET Allocated :
        /\ rp' = [rp EXCEPT !["a1"] = r]
        /\ cache' = [cache EXCEPT !["a1"] = c]
    /\ broken' = broken \cup Fails("BulkMeetsCoreCreate", CCreate)
    /\ UNCHANGED <<next, author, dupUsed, eVars, hVars, rVars, hint>>
    /\ UNCHANGED restarted

Stutter == UNCHANGED vars

(* A step is kept only if every guarantee holds after it, and the heads  *)
(* L's marker requires do not decrease within an incarnation: a purge on  *)
(* the new build discards the marker.                                      *)
Kept ==
    /\ Guarantees'
    /\ (~recreated /\ recreated' /\ ~legacy') \/ CloseHeadMonotone

Next ==
    \/ \E a \in Acts : Claim(a) /\ Kept
    \/ \E a \in Acts : OwnAlloc(a) /\ Kept
    \/ Duplicate /\ Kept
    \/ ForeignOpen /\ Kept
    \/ ForeignAlloc /\ Kept
    \/ Close /\ Kept
    \/ Restart /\ Kept
    \/ \E i \in Inits : Represent(i) /\ Kept
    \/ Purge /\ Kept
    \/ LegacyPurge /\ Kept
    \/ CatchUp /\ Kept
    \/ StraddlerAlloc /\ Kept
    \/ FirstInit /\ Kept
    \/ Birth /\ Kept
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The edges. Each holds unless a kept step of the kind it governs failed *)
(* the assumption its consumer makes.                                     *)
(***************************************************************************)

EntryCheckMeetsHandOffAdmission == "EntryCheckMeetsHandOffAdmission" \notin broken
ClaimHeadMeetsHandOffClose == "ClaimHeadMeetsHandOffClose" \notin broken
EntryCheckMeetsRoutingAdmission == "EntryCheckMeetsRoutingAdmission" \notin broken
ClaimWriteMeetsRoutingClaim == "ClaimWriteMeetsRoutingClaim" \notin broken
ClaimHeadMeetsRoutingInit == "ClaimHeadMeetsRoutingInit" \notin broken
EpochMeetsCoreClaim == "EpochMeetsCoreClaim" \notin broken
EpochMeetsCoreOwnAlloc == "EpochMeetsCoreOwnAlloc" \notin broken
HandOffMeetsCoreForeignAlloc == "HandOffMeetsCoreForeignAlloc" \notin broken
HandOffMeetsCoreClose == "HandOffMeetsCoreClose" \notin broken
RoutingMeetsCoreStraddler == "RoutingMeetsCoreStraddler" \notin broken
RoutingMeetsCoreInit == "RoutingMeetsCoreInit" \notin broken
BulkMeetsCoreCreate == "BulkMeetsCoreCreate" \notin broken
=============================================================================
