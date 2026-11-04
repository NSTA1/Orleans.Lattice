-------------------------- MODULE WalRoutingFloor --------------------------
(***************************************************************************)
(* The routing floor of the fenced live checkpoint advance (epic #5605):   *)
(* how a tree's purge and re-creation keep a write the purged incarnation  *)
(* admitted - a straddler - out of the range a re-created leaf L advances  *)
(* over without reading, and keep every live shard root's routing          *)
(* generation above its own floor. It is module R of five focused modules  *)
(* promoted from the design model spec/wal/design/WalLiveAdvance.tla. It   *)
(* assumes only module E's entry check, as an environment constraint: a    *)
(* stamp below its initiator's loaded entry is never allocated, and a      *)
(* claim raises the loaded entry in the same step that reads the allocator *)
(* head. Leaf epochs, hand-off windows, bulk loads, replay and read        *)
(* positions belong to the other modules and are not modelled here.        *)
(*                                                                         *)
(* Initiators are shard roots: s, L's own shard root, whose marker L       *)
(* reads, and s2 (WithSecondInitiator), standing for every other shard     *)
(* root, whose keys the re-created tree may map to L. Each has a routing   *)
(* generation, a durable generation entry at L's WAL partition and the WAL *)
(* shard's loaded copy of it, a floor in the tree's routing-floor row, and *)
(* the WAL shard's in-memory copy of that floor. The WAL admits a stamped  *)
(* append on the loaded entry alone; the floor is checked only when a      *)
(* generation is claimed, which an append meeting an unloaded entry must   *)
(* do again (LiveAdvanceDesign.md, section 2, "Purging a tree's fence      *)
(* rows").                                                                 *)
(*                                                                         *)
(* The purge (#5668) raises every initiator's floor over its own           *)
(* generation and entries before any delete, then retires the entries,    *)
(* raising the WAL-side floors and unloading the loaded entries, and       *)
(* deletes L's marker, so L's marker state is uninitialised. The           *)
(* re-created incarnation's first initialisation seeds the floor plus one, *)
(* claims it, and records the allocator head as a close before the marker  *)
(* reads initialised. A write the purged incarnation admitted is never     *)
(* re-stamped: its seam retry surfaces the deleted tree (#5705). A purge   *)
(* that finished on 10.2.0 (WithLegacyPurge) leaves every row and raises   *)
(* no floor; re-seeding the tombstone then runs a catch-up for a marker of *)
(* another incarnation (#5716), raising only its own initiator's floor.    *)
(* WithRestart lets the WAL shard reactivate: it unloads every entry and   *)
(* reloads its floors from the row.                                        *)
(*                                                                         *)
(* See RoutingFloorRefinement.md for the mapping to production, the       *)
(* interface, and what this model does NOT cover.                         *)
(***************************************************************************)
EXTENDS Integers

(***************************************************************************)
(* Instance. WithSecondInitiator adds s2; WithLegacyPurge adds the purge   *)
(* that finished on 10.2.0 beside the new build's; WithRestart adds the    *)
(* WAL shard's reactivation. The purged incarnation's generations start    *)
(* anywhere in 1..MaxInitGen, and MaxGen bounds every generation.          *)
(***************************************************************************)
CONSTANTS WithSecondInitiator, WithLegacyPurge, WithRestart

MaxOff == 3
Offs == 0..(MaxOff - 1)
Inits == {"s", "s2"}
NoAuthor == "none"
MaxInitGen == 2
MaxGen == 4
Gens == 0..MaxGen
Unloaded == -1

Max2(x, y) == IF x >= y THEN x ELSE y

\* s2 takes part only WithSecondInitiator.
Active(i) == i = "s" \/ WithSecondInitiator

VARIABLES
    gen,       \* each initiator's routing generation, the one its live activation stamps with
    entryRow,  \* each initiator's durable generation entry at L's partition; 0 when absent
    loaded,    \* the WAL shard's loaded entry for each initiator; Unloaded when not loaded
    rowFloor,  \* each initiator's floor in the tree's routing-floor row
    walFloor,  \* the WAL shard's in-memory floor for each initiator
    live,      \* each initiator's live shard-root activation stamps with gen
    pend,      \* each initiator has admitted a write not yet allocated or failed
    pstamp,    \* the generation that write was admitted under
    next,      \* the WAL shard's allocator: the next offset it assigns
    author,    \* the initiator each allocated offset went to
    recreated, \* the tree has been purged and re-created
    legacy,    \* the purge finished on 10.2.0: no row was retired and no floor raised
    restarted, \* the WAL shard has reactivated
    mInit,     \* L's marker state is initialised
    closedAt,  \* the allocator head L's marker's last close recorded
    markNew,   \* L's marker carries the re-created incarnation's identity (#5716)
    caught,    \* s has re-seeded the purged tombstone (WithLegacyPurge)
    dup,       \* a duplicate activation of s has re-seeded it too
    rd         \* each initiator's cold load in flight: the value it read; Unloaded when none

vars == <<gen, entryRow, loaded, rowFloor, walFloor, live, pend, pstamp, next, author,
          recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup, rd>>

TypeOK ==
    /\ gen \in [Inits -> Gens]
    /\ entryRow \in [Inits -> Gens]
    /\ loaded \in [Inits -> {Unloaded} \cup 0..(MaxGen + 1)]
    /\ rowFloor \in [Inits -> Gens]
    /\ walFloor \in [Inits -> Gens]
    /\ live \in [Inits -> BOOLEAN]
    /\ pend \in [Inits -> BOOLEAN]
    /\ pstamp \in [Inits -> Gens]
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Inits \cup {NoAuthor}]
    /\ recreated \in BOOLEAN
    /\ legacy \in BOOLEAN
    /\ restarted \in BOOLEAN
    /\ mInit \in BOOLEAN
    /\ closedAt \in 0..MaxOff
    /\ markNew \in BOOLEAN
    /\ caught \in BOOLEAN
    /\ dup \in BOOLEAN
    /\ rd \in [Inits -> {Unloaded} \cup 0..(MaxGen + 1)]

(* The incarnation to be purged is live: each shard root has claimed its   *)
(* generation at L's partition, no floor has been raised, and L's marker   *)
(* is initialised.                                                         *)
Init ==
    /\ gen \in {g \in [Inits -> 1..MaxInitGen] : WithSecondInitiator \/ g["s2"] = 1}
    /\ entryRow = gen
    /\ loaded = gen
    /\ rowFloor = [i \in Inits |-> 0]
    /\ walFloor = [i \in Inits |-> 0]
    /\ live = [i \in Inits |-> TRUE]
    /\ pend = [i \in Inits |-> FALSE]
    /\ pstamp = [i \in Inits |-> 0]
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ recreated = FALSE
    /\ legacy = FALSE
    /\ restarted = FALSE
    /\ mInit = TRUE
    /\ closedAt = 0
    /\ markNew = FALSE
    /\ caught = FALSE
    /\ dup = FALSE
    /\ rd = [i \in Inits |-> Unloaded]

(***************************************************************************)
(* The purged incarnation's writes.                                        *)
(*  Admit(i): before the purge, a live shard root admits a write stamped   *)
(*    with its generation at admission.                                    *)
(*  Alloc(i): the WAL admits the stamp on i's loaded entry alone - loaded  *)
(*    and not above the stamp (E's entry check) - and assigns an offset.   *)
(*  PreArmAlloc(i) (#5833): the same write appended under the PreArm      *)
(*    class, by a leaf that has not armed. It has no leaf-entry check, but *)
(*    its routing generation is checked exactly as a stamped append's.     *)
(*  ColdLoadRead(i), ColdLoadApply(i): the append met an unloaded entry,   *)
(*    so the WAL shard loads it, read-only (#5833, C1): it reads the       *)
(*    durable entry, then, under its gate, loads the highest of that read, *)
(*    i's in-memory floor plus one, and any value loaded meanwhile. It     *)
(*    writes nothing, and re-runs admission. The read and the load are     *)
(*    separate steps, so a re-claim or a retirement can land between them: *)
(*    the max-merge never lowers a value a re-claim raised (C7), and the   *)
(*    floor taken at the load never re-admits a generation a retirement    *)
(*    unloaded and floored after the read.                                 *)
(*  Refuse(i): the append was refused, stale against the loaded entry. The *)
(*    seam retry finds the stamp at or below i's floor and surfaces the    *)
(*    deleted tree; it never re-stamps (#5705).                            *)
(***************************************************************************)
Admit(i) ==
    /\ UNCHANGED rd
    /\ Active(i)
    /\ ~recreated
    /\ live[i]
    /\ ~pend[i]
    /\ pend' = [pend EXCEPT ![i] = TRUE]
    /\ pstamp' = [pstamp EXCEPT ![i] = gen[i]]
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, live, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

Alloc(i) ==
    /\ UNCHANGED rd
    /\ pend[i]
    /\ next < MaxOff
    /\ loaded[i] # Unloaded
    /\ pstamp[i] >= loaded[i]
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = i]
    /\ pend' = [pend EXCEPT ![i] = FALSE]
    /\ pstamp' = [pstamp EXCEPT ![i] = 0]
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, live>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

ClaimAdmits(i) == pstamp[i] > walFloor[i] /\ pstamp[i] >= entryRow[i]

PreArmAlloc(i) ==
    /\ UNCHANGED rd
    /\ pend[i]
    /\ next < MaxOff
    /\ loaded[i] # Unloaded
    /\ pstamp[i] >= loaded[i]
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = i]
    /\ pend' = [pend EXCEPT ![i] = FALSE]
    /\ pstamp' = [pstamp EXCEPT ![i] = 0]
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, live>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

ColdLoadRead(i) ==
    /\ pend[i]
    /\ loaded[i] = Unloaded
    /\ rd[i] = Unloaded
    /\ rd' = [rd EXCEPT ![i] = entryRow[i]]
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, live, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

ColdLoadApply(i) ==
    /\ rd[i] # Unloaded
    /\ loaded' = [loaded EXCEPT ![i] = Max2(@, Max2(rd[i], walFloor[i] + 1))]
    /\ rd' = [rd EXCEPT ![i] = Unloaded]
    /\ UNCHANGED <<gen, entryRow, rowFloor, walFloor, live, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

Refuse(i) ==
    /\ UNCHANGED rd
    /\ pend[i]
    /\ loaded[i] # Unloaded /\ pstamp[i] < loaded[i]
    /\ pstamp[i] <= rowFloor[i]
    /\ pend' = [pend EXCEPT ![i] = FALSE]
    /\ pstamp' = [pstamp EXCEPT ![i] = 0]
    /\ UNCHANGED <<entryRow, loaded>>
    /\ UNCHANGED <<gen, rowFloor, walFloor, live, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

(***************************************************************************)
(* The purge. Purge: the new build reads every routing row and raises      *)
(* each initiator's floor over its own generation and entries in one      *)
(* write, before any delete; it then retires the entries, raising the WAL  *)
(* shard's in-memory floors and unloading the loaded entries, and deletes  *)
(* L's marker, whose state is no longer initialised. LegacyPurge           *)
(* (WithLegacyPurge): the purge finished on 10.2.0, which knows none of    *)
(* these rows and keeps them all. A cross-shard straddler spanning it is   *)
(* the documented mixed-version residue, so it is excluded. Either way the *)
(* purged incarnation's shard roots are gone.                              *)
(***************************************************************************)
Purge ==
    /\ UNCHANGED rd
    /\ ~recreated
    /\ recreated' = TRUE
    /\ LET rf == [i \in Inits |-> Max2(rowFloor[i], Max2(gen[i], Max2(entryRow[i], loaded[i])))] IN
        /\ rowFloor' = rf
        /\ walFloor' = rf
    /\ entryRow' = [i \in Inits |-> 0]
    /\ loaded' = [i \in Inits |-> Unloaded]
    /\ live' = [i \in Inits |-> FALSE]
    /\ closedAt' = 0
    /\ mInit' = FALSE
    /\ markNew' = FALSE
    /\ legacy' = FALSE
    /\ UNCHANGED <<gen, pend, pstamp, next, author, restarted, caught, dup>>

LegacyPurge ==
    /\ UNCHANGED rd
    /\ WithLegacyPurge
    /\ ~recreated
    /\ ~pend["s2"]
    /\ recreated' = TRUE
    /\ legacy' = TRUE
    /\ live' = [i \in Inits |-> FALSE]
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, pend, pstamp, next, author>>
    /\ UNCHANGED <<restarted, mInit, closedAt, markNew, caught, dup>>

(***************************************************************************)
(* Re-seeding the tombstone after a 10.2.0 purge (#5716). CatchUp: s finds *)
(* its marker carrying another incarnation's identity, raises only its     *)
(* own floor row over the marker's generation and entry, and resets the   *)
(* marker to uninitialised; FirstInit then fences. A running WAL shard     *)
(* keeps its in-memory floor until it reactivates, and the entries stay. A *)
(* second run is a duplicate activation's, whose reset does not reach the  *)
(* first activation's cached generation. CatchUpSkip: the marker carries   *)
(* this incarnation's identity, so the re-seed leaves it.                  *)
(***************************************************************************)
CatchUp ==
    /\ UNCHANGED rd
    /\ recreated
    /\ legacy
    /\ ~dup
    /\ dup' = caught
    /\ caught' = TRUE
    /\ ~markNew
    /\ LET rc == Max2(rowFloor["s"], Max2(gen["s"], entryRow["s"])) IN
        /\ rc < MaxGen
        /\ rowFloor' = [rowFloor EXCEPT !["s"] = rc]
    /\ mInit' = FALSE
    /\ live' = [live EXCEPT !["s"] = live["s"] /\ caught]
    /\ UNCHANGED <<gen, entryRow, loaded, walFloor, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, closedAt, markNew>>

CatchUpSkip ==
    /\ UNCHANGED rd
    /\ recreated
    /\ legacy
    /\ ~dup
    /\ markNew
    /\ dup' = caught
    /\ caught' = TRUE
    /\ UNCHANGED <<gen, entryRow, loaded, rowFloor, walFloor, live, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew>>

(***************************************************************************)
(* First initialisations. FirstInit: the re-created incarnation of s, its  *)
(* marker uninitialised (and, after a 10.2.0 purge, its tombstone          *)
(* re-seeded), seeds its floor plus one, claims it at L's partition, and   *)
(* records the allocator head as a close before the marker reads           *)
(* initialised, tagged with this incarnation. S2Init: s2's re-created      *)
(* incarnation raises its own floor over anything a 10.2.0 purge left,    *)
(* seeds the floor plus one and claims it. L's first initialisation does   *)
(* not claim s2's generation. Each seed is above the WAL-side floor, which *)
(* never exceeds the row's, so the claim is admitted.                      *)
(***************************************************************************)
FirstInit ==
    /\ UNCHANGED rd
    /\ recreated
    /\ ~mInit
    /\ ~legacy \/ caught
    /\ rowFloor["s"] + 1 <= MaxGen
    /\ gen' = [gen EXCEPT !["s"] = rowFloor["s"] + 1]
    /\ entryRow' = [entryRow EXCEPT !["s"] = Max2(@, gen'["s"])]
    /\ loaded' = [loaded EXCEPT !["s"] = Max2(@, gen'["s"])]
    /\ closedAt' = next
    /\ mInit' = TRUE
    /\ markNew' = TRUE
    /\ live' = [live EXCEPT !["s"] = TRUE]
    /\ UNCHANGED <<rowFloor, walFloor, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, caught, dup>>

S2Init ==
    /\ UNCHANGED rd
    /\ WithSecondInitiator
    /\ recreated
    /\ ~live["s2"]
    /\ LET rf2 == Max2(rowFloor["s2"], Max2(gen["s2"], entryRow["s2"])) IN
        /\ rf2 < MaxGen
        /\ rowFloor' = [rowFloor EXCEPT !["s2"] = rf2]
        /\ gen' = [gen EXCEPT !["s2"] = rf2 + 1]
    /\ entryRow' = [entryRow EXCEPT !["s2"] = Max2(@, gen'["s2"])]
    /\ loaded' = [loaded EXCEPT !["s2"] = Max2(@, gen'["s2"])]
    /\ live' = [live EXCEPT !["s2"] = TRUE]
    /\ UNCHANGED <<walFloor, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, restarted, mInit, closedAt, markNew, caught, dup>>

(* Restart (WithRestart): the WAL shard reactivates, or a move cuts over.  *)
(* It loses its loaded entries and any cold load in flight; an entry      *)
(* reloads through a cold load. It loads every initiator's floor from the  *)
(* routing-floor row.                                                      *)
(* The durable entries are rows in grain storage and survive.              *)
Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ loaded' = [i \in Inits |-> Unloaded]
    /\ walFloor' = rowFloor
    /\ rd' = [i \in Inits |-> Unloaded]
    /\ UNCHANGED <<gen, entryRow, rowFloor, live, pend, pstamp, next, author>>
    /\ UNCHANGED <<recreated, legacy, mInit, closedAt, markNew, caught, dup>>

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ \E i \in Inits : Admit(i)
    \/ \E i \in Inits : Alloc(i)
    \/ \E i \in Inits : PreArmAlloc(i)
    \/ \E i \in Inits : ColdLoadRead(i)
    \/ \E i \in Inits : ColdLoadApply(i)
    \/ \E i \in Inits : Refuse(i)
    \/ Purge
    \/ LegacyPurge
    \/ CatchUp
    \/ CatchUpSkip
    \/ FirstInit
    \/ S2Init
    \/ Restart
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The guarantees. Each is a checked invariant, and each is what the core  *)
(* module assumes of the routing floor.                                    *)
(***************************************************************************)

(* L's marker reads initialised to a re-created leaf. A re-created leaf    *)
(* exists only once its shard root has re-seeded the tombstone, so after a *)
(* 10.2.0 purge it is held until the re-seed has run.                      *)
MarkerReadsInit == mInit /\ ~(legacy /\ ~caught)

(* Every offset allocated to the purged incarnation's shard roots - a      *)
(* straddler - lies below the close the re-created incarnation's first     *)
(* initialisation recorded, whenever L's marker reads initialised. A       *)
(* straddler is allocated before that close, or not at all.                *)
StraddlerBelowInit ==
    recreated /\ MarkerReadsInit => \A o \in Offs : author[o] # NoAuthor => o < closedAt

(* Every live initiator's generation is above its own routing floor, so    *)
(* the WAL never refuses a live shard root's claim and #5705's guard never *)
(* fails a live tree's write as deleted. The WAL-side floor never exceeds  *)
(* the row's, so the row is the stronger bound.                            *)
LiveGenerationAboveFloor ==
    \A i \in Inits : live[i] => gen[i] > rowFloor[i]

(* The first initialisation records a head above every offset allocated    *)
(* before it, whoever allocated it: the head is its claim's, the allocator *)
(* head at the claim (E's ClaimHeadCoversPriorAppends), not a bound on any *)
(* one author's appends. Every allocation here is a straddler, but the     *)
(* core relies on the same head for a foreign record a window the purge    *)
(* left open holds.                                                        *)
InitHeadCoversPriorAllocations ==
    [][~mInit /\ mInit' => \A o \in Offs : author[o] # NoAuthor => o < closedAt']_vars
=============================================================================
