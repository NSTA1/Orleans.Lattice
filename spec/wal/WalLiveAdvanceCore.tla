------------------------- MODULE WalLiveAdvanceCore -------------------------
(***************************************************************************)
(* The core of the fenced live checkpoint advance (epic #5605): when one   *)
(* leaf L's activation may advance its read position over its own applied *)
(* appends without reading the WAL, and what that does to the shared      *)
(* snapshot and the GC trim. It is module C of five focused modules       *)
(* promoted from the design model spec/wal/design/WalLiveAdvance.tla, and  *)
(* the sink of their acyclic assume-guarantee graph: it assumes every     *)
(* guarantee of the append-epoch (E), hand-off (H), routing-floor (R) and *)
(* bulk-switch (B) modules, as stated in LiveAdvanceInterface.md, and     *)
(* models each only as an environment constraint. It never models their   *)
(* state machines:                                                         *)
(*  - Own appends (E): a claim makes one activation current, with its     *)
(*    claim head the allocator head at the raise; only the current        *)
(*    activation allocates a stamped append, so a superseded one never    *)
(*    allocates again and no two share a raised epoch.                    *)
(*  - Foreign appends (H): allocated only while L's marker is open or not *)
(*    initialised; once it is, a close records a head above them,         *)
(*    max-merged into closedAt; an uninitialised marker holds L.          *)
(*  - Straddler appends (R): allocated only after a purge and before the   *)
(*    re-created incarnation's first initialisation, which records a head *)
(*    above every allocation before it.                                   *)
(*  - A bulk-born leaf (B): born holding every record at or below its     *)
(*    read position, with its marker open.                                *)
(*  - Unstamped appends: always admitted and always reported. The core    *)
(*    models the partition's unstamped watermark itself, its reset on a   *)
(*    WAL shard restart, and an activation's own unstamped append         *)
(*    reporting itself.                                                   *)
(*                                                                         *)
(* The core owns Armed(a) (LiveAdvanceDesign.md, section 1, "Arming"),    *)
(* the live advance, Replay, the shared snapshot's capture, the GC trim,  *)
(* the applied-prefix tracker, the unstamped watermark and self-report,   *)
(* and the duplicate activation.                                          *)
(*                                                                         *)
(* Safety, and one liveness property, NewbornPinEventuallyUsable (epic    *)
(* #5605, member 6b), checked under LiveSpec's explicit fairness on the   *)
(* base configuration only.                                                *)
(*                                                                         *)
(* See LiveAdvanceCoreRefinement.md for the mapping to production, the    *)
(* interface, and what this model does NOT cover.                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

(***************************************************************************)
(* Instance. MaxWindows bounds the foreign hand-off windows. The switches *)
(* turn the environment steps on: WithMixedVersion (a 10.2.0 silo's       *)
(* unstamped append), WithGateFlip (an activation's own unstamped         *)
(* appends), WithRestart (a WAL shard restart), WithRecreate (L's tree    *)
(* purged and re-created, with a straddler of the purged incarnation) and *)
(* WithBulk (L born by a bulk switch). WithClaimedPurge and               *)
(* WithUnclaimedSurvivor slice the purge by whether it finds a1 claimed,  *)
(* to keep each configuration within the TLC budget (see Recreate).       *)
(***************************************************************************)
CONSTANTS MaxWindows, WithMixedVersion, WithGateFlip, WithRestart, WithRecreate, WithBulk,
          WithUnclaimedSurvivor, WithClaimedPurge

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
NoAct == "none"
Foreign == "f"
OldAuthor == "o"
Straddler == "s"
PreAuthor == "p"
NoAuthor == "x"
Authors == Acts \cup {Foreign, OldAuthor, Straddler, PreAuthor}

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    next,        \* the WAL shard's allocator: the next offset it assigns
    author,      \* the author each allocated offset was allocated to
    inflight,    \* allocated, not yet durable
    durable,     \* durable entries
    acked,       \* acknowledged entries
    floor,       \* the GC trim floor: every offset below it is trimmed
    up,          \* L's running activations
    cache,       \* each activation's projection: the entries it holds
    rp,          \* each activation's read position (its checkpoint)
    mine,        \* the applied-prefix tracker: each activation's own applied appends
    snapCov,     \* the shared per-leaf snapshot's coverage
    snapRows,    \* the shared snapshot's rows
    cur,         \* E: the current activation, whose raised epoch is the highest
    claimed,     \* E: each activation's claim at the partition is raised
    claimHead,   \* E: the allocator head each activation's raise returned
    stale,       \* E: activations that may never claim or allocate stamped again
    dupUsed,     \* the duplicate activation has activated
    mInit,       \* H: L's marker state is initialised
    hoOpen,      \* H: L's marker is open
    hoClosedAt,  \* H: the maximum of the heads every close (and the initialisation) recorded
    windows,     \* H: hand-off windows opened so far
    lastU,       \* the partition's unstamped watermark: the highest unstamped offset
    seenU,       \* the highest unstamped offset reported to each activation
    oldUsed,     \* the 10.2.0 silo has made its unstamped append
    restarted,   \* the WAL shard has restarted
    recreated,   \* L's tree has been purged and re-created
    reinit,      \* R: the re-created incarnation's first initialisation recorded its heads
    spUsed,      \* R: the straddler has been allocated
    phase,       \* B: "window" while a bulk switch is creating L, then "live"
    hint,        \* B: the bulk-born leaf's birth hint
    oldRejected  \* ghost: an unstamped append was rejected

walVars == <<next, author, inflight, durable, acked, floor>>
leafVars == <<up, cache, rp, mine, snapCov, snapRows>>
epochVars == <<cur, claimed, claimHead, stale, dupUsed>>
handVars == <<mInit, hoOpen, hoClosedAt, windows>>
oldVars == <<lastU, seenU, oldUsed, restarted, oldRejected>>
envVars == <<recreated, reinit, spUsed, phase, hint>>

vars == <<walVars, leafVars, epochVars, handVars, oldVars, envVars>>

TypeOK ==
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Authors \cup {NoAuthor}]
    /\ inflight \subseteq Offs
    /\ durable \subseteq Offs
    /\ acked \subseteq Offs
    /\ floor \in 0..MaxOff
    /\ up \subseteq Acts
    /\ cache \in [Acts -> SUBSET Offs]
    /\ rp \in [Acts -> -1..(MaxOff - 1)]
    /\ mine \in [Acts -> SUBSET Offs]
    /\ snapCov \in -1..(MaxOff - 1)
    /\ snapRows \subseteq Offs
    /\ cur \in Acts \cup {NoAct}
    /\ claimed \in [Acts -> BOOLEAN]
    /\ claimHead \in [Acts -> 0..MaxOff]
    /\ stale \subseteq Acts
    /\ dupUsed \in BOOLEAN
    /\ mInit \in BOOLEAN
    /\ hoOpen \in BOOLEAN
    /\ hoClosedAt \in 0..MaxOff
    /\ windows \in 0..MaxWindows
    /\ lastU \in -1..(MaxOff - 1)
    /\ seenU \in [Acts -> -1..(MaxOff - 1)]
    /\ oldUsed \in BOOLEAN
    /\ restarted \in BOOLEAN
    /\ recreated \in BOOLEAN
    /\ reinit \in BOOLEAN
    /\ spUsed \in BOOLEAN
    /\ phase \in {"window", "live"}
    /\ hint \in 0..MaxOff
    /\ oldRejected \in BOOLEAN

(***************************************************************************)
(* Init. Without WithBulk, L is a new tree's first leaf: a1 is running,   *)
(* has read nothing, and its marker was initialised at its birth. With    *)
(* WithBulk, a bulk switch is creating L: the range already holds up to   *)
(* one pre-existing record, the switch has opened L's marker and taken    *)
(* the WAL head as L's birth hint, and L is not yet running.              *)
(***************************************************************************)
Init ==
    /\ \E n \in (IF WithBulk THEN 0..1 ELSE {0}) :
        /\ next = n
        /\ author = [o \in Offs |-> IF o < n THEN PreAuthor ELSE NoAuthor]
        /\ durable = 0..(n - 1)
        /\ acked = 0..(n - 1)
        /\ hint = n
    /\ inflight = {}
    /\ floor = 0
    /\ up = IF WithBulk THEN {} ELSE {"a1"}
    /\ cache = [a \in Acts |-> {}]
    /\ rp = [a \in Acts |-> -1]
    /\ mine = [a \in Acts |-> {}]
    /\ snapCov = -1
    /\ snapRows = {}
    /\ cur = NoAct
    /\ claimed = [a \in Acts |-> FALSE]
    /\ claimHead = [a \in Acts |-> 0]
    /\ stale = {}
    /\ dupUsed = FALSE
    /\ mInit = TRUE
    /\ hoOpen = WithBulk
    /\ hoClosedAt = 0
    /\ windows = IF WithBulk THEN 1 ELSE 0
    /\ lastU = -1
    /\ seenU = [a \in Acts |-> -1]
    /\ oldUsed = FALSE
    /\ restarted = FALSE
    /\ recreated = FALSE
    /\ reinit = FALSE
    /\ spUsed = FALSE
    /\ phase = IF WithBulk THEN "window" ELSE "live"
    /\ oldRejected = FALSE

(***************************************************************************)
(* B's BulkHintBound, as an environment step. Create: the bulk-born leaf's *)
(* first activation, its read position anywhere it holds every record at *)
(* or below, and possibly more: BulkHintBound exactly. B's protocol starts *)
(* it at or below its birth hint, but B does not guarantee that.           *)
(***************************************************************************)
Allocated == {o \in Offs : author[o] # NoAuthor}

Create ==
    /\ phase = "window"
    /\ phase' = "live"
    /\ up' = {"a1"}
    /\ \E r \in -1..(next - 1), held \in SUBSET Allocated :
        /\ \A o \in Allocated : o <= r => o \in held
        /\ cache' = [cache EXCEPT !["a1"] = held]
        /\ rp' = [rp EXCEPT !["a1"] = r]
    /\ UNCHANGED <<walVars, mine, snapCov, snapRows, epochVars, handVars, oldVars>>
    /\ UNCHANGED <<recreated, reinit, spUsed, hint>>

(***************************************************************************)
(* E's guarantees, as environment steps. Claim(a): a's claim is raised: it *)
(* becomes the current activation, its claim head is the allocator head   *)
(* now (ClaimHeadCoversPriorAppends), and the activation it supersedes    *)
(* never allocates a stamped append again (StaleEpochNeverAppends,        *)
(* RaisedEpochUnique). A superseded activation, or one of a purged        *)
(* incarnation, never claims again.                                        *)
(***************************************************************************)
Claim(a) ==
    /\ a \in up
    /\ a \notin stale
    /\ ~claimed[a]
    /\ claimed' = [claimed EXCEPT ![a] = TRUE]
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ cur' = a
    /\ stale' = IF cur \in Acts THEN stale \cup {cur} ELSE stale
    /\ UNCHANGED <<walVars, leafVars, dupUsed, handVars, oldVars, envVars>>

(* OwnAlloc(a): only the current activation's stamped append is allocated. *)
(* The response reports the partition's unstamped watermark.               *)
OwnAlloc(a) ==
    /\ a \in up
    /\ cur = a
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, lastU)]
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars>>
    /\ UNCHANGED <<lastU, oldUsed, restarted, oldRejected, envVars>>

(* OwnAllocUnstamped(a) (WithGateFlip): an own append made unstamped - a  *)
(* closed gate view, exhausted claim rounds, a claim fault, or an         *)
(* unreachable leaf - by any running activation, superseded or not. It is *)
(* always admitted, raises the watermark, and reports itself.              *)
OwnAllocUnstamped(a) ==
    /\ WithGateFlip
    /\ a \in up
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ lastU' = next
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, next)]
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars>>
    /\ UNCHANGED <<oldUsed, restarted, oldRejected, envVars>>

(* Land(o): the append becomes durable. An own append is applied by its   *)
(* author, recorded by its tracker and acknowledged; the 10.2.0 silo's and *)
(* the straddler's writers acknowledge their own; a foreign entry is       *)
(* acknowledged only when it is handed off.                                *)
Land(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ IF author[o] \in up
       THEN /\ cache' = [cache EXCEPT ![author[o]] = @ \cup {o}]
            /\ mine' = [mine EXCEPT ![author[o]] = @ \cup {o}]
            /\ acked' = acked \cup {o}
       ELSE /\ acked' = IF author[o] \in {OldAuthor, Straddler} THEN acked \cup {o} ELSE acked
            /\ UNCHANGED <<cache, mine>>
    /\ UNCHANGED <<next, author, floor, up, rp, snapCov, snapRows, epochVars, handVars, oldVars, envVars>>

(***************************************************************************)
(* H's guarantees, as environment steps. ForeignOpen opens L's marker for *)
(* a hand-off window. ForeignAlloc: a foreign append is allocated only    *)
(* while the marker is open, or while it is not initialised, where        *)
(* HandOffBound, conditioned on the marker reading closed, says nothing.  *)
(* Close records any head up to the allocator's, above every foreign      *)
(* record once the marker is initialised, the weakest HandOffBound        *)
(* allows, and the marker max-merges it into closedAt (CloseHeadMonotone). *)
(* HandOff(o, a) delivers a foreign record to an activation, which is     *)
(* when it is acknowledged; it may never happen.                          *)
(***************************************************************************)
ForeignOpen ==
    /\ phase = "live"
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, hoClosedAt, oldVars, envVars>>

ForeignAlloc ==
    /\ hoOpen \/ ~mInit
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars, envVars>>

(* The heads a close may record: any head up to the allocator's above     *)
(* every allocated offset whose author is in S.                            *)
HeadsAbove(S) == {h \in 0..next : \A o \in Offs : o < next /\ author[o] \in S => o < h}

Close ==
    /\ phase = "live"
    /\ hoOpen
    /\ hoOpen' = FALSE
    /\ \E h \in IF mInit THEN HeadsAbove({Foreign}) ELSE 0..next : hoClosedAt' = Max2(hoClosedAt, h)
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, windows, oldVars, envVars>>

HandOff(o, a) ==
    /\ a \in up
    /\ o \in durable
    /\ author[o] = Foreign
    /\ o \notin acked
    /\ cache' = [cache EXCEPT ![a] = @ \cup {o}]
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<next, author, inflight, durable, floor, up, rp, mine, snapCov, snapRows>>
    /\ UNCHANGED <<epochVars, handVars, oldVars, envVars>>

(***************************************************************************)
(* OldAlloc (WithMixedVersion): a 10.2.0 silo's unstamped append. The      *)
(* shard admits it unless UnstampedAdmitted says otherwise, and raises    *)
(* the partition's unstamped watermark to its offset. Its writer           *)
(* acknowledges it.                                                        *)
(***************************************************************************)
UnstampedAdmitted == TRUE

OldAlloc ==
    /\ WithMixedVersion
    /\ phase = "live"
    /\ ~oldUsed
    /\ next < MaxOff
    /\ oldUsed' = TRUE
    /\ IF UnstampedAdmitted
       THEN /\ next' = next + 1
            /\ author' = [author EXCEPT ![next] = OldAuthor]
            /\ inflight' = inflight \cup {next}
            /\ lastU' = next
            /\ UNCHANGED oldRejected
       ELSE /\ oldRejected' = TRUE
            /\ UNCHANGED <<next, author, inflight, lastU>>
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, seenU, restarted, envVars>>

(* Replay(a): read and apply the next durable entry; never a trimmed one. *)
Replay(a) ==
    /\ a \in up
    /\ rp[a] + 1 < MaxOff
    /\ rp[a] + 1 \in durable
    /\ rp[a] + 1 >= floor
    /\ cache' = [cache EXCEPT ![a] = @ \cup {rp[a] + 1}]
    /\ rp' = [rp EXCEPT ![a] = rp[a] + 1]
    /\ UNCHANGED <<walVars, up, mine, snapCov, snapRows, epochVars, handVars, oldVars, envVars>>

(***************************************************************************)
(* Armed(a), the design note's "Arming" list, conjunct for conjunct:      *)
(*  - a's claim is raised at the partition;                                *)
(*  - L's marker state is initialised;                                     *)
(*  - a has replayed to its claim head;                                    *)
(*  - no marker is open, and a has replayed to its closes' max heads;      *)
(*  - a has read past every unstamped offset reported to it;              *)
(*  - every own append a issued is applied (the applied-prefix tracker).  *)
(* LiveAdvance(a): advance over a's own applied appends without reading.  *)
(***************************************************************************)
Armed(a) ==
    /\ claimed[a]
    /\ mInit
    /\ rp[a] >= claimHead[a] - 1
    /\ ~hoOpen
    /\ rp[a] >= hoClosedAt - 1
    /\ rp[a] >= seenU[a]
    /\ \A o \in inflight : author[o] # a

LiveAdvance(a) ==
    /\ a \in up
    /\ mine[a] # {}
    /\ Armed(a)
    /\ Max(mine[a]) > rp[a]
    /\ rp' = [rp EXCEPT ![a] = Max(mine[a])]
    /\ UNCHANGED <<walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, oldVars, envVars>>

(* Capture(a): the shared per-leaf snapshot keeps the highest coverage.  *)
Capture(a) ==
    /\ a \in up
    /\ rp[a] > snapCov
    /\ snapCov' = rp[a]
    /\ snapRows' = {o \in cache[a] : o <= rp[a]}
    /\ UNCHANGED <<walVars, up, cache, rp, mine, epochVars, handVars, oldVars, envVars>>

(* GcTrim: trim the next durable entry below the leaf's pin, the snapshot's coverage. *)
GcTrim ==
    /\ floor <= snapCov
    /\ floor \in durable
    /\ floor' = floor + 1
    /\ UNCHANGED <<next, author, inflight, durable, acked, leafVars, epochVars, handVars, oldVars, envVars>>

(* Duplicate: a second activation of L, or the next one, starts from the   *)
(* shared snapshot. It must claim before it appends stamped.               *)
Duplicate ==
    /\ phase = "live"
    /\ ~dupUsed
    /\ dupUsed' = TRUE
    /\ up' = up \cup {"a2"}
    /\ cache' = [cache EXCEPT !["a2"] = snapRows]
    /\ rp' = [rp EXCEPT !["a2"] = snapCov]
    /\ UNCHANGED <<walVars, mine, snapCov, snapRows, cur, claimed, claimHead, stale, handVars, oldVars, envVars>>

(* Restart (WithRestart): the WAL shard restarts, a reactivation or a      *)
(* move's cutover. Its watermark is in memory, so it restarts at the       *)
(* recovered allocator head minus one, taking everything before as         *)
(* possibly unstamped.                                                     *)
Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ lastU' = next - 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, seenU, oldUsed, oldRejected, envVars>>

(***************************************************************************)
(* The purge, H's AbsentMarkerHolds and R's guarantees, as environment    *)
(* steps (WithRecreate). Recreate: L's tree is purged and re-created. L's *)
(* marker is deleted, so its state is not initialised and holds L. The    *)
(* purge retires every raised epoch, so no activation is current: one that *)
(* claimed never claims again and never appends stamped. One that never   *)
(* claimed is not refused by E, whose clear finds no row of it, so it may *)
(* claim and become the re-created incarnation's activation (F1). Which   *)
(* activations are unclaimed at the purge selects the configuration's     *)
(* slice: WithUnclaimedSurvivor for a purge that finds a1 unclaimed,      *)
(* WithClaimedPurge for every other. StraddlerAlloc: a write a shard root *)
(* of the purged incarnation admitted is allocated, only before the       *)
(* re-created incarnation's first initialisation. MarkInit: that          *)
(* initialisation records, as a close, any head above every foreign and   *)
(* straddler record allocated before it (InitHeadCoversPriorAllocations), *)
(* max-merged into closedAt, and initialises L's marker state.             *)
(***************************************************************************)
Recreate ==
    /\ WithRecreate
    /\ phase = "live"
    /\ ~recreated
    /\ IF "a1" \in up /\ ~claimed["a1"] THEN WithUnclaimedSurvivor ELSE WithClaimedPurge
    /\ recreated' = TRUE
    /\ mInit' = FALSE
    /\ hoOpen' = FALSE
    /\ hoClosedAt' = 0
    /\ cur' = NoAct
    /\ UNCHANGED <<walVars, leafVars, claimed, claimHead, stale, dupUsed, windows, oldVars>>
    /\ UNCHANGED <<reinit, spUsed, phase, hint>>

StraddlerAlloc ==
    /\ recreated
    /\ ~reinit
    /\ ~spUsed
    /\ next < MaxOff
    /\ spUsed' = TRUE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Straddler]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars>>
    /\ UNCHANGED <<recreated, reinit, phase, hint>>

MarkInit ==
    /\ recreated
    /\ ~mInit
    /\ mInit' = TRUE
    /\ \E h \in HeadsAbove({Foreign, Straddler}) : hoClosedAt' = Max2(hoClosedAt, h)
    /\ reinit' = TRUE
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoOpen, windows, oldVars>>
    /\ UNCHANGED <<recreated, spUsed, phase, hint>>

(* A behaviour may stop anywhere: this is a safety model.                  *)
Stutter == UNCHANGED vars

Next ==
    \/ Create
    \/ \E a \in Acts : Claim(a)
    \/ \E a \in Acts : OwnAlloc(a)
    \/ \E a \in Acts : OwnAllocUnstamped(a)
    \/ \E o \in Offs : Land(o)
    \/ ForeignOpen
    \/ ForeignAlloc
    \/ Close
    \/ \E o \in Offs, a \in Acts : HandOff(o, a)
    \/ OldAlloc
    \/ \E a \in Acts : Replay(a)
    \/ \E a \in Acts : LiveAdvance(a)
    \/ \E a \in Acts : Capture(a)
    \/ GcTrim
    \/ Duplicate
    \/ Restart
    \/ Recreate
    \/ StraddlerAlloc
    \/ MarkInit
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Fairness, for NewbornPinEventuallyUsable. Weak fairness of the leaf's  *)
(* own steps - its claim, the apply of an append, the live advance and the *)
(* snapshot's capture - and of the environment steps that end a hold: a   *)
(* close, a bulk switch's creation, the first initialisation. A replay is *)
(* fair only while a bound the arming list names lies above the read      *)
(* position (ReplayNeeded). That is a deliberate under-approximation:     *)
(* production's starvation drive also replays from the checkpoint to the  *)
(* head, over the leaf's own appends, but the property does not rely on   *)
(* it, so it shows the live advance alone makes the pin usable. With       *)
(* replay fair unconditionally the property holds with no live advance at *)
(* all. Weakening a fairness assumption only strengthens the claim.       *)
(* Precondition: every step of Progress, and every other step but         *)
(* Stutter, can be taken only finitely often in every configuration, so   *)
(* one condition over the disjunction forces what one per step would. A   *)
(* configuration with an unbounded step needs a condition per step.       *)
(***************************************************************************)
ReplayNeeded(a) ==
    \/ claimed[a] /\ rp[a] < claimHead[a] - 1
    \/ rp[a] < hoClosedAt - 1
    \/ rp[a] < seenU[a]

Progress ==
    \/ \E a \in Acts :
        \/ Claim(a)
        \/ ReplayNeeded(a) /\ Replay(a)
        \/ LiveAdvance(a)
        \/ Capture(a)
    \/ \E o \in Offs : Land(o)
    \/ Close
    \/ Create
    \/ MarkInit

(* One weak-fairness condition over the disjunction. Given the finiteness  *)
(* precondition above, a fair behaviour ends in a state where no Progress  *)
(* step is enabled: exactly what one condition per step would force, at a  *)
(* fraction of TLC's liveness cost.                                        *)
Fairness == WF_vars(Progress)

LiveSpec == Spec /\ Fairness

(***************************************************************************)
(* The core's guarantees.                                                  *)
(***************************************************************************)

(* No running activation's read position passes a durable entry it does   *)
(* not hold. Unacknowledged entries count: a WAL reader applies them, so  *)
(* an owner that skipped one would diverge from its replicas and views.   *)
ReadPositionHonest ==
    \A a \in up : \A o \in durable : o <= rp[a] => o \in cache[a]

(* The shared snapshot never claims coverage of a durable entry it lacks. *)
SnapshotHonest ==
    \A o \in durable : o <= snapCov => o \in snapRows

(* The GC never trims an acknowledged append the snapshot lacks, whoever  *)
(* appended it.                                                            *)
NoTrimPastAcked ==
    \A o \in acked : o < floor => o \in snapRows

(* An unstamped append - a 10.2.0 silo's, or any while the gate is        *)
(* closed - is never rejected.                                             *)
OldNeverRejected == ~oldRejected

(* The current activation has fallen off the log: the GC trimmed an entry *)
(* it must still replay. It is reachable in production too: the leaf's WAL *)
(* pin is one per-leaf row merged monotonic-max, so a duplicate            *)
(* activation's capture can raise it past the current activation's         *)
(* checkpoint. Production's response is the existing #3477 path: the       *)
(* partition latches stale and refuses hints and the live advance, and     *)
(* deactivation clears it, the reactivation rehydrating from the newer     *)
(* snapshot. No progress is owed by the stale activation meanwhile.        *)
CurrentFellOffLog == cur \in up /\ rp[cur] + 1 < floor

(* The armed newborn (LiveAdvanceDesign.md, member 6): every own append    *)
(* that becomes durable is eventually covered by the shared snapshot, so   *)
(* the leaf's pin becomes usable without a replay from -1, unless the      *)
(* current activation has fallen off the log. Under LiveSpec only the live *)
(* advance carries the read position past the arming bounds, so this holds *)
(* only if the newborn eventually arms and advances.                       *)
NewbornPinEventuallyUsable ==
    <>[](CurrentFellOffLog \/ \A o \in durable : author[o] \in Acts => snapCov >= o)
=============================================================================
