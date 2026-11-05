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
          WithUnclaimedSurvivor, WithClaimedPurge, WithPreArm, WithKeyMove, WithFreeAdvance,
          WithSplitHint, WithSplitBirth, WithDonorRestart,
          WithDuplicate, WithEvict, WithOrphanForward

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
NoAct == "none"
Foreign == "f"
OldAuthor == "o"
Straddler == "s"
PreAuthor == "p"
NoAuthor == "x"
StaleDonor == "d"
DonorLive == "v"
OrphanFwd == "w"
Authors == Acts \cup {Foreign, OldAuthor, Straddler, PreAuthor, StaleDonor, DonorLive, OrphanFwd}
MaxGen == 3

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
    oldRejected,  \* ghost: an unstamped append was rejected
    preArm,       \* the offsets appended under the PreArm class (#5833)
    seenP,        \* the leaf-scoped watermark reported to each activation: its foreignHigh
    startHead,    \* the WAL shard's allocator head when its activation began
    evStart,      \* the allocator head when L's foreign-high entry was last evicted; 0 never
    evFloor,      \* the shard's eviction floor: the highest high of an evicted entry
    gD,           \* the shard root's durable routing generation
    gL,           \* the generation loaded (claimed) at the partition
    pend,         \* L's marker holds a deferred close head at the partition
    pendGen,      \* the generation that deferred head is to re-claim
    moved,        \* a key-moving close into L has happened
    otherMoved,   \* a later key-moving close into another receiver, re-claimed by its arm
    dUsed,        \* the stale donor has made its one write for L's keys
    reclaims,     \* ghost: generation re-claims an arm wrote at the partition
    strayReclaims, \* ghost: generation claims written anywhere but an arm
    sHi,          \* the WAL shard's staleHigh: the highest offset admitted under the generation a key-moving close raises past
    seenS,        \* the staleHigh reported to each activation
    ft,           \* each activation's fixed free-advance target; -1 none
    sh,           \* the allocator head each activation's split captured; -1 none
    sibHint,      \* the split sibling's hint (inclusive); -1 none
    sibRows,      \* the rows the split handed the sibling
    tRecs,        \* the split transfer's records, appended by L under the per-leaf class
    dNarrowed,    \* WithSplitBirth: L's live donor has narrowed its span
    sDone,        \* WithSplitBirth: the donor's stragglers have reached L
    dKnown,       \* WithSplitBirth: the moved-key writes the donor's closing activation knows it made
    dResumed      \* WithDonorRestart: the split was resumed by a new donor activation

walVars == <<next, author, inflight, durable, acked, floor>>
leafVars == <<up, cache, rp, mine, snapCov, snapRows>>
epochVars == <<cur, claimed, claimHead, stale, dupUsed>>
handVars == <<mInit, hoOpen, hoClosedAt, windows>>
oldVars == <<lastU, seenU, oldUsed, restarted, oldRejected>>
envVars == <<recreated, reinit, spUsed, phase, hint>>
xferVars == <<tRecs, dNarrowed, sDone, dKnown, dResumed>>
freeVars == <<sHi, seenS, ft, sh, sibHint, sibRows, xferVars>>
armVars == <<preArm, seenP, startHead, evStart, evFloor, freeVars>>
moveVars == <<gD, gL, pend, pendGen, moved, otherMoved, dUsed, reclaims, strayReclaims>>

vars == <<walVars, leafVars, epochVars, handVars, oldVars, envVars, armVars, moveVars>>

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
    /\ preArm \subseteq Offs
    /\ seenP \in [Acts -> -1..(MaxOff - 1)]
    /\ startHead \in 0..MaxOff
    /\ evStart \in 0..MaxOff
    /\ evFloor \in -1..(MaxOff - 1)
    /\ gD \in 1..MaxGen
    /\ gL \in 1..MaxGen
    /\ pend \in BOOLEAN
    /\ pendGen \in 0..MaxGen
    /\ moved \in BOOLEAN
    /\ otherMoved \in BOOLEAN
    /\ dUsed \in BOOLEAN
    /\ reclaims \in 0..2
    /\ strayReclaims \in 0..2
    /\ sHi \in -1..(MaxOff - 1)
    /\ seenS \in [Acts -> -1..(MaxOff - 1)]
    /\ ft \in [Acts -> -1..(MaxOff - 1)]
    /\ sh \in [Acts -> -1..MaxOff]
    /\ sibHint \in -1..(MaxOff - 1)
    /\ sibRows \subseteq Offs
    /\ tRecs \subseteq Offs
    /\ dNarrowed \in BOOLEAN
    /\ sDone \in BOOLEAN
    /\ dKnown \subseteq Offs
    /\ dResumed \in BOOLEAN

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
    /\ hoOpen = (WithBulk \/ WithSplitBirth)
    /\ hoClosedAt = 0
    /\ windows = IF WithBulk \/ WithSplitBirth THEN 1 ELSE 0
    /\ lastU = -1
    /\ seenU = [a \in Acts |-> -1]
    /\ oldUsed = FALSE
    /\ restarted = FALSE
    /\ recreated = FALSE
    /\ reinit = FALSE
    /\ spUsed = FALSE
    /\ phase = IF WithBulk THEN "window" ELSE "live"
    /\ oldRejected = FALSE
    /\ preArm = {}
    /\ seenP = [a \in Acts |-> -1]
    /\ startHead = 0
    /\ evStart = 0
    /\ evFloor = -1
    /\ gD = 1
    /\ gL = 1
    /\ pend = FALSE
    /\ pendGen = 0
    /\ moved = FALSE
    /\ otherMoved = FALSE
    /\ dUsed = FALSE
    /\ reclaims = 0
    /\ strayReclaims = 0
    /\ sHi = -1
    /\ seenS = [a \in Acts |-> -1]
    /\ ft = [a \in Acts |-> -1]
    /\ sh = [a \in Acts |-> -1]
    /\ sibHint = -1
    /\ sibRows = {}
    /\ tRecs = {}
    /\ dNarrowed = FALSE
    /\ sDone = FALSE
    /\ dKnown = {}
    /\ dResumed = FALSE

(***************************************************************************)
(* B's BulkHintBound, as an environment step. Create: the bulk-born leaf's *)
(* first activation, its read position anywhere it holds every record at *)
(* or below, and possibly more: BulkHintBound exactly. B's protocol starts *)
(* it at or below its birth hint, but B does not guarantee that.           *)
(***************************************************************************)
Allocated == {o \in Offs : author[o] # NoAuthor}

Create ==
    /\ UNCHANGED <<armVars, moveVars>>
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
(***************************************************************************)
(* The lazy arm (#5833, WithPreArm). foreignHigh is what the WAL shard     *)
(* returns for an activation of L: the highest offset any OTHER activation *)
(* of L allocated at the partition, stamped or PreArm, since the shard     *)
(* began tracking (its top two distinct activations by high offset), and   *)
(* never below what memory lost: the shard's start head minus one, and the *)
(* eviction floor.                                                         *)
(***************************************************************************)
TrackStart == Max2(startHead, evStart)

(* The token a forward is attributed to its forwarding leaf under (#5837,  *)
(* condition 4): the empty token, foreign to every activation of L, the    *)
(* forwarding one included.                                                *)
FwdToken == "empty"


ForeignHigh(a) ==
    Max({startHead - 1, evFloor} \cup {o \in Offs : o >= TrackStart /\ author[o] \in Acts \ {a}}
        \cup {o \in Offs : o >= TrackStart /\ author[o] = OrphanFwd /\ a # FwdToken})

(* The report a response carries under the lazy arm; nothing in eager mode. *)
ReportForeign(a) == seenP' = IF WithPreArm THEN [seenP EXCEPT ![a] = Max2(@, ForeignHigh(a))] ELSE seenP

(* The claimless advance (#5837, WithFreeAdvance) runs only under the lazy *)
(* arm. ReportStale(a): a routed response also carries its initiator's    *)
(* generation table (D2); the model has one key-moving close, so the      *)
(* table is the one entry it is read against: the highest offset admitted *)
(* under the generation the close raises past.                            *)
FreeOn == WithFreeAdvance /\ WithPreArm
ReportStale(a) == seenS' = IF FreeOn THEN [seenS EXCEPT ![a] = Max2(@, sHi)] ELSE seenS

(* Resolving the deferred close head (C2, C5): the arm re-claims the      *)
(* deferred generation at the partition in the same step, or takes the    *)
(* loaded one when a later raise is already re-claimed there, and records  *)
(* the allocator head as a close head on L's marker. A re-claim writes the *)
(* generation row only when it raises the loaded value.                   *)
ResolveDeferred ==
    /\ pend' = FALSE
    /\ gL' = Max2(gL, pendGen)
    /\ reclaims' = reclaims + (IF pendGen > gL THEN 1 ELSE 0)
    /\ hoClosedAt' = Max2(hoClosedAt, IF FreeOn THEN sHi + 1 ELSE next)
    /\ UNCHANGED <<gD, pendGen, moved, otherMoved, dUsed, strayReclaims>>

Claim(a) ==
    /\ a \in up
    /\ a \notin stale
    /\ ~claimed[a]
    /\ claimed' = [claimed EXCEPT ![a] = TRUE]
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ cur' = a
    /\ stale' = IF cur \in Acts THEN stale \cup {cur} ELSE stale
    /\ ReportForeign(a)
    /\ IF pend /\ gD >= pendGen THEN ResolveDeferred ELSE UNCHANGED <<moveVars, hoClosedAt>>
    /\ UNCHANGED <<walVars, leafVars, dupUsed, mInit, hoOpen, windows, oldVars, envVars>>
    /\ UNCHANGED <<preArm, startHead, evStart, evFloor, freeVars>>

(* Resolve(a): an armed activation finds a deferred head on its marker    *)
(* (a key-moving close since it armed) and resolves it by re-arming.       *)
Resolve(a) ==
    /\ a \in up
    /\ cur = a
    /\ pend
    /\ ResolveDeferred
    /\ ReportForeign(a)
    /\ gD >= pendGen
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, hoOpen, windows, oldVars, envVars>>
    /\ UNCHANGED <<preArm, startHead, evStart, evFloor, freeVars>>

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
    /\ ReportForeign(a)
    /\ ReportStale(a)
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars>>
    /\ UNCHANGED <<lastU, oldUsed, restarted, oldRejected, envVars>>
    /\ UNCHANGED <<preArm, startHead, evStart, evFloor, moveVars, sHi, ft, sh, sibHint, sibRows, xferVars>>

(* PreArmAlloc(a) (WithPreArm): an append by an activation that has not   *)
(* armed, under the PreArm class: no entry check and no row, admitted on  *)
(* the routing generation it carries (the current one, at or above the    *)
(* loaded one, so admitted without a claim: C1). It raises only L's       *)
(* leaf-scoped watermark, never the shared one, and its response reports  *)
(* the shared watermark as a stamped one does.                             *)
PreArmAlloc(a) ==
    /\ WithPreArm
    /\ a \in up
    /\ a \notin stale
    /\ ~claimed[a]
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ preArm' = preArm \cup {next}
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, lastU)]
    /\ IF FreeOn THEN ReportForeign(a) ELSE UNCHANGED seenP
    /\ ReportStale(a)
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars>>
    /\ UNCHANGED <<lastU, oldUsed, restarted, oldRejected, envVars>>
    /\ UNCHANGED <<startHead, evStart, evFloor, moveVars, sHi, ft, sh, sibHint, sibRows, xferVars>>

(* OwnAllocUnstamped(a) (WithGateFlip): an own append made unstamped - a  *)
(* closed gate view, exhausted claim rounds, a claim fault, or an         *)
(* unreachable leaf - by any running activation, superseded or not. It is *)
(* always admitted, raises the watermark, and reports itself.              *)
OwnAllocUnstamped(a) ==
    /\ UNCHANGED <<armVars, moveVars>>
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
       ELSE /\ acked' = IF author[o] \in {OldAuthor, Straddler, StaleDonor, DonorLive, OrphanFwd} THEN acked \cup {o} ELSE acked
            /\ UNCHANGED <<cache, mine>>
    /\ UNCHANGED <<next, author, floor, up, rp, snapCov, snapRows, epochVars, handVars, oldVars, envVars>>
    /\ UNCHANGED <<armVars, moveVars>>

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
    /\ UNCHANGED <<armVars, moveVars>>
    /\ phase = "live"
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, hoClosedAt, oldVars, envVars>>

ForeignAlloc ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ ~WithSplitBirth
    /\ hoOpen \/ ~mInit
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars, envVars>>

(* The heads a close may record: any head up to the allocator's above     *)
(* every allocated offset whose author is in S.                            *)
HeadsAbove(S) == {h \in 0..next : \A o \in Offs : o < next /\ author[o] \in S => o < h}

(* The authors a close's head is above. A bulk switch's close (WithBulk:  *)
(* the only window) records the allocator head at its re-claim, so under  *)
(* the lazy arm, which no longer replays to a claim head, it is also what *)
(* covers the records the range held before the switch (B's              *)
(* SwitchCloseCoversPriorAllocations).                                    *)
CloseCovers == IF WithPreArm /\ WithBulk THEN {Foreign, PreAuthor} ELSE {Foreign}

Close ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ ~WithSplitBirth
    /\ phase = "live"
    /\ hoOpen
    /\ hoOpen' = FALSE
    /\ \E h \in IF mInit THEN HeadsAbove(CloseCovers) ELSE 0..next : hoClosedAt' = Max2(hoClosedAt, h)
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, windows, oldVars, envVars>>

HandOff(o, a) ==
    /\ UNCHANGED <<armVars, moveVars>>
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
    /\ UNCHANGED <<armVars, moveVars>>
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
    /\ UNCHANGED <<armVars, moveVars>>
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
    /\ IF WithPreArm THEN rp[a] >= seenP[a] ELSE rp[a] >= claimHead[a] - 1
    /\ ~pend
    /\ ~hoOpen
    /\ rp[a] >= hoClosedAt - 1
    /\ rp[a] >= seenU[a]
    /\ \A o \in inflight : author[o] # a

LiveAdvance(a) ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ a \in up
    /\ mine[a] # {}
    /\ Armed(a)
    /\ Max(mine[a]) > rp[a]
    /\ rp' = [rp EXCEPT ![a] = Max(mine[a])]
    /\ UNCHANGED <<walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, oldVars, envVars>>

(***************************************************************************)
(* The claimless advance (#5837, WithFreeAdvance; D3). FreeTarget(a)      *)
(* fixes the target: the highest own applied append. FreeCommit(a) then   *)
(* reads L's marker view - after the target is fixed - and advances the   *)
(* read position to it with no claim, no WAL read and no row, iff:        *)
(*  - L's marker state is initialised, no window is open, and a has read  *)
(*    past its closes' heads;                                              *)
(*  - a has read past the foreignHigh, the shared unstamped watermark     *)
(*    and, while a deferred close is pending, the staleHigh reported to   *)
(*    it: the responses to its own appends, the target's included, so     *)
(*    every record allocated before the target is below them;             *)
(*  - every own append at or below the target is applied.                 *)
(***************************************************************************)
FreeTarget(a) ==
    /\ FreeOn
    /\ a \in up
    /\ mine[a] # {}
    /\ Max(mine[a]) > rp[a]
    /\ ft[a] # Max(mine[a])
    /\ ft' = [ft EXCEPT ![a] = Max(mine[a])]
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, sHi, seenS, sh, sibHint, sibRows, xferVars>>

FreeCommit(a) ==
    /\ FreeOn
    /\ a \in up
    /\ ft[a] > rp[a]
    /\ mInit
    /\ ~hoOpen
    /\ rp[a] >= hoClosedAt - 1
    /\ rp[a] >= seenP[a]
    /\ rp[a] >= seenU[a]
    /\ pend => rp[a] >= seenS[a]
    /\ \A o \in inflight : author[o] = a => o > ft[a]
    /\ rp' = [rp EXCEPT ![a] = ft[a]]
    /\ ft' = [ft EXCEPT ![a] = -1]
    /\ UNCHANGED <<walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, sHi, seenS, sh, sibHint, sibRows, xferVars>>

(***************************************************************************)
(* The split's hint (#5837, D4). SplitHeads(a): L, as a split's donor,    *)
(* captures the allocator head, and the capture returns the same bounds   *)
(* an append response does, in the same WAL-shard turn. SplitCommit(a):   *)
(* the donor then reads its marker view and, under FreeCommit's guard     *)
(* with the head in place of the target, hands the sibling the head minus *)
(* one (inclusive, #5760) as its hint, with every row it holds below it.  *)
(* One split per behaviour. The sibling's own later safety rests on its   *)
(* deferred close; what the hint must give it is SiblingHintHonest.       *)
(***************************************************************************)
SplitHeads(a) ==
    /\ FreeOn
    /\ WithSplitHint
    /\ a \in up
    /\ sibHint = -1
    /\ sibRows = {}
    /\ \A b \in Acts : sh[b] = -1
    /\ sh' = [sh EXCEPT ![a] = next]
    /\ ReportForeign(a)
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, lastU)]
    /\ ReportStale(a)
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, envVars, moveVars>>
    /\ UNCHANGED <<lastU, oldUsed, restarted, oldRejected>>
    /\ UNCHANGED <<preArm, startHead, evStart, evFloor, sHi, ft, sibHint, sibRows, xferVars>>

SplitCommit(a) ==
    /\ FreeOn
    /\ a \in up
    /\ sh[a] # -1
    /\ mInit
    /\ ~hoOpen
    /\ rp[a] >= hoClosedAt - 1
    /\ rp[a] >= seenP[a]
    /\ rp[a] >= seenU[a]
    /\ pend => rp[a] >= seenS[a]
    /\ \A o \in Offs : o < sh[a] /\ author[o] = a => o \in cache[a]
    /\ sibHint' = sh[a] - 1
    /\ sibRows' = {o \in cache[a] : o < sh[a]}
    /\ sh' = [sh EXCEPT ![a] = -1]
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, sHi, seenS, ft, xferVars>>

(***************************************************************************)
(* A split-born receiver (#5837, WithSplitBirth). L is born by a split,   *)
(* its birth window open and its rows at or below its hint (the split's   *)
(* heads; here nothing) handed to it. Until it narrows, the live donor    *)
(* still admits writes for L's keys (DonorAlloc), attributed to the donor *)
(* and so in none of L's bounds. The donor narrows (DonorNarrow), drains  *)
(* the moved-key mutations it admitted, and closes L's birth window       *)
(* (DonorClose) with the head one above its own highest moved-key write   *)
(* since the heads - which it knows from its own append responses and     *)
(* tracks anyway, for the stragglers - not the partition's head. It      *)
(* transfers what it applied since the heads to L as stragglers           *)
(* (Straggler): an append by L's own activation, applied in the same      *)
(* call, under the per-leaf class: in its tracker and its token's         *)
(* foreignHigh, never a claim, never the shared watermark (T3). The copy  *)
(* covers the originals only for the activation that applied it, so it   *)
(* is not what bounds them: the close head is. Then L is linked: KeyMove, *)
(* the shard root's raise and deferred head. A stale duplicate of the     *)
(* donor writes for L's keys under the generation before the link,        *)
(* attributed to the donor leaf: StaleRouteAlloc, which also stands for a *)
(* stale duplicate receiver's transfer of keys it no longer owns (T1),    *)
(* since the transfer carries the routing generation its span was         *)
(* established under.                                                      *)
(***************************************************************************)
(* The records the donor's close head is above: every append of the donor *)
(* leaf since the heads, by any of its activations, once the split was     *)
(* resumed; otherwise the closing activation's own, which are all of them. *)
DonorCloseCovers == IF dResumed THEN {o \in Offs : author[o] = DonorLive} ELSE dKnown

DonorAlloc ==
    /\ FreeOn
    /\ WithSplitBirth
    /\ ~dNarrowed
    /\ \A o \in Offs : author[o] # DonorLive
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = DonorLive]
    /\ inflight' = inflight \cup {next}
    /\ dKnown' = dKnown \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, sHi, seenS, ft, sh, sibHint, sibRows>>
    /\ UNCHANGED <<tRecs, dNarrowed, sDone, dResumed>>

DonorNarrow ==
    /\ FreeOn
    /\ WithSplitBirth
    /\ ~dNarrowed
    /\ dNarrowed' = TRUE
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, sHi, seenS, ft, sh, sibHint, sibRows, tRecs, sDone, dKnown, dResumed>>

DonorClose ==
    /\ FreeOn
    /\ WithSplitBirth
    /\ hoOpen
    /\ dNarrowed
    /\ \A o \in inflight : o \notin dKnown
    /\ hoOpen' = FALSE
    /\ hoClosedAt' = Max2(hoClosedAt, Max({-1} \cup DonorCloseCovers) + 1)
    /\ pend' = TRUE
    /\ pendGen' = gD + 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, mInit, windows, oldVars, envVars, armVars>>
    /\ UNCHANGED <<gD, gL, moved, otherMoved, dUsed, reclaims, strayReclaims>>

(* DonorRestart (WithDonorRestart): the donor restarts after taking the    *)
(* heads and before closing, and a new activation resumes the split from   *)
(* its durable record. It never learns the old activation's writes, and    *)
(* the old activation's token is no longer the live one: the WAL counts   *)
(* its records in the donor's staleHigh, and any still in flight lands    *)
(* there as a stale donor's write (StaleRouteAlloc).                      *)
DonorRestart ==
    /\ FreeOn
    /\ WithDonorRestart
    /\ hoOpen
    /\ ~dResumed
    /\ dResumed' = TRUE
    /\ dKnown' = {}
    /\ sHi' = Max({sHi} \cup {o \in Offs : author[o] = DonorLive})
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, moveVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, seenS, ft, sh, sibHint, sibRows>>
    /\ UNCHANGED <<tRecs, dNarrowed, sDone>>

Straggler(a) ==
    /\ FreeOn
    /\ WithSplitBirth
    /\ a \in up
    /\ ~sDone
    /\ dNarrowed
    /\ \A o \in inflight : author[o] # DonorLive
    /\ next < MaxOff
    /\ sDone' = TRUE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ tRecs' = tRecs \cup {next}
    /\ cache' = [cache EXCEPT ![a] = @ \cup {o \in durable : author[o] = DonorLive}]
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, lastU)]
    /\ ReportForeign(a)
    /\ ReportStale(a)
    /\ UNCHANGED <<durable, acked, floor, up, rp, mine, snapCov, snapRows, epochVars, handVars>>
    /\ UNCHANGED <<lastU, oldUsed, restarted, oldRejected, envVars, moveVars>>
    /\ UNCHANGED <<preArm, startHead, evStart, evFloor, sHi, ft, sh, sibHint, sibRows, dNarrowed, dKnown, dResumed>>

(***************************************************************************)
(* An orphan's forward (#5837, condition 4; WithOrphanForward). The stale  *)
(* duplicate a2 of L, holding a span from before a split the live          *)
(* activation never made, splits it off to a sibling O that is never       *)
(* linked, and forwards a new acknowledged write of a key L still owns to  *)
(* O, out of its own span view, so it never applies it. Under #5837's      *)
(* transfer and forward class the record is O's PreArm append, under the  *)
(* generation a2's span was established at, which nothing raises; it is   *)
(* attributed to O, and also to its forwarder, L, under FwdToken.          *)
(***************************************************************************)
OrphanForward ==
    /\ FreeOn
    /\ WithOrphanForward
    /\ "a2" \in up
    /\ \A o \in Offs : author[o] # OrphanFwd
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = OrphanFwd]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars, envVars, armVars, moveVars>>

(* Capture(a): the shared per-leaf snapshot keeps the highest coverage.  *)
Capture(a) ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ a \in up
    /\ rp[a] > snapCov
    /\ snapCov' = rp[a]
    /\ snapRows' = {o \in cache[a] : o <= rp[a]}
    /\ UNCHANGED <<walVars, up, cache, rp, mine, epochVars, handVars, oldVars, envVars>>

(* GcTrim: trim the next durable entry below the leaf's pin, the snapshot's coverage. *)
GcTrim ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ floor <= snapCov
    /\ floor \in durable
    /\ floor' = floor + 1
    /\ UNCHANGED <<next, author, inflight, durable, acked, leafVars, epochVars, handVars, oldVars, envVars>>

(* Duplicate: a second activation of L, or the next one, starts from the   *)
(* shared snapshot. It must claim before it appends stamped.               *)
Duplicate ==
    /\ UNCHANGED <<armVars, moveVars>>
    /\ WithDuplicate
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
    /\ startHead' = IF WithPreArm THEN next ELSE startHead
    /\ sHi' = IF FreeOn THEN Max2(sHi, next - 1) ELSE sHi
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, seenU, oldUsed, oldRejected, envVars>>
    /\ UNCHANGED <<preArm, seenP, evStart, evFloor, moveVars, seenS, ft, sh, sibHint, sibRows, xferVars>>

(* Evict (WithRestart): the WAL shard evicts L's foreign-high entry to     *)
(* bound memory, raising its eviction floor to the entry's highest offset. *)
Evict ==
    /\ WithRestart
    /\ WithEvict
    /\ WithPreArm
    /\ evStart = 0
    /\ next > 0
    /\ evStart' = next
    /\ evFloor' = Max({evFloor} \cup {o \in Offs : o >= TrackStart /\ author[o] \in Acts})
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars>>
    /\ UNCHANGED <<preArm, seenP, startHead, moveVars, freeVars>>

(***************************************************************************)
(* A key-moving close into L (#5833, WithKeyMove): a split or a            *)
(* consolidation hands keys to L. KeyMove: the shard root, in its own turn *)
(* as it links the split (C6), raises its routing generation, with no WAL  *)
(* claim, and closes L's marker with a deferred head at the partition: the *)
(* raised generation, to be re-claimed by L's next arm there (C2, C3).    *)
(* OtherReceiverArm: a later key-moving close into another receiver, whose *)
(* arm re-claims that later generation at the partition before L arms     *)
(* (C5). StaleRouteAlloc: a stale activation of the donor, still routing  *)
(* L's keys to itself under the generation before the close, appends one  *)
(* of them as PreArm, admitted while the loaded generation is not above   *)
(* its stamp. UnroutedAlloc: the stale donor's write for L's keys with no *)
(* routing stamp, appended truly unstamped and so raising the shared      *)
(* watermark (C4). Either is the donor's one write.                       *)
(***************************************************************************)
KeyMove ==
    /\ WithKeyMove
    /\ phase = "live"
    /\ ~moved
    /\ moved' = TRUE
    /\ gD' = gD + 1
    /\ pend' = TRUE
    /\ pendGen' = gD + 1
    /\ WithSplitBirth => ~hoOpen
    /\ UNCHANGED <<gL, otherMoved, dUsed, reclaims, strayReclaims>>
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, armVars>>

OtherReceiverArm ==
    /\ WithKeyMove
    /\ moved
    /\ ~otherMoved
    /\ ~WithSplitBirth
    /\ gD + 1 <= MaxGen
    /\ otherMoved' = TRUE
    /\ gD' = gD + 1
    /\ gL' = gD + 1
    /\ UNCHANGED <<pend, pendGen, moved, dUsed, reclaims, strayReclaims>>
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, oldVars, envVars, armVars>>

StaleRouteAlloc ==
    /\ WithKeyMove
    /\ moved \/ WithSplitBirth
    /\ ~dUsed
    /\ next < MaxOff
    /\ 1 >= gL
    /\ dUsed' = TRUE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = StaleDonor]
    /\ inflight' = inflight \cup {next}
    /\ sHi' = IF FreeOn THEN next ELSE sHi
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, oldVars, envVars>>
    /\ UNCHANGED <<gD, gL, pend, pendGen, moved, otherMoved, reclaims, strayReclaims>>
    /\ UNCHANGED <<preArm, seenP, startHead, evStart, evFloor, seenS, ft, sh, sibHint, sibRows, xferVars>>

UnroutedAlloc ==
    /\ WithKeyMove
    /\ moved \/ WithSplitBirth
    /\ ~dUsed
    /\ next < MaxOff
    /\ dUsed' = TRUE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = StaleDonor]
    /\ inflight' = inflight \cup {next}
    /\ lastU' = next
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, envVars>>
    /\ UNCHANGED <<seenU, oldUsed, restarted, oldRejected>>
    /\ UNCHANGED <<gD, gL, pend, pendGen, moved, otherMoved, reclaims, strayReclaims, armVars>>

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
    /\ UNCHANGED <<armVars, moveVars>>
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
    /\ UNCHANGED <<armVars, moveVars>>
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
    /\ UNCHANGED <<armVars, moveVars>>
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
    \/ \E a \in Acts : PreArmAlloc(a)
    \/ \E a \in Acts : Resolve(a)
    \/ \E o \in Offs : Land(o)
    \/ ForeignOpen
    \/ ForeignAlloc
    \/ Close
    \/ \E o \in Offs, a \in Acts : HandOff(o, a)
    \/ OldAlloc
    \/ \E a \in Acts : Replay(a)
    \/ \E a \in Acts : LiveAdvance(a)
    \/ \E a \in Acts : FreeTarget(a)
    \/ \E a \in Acts : FreeCommit(a)
    \/ \E a \in Acts : SplitHeads(a)
    \/ \E a \in Acts : SplitCommit(a)
    \/ DonorAlloc
    \/ DonorNarrow
    \/ DonorClose
    \/ DonorRestart
    \/ \E a \in Acts : Straggler(a)
    \/ OrphanForward
    \/ \E a \in Acts : Capture(a)
    \/ GcTrim
    \/ Duplicate
    \/ Restart
    \/ Evict
    \/ KeyMove
    \/ OtherReceiverArm
    \/ StaleRouteAlloc
    \/ UnroutedAlloc
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
    \/ (claimed[a] \/ FreeOn) /\ (IF WithPreArm THEN rp[a] < seenP[a] ELSE rp[a] < claimHead[a] - 1)
    \/ rp[a] < hoClosedAt - 1
    \/ rp[a] < seenU[a]
    \/ FreeOn /\ pend /\ rp[a] < seenS[a]

(* ArmTrigger(a) (#5833): under the lazy arm a leaf arms only when it     *)
(* needs a live advance - its pin holds the WAL floor, i.e. an own durable *)
(* append the shared snapshot does not yet cover. Claims stay possible at  *)
(* any time for safety; only their fairness is conditioned on the trigger. *)
(* Under the claimless advance no claim is fair: the pin must become      *)
(* usable by the free advance alone.                                       *)
ArmTrigger(a) == ~WithPreArm \/ (~FreeOn /\ \E o \in durable : author[o] \in Acts /\ o > snapCov)

Progress ==
    \/ \E a \in Acts :
        \/ ArmTrigger(a) /\ Claim(a)
        \/ ~FreeOn /\ Resolve(a)
        \/ FreeTarget(a)
        \/ FreeCommit(a)
        \/ ReplayNeeded(a) /\ Replay(a)
        \/ LiveAdvance(a)
        \/ Capture(a)
    \/ \E o \in Offs : Land(o)
    \/ Close
    \/ Create
    \/ MarkInit
    \/ DonorNarrow
    \/ DonorClose

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

(* The split's hint (#5837, #5752): the sibling is handed every durable  *)
(* record at or below its hint.                                            *)
SiblingHintHonest ==
    \A o \in durable : o <= sibHint => o \in sibRows

(* The split transfer's cost witness (#5837): a record appended under the *)
(* per-leaf class never raises the partition's shared unstamped watermark. *)
TransferCostWitness == lastU \notin tRecs

(* The deferred close's cost witness (#5837): a split-born receiver's      *)
(* resolved deferred head is the stale donor's bound plus one, never the   *)
(* partition's allocator head.                                             *)
DeferredHeadCostWitness ==
    WithSplitBirth => hoClosedAt <= Max2(sHi, Max({-1} \cup {o \in Offs : author[o] = DonorLive})) + 1

(* The widened excuse is for duplicates only: a sole running activation    *)
(* never falls off the log, since the GC never trims past its own pin.     *)
SoleActivationNeverFallsOff ==
    Cardinality(up) = 1 => \A a \in up : rp[a] + 1 >= floor

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

(* The lazy arm's cost witnesses (#5833). A PreArm append never raises the *)
(* partition's shared unstamped watermark, so no activation is ever told   *)
(* to replay past one of its own PreArm appends: on a growing tree, where  *)
(* unarmed leaves dominate, the shared watermark would otherwise hold every *)
(* armed leaf of the partition on the replay-based advance.                 *)
PreArmCostWitness ==
    \A a \in Acts : seenU[a] \in preArm => author[seenU[a]] # a

(* A key-moving close writes no WAL generation claim, and the deferred     *)
(* re-claim writes the generation row at most once per raise and           *)
(* partition: an append never claims a higher generation it carries (C1).  *)
ReclaimCostBound == strayReclaims = 0 /\ reclaims <= 1

(* The current activation has fallen off the log: the GC trimmed an entry *)
(* it must still replay. It is reachable in production too: the leaf's WAL *)
(* pin is one per-leaf row merged monotonic-max, so a duplicate            *)
(* activation's capture can raise it past the current activation's         *)
(* checkpoint. Production's response is the existing #3477 path: the       *)
(* partition latches stale and refuses hints and the live advance, and     *)
(* deactivation clears it, the reactivation rehydrating from the newer     *)
(* snapshot. No progress is owed by the stale activation meanwhile.        *)
(* Under the claimless advance (#5837) no activation need ever claim, so   *)
(* there is no current one to name: the latch applies to whichever running *)
(* activation has fallen off, as it does in production.                    *)
CurrentFellOffLog ==
    IF FreeOn THEN \E a \in up : rp[a] + 1 < floor
    ELSE cur \in up /\ rp[cur] + 1 < floor

(* The armed newborn (LiveAdvanceDesign.md, member 6): every own append    *)
(* that becomes durable is eventually covered by the shared snapshot, so   *)
(* the leaf's pin becomes usable without a replay from -1, unless the      *)
(* current activation has fallen off the log. Under LiveSpec only the live *)
(* advance carries the read position past the arming bounds, so this holds *)
(* only if the newborn eventually arms and advances.                       *)
NewbornPinEventuallyUsable ==
    <>[](CurrentFellOffLog \/ \A o \in durable : author[o] \in Acts => snapCov >= o)
=============================================================================
