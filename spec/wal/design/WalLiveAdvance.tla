---- MODULE WalLiveAdvance ----
(***************************************************************************)
(* Design model for the fenced live checkpoint advance (issues #5572,      *)
(* #5593, epic #5605). A draft: it describes production that does not      *)
(* exist yet, so it has no refinement note and sits outside spec module    *)
(* discovery; it becomes the companion module spec/wal/WalLiveAdvance once *)
(* production lands. See LiveAdvanceDesign.md beside it.                   *)
(*                                                                         *)
(* One WAL partition, one owner leaf L, and only entries L owns: entries   *)
(* other leaves own are invisible to L's replay and to this model.         *)
(*                                                                         *)
(* Authors of entries L owns:                                              *)
(*  - L's activations: a1, created by a bulk load, and a2, a duplicate the *)
(*    grain directory admits beside a running one, which also stands for   *)
(*    the next activation after a1 stops.                                  *)
(*  - A foreign author f, standing for every other writer of an entry L    *)
(*    owns: a split donor, a reclaim neighbour, an adaptive split's or a   *)
(*    shard fold's donor shard, and the leaf a write reached before a bulk *)
(*    switch. f's writes come in two kinds, point and batch. A foreign     *)
(*    entry is acknowledged only once it is handed off to L, and the       *)
(*    hand-off may never happen (its author crashed).                      *)
(*  - A pre-existing writer p, whose entries were in the range before a    *)
(*    bulk append began (#5610).                                           *)
(*                                                                         *)
(* The fences:                                                             *)
(*  - Epoch. Before its first append an activation persists the epoch it   *)
(*    read plus one to L's row, conditional on that read (a strict CAS     *)
(*    that never adopts), and claims it at the WAL shard, which raises its *)
(*    per-leaf entry and returns its allocator head. The shard rejects an  *)
(*    append below the entry. Retiring an entry raises the partition's     *)
(*    retired-epoch floor, which a claim into an absent entry must exceed. *)
(*    The in-memory entry is lost when the shard restarts and reloaded     *)
(*    when the leaf re-presents its claim; an append from an unloaded      *)
(*    entry is rejected until then.                                        *)
(*  - Hand-off. A foreign author opens a durable marker on L before its    *)
(*    first admission of a write L will own. It closes it by fencing: it   *)
(*    stops admitting, then re-claims its own epoch (or, for a shard-level *)
(*    initiator, its routing generation) at the WAL shard. A write it      *)
(*    admitted earlier carries the epoch it was admitted under, so if it   *)
(*    has not been allocated it is rejected, and if it has, it lies below  *)
(*    the allocator head the re-claim returns, which the close records.    *)
(*    A bulk load opens the marker at the leaf's birth and records the     *)
(*    head when it began, over a range it found empty, as the birth hint. *)
(*  - Armed. An activation advances over its own applied appends without   *)
(*    reading only once it has replayed to its claim head, it sees no      *)
(*    open marker, it has replayed to the last close's head, and every     *)
(*    append it issued has been applied.                                   *)
(*                                                                         *)
(* Safety: no read position passes a durable entry the activation does not *)
(* hold - acknowledged or not, because a WAL reader applies every durable  *)
(* entry - the shared snapshot is honest, the GC never trims an            *)
(* acknowledged entry the snapshot lacks, and no stale epoch ever appends, *)
(* across a shard restart too.                                             *)
(*                                                                         *)
(* Feature switches keep each check small: WithRecreate adds a purge and   *)
(* re-creation of L, WithRestart a WAL shard restart (a reactivation or a  *)
(* move's cutover), FrozenView makes a superseded activation keep the      *)
(* marker state it last saw, and Ghosts enables the witness ghosts.        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS WithRecreate, WithRestart, FrozenView, Ghosts

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
Foreign == "f"
PreAuthor == "p"
NoAuthor == "none"
Authors == Acts \cup {Foreign, PreAuthor}
Kinds == {"point", "batch"}
MaxWindows == 2
MaxEpoch == 3
Epochs == 0..MaxEpoch

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    phase,      \* "pre" before the bulk window opens, "window", then "live"
    hint,       \* the WAL head when the bulk load began: the leaf's birth hint
    next,       \* the WAL shard's allocator: the next offset it assigns
    author,     \* the author each allocated offset was allocated to
    inflight,   \* allocated, not yet durable
    durable,
    acked,
    floor,      \* the GC trim floor: every offset below it is trimmed
    up,         \* L's running activations
    cache,      \* each activation's projection
    rp,         \* each activation's read position (its checkpoint)
    mine,       \* each activation's own applied appends
    snapCov,    \* the shared snapshot's coverage
    snapRows,   \* the shared snapshot's rows
    rowEpoch,   \* the epoch persisted in L's row
    seen,       \* the row epoch each activation last read
    epoch,      \* each activation's claimed epoch; 0 before its claim
    claimHead,  \* the allocator head each activation's claim returned
    fence,      \* the WAL shard's durable epoch entry for L; 0 when absent
    memFence,   \* the shard's in-memory entry for L; -1 when not loaded
    retired,    \* the partition's retired-epoch floor
    maxClaimed, \* the highest epoch ever claimed for L
    hoOpen,     \* L's durable hand-off marker is open
    hoClosedAt, \* the allocator head the last close recorded
    windows,    \* hand-off windows opened so far
    fpend,      \* a foreign write is admitted and not yet allocated or rejected
    fkind,      \* its kind
    fstamp,     \* the epoch it was admitted under
    fEpoch,     \* the foreign author's current epoch (or routing generation)
    fFence,     \* the WAL shard's entry for the foreign author
    fz,         \* each activation has been superseded and froze its marker view
    fzOpen,     \* the marker state it froze
    fzClosedAt, \* the close head it froze
    dupUsed,
    restarted,
    recreated,
    staleAccepted,        \* an append from an epoch below maxClaimed was allocated
    rejected,             \* witness: a stale epoch's append was rejected
    fRejected,            \* witness: an admitted foreign write was fenced out
    advanced,             \* witness: some activation advanced live
    advancedPastForeign,  \* witness: a live advance passed a foreign entry
    dupAdvanced,          \* witness: the duplicate advanced live
    trimAfterAdvance,     \* witness: the GC trimmed after a live advance
    reloaded,             \* witness: a claimed entry was unloaded and reloaded
    floorClaim            \* witness: a claim was raised by the retired floor

walVars == <<next, author, inflight, durable, acked, floor>>
leafVars == <<up, cache, rp, mine, snapCov, snapRows>>
epochVars == <<rowEpoch, seen, epoch, claimHead, fence, memFence, retired, maxClaimed>>
handVars == <<hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence>>
viewVars == <<fz, fzOpen, fzClosedAt>>
envVars == <<phase, hint, dupUsed, restarted, recreated>>
wghosts == <<rejected, fRejected, advanced, advancedPastForeign, dupAdvanced,
             trimAfterAdvance, reloaded, floorClaim>>

vars == <<walVars, leafVars, epochVars, handVars, viewVars, envVars, staleAccepted, wghosts>>

TypeOK ==
    /\ phase \in {"pre", "window", "live"}
    /\ hint \in 0..MaxOff
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
    /\ rowEpoch \in Epochs
    /\ seen \in [Acts -> Epochs]
    /\ epoch \in [Acts -> Epochs]
    /\ claimHead \in [Acts -> 0..MaxOff]
    /\ fence \in Epochs
    /\ memFence \in -1..MaxEpoch
    /\ retired \in Epochs
    /\ maxClaimed \in Epochs
    /\ hoOpen \in BOOLEAN
    /\ hoClosedAt \in 0..MaxOff
    /\ windows \in 0..MaxWindows
    /\ fpend \in BOOLEAN
    /\ fkind \in Kinds
    /\ fstamp \in Epochs
    /\ fEpoch \in Epochs
    /\ fFence \in Epochs
    /\ fz \in [Acts -> BOOLEAN]
    /\ fzOpen \in [Acts -> BOOLEAN]
    /\ fzClosedAt \in [Acts -> 0..MaxOff]
    /\ dupUsed \in BOOLEAN
    /\ restarted \in BOOLEAN
    /\ recreated \in BOOLEAN
    /\ staleAccepted \in BOOLEAN
    /\ rejected \in BOOLEAN
    /\ fRejected \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ advancedPastForeign \in BOOLEAN
    /\ dupAdvanced \in BOOLEAN
    /\ trimAfterAdvance \in BOOLEAN
    /\ reloaded \in BOOLEAN
    /\ floorClaim \in BOOLEAN

Init ==
    /\ phase = "pre"
    /\ hint = 0
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ inflight = {}
    /\ durable = {}
    /\ acked = {}
    /\ floor = 0
    /\ up = {}
    /\ cache = [a \in Acts |-> {}]
    /\ rp = [a \in Acts |-> -1]
    /\ mine = [a \in Acts |-> {}]
    /\ snapCov = -1
    /\ snapRows = {}
    /\ rowEpoch = 0
    /\ seen = [a \in Acts |-> 0]
    /\ epoch = [a \in Acts |-> 0]
    /\ claimHead = [a \in Acts |-> 0]
    /\ fence = 0
    /\ memFence = 0
    /\ retired = 0
    /\ maxClaimed = 0
    /\ hoOpen = FALSE
    /\ hoClosedAt = 0
    /\ windows = 0
    /\ fpend = FALSE
    /\ fkind = "point"
    /\ fstamp = 0
    /\ fEpoch = 1
    /\ fFence = 1
    /\ fz = [a \in Acts |-> FALSE]
    /\ fzOpen = [a \in Acts |-> FALSE]
    /\ fzClosedAt = [a \in Acts |-> 0]
    /\ dupUsed = FALSE
    /\ restarted = FALSE
    /\ recreated = FALSE
    /\ staleAccepted = FALSE
    /\ rejected = FALSE
    /\ fRejected = FALSE
    /\ advanced = FALSE
    /\ advancedPastForeign = FALSE
    /\ dupAdvanced = FALSE
    /\ trimAfterAdvance = FALSE
    /\ reloaded = FALSE
    /\ floorClaim = FALSE

(***************************************************************************)
(* PreWrite: before a bulk append begins, an existing leaf takes a write   *)
(* the bulk range will own. Begin: a bulk load or append begins, but only  *)
(* over a range it finds empty (#5610) - captured in the same serial turn  *)
(* as the birth hint, the WAL head now. The leaf it will create is born    *)
(* with its hand-off marker open: the switch is a hand-off.                *)
(***************************************************************************)
PreWrite ==
    /\ phase = "pre"
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = PreAuthor]
    /\ durable' = durable \cup {next}
    /\ acked' = acked \cup {next}
    /\ UNCHANGED <<inflight, floor, leafVars, epochVars, handVars, viewVars, envVars,
                   staleAccepted, wghosts>>

Begin ==
    /\ phase = "pre"
    /\ next = 0
    /\ phase' = "window"
    /\ hint' = next
    /\ hoOpen' = TRUE
    /\ windows' = 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoClosedAt, fpend, fkind, fstamp, fEpoch,
                   fFence, viewVars, dupUsed, restarted, recreated, staleAccepted, wghosts>>

(* Create: the leaf's first activation, born at its birth hint. *)
Create ==
    /\ phase = "window"
    /\ phase' = "live"
    /\ up' = {"a1"}
    /\ rp' = [rp EXCEPT !["a1"] = hint - 1]
    /\ seen' = [seen EXCEPT !["a1"] = rowEpoch]
    /\ UNCHANGED <<hint, walVars, cache, mine, snapCov, snapRows, rowEpoch, epoch, claimHead,
                   fence, memFence, retired, maxClaimed, handVars, viewVars, dupUsed,
                   restarted, recreated, staleAccepted, wghosts>>

(***************************************************************************)
(* Claim(a): the activation persists the epoch it read plus one - raised   *)
(* above the retired floor - to L's row, as a strict conditional write on  *)
(* that read, and claims it at the WAL shard, which raises its entry and   *)
(* returns its allocator head under the gate that assigns offsets. Every   *)
(* other running activation is now superseded. ClaimConflict(a): the       *)
(* conditional write fails because the row's epoch moved, so this          *)
(* activation is stale and stops. (Production compares ETags; a conflict   *)
(* whose re-read finds the epoch unchanged is retried, which is this       *)
(* comparison of epochs.)                                                  *)
(***************************************************************************)
Superseded(x, a) == FrozenView /\ x # a /\ x \in up /\ ~fz[x]

Claim(a) ==
    /\ a \in up
    /\ epoch[a] = 0
    /\ seen[a] = rowEpoch
    /\ LET e == Max2(seen[a], retired) + 1 IN
        /\ e <= MaxEpoch
        /\ rowEpoch' = e
        /\ epoch' = [epoch EXCEPT ![a] = e]
        /\ fence' = Max2(fence, e)
        /\ memFence' = Max2(fence, e)
        /\ claimHead' = [claimHead EXCEPT ![a] = next]
        /\ maxClaimed' = Max2(maxClaimed, e)
        /\ floorClaim' = (floorClaim \/ (Ghosts /\ e > seen[a] + 1))
    /\ fz' = [x \in Acts |-> fz[x] \/ Superseded(x, a)]
    /\ fzOpen' = [x \in Acts |-> IF Superseded(x, a) THEN hoOpen ELSE fzOpen[x]]
    /\ fzClosedAt' = [x \in Acts |-> IF Superseded(x, a) THEN hoClosedAt ELSE fzClosedAt[x]]
    /\ UNCHANGED <<walVars, leafVars, seen, retired, handVars, envVars, staleAccepted,
                   rejected, fRejected, advanced, advancedPastForeign, dupAdvanced,
                   trimAfterAdvance, reloaded>>

ClaimConflict(a) ==
    /\ a \in up
    /\ epoch[a] = 0
    /\ seen[a] # rowEpoch
    /\ up' = up \ {a}
    /\ cache' = [cache EXCEPT ![a] = {}]
    /\ mine' = [mine EXCEPT ![a] = {}]
    /\ UNCHANGED <<walVars, rp, snapCov, snapRows, epochVars, handVars, viewVars, envVars,
                   staleAccepted, wghosts>>

(***************************************************************************)
(* OwnAlloc(a): the WAL shard accepts a claimed activation's append only   *)
(* from an epoch not below its loaded entry, and assigns it an offset.     *)
(* Land(o): the append becomes durable; its author, if still running,      *)
(* applies and acknowledges it. StaleAppend(a): the shard rejects an       *)
(* append below its entry; the write fails and is not acknowledged.        *)
(***************************************************************************)
OwnAlloc(a) ==
    /\ a \in up
    /\ epoch[a] > 0
    /\ memFence > 0
    /\ epoch[a] >= memFence
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed)
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, handVars, viewVars, envVars,
                   wghosts>>

StaleAppend(a) ==
    /\ Ghosts
    /\ ~rejected
    /\ a \in up
    /\ epoch[a] > 0
    /\ memFence > 0
    /\ epoch[a] < memFence
    /\ next < MaxOff
    /\ rejected' = TRUE
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, viewVars, envVars, staleAccepted,
                   fRejected, advanced, advancedPastForeign, dupAdvanced, trimAfterAdvance,
                   reloaded, floorClaim>>

Land(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ IF author[o] \in up
       THEN /\ cache' = [cache EXCEPT ![author[o]] = @ \cup {o}]
            /\ mine' = [mine EXCEPT ![author[o]] = @ \cup {o}]
            /\ acked' = acked \cup {o}
       ELSE UNCHANGED <<cache, mine, acked>>
    /\ UNCHANGED <<next, author, floor, up, rp, snapCov, snapRows, epochVars, handVars,
                   viewVars, envVars, staleAccepted, wghosts>>

(***************************************************************************)
(* The foreign author: open the marker, admit a write under its current    *)
(* epoch, have it allocated (accepted) or fenced out (rejected), hand an   *)
(* entry off to L, and close. Close fences: admission has stopped, and the *)
(* author re-claims its epoch at the shard, which returns its allocator    *)
(* head under the same gate; the marker records that head.                 *)
(***************************************************************************)
ForeignOpen ==
    /\ phase = "live"
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoClosedAt, fpend, fkind, fstamp, fEpoch,
                   fFence, viewVars, envVars, staleAccepted, wghosts>>

ForeignAdmit(k) ==
    /\ hoOpen
    /\ ~fpend
    /\ next < MaxOff
    /\ fpend' = TRUE
    /\ fkind' = k
    /\ fstamp' = fEpoch
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoOpen, hoClosedAt, windows, fEpoch, fFence,
                   viewVars, envVars, staleAccepted, wghosts>>

ForeignAlloc ==
    /\ fpend
    /\ fstamp >= fFence
    /\ next < MaxOff
    /\ fpend' = FALSE
    /\ fkind' = "point"
    /\ fstamp' = 0
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, hoOpen, hoClosedAt, windows,
                   fEpoch, fFence, viewVars, envVars, staleAccepted, wghosts>>

ForeignReject ==
    /\ fpend
    /\ fstamp < fFence
    /\ fpend' = FALSE
    /\ fkind' = "point"
    /\ fstamp' = 0
    /\ fRejected' = (fRejected \/ Ghosts)
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoOpen, hoClosedAt, windows, fEpoch, fFence,
                   viewVars, envVars, staleAccepted, rejected, advanced, advancedPastForeign,
                   dupAdvanced, trimAfterAdvance, reloaded, floorClaim>>

HandOff(o, a) ==
    /\ a \in up
    /\ o \in durable
    /\ author[o] = Foreign
    /\ o \notin acked
    /\ cache' = [cache EXCEPT ![a] = @ \cup {o}]
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<next, author, inflight, durable, floor, up, rp, mine, snapCov, snapRows,
                   epochVars, handVars, viewVars, envVars, staleAccepted, wghosts>>

Close ==
    /\ phase = "live"
    /\ hoOpen
    /\ fEpoch < MaxEpoch
    /\ hoOpen' = FALSE
    /\ fEpoch' = fEpoch + 1
    /\ fFence' = fEpoch + 1
    /\ hoClosedAt' = next
    /\ UNCHANGED <<walVars, leafVars, epochVars, windows, fpend, fkind, fstamp, viewVars,
                   envVars, staleAccepted, wghosts>>

(* Replay(a): read and apply the next durable entry; never a trimmed one. *)
Replay(a) ==
    /\ a \in up
    /\ rp[a] + 1 < MaxOff
    /\ rp[a] + 1 \in durable
    /\ rp[a] + 1 >= floor
    /\ cache' = [cache EXCEPT ![a] = @ \cup {rp[a] + 1}]
    /\ rp' = [rp EXCEPT ![a] = rp[a] + 1]
    /\ UNCHANGED <<walVars, up, mine, snapCov, snapRows, epochVars, handVars, viewVars,
                   envVars, staleAccepted, wghosts>>

(***************************************************************************)
(* Armed(a): the conditions under which a's live advance cannot pass an    *)
(* entry it does not hold. A superseded activation reads the marker state  *)
(* it last saw (FrozenView); the current one reads the row. Any condition  *)
(* missing falls back to Replay. LiveAdvance(a): advance over a's own      *)
(* applied appends without reading. (Claimed is implied: mine[a] is        *)
(* non-empty only after a claim.)                                          *)
(***************************************************************************)
MarkerOpen(a) == IF fz[a] THEN fzOpen[a] ELSE hoOpen
MarkerClosedAt(a) == IF fz[a] THEN fzClosedAt[a] ELSE hoClosedAt

Armed(a) ==
    /\ rp[a] >= claimHead[a] - 1
    /\ ~MarkerOpen(a)
    /\ rp[a] >= MarkerClosedAt(a) - 1
    /\ \A o \in inflight : author[o] # a

LiveAdvance(a) ==
    /\ a \in up
    /\ mine[a] # {}
    /\ Armed(a)
    /\ LET target == Max(mine[a]) IN
        /\ target > rp[a]
        /\ rp' = [rp EXCEPT ![a] = target]
        /\ advancedPastForeign' =
            (advancedPastForeign \/ (Ghosts /\ \E o \in Offs : author[o] = Foreign /\ o < target))
    /\ advanced' = (advanced \/ Ghosts)
    /\ dupAdvanced' = (dupAdvanced \/ (Ghosts /\ a = "a2"))
    /\ UNCHANGED <<walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, viewVars,
                   envVars, staleAccepted, rejected, fRejected, trimAfterAdvance, reloaded,
                   floorClaim>>

(* Capture(a): the shared per-leaf snapshot keeps the highest coverage. *)
Capture(a) ==
    /\ a \in up
    /\ rp[a] > snapCov
    /\ snapCov' = rp[a]
    /\ snapRows' = {o \in cache[a] : o <= rp[a]}
    /\ UNCHANGED <<walVars, up, cache, rp, mine, epochVars, handVars, viewVars, envVars,
                   staleAccepted, wghosts>>

(* GcTrim: trim the next durable entry below the leaf's pin. *)
GcTrim ==
    /\ floor <= snapCov
    /\ floor \in durable
    /\ floor' = floor + 1
    /\ trimAfterAdvance' = (trimAfterAdvance \/ (Ghosts /\ advanced))
    /\ UNCHANGED <<next, author, inflight, durable, acked, leafVars, epochVars, handVars,
                   viewVars, envVars, staleAccepted, rejected, fRejected, advanced,
                   advancedPastForeign, dupAdvanced, reloaded, floorClaim>>

(***************************************************************************)
(* Environment.                                                            *)
(*  - Duplicate: a second activation (or the next one after a stop) reads  *)
(*    the snapshot and the row, and must claim before it appends.          *)
(*  - Restart: the WAL shard restarts (a reactivation, or a move's         *)
(*    cutover) and loses its in-memory entries; Reload: a leaf re-presents *)
(*    its claim and the shard loads L's durable entry.                     *)
(*  - Recreate: L is purged and re-created; its row starts again at epoch  *)
(*    0 and its entry is retired, raising the partition's retired floor.   *)
(*    A running activation of either incarnation is untouched: a duplicate *)
(*    the purge never reached survives.                                    *)
(***************************************************************************)
Duplicate ==
    /\ phase = "live"
    /\ ~dupUsed
    /\ "a2" \notin up
    /\ dupUsed' = TRUE
    /\ up' = up \cup {"a2"}
    /\ cache' = [cache EXCEPT !["a2"] = snapRows]
    /\ rp' = [rp EXCEPT !["a2"] = snapCov]
    /\ seen' = [seen EXCEPT !["a2"] = rowEpoch]
    /\ UNCHANGED <<walVars, mine, snapCov, snapRows, rowEpoch, epoch, claimHead, fence,
                   memFence, retired, maxClaimed, handVars, viewVars, phase, hint,
                   restarted, recreated, staleAccepted, wghosts>>

Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ memFence' = -1
    /\ UNCHANGED <<walVars, leafVars, rowEpoch, seen, epoch, claimHead, fence, retired,
                   maxClaimed, handVars, viewVars, phase, hint, dupUsed, recreated,
                   staleAccepted, wghosts>>

Reload ==
    /\ memFence = -1
    /\ memFence' = fence
    /\ reloaded' = (reloaded \/ (Ghosts /\ fence > 0))
    /\ UNCHANGED <<walVars, leafVars, rowEpoch, seen, epoch, claimHead, fence, retired,
                   maxClaimed, handVars, viewVars, envVars, staleAccepted, rejected,
                   fRejected, advanced, advancedPastForeign, dupAdvanced, trimAfterAdvance,
                   floorClaim>>

Recreate ==
    /\ WithRecreate
    /\ phase = "live"
    /\ ~recreated
    /\ recreated' = TRUE
    /\ rowEpoch' = 0
    /\ retired' = Max2(retired, fence)
    /\ fence' = 0
    /\ memFence' = IF memFence = -1 THEN -1 ELSE 0
    /\ UNCHANGED <<walVars, leafVars, seen, epoch, claimHead, maxClaimed, handVars, viewVars,
                   phase, hint, dupUsed, restarted, staleAccepted, wghosts>>

\* A behaviour may stop anywhere: this is a safety model.
Done == UNCHANGED vars

Next ==
    \/ PreWrite \/ Begin \/ Create
    \/ \E a \in Acts : Claim(a) \/ ClaimConflict(a) \/ OwnAlloc(a) \/ StaleAppend(a)
    \/ \E o \in Offs : Land(o)
    \/ ForeignOpen \/ ForeignAlloc \/ ForeignReject \/ Close
    \/ \E k \in Kinds : ForeignAdmit(k)
    \/ \E o \in Offs, a \in Acts : HandOff(o, a)
    \/ \E a \in Acts : Replay(a) \/ LiveAdvance(a) \/ Capture(a)
    \/ GcTrim
    \/ Duplicate \/ Restart \/ Reload \/ Recreate
    \/ Done

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* No running activation's read position passes a durable entry it does not
\* hold. Unacknowledged entries count: a WAL reader applies them, so an owner
\* that skipped one would diverge from its replicas and views for good.
ReadPositionHonest ==
    \A a \in up : \A o \in durable : o <= rp[a] => o \in cache[a]

\* The shared snapshot never claims coverage of a durable entry it lacks.
SnapshotHonest ==
    \A o \in durable : o <= snapCov => o \in snapRows

\* No trim past an acknowledged append, whoever appended it.
NoTrimPastAcked ==
    \A o \in acked : o < floor => o \in snapRows

\* No append from an epoch below the highest claimed one is ever accepted,
\* across a shard restart too.
StaleEpochNeverAppends == ~staleAccepted

(***************************************************************************)
(* Witnesses: each must be VIOLATED, with Ghosts = TRUE, proving its step  *)
(* is reachable in the instance and the clean run is not vacuous.          *)
(***************************************************************************)
WitnessStaleAppendRejected == ~rejected
WitnessDuplicateAppends == \A o \in Offs : author[o] # "a2"
WitnessForeignAppendInOpenWindow == \A o \in Offs : author[o] # Foreign
WitnessAdmittedForeignWriteFenced == ~fRejected
WitnessLiveAdvance == ~advanced
WitnessLiveAdvanceAfterHandOff == ~advancedPastForeign
WitnessDuplicateLiveAdvance == ~dupAdvanced
WitnessTrimAfterLiveAdvance == ~trimAfterAdvance
WitnessUnloadedEntryReloaded == ~reloaded
WitnessClaimAboveRetiredFloor == ~floorClaim
====
