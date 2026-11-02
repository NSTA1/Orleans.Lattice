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
(*    grain directory admits beside a running one, which also stands for  *)
(*    the next activation after a1 stops.                                  *)
(*  - A foreign author f, standing for every other writer of an entry L    *)
(*    owns: a split donor, a reclaim neighbour, an adaptive split's or a   *)
(*    shard fold's donor shard, a range delete's shard roots, and the leaf *)
(*    a write reached before a bulk switch. f's writes come in two kinds,  *)
(*    point and batch. A foreign entry is acknowledged only once it is     *)
(*    handed off to L, and the hand-off may never happen.                  *)
(*  - A pre-existing writer p, whose rows the range already held when a    *)
(*    bulk append began (#5610).                                           *)
(*  - An unstamped author o: a 10.2.0 silo, or any silo while the fence    *)
(*    gate is closed (WithMixedVersion).                                   *)
(*                                                                         *)
(* The fences:                                                             *)
(*  - Epoch. Once the fence gate is open, an activation claims an epoch in *)
(*    three steps: it persists the epoch it read plus one (raised above    *)
(*    the retired floor) to its own epoch row, as a strict conditional     *)
(*    write; it max-merges that epoch into the WAL shard's durable entry,  *)
(*    refused at or below the entry; and it raises the in-memory entry and *)
(*    reads the allocator head under the shard's lock. The shard rejects a *)
(*    stamped append below the loaded entry. Retiring an entry raises the  *)
(*    retired floor. A restart loses the in-memory entry, which reloads    *)
(*    from the durable one.                                                *)
(*  - Hand-off. A foreign author opens a durable marker on L before its    *)
(*    first admission of a write L will own, stamps each write with the    *)
(*    epoch (or routing generation) it was admitted under, and closes by   *)
(*    fencing: it re-claims its epoch at the shard, so a write admitted     *)
(*    earlier is either allocated below the head the close records, or     *)
(*    rejected. The routing-generation entry is durable. A hand-off begun  *)
(*    before the gate opened (WithLegacyHandOff) has no marker; when it    *)
(*    resumes it opens one before its next admission, and L's marker state *)
(*    is not initialised until no such hand-off is in flight.              *)
(*  - Unstamped. Unstamped appends are always admitted, never rejected;    *)
(*    the shard reports the highest one to every stamped append, and an    *)
(*    activation's own unstamped append (WithGateFlip) reports itself.     *)
(*  - Armed. An activation advances over its own applied appends without   *)
(*    reading only once it has claimed, its marker state is initialised,   *)
(*    it has replayed to its claim head, it sees no open marker, it has    *)
(*    replayed to the last close's head and past every reported unstamped  *)
(*    append, and every append it issued has been applied.                 *)
(*                                                                         *)
(* Safety: no read position passes a durable entry the activation does not *)
(* hold - acknowledged or not, because a WAL reader applies every durable  *)
(* entry - the shared snapshot is honest, the GC never trims an            *)
(* acknowledged entry the snapshot lacks, no stale epoch ever appends,     *)
(* across a shard restart too, no unstamped append is ever rejected, and   *)
(* the owner resolves a key a bulk append contests to the value a WAL      *)
(* reader resolves (last-writer-wins, the point write wins).               *)
(*                                                                         *)
(* Switches keep each check small. Instance: WithBatch (the batch write    *)
(* kind) and MaxWindows (hand-off windows). Environment: WithDuplicate,    *)
(* WithRecreate (purge and re-creation of L), WithRestart (a WAL shard     *)
(* restart: a reactivation, or a move's cutover), FrozenView (a superseded *)
(* activation keeps the marker state it last saw), WithMixedVersion (the   *)
(* unstamped author), WithGateFlip (own unstamped appends),               *)
(* WithLegacyHandOff (a hand-off in flight across the gate opening), and  *)
(* WithTreeWalk (another shard's walk of a tree-level range delete).      *)
(* Ghosts enables the witness ghosts.                                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS WithBatch, MaxWindows, WithDuplicate, WithRecreate, WithRestart, FrozenView,
          WithMixedVersion, WithGateFlip, WithLegacyHandOff, WithTreeWalk, Ghosts

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
Foreign == "f"
PreAuthor == "p"
OldAuthor == "o"
TreeWalk == "g"
NoAuthor == "none"
Authors == Acts \cup {Foreign, PreAuthor, OldAuthor, TreeWalk}
Kinds == IF WithBatch THEN {"point", "batch"} ELSE {"point"}
MaxEpoch == 3
Epochs == 0..MaxEpoch

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    gst,        \* another shard's range-delete walk: "idle", "pend" (admitted), "repend"
                \* (re-stamped after a rejection), "rejected", "done"
    closing,    \* a close is between its re-claim and its marker close; always FALSE
                \* here, because the close marks the intent closed atomically with it
    gstamp,     \* the tree range-delete generation it was admitted under
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
    rowEpoch,   \* the epoch in L's epoch row
    seen,       \* the epoch row each activation last read
    epoch,      \* the epoch each activation persisted; 0 before its claim begins
    cstage,     \* each activation's claim: 0 none, 1 row persisted, 2 entry written, 3 raised
    claimHead,  \* the allocator head each activation's claim returned
    fence,      \* the WAL shard's durable entry for L; 0 when absent
    memFence,   \* the shard's in-memory entry for L; -1 when not loaded
    retired,    \* the partition's retired-epoch floor
    maxClaimed, \* the highest epoch ever raised for L
    hoOpen,     \* L's durable hand-off marker is open
    hoClosedAt, \* the allocator head the last close recorded
    windows,    \* hand-off windows opened so far
    fpend,      \* a foreign write is admitted and not yet allocated or rejected
    fkind,      \* its kind
    fstamp,     \* the epoch it was admitted under
    fEpoch,     \* the foreign author's current epoch (or routing generation)
    fFence,     \* the WAL shard's durable entry for the foreign author
    gate,       \* the cluster's fence gate is open
    hfly,       \* a hand-off begun before the gate opened, with no marker, is in flight
    resumed,    \* that hand-off has resumed under the gate
    mInit,      \* L's marker state is initialised
    legacyUsed, \* that hand-off has made its unstamped append
    fz,         \* each activation has been superseded and froze its marker view
    fzOpen,     \* the marker state it froze
    fzClosedAt, \* the close head it froze
    lastU,      \* the highest offset the shard assigned to an unstamped append
    seenU,      \* the highest unstamped offset reported to each activation
    oldUsed,    \* the unstamped (old-version) author has appended
    dupUsed,
    restarted,
    recreated,
    staleAccepted,        \* an append from an epoch below maxClaimed was allocated
    oldRejected,          \* an unstamped append was rejected
    rejected,             \* witness: a stale epoch's append was rejected
    fRejected,            \* witness: an admitted foreign write was fenced out
    advanced,             \* witness: some activation advanced live
    advancedPastForeign,  \* witness: a live advance passed a foreign entry
    dupAdvanced,          \* witness: the duplicate advanced live
    trimAfterAdvance,     \* witness: the GC trimmed after a live advance
    reloaded,             \* witness: a claimed entry was unloaded and reloaded
    floorClaim,           \* witness: a claim was raised by the retired floor
    claimRefused,         \* witness: a claim was refused at the shard
    selfReported          \* witness: an own unstamped append reported itself

walkVars == <<gst, gstamp, closing>>
walVars == <<next, author, inflight, durable, acked, floor>>
leafVars == <<up, cache, rp, mine, snapCov, snapRows>>
epochVars == <<rowEpoch, seen, epoch, cstage, claimHead, fence, memFence, retired, maxClaimed>>
handVars == <<hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch, fFence>>
gateVars == <<gate, hfly, resumed, mInit, legacyUsed>>
viewVars == <<fz, fzOpen, fzClosedAt>>
oldVars == <<lastU, seenU, oldUsed>>
envVars == <<phase, hint, dupUsed, restarted, recreated>>
safeVars == <<staleAccepted, oldRejected>>
wghosts == <<rejected, fRejected, advanced, advancedPastForeign, dupAdvanced, trimAfterAdvance,
             reloaded, floorClaim, claimRefused, selfReported>>

vars == <<walkVars, walVars, leafVars, epochVars, handVars, gateVars, viewVars, oldVars, envVars,
          safeVars, wghosts>>

TypeOK ==
    /\ gst \in {"idle", "pend", "repend", "rejected", "done"}
    /\ closing \in BOOLEAN
    /\ gstamp \in Epochs
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
    /\ cstage \in [Acts -> 0..3]
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
    /\ gate \in BOOLEAN
    /\ hfly \in BOOLEAN
    /\ resumed \in BOOLEAN
    /\ mInit \in BOOLEAN
    /\ legacyUsed \in BOOLEAN
    /\ fz \in [Acts -> BOOLEAN]
    /\ fzOpen \in [Acts -> BOOLEAN]
    /\ fzClosedAt \in [Acts -> 0..MaxOff]
    /\ lastU \in -1..(MaxOff - 1)
    /\ seenU \in [Acts -> -1..(MaxOff - 1)]
    /\ oldUsed \in BOOLEAN
    /\ dupUsed \in BOOLEAN
    /\ restarted \in BOOLEAN
    /\ recreated \in BOOLEAN
    /\ staleAccepted \in BOOLEAN
    /\ oldRejected \in BOOLEAN
    /\ rejected \in BOOLEAN
    /\ fRejected \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ advancedPastForeign \in BOOLEAN
    /\ dupAdvanced \in BOOLEAN
    /\ trimAfterAdvance \in BOOLEAN
    /\ reloaded \in BOOLEAN
    /\ floorClaim \in BOOLEAN
    /\ claimRefused \in BOOLEAN
    /\ selfReported \in BOOLEAN

Init ==
    /\ gst = "idle"
    /\ gstamp = 0
    /\ closing = FALSE
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
    /\ cstage = [a \in Acts |-> 0]
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
    /\ gate = ~WithLegacyHandOff
    /\ hfly = WithLegacyHandOff
    /\ resumed = FALSE
    /\ mInit = ~WithLegacyHandOff
    /\ legacyUsed = FALSE
    /\ fz = [a \in Acts |-> FALSE]
    /\ fzOpen = [a \in Acts |-> FALSE]
    /\ fzClosedAt = [a \in Acts |-> 0]
    /\ lastU = -1
    /\ seenU = [a \in Acts |-> -1]
    /\ oldUsed = FALSE
    /\ dupUsed = FALSE
    /\ restarted = FALSE
    /\ recreated = FALSE
    /\ staleAccepted = FALSE
    /\ oldRejected = FALSE
    /\ rejected = FALSE
    /\ fRejected = FALSE
    /\ advanced = FALSE
    /\ advancedPastForeign = FALSE
    /\ dupAdvanced = FALSE
    /\ trimAfterAdvance = FALSE
    /\ reloaded = FALSE
    /\ floorClaim = FALSE
    /\ claimRefused = FALSE
    /\ selfReported = FALSE

(***************************************************************************)
(* PreWrite: before a bulk append begins, an existing leaf takes a write   *)
(* the bulk range will own. Begin: a bulk load or append begins; the birth *)
(* hint is the WAL head now, captured in the same serial turn that finds   *)
(* the rows the range already holds (#5610). The leaf it will create is    *)
(* born holding those rows, relocated as a split relocates, and with its   *)
(* hand-off marker open: the switch is a hand-off.                         *)
(***************************************************************************)
PreWrite ==
    /\ phase = "pre"
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = PreAuthor]
    /\ durable' = durable \cup {next}
    /\ acked' = acked \cup {next}
    /\ UNCHANGED <<walkVars, inflight, floor, leafVars, epochVars, handVars, gateVars, viewVars, oldVars,
                   envVars, safeVars, wghosts>>

Begin ==
    /\ phase = "pre"
    /\ phase' = "window"
    /\ hint' = next
    /\ hoOpen' = TRUE
    /\ windows' = 1
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, hoClosedAt, fpend, fkind, fstamp, fEpoch,
                   fFence, gateVars, viewVars, oldVars, dupUsed, restarted, recreated, safeVars,
                   wghosts>>

(* Create: the leaf's first activation, born at its birth hint, holding the *)
(* rows relocated into it.                                                 *)
Create ==
    /\ phase = "window"
    /\ phase' = "live"
    /\ up' = {"a1"}
    /\ cache' = [cache EXCEPT !["a1"] = {o \in Offs : author[o] = PreAuthor}]
    /\ rp' = [rp EXCEPT !["a1"] = hint - 1]
    /\ seen' = [seen EXCEPT !["a1"] = rowEpoch]
    /\ UNCHANGED <<walkVars, hint, walVars, mine, snapCov, snapRows, rowEpoch, epoch, cstage, claimHead,
                   fence, memFence, retired, maxClaimed, handVars, gateVars, viewVars, oldVars,
                   dupUsed, restarted, recreated, safeVars, wghosts>>

StopAct(a) ==
    /\ up' = up \ {a}
    /\ cache' = [cache EXCEPT ![a] = {}]
    /\ mine' = [mine EXCEPT ![a] = {}]

(***************************************************************************)
(* The claim, three steps, only while the fence gate is open.              *)
(*  ClaimRow(a): persist the epoch read plus one, raised above the retired *)
(*    floor, to L's epoch row as a strict conditional write. Only claims   *)
(*    write that row, so ClaimConflict(a) - the row moved since a read it  *)
(*    - means another activation claimed: a is stale and stops. Every      *)
(*    other running activation is now superseded (FrozenView).             *)
(*  ClaimWrite(a): max-merge the epoch into the shard's durable entry,     *)
(*    refused at or below the higher of the loaded and durable entries     *)
(*    and the retired floor.                                               *)
(*  ClaimRaise(a): under the lock, raise the in-memory entry and read the  *)
(*    allocator head; refused if another claim raised the entry above, or  *)
(*    if a retirement since the write raised the floor to the epoch.       *)
(*  ClaimRefuse(a): a refused claim stops its activation.                  *)
(***************************************************************************)
Superseded(x, a) == FrozenView /\ x # a /\ x \in up /\ ~fz[x]

ClaimRow(a) ==
    /\ a \in up
    /\ gate
    /\ cstage[a] = 0
    /\ seen[a] = rowEpoch
    /\ LET e == Max2(seen[a], retired) + 1 IN
        /\ e <= MaxEpoch
        /\ rowEpoch' = e
        /\ epoch' = [epoch EXCEPT ![a] = e]
        /\ floorClaim' = (floorClaim \/ (Ghosts /\ e > seen[a] + 1))
    /\ cstage' = [cstage EXCEPT ![a] = 1]
    /\ fz' = [x \in Acts |-> fz[x] \/ Superseded(x, a)]
    /\ fzOpen' = [x \in Acts |-> IF Superseded(x, a) THEN hoOpen ELSE fzOpen[x]]
    /\ fzClosedAt' = [x \in Acts |-> IF Superseded(x, a) THEN hoClosedAt ELSE fzClosedAt[x]]
    /\ UNCHANGED <<walkVars, walVars, leafVars, seen, claimHead, fence, memFence, retired, maxClaimed,
                   handVars, gateVars, oldVars, envVars, safeVars, rejected, fRejected, advanced,
                   advancedPastForeign, dupAdvanced, trimAfterAdvance, reloaded, claimRefused,
                   selfReported>>

ClaimConflict(a) ==
    /\ a \in up
    /\ cstage[a] = 0
    /\ seen[a] # rowEpoch
    /\ StopAct(a)
    /\ UNCHANGED <<walkVars, walVars, rp, snapCov, snapRows, epochVars, handVars, gateVars, viewVars,
                   oldVars, envVars, safeVars, wghosts>>

ClaimWrite(a) ==
    /\ a \in up
    /\ cstage[a] = 1
    /\ epoch[a] > Max2(Max2(fence, memFence), retired)
    /\ fence' = Max2(fence, epoch[a])
    /\ cstage' = [cstage EXCEPT ![a] = 2]
    /\ UNCHANGED <<walkVars, walVars, leafVars, rowEpoch, seen, epoch, claimHead, memFence, retired,
                   maxClaimed, handVars, gateVars, viewVars, oldVars, envVars, safeVars, wghosts>>

ClaimRaise(a) ==
    /\ a \in up
    /\ cstage[a] = 2
    /\ epoch[a] >= Max2(fence, memFence)
    /\ epoch[a] > retired
    /\ memFence' = epoch[a]
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ maxClaimed' = Max2(maxClaimed, epoch[a])
    /\ cstage' = [cstage EXCEPT ![a] = 3]
    /\ UNCHANGED <<walkVars, walVars, leafVars, rowEpoch, seen, epoch, fence, retired, handVars, gateVars,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

ClaimRefuse(a) ==
    /\ a \in up
    /\ \/ cstage[a] = 1 /\ epoch[a] <= Max2(Max2(fence, memFence), retired)
       \/ cstage[a] = 2 /\ (epoch[a] < Max2(fence, memFence) \/ epoch[a] <= retired)
    /\ StopAct(a)
    /\ cstage' = [cstage EXCEPT ![a] = 0]
    /\ claimRefused' = (claimRefused \/ Ghosts)
    /\ UNCHANGED <<walkVars, walVars, rp, snapCov, snapRows, rowEpoch, seen, epoch, claimHead, fence,
                   memFence, retired, maxClaimed, handVars, gateVars, viewVars, oldVars, envVars,
                   safeVars, rejected, fRejected, advanced, advancedPastForeign, dupAdvanced,
                   trimAfterAdvance, reloaded, floorClaim, selfReported>>

(***************************************************************************)
(* OwnAlloc(a): the shard accepts a claimed activation's stamped append    *)
(* only from an epoch not below its loaded entry, assigns it an offset,    *)
(* and reports the highest unstamped offset. StaleAppend(a): the shard     *)
(* rejects a stamped append below its entry. OwnAllocUnstamped(a): an     *)
(* activation whose view of the gate is closed appends unstamped; the      *)
(* shard admits it and the activation counts it as reported. Land(o): the  *)
(* append becomes durable; its author, if still running, applies and      *)
(* acknowledges it.                                                        *)
(***************************************************************************)
OwnAlloc(a) ==
    /\ a \in up
    /\ cstage[a] = 3
    /\ memFence > 0
    /\ epoch[a] >= memFence
    /\ epoch[a] > retired
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed \/ epoch[a] <= retired)
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, lastU)]
    /\ UNCHANGED <<walkVars, lastU, oldUsed, durable, acked, floor, leafVars, epochVars, handVars, gateVars,
                   viewVars, envVars, oldRejected, wghosts>>

StaleAppend(a) ==
    /\ Ghosts
    /\ ~rejected
    /\ a \in up
    /\ cstage[a] = 3
    /\ memFence > 0
    /\ (epoch[a] < memFence \/ epoch[a] <= retired)
    /\ next < MaxOff
    /\ rejected' = TRUE
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, handVars, gateVars, viewVars, oldVars, envVars,
                   safeVars, fRejected, advanced, advancedPastForeign, dupAdvanced,
                   trimAfterAdvance, reloaded, floorClaim, claimRefused, selfReported>>

OwnAllocUnstamped(a) ==
    /\ WithGateFlip
    /\ a \in up
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ inflight' = inflight \cup {next}
    /\ lastU' = next
    /\ seenU' = [seenU EXCEPT ![a] = Max2(@, next)]
    /\ selfReported' = (selfReported \/ Ghosts)
    /\ UNCHANGED <<walkVars, oldUsed, durable, acked, floor, leafVars, epochVars, handVars, gateVars,
                   viewVars, envVars, safeVars, rejected, fRejected, advanced,
                   advancedPastForeign, dupAdvanced, trimAfterAdvance, reloaded, floorClaim,
                   claimRefused>>

Land(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ IF author[o] \in up
       THEN /\ cache' = [cache EXCEPT ![author[o]] = @ \cup {o}]
            /\ mine' = [mine EXCEPT ![author[o]] = @ \cup {o}]
            /\ acked' = acked \cup {o}
       ELSE /\ acked' = IF author[o] = OldAuthor THEN acked \cup {o} ELSE acked
            /\ UNCHANGED <<walkVars, cache, mine>>
    /\ UNCHANGED <<walkVars, next, author, floor, up, rp, snapCov, snapRows, epochVars, handVars, gateVars,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

(***************************************************************************)
(* The foreign author: open the marker, admit a write under its current    *)
(* epoch, have it allocated (accepted) or fenced out (rejected), hand an   *)
(* entry off to L, and close. Close fences: admission has stopped, and the *)
(* author re-claims its epoch at the shard, which returns its allocator    *)
(* head under the same lock; the marker records that head.                 *)
(***************************************************************************)
ForeignOpen ==
    /\ phase = "live"
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, hoClosedAt, fpend, fkind, fstamp, fEpoch,
                   fFence, gateVars, viewVars, oldVars, envVars, safeVars, wghosts>>

ForeignAdmit(k) ==
    /\ hoOpen \/ (hfly /\ resumed)
    /\ ~closing
    /\ ~fpend
    /\ next < MaxOff
    /\ fpend' = TRUE
    /\ fkind' = k
    /\ fstamp' = fEpoch
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, hoOpen, hoClosedAt, windows, fEpoch, fFence,
                   gateVars, viewVars, oldVars, envVars, safeVars, wghosts>>

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
    /\ UNCHANGED <<walkVars, durable, acked, floor, leafVars, epochVars, hoOpen, hoClosedAt, windows,
                   fEpoch, fFence, gateVars, viewVars, oldVars, envVars, safeVars, wghosts>>

ForeignReject ==
    /\ fpend
    /\ fstamp < fFence
    /\ fpend' = FALSE
    /\ fkind' = "point"
    /\ fstamp' = 0
    /\ fRejected' = (fRejected \/ Ghosts)
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, hoOpen, hoClosedAt, windows, fEpoch, fFence,
                   gateVars, viewVars, oldVars, envVars, safeVars, rejected, advanced,
                   advancedPastForeign, dupAdvanced, trimAfterAdvance, reloaded, floorClaim,
                   claimRefused, selfReported>>

(***************************************************************************)
(* Another shard's walk of a tree-level range delete (WithTreeWalk). Its   *)
(* record is replayed by L because range-delete records are not key-       *)
(* scoped. It is admitted only under an open intent, which opened L's      *)
(* marker, and is stamped with the per-tree range-delete generation that   *)
(* f's epoch stands for: one re-claim of that generation at the partition  *)
(* (Close) fences every shard's walk at once. A walk with no open intent   *)
(* (a replicated or shadow-forwarded range delete, or a fan-out spanning   *)
(* the gate opening) is appended unstamped: OldAlloc covers it.            *)
(***************************************************************************)
GAdmit ==
    /\ WithTreeWalk
    /\ phase = "live"
    /\ hoOpen
    /\ ~closing
    /\ gst = "idle"
    /\ gst' = "pend"
    /\ gstamp' = fEpoch
    /\ UNCHANGED <<closing, walVars, leafVars, epochVars, handVars, gateVars, viewVars, oldVars,
                   envVars, safeVars, wghosts>>

GAlloc ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp >= fFence
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = TreeWalk]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<gstamp, closing, durable, acked, floor, leafVars, epochVars, handVars, gateVars,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

GReject ==
    /\ gst \in {"pend", "repend"}
    /\ gstamp < fFence
    /\ gst' = "rejected"
    /\ UNCHANGED <<gstamp, closing, walVars, leafVars, epochVars, handVars, gateVars, viewVars,
                   oldVars, envVars, safeVars, wghosts>>

(* The seam retry of a rejected walk asks the fence grain whether an intent *)
(* is open. The fence grain answers from one state, in turns that do not   *)
(* interleave with a close, and marks the intent closed before the close's *)
(* first re-claim, so the answer is "open" only while no close is under    *)
(* way. GRestamp: open, so the walk re-stamps with the current generation  *)
(* (another delete's, since its own intent closed when the generation it   *)
(* carried was re-claimed); the check reads the marker, over-approximating *)
(* "its own intent". GUnstamped: not open, so the walk appends unstamped,  *)
(* admitted and reported.                                                  *)
GRestamp ==
    /\ gst = "rejected"
    /\ hoOpen
    /\ ~closing
    /\ gst' = "repend"
    /\ gstamp' = fEpoch
    /\ UNCHANGED <<closing, walVars, leafVars, epochVars, handVars, gateVars, viewVars, oldVars,
                   envVars, safeVars, wghosts>>

GUnstamped ==
    /\ gst = "rejected"
    /\ ~(hoOpen /\ ~closing)
    /\ next < MaxOff
    /\ gst' = "done"
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = TreeWalk]
    /\ inflight' = inflight \cup {next}
    /\ lastU' = next
    /\ UNCHANGED <<gstamp, closing, durable, acked, floor, leafVars, epochVars, handVars, gateVars,
                   viewVars, seenU, oldUsed, envVars, safeVars, wghosts>>

HandOff(o, a) ==    /\ a \in up
    /\ o \in durable
    /\ author[o] = Foreign
    /\ o \notin acked
    /\ cache' = [cache EXCEPT ![a] = @ \cup {o}]
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<walkVars, next, author, inflight, durable, floor, up, rp, mine, snapCov, snapRows,
                   epochVars, handVars, gateVars, viewVars, oldVars, envVars, safeVars, wghosts>>

Close ==
    /\ phase = "live"
    /\ hoOpen
    /\ fEpoch < MaxEpoch
    /\ hoOpen' = FALSE
    /\ fEpoch' = fEpoch + 1
    /\ fFence' = fEpoch + 1
    /\ hoClosedAt' = next
    /\ hfly' = IF resumed THEN FALSE ELSE hfly
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, windows, fpend, fkind, fstamp, gate, resumed,
                   mInit, legacyUsed, viewVars, oldVars, envVars, safeVars, wghosts>>

(***************************************************************************)
(* The rollout (WithLegacyHandOff). A hand-off begun before the gate       *)
(* opened is in flight with no marker. LegacyWrite: before it resumes, it  *)
(* appends unstamped (admitted and reported). OpenGate: the last old silo  *)
(* has left. Resume: the hand-off resumes under the gate and opens its     *)
(* marker before its next admission, after which it admits stamped writes  *)
(* and closes by fence. LegacyComplete: a resumed hand-off that never      *)
(* opened a marker completes without one (reachable only if Resume skips   *)
(* the marker). MarkInit: with no such hand-off in flight, L's marker      *)
(* state is initialised; an absent marker is never read as closed.         *)
(***************************************************************************)
LegacyWrite ==
    /\ hfly
    /\ phase = "live"
    /\ ~resumed
    /\ ~legacyUsed
    /\ next < MaxOff
    /\ legacyUsed' = TRUE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ inflight' = inflight \cup {next}
    /\ lastU' = next
    /\ UNCHANGED <<walkVars, durable, acked, floor, leafVars, epochVars, handVars, gate, hfly, resumed,
                   mInit, viewVars, seenU, oldUsed, envVars, safeVars, wghosts>>

OpenGate ==
    /\ ~gate
    /\ gate' = TRUE
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, handVars, hfly, resumed, mInit, legacyUsed,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

Resume ==
    /\ hfly
    /\ gate
    /\ ~resumed
    /\ phase = "live"
    /\ resumed' = TRUE
    /\ hoOpen' = TRUE
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, hoClosedAt, windows, fpend, fkind, fstamp,
                   fEpoch, fFence, gate, hfly, mInit, legacyUsed, viewVars, oldVars, envVars,
                   safeVars, wghosts>>

LegacyComplete ==
    /\ hfly
    /\ resumed
    /\ ~hoOpen
    /\ ~fpend
    /\ hfly' = FALSE
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, handVars, gate, resumed, mInit, legacyUsed,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

MarkInit ==
    /\ ~hfly
    /\ ~mInit
    /\ mInit' = TRUE
    /\ UNCHANGED <<walkVars, walVars, leafVars, epochVars, handVars, gate, hfly, resumed, legacyUsed,
                   viewVars, oldVars, envVars, safeVars, wghosts>>

(* Replay(a): read and apply the next durable entry; never a trimmed one. *)
Replay(a) ==
    /\ a \in up
    /\ rp[a] + 1 < MaxOff
    /\ rp[a] + 1 \in durable
    /\ rp[a] + 1 >= floor
    /\ cache' = [cache EXCEPT ![a] = @ \cup {rp[a] + 1}]
    /\ rp' = [rp EXCEPT ![a] = rp[a] + 1]
    /\ UNCHANGED <<walkVars, walVars, up, mine, snapCov, snapRows, epochVars, handVars, gateVars, viewVars,
                   oldVars, envVars, safeVars, wghosts>>

(***************************************************************************)
(* Armed(a): the conditions under which a's live advance cannot pass an    *)
(* entry it does not hold. A superseded activation reads the marker state  *)
(* it last saw (FrozenView); the current one reads the marker fresh. Any   *)
(* condition missing falls back to Replay. LiveAdvance(a): advance over    *)
(* a's own applied appends without reading.                                *)
(***************************************************************************)
MarkerOpen(a) == IF fz[a] THEN fzOpen[a] ELSE hoOpen
MarkerClosedAt(a) == IF fz[a] THEN fzClosedAt[a] ELSE hoClosedAt

Armed(a) ==
    /\ cstage[a] = 3
    /\ mInit
    /\ rp[a] >= claimHead[a] - 1
    /\ ~MarkerOpen(a)
    /\ rp[a] >= MarkerClosedAt(a) - 1
    /\ rp[a] >= seenU[a]
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
    /\ UNCHANGED <<walkVars, walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, gateVars,
                   viewVars, oldVars, envVars, safeVars, rejected, fRejected, trimAfterAdvance,
                   reloaded, floorClaim, claimRefused, selfReported>>

(* Capture(a): the shared per-leaf snapshot keeps the highest coverage. *)
Capture(a) ==
    /\ a \in up
    /\ rp[a] > snapCov
    /\ snapCov' = rp[a]
    /\ snapRows' = {o \in cache[a] : o <= rp[a]}
    /\ UNCHANGED <<walkVars, walVars, up, cache, rp, mine, epochVars, handVars, gateVars, viewVars, oldVars,
                   envVars, safeVars, wghosts>>

(* GcTrim: trim the next durable entry below the leaf's pin. *)
GcTrim ==
    /\ floor <= snapCov
    /\ floor \in durable
    /\ floor' = floor + 1
    /\ trimAfterAdvance' = (trimAfterAdvance \/ (Ghosts /\ advanced))
    /\ UNCHANGED <<walkVars, next, author, inflight, durable, acked, leafVars, epochVars, handVars, gateVars,
                   viewVars, oldVars, envVars, safeVars, rejected, fRejected, advanced,
                   advancedPastForeign, dupAdvanced, reloaded, floorClaim, claimRefused,
                   selfReported>>

(***************************************************************************)
(* Environment.                                                            *)
(*  - Duplicate: a second activation (or the next one after a stop) reads  *)
(*    the snapshot and the epoch row, and must claim before it appends.    *)
(*  - Restart: the WAL shard restarts (a reactivation, or a move's         *)
(*    cutover). It loses its in-memory entry for L and its unstamped       *)
(*    watermark, which restarts at the allocator head minus one. The       *)
(*    foreign author's routing-generation entry is durable and survives.   *)
(*    Reload: a leaf re-presents its claim and the entry loads.            *)
(*  - Recreate: L is purged and re-created; its epoch row starts again at  *)
(*    0 and its entry is retired, raising the retired floor. A running     *)
(*    activation of either incarnation is untouched.                       *)
(*  - OldAlloc: the unstamped author appends; the shard admits it unless   *)
(*    UnstampedAdmitted says otherwise, and records it as the highest      *)
(*    unstamped offset. The writing silo acknowledges its own write.       *)
(***************************************************************************)
Duplicate ==
    /\ WithDuplicate
    /\ phase = "live"
    /\ ~dupUsed
    /\ "a2" \notin up
    /\ dupUsed' = TRUE
    /\ up' = up \cup {"a2"}
    /\ cache' = [cache EXCEPT !["a2"] = snapRows]
    /\ rp' = [rp EXCEPT !["a2"] = snapCov]
    /\ seen' = [seen EXCEPT !["a2"] = rowEpoch]
    /\ UNCHANGED <<walkVars, walVars, mine, snapCov, snapRows, rowEpoch, epoch, cstage, claimHead, fence,
                   memFence, retired, maxClaimed, handVars, gateVars, viewVars, oldVars, phase,
                   hint, restarted, recreated, safeVars, wghosts>>

Restart ==
    /\ WithRestart
    /\ ~restarted
    /\ restarted' = TRUE
    /\ memFence' = -1
    /\ lastU' = next - 1
    /\ UNCHANGED fFence
    /\ UNCHANGED <<walkVars, walVars, leafVars, rowEpoch, seen, epoch, cstage, claimHead, fence, retired,
                   maxClaimed, hoOpen, hoClosedAt, windows, fpend, fkind, fstamp, fEpoch,
                   gateVars, viewVars, seenU, oldUsed, phase, hint, dupUsed, recreated, safeVars,
                   wghosts>>

Reload ==
    /\ memFence = -1
    /\ memFence' = fence
    /\ reloaded' = (reloaded \/ (Ghosts /\ fence > 0))
    /\ UNCHANGED <<walkVars, walVars, leafVars, rowEpoch, seen, epoch, cstage, claimHead, fence, retired,
                   maxClaimed, handVars, gateVars, viewVars, oldVars, envVars, safeVars, rejected,
                   fRejected, advanced, advancedPastForeign, dupAdvanced, trimAfterAdvance,
                   floorClaim, claimRefused, selfReported>>

Recreate ==
    /\ WithRecreate
    /\ phase = "live"
    /\ ~recreated
    /\ recreated' = TRUE
    /\ rowEpoch' = 0
    /\ retired' = Max2(retired, fence)
    /\ fence' = 0
    /\ memFence' = IF memFence = -1 THEN -1 ELSE 0
    /\ UNCHANGED <<walkVars, walVars, leafVars, seen, epoch, cstage, claimHead, maxClaimed, handVars,
                   gateVars, viewVars, oldVars, phase, hint, dupUsed, restarted, safeVars, wghosts>>

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
            /\ UNCHANGED <<walkVars, next, author, inflight, lastU>>
    /\ UNCHANGED <<walkVars, seenU, durable, acked, floor, leafVars, epochVars, handVars, gateVars, viewVars,
                   envVars, staleAccepted, wghosts>>

\* A behaviour may stop anywhere: this is a safety model.
Done == UNCHANGED vars

Next ==
    \/ PreWrite \/ Begin \/ Create
    \/ \E a \in Acts : ClaimRow(a) \/ ClaimConflict(a) \/ ClaimWrite(a) \/ ClaimRaise(a)
                       \/ ClaimRefuse(a)
    \/ \E a \in Acts : OwnAlloc(a) \/ StaleAppend(a) \/ OwnAllocUnstamped(a)
    \/ \E o \in Offs : Land(o)
    \/ ForeignOpen \/ ForeignAlloc \/ ForeignReject \/ Close
    \/ GAdmit \/ GAlloc \/ GReject \/ GRestamp \/ GUnstamped
    \/ \E k \in Kinds : ForeignAdmit(k)
    \/ \E o \in Offs, a \in Acts : HandOff(o, a)
    \/ LegacyWrite \/ OpenGate \/ Resume \/ LegacyComplete \/ MarkInit
    \/ \E a \in Acts : Replay(a) \/ LiveAdvance(a) \/ Capture(a)
    \/ GcTrim
    \/ Duplicate \/ Restart \/ Reload \/ Recreate \/ OldAlloc
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

\* No append from an epoch below the highest raised one is ever accepted,
\* across a shard restart too.
StaleEpochNeverAppends == ~staleAccepted

\* An unstamped append - a 10.2.0 silo's, or any while the gate is closed - is
\* never rejected.
OldNeverRejected == ~oldRejected

\* The contested key (#5610). Every entry L owns writes one key K, the
\* contention worst case. Point and foreign entries carry real versions, which
\* the model orders by offset; the bulk chunk's entry for K carries a version
\* ticked from HybridLogicalClock.Zero, below every real one, and every
\* activation of L and every WAL reader holds it. Each side resolves K
\* last-writer-wins: the owner over the entries it holds up to its read
\* position, a replica or view over the durable entries up to the same
\* position. -1 is the bulk value.
BulkRank == -1
LwwRank(S) == IF S = {} THEN BulkRank ELSE Max(S)
OwnerRank(a) == LwwRank({o \in cache[a] : o <= rp[a]})
ReaderRank(a) == LwwRank({o \in durable : o <= rp[a]})

\* Origin and replica agree on the contested key: the owner resolves K to the
\* value a WAL reader resolves at the same position. A bulk append over a key
\* the range already holds therefore returns the point write's value on the
\* origin, as on every replica and view.
ContestedKeyAgrees == \A a \in up : OwnerRank(a) = ReaderRank(a)
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
WitnessClaimRefused == ~claimRefused
WitnessUnstampedAppendReported == \A a \in Acts : seenU[a] = -1
WitnessOwnUnstampedReportsItself == ~selfReported
\* The owner resolves the contested key to a point write made before the
\* switch, over the bulk value it was born with.
WitnessContestedKeyResolvedToPoint ==
    ~(\E a \in up : OwnerRank(a) # BulkRank /\ author[OwnerRank(a)] \in {PreAuthor, Foreign})
\* Another shard's walk is fenced out at L's close (WithTreeWalk).
WitnessTreeWalkFenced == ~(gst = "pend" /\ gstamp < fFence)
\* Another shard's walk is allocated under L's open marker (WithTreeWalk).
WitnessTreeWalkAllocated == \A o \in Offs : author[o] # TreeWalk
\* A rejected walk re-stamps under another open intent's generation (WithTreeWalk).
WitnessTreeWalkRestamped == gst # "repend"
\* A walk whose intent closed is appended unstamped and reported (WithTreeWalk).
WitnessTreeWalkUnstamped == \A o \in Offs : ~(author[o] = TreeWalk /\ lastU = o)
WitnessLegacyHandOffResumed == ~(resumed /\ advanced)
====
