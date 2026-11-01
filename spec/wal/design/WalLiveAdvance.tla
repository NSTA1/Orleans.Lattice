---- MODULE WalLiveAdvance ----
(***************************************************************************)
(* Design model for the fenced live checkpoint advance (issues #5572,      *)
(* #5593). A draft: it describes production that does not exist yet, so it *)
(* has no refinement note and sits outside spec module discovery; it        *)
(* becomes the companion module spec/wal/WalLiveAdvance once production     *)
(* lands. See LiveAdvanceDesign.md beside it.                              *)
(*                                                                         *)
(* One WAL partition, one owner leaf L, and only entries L owns: entries   *)
(* other leaves own are invisible to L's replay and to this model. Entries  *)
(* are appended by L's activations - a1, created by a bulk load (born); a2, *)
(* a duplicate the grain directory admits beside a running one, which    *)
(* also stands for the next activation after a1 stops - and by a foreign  *)
(* author f standing                                                       *)
(* for every other writer of an entry L owns: a split donor, a fold        *)
(* neighbour, an adaptive split's donor shard, a range delete's author,    *)
(* and the leaf a write reached before a bulk switch. A foreign entry is   *)
(* acknowledged only once it is handed off to L, and the hand-off may      *)
(* never happen (its author crashed).                                      *)
(*                                                                         *)
(* The fences:                                                             *)
(*  - Epoch. Before its first append an activation persists a new epoch    *)
(*    to L's row, conditional on the row it read (an ETag check), and     *)
(*    claims it at the WAL shard, which from then on rejects appends from *)
(*    an older epoch of L and returns its head. An activation whose        *)
(*    persist conflicts is stale and stops.                                *)
(*  - Hand-off. A foreign author opens a durable marker on L before its    *)
(*    first append and closes it, recording the WAL head, only after its  *)
(*    appends have drained. A bulk load opens it at the leaf's birth and   *)
(*    records the head when it began as the birth hint.                    *)
(*  - Armed. An activation advances over its own applied appends without  *)
(*    reading only once it has claimed, replayed to its claim head, no    *)
(*    hand-off is open, and it has replayed to the last close's head.      *)
(*                                                                         *)
(* Safety: no read position passes a durable entry the activation does not *)
(* hold - acknowledged or not, because a WAL reader applies every durable  *)
(* entry - the shared snapshot is honest, the GC never trims an            *)
(* acknowledged entry the snapshot lacks, and no stale epoch ever appends, *)
(* across a shard move too.                                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

MaxOff == 3
Offs == 0..(MaxOff - 1)
Acts == {"a1", "a2"}
Foreign == "f"
NoAuthor == "none"
Authors == Acts \cup {Foreign}
MaxWindows == 2
Epochs == 0..2

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    phase,      \* "pre" before the bulk window opens, "window", then "live"
    hint,       \* the WAL head when the bulk load began: the leaf's birth hint
    next,       \* the next offset the WAL shard allocates
    author,     \* the author each allocated offset was allocated to
    inflight,   \* allocated, not yet durable
    durable,
    acked,
    up,         \* L's running activations
    cache,      \* each activation's projection
    rp,         \* each activation's read position (its checkpoint)
    mine,       \* each activation's own applied appends
    snapCov,    \* the shared snapshot's coverage
    snapRows,   \* the shared snapshot's rows
    floor,      \* the GC trim floor: every offset below it is trimmed
    rowEpoch,   \* the epoch persisted in L's row
    seen,       \* the row epoch each activation read when it activated
    epoch,      \* each activation's claimed epoch; 0 before its claim
    claimHead,  \* the WAL head each activation's claim returned
    shardFence, \* the WAL shard's epoch table entry for L
    maxClaimed, \* ghost: the highest epoch ever claimed
    hoOpen,     \* L's durable hand-off marker is open
    hoClosedAt, \* the WAL head the last close recorded
    fpending,   \* a foreign append has been issued and not yet allocated
    windows,    \* hand-off windows opened so far
    dupUsed,
    moved,      \* the WAL shard has moved
    rejected,   \* ghost: a stale epoch's append was rejected
    staleAccepted, \* ghost: an append from an epoch below maxClaimed landed
    advanced,   \* ghost: some activation advanced live
    advancedPastForeign \* ghost: a live advance passed a foreign entry it had read

vars == <<phase, hint, next, author, inflight, durable, acked, up, cache, rp, mine,
          snapCov, snapRows, floor, rowEpoch, seen, epoch, claimHead, shardFence,
          maxClaimed, hoOpen, hoClosedAt, fpending, windows, dupUsed,
          moved, rejected, staleAccepted, advanced, advancedPastForeign>>

ghosts == <<maxClaimed, rejected, staleAccepted, advanced, advancedPastForeign>>
walVars == <<next, author, inflight, durable, acked, floor>>
leafVars == <<up, cache, rp, mine, snapCov, snapRows>>
epochVars == <<rowEpoch, seen, epoch, claimHead, shardFence>>
handVars == <<hoOpen, hoClosedAt, fpending, windows>>
envVars == <<phase, hint, dupUsed, moved>>

TypeOK ==
    /\ phase \in {"pre", "window", "live"}
    /\ hint \in 0..MaxOff
    /\ next \in 0..MaxOff
    /\ author \in [Offs -> Authors \cup {NoAuthor}]
    /\ inflight \subseteq Offs
    /\ durable \subseteq Offs
    /\ acked \subseteq Offs
    /\ up \subseteq Acts
    /\ cache \in [Acts -> SUBSET Offs]
    /\ rp \in [Acts -> -1..(MaxOff - 1)]
    /\ mine \in [Acts -> SUBSET Offs]
    /\ snapCov \in -1..(MaxOff - 1)
    /\ snapRows \subseteq Offs
    /\ floor \in 0..MaxOff
    /\ rowEpoch \in Epochs
    /\ seen \in [Acts -> Epochs]
    /\ epoch \in [Acts -> Epochs]
    /\ claimHead \in [Acts -> 0..MaxOff]
    /\ shardFence \in Epochs
    /\ maxClaimed \in Epochs
    /\ hoOpen \in BOOLEAN
    /\ hoClosedAt \in 0..MaxOff
    /\ fpending \in BOOLEAN
    /\ windows \in 0..MaxWindows
    /\ dupUsed \in BOOLEAN
    /\ moved \in BOOLEAN
    /\ rejected \in BOOLEAN
    /\ staleAccepted \in BOOLEAN
    /\ advanced \in BOOLEAN
    /\ advancedPastForeign \in BOOLEAN

Init ==
    /\ phase = "pre"
    /\ hint = 0
    /\ next = 0
    /\ author = [o \in Offs |-> NoAuthor]
    /\ inflight = {}
    /\ durable = {}
    /\ acked = {}
    /\ up = {}
    /\ cache = [a \in Acts |-> {}]
    /\ rp = [a \in Acts |-> -1]
    /\ mine = [a \in Acts |-> {}]
    /\ snapCov = -1
    /\ snapRows = {}
    /\ floor = 0
    /\ rowEpoch = 0
    /\ seen = [a \in Acts |-> 0]
    /\ epoch = [a \in Acts |-> 0]
    /\ claimHead = [a \in Acts |-> 0]
    /\ shardFence = 0
    /\ maxClaimed = 0
    /\ hoOpen = FALSE
    /\ hoClosedAt = 0
    /\ fpending = FALSE
    /\ windows = 0
    /\ dupUsed = FALSE
    /\ moved = FALSE
    /\ rejected = FALSE
    /\ staleAccepted = FALSE
    /\ advanced = FALSE
    /\ advancedPastForeign = FALSE

(***************************************************************************)
(* Begin: a bulk load or append begins. The leaf it will create is born    *)
(* with its hand-off marker open (the switch is the hand-off) and the WAL  *)
(* head now as its birth hint. Before it, the range held no entry.         *)
(***************************************************************************)
Begin ==
    /\ phase = "pre"
    /\ phase' = "window"
    /\ hint' = next
    /\ hoOpen' = TRUE
    /\ windows' = 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoClosedAt, fpending,
                   dupUsed, moved, ghosts>>

(* Create: the leaf's first activation, born at its birth hint. *)
Create ==
    /\ phase = "window"
    /\ phase' = "live"
    /\ up' = {"a1"}
    /\ rp' = [rp EXCEPT !["a1"] = hint - 1]
    /\ seen' = [seen EXCEPT !["a1"] = rowEpoch]
    /\ UNCHANGED <<hint, walVars, cache, mine, snapCov, snapRows,
                   rowEpoch, epoch, claimHead, shardFence, handVars,
                   dupUsed, moved, ghosts>>

(***************************************************************************)
(* Claim(a): before its first append the activation persists the next     *)
(* epoch to L's row, conditional on the epoch it read, and claims it at   *)
(* the WAL shard, which records it in its epoch table and returns its head.*)
(* ClaimConflict(a): the conditional persist fails - another activation   *)
(* claimed since this one read the row - so this one is stale and stops.  *)
(***************************************************************************)
Claim(a) ==
    /\ a \in up
    /\ epoch[a] = 0
    /\ seen[a] = rowEpoch
    /\ rowEpoch < 2
    /\ rowEpoch' = rowEpoch + 1
    /\ epoch' = [epoch EXCEPT ![a] = rowEpoch + 1]
    /\ shardFence' = Max2(shardFence, rowEpoch + 1)
    /\ claimHead' = [claimHead EXCEPT ![a] = next]
    /\ maxClaimed' = rowEpoch + 1
    /\ UNCHANGED <<walVars, leafVars, seen, handVars, envVars,
                   rejected, staleAccepted, advanced, advancedPastForeign>>

ClaimConflict(a) ==
    /\ a \in up
    /\ epoch[a] = 0
    /\ seen[a] # rowEpoch
    /\ up' = up \ {a}
    /\ cache' = [cache EXCEPT ![a] = {}]
    /\ mine' = [mine EXCEPT ![a] = {}]
    /\ UNCHANGED <<walVars, rp, snapCov, snapRows, epochVars, handVars, envVars, ghosts>>

(***************************************************************************)
(* OwnAppend(a): a claimed activation appends, applies and acknowledges a *)
(* write; the shard accepts it only from an epoch not below its table's.  *)
(* StaleAppend(a): the shard rejects an append from an older epoch; the    *)
(* write fails and is not acknowledged.                                    *)
(***************************************************************************)
OwnAppend(a) ==
    /\ a \in up
    /\ epoch[a] > 0
    /\ epoch[a] >= shardFence
    /\ next < MaxOff
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = a]
    /\ durable' = durable \cup {next}
    /\ acked' = acked \cup {next}
    /\ cache' = [cache EXCEPT ![a] = @ \cup {next}]
    /\ mine' = [mine EXCEPT ![a] = @ \cup {next}]
    /\ staleAccepted' = (staleAccepted \/ epoch[a] < maxClaimed)
    /\ UNCHANGED <<inflight, floor, up, rp, snapCov, snapRows, epochVars, handVars,
                   envVars, maxClaimed, rejected, advanced, advancedPastForeign>>

StaleAppend(a) ==
    /\ a \in up
    /\ epoch[a] > 0
    /\ epoch[a] < shardFence
    /\ next < MaxOff
    /\ rejected' = TRUE
    /\ UNCHANGED <<walVars, leafVars, epochVars, handVars, envVars,
                   maxClaimed, staleAccepted, advanced, advancedPastForeign>>

(***************************************************************************)
(* The foreign author: open the marker, issue an append, allocate it (in  *)
(* flight), land it, hand it off, and close the marker - only once every  *)
(* issued append has landed - recording the WAL head.                     *)
(***************************************************************************)
ForeignOpen ==
    /\ phase = "live"
    /\ ~hoOpen
    /\ windows < MaxWindows
    /\ hoOpen' = TRUE
    /\ windows' = windows + 1
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoClosedAt, fpending, envVars, ghosts>>

ForeignIssue ==
    /\ hoOpen
    /\ ~fpending
    /\ next < MaxOff
    /\ fpending' = TRUE
    /\ UNCHANGED <<walVars, leafVars, epochVars, hoOpen, hoClosedAt, windows, envVars, ghosts>>

ForeignAlloc ==
    /\ fpending
    /\ next < MaxOff
    /\ fpending' = FALSE
    /\ next' = next + 1
    /\ author' = [author EXCEPT ![next] = Foreign]
    /\ inflight' = inflight \cup {next}
    /\ UNCHANGED <<durable, acked, floor, leafVars, epochVars, hoOpen, hoClosedAt,
                   windows, envVars, ghosts>>

Land(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ UNCHANGED <<next, author, acked, floor, leafVars, epochVars, handVars, envVars, ghosts>>

HandOff(o, a) ==
    /\ a \in up
    /\ o \in durable
    /\ author[o] = Foreign
    /\ o \notin acked
    /\ cache' = [cache EXCEPT ![a] = @ \cup {o}]
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<next, author, inflight, durable, floor, up, rp, mine, snapCov, snapRows,
                   epochVars, handVars, envVars, ghosts>>

Close ==
    /\ phase = "live"
    /\ hoOpen
    /\ ~fpending
    /\ \A o \in inflight : author[o] # Foreign
    /\ hoOpen' = FALSE
    /\ hoClosedAt' = next
    /\ UNCHANGED <<walVars, leafVars, epochVars, fpending, windows, envVars, ghosts>>

(* Replay(a): read and apply the next durable entry; never a trimmed one. *)
Replay(a) ==
    /\ a \in up
    /\ rp[a] + 1 < MaxOff
    /\ rp[a] + 1 \in durable
    /\ rp[a] + 1 >= floor
    /\ cache' = [cache EXCEPT ![a] = @ \cup {rp[a] + 1}]
    /\ rp' = [rp EXCEPT ![a] = rp[a] + 1]
    /\ UNCHANGED <<walVars, up, mine, snapCov, snapRows, epochVars, handVars, envVars, ghosts>>

(***************************************************************************)
(* Armed(a): the conditions under which a's live advance cannot pass an    *)
(* entry it does not hold. LiveAdvance(a): advance over a's own applied    *)
(* appends without reading. Any condition missing falls back to Replay.   *)
(***************************************************************************)
Armed(a) ==
    /\ epoch[a] > 0
    /\ rp[a] >= claimHead[a] - 1
    /\ ~hoOpen
    /\ rp[a] >= hoClosedAt - 1

LiveAdvance(a) ==
    /\ a \in up
    /\ mine[a] # {}
    /\ Armed(a)
    /\ LET target == Max(mine[a]) IN
        /\ target > rp[a]
        /\ rp' = [rp EXCEPT ![a] = target]
        /\ advancedPastForeign' =
            (advancedPastForeign \/ \E o \in Offs : author[o] = Foreign /\ o < target)
    /\ advanced' = TRUE
    /\ UNCHANGED <<walVars, up, cache, mine, snapCov, snapRows, epochVars, handVars, envVars,
                   maxClaimed, rejected, staleAccepted>>

(* Capture(a): the shared per-leaf snapshot keeps the highest coverage. *)
Capture(a) ==
    /\ a \in up
    /\ rp[a] > snapCov
    /\ snapCov' = rp[a]
    /\ snapRows' = {o \in cache[a] : o <= rp[a]}
    /\ UNCHANGED <<walVars, up, cache, rp, mine, epochVars, handVars, envVars, ghosts>>

(* GcTrim: trim the next durable entry below the leaf's pin. *)
GcTrim ==
    /\ floor <= snapCov
    /\ floor \in durable
    /\ floor' = floor + 1
    /\ UNCHANGED <<next, author, inflight, durable, acked, leafVars, epochVars, handVars,
                   envVars, ghosts>>

(***************************************************************************)
(* Environment: a duplicate activation (or the next activation after a    *)
(* stop: both read the row and must claim), and a WAL shard move that     *)
(* carries the epoch table.                                                *)
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
    /\ UNCHANGED <<walVars, mine, snapCov, snapRows, rowEpoch, epoch, claimHead, shardFence,
                   handVars, phase, hint, moved, ghosts>>

ShardMove ==
    /\ ~moved
    /\ moved' = TRUE
    \* The epoch table moves with the shard's state.
    /\ shardFence' = shardFence
    /\ UNCHANGED <<walVars, leafVars, rowEpoch, seen, epoch, claimHead, handVars,
                   phase, hint, dupUsed, ghosts>>

\* A behaviour may stop anywhere: this is a safety model.
Done == UNCHANGED vars

Next ==
    \/ Begin
    \/ Create
    \/ \E a \in Acts : Claim(a) \/ ClaimConflict(a) \/ OwnAppend(a) \/ StaleAppend(a)
    \/ ForeignOpen \/ ForeignIssue \/ ForeignAlloc \/ Close
    \/ \E o \in Offs : Land(o)
    \/ \E o \in Offs, a \in Acts : HandOff(o, a)
    \/ \E a \in Acts : Replay(a) \/ LiveAdvance(a) \/ Capture(a)
    \/ GcTrim
    \/ Duplicate \/ ShardMove
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
\* across a shard move too.
StaleEpochNeverAppends == ~staleAccepted

(***************************************************************************)
(* Witnesses: each must be VIOLATED, proving the step is reachable in this *)
(* instance and the clean run is not vacuous.                              *)
(***************************************************************************)
WitnessStaleAppendRejected == ~rejected
WitnessDuplicateAppends == \A o \in Offs : author[o] # "a2"
WitnessForeignAppendInOpenWindow == \A o \in Offs : author[o] # Foreign
WitnessLiveAdvance == ~advanced
WitnessLiveAdvanceAfterHandOff == ~advancedPastForeign
====
