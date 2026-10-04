--------------------------- MODULE ShardOwnership ---------------------------
(***************************************************************************)
(* Key ownership in Orleans.Lattice across an adaptive shard split, an     *)
(* online reshard, and an online resize T -> R (fence, alias flip, undo,   *)
(* purge), concurrent with stale routing caches, an atomic-write saga      *)
(* bound to one physical copy, and a later plain write. The registry here  *)
(* always reports the saga's decision; ShardOwnershipRetention models the  *)
(* mask, the row's retirement, late forwards and leaf reactivation.        *)
(*                                                                         *)
(* See README.md for the instance and how to run TLC, and Refinement.md    *)
(* for the mapping of every variable, action and property to production.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS T, R, s1, s2, k1, k2

Copies == {T, R}
Shards == {s1, s2}
Keys == {k1, k2}

\* A row holds a version of a write. Rank orders versions by the writes'
\* real-time order (what acknowledgement and reads are judged by); Stamp is the
\* HLC stamp last-writer-wins compares. The base versions are stamped in
\* commit order (Stamp = 10 * Rank); the other three exist so a mutation can
\* express a stamp that disagrees with real time (#4522): the later write
\* stamped below the saga's P on a leaf whose clock never saw P, a saga value
\* installed at a fresh dominating stamp, and a saga bucket re-minted at a
\* copy's own clock.
Absent == 0
InitV == 1
SagaV == 2
LaterV == 3
LaterLowV == 4
SagaUV == 5
FreshV == 6
Vals == {Absent, InitV, SagaV, LaterV, LaterLowV, SagaUV, FreshV}

Rank(v) == CASE v = LaterLowV -> LaterV
             [] v \in {SagaUV, FreshV} -> SagaV
             [] OTHER -> v
Stamp(v) == CASE v = LaterLowV -> 15
              [] v = SagaUV -> 12
              [] v = FreshV -> 40
              [] OTHER -> 10 * v

Max(a, b) == IF a > b THEN a ELSE b
LWW(a, b) == IF Stamp(a) >= Stamp(b) THEN a ELSE b

\* A routing map is abstracted to the shard k2's virtual slot routes to; k1's
\* slot never moves in this instance.
MapOf(m, k) == IF k = k2 THEN m ELSE s1

Pairs == Copies \X Shards

VARIABLES
    alias,      \* registry: the physical copy the logical tree resolves to
    rmap,       \* registry: the logical row's map (where k2 routes)
    published,  \* every (copy, map) pair the registry has published; any stale router may still hold one
    rmapR,      \* R's own registry entry's map, which the resize swap carries onto the logical row
    rmapOld,    \* the logical row's map captured when the resize began, which an undo restores
    row,        \* row[c][s][k]: committed projection value on shard s of copy c
    pend,       \* pend[c][s][k]: the saga's prepared bucket: "none", "old" (stamped before the later write) or "new"
    term,       \* term[c][s]: the leaf activation's memory that the saga's terminal applied here
    mig,        \* mig[c][s][k]: the row was last written by a cross-shard migration (MergeManyAsync, isCrossShardMigration)
    sp,         \* adaptive split phase
    spCopy,     \* the physical copy the split is bound to
    rs,         \* reshard coordinator phase
    rz,         \* resize phase
    rzShards,   \* T's shards the resize shadow-forwards, fences and releases
    fence,      \* T's shards in ShadowForwardPhase.Rejecting
    redir,      \* R's shards armed with a retained redirect by an undo
    refusals,   \* the environment's budget for refusing one alias flip
    sg,         \* saga phase
    bound,      \* the physical copy the saga is bound to
    prepped,    \* keys dispatched under the current binding
    told,       \* shards of the bound copy the terminal broadcast has visited
    dec,        \* the registry's recorded decision for the saga
    vis,        \* ghost: vis[k] is the highest value a fresh reader has been served for k
    wDone,      \* the later plain write of k2 has happened
    ackOn       \* ghost: ackOn[c][k] is the highest value acknowledged that copy c must hold

vars == <<alias, rmap, published, rmapR, rmapOld, row, pend, term, mig, sp, spCopy, rs,
          rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec,
          wDone, ackOn, vis>>

\* Every variable but the ghost vis, which the step relation sets alongside each
\* action (see VisStep). Fairness is stated over these, since an action alone
\* leaves vis' undetermined.
svars == <<alias, rmap, published, rmapR, rmapOld, row, pend, term, mig, sp, spCopy, rs,
           rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec,
           wDone, ackOn>>

-----------------------------------------------------------------------------
(* Derived state                                                           *)

\* Resize phases in which T still forwards every accepted mutation to R: until
\* the undo's last step clears T's shadow-forward state, or the purge.
FwdPhases == {"snap", "copied", "swapped", "retired", "undoing"}

ResizeMirrors(c, s) == c = T /\ s \in rzShards /\ rz \in FwdPhases

SplitWindow == {"shadow", "swept"}
SplitFrozen == {"frozen", "done"}

\* The split source mirrors moved-slot writes to the destination.
SplitMirrors(c, s, k) == c = spCopy /\ s = s1 /\ k = k2 /\ sp \in SplitWindow

\* The split source refuses moved-slot operations from the freeze on.
SplitRejects(c, s, k) == c = spCopy /\ s = s1 /\ k = k2 /\ sp \in SplitFrozen

Fenced(c, s) == c = T /\ s \in fence
Redirected(c) == c = R /\ redir
\* The old copy after the purge. A routed operation there is refused, so the
\* caller refreshes its pair: the purge leaves a tombstone that refuses a router
\* whose logical tree resolves elsewhere (#4503, fixed by #4528).
Gone(c) == c = T /\ rz = "purged"

\* Whether copy c can still become, or still is, the tree's copy.
Live(c) == IF c = T THEN rz # "purged"
                    ELSE rz \in {"snap", "copied", "swapped", "retired", "undoing", "purged"}

\* A routed (logical-alias) operation on key k refused at shard s of copy c.
RoutedRefused(c, s, k) == SplitRejects(c, s, k) \/ Fenced(c, s) \/ Redirected(c) \/ Gone(c)

Loc(p, k) == <<p[1], MapOf(p[2], k)>>
Serves(p, k) == ~RoutedRefused(p[1], MapOf(p[2], k), k)

CurrentPair == <<alias, rmap>>
Owner(k) == <<alias, MapOf(rmap, k)>>

\* What the registry answers for the saga: InFlight before a decision, the
\* recorded outcome after it. This module's registry always reports it.
RegistryView == IF dec = "none" THEN "inflight" ELSE dec

\* The version a saga bucket on copy c carries: marked with the saga's prepare
\* stamp P wherever it lands (intended design, #4522 PR2: the resize mirror
\* carries P to R).
BVal(c) == SagaV

\* Whether shard s of copy c has seen the saga's stamp P: its leaf clock has
\* merged a row stamped at or above P, or one of the saga's buckets.
KnowsP(c, s) == \E k \in Keys : Stamp(row[c][s][k]) >= Stamp(SagaV) \/ pend[c][s][k] # "none"

\* The version of the later write accepted at (c, s): stamped above the saga's
\* prepare when its leaf has seen P, and below it otherwise. Every forward of
\* it carries this stamp (the split's shadow-forward ships the source stamp, and
\* the resize mirror forwards plain writes at T's stamp: intended design).
WVal(c, s) == IF KnowsP(c, s) THEN LaterV ELSE LaterLowV

\* The leaf read gate: a bucket of a committed saga surfaces unless the
\* activation remembers applying its terminal (the orphan guard). A bucket
\* stamped after the later write outranks the row; an older one is LWW-merged.
Surfaced(c, s, k) == pend[c][s][k] # "none" /\ RegistryView = "committed" /\ ~term[c][s]

ValueAt(c, s, k) ==
    IF Surfaced(c, s, k)
    THEN IF pend[c][s][k] = "new" THEN BVal(c) ELSE LWW(row[c][s][k], BVal(c))
    ELSE row[c][s][k]

ReadVia(p, k) == Rank(ValueAt(p[1], MapOf(p[2], k), k))
OwnerValue(k) == Rank(ValueAt(alias, MapOf(rmap, k), k))

\* The map that lays out copy c's shards.
CopyMap(c) == IF c = alias THEN rmap ELSE IF c = R THEN rmapR ELSE rmapOld

\* Every location a mutation accepted at (c, s) for key k lands on.
\* A resize mirror reaches R's shard through that shard's own write path, so a
\* split window open on R forwards it on in turn.
Landing(c, s, k) ==
    LET local == {<<c, s>>} \cup (IF SplitMirrors(c, s, k) THEN {<<c, s2>>} ELSE {})
        mirrored == {<<R, x[2]>> : x \in {y \in local : ResizeMirrors(y[1], y[2])}}
        chained == IF \E y \in mirrored : SplitMirrors(R, y[2], k) THEN {<<R, s2>>} ELSE {}
    IN local \cup mirrored \cup chained

\* Whether every location a mutation at (c, s) for key k would land on takes it.
\* A shadow forward the destination refuses (a moved-away slot, for instance)
\* fails the whole call, which the caller retries (ShardRootGrain.ForwardWithDeadlineAsync
\* rethrows every forward failure).
Landable(c, s, k) == \A x \in Landing(c, s, k) : x = <<c, s>> \/ ~SplitRejects(x[1], x[2], k)

\* The copies a write acknowledged on copy c obliges to hold it: while a
\* resize is in flight, or once it completed and purged T, R must hold
\* everything T acknowledged, because the logical tree acknowledged it.
AckCopies(c) == IF c = T /\ rz \in FwdPhases \cup {"purged"} THEN {T, R} ELSE {c}

\* Whether the bound copy mirrors everything into the copy the tree resolves to.
BoundMirrors == bound = T /\ alias = R /\ ResizeMirrors(T, s1)

\* The copy the terminal broadcast addresses: the bound copy, or once a purge
\* has cleared it, the copy it mirrored into, which the tree resolves to by
\* then (intended design; production fails the broadcast, #4475).
TermCopy == IF Gone(bound) THEN alias ELSE bound

\* The shards of that copy the broadcast must visit: the owners of the batch's
\* keys now, every shard holding one of its buckets, and the split destination
\* once a split of that copy has opened its window. Computed on the copy the
\* terminal goes to, so a split of the resized copy after the purge is
\* followed too.
TermTargets ==
    {MapOf(CopyMap(TermCopy), k) : k \in Keys}
    \cup {s \in Shards : \E k \in Keys : pend[TermCopy][s][k] # "none"}
    \cup (IF spCopy = TermCopy /\ sp # "idle" THEN {s2} ELSE {})

\* The terminal's effect on one key at one shard. An activation that already
\* applied it discards a bucket (DiscardOrphan); otherwise a bucket drains with
\* its own stamp, and the committed-values backstop covers a key the shard owns.
\* An abort terminal discards the bucket and carries no backstop.
TermRow(c, s, k) ==
    IF term[c][s] \/ dec = "aborted" THEN row[c][s][k]
    ELSE IF pend[c][s][k] = "new" THEN BVal(c)
    ELSE IF pend[c][s][k] = "old" THEN LWW(row[c][s][k], BVal(c))
    ELSE IF MapOf(CopyMap(c), k) = s THEN LWW(row[c][s][k], SagaV)
    ELSE row[c][s][k]

-----------------------------------------------------------------------------
Init ==
    /\ alias = T
    /\ rmap = s1
    /\ published = {<<T, s1>>}
    /\ rmapR = s1
    /\ rmapOld = s1
    /\ row = [c \in Copies |-> [s \in Shards |-> [k \in Keys |->
                IF c = T /\ s = s1 THEN InitV ELSE Absent]]]
    /\ pend = [c \in Copies |-> [s \in Shards |-> [k \in Keys |-> "none"]]]
    /\ term = [c \in Copies |-> [s \in Shards |-> FALSE]]
    /\ mig = [c \in Copies |-> [s \in Shards |-> [k \in Keys |-> FALSE]]]
    /\ sp = "idle"
    /\ spCopy = T
    /\ rs = "idle"
    /\ rz = "idle"
    /\ rzShards = {}
    /\ fence = {}
    /\ redir = FALSE
    /\ refusals = 1
    /\ sg = "idle"
    /\ bound = T
    /\ prepped = {}
    /\ told = {}
    /\ dec = "none"
    /\ wDone = FALSE
    /\ ackOn = [c \in Copies |-> [k \in Keys |-> IF c = T THEN InitV ELSE Absent]]
    /\ vis = [k \in Keys |-> InitV]

-----------------------------------------------------------------------------
(* Adaptive split of k2's slot from s1 to s2 on the copy the tree resolves  *)
(* to (TreeShardSplitGrain). A reshard's split relies on the reshard's own *)
(* interlock with resize. An adaptive split refuses a resize until the old *)
(* copy is purged or the resize undone, and a resize refuses a split in   *)
(* flight (#4452). The old copy mirrors index-for-index into the resized  *)
(* copy and a saga may stay bound to it until the purge, so a split that  *)
(* changed the resized copy's layout meanwhile would strand the bound      *)
(* saga's buckets (mutation NoKeyLostSplitInSoftDeleteWindow).            *)

SplitBegin ==
    /\ sp = "idle"
    /\ rs = "migrating" \/ rz \in {"idle", "purged", "undone"}
    /\ rmap = s1
    /\ ~Fenced(alias, s1)
    /\ sp' = "shadow"
    /\ spCopy' = alias
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* Retroactive sweep of prepares that predate the window: a bucket whose saga
\* the registry reports decided is resolved at the destination (with the
\* committed-values backstop for a commit) instead of being replayed; one the
\* registry reports InFlight is replayed. ShardOwnershipRetention covers the
\* registry declining to answer (#4473).
SplitSweep ==
    /\ sp = "shadow"
    /\ alias = spCopy
    /\ LET c == spCopy
           b == pend[c][s1][k2]
           view == RegistryView
           dests == {<<c, s2>>} \cup (IF ResizeMirrors(c, s2) THEN {<<R, s2>>} ELSE {})
       IN IF b = "none"
          THEN UNCHANGED <<row, pend, term, mig>>
          ELSE IF view \in {"committed", "aborted"}
               THEN /\ row' = IF view = "committed"
                              THEN [row EXCEPT ![c][s2][k2] = LWW(@, SagaV)]
                              ELSE row
                    /\ mig' = IF view = "committed" /\ LWW(row[c][s2][k2], SagaV) # row[c][s2][k2]
                              THEN [mig EXCEPT ![c][s2][k2] = FALSE]
                              ELSE mig
                    /\ term' = [term EXCEPT ![c][s2] = TRUE]
                    /\ UNCHANGED pend
               ELSE /\ pend' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                                   IF <<x, s>> \in dests /\ k = k2 THEN b ELSE pend[x][s][k]]]]
                    /\ UNCHANGED <<row, term, mig>>
    /\ sp' = "swept"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* Mark the source's leaves moved away and enter Reject: the source refuses k2.
SplitFreeze ==
    /\ sp = "swept"
    /\ alias = spCopy
    /\ ~Fenced(spCopy, s1)
    /\ sp' = "frozen"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* Final authoritative drain, then the fenced ReassignSlotsAsync: the map moves
\* k2 to s2 only while the tree still resolves to the bound copy
\* (ShardMapCommitFence).
SplitCommit ==
    /\ sp = "frozen"
    /\ LET c == spCopy
           v == LWW(row[c][s2][k2], row[c][s1][k2])
           into(x, s, k) == k = k2 /\ s = s2 /\ (x = c \/ (x = R /\ ResizeMirrors(c, s2)))
       IN /\ row' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                        IF into(x, s, k) THEN LWW(row[x][s][k], v) ELSE row[x][s][k]]]]
          /\ mig' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                        IF into(x, s, k) /\ x = c /\ LWW(row[x][s][k], v) # row[x][s][k]
                        THEN TRUE ELSE mig[x][s][k]]]]
    /\ alias = spCopy
    /\ rmap' = s2
    /\ published' = published \cup {<<alias, s2>>}
    /\ sp' = "done"
    /\ UNCHANGED <<alias, rmapR, rmapOld, pend, term, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

-----------------------------------------------------------------------------
(* Online reshard (TreeReshardGrain): drives splits until the map names the *)
(* target shard count. Interlocked with resize in both directions.          *)

ReshardStart ==
    /\ rs = "idle"
    /\ sp = "idle"
    /\ rz \in {"idle", "purged", "undone"}
    /\ rs' = "migrating"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

ReshardFinish ==
    /\ rs = "migrating"
    /\ rmap = s2
    /\ rs' = "complete"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

-----------------------------------------------------------------------------
(* Online resize T -> R (TreeResizeGrain + TreeSnapshotGrain).              *)

ResizeBegin ==
    /\ rz = "idle"
    /\ alias = T
    /\ rs # "migrating"
    /\ sp \in {"idle", "done"}
    /\ rz' = "snap"
    /\ rzShards' = {s1, rmap}
    /\ rmapR' = rmap
    /\ rmapOld' = rmap
    /\ ackOn' = [ackOn EXCEPT ![R] = ackOn[T]]
    /\ UNCHANGED <<alias, rmap, published, row, pend, term, sp, spCopy, rs, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, mig>>

\* The online snapshot copies T's committed entries index-for-index, keeping an
\* entry only on the shard the copy's map routes it to. The intended design also
\* carries T's prepared buckets; production copies committed entries only.
SnapCopy ==
    /\ rz = "snap"
    /\ row' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                 IF x = R /\ s \in rzShards /\ MapOf(rmapR, k) = s
                 THEN LWW(row[R][s][k], row[T][s][k]) ELSE row[x][s][k]]]]
    /\ pend' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                 IF x = R /\ s \in rzShards /\ pend[R][s][k] = "none"
                 THEN pend[T][s][k] ELSE pend[x][s][k]]]]
    /\ rz' = "copied"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, term, mig, sp, spCopy, rs, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* EnterRejectingAsync on one of T's shards, before the alias flip (#4362).
ResizeFence(s) ==
    /\ rz = "copied"
    /\ s \in rzShards \ fence
    /\ fence' = fence \cup {s}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* The single-write SwapAliasAsync (#4357), only once every shard is fenced.
ResizeFlip ==
    /\ rz = "copied"
    /\ fence = rzShards
    /\ alias' = R
    /\ rmap' = rmapR
    /\ published' = published \cup {<<R, rmapR>>}
    /\ rz' = "swapped"
    /\ UNCHANGED <<rmapR, rmapOld, row, pend, term, sp, spCopy, rs, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn, mig>>

\* Environment: the ownership guard refuses the flip, and
\* LiftFenceUnlessSwappedAsync lifts the fence because the alias did not move.
ResizeFlipRefused ==
    /\ rz = "copied"
    /\ fence # {}
    /\ refusals > 0
    /\ alias = T
    /\ fence' = {}
    /\ refusals' = refusals - 1
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, redir, sg, bound, prepped, told, dec, wDone, ackOn>>

\* RejectOldShardsAsync then CleanupOldTreeAsync: T is soft-deleted and stays
\* fenced; a saga bound to it is still admitted.
ResizeRetire ==
    /\ rz = "swapped"
    /\ fence' = rzShards
    /\ rz' = "retired"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rzShards, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* The purge after SoftDeleteDuration clears T's shard rows. A router may still
\* hold a pair naming T: nothing bounds a routing activation's lifetime below
\* SoftDeleteDuration, so the pairs stay published (#4503).
ResizePurge ==
    /\ rz = "retired"
    /\ rz' = "purged"
    /\ row' = [row EXCEPT ![T] = [s \in Shards |-> [k \in Keys |-> Absent]]]
    /\ pend' = [pend EXCEPT ![T] = [s \in Shards |-> [k \in Keys |-> "none"]]]
    /\ term' = [term EXCEPT ![T] = [s \in Shards |-> FALSE]]
    /\ mig' = [mig EXCEPT ![T] = [s \in Shards |-> [k \in Keys |-> FALSE]]]
    /\ fence' = {}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, sp, spCopy, rs, rzShards, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* Undo before the flip: abort the snapshot, clear T's forwarding, discard R.
UndoBeforeFlip ==
    /\ rz \in {"snap", "copied"}
    /\ fence' = {}
    /\ rz' = "undone"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rzShards, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* Undo after the flip, first step: ArmRedirectsAsync arms the resized copy's
\* shards to redirect routers that still address it (#4357). The undo arms R
\* and swaps before it lifts T's fence, so no instant has both copies serving
\* (#4453).
UndoArm ==
    /\ rz \in {"swapped", "retired"}
    /\ alias = R
    /\ redir' = TRUE
    /\ rz' = "undoing"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rzShards, fence, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

\* One registry write moves the alias and the old map back (#4357).
UndoSwap ==
    /\ rz = "undoing"
    /\ alias = R
    /\ alias' = T
    /\ rmap' = rmapOld
    /\ published' = published \cup {<<T, rmapOld>>}
    /\ UNCHANGED <<rmapR, rmapOld, row, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn, mig>>

\* Recover T and ClearShadowForwardAsync on its shards: the fence lifts and T
\* serves again under the alias that now names it.
UndoClear ==
    /\ rz = "undoing"
    /\ alias = T
    /\ fence' = {}
    /\ rz' = "undone"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rzShards, redir, refusals, sg, bound, prepped, told, dec, wDone, ackOn>>

-----------------------------------------------------------------------------
(* The atomic-write saga (AtomicWriteGrain), writing SagaV to k1 and k2.    *)

SagaStart ==
    /\ sg = "idle"
    /\ sg' = "exec"
    /\ bound' = alias
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, prepped, told, dec, wDone, ackOn>>

\* One key's prepared write, dispatched through a routing activation holding
\* pair p. The routing tier re-reads the registry when its cached copy is not
\* the bound one and refuses when the tree really moved (#4358), but dispatches
\* to the bound copy while it mirrors into the resolved one; the bound copy's
\* shards admit the bound batch through a resize fence (#4369).
SagaPrepare(k, p) ==
    /\ sg = "exec"
    /\ k \notin prepped
    /\ LET c == IF p[1] = bound \/ alias = bound \/ BoundMirrors THEN bound ELSE alias
           m == IF p[1] = bound THEN p[2] ELSE CopyMap(bound)
           s == MapOf(m, k)
       IN /\ c = bound
          /\ ~SplitRejects(c, s, k)
          /\ ~(Fenced(c, s) /\ c # bound)
          /\ ~Redirected(c)
          /\ ~Gone(c)
          /\ Landable(c, s, k)
          /\ pend' = [x \in Copies |-> [y \in Shards |-> [j \in Keys |->
                         IF j = k /\ <<x, y>> \in Landing(c, s, k)
                         THEN IF wDone THEN "new" ELSE "old"
                         ELSE pend[x][y][j]]]]
    /\ prepped' = prepped \cup {k}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, told, dec, wDone, ackOn>>

\* TryRebindToResolvedCopyAsync: the routing tier refused part of the batch
\* because the tree moved off the bound copy; re-bind and re-dispatch it all.
\* The intended design stays bound while the bound copy mirrors into the copy
\* the tree resolves to, as the pre-decision check does (#4369); production
\* re-binds unconditionally (see Refinement.md).
SagaRebindOnRefusal ==
    /\ sg = "exec"
    /\ ~BoundMirrors
    /\ prepped # Keys
    /\ alias # bound
    /\ bound' = alias
    /\ prepped' = {}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, told, dec, wDone, ackOn>>

\* RebindAcrossAliasSwapAsync, when the bound copy does not mirror into the
\* copy the tree resolves to now: re-bind and re-run the execute phase.
SagaRebindBeforeDecision ==
    /\ sg = "exec"
    /\ prepped = Keys
    /\ alias # bound
    /\ ~BoundMirrors
    /\ bound' = alias
    /\ prepped' = {}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, told, dec, wDone, ackOn>>

\* Record the commit decision: the tree still resolves to the bound copy, or
\* the bound copy mirrors into the one it resolves to (#4369).
SagaDecide ==
    /\ sg = "exec"
    /\ prepped = Keys
    /\ alias = bound \/ BoundMirrors
    /\ dec' = "committed"
    /\ sg' = "decided"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, bound, prepped, told, wDone, ackOn>>

\* One shard of the terminal broadcast to the bound copy, mirrored to R when
\* that shard forwards. A direct terminal passes a resize fence (#4369).
\* The intended design modelled here delivers a terminal a purged old copy
\* refuses to the copy it mirrored into, at the same shard (the split interlock
\* keeps the layouts equal), where production fails the broadcast (#4475); and
\* it counts one a resized copy an undo discarded refuses as delivered, since
\* that copy's batch is discarded with it, where production re-sends it to the
\* old copy (#4474).
SagaTerminal(s) ==
    /\ sg = "decided"
    /\ s \in TermTargets \ told
    /\ told' = told \cup {s}
    /\ LET target == TermCopy
           hit == IF bound = R /\ ~Live(R) THEN {}
                  ELSE {<<target, s>>} \cup (IF ResizeMirrors(target, s) THEN {<<R, s>>} ELSE {})
       IN /\ row' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF <<x, y>> \in hit THEN TermRow(x, y, k) ELSE row[x][y][k]]]]
          /\ pend' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF <<x, y>> \in hit THEN "none" ELSE pend[x][y][k]]]]
          /\ term' = [x \in Copies |-> [y \in Shards |->
                        IF <<x, y>> \in hit THEN TRUE ELSE term[x][y]]]
          /\ mig' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF <<x, y>> \in hit /\ TermRow(x, y, k) # row[x][y][k] THEN FALSE ELSE mig[x][y][k]]]]
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, dec, wDone, ackOn>>

\* The broadcast has visited every target: the saga completes, and a committed
\* saga's caller is acknowledged.
SagaComplete ==
    /\ sg = "decided"
    /\ TermTargets \subseteq told
    /\ sg' = "done"
    /\ ackOn' = [c \in Copies |-> [k \in Keys |->
                    IF dec = "committed" /\ c \in AckCopies(bound) THEN Max(ackOn[c][k], SagaV) ELSE ackOn[c][k]]]
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, bound, prepped, told, dec, wDone>>

-----------------------------------------------------------------------------
(* Environment                                                              *)

\* A later plain write of k2, through a routing activation holding pair p.
LaterWrite(p) ==
    /\ ~wDone
    /\ dec # "none"
    /\ LET c == p[1]
           s == MapOf(p[2], k2)
       IN /\ ~RoutedRefused(c, s, k2)
          /\ Landable(c, s, k2)
          /\ row' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF k = k2 /\ <<x, y>> \in Landing(c, s, k2)
                        THEN LWW(row[x][y][k], WVal(c, s)) ELSE row[x][y][k]]]]
          /\ mig' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF k = k2 /\ <<x, y>> \in Landing(c, s, k2) /\ LWW(row[x][y][k], WVal(c, s)) # row[x][y][k]
                        THEN y = s2 /\ SplitMirrors(x, s1, k2) /\ <<x, y>> # <<c, s>>
                        ELSE mig[x][y][k]]]]
          /\ ackOn' = [x \in Copies |-> [k \in Keys |->
                          IF k = k2 /\ x \in AckCopies(c) THEN Max(ackOn[x][k], LaterV) ELSE ackOn[x][k]]]
    /\ wDone' = TRUE
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, sg, bound, prepped, told, dec>>

\* The saga records an abort: a prepare failed past its retries, or the caller
\* went away. Any point of the execute phase may end this way, so the action is
\* unguarded beyond that; its terminal broadcast is the compensation.
SagaAbort ==
    /\ sg = "exec"
    /\ dec' = "aborted"
    /\ sg' = "decided"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, mig, pend, term, sp, spCopy, rs, rz, rzShards, fence, redir, refusals, bound, prepped, told, wDone, ackOn>>

\* Nothing is in flight: every operation that started has finished. An operation
\* that never started is not owed, so a state with no enabled step is a deadlock
\* only when something is stuck part way.
Quiescent ==
    /\ sg \in {"idle", "done"}
    /\ sp \in {"idle", "done"}
    /\ rz \in {"idle", "purged", "undone"}
    /\ rs # "migrating"

Stutter ==
    /\ Quiescent
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ SplitBegin
    \/ SplitSweep
    \/ SplitFreeze
    \/ SplitCommit
    \/ ReshardStart
    \/ ReshardFinish
    \/ ResizeBegin
    \/ SnapCopy
    \/ \E s \in Shards : ResizeFence(s)
    \/ ResizeFlip
    \/ ResizeFlipRefused
    \/ ResizeRetire
    \/ ResizePurge
    \/ UndoBeforeFlip
    \/ UndoArm
    \/ UndoSwap
    \/ UndoClear
    \/ SagaStart
    \/ \E k \in Keys, p \in published : SagaPrepare(k, p)
    \/ SagaRebindOnRefusal
    \/ SagaRebindBeforeDecision
    \/ SagaDecide
    \/ SagaAbort
    \/ \E s \in Shards : SagaTerminal(s)
    \/ SagaComplete
    \/ \E p \in published : LaterWrite(p)
    \/ Stutter

\* The ghost vis tracks, per key, the highest value a fresh reader has been
\* served. The undo's swap back to T resets it, because an undo discards the
\* resized copy's writes by contract.
VisStep ==
    vis' = [k \in Keys |->
              IF alias = R /\ alias' = T THEN OwnerValue(k)'
              ELSE Max(vis[k], OwnerValue(k)')]

\* Every step a running coordinator takes on its own is weakly fair. Starting
\* an operation, an undo, a refused flip, a stale router's call and an abort
\* are not.

Spec ==
    /\ Init /\ [][Next /\ VisStep]_vars
    /\ WF_svars(SplitSweep) /\ WF_svars(SplitFreeze) /\ WF_svars(SplitCommit)
    /\ WF_svars(rs = "migrating" /\ SplitBegin) /\ WF_svars(ReshardFinish)
    /\ WF_svars(SnapCopy) /\ WF_svars(\E s \in Shards : ResizeFence(s)) /\ WF_svars(ResizeFlip)
    /\ WF_svars(ResizeRetire) /\ WF_svars(ResizePurge) /\ WF_svars(UndoSwap) /\ WF_svars(UndoClear)
    /\ WF_svars(\E k \in Keys : SagaPrepare(k, CurrentPair))
    /\ WF_svars(SagaRebindOnRefusal) /\ WF_svars(SagaRebindBeforeDecision) /\ WF_svars(SagaDecide)
    /\ WF_svars(\E s \in Shards : SagaTerminal(s)) /\ WF_svars(SagaComplete)

-----------------------------------------------------------------------------
(* Properties                                                               *)

TypeOK ==
    /\ alias \in Copies
    /\ rmap \in Shards
    /\ published \subseteq Pairs
    /\ rmapR \in Shards
    /\ rmapOld \in Shards
    /\ row \in [Copies -> [Shards -> [Keys -> Vals]]]
    /\ pend \in [Copies -> [Shards -> [Keys -> {"none", "old", "new"}]]]
    /\ term \in [Copies -> [Shards -> BOOLEAN]]
    /\ mig \in [Copies -> [Shards -> [Keys -> BOOLEAN]]]
    /\ sp \in {"idle", "shadow", "swept", "frozen", "done"}
    /\ spCopy \in Copies
    /\ rs \in {"idle", "migrating", "complete"}
    /\ rz \in {"idle", "snap", "copied", "swapped", "retired", "purged", "undoing", "undone"}
    /\ rzShards \subseteq Shards
    /\ fence \subseteq Shards
    /\ redir \in BOOLEAN
    /\ refusals \in 0..1
    /\ sg \in {"idle", "exec", "decided", "done"}
    /\ bound \in Copies
    /\ prepped \subseteq Keys
    /\ told \subseteq Shards
    /\ dec \in {"none", "committed", "aborted"}
    /\ wDone \in BOOLEAN
    /\ ackOn \in [Copies -> [Keys -> Vals]]
    /\ vis \in [Keys -> Vals]

\* Every routing pair any router may hold, fresh or stale, either is refused
\* for a key or reaches that key's one authoritative location.
UniqueOwner ==
    \A p \in published, k \in Keys : Serves(p, k) => Loc(p, k) = Owner(k)

\* The owner's location holds every value acknowledged to a writer of the key.
NoKeyLost ==
    \A k \in Keys : OwnerValue(k) >= ackOn[alias][k]

\* No read served through any routing pair returns a value older than one
\* already acknowledged.
NoResurrection ==
    \A p \in published, k \in Keys :
        Serves(p, k) => ReadVia(p, k) >= ackOn[alias][k]

\* Once committed, the saga holds prepared buckets only on the copy it is bound
\* to, or on the copy that copy mirrors into. An aborted saga's stray bucket never
\* surfaces (the gate falls through on Aborted), so it is a stranded prepare
\* rather than an ownership hazard; ShardOwnershipRetention's NoStrandedBucket
\* covers stranding.
SagaBatchOnOneCopy ==
    dec = "committed" =>
        \A c \in Copies, s \in Shards, k \in Keys :
            (pend[c][s][k] # "none" /\ Live(c)) => (c = bound \/ (bound = T /\ c = R))

\* Until the later write lands, a fresh reader sees the saga's batch on both keys
\* or on neither.
AtomicOnOwner ==
    ~wDone => ((OwnerValue(k1) >= SagaV) = (OwnerValue(k2) >= SagaV))

\* The value a fresh reader gets for a key never moves backwards: it is never
\* below the highest value already served, except across the undo's swap back
\* to T, which discards the resized copy's writes by contract. Stated over
\* history (the ghost vis), as ShardOwnershipRetention must state it.
OwnerMonotonic ==
    \A k \in Keys : OwnerValue(k) >= vis[k]

SplitCompletes == sp \in {"shadow", "swept", "frozen"} ~> sp = "done"
ReshardCompletes == rs = "migrating" ~> rs = "complete"
ResizeCompletes == rz \in {"snap", "copied", "swapped", "retired", "undoing"} ~> rz \in {"purged", "undone"}
SagaCompletes == sg \in {"exec", "decided"} ~> sg = "done"
RoutingConverges == <>[](\A k \in Keys : Serves(CurrentPair, k))

=============================================================================
