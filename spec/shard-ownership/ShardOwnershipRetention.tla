------------------------ MODULE ShardOwnershipRetention ------------------------
(***************************************************************************)
(* What the transaction registry's retention does to a saga bound across  *)
(* an adaptive shard split and an online resize T -> R with its undo: the  *)
(* registry declining to report the decision (Indeterminate), the row's    *)
(* retirement, a delayed shadow-forwarded prepare, and a leaf reactivation *)
(* that loses per-activation memory and shadow markers.                    *)
(*                                                                         *)
(* The companion of ShardOwnership, which owns routing, the reshard, the   *)
(* refused flip, the undo before a flip and the saga's re-binds; this      *)
(* module keeps only the ownership machinery its concerns act through.    *)
(* See README.md for the decomposition and RefinementRetention.md for the  *)
(* mapping of every variable, action and property to production.          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS T, R, s1, s2, k1, k2

Copies == {T, R}
Shards == {s1, s2}
Keys == {k1, k2}

\* Row values are write stamps in commit order, so LWW is Max.
Absent == 0
InitV == 1
SagaV == 2
LaterV == 3
Vals == {Absent, InitV, SagaV, LaterV}

Max(a, b) == IF a > b THEN a ELSE b

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
    sp,         \* adaptive split phase
    spCopy,     \* the physical copy the split is bound to
    rz,         \* resize phase
    rzShards,   \* T's shards the resize shadow-forwards, fences and releases
    fence,      \* T's shards in ShadowForwardPhase.Rejecting
    redir,      \* R's shards armed with a retained redirect by an undo
    sg,         \* saga phase
    bound,      \* the physical copy the saga is bound to
    prepped,    \* keys dispatched under the current binding
    told,       \* shards of the bound copy the terminal broadcast has visited
    dec,        \* the registry's recorded decision for the saga
    masked,     \* the registry declines to report the saga's row (TxStatus.Indeterminate)
    forgotten,  \* the saga's row has left the registry (it reads InFlight)
    vis,        \* ghost: vis[k] is the highest value a fresh reader has been served for k
    late,       \* a delayed shadow-forwarded prepare of k2 en route to the split destination
    wDone,      \* the later plain write of k2 has happened
    reacted,    \* the reactivation budget has been spent
    ackOn       \* ghost: ackOn[c][k] is the highest value acknowledged that copy c must hold

vars == <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy,
          rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late,
          wDone, reacted, ackOn, vis>>

\* Every variable but the ghost vis, which the step relation sets alongside each
\* action (see VisStep). Fairness is stated over these, since an action alone
\* leaves vis' undetermined.
svars == <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy,
           rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late,
           wDone, reacted, ackOn>>

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

\* What the registry answers for the saga: InFlight once its row has left the
\* registry (and before a decision), Indeterminate while it declines to report
\* the row, the recorded outcome otherwise.
RegistryView ==
    IF forgotten \/ dec = "none" THEN "inflight"
    ELSE IF masked THEN "indeterminate"
    ELSE dec

\* A key the read gate hides: the registry reports Indeterminate for the saga
\* whose bucket the leaf holds, which the gate tests ahead of its orphan guard.
\* A hidden read is the gate declining to answer, not a value.
Hidden == 99

\* The leaf read gate: a bucket of a committed saga surfaces unless the
\* activation remembers applying its terminal (the orphan guard). A bucket
\* stamped after the later write outranks the row; an older one is LWW-merged.
Surfaced(c, s, k) == pend[c][s][k] \in {"old", "new"} /\ RegistryView = "committed" /\ ~term[c][s]

\* A "mark" is a destination-side shadow marker without a bucket
\* (ShadowedMigrationReadGuard): it gates the migrated value while the saga may
\* have committed and its terminal has not landed here. Markers live in
\* activation memory only. The base never installs one without a bucket.
ValueAt(c, s, k) ==
    IF pend[c][s][k] = "mark"
    THEN IF RegistryView \in {"committed", "indeterminate"} /\ ~term[c][s] THEN Hidden ELSE row[c][s][k]
    ELSE IF pend[c][s][k] # "none" /\ RegistryView = "indeterminate" THEN Hidden
    ELSE IF Surfaced(c, s, k)
    THEN IF pend[c][s][k] = "new" THEN SagaV ELSE Max(row[c][s][k], SagaV)
    ELSE row[c][s][k]

ReadVia(p, k) == ValueAt(p[1], MapOf(p[2], k), k)
OwnerValue(k) == ValueAt(alias, MapOf(rmap, k), k)

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
\* resize is in flight R must hold everything T acknowledged.
AckCopies(c) == IF c = T /\ rz \in FwdPhases THEN {T, R} ELSE {c}

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
    ELSE IF pend[c][s][k] = "new" THEN SagaV
    ELSE IF pend[c][s][k] = "old" \/ MapOf(CopyMap(c), k) = s THEN Max(row[c][s][k], SagaV)
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
    /\ sp = "idle"
    /\ spCopy = T
    /\ rz = "idle"
    /\ rzShards = {}
    /\ fence = {}
    /\ redir = FALSE
    /\ sg = "idle"
    /\ bound = T
    /\ prepped = {}
    /\ told = {}
    /\ dec = "none"
    /\ masked = FALSE
    /\ forgotten = FALSE
    /\ late = "none"
    /\ wDone = FALSE
    /\ reacted = FALSE
    /\ ackOn = [c \in Copies |-> [k \in Keys |-> IF c = T THEN InitV ELSE Absent]]
    /\ vis = [k \in Keys |-> InitV]

-----------------------------------------------------------------------------
(* Adaptive split of k2's slot from s1 to s2 on the copy the tree resolves  *)
(* to (TreeShardSplitGrain). An adaptive split refuses a resize until the old *)
(* copy is purged or the resize undone, and a resize refuses a split in   *)
(* flight (#4452). The old copy mirrors index-for-index into the resized  *)
(* copy and a saga may stay bound to it until the purge, so a split that  *)
(* changed the resized copy's layout meanwhile would strand the bound      *)
(* saga's buckets (mutation NoKeyLostSplitInSoftDeleteWindow).            *)

SplitBegin ==
    /\ sp = "idle"
    /\ rz \in {"idle", "purged", "undone"}
    /\ rmap = s1
    /\ ~Fenced(alias, s1)
    /\ sp' = "shadow"
    /\ spCopy' = alias
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* Retroactive sweep of prepares that predate the window: a bucket whose saga
\* the registry reports decided is resolved at the destination (with the
\* committed-values backstop for a commit) instead of being replayed; one the
\* registry reports InFlight is replayed. The intended design resolves an
\* Indeterminate answer to the recorded decision behind it, as the #4445 leaf
\* refusal does; production replays it as a forwarded prepare the destination
\* refuses, leaving only an activation-scoped shadow marker there
\* (#4473).
SplitSweep ==
    /\ sp = "shadow"
    /\ alias = spCopy
    /\ LET c == spCopy
           b == pend[c][s1][k2]
           view == IF RegistryView = "indeterminate" THEN dec ELSE RegistryView
           dests == {<<c, s2>>} \cup (IF ResizeMirrors(c, s2) THEN {<<R, s2>>} ELSE {})
       IN IF b = "none"
          THEN UNCHANGED <<row, pend, term>>
          ELSE IF view \in {"committed", "aborted"}
               THEN /\ row' = IF view = "committed"
                              THEN [row EXCEPT ![c][s2][k2] = Max(@, SagaV)]
                              ELSE row
                    /\ term' = [term EXCEPT ![c][s2] = TRUE]
                    /\ UNCHANGED pend
               ELSE /\ pend' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                                   IF <<x, s>> \in dests /\ k = k2 THEN b ELSE pend[x][s][k]]]]
                    /\ UNCHANGED <<row, term>>
    /\ sp' = "swept"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* Mark the source's leaves moved away and enter Reject: the source refuses k2.
SplitFreeze ==
    /\ sp = "swept"
    /\ alias = spCopy
    /\ ~Fenced(spCopy, s1)
    /\ sp' = "frozen"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* Final authoritative drain, then the fenced ReassignSlotsAsync: the map moves
\* k2 to s2 only while the tree still resolves to the bound copy
\* (ShardMapCommitFence).
SplitCommit ==
    /\ sp = "frozen"
    /\ LET c == spCopy
           v == Max(row[c][s2][k2], row[c][s1][k2])
       IN row' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                     IF k = k2 /\ s = s2 /\ (x = c \/ (x = R /\ ResizeMirrors(c, s2)))
                     THEN Max(row[x][s][k], v) ELSE row[x][s][k]]]]
    /\ alias = spCopy
    /\ rmap' = s2
    /\ published' = published \cup {<<alias, s2>>}
    /\ sp' = "done"
    /\ UNCHANGED <<alias, rmapR, rmapOld, pend, term, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

-----------------------------------------------------------------------------
(* Online resize T -> R (TreeResizeGrain + TreeSnapshotGrain).              *)

ResizeBegin ==
    /\ rz = "idle"
    /\ alias = T
    /\ sp \in {"idle", "done"}
    /\ rz' = "snap"
    /\ rzShards' = {s1, rmap}
    /\ rmapR' = rmap
    /\ rmapOld' = rmap
    /\ ackOn' = [ackOn EXCEPT ![R] = ackOn[T]]
    /\ UNCHANGED <<alias, rmap, published, row, pend, term, sp, spCopy, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted>>

\* The online snapshot copies T's committed entries index-for-index, keeping an
\* entry only on the shard the copy's map routes it to. The intended design also
\* carries T's prepared buckets; production copies committed entries only.
SnapCopy ==
    /\ rz = "snap"
    /\ row' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                 IF x = R /\ s \in rzShards /\ MapOf(rmapR, k) = s
                 THEN Max(row[R][s][k], row[T][s][k]) ELSE row[x][s][k]]]]
    /\ pend' = [x \in Copies |-> [s \in Shards |-> [k \in Keys |->
                 IF x = R /\ s \in rzShards /\ pend[R][s][k] = "none"
                 THEN pend[T][s][k] ELSE pend[x][s][k]]]]
    /\ rz' = "copied"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, term, sp, spCopy, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* EnterRejectingAsync on one of T's shards, before the alias flip (#4362).
ResizeFence(s) ==
    /\ rz = "copied"
    /\ s \in rzShards \ fence
    /\ fence' = fence \cup {s}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* The single-write SwapAliasAsync (#4357), only once every shard is fenced.
ResizeFlip ==
    /\ rz = "copied"
    /\ fence = rzShards
    /\ alias' = R
    /\ rmap' = rmapR
    /\ published' = published \cup {<<R, rmapR>>}
    /\ rz' = "swapped"
    /\ UNCHANGED <<rmapR, rmapOld, row, pend, term, sp, spCopy, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* RejectOldShardsAsync then CleanupOldTreeAsync: T is soft-deleted and stays
\* fenced; a saga bound to it is still admitted.
ResizeRetire ==
    /\ rz = "swapped"
    /\ fence' = rzShards
    /\ rz' = "retired"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rzShards, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* The purge after SoftDeleteDuration clears T's shard rows. A router whose
\* cached pair still names T is assumed gone by then (see RefinementRetention.md).
ResizePurge ==
    /\ rz = "retired"
    /\ rz' = "purged"
    /\ published' = {p \in published : p[1] # T}
    /\ row' = [row EXCEPT ![T] = [s \in Shards |-> [k \in Keys |-> Absent]]]
    /\ pend' = [pend EXCEPT ![T] = [s \in Shards |-> [k \in Keys |-> "none"]]]
    /\ term' = [term EXCEPT ![T] = [s \in Shards |-> FALSE]]
    /\ fence' = {}
    /\ UNCHANGED <<alias, rmap, rmapR, rmapOld, sp, spCopy, rzShards, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* Undo after the flip, first step: ArmRedirectsAsync arms the resized copy's
\* shards to redirect routers that still address it (#4357). The undo arms R
\* and swaps before it lifts T's fence, so no instant has both copies serving
\* (#4453).
UndoArm ==
    /\ rz \in {"swapped", "retired"}
    /\ alias = R
    /\ redir' = TRUE
    /\ rz' = "undoing"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rzShards, fence, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* One registry write moves the alias and the old map back (#4357).
UndoSwap ==
    /\ rz = "undoing"
    /\ alias = R
    /\ alias' = T
    /\ rmap' = rmapOld
    /\ published' = published \cup {<<T, rmapOld>>}
    /\ UNCHANGED <<rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* Recover T and ClearShadowForwardAsync on its shards: the fence lifts and T
\* serves again under the alias that now names it.
UndoClear ==
    /\ rz = "undoing"
    /\ alias = T
    /\ fence' = {}
    /\ rz' = "undone"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rzShards, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

-----------------------------------------------------------------------------
(* The atomic-write saga (AtomicWriteGrain), writing SagaV to k1 and k2.    *)

SagaStart ==
    /\ sg = "idle"
    /\ sg' = "exec"
    /\ bound' = alias
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, prepped, told, dec, masked, forgotten, late, wDone, reacted, ackOn>>

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
          /\ late' = IF SplitMirrors(c, s, k) /\ late = "none" THEN "inflight" ELSE late
    /\ prepped' = prepped \cup {k}
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, told, dec, masked, forgotten, wDone, reacted, ackOn>>

\* Record the commit decision: the tree still resolves to the bound copy, or
\* the bound copy mirrors into the one it resolves to (#4369).
SagaDecide ==
    /\ sg = "exec"
    /\ prepped = Keys
    /\ alias = bound \/ BoundMirrors
    /\ dec' = "committed"
    /\ sg' = "decided"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, bound, prepped, told, masked, forgotten, late, wDone, reacted, ackOn>>

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
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, dec, masked, forgotten, late, wDone, reacted, ackOn>>

\* The broadcast has visited every target: the saga completes, and a committed
\* saga's caller is acknowledged.
SagaComplete ==
    /\ sg = "decided"
    /\ TermTargets \subseteq told
    /\ sg' = "done"
    /\ ackOn' = [c \in Copies |-> [k \in Keys |->
                    IF dec = "committed" /\ c \in AckCopies(bound) THEN Max(ackOn[c][k], SagaV) ELSE ackOn[c][k]]]
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, bound, prepped, told, dec, masked, forgotten, late, wDone, reacted>>

-----------------------------------------------------------------------------
(* Environment                                                              *)

\* A delayed copy of the split window's shadow-forwarded prepare reaches the
\* destination. The leaf refuses it when its activation remembers applying the
\* saga's terminal, or when the registry answers anything but InFlight for the
\* saga (#4445): an Indeterminate answer is resolved to the recorded decision
\* the registry holds behind its mask.
DeliverLate ==
    /\ late = "inflight"
    /\ late' = "delivered"
    /\ IF term[spCopy][s2] \/ RegistryView # "inflight" \/ Gone(spCopy)
       THEN UNCHANGED pend
       ELSE pend' = [x \in Copies |-> [y \in Shards |-> [k \in Keys |->
                        IF k = k2 /\ <<x, y>> \in Landing(spCopy, s2, k2)
                        THEN IF wDone THEN "new" ELSE "old"
                        ELSE pend[x][y][k]]]]
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, wDone, reacted, ackOn>>

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
                        THEN Max(row[x][y][k], LaterV) ELSE row[x][y][k]]]]
          /\ ackOn' = [x \in Copies |-> [k \in Keys |->
                          IF k = k2 /\ x \in AckCopies(c) THEN Max(ackOn[x][k], LaterV) ELSE ackOn[x][k]]]
    /\ wDone' = TRUE
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, pend, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, reacted>>

\* The saga records an abort: a prepare failed past its retries, or the caller
\* went away. Any point of the execute phase may end this way, so the action is
\* unguarded beyond that; its terminal broadcast is the compensation.
SagaAbort ==
    /\ sg = "exec"
    /\ dec' = "aborted"
    /\ sg' = "decided"
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, bound, prepped, told, masked, forgotten, late, wDone, reacted, ackOn>>

\* The registry stops reporting the saga's row, or starts again. Production
\* answers TxStatus.Indeterminate for a stored row once the tombstone retention
\* window has elapsed before PruneExpired purged it, and for a delegated
\* cross-tree txid whose coordinator cannot be dialled; a snapshot pin covering
\* an expired tombstone re-exposes the row. The dial failure has no ordering at
\* all, so the action may fire at any point before the row is retired.
RegistryMask ==
    /\ dec # "none"
    /\ ~forgotten
    /\ masked' = ~masked
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, forgotten, late, wDone, reacted, ackOn>>

\* The saga's row leaves the registry: ForgetAsync, the PruneExpired purge
\* behind it, or the zero-retention branch. Each follows the saga's terminal
\* fan-out, so the row is retired only once the saga has completed.
RegistryForget ==
    /\ sg = "done"
    /\ ~forgotten
    /\ forgotten' = TRUE
    /\ masked' = FALSE
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, pend, term, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, late, wDone, reacted, ackOn>>

\* A leaf activation is replaced: its memory of applied terminals and its shadow
\* markers are lost; its prepared buckets are rebuilt from the log.
\* Next offers it on s2 only: nothing reaches a shard after its terminal except
\* at the split destination (a late forward, a sweep replay or its marker), so
\* on s1 the lost memory is never read and the step would only spend the budget.
Reactivate(c, s) ==
    /\ ~reacted
    /\ term[c][s] \/ \E k \in Keys : pend[c][s][k] = "mark"
    /\ term' = [term EXCEPT ![c][s] = FALSE]
    /\ pend' = [pend EXCEPT ![c][s] = [k \in Keys |-> IF @[k] = "mark" THEN "none" ELSE @[k]]]
    /\ reacted' = TRUE
    /\ UNCHANGED <<alias, rmap, published, rmapR, rmapOld, row, sp, spCopy, rz, rzShards, fence, redir, sg, bound, prepped, told, dec, masked, forgotten, late, wDone, ackOn>>

\* Nothing is in flight: every operation that started has finished. An operation
\* that never started is not owed, so a state with no enabled step is a deadlock
\* only when something is stuck part way.
Quiescent ==
    /\ sg \in {"idle", "done"}
    /\ sp \in {"idle", "done"}
    /\ rz \in {"idle", "purged", "undone"}
    /\ late # "inflight"

Stutter ==
    /\ Quiescent
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ SplitBegin
    \/ SplitSweep
    \/ SplitFreeze
    \/ SplitCommit
    \/ ResizeBegin
    \/ SnapCopy
    \/ \E s \in Shards : ResizeFence(s)
    \/ ResizeFlip
    \/ ResizeRetire
    \/ ResizePurge
    \/ UndoArm
    \/ UndoSwap
    \/ UndoClear
    \/ SagaStart
    \/ \E k \in Keys : SagaPrepare(k, CurrentPair)
    \/ SagaDecide
    \/ SagaAbort
    \/ \E s \in Shards : SagaTerminal(s)
    \/ SagaComplete
    \/ RegistryMask
    \/ RegistryForget
    \/ DeliverLate
    \/ LaterWrite(CurrentPair)
    \/ \E c \in Copies : Reactivate(c, s2)
    \/ Stutter

\* Every step a running coordinator takes on its own is weakly fair. Starting
\* an operation, an undo, the registry's mask and retirement, the late
\* delivery and the reactivation are not. With no re-bind here, a saga whose
\* bound copy can no longer commit is fairly aborted, which is what
\* production's prepare retries come to when they cannot re-bind.
\* The ghost vis tracks, per key, the highest value a fresh reader has been
\* served; a hidden read leaves it alone. The undo's swap back to T resets it,
\* because an undo discards the resized copy's writes by contract.
VisStep ==
    vis' = [k \in Keys |->
              IF alias = R /\ alias' = T
              THEN (IF OwnerValue(k)' = Hidden THEN InitV ELSE OwnerValue(k)')
              ELSE IF OwnerValue(k)' = Hidden THEN vis[k]
              ELSE Max(vis[k], OwnerValue(k)')]

Spec ==
    /\ Init /\ [][Next /\ VisStep]_vars
    /\ WF_svars(SplitSweep) /\ WF_svars(SplitFreeze) /\ WF_svars(SplitCommit)
    /\ WF_svars(SnapCopy) /\ WF_svars(\E s \in Shards : ResizeFence(s)) /\ WF_svars(ResizeFlip)
    /\ WF_svars(ResizeRetire) /\ WF_svars(ResizePurge) /\ WF_svars(UndoSwap) /\ WF_svars(UndoClear)
    /\ WF_svars(\E k \in Keys : SagaPrepare(k, CurrentPair))
    /\ WF_svars(SagaDecide) /\ WF_svars(~(alias = bound \/ BoundMirrors) /\ SagaAbort)
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
    /\ pend \in [Copies -> [Shards -> [Keys -> {"none", "old", "new", "mark"}]]]
    /\ term \in [Copies -> [Shards -> BOOLEAN]]
    /\ sp \in {"idle", "shadow", "swept", "frozen", "done"}
    /\ spCopy \in Copies
    /\ rz \in {"idle", "snap", "copied", "swapped", "retired", "purged", "undoing", "undone"}
    /\ rzShards \subseteq Shards
    /\ fence \subseteq Shards
    /\ redir \in BOOLEAN
    /\ sg \in {"idle", "exec", "decided", "done"}
    /\ bound \in Copies
    /\ prepped \subseteq Keys
    /\ told \subseteq Shards
    /\ dec \in {"none", "committed", "aborted"}
    /\ masked \in BOOLEAN
    /\ forgotten \in BOOLEAN
    /\ late \in {"none", "inflight", "delivered"}
    /\ wDone \in BOOLEAN
    /\ reacted \in BOOLEAN
    /\ ackOn \in [Copies -> [Keys -> Vals]]
    /\ vis \in [Keys -> Vals]

\* The owner's location holds every value acknowledged to a writer of the key.
NoKeyLost ==
    \A k \in Keys : OwnerValue(k) = Hidden \/ OwnerValue(k) >= ackOn[alias][k]

\* No read served through any routing pair returns a value older than one
\* already acknowledged.
NoResurrection ==
    \A p \in published, k \in Keys :
        (Serves(p, k) /\ ReadVia(p, k) # Hidden) => ReadVia(p, k) >= ackOn[alias][k]

\* Until the later write lands, a fresh reader sees the saga's batch on both keys
\* or on neither.
AtomicOnOwner ==
    (~wDone /\ OwnerValue(k1) # Hidden /\ OwnerValue(k2) # Hidden)
        => ((OwnerValue(k1) >= SagaV) = (OwnerValue(k2) >= SagaV))

\* The value a fresh reader gets for a key never moves backwards: it is never
\* below the highest value already served, except across the undo's swap back
\* to T, which discards the resized copy's writes by contract. Stated over
\* history (the ghost vis) because a hidden read in between must not launder a
\* reversion.
OwnerMonotonic ==
    \A k \in Keys : OwnerValue(k) = Hidden \/ OwnerValue(k) >= vis[k]

SplitCompletes == sp \in {"shadow", "swept", "frozen"} ~> sp = "done"
ResizeCompletes == rz \in {"snap", "copied", "swapped", "retired", "undoing"} ~> rz \in {"purged", "undone"}
SagaCompletes == sg \in {"exec", "decided"} ~> sg = "done"

\* No stranded bucket: a prepared bucket of a decided saga on a copy that can
\* still become the tree is eventually consumed by its terminal, unless the
\* registry has retired the saga's row first. A bucket admitted after that is
\* outside it: the read gate reads it as InFlight and falls through to the
\* projection, and only a later prepare of the key shadows it (#4445).
LiveBucket == \E c \in Copies, s \in Shards, k \in Keys : Live(c) /\ pend[c][s][k] \in {"old", "new"}

\* Stated as one leads-to rather than one per bucket: with one saga, no bucket
\* appears after the decision until the row is retired (a forwarded prepare
\* is refused while the registry reports the decision), so "every bucket is
\* eventually consumed" and "eventually none is left" coincide here.
NoStrandedBucket ==
    (dec # "none" /\ LiveBucket) ~> (~LiveBucket \/ forgotten)

=============================================================================
