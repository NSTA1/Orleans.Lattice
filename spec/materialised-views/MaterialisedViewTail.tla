------------------------ MODULE MaterialisedViewTail ------------------------
(***************************************************************************)
(* A materialised view tailing its source tree's write-ahead log: the     *)
(* steady drain from the durable applied offset, the checkpoint that      *)
(* publishes the view's read position and reports its GC cursor pin, WAL   *)
(* GC (the cursor-entitled trim and the retention-TTL trim that may       *)
(* overtake a reader), fall-off detection, lag eviction, crash and replay, *)
(* the shadow-generation rebuild with its resume floor, reconcile after   *)
(* out-of-band drift, generation reclaim, decommission, and a source      *)
(* identity swap (an online resize, a reshard's physical binding change,  *)
(* or a shadow-cutover restore) followed by the view's rebind onto the     *)
(* new physical log.                                                       *)
(*                                                                         *)
(* Values are reduced to per-key versions; a source write appends a       *)
(* (key, version) entry to the physical log the alias names, and the view *)
(* overwrites each key with the entry it applies (a plain view-tree      *)
(* write, so replay transiently regresses keys). Atomic and cross-tree   *)
(* staging is out of scope (see README.md); the alias reservation, copy *)
(* and fence ordering of the lifecycle operations are TreeLifecycle's, so the *)
(* identity swap is an environment step here.                              *)
(*                                                                         *)
(* See README.md for the instance and RefinementTail.md for the mapping.   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Keys, MaxWrites, MaxTtl, MaxDrift, MaxCrashes, LagBudget

\* P1 is the physical log the logical tree names at creation (its physical id
\* equals its logical id); P2 is the copy a resize, reshard or restore swaps in.
Trees == {"P1", "P2"}
Logical == "P1"
Phases == {"idle", "scanning", "built"}
Entries == [k : Keys, v : 1..MaxWrites]

VARIABLES
    logs,      \* logs[p]: the physical WAL of copy p, a sequence of (key, version) entries
    snap,      \* snap[p]: the state copy p started from (P2's copy or restored backup)
    alias,     \* registry: the physical copy the logical source resolves to
    swapped,   \* the single identity swap has happened
    written,   \* versions issued so far
    trim,      \* trim[p]: entries 1..trim[p] of copy p have been reclaimed by WAL GC
    ttlUsed,   \* retention-TTL trims spent
    driftUsed, \* out-of-band view mutations spent
    drifted,   \* the served view carries an out-of-band mutation no rebuild has replaced
    crashes,   \* maintainer crashes spent
    bound,     \* the physical copy the view's checkpoint is bound to
    vval,      \* the served (active generation) view: highest version per key
    applied,   \* in-memory: entries of logs[bound] applied to the view tree
    hw,        \* high-water: the served view already reflects logs[bound][1..hw]
    ckpt,      \* durable applied offset (AppliedOffsets)
    readReg,   \* copies whose offset-consumer registry lists the view
    pubTree,   \* the copy the view answers read positions for, or none
    pubPos,    \* the read position it answers there (the GC offset floor)
    pinned,    \* copies under which a GC cursor pin is registered for the view
    pin,       \* pin[p]: the reported cursor on copy p
    rb,        \* rebuild: idle -> scanning -> built
    rbTree,    \* the copy the rebuild reads
    rbFloor,   \* the resume floor the rebuild captured before scanning
    rbVal,     \* the shadow generation's scanned contents
    rbHw,      \* the prefix of logs[rbTree] the scan reflected
    active,    \* the active generation
    gens,      \* generation trees still present
    shadow,    \* the physical shadow tree (generation active+1) holds rebuild rows
    deleted    \* the view has been decommissioned

vars == <<logs, snap, alias, swapped, written, trim, ttlUsed, driftUsed, drifted,
          crashes, bound, vval, applied, hw, ckpt, readReg, pubTree, pubPos, pinned, pin,
          rb, rbTree, rbFloor, rbVal, rbHw, active, gens, shadow, deleted>>

Max(S) == CHOOSE m \in S : \A x \in S : x <= m
Min(a, b) == IF a <= b THEN a ELSE b
Zero == [k \in Keys |-> 0]

\* The state of copy p after its first n log entries.
StateAt(p, n) ==
    [k \in Keys |-> Max({snap[p][k]} \cup {logs[p][j].v : j \in {i \in 1..n : logs[p][i].k = k}})]

SrcState(p) == StateAt(p, Len(logs[p]))

\* The bound on how far cursor-entitled GC may trim copy p: the reported
\* cursor, capped by the published read position when the view answers for p.
\* A registration on a copy the view no longer answers for holds nothing.
HeldBy(p) == p \in readReg /\ pubTree = p
CursorBound(p) == IF HeldBy(p) THEN Min(pin[p], pubPos) ELSE pin[p]

TypeOK ==
    /\ logs \in [Trees -> Seq(Entries)]
    /\ \A p \in Trees : Len(logs[p]) <= MaxWrites
    /\ snap \in [Trees -> [Keys -> 0..MaxWrites]]
    /\ alias \in Trees /\ swapped \in BOOLEAN /\ written \in 0..MaxWrites
    /\ trim \in [Trees -> 0..MaxWrites]
    /\ ttlUsed \in 0..MaxTtl /\ driftUsed \in 0..MaxDrift /\ drifted \in BOOLEAN
    /\ crashes \in 0..MaxCrashes
    /\ bound \in Trees /\ vval \in [Keys -> 0..MaxWrites]
    /\ applied \in 0..MaxWrites /\ hw \in 0..MaxWrites /\ ckpt \in 0..MaxWrites
    /\ readReg \subseteq Trees /\ pubTree \in Trees \cup {"none"}
    /\ pubPos \in 0..MaxWrites
    /\ pinned \subseteq Trees /\ pin \in [Trees -> 0..MaxWrites]
    /\ rb \in Phases /\ rbTree \in Trees /\ rbFloor \in 0..MaxWrites
    /\ rbVal \in [Keys -> 0..MaxWrites] /\ rbHw \in 0..MaxWrites
    /\ active \in Nat /\ gens \subseteq Nat /\ shadow \in BOOLEAN /\ deleted \in BOOLEAN

Init ==
    /\ logs = [p \in Trees |-> <<>>]
    /\ snap = [p \in Trees |-> Zero]
    /\ alias = "P1" /\ swapped = FALSE /\ written = 0
    /\ trim = [p \in Trees |-> 0]
    /\ ttlUsed = 0 /\ driftUsed = 0 /\ drifted = FALSE /\ crashes = 0
    /\ bound = "P1" /\ vval = Zero /\ applied = 0 /\ hw = 0 /\ ckpt = 0
    /\ readReg = {"P1"} /\ pubTree = "P1" /\ pubPos = 0
    /\ pinned = {} /\ pin = [p \in Trees |-> 0]
    /\ rb = "idle" /\ rbTree = "P1" /\ rbFloor = 0 /\ rbVal = Zero /\ rbHw = 0
    /\ active = 0 /\ gens = {0} /\ shadow = FALSE /\ deleted = FALSE

ViewVars == <<bound, vval, applied, hw, ckpt, readReg, pubTree, pubPos, pinned, pin,
              rb, rbTree, rbFloor, rbVal, rbHw, active, gens, shadow, deleted>>
SourceVars == <<logs, snap, alias, swapped, written>>
EnvVars == <<trim, ttlUsed, driftUsed, drifted, crashes>>

\* CaptureResumeFloors: register as a reader of copy t, record the head as
\* the resume floor and publish it as the read position before the scan.
StartRebuild(t) ==
    /\ rb' = "scanning" /\ rbTree' = t /\ rbFloor' = Len(logs[t])
    /\ readReg' = readReg \cup {t}
    /\ pubTree' = t /\ pubPos' = Len(logs[t])

-----------------------------------------------------------------------------
(* Source and environment                                                  *)

\* **Environment.** A source write lands on the copy the alias names.
Write ==
    /\ written < MaxWrites
    /\ \E k \in Keys :
        logs' = [logs EXCEPT ![alias] = Append(@, [k |-> k, v |-> written + 1])]
    /\ written' = written + 1
    /\ UNCHANGED <<snap, alias, swapped, EnvVars, ViewVars>>

\* **Environment.** A resize, reshard or shadow-cutover restore moves the
\* alias onto P2: either a copy of P1's current state, or a restored backup
\* taken before any write. The old log is retained and receives no writes.
SourceSwap ==
    /\ ~swapped
    /\ \E copy \in {SrcState("P1"), Zero} :
        snap' = [snap EXCEPT !["P2"] = copy]
    /\ alias' = "P2" /\ swapped' = TRUE
    /\ UNCHANGED <<logs, written, EnvVars, ViewVars>>

\* **Environment.** WAL GC's cursor arm trims a prefix every registered
\* cursor on the copy has passed, never past a published read position.
CursorTrim(p) ==
    /\ p \in pinned
    /\ \E t \in (trim[p] + 1)..CursorBound(p) :
        trim' = [trim EXCEPT ![p] = t]
    /\ UNCHANGED <<SourceVars, ttlUsed, driftUsed, drifted, crashes, ViewVars>>

\* **Environment.** The retention-TTL arm may trim past any reader.
RetentionTrim(p) ==
    /\ ttlUsed < MaxTtl
    /\ \E t \in (trim[p] + 1)..Len(logs[p]) :
        trim' = [trim EXCEPT ![p] = t]
    /\ ttlUsed' = ttlUsed + 1
    /\ UNCHANGED <<SourceVars, driftUsed, drifted, crashes, ViewVars>>

\* **Environment.** An out-of-band write regresses one key of the view tree.
Drift ==
    /\ ~deleted /\ driftUsed < MaxDrift
    /\ \E k \in Keys :
        /\ vval[k] > 0
        /\ vval' = [vval EXCEPT ![k] = 0]
    /\ driftUsed' = driftUsed + 1 /\ drifted' = TRUE
    /\ UNCHANGED <<SourceVars, trim, ttlUsed, crashes, bound, applied, hw, ckpt,
                   readReg, pubTree, pubPos, pinned, pin, rb, rbTree, rbFloor, rbVal, rbHw,
                   active, gens, shadow, deleted>>

\* **Environment.** The maintainer crashes: in-memory progress and any
\* rebuild in flight are lost; the view tree, checkpoint and registrations
\* survive, and so does any partial shadow generation tree the interrupted
\* rebuild already wrote (the next rebuild clears and reuses it).
Crash ==
    /\ crashes < MaxCrashes /\ ~deleted
    /\ applied' = ckpt /\ rb' = "idle"
    /\ pubTree' = bound /\ pubPos' = ckpt
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<SourceVars, trim, ttlUsed, driftUsed, drifted, bound, vval,
                   hw, ckpt, readReg, pinned, pin, rbTree, rbFloor, rbVal, rbHw,
                   active, gens, shadow, deleted>>

-----------------------------------------------------------------------------
(* Maintainer                                                              *)

\* Apply the next entry of the bound log. The view-tree write is a plain
\* overwrite (ApplyAsync's SetAsync/DeleteAsync), not a last-writer-wins
\* merge by source HLC, so re-applying an entry the view already reflects
\* (a crash replay from the checkpoint, or a post-rebuild catch-up from the
\* resume floor) transiently regresses its key until the replay passes the
\* high-water mark.
Drain ==
    /\ ~deleted /\ rb = "idle"
    /\ trim[bound] <= applied /\ applied < Len(logs[bound])
    /\ LET e == logs[bound][applied + 1]
       IN vval' = [vval EXCEPT ![e.k] = e.v]
    /\ applied' = applied + 1
    /\ hw' = IF applied + 1 > hw THEN applied + 1 ELSE hw
    /\ UNCHANGED <<SourceVars, EnvVars, bound, ckpt, readReg, pubTree, pubPos, pinned,
                   pin, rb, rbTree, rbFloor, rbVal, rbHw, active, gens, shadow, deleted>>

\* Persist the applied offset, publish it as the read position, and report
\* the GC cursor under the bound physical copy.
Checkpoint ==
    /\ ~deleted /\ rb = "idle" /\ applied > ckpt
    /\ ckpt' = applied
    /\ readReg' = readReg \cup {bound}
    /\ pubTree' = bound /\ pubPos' = applied
    /\ pinned' = pinned \cup {bound}
    /\ pin' = [pin EXCEPT ![bound] = applied]
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, rb, rbTree,
                   rbFloor, rbVal, rbHw, active, gens, shadow, deleted>>

\* The log was trimmed past the applied offset: rebuild from source state.
FallOff ==
    /\ ~deleted /\ rb = "idle"
    /\ trim[bound] > applied
    /\ StartRebuild(bound)
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, pinned,
                   pin, rbVal, rbHw, active, gens, shadow, deleted>>

\* The view lags its source by more than its budget: evict and rebuild.
LagEvict ==
    /\ ~deleted /\ rb = "idle"
    /\ trim[bound] <= applied
    /\ Len(logs[bound]) - applied > LagBudget
    /\ StartRebuild(bound)
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, pinned,
                   pin, rbVal, rbHw, active, gens, shadow, deleted>>

\* The source resolves to a different physical copy: release the pin on the
\* old copy and rebuild against the new one. The old binding stays until the
\* rebuild swaps.
Rebind ==
    /\ ~deleted /\ rb = "idle"
    /\ alias # bound
    /\ pinned' = pinned \ {bound}
    /\ StartRebuild(alias)
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, pin,
                   rbVal, rbHw, active, gens, shadow, deleted>>

\* A caught-up view whose digest differs from its source is rebuilt.
Reconcile ==
    /\ ~deleted /\ rb = "idle"
    /\ bound = alias /\ trim[bound] <= applied /\ applied = Len(logs[bound])
    /\ vval # SrcState(bound)
    /\ StartRebuild(bound)
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, pinned,
                   pin, rbVal, rbHw, active, gens, shadow, deleted>>

\* Clear the physical shadow tree (generation active+1) and scan the source
\* copy's current state into it.
RebuildScan ==
    /\ ~deleted /\ rb = "scanning"
    /\ rbVal' = SrcState(rbTree) /\ rbHw' = Len(logs[rbTree])
    /\ shadow' = TRUE
    /\ rb' = "built"
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, readReg,
                   pubTree, pubPos, pinned, pin, rbTree, rbFloor, active, gens,
                   deleted>>

\* One durable write flips the active generation, binds the scanned copy,
\* resumes from the floor, and reports the cursor under the bound copy; the
\* previous generation awaits reclamation, and a rebind withdraws its old
\* read registration.
RebuildSwap ==
    /\ ~deleted /\ rb = "built"
    /\ active' = active + 1
    /\ gens' = gens \cup {active + 1}
    /\ shadow' = FALSE
    /\ vval' = rbVal /\ drifted' = FALSE
    /\ applied' = rbFloor /\ ckpt' = rbFloor /\ hw' = rbHw
    /\ bound' = rbTree
    /\ readReg' = IF rbTree = bound THEN readReg ELSE readReg \ {bound}
    /\ pinned' = pinned \cup {rbTree}
    /\ pin' = [pin EXCEPT ![rbTree] = rbFloor]
    /\ rb' = "idle"
    /\ UNCHANGED <<SourceVars, trim, ttlUsed, driftUsed, crashes, pubTree, pubPos,
                   rbTree, rbFloor, rbVal, rbHw, deleted>>

\* Delete a superseded generation tree once its reclaim grace has passed.
Reclaim ==
    /\ ~deleted
    /\ \E g \in gens \ {active} : gens' = gens \ {g}
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, readReg,
                   pubTree, pubPos, pinned, pin, rb, rbTree, rbFloor, rbVal, rbHw, active,
                   shadow, deleted>>

\* **Environment.** Decommission: release the cursor pin under the logical
\* id and the bound physical id, withdraw both read registrations, and
\* delete every generation tree, including the shadow generation an
\* interrupted rebuild may have left.
Delete ==
    /\ ~deleted /\ rb = "idle"
    /\ pinned' = pinned \ {Logical, bound}
    /\ readReg' = readReg \ {Logical, bound}
    /\ gens' = {}
    /\ shadow' = FALSE
    /\ deleted' = TRUE
    /\ pubTree' = "none"
    /\ UNCHANGED <<SourceVars, EnvVars, bound, vval, applied, hw, ckpt, pubPos,
                   pin, rb, rbTree, rbFloor, rbVal, rbHw, active>>

Stutter ==
    /\ UNCHANGED vars

Next ==
    \/ Write
    \/ SourceSwap
    \/ \E p \in Trees : CursorTrim(p)
    \/ \E p \in Trees : RetentionTrim(p)
    \/ Drift
    \/ Crash
    \/ Drain
    \/ Checkpoint
    \/ FallOff
    \/ LagEvict
    \/ Rebind
    \/ Reconcile
    \/ RebuildScan
    \/ RebuildSwap
    \/ Reclaim
    \/ Delete
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Drain) /\ WF_vars(Checkpoint) /\ WF_vars(FallOff)
    /\ WF_vars(LagEvict) /\ WF_vars(Rebind) /\ WF_vars(Reconcile)
    /\ WF_vars(RebuildScan) /\ WF_vars(RebuildSwap) /\ WF_vars(Reclaim)

-----------------------------------------------------------------------------
(* Properties                                                              *)

\* The served view never holds a version its bound copy does not.
ServedNeverAhead ==
    \A k \in Keys : vval[k] <= SrcState(bound)[k]

\* Without out-of-band drift, the served view holds at least the bound
\* copy's state through the applied offset: no trimmed or replayed entry is
\* skipped or overwritten by an older one.
NoLostWrite ==
    ~drifted => \A k \in Keys : vval[k] >= StateAt(bound, applied)[k]

\* The durable checkpoint never runs ahead of the applied view state, and an
\* idle view answers read positions for its bound copy at its checkpoint.
CheckpointWithinApplied ==
    /\ ckpt <= applied /\ applied <= Len(logs[bound])
    /\ (~deleted /\ rb = "idle") => (pubTree = bound /\ pubPos = ckpt)

\* The GC cursor pin is held only under the bound physical copy, never
\* ahead of the durable checkpoint.
PinOnlyOnBinding ==
    /\ pinned \subseteq {bound}
    /\ \A p \in pinned : pin[p] <= ckpt

\* The active generation exists until decommission, and decommission
\* releases every pin, read-position hold and generation, including a
\* partial shadow generation an interrupted rebuild left behind.
DecommissionReleases ==
    /\ ~deleted => active \in gens
    /\ deleted => pinned = {} /\ pubTree = "none" /\ gens = {} /\ ~shadow

\* Cursor-entitled GC trims only below a reported cursor and a published
\* read position.
CursorTrimEntitled ==
    [][\A p \in Trees :
         (trim'[p] > trim[p] /\ ttlUsed' = ttlUsed)
            => /\ p \in pinned /\ trim'[p] <= pin[p]
               /\ (HeldBy(p) => trim'[p] <= pubPos)]_vars

\* Readers never see the view regress while it stays bound to one copy and
\* is not replaying entries it already reflects, other than through
\* out-of-band drift: no partial or empty generation is ever served. A
\* replay below the high-water mark (after a crash, or catching up from a
\* rebuild's resume floor) may transiently regress a key, because view
\* writes overwrite; ReplayConverges bounds that window.
ServedMonotone ==
    [][(bound' = bound /\ driftUsed' = driftUsed /\ applied >= hw)
         => \A k \in Keys : vval'[k] >= vval[k]]_vars

\* The high-water mark never runs past the bound log, and once a replay
\* passes it the served view again reflects the whole prefix it covers.
ReplayConverges ==
    /\ applied <= hw /\ hw <= Len(logs[bound])
    /\ (~drifted /\ applied >= hw /\ rb = "idle")
          => \A k \in Keys : vval[k] = StateAt(bound, applied)[k]

\* The view converges to the copy its source names.
ViewConverges ==
    <>[](deleted \/ (rb = "idle" /\ bound = alias /\ vval = SrcState(alias)))

\* The GC cursor pin converges onto the current copy at its head once that
\* copy has an entry to checkpoint.
PinConverges ==
    <>[](deleted \/ (pinned \subseteq {alias} /\ \A p \in pinned : pin[p] = Len(logs[p])
                     /\ (Len(logs[alias]) > 0 => alias \in pinned)))

\* Superseded generations are reclaimed.
GenerationsReclaimed ==
    <>[](gens \subseteq {active})
=============================================================================
