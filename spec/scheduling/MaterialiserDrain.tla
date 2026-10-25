---- MODULE MaterialiserDrain ----
EXTENDS Integers, FiniteSets

CONSTANTS Consumers, DrainCap, Leaves, LeavesPerPass, Head, ReplayBatch,
          DrainBudget, InitialLag, Offered, LagWitness, Keys
VARIABLES mode, phase, queued, active, remaining, work,
          applied, checkpoint, covered, pin, checkpointFailed,
          lag, pressure, acute, batch,
          outstanding, pending, elapsed, drainRound, faultOwner,
          split, route, key, repairKind
vars == <<mode, phase, queued, active, remaining, work,
          applied, checkpoint, covered, pin, checkpointFailed,
          lag, pressure, acute, batch,
          outstanding, pending, elapsed, drainRound, faultOwner,
          split, route, key, repairKind>>
Min(a, b) == IF a < b THEN a ELSE b
Owner == <<"tree-A", 1>>

\* Reuse the reviewed consumer abstraction, not a second throttle policy.
\* Only the pure ViewBatch operator is consumed. Other substitutions make the
\* unrelated closed-loop instance explicit; none is a scheduling premise.
BP == INSTANCE WalBackPressure WITH
    Trees <- {1}, Partitions <- {1}, RecoveryWindow <- 0, AcuteOnly <- TRUE,
    Windows <- 1, DispatchBudget <- 1, QuiesceBudget <- 1, Bursts <- 1,
    AllowReroute <- FALSE, FullBatch <- 4, CauseBound <- 1,
    input <- 0, verdict <- 0, treeVerdict <- 0, recovery <- 0,
    history <- 0, phase <- 0, remaining <- 0, sampledCause <- 0,
    consumer <- 0, saga <- 0, epoch <- 0, aliased <- FALSE,
    snapshotUsable <- FALSE, shuttingDown <- FALSE, waitsOn <- {},
    reliefDone <- {}

TypeOK ==
    /\ mode \in {"Slots", "Leaf", "View", "Shutdown", "Alias"}
    /\ phase \in {"new", "copy", "admission", "refused", "replay", "snapshot", "pin", "offer",
                   "sample", "pull", "drain", "route", "read", "publish", "done"}
    /\ queued \subseteq Consumers /\ active \subseteq Consumers
    /\ remaining \in [Consumers -> 0..Leaves] /\ work \in 0..(Leaves + 1)
    /\ applied \in -1..Head /\ checkpoint \in -1..Head
    /\ covered \in -1..Head /\ pin \in -1..Head
    /\ checkpointFailed \in BOOLEAN
    /\ lag \in 0..LagWitness /\ pressure \in 0..2 /\ acute \in BOOLEAN
    /\ batch \in 0..4
    /\ outstanding \subseteq Consumers /\ pending \in BOOLEAN
    /\ elapsed \in 0..DrainBudget /\ drainRound \in 1..2
    /\ faultOwner \in {<<>>, Owner, <<"tree-B", 2>>}
    /\ split \in 0..1 /\ route \in 0..1 /\ key \in 1..(Keys + 1)
    /\ repairKind \in {"Rebuild", "Reconcile"}
DrainSlotsRespected == Cardinality(active) <= DrainCap
BackgroundPassBounded == work <= LeavesPerPass
NoDrainPastHead == applied < Head /\ checkpoint <= applied
PinBackedByDurability == pin <= checkpoint /\ pin <= covered
BudgetForceFaultAttributed == faultOwner # <<>> => faultOwner = Owner
ConsumerBatchesRespond ==
    [][phase = "sample" /\ phase' = "pull" =>
        /\ batch' = BP!ViewBatch(pressure')
        /\ pressure' \in (IF acute THEN {2} ELSE {0, 1})]_vars
CheckpointAfterApply ==
    [][checkpoint' > checkpoint => checkpoint' <= applied]_vars
SourceReadUsesLiveTopology ==
    [][mode = "Alias" /\ phase = "read" /\ key' > key => route = split]_vars
LagBounded == mode = "View" => []<>(lag <= InitialLag)
DrainEventuallyCompletes == mode = "Shutdown" => <>(phase = "done")
RebuildProgressAcrossAlias == mode = "Alias" => <>(phase = "done")
LeafDrainCompletes == mode = "Leaf" => <>(phase = "refused" \/ (phase = "done" /\ pin = Head - 1))
OnlineDrainCompletes == mode = "Slots" => <>(queued = {} /\ active = {} /\ phase = "done")

Init ==
    /\ mode \in {"Slots", "Leaf", "View", "Shutdown", "Alias"}
    /\ phase = "new" /\ queued = Consumers /\ active = {}
    /\ remaining = [c \in Consumers |-> Leaves] /\ work = 0
    /\ applied = -1 /\ checkpoint = -1 /\ covered = -1 /\ pin = -1
    /\ checkpointFailed = FALSE
    /\ lag = InitialLag /\ pressure = 0 /\ acute = FALSE /\ batch = 4
    /\ outstanding = Consumers /\ pending \in BOOLEAN
    /\ elapsed = 0 /\ drainRound = 1 /\ faultOwner = <<>>
    /\ split = 0 /\ route = 0 /\ key = 1
    /\ repairKind \in {"Rebuild", "Reconcile"}

Begin ==
    /\ phase = "new"
    /\ phase' = CASE mode = "Slots" -> "copy" [] mode = "Leaf" -> "admission"
                 [] mode = "View" -> "offer" [] mode = "Shutdown" -> "drain"
                 [] OTHER -> "route"
    /\ UNCHANGED <<queued, active, remaining, work, applied, checkpoint, covered,
        pin, checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind, mode>>

StartDrain(c) ==
    /\ mode = "Slots" /\ phase = "copy" /\ c \in queued
    /\ Cardinality(active) < DrainCap
    /\ queued' = queued \ {c} /\ active' = active \cup {c}
    /\ UNCHANGED <<mode, phase, remaining, work, applied, checkpoint, covered,
        pin, checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

LeafPass(c) ==
    /\ mode = "Slots" /\ phase = "copy" /\ c \in active /\ remaining[c] > 0
    /\ work' = Min(LeavesPerPass, remaining[c])
    /\ remaining' = [remaining EXCEPT ![c] = @ - work']
    /\ UNCHANGED <<mode, phase, queued, active, applied, checkpoint, covered,
        pin, checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

CompleteDrain(c) ==
    /\ mode = "Slots" /\ phase = "copy" /\ c \in active /\ remaining[c] = 0
    /\ active' = active \ {c}
    /\ phase' = IF queued = {} /\ active' = {} THEN "done" ELSE phase
    /\ UNCHANGED <<mode, queued, remaining, work, applied, checkpoint, covered,
        pin, checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

\* Composition boundary with ReplayPermits: completed admission grants the
\* owner or terminally refuses it. Waiting/cancellation/pool scheduling live in
\* that companion, not a second invented pool here. Successful replay storage
\* and subsequent durability completions are premises of LeafDrainCompletes.
ReplayAdmission ==
    /\ mode = "Leaf" /\ phase = "admission"
    /\ phase' \in {"replay", "refused"}
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

ReplayRead ==
    /\ mode = "Leaf" /\ phase = "replay" /\ applied < Head - 1
    /\ applied' = Min(Head - 1, applied + ReplayBatch)
    /\ UNCHANGED <<mode, phase, queued, active, remaining, work, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

CheckpointFault ==
    /\ mode = "Leaf" /\ phase = "replay" /\ applied = Head - 1 /\ ~checkpointFailed
    /\ checkpointFailed' = TRUE
    /\ UNCHANGED <<mode, phase, queued, active, remaining, work, applied,
        checkpoint, covered, pin, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

CheckpointPersist ==
    /\ mode = "Leaf" /\ phase = "replay" /\ applied = Head - 1
    /\ checkpoint' = applied /\ phase' = "snapshot"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, covered, pin,
        checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

SnapshotCapture ==
    /\ mode = "Leaf" /\ phase = "snapshot"
    /\ covered' = checkpoint /\ phase' = "pin"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        pin, checkpointFailed, lag, pressure, acute, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

PinPublish ==
    /\ mode = "Leaf" /\ phase = "pin"
    /\ pin' = Min(checkpoint, covered) /\ phase' = "done"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

\* A round is ONE successful service opportunity, including any configured
\* background pause. Offered bounds writes between such opportunities, not
\* writes per sampler tick. LagWitness is a saturating overflow witness, not
\* an arrival guard: a broken consumer can reach it and remain above the bound.
OfferWrites(n, a) ==
    /\ mode = "View" /\ phase = "offer" /\ n \in 0..Offered /\ a \in BOOLEAN
    /\ lag' = Min(LagWitness, lag + n) /\ acute' = a /\ phase' = "sample"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, pressure, batch, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

Sample ==
    /\ mode = "View" /\ phase = "sample"
    \* Backlog is a work count, while the real lag classifier measures the age
    \* of the oldest unread offset over qualifying windows. Do not equate them.
    \* Abstract that completed classifier decision by either non-acute arm;
    \* only an independent Saturated regime can select the third arm.
    /\ pressure' \in (IF acute THEN {2} ELSE {0, 1})
    /\ batch' = BP!ViewBatch(pressure') /\ phase' = "pull"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, acute, outstanding, pending,
        elapsed, drainRound, faultOwner, split, route, key, repairKind>>

ViewPull ==
    /\ mode = "View" /\ phase = "pull"
    /\ lag' = lag - Min(lag, batch) /\ phase' = "offer"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

\* Storage may ignore cancellation forever. Only timer delivery, not storage
\* success, is required for the finite-budget shutdown obligation.
StorageSettle(c) ==
    /\ mode = "Shutdown" /\ phase = "drain" /\ c \in outstanding
    /\ outstanding' = outstanding \ {c}
    /\ UNCHANGED <<mode, phase, queued, active, remaining, work, applied,
        checkpoint, covered, pin, checkpointFailed, lag, pressure, acute, batch,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

BudgetTick ==
    /\ mode = "Shutdown" /\ phase = "drain" /\ outstanding # {}
    /\ elapsed < DrainBudget /\ elapsed' = elapsed + 1
    /\ UNCHANGED <<mode, phase, queued, active, remaining, work, applied,
        checkpoint, covered, pin, checkpointFailed, lag, pressure, acute, batch,
        outstanding, pending, drainRound, faultOwner, split, route, key, repairKind>>

DrainFinish ==
    /\ mode = "Shutdown" /\ phase = "drain"
    /\ outstanding = {} \/ elapsed = DrainBudget
    /\ LET expired == outstanding # {}
           again == ~expired /\ pending
       IN /\ faultOwner' = IF expired THEN Owner ELSE faultOwner
          /\ outstanding' = IF again THEN Consumers ELSE {}
          /\ pending' = FALSE
          /\ drainRound' = IF again THEN 2 ELSE drainRound
          /\ elapsed' = IF again THEN 0 ELSE elapsed
          /\ phase' = IF again THEN "drain" ELSE "done"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch,
        split, route, key, repairKind>>

\* One split under a STABLE physical identity, not an alias cutover.
SplitTopology ==
    /\ mode = "Alias" /\ split = 0
    /\ split' = 1
    /\ UNCHANGED <<mode, phase, queued, active, remaining, work, applied,
        checkpoint, covered, pin, checkpointFailed, lag, pressure, acute, batch,
        outstanding, pending, elapsed, drainRound, faultOwner, route, key, repairKind>>

RefreshSourceRouting ==
    /\ mode = "Alias" /\ phase \in {"route", "read"} /\ key <= Keys
    /\ route' = split /\ phase' = "read"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, key, repairKind>>

ReadSource ==
    /\ mode = "Alias" /\ phase = "read" /\ route = split /\ key <= Keys
    /\ key' = key + 1
    /\ phase' = IF key' > Keys THEN "publish" ELSE "route"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, repairKind>>

PublishRepair ==
    /\ mode = "Alias" /\ phase = "publish" /\ phase' = "done"
    /\ UNCHANGED <<mode, queued, active, remaining, work, applied, checkpoint,
        covered, pin, checkpointFailed, lag, pressure, acute, batch, outstanding,
        pending, elapsed, drainRound, faultOwner, split, route, key, repairKind>>

Stutter == UNCHANGED vars
Next ==
    \/ Begin
    \/ \E c \in Consumers : StartDrain(c)
    \/ \E c \in Consumers : LeafPass(c)
    \/ \E c \in Consumers : CompleteDrain(c)
    \/ ReplayAdmission
    \/ ReplayRead
    \/ CheckpointFault
    \/ CheckpointPersist
    \/ SnapshotCapture
    \/ PinPublish
    \/ \E n \in 0..Offered, a \in BOOLEAN : OfferWrites(n, a)
    \/ Sample
    \/ ViewPull
    \/ \E c \in Consumers : StorageSettle(c)
    \/ BudgetTick
    \/ DrainFinish
    \/ SplitTopology
    \/ RefreshSourceRouting
    \/ ReadSource
    \/ PublishRepair
    \/ Stutter

\* Delivered callbacks/await completions, not arbitrary competing policies.
\* Refresh occurs only for a new key or a stale response; an already-current
\* route cannot monopolise the successful read opportunity.
Delivery ==
    \/ Begin
    \/ \E c \in Consumers : StartDrain(c) \/ LeafPass(c) \/ CompleteDrain(c)
    \/ ReplayAdmission \/ ReplayRead \/ CheckpointPersist \/ SnapshotCapture \/ PinPublish
    \/ \E n \in 0..Offered, a \in BOOLEAN : OfferWrites(n, a)
    \/ Sample \/ ViewPull
    \/ BudgetTick \/ DrainFinish
    \/ (phase = "route" \/ route # split) /\ RefreshSourceRouting
    \/ ReadSource \/ PublishRepair
Spec == Init /\ [][Next]_vars /\ WF_vars(Delivery)
====
