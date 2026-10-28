---- MODULE WalGcStarvationDrive ----
EXTENDS Naturals, Integers, FiniteSets, TLC

\* The CI-bound core of the GC starvation-drive model. It keeps permits, the
\* GC pass, timer turns, refusal accounting and the storage-gated floor, and
\* drops the per-leaf stall clock and the ordinary call queue. Their claim, that
\* a timer never holds its turn across a drive, is restated here as
\* TurnReleased, and WalGcStarvationDriveWitness refines onto this module to
\* show that dropping them loses no core behaviour.

CONSTANTS LeafCount, Ceiling, Withheld, DriveBudget, RefusedSweepHold
Leaves == 1..LeafCount
VARIABLES leaf, free, cold, hold, head, appended, deleted, timely, admitted, leadDone, rank
vars == <<leaf, free, cold, hold, head, appended, deleted, timely, admitted, leadDone, rank>>
Max(a, b) == IF a > b THEN a ELSE b
Circulating == Max(1, Ceiling - Withheld)
Share == Max(1, Circulating \div 2)
Running == {l \in Leaves : leaf[l].drive # "none"}
Sweeps == {l \in Leaves : leaf[l].drive = "sweep"}
Limit(origin) == IF origin = "timer" /\ Share > 1 THEN Share - 1 ELSE Share
CanAdmit(origin) ==
    /\ free > 0 /\ Cardinality(Running) < Limit(origin)
    /\ ~(origin = "timer" /\ Share = 1 /\ hold > 0)
Eligible == {l \in Leaves :
    /\ leaf[l].pin < head /\ leaf[l].drive = "none" /\ leaf[l].attempts < 2
    /\ (leaf[l].touch = "queued" \/
        (leaf[l].touch = "refused" /\ leaf[l].credit /\ leaf[l].retries < 2))}
RankForPass == [l \in Leaves |-> Cardinality({q \in Leaves :
    leaf[q].pin < leaf[l].pin \/ (leaf[q].pin = leaf[l].pin /\ q < l)})]
Nearest == {l \in Eligible : \A q \in Eligible : rank[l] <= rank[q]}
PendingRankMembers == {q \in Leaves : leaf[q].touch \in {"queued", "refused"}}
RanksAfterSweep(l) ==
    LET pending == PendingRankMembers \ {l}
    IN [q \in Leaves |-> IF q \in pending
        THEN Cardinality({p \in pending : rank[p] < rank[q]}) ELSE 0]

TypeOK ==
    /\ leaf \in [Leaves -> [
        pin : 0..(Cardinality(Leaves) + 1),
        drive : {"none", "sweep", "timer"},
        left : 0..DriveBudget,
        timer : {"new", "dispatch", "rearm", "done"},
        turn : {"idle", "timer"},
        refreshed : BOOLEAN,
        round : 0..2,
        attempts : 0..2,
        touch : {"queued", "flying", "refused", "done"},
        credit : BOOLEAN,
        retries : 0..2]]
    /\ free \in 0..Circulating /\ cold \in 0..Circulating
    /\ hold \in 0..RefusedSweepHold
    /\ head \in Cardinality(Leaves)..(Cardinality(Leaves) + 1)
    /\ appended \in BOOLEAN /\ deleted \in BOOLEAN /\ timely \in BOOLEAN
    /\ admitted \subseteq {1}
    /\ leadDone \in BOOLEAN
    /\ rank \in [Leaves -> 0..(Cardinality(Leaves) - 1)]
PermitConservation == free + cold + Cardinality(Running) = Circulating
ShareRespected == Cardinality(Running) <= Share
TimerReservedSlot == [][\A l \in Leaves :
    leaf[l].drive = "none" /\ leaf'[l].drive = "timer"
        => Cardinality(Running) < Limit("timer") /\ ~(Share = 1 /\ hold > 0)]_vars
NoDeletedTreeReactivation == [][\A l \in Leaves :
    leaf[l].drive = "none" /\ leaf'[l].drive # "none" => ~deleted]_vars
NoAskWithoutWalAhead == [][\A l \in Leaves :
    leaf[l].timer = "new" /\ leaf'[l].timer = "dispatch" => leaf[l].pin < head]_vars
RefusalNotCharged == [][\A l \in Leaves :
    leaf[l].drive = "none" /\ leaf'[l].drive = "none"
        => leaf'[l].attempts <= leaf[l].attempts]_vars
NearestFirst == [][\A l \in Leaves :
    leaf[l].drive = "none" /\ leaf'[l].drive = "sweep" => l \in Nearest]_vars
NoAwaitedDrive == \A l \in Leaves : leaf[l].drive # "none" => leaf[l].turn = "idle"
RearmRefreshes == \A l \in Leaves :
    leaf[l].round > 0 /\ leaf[l].timer \in {"new", "done"} => leaf[l].refreshed
RetryAfterSibling == [][\A l \in Leaves :
    leaf[l].touch = "refused" /\ leaf'[l].touch = "flying"
        => leaf[l].credit /\ leaf[l].retries < 2]_vars
FirstTouchAlone == [][~leadDone /\ Sweeps # {} => Sweeps' \subseteq Sweeps]_vars
BudgetReleases == [][\A l \in Leaves :
    leaf[l].drive # "none" /\ leaf[l].left = 1 /\ leaf'[l].left = 0
        => leaf'[l].drive = "none"]_vars
PinMonotone == [][\A l \in Leaves : leaf'[l].pin >= leaf[l].pin]_vars
FloorHolderEventuallyDriven ==
    (<>[](cold = 0 /\ ~deleted)) => <>(1 \in admitted \/ leaf[1].pin = head)
FloorAdvances ==
    (timely /\ <>[](cold = 0 /\ ~deleted)) => <>[](\A l \in Leaves : leaf[l].pin = head)
TurnReleased == \A l \in Leaves : <>[](leaf[l].turn = "idle")

Init ==
    /\ leaf = [l \in Leaves |-> [
        pin |-> l - 1, drive |-> "none", left |-> 0,
        timer |-> "new", turn |-> "idle",
        refreshed |-> FALSE, round |-> 0, attempts |-> 0,
        touch |-> "queued", credit |-> FALSE, retries |-> 0]]
    /\ cold \in 0..Circulating /\ free = Circulating - cold
    /\ hold = 0 /\ head = Cardinality(Leaves) /\ appended = FALSE
    /\ deleted = FALSE /\ timely \in BOOLEAN /\ admitted = {}
    /\ leadDone = FALSE
    /\ rank = [l \in Leaves |-> l - 1]

DriveStart(l, origin) ==
    /\ leaf' = [leaf EXCEPT
        ![l].drive = origin, ![l].left = DriveBudget,
        ![l].attempts = IF origin = "sweep" THEN @ + 1 ELSE @,
        ![l].touch = IF origin = "sweep" THEN "flying" ELSE @,
        ![l].credit = IF origin = "sweep" THEN FALSE ELSE @,
        ![l].retries = IF origin = "sweep" THEN 0 ELSE @,
        ![l].turn = "idle",
        ![l].timer = IF origin = "timer" THEN "rearm" ELSE @]
    /\ free' = free - 1
    /\ hold' = IF origin = "sweep" THEN 0 ELSE hold
    /\ admitted' = IF origin = "sweep" /\ l = 1 THEN {1} ELSE admitted
    /\ rank' = IF origin = "sweep" THEN RanksAfterSweep(l) ELSE rank
    /\ UNCHANGED <<cold, head, appended, deleted, timely, leadDone>>

DriveRefused(l, origin) ==
    /\ leaf' = [leaf EXCEPT
        ![l].touch = IF origin = "sweep" THEN "refused" ELSE @,
        ![l].credit = IF origin = "sweep" THEN FALSE ELSE @,
        ![l].retries = IF origin = "sweep" THEN @ + 1 ELSE @,
        ![l].turn = "idle",
        ![l].timer = IF origin = "timer" THEN "rearm" ELSE @]
    /\ hold' = IF origin = "sweep" THEN RefusedSweepHold ELSE hold
    /\ leadDone' = IF origin = "sweep" THEN TRUE ELSE leadDone
    /\ UNCHANGED <<free, cold, head, appended, deleted, timely, admitted, rank>>

GcPass ==
    /\ ~deleted /\ Eligible # {}
    /\ leadDone \/ Sweeps = {}
    /\ Cardinality(Sweeps) < Share
    /\ \E l \in Nearest :
        IF CanAdmit("sweep") THEN DriveStart(l, "sweep") ELSE DriveRefused(l, "sweep")

PassRearm ==
    /\ ~deleted /\ Eligible = {} /\ Sweeps = {}
    /\ \E l \in Leaves : leaf[l].pin < head
    /\ leaf' = [l \in Leaves |-> [leaf[l] EXCEPT
        !.touch = "queued", !.credit = FALSE, !.retries = 0,
        !.attempts = IF @ = 2 THEN 0 ELSE @]]
    /\ leadDone' = FALSE
    /\ rank' = RankForPass
    /\ UNCHANGED <<free, cold, hold, head, appended, deleted, timely, admitted>>

TimerTick(l) ==
    /\ leaf[l].timer = "new" /\ leaf[l].turn = "idle" /\ ~deleted
    /\ leaf' = [leaf EXCEPT
           ![l].refreshed = FALSE,
           ![l].timer = IF leaf[l].pin < head /\ leaf[l].drive = "none" THEN "dispatch" ELSE "rearm",
           ![l].turn = IF leaf[l].pin < head /\ leaf[l].drive = "none" THEN "timer" ELSE "idle"]
    /\ UNCHANGED <<free, cold, hold, head, appended, deleted, timely, admitted, leadDone, rank>>

TimerDispatchDrive(l) ==
    /\ leaf[l].timer = "dispatch"
    /\ IF ~deleted /\ leaf[l].drive = "none" /\ CanAdmit("timer")
       THEN DriveStart(l, "timer")
       ELSE DriveRefused(l, "timer")

DriveOpportunity(l) ==
    /\ leaf[l].drive # "none"
    /\ LET advanced == [leaf EXCEPT
        ![l].left = @ - 1,
        ![l].drive = IF leaf[l].left = 1 THEN "none" ELSE @,
        ![l].touch = IF leaf[l].left = 1 /\ leaf[l].drive = "sweep" THEN "done" ELSE @,
        ![l].pin = IF timely /\ @ < head THEN @ + 1 ELSE @]
       IN leaf' = IF leaf[l].left = 1 /\ leaf[l].drive = "sweep"
           THEN [q \in Leaves |->
               IF leaf[q].touch = "refused"
               THEN [advanced[q] EXCEPT !.credit = TRUE] ELSE advanced[q]]
           ELSE advanced
    /\ free' = IF leaf[l].left = 1 THEN free + 1 ELSE free
    /\ leadDone' = (leadDone \/ (leaf[l].left = 1 /\ leaf[l].drive = "sweep"))
    /\ UNCHANGED <<cold, hold, head, appended, deleted, timely, admitted, rank>>

ReArm(l) ==
    /\ leaf[l].timer = "rearm"
    /\ leaf' = [leaf EXCEPT
            ![l].refreshed = TRUE, ![l].round = @ + 1,
            ![l].timer = IF leaf[l].round = 0 THEN "new" ELSE "done"]
    /\ UNCHANGED <<free, cold, hold, head, appended, deleted, timely, admitted, leadDone, rank>>

RefusalWindowTick ==
    /\ hold > 0 /\ hold' = hold - 1
    /\ UNCHANGED <<leaf, free, cold, head, appended, deleted, timely, admitted, leadDone, rank>>
ColdReturn ==
    /\ cold > 0 /\ cold' = cold - 1 /\ free' = free + 1
    /\ UNCHANGED <<leaf, hold, head, appended, deleted, timely, admitted, leadDone, rank>>
WalAppend ==
    /\ ~appended /\ ~deleted /\ head' = head + 1 /\ appended' = TRUE
    /\ UNCHANGED <<leaf, free, cold, hold, deleted, timely, admitted, leadDone, rank>>
DeleteTree ==
    /\ ~deleted /\ deleted' = TRUE
    /\ UNCHANGED <<leaf, free, cold, hold, head, appended, timely, admitted, leadDone, rank>>
Stutter == UNCHANGED vars
Next ==
    \/ GcPass
    \/ PassRearm
    \/ \E l \in Leaves : TimerTick(l)
    \/ \E l \in Leaves : TimerDispatchDrive(l)
    \/ \E l \in Leaves : DriveOpportunity(l)
    \/ \E l \in Leaves : ReArm(l)
    \/ RefusalWindowTick
    \/ ColdReturn
    \/ WalAppend
    \/ DeleteTree
    \/ Stutter

Fairness ==
    /\ WF_vars(GcPass)
    /\ WF_vars(PassRearm)
    /\ WF_vars(RefusalWindowTick)
    /\ \A l \in Leaves :
        /\ WF_vars(TimerTick(l))
        /\ WF_vars(TimerDispatchDrive(l))
        /\ WF_vars(DriveOpportunity(l))
        /\ WF_vars(ReArm(l))

Spec == Init /\ [][Next]_vars /\ Fairness
====
