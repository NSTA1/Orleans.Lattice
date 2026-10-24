---- MODULE WalAdmission ----
EXTENDS Naturals, Sequences, FiniteSets

\* Gate release and semaphore admission are different boundaries. A released
\* waiter still needs a semaphore slot; only Flush frees that slot.
CONSTANTS Partitions, Callers, Cap, ReleaseBatch, WaitBudget, CallBudget,
          RecoveryWindow, AcuteOnly, FixedHomes
VARIABLES inflight, parked, state, acuteCause, faultPhase, recovery,
          outcome, home, waitLeft, callLeft, phase, cursor, refusalState
vars == <<inflight, parked, state, acuteCause, faultPhase, recovery,
          outcome, home, waitLeft, callLeft, phase, cursor, refusalState>>

Min(a, b) == IF a < b THEN a ELSE b
Members(q) == {q[i] : i \in 1..Len(q)}
Remove(q, c) == SelectSeq(q, LAMBDA x : x # c)
Eligible(p) == state[p] = "Healthy" \/ (AcuteOnly /\ state[p] = "Throttled")
Classify(p) ==
    IF acuteCause[p] \/ (~AcuteOnly /\ inflight[p] >= Cap) THEN "Saturated"
    ELSE IF inflight[p] > 0 THEN "Throttled"
    ELSE "Healthy"
Terminal == {"admitted", "refused", "cancelled", "done"}
Resolved == Terminal \cup {"released"}

TypeOK ==
    /\ inflight \in [Partitions -> 0..(Cardinality(Callers) + Cap)]
    /\ parked \in [Partitions -> Seq(Callers)]
    /\ state \in [Partitions -> {"Healthy", "Throttled", "Saturated"}]
    /\ acuteCause \in [Partitions -> BOOLEAN]
    /\ faultPhase \in [Partitions -> {"new", "on", "off"}]
    /\ recovery \in 0..RecoveryWindow
    /\ outcome \in [Callers -> {"new", "pending", "parked", "released"} \cup Terminal]
    /\ home \in [Callers -> Partitions]
    /\ waitLeft \in [Callers -> 0..WaitBudget]
    /\ callLeft \in [Callers -> 0..CallBudget]
    /\ phase \in {"sample", "release", "expire"}
    /\ cursor \in Callers
    /\ refusalState \in [Callers -> {"none", "Healthy", "Throttled", "Saturated"}]

CapRespected == \A p \in Partitions : inflight[p] <= Cap
NoLostWaiter ==
    /\ \A c \in Callers :
        outcome[c] = "parked" <=> \E p \in Partitions : c \in Members(parked[p])
    /\ \A p \in Partitions :
        /\ Len(parked[p]) = Cardinality(Members(parked[p]))
        /\ \A c \in Members(parked[p]) : home[c] = p
Releasing == phase = "release" /\ phase' = "expire"
Released(p) == Members(parked[p]) \ Members(parked'[p])
OldestFirst == [][Releasing => \A p \in Partitions :
    parked'[p] = SubSeq(parked[p], Cardinality(Released(p)) + 1, Len(parked[p]))]_vars
PerPartitionIsolation == [][Releasing => \A p \in Partitions :
    ~Eligible(p) => parked'[p] = parked[p]]_vars
RefusalOnlyWhenSaturated == \A c \in Callers :
    refusalState[c] # "none" => refusalState[c] = "Saturated"
UpgradeSound == [][\A p \in Partitions :
    (phase = "sample" /\ phase' = "release" /\ state'[p] = "Throttled" /\ inflight[p] = 0)
        => recovery' > 0 /\ (\A q \in Partitions : Classify(q) = "Healthy")]_vars
PacedRecoveryRelease == [][Releasing => \A p \in Partitions :
    Cardinality(Released(p)) <= ReleaseBatch]_vars
BoundedResidueDrain == [][Releasing => \A p \in Partitions :
    Cardinality(Released(p)) =
        (IF Eligible(p) THEN Min(Len(parked[p]), ReleaseBatch) ELSE 0)]_vars
ProgressWhenCapacityFree == [][Releasing => \A p \in Partitions :
    Eligible(p) /\ Len(parked[p]) > 0 => Cardinality(Released(p)) > 0]_vars
EveryWaiterResolves == \A c \in Callers : outcome[c] = "parked" ~> outcome[c] \in Resolved
NoAbsorbingSaturation == \A p \in Partitions :
    (<>[](~acuteCause[p] /\ inflight[p] = 0)) => <>[](state[p] # "Saturated")

Init ==
    /\ inflight = [p \in Partitions |-> 0]
    /\ parked = [p \in Partitions |-> <<>>]
    /\ state = [p \in Partitions |-> "Healthy"]
    /\ acuteCause = [p \in Partitions |-> FALSE]
    /\ faultPhase = [p \in Partitions |-> "new"]
    /\ recovery = 0
    /\ outcome = [c \in Callers |-> "new"]
    /\ IF FixedHomes
       THEN home = [c \in Callers |-> 1 + ((c - 1) % Cardinality(Partitions))]
       ELSE home \in [Callers -> Partitions]
    /\ waitLeft = [c \in Callers |-> WaitBudget]
    /\ callLeft = [c \in Callers |-> CallBudget]
    /\ phase = "sample" /\ cursor = 1
    /\ refusalState = [c \in Callers |-> "none"]

Arrive(c, p) ==
    /\ outcome[c] = "new" /\ home[c] = p
    /\ outcome' = [outcome EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<inflight, parked, state, acuteCause, faultPhase, recovery,
        home, waitLeft, callLeft, phase, cursor, refusalState>>

Park(c, p) ==
    /\ outcome[c] = "pending" /\ home[c] = p /\ state[p] = "Saturated"
    /\ parked' = [parked EXCEPT ![p] = Append(@, c)]
    /\ outcome' = [outcome EXCEPT ![c] = "parked"]
    /\ UNCHANGED <<inflight, state, acuteCause, faultPhase, recovery,
        home, waitLeft, callLeft, phase, cursor, refusalState>>

Admit(c, p) ==
    /\ outcome[c] \in {"pending", "released"} /\ home[c] = p
    /\ (outcome[c] = "released" \/ state[p] # "Saturated") /\ inflight[p] < Cap
    /\ inflight' = [inflight EXCEPT ![p] = @ + 1]
    /\ outcome' = [outcome EXCEPT ![c] = "admitted"]
    /\ waitLeft' = [waitLeft EXCEPT ![c] = 0]
    /\ callLeft' = [callLeft EXCEPT ![c] = 0]
    /\ UNCHANGED <<parked, state, acuteCause, faultPhase, recovery,
        home, phase, cursor, refusalState>>

Flush(c, p) ==
    /\ outcome[c] = "admitted" /\ home[c] = p
    /\ inflight' = [inflight EXCEPT ![p] = @ - 1]
    /\ outcome' = [outcome EXCEPT ![c] = "done"]
    /\ UNCHANGED <<parked, state, acuteCause, faultPhase, recovery,
        home, waitLeft, callLeft, phase, cursor, refusalState>>

\* A sample and all its callbacks form one finite sampler iteration.
\* Decrement the recovery age BEFORE classifying, so state and age agree.
Sample ==
    /\ phase = "sample"
    /\ recovery' =
        IF \E p \in Partitions : Classify(p) = "Saturated" THEN RecoveryWindow
        ELSE IF recovery > 0 THEN recovery - 1 ELSE 0
    /\ state' = [p \in Partitions |->
        IF Classify(p) = "Healthy" /\ recovery' > 0
            /\ (\A q \in Partitions : Classify(q) = "Healthy")
        THEN "Throttled" ELSE Classify(p)]
    /\ waitLeft' = [c \in Callers |->
        IF outcome[c] = "parked" /\ waitLeft[c] > 0 THEN waitLeft[c] - 1 ELSE waitLeft[c]]
    /\ callLeft' = [c \in Callers |->
        IF outcome[c] # "new" /\ callLeft[c] > 0 THEN callLeft[c] - 1 ELSE callLeft[c]]
    /\ phase' = "release"
    /\ UNCHANGED <<inflight, parked, acuteCause, faultPhase, outcome, home,
        cursor, refusalState>>

ReleaseTick ==
    /\ phase = "release"
    /\ LET chosen == [p \in Partitions |->
                IF Eligible(p) THEN SubSeq(parked[p], 1, Min(Len(parked[p]), ReleaseBatch))
                ELSE <<>>]
           all == UNION {Members(chosen[p]) : p \in Partitions}
       IN /\ parked' = [p \in Partitions |->
                    SubSeq(parked[p], Len(chosen[p]) + 1, Len(parked[p]))]
          /\ outcome' = [c \in Callers |-> IF c \in all THEN "released" ELSE outcome[c]]
          /\ waitLeft' = [c \in Callers |-> IF c \in all THEN 0 ELSE waitLeft[c]]
          /\ callLeft' = [c \in Callers |-> IF c \in all THEN 0 ELSE callLeft[c]]
    /\ phase' = "expire" /\ cursor' = 1
    /\ UNCHANGED <<inflight, state, acuteCause, faultPhase, recovery,
        home, refusalState>>

\* Expiry re-reads the CURRENT verdict, not the state that caused the park.
BudgetExpire ==
    /\ phase = "expire"
    /\ LET c == cursor
           expired == outcome[c] = "parked" /\ (waitLeft[c] = 0 \/ callLeft[c] = 0)
           refuse == expired /\ state[home[c]] = "Saturated"
       IN /\ outcome' = [outcome EXCEPT ![c] =
                    IF expired THEN (IF refuse THEN "refused" ELSE "released") ELSE @]
          /\ parked' = [p \in Partitions |-> IF expired THEN Remove(parked[p], c) ELSE parked[p]]
          /\ refusalState' = [refusalState EXCEPT ![c] = IF refuse THEN state[home[c]] ELSE @]
          /\ waitLeft' = [waitLeft EXCEPT ![c] = IF expired THEN 0 ELSE @]
          /\ callLeft' = [callLeft EXCEPT ![c] = IF expired THEN 0 ELSE @]
    /\ cursor' = IF cursor = Cardinality(Callers) THEN 1 ELSE cursor + 1
    /\ phase' = IF cursor = Cardinality(Callers) THEN "sample" ELSE "expire"
    /\ UNCHANGED <<inflight, state, acuteCause, faultPhase, recovery, home>>

Cancel(c) ==
    /\ outcome[c] = "parked"
    /\ parked' = [p \in Partitions |-> Remove(parked[p], c)]
    /\ outcome' = [outcome EXCEPT ![c] = "cancelled"]
    /\ waitLeft' = [waitLeft EXCEPT ![c] = 0]
    /\ callLeft' = [callLeft EXCEPT ![c] = 0]
    /\ UNCHANGED <<inflight, state, acuteCause, faultPhase, recovery, home,
        phase, cursor, refusalState>>

AcuteOn(p) ==
    /\ faultPhase[p] = "new"
    /\ acuteCause' = [acuteCause EXCEPT ![p] = TRUE]
    /\ faultPhase' = [faultPhase EXCEPT ![p] = "on"]
    /\ UNCHANGED <<inflight, parked, state, recovery, outcome, home,
        waitLeft, callLeft, phase, cursor, refusalState>>

AcuteOff(p) ==
    /\ faultPhase[p] = "on"
    /\ acuteCause' = [acuteCause EXCEPT ![p] = FALSE]
    /\ faultPhase' = [faultPhase EXCEPT ![p] = "off"]
    /\ UNCHANGED <<inflight, parked, state, recovery, outcome, home,
        waitLeft, callLeft, phase, cursor, refusalState>>

Stutter == UNCHANGED vars
Next ==
    \/ \E c \in Callers, p \in Partitions : Arrive(c, p)
    \/ \E c \in Callers, p \in Partitions : Park(c, p)
    \/ \E c \in Callers, p \in Partitions : Admit(c, p)
    \/ \E c \in Callers, p \in Partitions : Flush(c, p)
    \/ Sample
    \/ ReleaseTick
    \/ BudgetExpire
    \/ \E c \in Callers : Cancel(c)
    \/ \E p \in Partitions : AcuteOn(p)
    \/ \E p \in Partitions : AcuteOff(p)
    \/ Stutter

SamplerStep == Sample \/ ReleaseTick \/ BudgetExpire
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(SamplerStep)
    /\ \A c \in Callers, p \in Partitions : WF_vars(Flush(c, p))
====
