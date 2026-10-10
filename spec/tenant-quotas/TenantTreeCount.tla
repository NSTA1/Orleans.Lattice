------------------------ MODULE TenantTreeCount ------------------------
EXTENDS Naturals, FiniteSets, TLC

\* Three concurrent creates; check/count and register are separate steps.
\* Ceiling two with 50% burst means three; ceiling one has no floored burst.
Requests == 1..3
VARIABLES limit, burst, phase, checked, owned, observed, allow
vars == <<limit, burst, phase, checked, owned, observed, allow>>
Effective == limit + ((limit * burst) \div 100)
Expected == limit + ((limit * burst) \div 100)

Init ==
    /\ limit \in {1, 2}
    /\ burst \in {0, 50}
    /\ phase = [q \in Requests |-> "new"]
    /\ checked = [q \in Requests |-> 0]
    /\ owned = {}
    /\ observed = 0
    /\ allow = TRUE

CheckCreate(q) ==
    /\ phase[q] = "new"
    /\ checked' = [checked EXCEPT ![q] = Cardinality(owned)]
    /\ phase' = [phase EXCEPT ![q] =
         IF Cardinality(owned) + 1 <= Effective THEN "admitted" ELSE "refused"]
    /\ UNCHANGED <<limit, burst, owned, observed, allow>>

Register(q) ==
    /\ phase[q] = "admitted"
    /\ owned' = owned \cup {q}
    /\ phase' = [phase EXCEPT ![q] = "registered"]
    /\ UNCHANGED <<limit, burst, checked, observed, allow>>

\* At least one probe remains available after every create has finished.
Probe ==
    /\ observed' = Cardinality(owned)
    /\ allow' = (Cardinality(owned) + 1 <= Effective)
    /\ UNCHANGED <<limit, burst, owned, phase, checked>>

Next ==
    \/ \E q \in Requests : CheckCreate(q)
    \/ \E q \in Requests : Register(q)
    \/ Probe

Spec == Init /\ [][Next]_vars
    /\ (\A q \in Requests : WF_vars(CheckCreate(q)) /\ WF_vars(Register(q)))
    /\ WF_vars(Probe)

TypeOK ==
    /\ limit \in {1, 2} /\ burst \in {0, 50}
    /\ phase \in [Requests -> {"new", "admitted", "refused", "registered"}]
    /\ checked \in [Requests -> 0..3]
    /\ owned \subseteq Requests
    /\ observed \in 0..3 /\ allow \in BOOLEAN
RegistrationAccounted == owned = {q \in Requests : phase[q] = "registered"}
TruthfulCreate == \A q \in Requests :
    (phase[q] \in {"admitted", "registered"} => checked[q] + 1 <= Expected)
    /\ (phase[q] = "refused" => checked[q] + 1 > Expected)
AllCreatesFinish == <> (\A q \in Requests : phase[q] \in {"registered", "refused"})
ProbeTruth == allow = (observed + 1 <= Expected)
EventualCreateRefusal == ((\A q \in Requests : phase[q] \in {"registered", "refused"})
    /\ Cardinality(owned) >= Effective) ~> ~allow
=============================================================================
