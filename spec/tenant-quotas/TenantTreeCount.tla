------------------------ MODULE TenantTreeCount ------------------------
EXTENDS Naturals, FiniteSets, TLC

\* Two concurrent creates and one alias onto a new logical tree id. Check/count
\* and register are separate steps for both. A re-point of an already registered
\* logical id, purges and one quota change interleave with them.
\* Ceiling two with 50% burst means three; ceiling one has no floored burst.
Creates == {1, 2}
Aliases == {3}
Requests == Creates \cup Aliases
VARIABLES limit, burst, phase, checked, decided, owned, observed, probed, allow, repoint, quotaSet
vars == <<limit, burst, phase, checked, decided, owned, observed, probed, allow, repoint, quotaSet>>
Effective == limit + ((limit * burst) \div 100)
Expected(l) == l + ((l * burst) \div 100)

Init ==
    /\ limit \in {1, 2}
    /\ burst \in {0, 50}
    /\ phase = [q \in Requests |-> "new"]
    /\ checked = [q \in Requests |-> 0]
    /\ decided = [q \in Requests |-> limit]
    /\ owned = {}
    /\ observed = 0
    /\ probed = limit
    /\ allow = TRUE
    /\ repoint = "none"
    /\ quotaSet = FALSE

CheckCreate(q) ==
    /\ q \in Creates
    /\ phase[q] = "new"
    /\ checked' = [checked EXCEPT ![q] = Cardinality(owned)]
    /\ decided' = [decided EXCEPT ![q] = limit]
    /\ phase' = [phase EXCEPT ![q] =
         IF Cardinality(owned) + 1 <= Effective THEN "admitted" ELSE "refused"]
    /\ UNCHANGED <<limit, burst, owned, observed, probed, allow, repoint, quotaSet>>

\* An alias whose logical id has no registry row adds one, so it is admitted
\* exactly as a create: the same registry count, charged for the new id.
CheckAlias(q) ==
    /\ q \in Aliases
    /\ phase[q] = "new"
    /\ checked' = [checked EXCEPT ![q] = Cardinality(owned)]
    /\ decided' = [decided EXCEPT ![q] = limit]
    /\ phase' = [phase EXCEPT ![q] =
         IF Cardinality(owned) + 1 > Effective THEN "refused" ELSE "admitted"]
    /\ UNCHANGED <<limit, burst, owned, observed, probed, allow, repoint, quotaSet>>

Register(q) ==
    /\ phase[q] = "admitted"
    /\ owned' = owned \cup {q}
    /\ phase' = [phase EXCEPT ![q] = "registered"]
    /\ UNCHANGED <<limit, burst, checked, decided, observed, probed, allow, repoint, quotaSet>>

\* A purge removes the tree's registry row, so it leaves the count.
Purge(q) ==
    /\ phase[q] = "registered"
    /\ owned' = owned \ {q}
    /\ phase' = [phase EXCEPT ![q] = "purged"]
    /\ UNCHANGED <<limit, burst, checked, decided, observed, probed, allow, repoint, quotaSet>>

\* Re-pointing a logical id that already has a registry row adds no row, so it
\* is not charged and is never refused for the tree quota.
Repoint ==
    /\ repoint = "none"
    /\ owned # {}
    /\ repoint' = "done"
    /\ UNCHANGED <<limit, burst, phase, checked, decided, owned, observed, probed, allow, quotaSet>>

\* One operator quota change, at any point in the interleaving.
SetQuota ==
    /\ ~quotaSet
    /\ limit' \in {1, 2} \ {limit}
    /\ quotaSet' = TRUE
    /\ UNCHANGED <<burst, phase, checked, decided, owned, observed, probed, allow, repoint>>

\* At least one probe remains available after every create has finished.
Probe ==
    /\ observed' = Cardinality(owned)
    /\ allow' = (Cardinality(owned) + 1 <= Effective)
    /\ probed' = limit
    /\ UNCHANGED <<limit, burst, owned, phase, checked, decided, repoint, quotaSet>>

Next ==
    \/ \E q \in Requests : CheckCreate(q)
    \/ \E q \in Requests : CheckAlias(q)
    \/ \E q \in Requests : Register(q)
    \/ \E q \in Requests : Purge(q)
    \/ Repoint
    \/ SetQuota
    \/ Probe

Spec == Init /\ [][Next]_vars
    /\ (\A q \in Requests : WF_vars(CheckCreate(q)) /\ WF_vars(CheckAlias(q)) /\ WF_vars(Register(q)))
    /\ WF_vars(Probe)

TypeOK ==
    /\ limit \in {1, 2} /\ burst \in {0, 50}
    /\ phase \in [Requests -> {"new", "admitted", "refused", "registered", "purged"}]
    /\ checked \in [Requests -> 0..3]
    /\ decided \in [Requests -> {1, 2}]
    /\ owned \subseteq Requests
    /\ observed \in 0..3 /\ probed \in {1, 2} /\ allow \in BOOLEAN
    /\ repoint \in {"none", "done", "refused"} /\ quotaSet \in BOOLEAN
RegistrationAccounted == owned = {q \in Requests : phase[q] = "registered"}
Truthful(q) ==
    /\ (phase[q] \in {"admitted", "registered", "purged"} => checked[q] + 1 <= Expected(decided[q]))
    /\ (phase[q] = "refused" => checked[q] + 1 > Expected(decided[q]))
TruthfulCreate == \A q \in Creates : Truthful(q)
TruthfulAlias == \A q \in Aliases : Truthful(q)
RepointUncharged == repoint # "refused"
AllCreatesFinish == <> (\A q \in Requests : phase[q] \in {"registered", "refused", "purged"})
ProbeTruth == allow = (observed + 1 <= Expected(probed))
EventualCreateRefusal == (<>[](Cardinality(owned) >= Expected(limit))) => <>[](~allow)
=============================================================================
