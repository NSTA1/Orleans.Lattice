--------------------- MODULE TenantQuotaEvaluation ---------------------
EXTENDS Naturals, Sequences, TLC

\* Finite arithmetic analogue of long.MaxValue saturation; the four
\* dimensions are evaluated in production order, independently nullable.
Dimensions == <<"bytes", "keys", "memory", "trees">>
Names == {"bytes", "keys", "memory", "trees"}
StorageMax == 3
Unbounded == 3
VARIABLES quotas, usage, burst, result
vars == <<quotas, usage, burst, result>>
Saturate(n) == IF n > StorageMax THEN StorageMax ELSE n
Ceiling(d) == IF quotas[d] = Unbounded THEN StorageMax
    ELSE Saturate(quotas[d] + ((quotas[d] * burst) \div 100))
Breach(d) == quotas[d] # Unbounded /\ usage[d] > Ceiling(d)
FirstBreach == IF Breach("bytes") THEN "bytes" ELSE
    IF Breach("keys") THEN "keys" ELSE
    IF Breach("memory") THEN "memory" ELSE
    IF Breach("trees") THEN "trees" ELSE "admit"

\* Oracle is deliberately independent of FirstBreach and Ceiling.
ExpectedBound(d) == IF quotas[d] = Unbounded THEN StorageMax
    ELSE Saturate(quotas[d] + ((quotas[d] * burst) \div 100))
ExpectedBreaches == {d \in Names : quotas[d] # Unbounded /\ usage[d] > ExpectedBound(d)}
Expected == IF ExpectedBreaches = {} THEN "admit" ELSE
    Dimensions[CHOOSE i \in 1..4 :
        Dimensions[i] \in ExpectedBreaches /\
        \A j \in 1..4 : Dimensions[j] \in ExpectedBreaches => i <= j]

Init ==
    /\ quotas \in [Names -> 0..3]
    /\ usage \in [Names -> 0..2]
    /\ burst \in {0, 50, 100}
    /\ result = "unchecked"

Evaluate ==
    /\ result' = FirstBreach
    /\ UNCHANGED <<quotas, usage, burst>>

Next ==
    \/ Evaluate

Spec == Init /\ [][Next]_vars /\ WF_vars(Evaluate)
TypeOK ==
    /\ quotas \in [Names -> 0..3]
    /\ usage \in [Names -> 0..2]
    /\ burst \in {0, 50, 100}
    /\ result \in Names \cup {"admit", "unchecked"}
TruthfulEvaluation == result = "unchecked" \/ result = Expected
EventuallyEvaluated == <> (result # "unchecked")
=============================================================================
