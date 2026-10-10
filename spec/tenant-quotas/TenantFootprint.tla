------------------------ MODULE TenantFootprint ------------------------
EXTENDS Naturals, FiniteSets, TLC

\* Two independent clusters, one finite local request and one inbound apply
\* per cluster. Unit writes; no deletion, quota change or residency change.
\* StorageMax abstracts long.MaxValue; totals saturate, never wrap.
Regions == {"a", "b"}
StorageMax == 3
Limit == 1
Refresh == 2
VARIABLES scope, band, age, phase, live, inbound, sample, published, slots, decision
vars == <<scope, band, age, phase, live, inbound, sample, published, slots, decision>>
Max(x, y) == IF x > y THEN x ELSE y
Sat(x) == IF x > StorageMax THEN StorageMax ELSE x
Fold(s) == Sat(s["a"] + s["b"])
Usage(r) == IF scope = "GlobalConverged" THEN Fold(slots[r]) ELSE slots[r][r]
ExpectedUsage(r) == IF scope = "GlobalConverged" THEN Sat(slots[r]["a"] + slots[r]["b"]) ELSE slots[r][r]
TrueUsage(r) == IF scope = "GlobalConverged" THEN Sat(live["a"] + live["b"]) ELSE live[r]
Allowed(r) == Usage(r) <= Limit

Init ==
    /\ scope \in {"GlobalConverged", "PerCluster"}
    /\ band \in {0, 2}
    /\ age = [r \in Regions |-> 0]
    /\ phase = [r \in Regions |-> "new"]
    /\ live = [r \in Regions |-> 0]
    /\ inbound = [r \in Regions |-> FALSE]
    /\ sample = [r \in Regions |-> 0]
    /\ published = [r \in Regions |-> 0]
    /\ slots = [r \in Regions |-> [o \in Regions |-> 0]]
    /\ decision = [r \in Regions |-> [value |-> 0, expected |-> 0, allow |-> TRUE]]

Admit(r) ==
    /\ phase[r] = "new"
    /\ phase' = [phase EXCEPT ![r] = IF Allowed(r) THEN "admitted" ELSE "refused"]
    /\ decision' = [decision EXCEPT ![r] = [value |-> Usage(r), expected |-> ExpectedUsage(r), allow |-> Allowed(r)]]
    /\ UNCHANGED <<scope, band, age, live, inbound, sample, published, slots>>

Commit(r) ==
    /\ phase[r] = "admitted"
    /\ phase' = [phase EXCEPT ![r] = "committed"]
    /\ live' = [live EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<scope, band, age, inbound, sample, published, slots, decision>>

ApplyReplication(r) ==
    /\ ~inbound[r]
    /\ inbound' = [inbound EXCEPT ![r] = TRUE]
    /\ live' = [live EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<scope, band, age, phase, sample, published, slots, decision>>

Sample(r) ==
    /\ sample' = [sample EXCEPT ![r] = live[r]]
    /\ UNCHANGED <<scope, band, age, phase, live, inbound, published, slots, decision>>

Publish(r) ==
    /\ sample[r] # published[r]
    /\ published[r] = 0 \/ sample[r] - published[r] >= band \/ age[r] >= Refresh
    /\ published' = [published EXCEPT ![r] = sample[r]]
    /\ slots' = [slots EXCEPT ![r][r] = Max(@, sample[r])]
    /\ age' = [age EXCEPT ![r] = 0]
    /\ UNCHANGED <<scope, band, phase, live, inbound, sample, decision>>

\* Any old stamped sample can be redelivered. Here increasing unit usage is
\* also the stamp order; deletion and equal-HLC writer tie-breaks are excluded.
DeliverUsage(r, o) ==
    /\ r # o
    /\ \E old \in 0..published[o] :
         slots' = [slots EXCEPT ![r][o] = Max(@, old)]
    /\ UNCHANGED <<scope, band, age, phase, live, inbound, sample, published, decision>>

Probe(r) ==
    /\ decision' = [decision EXCEPT ![r] = [value |-> Usage(r), expected |-> ExpectedUsage(r), allow |-> Allowed(r)]]
    /\ UNCHANGED <<scope, band, age, phase, live, inbound, sample, published, slots>>

\* Supplied cadence-clock time advances while metering remains available.
ClockTick ==
    /\ age' = [r \in Regions |-> IF age[r] < Refresh THEN age[r] + 1 ELSE age[r]]
    /\ UNCHANGED <<scope, band, phase, live, inbound, sample, published, slots, decision>>

Next ==
    \/ \E r \in Regions : Admit(r)
    \/ \E r \in Regions : Commit(r)
    \/ \E r \in Regions : ApplyReplication(r)
    \/ \E r \in Regions : Sample(r)
    \/ \E r \in Regions : Publish(r)
    \/ \E r, o \in Regions : DeliverUsage(r, o)
    \/ \E r \in Regions : Probe(r)
    \/ ClockTick

\* Delivery is strongly fair: an old sample can be selected arbitrarily many
\* times, but every continually available latest slot must eventually land.
DeliverLatest(r, o) ==
    /\ DeliverUsage(r, o)
    /\ slots'[r][o] = published[o]

Spec == Init /\ [][Next]_vars
    /\ (\A r \in Regions : WF_vars(Sample(r)) /\ WF_vars(Publish(r)) /\ WF_vars(Probe(r)))
    /\ (\A r, o \in Regions : SF_vars(DeliverLatest(r, o)))
    /\ WF_vars(ClockTick)

TypeOK ==
    /\ scope \in {"GlobalConverged", "PerCluster"}
    /\ band \in {0, 2}
    /\ age \in [Regions -> 0..Refresh]
    /\ phase \in [Regions -> {"new", "admitted", "refused", "committed"}]
    /\ live \in [Regions -> 0..2]
    /\ inbound \in [Regions -> BOOLEAN]
    /\ sample \in [Regions -> 0..2]
    /\ published \in [Regions -> 0..2]
    /\ slots \in [Regions -> [Regions -> 0..2]]
    /\ decision \in [Regions -> [value : 0..StorageMax, expected : 0..StorageMax, allow : BOOLEAN]]

AdmissionTruth == \A r \in Regions : decision[r].allow = (decision[r].value <= Limit)
ScopeCorrect == \A r \in Regions : decision[r].value = decision[r].expected
AccountingCorrect == \A r \in Regions :
    live[r] = (IF phase[r] = "committed" THEN 1 ELSE 0) + (IF inbound[r] THEN 1 ELSE 0)
SampleSound == \A r \in Regions : sample[r] <= live[r] /\ published[r] <= sample[r]
SlotMonotonic == [][\A r, o \in Regions : slots'[r][o] >= slots[r][o]]_vars
Quiescent == \A r \in Regions : phase[r] \in {"committed", "refused"} /\ inbound[r]
UsageConverges == Quiescent ~> (\A r, o \in Regions : slots[r][o] = live[o])
EventualRefusal == \A r \in Regions :
    (Quiescent /\ TrueUsage(r) > Limit) ~> ~decision[r].allow
=============================================================================
