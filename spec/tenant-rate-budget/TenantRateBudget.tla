------------------------- MODULE TenantRateBudget -------------------------
EXTENDS Naturals, FiniteSets, TLC

\* One tenant; independent silo coordinators, not a distributed lease authority.
\* Cadence expiry triggers refresh but NEVER expires the enforcing bucket.
\* A changed grant installs a fresh GCRA bucket, intentionally granting a burst.
CONSTANTS MaxTime, MaxCycles
Silos == {"a", "b"}
Rates == {1, 2}
Frequency == 2
Max(a, b) == IF a > b THEN a ELSE b
Even(rate, count) == Max(1, rate \div Max(1, count))
Share(rate, count, local, total) ==
    IF total = 0 THEN Even(rate, count)
    ELSE Max(1, IF local >= total THEN rate ELSE (rate * local) \div total)
Emission(share) == Max(1, Frequency \div share)
Tolerance(share) == Max(1, share \div 2) * Emission(share)
EmptyGrant == [rate |-> 2, count |-> 1, local |-> 0, total |-> 0, share |-> 2]

VARIABLES live, rate, changed, joined, left, restarted, now, phase, cycles,
          age, grant, configured, emission, tolerance, tat, demand, cancelled
vars == <<live, rate, changed, joined, left, restarted, now, phase, cycles,
          age, grant, configured, emission, tolerance, tat, demand, cancelled>>

Init ==
    /\ live = {"a"}
    /\ rate = 2
    /\ changed = FALSE
    /\ joined = FALSE
    /\ left = FALSE
    /\ restarted = FALSE
    /\ now = 0
    /\ phase = [s \in Silos |-> "idle"]
    /\ cycles = [s \in Silos |-> 0]
    /\ age = [s \in Silos |-> 1]
    /\ grant = [s \in Silos |-> EmptyGrant]
    /\ configured = [s \in Silos |-> s = "a"]
    /\ emission = [s \in Silos |-> 1]
    /\ tolerance = [s \in Silos |-> 1]
    /\ tat = [s \in Silos |-> 0]
    /\ demand = [s \in Silos |-> 0]
    /\ cancelled = [s \in Silos |-> FALSE]

Join ==
    /\ ~joined
    /\ "b" \notin live
    /\ live' = live \cup {"b"}
    /\ joined' = TRUE
    /\ UNCHANGED <<rate, changed, left, restarted, now, phase, cycles, age,
                   grant, configured, emission, tolerance, tat, demand, cancelled>>

Leave(s) ==
    /\ s \in live
    /\ ~left
    /\ live' = live \ {s}
    /\ left' = TRUE
    /\ phase' = [phase EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<rate, changed, joined, restarted, now, cycles, age,
                   grant, configured, emission, tolerance, tat, demand, cancelled>>

ChangeBudget(newRate) ==
    /\ ~changed
    /\ newRate \in Rates \ {rate}
    /\ rate' = newRate
    /\ changed' = TRUE
    /\ UNCHANGED <<live, joined, left, restarted, now, phase, cycles, age,
                   grant, configured, emission, tolerance, tat, demand, cancelled>>

BeginLease(s, total) ==
    /\ s \in live
    /\ phase[s] = "idle"
    /\ age[s] = 1
    /\ cycles[s] < MaxCycles
    /\ total \in {0, 2}
    /\ grant' = [grant EXCEPT ![s] =
         [rate |-> rate, count |-> Cardinality(live), local |-> demand[s],
          total |-> total, share |-> Share(rate, Cardinality(live), demand[s], total)]]
    /\ demand' = [demand EXCEPT ![s] = 0]
    /\ cancelled' = [cancelled EXCEPT ![s] = FALSE]
    /\ phase' = [phase EXCEPT ![s] = "pending"]
    /\ cycles' = [cycles EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<live, rate, changed, joined, left, restarted, now, age,
                   configured, emission, tolerance, tat>>

Cancel(s) ==
    /\ phase[s] = "pending"
    /\ ~cancelled[s]
    /\ cancelled' = [cancelled EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<live, rate, changed, joined, left, restarted, now, phase,
                   cycles, age, grant, configured, emission, tolerance, tat, demand>>

Deliver(s) ==
    /\ s \in live
    /\ phase[s] = "pending"
    /\ LET e == Emission(grant[s].share)
           t == Tolerance(grant[s].share)
           install == ~cancelled[s]
           same == configured[s] /\ emission[s] = e /\ tolerance[s] = t
       IN /\ configured' = [configured EXCEPT ![s] = IF install THEN TRUE ELSE @]
          /\ emission' = [emission EXCEPT ![s] = IF install THEN e ELSE @]
          /\ tolerance' = [tolerance EXCEPT ![s] = IF install THEN t ELSE @]
          /\ tat' = [tat EXCEPT ![s] = IF install /\ ~same THEN 0 ELSE @]
          /\ demand' = [demand EXCEPT ![s] = IF install /\ ~same THEN 0 ELSE @]
    /\ phase' = [phase EXCEPT ![s] = "idle"]
    /\ age' = [age EXCEPT ![s] = 0]
    /\ UNCHANGED <<live, rate, changed, joined, left, restarted, now, cycles, grant, cancelled>>

Restart(s) ==
    /\ s \in live
    /\ ~restarted
    /\ restarted' = TRUE
    /\ phase' = [phase EXCEPT ![s] = "idle"]
    /\ cancelled' = [cancelled EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<live, rate, changed, joined, left, now, cycles, age, grant,
                   configured, emission, tolerance, tat, demand>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ age' = [s \in Silos |-> 1]
    /\ UNCHANGED <<live, rate, changed, joined, left, restarted, phase, cycles,
                   grant, configured, emission, tolerance, tat, demand, cancelled>>

Acquire(s) ==
    /\ s \in live
    /\ configured[s]
    /\ demand[s] < 2
    /\ now >= tat[s] - tolerance[s]
    /\ tat' = [tat EXCEPT ![s] = Max(tat[s], now) + emission[s]]
    /\ demand' = [demand EXCEPT ![s] = @ + 1]
    /\ UNCHANGED <<live, rate, changed, joined, left, restarted, now, phase, cycles,
                   age, grant, configured, emission, tolerance, cancelled>>

Reject(s) ==
    /\ s \in live
    /\ configured[s]
    /\ now < tat[s] - tolerance[s]
    /\ UNCHANGED vars

Stutter == UNCHANGED vars

Next ==
    \/ Join
    \/ \E s \in Silos : Leave(s)
    \/ \E newRate \in Rates : ChangeBudget(newRate)
    \/ \E s \in Silos, total \in {0, 2} : BeginLease(s, total)
    \/ \E s \in Silos : Cancel(s)
    \/ \E s \in Silos : Deliver(s)
    \/ \E s \in Silos : Restart(s)
    \/ Tick
    \/ \E s \in Silos : Acquire(s)
    \/ \E s \in Silos : Reject(s)
    \/ Stutter

Spec == Init /\ [][Next]_vars /\ \A s \in Silos : WF_vars(Deliver(s))

TypeOK ==
    /\ live \subseteq Silos
    /\ rate \in Rates
    /\ now \in 0..MaxTime
    /\ phase \in [Silos -> {"idle", "pending"}]
    /\ cycles \in [Silos -> 0..MaxCycles]
    /\ age \in [Silos -> 0..1]
    /\ configured \in [Silos -> BOOLEAN]
    /\ cancelled \in [Silos -> BOOLEAN]
    /\ emission \in [Silos -> 1..Frequency]
    /\ tolerance \in [Silos -> 1..Frequency]
    /\ tat \in [Silos -> 0..(MaxTime + 2 * Frequency)]
    /\ demand \in [Silos -> 0..2]
    /\ changed \in BOOLEAN /\ joined \in BOOLEAN /\ left \in BOOLEAN /\ restarted \in BOOLEAN

ShareBound ==
    \A s \in Silos :
        /\ grant[s].share >= 1
        /\ grant[s].share <= grant[s].rate
        /\ grant[s].total = 0 =>
              grant[s].share = Even(grant[s].rate, grant[s].count)

CancelledGrantIgnored ==
    [][\A s \in Silos : (Deliver(s) /\ cancelled[s]) =>
        <<configured'[s], emission'[s], tolerance'[s], tat'[s], demand'[s]>> =
        <<configured[s], emission[s], tolerance[s], tat[s], demand[s]>>]_vars

RefreshPreservesDebt ==
    [][\A s \in Silos : (Deliver(s) /\ ~cancelled[s] /\ configured[s] /\
          emission[s] = Emission(grant[s].share) /\
          tolerance[s] = Tolerance(grant[s].share)) => tat'[s] = tat[s]]_vars

LocalAdmissionBound ==
    [][\A s \in Silos : Acquire(s) =>
        /\ now >= tat[s] - tolerance[s]
        /\ tat'[s] = Max(tat[s], now) + emission[s]
        /\ demand'[s] = demand[s] + 1]_vars

RejectedDemandUnchanged ==
    [][\A s \in Silos : Reject(s) => <<tat', demand'>> = <<tat, demand>>]_vars

CadenceRetainsEnforcement ==
    [][Tick => <<configured', emission', tolerance', tat'>> =
                <<configured, emission, tolerance, tat>>]_vars

RestartRetainsEnforcement ==
    [][\A s \in Silos : Restart(s) =>
        <<configured', emission', tolerance', tat'>> =
        <<configured, emission, tolerance, tat>>]_vars

DepartedSiloNotEnforcing ==
    [][\A s \in Silos : Leave(s) => s \notin live']_vars

JoinStartsUnconfigured ==
    [][Join => /\ "b" \in live' /\ ~configured'["b"]]_vars

LeaseSnapshotExact ==
    [][\A s \in Silos, total \in {0, 2} : BeginLease(s, total) =>
        /\ grant'[s].rate = rate
        /\ grant'[s].count = Cardinality(live)
        /\ grant'[s].local = demand[s]
        /\ demand'[s] = 0]_vars

CancelFencesGrant ==
    [][\A s \in Silos : Cancel(s) =>
        /\ cancelled'[s]
        /\ <<configured', emission', tolerance', tat'>> =
            <<configured, emission, tolerance, tat>>]_vars

BudgetChangeDeferred ==
    [][\A newRate \in Rates : ChangeBudget(newRate) =>
        <<configured', emission', tolerance', tat'>> =
            <<configured, emission, tolerance, tat>>]_vars

LeaseSettles == \A s \in Silos : (phase[s] = "pending") ~> (phase[s] = "idle")
=============================================================================
