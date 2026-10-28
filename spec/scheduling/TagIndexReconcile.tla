---- MODULE TagIndexReconcile ----
EXTENDS Integers, FiniteSets

CONSTANTS TreeCount, ChunkSize
Trees == 1..TreeCount
Min(a, b) == IF a < b THEN a ELSE b
VARIABLES phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
          repaired, probeOnly, pending, demand, servingDemand, sweeps,
          pump, armNeeded, busy, selected, failureUsed, work
vars == <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
          repaired, probeOnly, pending, demand, servingDemand, sweeps,
          pump, armNeeded, busy, selected, failureUsed, work>>

TypeOK ==
    /\ phase \in {"Idle", "Probe", "Repair"}
    /\ cursor \in 0..(TreeCount + 1)
    /\ dirty \subseteq Trees
    /\ baseline \in [Trees -> {-1, 0, 1}]
    /\ fingerprint \in [Trees -> {0, 1}]
    /\ pendingBaseline \in [Trees -> {-1, 0, 1}]
    /\ repaired \in [Trees -> {-1, 0, 1}]
    /\ probeOnly \in BOOLEAN /\ pending \in BOOLEAN
    /\ demand \in {"new", "pending", "served"} /\ servingDemand \in BOOLEAN
    /\ sweeps \in 0..2
    /\ pump \in BOOLEAN /\ armNeeded \in BOOLEAN /\ busy \in BOOLEAN
    /\ selected \in 0..TreeCount /\ failureUsed \in BOOLEAN
    /\ work \in 0..(TreeCount + 1)

ProbeOnlyNeverRepairs == probeOnly => repaired = [t \in Trees |-> -1]
ChunkBounded == work <= ChunkSize
BaselineOnlyAfterRepair ==
    [][\A t \in Trees :
        baseline'[t] # baseline[t] =>
            /\ phase = "Repair" /\ busy /\ ~busy'
            /\ selected = t /\ repaired'[t] = pendingBaseline[t]
            /\ baseline'[t] = pendingBaseline[t]]_vars
SingleSweep ==
    [][/\ sweeps' <= 1
       /\ (phase # "Idle" /\ phase' = phase => cursor' >= cursor)]_vars
RepairCursorOnlyAfterCompletion ==
    [][phase = "Repair" /\ cursor' > cursor =>
        /\ busy /\ ~busy' /\ selected \notin dirty'
        /\ repaired'[selected] = pendingBaseline[selected]]_vars
DivergenceEventuallyRepaired ==
    ~probeOnly => \A t \in Trees : (fingerprint[t] # repaired[t]) ~> (fingerprint[t] = repaired[t])
SweepTerminates == (phase # "Idle") ~> (phase = "Idle")
TriggerNotLost == (demand = "pending") ~> (demand = "served")

Init ==
    /\ phase = "Idle" /\ cursor = 0 /\ dirty = {}
    /\ baseline = [t \in Trees |-> -1]
    /\ fingerprint = [t \in Trees |-> 0]
    /\ pendingBaseline = [t \in Trees |-> -1]
    /\ repaired = [t \in Trees |-> -1]
    /\ probeOnly \in BOOLEAN /\ pending = FALSE
    /\ demand = "new" /\ servingDemand = FALSE /\ sweeps = 0
    /\ pump = FALSE /\ armNeeded = FALSE /\ busy = FALSE
    /\ selected = 0 /\ failureUsed = FALSE /\ work = 0

\* A finite, monotone environment version change; no fingerprint collision.
Mutate(t) ==
    /\ fingerprint[t] = 0
    /\ fingerprint' = [fingerprint EXCEPT ![t] = 1]
    /\ UNCHANGED <<phase, cursor, dirty, baseline, pendingBaseline, repaired,
                   probeOnly, pending, demand, servingDemand, sweeps, pump,
                   armNeeded, busy, selected, failureUsed, work>>

\* Arrival is queued by the non-reentrant activation, not another active sweep.
Demand ==
    /\ demand = "new"
    /\ demand' = "pending" /\ pending' = TRUE /\ armNeeded' = TRUE
    /\ UNCHANGED <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
                   repaired, probeOnly, servingDemand, sweeps, pump, busy,
                   selected, failureUsed, work>>

Reminder ==
    /\ pending' = IF phase = "Idle" THEN TRUE ELSE pending
    /\ armNeeded' = TRUE
    /\ UNCHANGED <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
                   repaired, probeOnly, demand, servingDemand, sweeps, pump,
                   busy, selected, failureUsed, work>>

\* Successful reminder-registry completion, including recovery after a fault.
Rearm ==
    /\ armNeeded
    /\ pump' = TRUE /\ armNeeded' = FALSE
    /\ UNCHANGED <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
                   repaired, probeOnly, pending, demand, servingDemand, sweeps,
                   busy, selected, failureUsed, work>>

BeginSweep ==
    /\ phase = "Idle" /\ pending /\ pump
    /\ phase' = "Probe" /\ cursor' = 0 /\ dirty' = {}
    /\ pendingBaseline' = [t \in Trees |-> -1]
    /\ pending' = FALSE /\ servingDemand' = (demand = "pending")
    /\ sweeps' = sweeps + 1 /\ work' = 0
    /\ UNCHANGED <<baseline, fingerprint, repaired, probeOnly, demand,
                   pump, armNeeded, busy, selected, failureUsed>>

ProbeChunk ==
    /\ phase = "Probe" /\ pump /\ cursor < TreeCount
    /\ LET stop == Min(TreeCount, cursor + ChunkSize)
           batch == (cursor + 1)..stop
           divergent == {t \in batch : fingerprint[t] # baseline[t]}
       IN /\ cursor' = stop
          /\ dirty' = dirty \cup divergent
          /\ pendingBaseline' = [t \in Trees |->
                 IF t \in divergent THEN fingerprint[t] ELSE pendingBaseline[t]]
          /\ work' = Cardinality(batch)
    /\ UNCHANGED <<phase, baseline, fingerprint, repaired, probeOnly, pending,
                   demand, servingDemand, sweeps, pump, armNeeded, busy,
                   selected, failureUsed>>

BeginRepair ==
    /\ phase = "Probe" /\ pump /\ cursor = TreeCount
    /\ dirty # {} /\ ~probeOnly
    /\ phase' = "Repair" /\ cursor' = 0 /\ work' = 0
    /\ UNCHANGED <<dirty, baseline, fingerprint, pendingBaseline, repaired,
                   probeOnly, pending, demand, servingDemand, sweeps, pump,
                   armNeeded, busy, selected, failureUsed>>

RepairStart ==
    /\ phase = "Repair" /\ pump /\ ~busy /\ dirty # {}
    /\ selected' \in {t \in dirty : \A u \in dirty : t <= u}
    /\ busy' = TRUE /\ work' = 0
    /\ UNCHANGED <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
                   repaired, probeOnly, pending, demand, servingDemand, sweeps,
                   pump, armNeeded, failureUsed>>

\* Complete means all required gate-accounted reads, repair and checkpoint
\* succeeded. The whole-tree RPC is opaque, not a bounded row/time operation.
CompleteRepair ==
    /\ phase = "Repair" /\ busy
    /\ baseline' = [baseline EXCEPT ![selected] = pendingBaseline[selected]]
    /\ repaired' = [repaired EXCEPT ![selected] = pendingBaseline[selected]]
    /\ dirty' = dirty \ {selected}
    /\ cursor' = cursor + 1 /\ busy' = FALSE /\ selected' = 0 /\ work' = 1
    /\ UNCHANGED <<phase, fingerprint, pendingBaseline, probeOnly, pending,
                   demand, servingDemand, sweeps, pump, armNeeded, failureUsed>>

FailRepair ==
    /\ phase = "Repair" /\ busy /\ ~failureUsed
    /\ busy' = FALSE /\ selected' = 0 /\ failureUsed' = TRUE /\ work' = 0
    /\ UNCHANGED <<phase, cursor, dirty, baseline, fingerprint, pendingBaseline,
                   repaired, probeOnly, pending, demand, servingDemand, sweeps,
                   pump, armNeeded>>

Finish ==
    /\ pump /\ ~busy
    /\ ((phase = "Probe" /\ cursor = TreeCount /\ (probeOnly \/ dirty = {}))
        \/ (phase = "Repair" /\ dirty = {}))
    /\ phase' = "Idle" /\ cursor' = 0 /\ dirty' = {}
    /\ demand' = IF servingDemand THEN "served" ELSE demand
    /\ servingDemand' = FALSE /\ sweeps' = sweeps - 1
    /\ pump' = FALSE /\ work' = 0
    /\ UNCHANGED <<baseline, fingerprint, pendingBaseline, repaired, probeOnly,
                   pending, armNeeded, busy, selected, failureUsed>>

Stutter == UNCHANGED vars
Next ==
    \/ \E t \in Trees : Mutate(t)
    \/ Demand
    \/ Reminder
    \/ Rearm
    \/ BeginSweep
    \/ ProbeChunk
    \/ BeginRepair
    \/ RepairStart
    \/ CompleteRepair
    \/ FailRepair
    \/ Finish
    \/ Stutter

\* Runtime callback delivery is the environment opportunity. The phase guards
\* are disjoint: this is not fairness over competing scheduling decisions.
RuntimeStep == BeginSweep \/ ProbeChunk \/ BeginRepair \/ RepairStart \/ Finish
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Reminder) /\ WF_vars(Rearm)
    /\ WF_vars(RuntimeStep) /\ WF_vars(CompleteRepair)
====
