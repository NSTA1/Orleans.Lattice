---- MODULE ReplayPermits ----
\* Cold replay owns an independent lifetime, not its caller's deadline.
\* The approved correction to #4967 separates drive residency from cold
\* cancellation release. See ReplayPermitsRefinement.md.
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Requests, Partitions, Ceiling, DepthPerPermit, GapLen, Slice,
          HoldBudget, RequestBudget, ServiceWithheld
VARIABLES holders, free, withheld, queue, status, role, observed,
          pressure, pressurePhase, drain, hung, holdLeft, deadline,
          frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
          clientActive, cancelled, retained
vars == <<holders, free, withheld, queue, status, role, observed,
          pressure, pressurePhase, drain, hung, holdLeft, deadline,
          frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
          clientActive, cancelled, retained>>

Classes == {"Interactive", "Bulk", "Drive"}
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Members(q) == {q[i] : i \in 1..Len(q)}
Remove(q, r) == SelectSeq(q, LAMBDA x : x # r)
Circulating == Ceiling - withheld
DriveHolders == {r \in holders : role[r] = "Drive"}
Bound(c) == IF c = "Bulk"
            THEN Max(1, DepthPerPermit * Circulating - Circulating)
            ELSE DepthPerPermit * Circulating
Gap == [p \in Partitions |-> GapLen - banked[p]]
Complete == \A p \in Partitions : banked[p] = GapLen
Focal == 1
OwnerComplete(r) == r # Focal \/ Complete
Live == {"pending", "queued", "holding"}
NoObservation == [depth |-> 0, bound |-> 1, drain |-> "Draining", excl |-> 0, cap |-> 1]
\* Work-conserving refusal (#5992 A1): a refusal is decided only while no
\* permit is free or a waiter is already ahead, re-checked at the decision.
WorkAvailable == free > 0 /\ queue = <<>>
RefuseForWait(r) ==
    /\ role[r] # "Drive" /\ observed[r].depth > 0 /\ observed[r].drain = "WaitExceeded"
    /\ ~WorkAvailable
\* Bulk's own check excludes every retained entry; the second disjunct refuses
\* any arrival at the current Interactive bound, at every DepthPerPermit (#5992).
RefuseForDepth(r) ==
    /\ role[r] # "Drive" /\ observed[r].drain = "NoProgress"
    /\ \/ observed[r].depth - observed[r].excl >= observed[r].bound
       \/ observed[r].depth >= observed[r].cap
    /\ ~WorkAvailable
\* A refused Interactive replay keeps its gate position while its caller is
\* refused. No new retention once the retained set has reached R = Circulating.
Retainable(r) == role[r] = "Interactive" /\ Cardinality(retained) < Circulating
\* Every step but retention and expiry keeps a retained entry exactly as long as
\* it stays parked in the gate.
RetentionTracksQueue == retained' = {x \in retained : x \in Members(queue')}

TypeOK ==
    /\ holders \subseteq Requests
    /\ free \in 0..Ceiling /\ withheld \in 0..Ceiling
    /\ queue \in Seq(Requests)
    /\ status \in [Requests -> {"new", "idle", "pending", "queued", "holding", "done", "refused"}]
    /\ role \in [Requests -> {"Bulk", "Interactive", "Drive"}]
    /\ observed \in [Requests -> [depth : 0..Cardinality(Requests),
          bound : 1..(DepthPerPermit * Ceiling),
          drain : {"Draining", "NoProgress", "WaitExceeded"},
          excl : 0..Cardinality(Requests), cap : 1..(DepthPerPermit * Ceiling)]]
    /\ pressure \in BOOLEAN /\ pressurePhase \in {"new", "on", "off"}
    /\ drain \in {"Draining", "NoProgress", "WaitExceeded"}
    /\ hung \subseteq Requests
    /\ holdLeft \in [Requests -> 0..HoldBudget]
    /\ deadline \in [Requests -> 0..RequestBudget]
    /\ frontier \in [Partitions -> 0..GapLen]
    /\ banked \in [Partitions -> 0..GapLen]
    /\ checkpoint \in [Partitions -> 0..GapLen]
    /\ holes \subseteq Partitions /\ ledger \subseteq Partitions
    /\ opportunity \in Requests
    /\ arrivalsClosed \in BOOLEAN
    /\ dispatchEpoch \in BOOLEAN
    /\ clientActive \in [Requests -> BOOLEAN]
    /\ cancelled \subseteq Requests
    /\ retained \subseteq Requests
PermitConservation ==
    /\ Cardinality(holders) + free = Circulating
    /\ holders = {r \in Requests : status[r] = "holding"}
FloorOne == Circulating >= 1
QueueOwnership ==
    /\ Members(queue) = {r \in Requests : status[r] = "queued"}
    /\ Len(queue) = Cardinality(Members(queue))
CoverageSound == \A p \in Partitions : banked[p] <= frontier[p]
UnresolvedWorkDurable == holes \subseteq ledger

\* Admission is an observe-before-increment policy, not a hard queue cap.
\* Fresh excessive waits refuse below depth; a draining queue may exceed depth.
\* A retained entry queues only as a refused, retainable wait; any other arrival
\* also respects the current Interactive bound, stated here independently.
AtInteractiveBound(r) ==
    /\ role[r] # "Drive" /\ observed[r].depth >= observed[r].cap
    /\ observed[r].drain = "NoProgress" /\ ~WorkAvailable
QueueBounded == [][\A r \in Requests :
    (status[r] = "pending" /\ status'[r] = "queued") =>
        IF r \in retained' THEN RefuseForWait(r) /\ Retainable(r)
        ELSE ~RefuseForWait(r) /\ ~RefuseForDepth(r) /\ ~AtInteractiveBound(r)]_vars
\* The caller is always refused; only a retainable replay keeps its position.
SmoothedWaitRefusal == [][\A r \in Requests :
    (status[r] = "pending" /\ RefuseForWait(r) /\ status'[r] \in {"queued", "holding", "refused"})
        => /\ ~clientActive'[r]
           /\ status'[r] = IF Retainable(r) THEN "queued" ELSE "refused"]_vars
BankingMonotone == [][\A p \in Partitions :
    banked'[p] >= banked[p] /\ Gap'[p] <= Gap[p] /\ checkpoint'[p] >= checkpoint[p]]_vars
MemoryWithholding == [][\A r \in Requests :
    (r \in holders /\ r \notin holders' /\ pressure /\ withheld < Ceiling - 1)
        => withheld' = withheld + 1 /\ free' = free]_vars
RestoreOwnership == [][withheld' < withheld =>
    withheld > ServiceWithheld /\ withheld' = withheld - 1 /\ free' = free + 1]_vars
ClassPriority == [][
    /\ \A r \in Requests :
        (status[r] \in {"new", "idle", "done", "refused"} /\ status'[r] = "pending" /\ role'[r] # "Drive") =>
            observed'[r].bound = Bound(role'[r])
    /\ \A r \in Requests :
        (status[r] = "pending" /\ role[r] = "Drive" /\ status'[r] \in {"holding", "refused"}) =>
            ((status'[r] = "holding") =
                (free > 0 /\ queue = <<>> /\ Cardinality(DriveHolders) < Max(1, Circulating \div 2)))
]_vars
\* The actual class guarantee is admission headroom, not six fair queues.
PriorityReservation ==
    /\ Bound("Bulk") >= 1
    /\ Bound("Interactive") - Bound("Bulk") = Min(Circulating, Bound("Interactive") - 1)
\* P ~> ~P is exactly []<>~P, without an avoidable temporal tableau.
HoldersComplete == \A r \in Requests : []<>(r \notin holders)
ClassNonStarvation ==
    (<>[](arrivalsClosed /\ ~pressure /\ drain = "Draining") /\ HoldersComplete) =>
        \A r \in Requests :
            (status[r] = "queued" /\ r \notin retained) ~> (r \in holders \/ r \in cancelled)
DriveResidencyBounded == \A r \in Requests :
    (status[r] = "holding" /\ role[r] = "Drive") => holdLeft[r] > 0
DeadlineBounded == \A r \in Requests :
    clientActive[r] => deadline[r] > 0
ColdCancellationReleases == \A r \in Requests :
    r \in cancelled ~> status[r] \notin Live
NoSelfSustainingBacklog == [][\A r \in Requests :
    (status[r] \in Live /\ ~clientActive[r] /\ clientActive'[r]) =>
        queue' = queue /\ holders' = holders /\ status' = status]_vars
ReplayConverges ==
    ([]<>(Focal \in holders /\ Focal \notin hung)) => <>Complete
\* No arrival is refused while a permit is free and no waiter is ahead of it.
RetainedBounded ==
    /\ Cardinality(retained) <= Ceiling
    /\ \A r \in retained : status[r] = "queued" /\ role[r] = "Interactive" /\ ~clientActive[r]
\* Retention takes a slot only below the current R; a walk-down evicts nothing.
RetentionGuarded == [][\A r \in Requests :
    (r \notin retained /\ r \in retained') => Cardinality(retained) < Circulating]_vars
\* Retention is bounded in time: a retained entry leaves, admitted or aged out,
\* even while every permit is held by a hung owner.
RetentionExpires == \A r \in Requests : []<>(r \notin retained)
\* One FIFO gate: an admission never overtakes an earlier parked waiter.
AdmissionNoOvertaking == [][\A r \in Requests :
    (status[r] = "queued" /\ status'[r] = "holding") => r = Head(queue)]_vars
\* Under continuing arrivals, a parked waiter of any class eventually leaves.
QueueProgress == HoldersComplete => \A r \in Requests : []<>(status[r] # "queued")
WorkConservingRefusal == [][\A r \in Requests :
    (status[r] = "pending" /\ status'[r] = "refused" /\ role[r] # "Drive") =>
        ~(free > 0 /\ queue = <<>>)]_vars

Init ==
    /\ holders = {} /\ free = Ceiling - ServiceWithheld /\ withheld = ServiceWithheld
    /\ queue = <<>> /\ status = [r \in Requests |-> "new"]
    /\ role = [r \in Requests |-> "Interactive"]
    /\ observed = [r \in Requests |-> NoObservation]
    /\ pressure = FALSE /\ pressurePhase = "new" /\ drain = "Draining" /\ hung = {}
    /\ holdLeft = [r \in Requests |-> 0] /\ deadline = [r \in Requests |-> 0]
    /\ frontier = [p \in Partitions |-> 0] /\ banked = [p \in Partitions |-> 0]
    \* Cold reread coverage starts below an already-applied durable checkpoint.
    /\ checkpoint = [p \in Partitions |-> GapLen]
    /\ holes = {} /\ ledger = {} /\ opportunity = 1 /\ arrivalsClosed = FALSE /\ dispatchEpoch = FALSE
    /\ clientActive = [r \in Requests |-> FALSE]
    /\ cancelled = {}
    /\ retained = {}

Request(r, c) ==
    /\ status[r] \in {"new", "idle", "refused"} /\ ~arrivalsClosed
    /\ status' = [status EXCEPT ![r] = "pending"]
    /\ role' = [role EXCEPT ![r] = c]
    /\ observed' = [observed EXCEPT ![r] =
        IF c = "Drive" THEN NoObservation
        ELSE [depth |-> Len(queue), bound |-> Bound(c),
              drain |-> IF queue = <<>> THEN "Draining" ELSE drain,
              excl |-> IF c = "Bulk" THEN Cardinality(retained) ELSE 0,
              cap |-> Bound("Interactive")]]
    /\ deadline' = [deadline EXCEPT ![r] = RequestBudget]
    /\ clientActive' = [clientActive EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<holders, free, withheld, queue, pressure, pressurePhase, drain,
        hung, holdLeft, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        cancelled>>

ClientReplacement(r) ==
    /\ ~arrivalsClosed /\ status[r] \in Live /\ role[r] # "Drive"
    /\ ~clientActive[r] /\ r \notin cancelled
    /\ clientActive' = [clientActive EXCEPT ![r] = TRUE]
    /\ deadline' = [deadline EXCEPT ![r] = RequestBudget]
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed,
        pressure, pressurePhase, drain, hung, holdLeft, frontier, banked,
        checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch, cancelled>>
    /\ r \notin retained

ClientDeadline(r) ==
    /\ clientActive[r] /\ deadline[r] > 0
    /\ deadline' = [deadline EXCEPT ![r] = @ - 1]
    /\ clientActive' = [clientActive EXCEPT ![r] = deadline'[r] > 0]
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed,
        pressure, pressurePhase, drain, hung, holdLeft, frontier, banked,
        checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch, cancelled>>

\* Identify the deadline action without assuming its owner-preserving result.
ColdDeadlineIsolation == [][\A r \in Requests :
    (ClientDeadline(r) /\ status[r] \in Live /\ role[r] # "Drive") =>
        UNCHANGED <<holders, free, withheld, queue, status, role, cancelled>>]_vars

CancelColdLifetime(r) ==
    /\ role[r] # "Drive" /\ status[r] \in {"queued", "holding"} /\ r \notin cancelled
    /\ cancelled' = cancelled \cup {r}
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed,
        pressure, pressurePhase, drain, hung, holdLeft, deadline, frontier, banked,
        checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch, clientActive>>

Enqueue(r) ==
    /\ status[r] = "pending" /\ ~RefuseForWait(r) /\ ~RefuseForDepth(r)
    /\ role[r] # "Drive"
    /\ queue' = Append(queue, r) /\ status' = [status EXCEPT ![r] = "queued"]
    /\ observed' = [observed EXCEPT ![r] = NoObservation]
    /\ UNCHANGED <<holders, free, withheld, role, pressure, pressurePhase,
        drain, hung, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

RefuseWait(r) ==
    /\ status[r] = "pending" /\ RefuseForWait(r)
    /\ status' = [status EXCEPT ![r] = "refused"]
    /\ deadline' = [deadline EXCEPT ![r] = 0]
    /\ clientActive' = [clientActive EXCEPT ![r] = FALSE]
    /\ observed' = [observed EXCEPT ![r] = NoObservation]
    /\ role' = [role EXCEPT ![r] = "Interactive"]
    /\ UNCHANGED <<holders, free, withheld, queue, pressure, pressurePhase,
        drain, hung, holdLeft, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        cancelled>>
    /\ ~Retainable(r)

RefuseDepth(r) ==
    /\ status[r] = "pending" /\ ~RefuseForWait(r) /\ RefuseForDepth(r)
    /\ status' = [status EXCEPT ![r] = "refused"]
    /\ deadline' = [deadline EXCEPT ![r] = 0]
    /\ clientActive' = [clientActive EXCEPT ![r] = FALSE]
    /\ observed' = [observed EXCEPT ![r] = NoObservation]
    /\ role' = [role EXCEPT ![r] = "Interactive"]
    /\ UNCHANGED <<holders, free, withheld, queue, pressure, pressurePhase,
        drain, hung, holdLeft, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        cancelled>>

\* The caller fails fast; the replay's own gate wait stays parked in arrival
\* order as a retained waiter (#5992). It takes no permit.
RetainOnRefusal(r) ==
    /\ status[r] = "pending" /\ RefuseForWait(r) /\ Retainable(r)
    /\ queue' = Append(queue, r) /\ status' = [status EXCEPT ![r] = "queued"]
    /\ retained' = retained \cup {r}
    /\ deadline' = [deadline EXCEPT ![r] = 0]
    /\ clientActive' = [clientActive EXCEPT ![r] = FALSE]
    /\ observed' = [observed EXCEPT ![r] = NoObservation]
    /\ UNCHANGED <<holders, free, withheld, role, pressure, pressurePhase,
        drain, hung, holdLeft, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        cancelled>>

\* A retained entry reaching its maximum age leaves the gate unadmitted. Its
\* age is counted in budget visits, like a drive's; one visit is the maximum.
ExpireRetained(r) ==
    /\ r \in retained
    /\ queue' = Remove(queue, r) /\ status' = [status EXCEPT ![r] = "idle"]
    /\ retained' = retained \ {r}
    /\ role' = [role EXCEPT ![r] = "Interactive"]
    /\ drain' = IF queue' = <<>> THEN "Draining" ELSE drain
    /\ UNCHANGED <<holders, free, withheld, observed, pressure, pressurePhase,
        hung, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

\* The gate is one FIFO over parked and retained waiters (#5992 G2).
Admit(r) ==
    /\ status[r] = "queued" /\ free > 0
    /\ queue' = Remove(queue, r) /\ holders' = holders \cup {r} /\ free' = free - 1
    /\ status' = [status EXCEPT ![r] = "holding"]
    /\ holdLeft' = [holdLeft EXCEPT ![r] = 0]
    /\ drain' = IF queue' = <<>> THEN "Draining" ELSE drain
    /\ UNCHANGED <<withheld, role, observed, pressure, pressurePhase, hung,
        deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed,
        clientActive, cancelled>>
    /\ r = Head(queue)

CapacityOpportunity ==
    /\ IF free > 0 /\ queue # <<>>
       THEN \E r \in Members(queue) : Admit(r)
       ELSE UNCHANGED <<holders, free, withheld, queue, status, role, observed,
            pressure, pressurePhase, drain, hung, holdLeft, deadline, frontier,
            banked, checkpoint, holes, ledger, opportunity, arrivalsClosed,
            clientActive, cancelled>>
    /\ dispatchEpoch' = ~dispatchEpoch

DriveAdmission(r) ==
    /\ status[r] = "pending" /\ role[r] = "Drive"
    \* G1: the non-queuing drive probe never takes a permit while a waiter is parked.
    /\ LET accepted == free > 0 /\ queue = <<>> /\ Cardinality(DriveHolders) < Max(1, Circulating \div 2)
       IN /\ status' = [status EXCEPT ![r] = IF accepted THEN "holding" ELSE "refused"]
          /\ holders' = IF accepted THEN holders \cup {r} ELSE holders
          /\ free' = IF accepted THEN free - 1 ELSE free
          /\ holdLeft' = [holdLeft EXCEPT ![r] = IF accepted THEN HoldBudget ELSE 0]
          /\ deadline' = [deadline EXCEPT ![r] = IF accepted THEN @ ELSE 0]
          /\ clientActive' = [clientActive EXCEPT ![r] = IF accepted THEN @ ELSE FALSE]
    /\ observed' = [observed EXCEPT ![r] = NoObservation]
    /\ UNCHANGED <<withheld, queue, role, pressure, pressurePhase, drain,
        hung, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        cancelled>>

\* A completed slice has durably banked its prefix. Unreached partitions
\* retain their previous claim, not the larger applied checkpoint.
ReplaySlice(r, p) ==
    /\ r = Focal
    /\ r \in holders /\ r \notin hung /\ frontier[p] < GapLen
    /\ frontier' = [frontier EXCEPT ![p] = Min(GapLen, @ + Slice)]
    /\ banked' = [banked EXCEPT ![p] = frontier'[p]]
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, pressure,
        pressurePhase, drain, hung, holdLeft, deadline, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

DeferTerminal(r, p) ==
    /\ r = Focal
    /\ r \in holders /\ p \notin holes
    /\ holes' = holes \cup {p} /\ ledger' = ledger \cup {p}
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, pressure,
        pressurePhase, drain, hung, holdLeft, deadline, frontier, banked, checkpoint, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

Return(r, result) ==
    /\ r \in holders
    /\ LET reduce == pressure /\ withheld < Ceiling - 1
       IN /\ withheld' = withheld + (IF reduce THEN 1 ELSE 0)
          /\ free' = free + (IF reduce THEN 0 ELSE 1)
    /\ holders' = holders \ {r} /\ hung' = hung \ {r}
    /\ status' = [status EXCEPT ![r] = result]
    /\ holdLeft' = [holdLeft EXCEPT ![r] = 0]
    /\ deadline' = [deadline EXCEPT ![r] = 0]
    /\ clientActive' = [clientActive EXCEPT ![r] = FALSE]
    /\ cancelled' = cancelled \ {r}
    /\ role' = [role EXCEPT ![r] = "Interactive"]
    /\ UNCHANGED <<queue, observed, pressure, pressurePhase, drain,
        frontier, banked, checkpoint, holes, ledger, arrivalsClosed, dispatchEpoch>>

Release(r) == OwnerComplete(r) /\ r \notin hung /\ Return(r, "done") /\ UNCHANGED opportunity
AbandonOnBudget(r) == holdLeft[r] = 1 /\ Return(r, "idle")

CancelQueued(r) ==
    /\ status[r] = "queued" /\ r \in cancelled
    /\ queue' = Remove(queue, r) /\ status' = [status EXCEPT ![r] = "idle"]
    /\ deadline' = [deadline EXCEPT ![r] = 0]
    /\ clientActive' = [clientActive EXCEPT ![r] = FALSE]
    /\ cancelled' = cancelled \ {r}
    /\ role' = [role EXCEPT ![r] = "Interactive"]
    /\ drain' = IF queue' = <<>> THEN "Draining" ELSE drain
    /\ UNCHANGED <<holders, free, withheld, observed, pressure, pressurePhase,
        hung, holdLeft, frontier, banked, checkpoint, holes, ledger, arrivalsClosed, dispatchEpoch>>

Restore ==
    /\ ~pressure /\ withheld > ServiceWithheld
    /\ withheld' = withheld - 1 /\ free' = free + 1
    /\ UNCHANGED <<holders, queue, status, role, observed, pressure, pressurePhase,
        drain, hung, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

Hang(r) ==
    /\ r \in holders /\ r \notin hung
    /\ hung' = hung \cup {r}
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, pressure,
        pressurePhase, drain, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

MemoryPressureOn ==
    /\ pressurePhase = "new" /\ pressure' = TRUE /\ pressurePhase' = "on"
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, drain, hung,
        holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>
MemoryPressureOff ==
    /\ pressurePhase = "on" /\ pressure' = FALSE /\ pressurePhase' = "off"
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, drain, hung,
        holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>
ObserveDrain(d) ==
    /\ queue # <<>> /\ drain' = d
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, pressure,
        pressurePhase, hung, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

CloseArrivals ==
    /\ ~arrivalsClosed /\ arrivalsClosed' = TRUE
    /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed, pressure,
        pressurePhase, drain, hung, holdLeft, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, dispatchEpoch,
        clientActive, cancelled>>

\* Each finite callback opportunity visits one request, including expiry.
\* A request deadline never cancels shared background replay. Only drives have
\* an automatic owner budget; cold cancellation must actually have occurred.
BudgetOpportunity ==
    /\ LET r == opportunity
       IN IF status[r] = "holding" /\ OwnerComplete(r) /\ r \notin hung
          THEN Return(r, "done")
          ELSE IF status[r] = "holding" /\ r \in cancelled
          THEN Return(r, "idle")
          ELSE IF status[r] = "queued" /\ r \in cancelled
          THEN CancelQueued(r)
          ELSE IF status[r] = "queued" /\ r \in retained
          THEN ExpireRetained(r)
          ELSE IF status[r] = "holding" /\ role[r] = "Drive" /\ holdLeft[r] = 1
          THEN AbandonOnBudget(r)
          ELSE /\ holdLeft' = [holdLeft EXCEPT ![r] =
                        IF status[r] = "holding" /\ role[r] = "Drive" THEN @ - 1 ELSE @]
               /\ UNCHANGED <<holders, free, withheld, queue, status, role, observed,
                   pressure, pressurePhase, drain, hung, deadline, frontier, banked, checkpoint, holes, ledger, arrivalsClosed, dispatchEpoch,
                   clientActive, cancelled>>
    /\ opportunity' = IF opportunity = Cardinality(Requests) THEN 1 ELSE opportunity + 1

Stutter == UNCHANGED vars
Next ==
    \/ \E r \in Requests, c \in Classes : Request(r, c) /\ RetentionTracksQueue
    \/ \E r \in Requests : ClientReplacement(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : ClientDeadline(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : CancelColdLifetime(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : Enqueue(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : RefuseWait(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : RefuseDepth(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : RetainOnRefusal(r)
    \/ \E r \in Requests : DriveAdmission(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : Release(r) /\ RetentionTracksQueue
    \/ \E r \in Requests : Hang(r) /\ RetentionTracksQueue
    \/ \E r \in Requests, p \in Partitions : ReplaySlice(r, p) /\ RetentionTracksQueue
    \/ \E r \in Requests, p \in Partitions : DeferTerminal(r, p) /\ RetentionTracksQueue
    \/ Restore /\ RetentionTracksQueue
    \/ MemoryPressureOn /\ RetentionTracksQueue
    \/ MemoryPressureOff /\ RetentionTracksQueue
    \/ CloseArrivals /\ RetentionTracksQueue
    \/ \E d \in {"Draining", "NoProgress", "WaitExceeded"} : ObserveDrain(d) /\ RetentionTracksQueue
    \/ CapacityOpportunity /\ RetentionTracksQueue
    \/ BudgetOpportunity /\ RetentionTracksQueue
    \/ Stutter /\ RetentionTracksQueue

StorageOpportunity(p) == \E r \in Requests : ReplaySlice(r, p) /\ RetentionTracksQueue
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(BudgetOpportunity /\ RetentionTracksQueue)
    /\ WF_vars(CapacityOpportunity /\ RetentionTracksQueue)
    /\ \A r \in Requests : WF_vars(ClientDeadline(r) /\ RetentionTracksQueue)
    /\ \A r \in Requests : WF_vars(\E c \in Classes : Request(r, c) /\ RetentionTracksQueue)
    /\ \A r \in Requests : WF_vars(((Enqueue(r) \/ RefuseWait(r) \/ RefuseDepth(r) \/ DriveAdmission(r))
                                     /\ RetentionTracksQueue) \/ RetainOnRefusal(r))
    \* Strong storage fairness supplies completion INSIDE some live attempt;
    \* mere eventual provider completion after every deadline is insufficient.
    /\ \A p \in Partitions : SF_vars(StorageOpportunity(p))
====
