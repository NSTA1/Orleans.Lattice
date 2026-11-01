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
          clientActive, cancelled
vars == <<holders, free, withheld, queue, status, role, observed,
          pressure, pressurePhase, drain, hung, holdLeft, deadline,
          frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
          clientActive, cancelled>>

Classes == {"Interactive", "Bulk", "Drive", "System"}
\* The reserved system lane (#5569). A "System" role is a system-tree replay
\* that found no free shared permit and waits off the shared queue; a "Lane"
\* role is one holding the reserved lane permit. Neither touches shared capacity.
LaneCapacity == 1
OnLane == {r \in Requests : role[r] = "Lane"}
LaneFree == LaneCapacity - Cardinality(OnLane)
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
NoObservation == [depth |-> 0, bound |-> 1, drain |-> "Draining"]
RefuseForWait(r) == role[r] # "Drive" /\ observed[r].depth > 0 /\ observed[r].drain = "WaitExceeded"
RefuseForDepth(r) ==
    role[r] # "Drive" /\ observed[r].depth >= observed[r].bound /\ observed[r].drain = "NoProgress"

TypeOK ==
    /\ holders \subseteq Requests
    /\ free \in 0..Ceiling /\ withheld \in 0..Ceiling
    /\ queue \in Seq(Requests)
    /\ status \in [Requests -> {"new", "idle", "pending", "queued", "holding", "done", "refused"}]
    /\ role \in [Requests -> {"Bulk", "Interactive", "Drive", "System", "Lane"}]
    /\ observed \in [Requests -> [depth : 0..Cardinality(Requests),
          bound : 1..(DepthPerPermit * Ceiling),
          drain : {"Draining", "NoProgress", "WaitExceeded"}]]
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
PermitConservation ==
    /\ Cardinality(holders \ OnLane) + free = Circulating
    /\ holders = {r \in Requests : status[r] = "holding"}
    /\ OnLane \subseteq holders /\ Cardinality(OnLane) <= LaneCapacity
FloorOne == Circulating >= 1
QueueOwnership ==
    /\ Members(queue) = {r \in Requests : status[r] = "queued" /\ role[r] # "System"}
    /\ Len(queue) = Cardinality(Members(queue))
CoverageSound == \A p \in Partitions : banked[p] <= frontier[p]
UnresolvedWorkDurable == holes \subseteq ledger

\* Admission is an observe-before-increment policy, not a hard queue cap.
\* Fresh excessive waits refuse below depth; a draining queue may exceed depth.
QueueBounded == [][\A r \in Requests :
    (status[r] = "pending" /\ status'[r] = "queued") =>
        ~RefuseForWait(r) /\ ~RefuseForDepth(r)]_vars
SmoothedWaitRefusal == [][\A r \in Requests :
    (status[r] = "pending" /\ RefuseForWait(r) /\ status'[r] \in {"queued", "holding", "refused"})
        => status'[r] = "refused"]_vars
BankingMonotone == [][\A p \in Partitions :
    banked'[p] >= banked[p] /\ Gap'[p] <= Gap[p] /\ checkpoint'[p] >= checkpoint[p]]_vars
MemoryWithholding == [][\A r \in Requests :
    (r \in holders /\ r \notin holders' /\ role[r] # "Lane" /\ pressure /\ withheld < Ceiling - 1)
        => withheld' = withheld + 1 /\ free' = free]_vars
RestoreOwnership == [][withheld' < withheld =>
    withheld > ServiceWithheld /\ withheld' = withheld - 1 /\ free' = free + 1]_vars
ClassPriority == [][
    /\ \A r \in Requests :
        (status[r] \in {"new", "idle", "done", "refused"} /\ status'[r] = "pending"
            /\ role'[r] \notin {"Drive", "System"}) =>
            observed'[r].bound = Bound(role'[r])
    /\ \A r \in Requests :
        (status[r] = "pending" /\ role[r] = "Drive" /\ status'[r] \in {"holding", "refused"}) =>
            ((status'[r] = "holding") =
                (free > 0 /\ Cardinality(DriveHolders) < Max(1, Circulating \div 2)))
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
            status[r] = "queued" ~> (r \in holders \/ r \in cancelled)
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
\* Only a system-tree replay ever holds the reserved lane (#5569).
LaneExclusive == [][\A r \in Requests :
    (role[r] # "Lane" /\ role'[r] = "Lane") => role[r] = "System"]_vars
\* A system waiter acquires while the SHARED pool is wedged. The premise names
\* no shared holder, pressure or drain state: it assumes only that system
\* replays are bounded (every lane holder eventually leaves the lane) and that
\* the offered population is finite. See ReplayPermitsRefinement.md.
LaneHoldersComplete == \A r \in Requests : []<>(role[r] # "Lane")
SystemLaneProgress ==
    (<>[]arrivalsClosed /\ LaneHoldersComplete) =>
        \A r \in Requests :
            (status[r] = "queued" /\ role[r] = "System") ~> (r \in holders \/ r \in cancelled)

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

Request(r, c) ==
    /\ status[r] \in {"new", "idle", "refused"} /\ ~arrivalsClosed
    /\ status' = [status EXCEPT ![r] = "pending"]
    /\ role' = [role EXCEPT ![r] = c]
    /\ observed' = [observed EXCEPT ![r] =
        IF c \in {"Drive", "System"} THEN NoObservation
        ELSE [depth |-> Len(queue), bound |-> Bound(c),
              drain |-> IF queue = <<>> THEN "Draining" ELSE drain]]
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
    /\ role[r] \notin {"Drive", "System"}
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

\* Semaphore selection is nondeterministic: no contractual FIFO is invented.
Admit(r) ==
    /\ status[r] = "queued" /\ free > 0
    /\ queue' = Remove(queue, r) /\ holders' = holders \cup {r} /\ free' = free - 1
    /\ status' = [status EXCEPT ![r] = "holding"]
    /\ holdLeft' = [holdLeft EXCEPT ![r] = 0]
    /\ drain' = IF queue' = <<>> THEN "Draining" ELSE drain
    /\ UNCHANGED <<withheld, role, observed, pressure, pressurePhase, hung,
        deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed,
        clientActive, cancelled>>

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
    /\ LET accepted == free > 0 /\ Cardinality(DriveHolders) < Max(1, Circulating \div 2)
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

\* A system-tree replay takes a free shared permit, else waits for the lane.
\* It is never judged by the shared arms and never joins the shared queue.
SystemAdmission(r) ==
    /\ status[r] = "pending" /\ role[r] = "System"
    /\ IF free > 0
       THEN /\ holders' = holders \cup {r} /\ free' = free - 1
            /\ status' = [status EXCEPT ![r] = "holding"]
       ELSE /\ status' = [status EXCEPT ![r] = "queued"]
            /\ UNCHANGED <<holders, free>>
    /\ holdLeft' = [holdLeft EXCEPT ![r] = 0]
    /\ UNCHANGED <<withheld, queue, role, observed, pressure, pressurePhase, drain,
        hung, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
        clientActive, cancelled>>

\* The lane permit goes only to a waiting system-tree replay, and its
\* availability does not depend on shared capacity.
LaneOpportunity ==
    \E r \in Requests :
        /\ status[r] = "queued" /\ role[r] = "System" /\ LaneFree > 0
        /\ holders' = holders \cup {r}
        /\ status' = [status EXCEPT ![r] = "holding"]
        /\ role' = [role EXCEPT ![r] = "Lane"]
        /\ holdLeft' = [holdLeft EXCEPT ![r] = 0]
        /\ UNCHANGED <<free, withheld, queue, observed, pressure, pressurePhase, drain,
            hung, deadline, frontier, banked, checkpoint, holes, ledger, opportunity, arrivalsClosed, dispatchEpoch,
            clientActive, cancelled>>

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
           lane == role[r] = "Lane"
       IN /\ withheld' = withheld + (IF reduce /\ ~lane THEN 1 ELSE 0)
          /\ free' = free + (IF reduce \/ lane THEN 0 ELSE 1)
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
    \/ \E r \in Requests, c \in Classes : Request(r, c)
    \/ \E r \in Requests : ClientReplacement(r)
    \/ \E r \in Requests : ClientDeadline(r)
    \/ \E r \in Requests : CancelColdLifetime(r)
    \/ \E r \in Requests : Enqueue(r)
    \/ \E r \in Requests : RefuseWait(r)
    \/ \E r \in Requests : RefuseDepth(r)
    \/ \E r \in Requests : DriveAdmission(r)
    \/ \E r \in Requests : SystemAdmission(r)
    \/ LaneOpportunity
    \/ \E r \in Requests : Release(r)
    \/ \E r \in Requests : Hang(r)
    \/ \E r \in Requests, p \in Partitions : ReplaySlice(r, p)
    \/ \E r \in Requests, p \in Partitions : DeferTerminal(r, p)
    \/ Restore
    \/ MemoryPressureOn
    \/ MemoryPressureOff
    \/ CloseArrivals
    \/ \E d \in {"Draining", "NoProgress", "WaitExceeded"} : ObserveDrain(d)
    \/ CapacityOpportunity
    \/ BudgetOpportunity
    \/ Stutter

StorageOpportunity(p) == \E r \in Requests : ReplaySlice(r, p)
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(BudgetOpportunity)
    /\ WF_vars(CapacityOpportunity)
    /\ WF_vars(LaneOpportunity)
    /\ \A r \in Requests : WF_vars(ClientDeadline(r))
    /\ \A r \in Requests : WF_vars(\E c \in Classes : Request(r, c))
    /\ \A r \in Requests : WF_vars(Enqueue(r) \/ RefuseWait(r) \/ RefuseDepth(r) \/ DriveAdmission(r) \/ SystemAdmission(r))
    \* Strong storage fairness supplies completion INSIDE some live attempt;
    \* mere eventual provider completion after every deadline is insufficient.
    /\ \A p \in Partitions : SF_vars(StorageOpportunity(p))
====
