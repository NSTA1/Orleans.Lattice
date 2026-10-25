---- MODULE ReplicationShipping ----
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Records, Cap, Timeout, InitialBackoff, MaxBackoff, FullBatch, Faults
Ids == 1..Records
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Kind(r) == CASE r % 3 = 1 -> "prepare" [] r % 3 = 2 -> "terminal" [] OTHER -> "point"
Capacity == [dlq : BOOLEAN, poison : BOOLEAN, quarantine : BOOLEAN]
AllCapacity(s) == s.dlq /\ s.poison /\ s.quarantine
VARIABLES cursor, sent, queue, applied, parked, mode, age, bad, space,
          failures, backoff, wait, faultsLeft, phase, accepted,
          receiverState, aliased, controlPhase, control, batchCap, stop, pause, anchor,
          decided, restartsLeft
vars == <<cursor, sent, queue, applied, parked, mode, age, bad, space,
          failures, backoff, wait, faultsLeft, phase, accepted,
          receiverState, aliased, controlPhase, control, batchCap, stop, pause, anchor,
          decided, restartsLeft>>

\* Reuse the ACTUAL back-pressure consumer action, not a second classifier.
\* Only its consumer/phase and logical-name verdict are live in this instance.
BP == INSTANCE WalBackPressure WITH
    Trees <- {1}, Partitions <- {1}, RecoveryWindow <- 1, AcuteOnly <- TRUE,
    Windows <- 1, DispatchBudget <- 1, QuiesceBudget <- 1, Bursts <- 1,
    AllowReroute <- FALSE, CauseBound <- 1,
    input <- anchor, verdict <- anchor, treeVerdict <- [t \in {1} |-> receiverState],
    recovery <- anchor, history <- anchor, phase <- controlPhase, remaining <- anchor,
    sampledCause <- anchor, consumer <- control, saga <- anchor, epoch <- anchor,
    snapshotUsable <- anchor, shuttingDown <- anchor, waitsOn <- anchor, reliefDone <- anchor

TypeOK ==
    /\ cursor \in 0..Records /\ sent \in 0..Records
    /\ queue \in Seq(Ids) /\ applied \subseteq Ids /\ parked \subseteq Ids
    /\ mode \in [Ids -> {"fresh", "poisoned", "retired", "quarantined"}]
    /\ age \in [Ids -> 0..(Timeout + 1)] /\ bad \subseteq Ids /\ space \in Capacity
    /\ failures \in 0..MaxBackoff /\ backoff \in InitialBackoff..MaxBackoff
    /\ wait \in 0..MaxBackoff /\ faultsLeft \in 0..Faults
    /\ phase \in {"fill", "receive", "expiry", "feedback", "ack", "wait", "done"}
    /\ accepted \in BOOLEAN /\ receiverState \in 0..2 /\ aliased \in BOOLEAN
    /\ controlPhase \in {10, 11} /\ control \in [receiver : 1..FullBatch]
    /\ batchCap \in 1..FullBatch /\ stop \in BOOLEAN /\ pause \in 0..2 /\ anchor = 0
    /\ decided \subseteq {r \in Ids : Kind(r) = "prepare"} /\ restartsLeft \in 0..1
InFlightRespected == Len(queue) <= Cap
NoSkipWithoutDeadLetter == 1..cursor \subseteq applied \cup parked
NoReseedLoop == [][\A r \in Ids :
    /\ (mode[r] = "quarantined" => mode'[r] = "quarantined")
    /\ (mode[r] = "retired" => mode'[r] # "poisoned")]_vars
DecidedPrepareSafe == \A r \in decided : mode[r] = "fresh" /\ r \notin parked
CursorMonotone == [][cursor' >= cursor]_vars
EveryDeferralBounded ==
    \A r \in Ids : (r \notin decided /\ age[r] = Timeout /\ AllCapacity(space) /\ mode[r] \in {"fresh", "retired"})
        ~> (mode[r] \in {"poisoned", "quarantined"} \/ r \in applied \cup parked)
StreamProgresses == (decided \cap bad = {}) ~> (cursor = Records)

Init ==
    /\ cursor = 0 /\ sent = 0 /\ queue = <<>> /\ applied = {} /\ parked = {}
    /\ mode = [r \in Ids |-> "fresh"] /\ age = [r \in Ids |-> Timeout + 1]
    /\ bad \in SUBSET Ids /\ space \in Capacity
    /\ failures = 0 /\ backoff = InitialBackoff /\ wait = 0 /\ faultsLeft = Faults
    /\ phase = "fill" /\ accepted = FALSE /\ receiverState = 0 /\ aliased \in BOOLEAN
    /\ controlPhase = 11 /\ control = [receiver |-> FullBatch]
    /\ batchCap = FullBatch /\ stop = FALSE /\ pause = 0 /\ anchor = 0
    /\ decided \in SUBSET {r \in Ids : Kind(r) = "prepare"} /\ restartsLeft = 1

Window == IF control.receiver < FullBatch THEN 1 ELSE Cap

Send ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "fill" /\ sent < Records /\ Len(queue) < Window /\ ~stop /\ pause = 0
    /\ sent' = sent + 1 /\ queue' = Append(queue, sent + 1)
    /\ UNCHANGED <<cursor, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, phase, accepted,
        receiverState, aliased, controlPhase, control, batchCap, stop, pause>>

SealWindow ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "fill"
    /\ (Len(queue) = Window \/ sent = Records \/ stop \/ pause > 0)
    /\ phase' = IF queue # <<>> THEN "receive" ELSE IF cursor = Records THEN "done" ELSE "wait"
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, accepted,
        receiverState, aliased, controlPhase, control, batchCap, stop, pause>>

Apply ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "receive" /\ Head(queue) \notin bad
    /\ applied' = applied \cup {Head(queue)} /\ accepted' = TRUE /\ phase' = "feedback"
    /\ UNCHANGED <<cursor, sent, queue, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

TransientFailure ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "receive" /\ faultsLeft > 0
    /\ faultsLeft' = faultsLeft - 1 /\ accepted' = FALSE /\ phase' = "ack"
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

Defer ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "receive" /\ Head(queue) \in bad
    /\ age' = [age EXCEPT ![Head(queue)] = IF @ = Timeout + 1 THEN 0 ELSE @]
    /\ phase' = "expiry" /\ accepted' = FALSE
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, bad, space,
        failures, backoff, wait, faultsLeft, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

Escalate ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "expiry"
    /\ LET r == Head(queue)
           expired == age[r] = Timeout
           poison == Kind(r) # "point" /\ r \notin decided /\ expired /\ mode[r] = "fresh" /\ space.poison
           quarantine == expired /\ mode[r] = "retired" /\ space.quarantine
           park == space.dlq /\ (Kind(r) = "point" \/ quarantine
                    \/ mode[r] = "quarantined"
                    \/ (Kind(r) = "prepare" /\ (poison \/ mode[r] = "poisoned")))
       IN /\ mode' = [mode EXCEPT ![r] =
                    IF quarantine THEN "quarantined" ELSE IF poison THEN "poisoned" ELSE @]
          /\ parked' = IF park THEN parked \cup {r} ELSE parked
          /\ accepted' = park
    /\ phase' = IF accepted' THEN "feedback" ELSE "ack"
    /\ UNCHANGED <<cursor, sent, queue, applied, age, bad, space,
        failures, backoff, wait, faultsLeft, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

ObserveReceiver ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "feedback" /\ controlPhase = 11
    /\ receiverState' \in 0..2 /\ controlPhase' = 10
    /\ pause' = Max(pause, IF aliased THEN 0 ELSE receiverState')
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, phase, accepted, aliased,
        control, batchCap, stop>>

ReceiverShrinkBatch ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, accepted, receiverState, aliased,
        batchCap, stop, pause>>
    /\ phase = "feedback" /\ BP!ReceiverShrinkBatch
    /\ phase' = "ack"

\* A custom receiver may send a batch hint without a pause. One first-head
\* opportunity distinguishes this input from the WAL policy's coupled hints.
ObserveBatchOnlyFeedback ==
    /\ phase = "feedback" /\ controlPhase = 11 /\ Head(queue) = 1
    /\ control' = [receiver |-> FullBatch \div 2] /\ phase' = "ack"
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, accepted, receiverState, aliased,
        controlPhase, batchCap, stop, pause, anchor, decided, restartsLeft>>

Accept ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "ack" /\ accepted
    /\ cursor' = Head(queue) /\ queue' = Tail(queue) /\ phase' = "fill"
    /\ failures' = 0 /\ backoff' = InitialBackoff
    /\ stop' = (stop \/ control.receiver < FullBatch \/ pause > 0)
    /\ UNCHANGED <<sent, applied, parked, mode, age, bad, space,
        wait, faultsLeft, accepted, receiverState, aliased,
        controlPhase, control, batchCap, pause>>

Reject ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "ack" /\ ~accepted
    /\ sent' = cursor /\ queue' = <<>> /\ phase' = "wait"
    /\ failures' = Min(MaxBackoff, failures + 1)
    /\ backoff' = Min(MaxBackoff, IF failures = 0 THEN InitialBackoff ELSE backoff * 2)
    /\ wait' = backoff'
    /\ UNCHANGED <<cursor, applied, parked, mode, age, bad, space,
        faultsLeft, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

Tick ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "wait"
    /\ wait' = IF wait > 0 THEN wait - 1 ELSE 0
    /\ pause' = IF pause > 0 THEN pause - 1 ELSE 0
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop>>

NextPump ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ phase = "wait" /\ wait = 0 /\ pause = 0
    /\ stop' = FALSE /\ batchCap' = control.receiver /\ phase' = "fill"
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad, space,
        failures, backoff, wait, faultsLeft, accepted, receiverState, aliased,
        controlPhase, control, pause>>

FreeCapacity ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ ~AllCapacity(space) /\ space' = [dlq |-> TRUE, poison |-> TRUE, quarantine |-> TRUE]
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, bad,
        failures, backoff, wait, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

Reseed(r) ==
    /\ UNCHANGED <<anchor, decided, restartsLeft>>
    /\ r \in Ids /\ mode[r] = "poisoned"
    /\ mode' = [mode EXCEPT ![r] = "retired"]
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, age, bad, space,
        failures, backoff, wait, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause>>

Restart ==
    /\ restartsLeft > 0 /\ restartsLeft' = restartsLeft - 1
    /\ age' = [r \in Ids |-> Timeout + 1]
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, bad, space,
        failures, backoff, wait, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause, anchor, decided>>

Elapse ==
    /\ \E r \in Ids : age[r] \in 0..(Timeout - 1)
    /\ age' = [r \in Ids |-> IF age[r] \in 0..(Timeout - 1) THEN age[r] + 1 ELSE age[r]]
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, bad, space,
        failures, backoff, wait, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause, anchor, decided, restartsLeft>>

RepairDecided ==
    /\ decided \cap bad # {} /\ bad' = bad \ decided
    /\ UNCHANGED <<cursor, sent, queue, applied, parked, mode, age, space,
        failures, backoff, wait, faultsLeft, phase, accepted, receiverState, aliased,
        controlPhase, control, batchCap, stop, pause, anchor, decided, restartsLeft>>

Stutter == UNCHANGED vars

RestartPreservesDurable == [][Restart => mode' = mode /\ parked' = parked /\ cursor' = cursor]_vars
CapacityFailClosed == [][Escalate =>
    /\ (~space.dlq => parked' = parked)
    /\ (~space.poison => \A r \in Ids : mode[r] = "fresh" => mode'[r] # "poisoned")
    /\ (~space.quarantine => \A r \in Ids : mode[r] = "retired" => mode'[r] = "retired")]_vars
NoEarlyEscalation == [][Escalate /\ age[Head(queue)] # Timeout => mode' = mode]_vars
FeedbackStopsReplenishment ==
    [][Accept /\ (stop \/ control.receiver < FullBatch \/ pause > 0) => stop']_vars
ReceiverRegimes == [][
    /\ (ReceiverShrinkBatch => control'.receiver = BP!ReceiverBatch(BP!LogicalValue(1)))
    /\ (ObserveReceiver => pause' >= (IF aliased THEN 0 ELSE receiverState'))
    /\ (ObserveBatchOnlyFeedback => control'.receiver = FullBatch \div 2 /\ pause' = pause)]_vars
FlowControlHonoured == [][Send => batchCap <= control.receiver /\ pause = 0 /\ ~stop]_vars
BackoffRecovers == [][Accept => backoff' = InitialBackoff /\ failures' = 0]_vars
PausePreserved == [][Accept => pause' >= pause]_vars
Next ==
    \/ Send
    \/ SealWindow
    \/ Apply
    \/ TransientFailure
    \/ Defer
    \/ Escalate
    \/ ObserveReceiver
    \/ ReceiverShrinkBatch
    \/ ObserveBatchOnlyFeedback
    \/ Accept
    \/ Reject
    \/ Tick
    \/ NextPump
    \/ FreeCapacity
    \/ \E r \in Ids : Reseed(r)
    \/ Restart
    \/ Elapse
    \/ RepairDecided
    \/ Stutter

\* A finite workload, finite transient faults, eventual capacity and completing
\* reseeds. Callback opportunity is fair; jitter is NOT a fairness premise.
RuntimeStep == Send \/ SealWindow \/ Apply \/ Defer \/ Escalate
    \/ ObserveReceiver \/ ReceiverShrinkBatch \/ ObserveBatchOnlyFeedback
    \/ Accept \/ Reject \/ NextPump
Spec == Init /\ [][Next]_vars
    /\ WF_vars(RuntimeStep) /\ WF_vars(Tick) /\ WF_vars(FreeCapacity) /\ WF_vars(Elapse)
    /\ \A r \in Ids : WF_vars(Reseed(r))
====
