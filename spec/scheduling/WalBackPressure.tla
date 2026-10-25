---- MODULE WalBackPressure ----
EXTENDS Naturals, Sequences, FiniteSets

\* Sampled values are cached: properties inspect the sample/consumer boundary,
\* not an input changed after the sample. Trip inputs are TREE-wide sums.
CONSTANTS Trees, Partitions, RecoveryWindow, AcuteOnly, Windows,
          DispatchBudget, QuiesceBudget, Bursts, AllowReroute, FullBatch, CauseBound
VARIABLES input, verdict, treeVerdict, recovery, history, phase, remaining,
          sampledCause, consumer, saga, epoch, aliased, snapshotUsable,
          shuttingDown, waitsOn, reliefDone
vars == <<input, verdict, treeVerdict, recovery, history, phase, remaining,
          sampledCause, consumer, saga, epoch, aliased, snapshotUsable,
          shuttingDown, waitsOn, reliefDone>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
Kinds == {"Depth", "Dispatch", "Provider", "Flush", "Lag", "Pin"}
ReliefActors == {"Flush", "Drain", "GcTrim", "Checkpoint", "PinWrite",
                "SagaTerminal", "SagaResume"}
Gates == {"Admission", "Pace", "Quiesce"}
ASSUME Partitions \subseteq 1..3
PartitionActor(p) == CASE p = 1 -> "Partition1" [] p = 2 -> "Partition2" [] p = 3 -> "Partition3"
Actors == ReliefActors \cup Gates \cup {PartitionActor(p) : p \in Partitions}
    \cup {"Writer", "Saga", "Receiver", "View", "Snapshot"}
EmptyInput == [causes |-> {}, partition |-> 1, trips |-> 0, streak |-> 0]
States == 0..2
Depth(t, p) == IF "Depth" \in input[t].causes /\ input[t].partition = p THEN 2 ELSE 0
TripSum(t) == Cardinality({p \in Partitions : p <= input[t].trips})
SampleStreak(t) == IF input[t].causes \cap {"Flush", "Lag", "Pin"} = {}
    THEN 0 ELSE Min(Windows, input[t].streak + 1)
Acute(t) == ("Dispatch" \in input[t].causes /\ TripSum(t) >= 2)
    \/ ("Provider" \in input[t].causes /\ TripSum(t) >= 2)
    \/ ("Flush" \in input[t].causes /\ SampleStreak(t) >= Windows)
Slow(t) == SampleStreak(t) >= Windows /\ ({"Lag", "Pin"} \cap input[t].causes # {})
Raw(t, p) == IF Acute(t) \/ (~AcuteOnly /\ Depth(t, p) = 2) THEN 2
             ELSE IF Depth(t, p) > 0 \/ Slow(t) THEN 1 ELSE 0
Maximum(values) == CHOOSE v \in values : \A other \in values : v >= other
RawTree(t) == Maximum({Raw(t, p) : p \in Partitions})
Cause(t) == CASE "Dispatch" \in input[t].causes /\ TripSum(t) >= 2 -> "Dispatch"
             [] "Provider" \in input[t].causes /\ TripSum(t) >= 2 -> "Provider"
             [] "Flush" \in input[t].causes /\ SampleStreak(t) >= Windows -> "Flush"
             [] "Depth" \in input[t].causes -> "Depth"
             [] Slow(t) /\ "Lag" \in input[t].causes -> "Lag"
             [] Slow(t) /\ "Pin" \in input[t].causes -> "Pin"
             [] OTHER -> "None"
NextRecovery(t) == IF RawTree(t) = 2 THEN Max(1, RecoveryWindow)
    ELSE IF RecoveryWindow = 99 THEN recovery[t]
    ELSE IF recovery[t] > 0 THEN recovery[t] - 1 ELSE 0
Floor(t) == RawTree(t) = 0 /\ RecoveryWindow # 0 /\ NextRecovery(t) > 0
PartitionVerdict(t, p) == IF Floor(t) THEN Max(1, Raw(t, p)) ELSE Raw(t, p)
TreeValue(t) == Maximum({PartitionVerdict(t, p) : p \in Partitions})
\* Writer, saga and view use a captured physical copy; snapshot/receiver
\* getters use exactly the logical name supplied, which can be unobserved.
LogicalValue(t) == IF aliased THEN 0 ELSE treeVerdict[t]
SagaValue == IF snapshotUsable THEN verdict[1][saga.partition] ELSE treeVerdict[1]
GateValue == verdict[1][1]
Response(v) == IF v = 2 THEN "paused" ELSE IF v = 1 THEN "paced" ELSE "full"
ReceiverBatch(v) == IF v = 2 THEN 1 ELSE IF v = 1 THEN 2 ELSE FullBatch
ViewBatch(v) == IF v = 2 THEN 1 ELSE IF v = 1 THEN 2 ELSE FullBatch
Shed(v) == v = 2
Edges == Actors \X Actors
Paths(r) == r \cup {<<pair[1][1], pair[2][2]>> :
    pair \in {edgePair \in r \X r : edgePair[1][2] = edgePair[2][1]}}
Closure(r) == Paths(Paths(Paths(Paths(r))))

TypeOK ==
    /\ input \in [Trees -> [causes : SUBSET Kinds, partition : Partitions,
                           trips : 0..2, streak : 0..Windows]]
    /\ verdict \in [Trees -> [Partitions -> States]]
    /\ treeVerdict \in [Trees -> States]
    /\ recovery \in [Trees -> 0..Max(99, RecoveryWindow)]
    /\ history \in [Trees -> Seq(BOOLEAN)]
    /\ phase \in 0..15 /\ remaining \in 0..Bursts
    /\ sampledCause \in [Trees -> Kinds \cup {"None"}]
    /\ consumer \in [writer : {"full", "paced", "paused", "refused", "shutdown"},
        pace : BOOLEAN, shed : BOOLEAN, receiver : 1..FullBatch, view : 1..FullBatch,
        expiry : States, source : {"None", "WalAdmission", "AtomicWriteSaga", "SnapshotCursorOpen", "Shutdown"}]
    /\ saga \in [status : {"Execute", "waiting", "ready", "refused", "Decided"},
        next : 0..1, partition : Partitions, capturedEpoch : 0..1,
        wait : 0..Max(DispatchBudget, QuiesceBudget), aborts : 0..1]
    /\ epoch \in 0..1 /\ aliased \in BOOLEAN /\ snapshotUsable \in BOOLEAN
    /\ shuttingDown \in BOOLEAN /\ waitsOn \subseteq Edges
    /\ reliefDone \subseteq ReliefActors

Init ==
    /\ input = [t \in Trees |-> EmptyInput]
    /\ verdict = [t \in Trees |-> [p \in Partitions |-> 0]]
    /\ treeVerdict = [t \in Trees |-> 0] /\ recovery = [t \in Trees |-> 0]
    /\ history = [t \in Trees |-> <<>>] /\ phase = 0 /\ remaining = Bursts
    /\ sampledCause = [t \in Trees |-> "None"]
    /\ consumer = [writer |-> "full", pace |-> FALSE, shed |-> FALSE,
        receiver |-> FullBatch, view |-> FullBatch, expiry |-> 0, source |-> "None"]
    /\ saga = [status |-> "Execute", next |-> 0, partition |-> 1,
        capturedEpoch |-> 0, wait |-> 0, aborts |-> 0]
    /\ epoch = 0 /\ aliased \in BOOLEAN /\ snapshotUsable \in BOOLEAN
    /\ shuttingDown = FALSE /\ waitsOn = {} /\ reliefDone = {}

CauseOn(t, p, cs, trips) ==
    /\ phase = 0 /\ remaining > 0 /\ cs \subseteq Kinds /\ Cardinality(cs) \in 1..CauseBound
    /\ input' = [input EXCEPT ![t] = [causes |-> cs, partition |-> p,
                                     trips |-> trips, streak |-> 0]]
    /\ remaining' = remaining - 1
    /\ UNCHANGED <<verdict, treeVerdict, recovery, history, phase, sampledCause,
        consumer, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

CauseOff(t) ==
    /\ phase \in {0, 4, 7}
    /\ input' = [input EXCEPT ![t] = EmptyInput]
    /\ UNCHANGED <<verdict, treeVerdict, recovery, history, phase, remaining, sampledCause,
        consumer, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

Sample ==
    /\ phase \in {0, 4, 7}
    /\ (phase = 7 => saga.wait > 0)
    /\ verdict' = [t \in Trees |-> [p \in Partitions |-> PartitionVerdict(t, p)]]
    /\ treeVerdict' = [t \in Trees |-> TreeValue(t)]
    /\ recovery' = [t \in Trees |-> NextRecovery(t)]
    /\ sampledCause' = [t \in Trees |-> Cause(t)]
    /\ input' = [t \in Trees |-> [input[t] EXCEPT !.streak = SampleStreak(t)]]
    /\ history' = [t \in Trees |-> Append(
        IF Len(history[t]) = 0 \/ Len(history[t]) < Max(1, Min(RecoveryWindow, 3))
        THEN history[t] ELSE Tail(history[t]),
        treeVerdict[t] = 2 /\ TreeValue(t) = 0)]
    /\ phase' = IF phase = 7 THEN phase ELSE phase + 1
    /\ saga' = IF phase = 7 THEN [saga EXCEPT !.wait = @ - 1] ELSE saga
    /\ UNCHANGED <<remaining, consumer, epoch, aliased, snapshotUsable,
        shuttingDown, waitsOn, reliefDone>>

GateCheck ==
    /\ phase = 1
    /\ consumer' = [consumer EXCEPT !.writer = Response(GateValue), !.source = "None"]
    /\ waitsOn' = IF GateValue = 2
        THEN {<<"Writer", "Admission">>, <<"Admission", PartitionActor(1)>>,
              <<PartitionActor(1), "Flush">>} ELSE {}
    /\ reliefDone' = {} /\ phase' = 2
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown>>

Pace ==
    /\ phase = 2 /\ phase' = 3
    /\ consumer' = [consumer EXCEPT !.pace = GateValue = 1]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ParkOrRefuse ==
    /\ phase = 3 /\ phase' = 4
    /\ consumer' = [consumer EXCEPT !.expiry = GateValue]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ReReadAfterExpiry ==
    /\ phase = 5 /\ phase' = 6
    /\ consumer' = [consumer EXCEPT
        !.writer = IF consumer.expiry = 2 /\ GateValue = 2 THEN "refused" ELSE Response(GateValue),
        !.source = IF consumer.expiry = 2 /\ GateValue = 2 THEN "WalAdmission" ELSE "None"]
    /\ waitsOn' = {}
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, reliefDone>>

SagaQuiesce ==
    /\ phase = 6 /\ phase' = 7
    /\ saga' = [saga EXCEPT !.status = IF SagaValue = 2 THEN "waiting" ELSE "ready",
        !.wait = Min(QuiesceBudget, DispatchBudget)]
    /\ waitsOn' = IF SagaValue = 2
        THEN {<<"Saga", "Quiesce">>, <<"Quiesce", PartitionActor(saga.partition)>>,
              <<PartitionActor(saga.partition), "SagaTerminal">>} ELSE {}
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, consumer, epoch, aliased, snapshotUsable, shuttingDown, reliefDone>>

SagaRefuse ==
    /\ phase = 7 /\ phase' = 8
    /\ (saga.status # "waiting" \/ SagaValue = 0 \/ saga.wait = 0)
    /\ saga' = [saga EXCEPT !.status =
        IF saga.status = "waiting" /\ SagaValue = 2 THEN "refused" ELSE "ready"]
    /\ consumer' = [consumer EXCEPT !.source =
        IF saga.status = "waiting" /\ SagaValue = 2 THEN "AtomicWriteSaga" ELSE @]
    /\ waitsOn' = {}
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, epoch, aliased, snapshotUsable, shuttingDown, reliefDone>>

SagaReRoute ==
    /\ phase = 8 /\ phase' = 9
    /\ epoch' \in IF AllowReroute THEN {epoch, 1} ELSE {epoch}
    /\ saga' = [saga EXCEPT !.capturedEpoch = epoch',
        !.partition = IF epoch' # epoch THEN 2 ELSE @,
        !.status = IF saga.status = "ready" /\ epoch' = epoch THEN "Decided" ELSE "Execute",
        !.next = IF saga.status = "ready" /\ epoch' = epoch THEN 1 ELSE @]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, consumer, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ShedOpen ==
    /\ phase = 9 /\ phase' = 10
    /\ consumer' = [consumer EXCEPT !.shed = Shed(LogicalValue(1)),
        !.source = IF Shed(LogicalValue(1)) THEN "SnapshotCursorOpen" ELSE @]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ReceiverShrinkBatch ==
    /\ phase = 10 /\ phase' = 11
    /\ consumer' = [consumer EXCEPT !.receiver = ReceiverBatch(LogicalValue(1))]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ViewShrinkBatch ==
    /\ phase = 11 /\ phase' = 12
    /\ consumer' = [consumer EXCEPT !.view = ViewBatch(treeVerdict[1])]
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

Relief(actor) ==
    /\ phase = 12 /\ actor \in ReliefActors \ reliefDone
    /\ reliefDone' = reliefDone \cup {actor}
    /\ input' = [t \in Trees |-> [input[t] EXCEPT !.causes = @ \
        CASE actor = "Flush" -> {"Depth"}
          [] actor = "Drain" -> {"Lag"}
          [] OTHER -> {}]]
    /\ UNCHANGED <<verdict, treeVerdict, recovery, history, phase, remaining,
        sampledCause, consumer, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn>>

ReliefComplete ==
    /\ phase = 12 /\ reliefDone = ReliefActors /\ phase' = 13
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, consumer, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

ShutdownDrain ==
    /\ phase \in {3, 7, 13} /\ phase' = 14
    /\ shuttingDown' \in IF phase = 13 THEN {FALSE, TRUE} ELSE {TRUE}
    /\ consumer' = [consumer EXCEPT !.writer = IF shuttingDown' THEN "shutdown" ELSE @,
        !.source = IF shuttingDown' THEN "Shutdown" ELSE @]
    /\ waitsOn' = {}
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, saga, epoch, aliased, snapshotUsable, reliefDone>>

NextRound ==
    /\ phase = 14 /\ phase' = IF shuttingDown THEN 15 ELSE 0
    /\ UNCHANGED <<input, verdict, treeVerdict, recovery, history, remaining,
        sampledCause, consumer, saga, epoch, aliased, snapshotUsable, shuttingDown, waitsOn, reliefDone>>

Stutter == UNCHANGED vars

Next ==
    \/ \E t \in Trees, p \in Partitions, cs \in SUBSET Kinds, trips \in 0..2 : CauseOn(t,p,cs,trips)
    \/ \E t \in Trees : CauseOff(t)
    \/ Sample
    \/ GateCheck
    \/ Pace
    \/ ParkOrRefuse
    \/ ReReadAfterExpiry
    \/ SagaQuiesce
    \/ SagaRefuse
    \/ SagaReRoute
    \/ ShedOpen
    \/ ReceiverShrinkBatch
    \/ ViewShrinkBatch
    \/ \E actor \in ReliefActors : Relief(actor)
    \/ ReliefComplete
    \/ ShutdownDrain
    \/ NextRound
    \/ Stutter

IsSampleStep == (phase \in {0,4} /\ phase' = phase + 1)
    \/ (phase = 7 /\ phase' = 7 /\ saga'.wait < saga.wait)
SaturatedLatencyOneTick == [][(IsSampleStep =>
    (\A t \in Trees : Acute(t) => treeVerdict'[t] = 2))]_vars
TotalOrderAndRollUp == [][(IsSampleStep =>
    (\A t \in Trees : treeVerdict'[t] = Maximum({verdict'[t][p] : p \in Partitions})))]_vars
PartitionScopedRefusal == [][(phase = 1 /\ phase' = 2 =>
    (consumer'.writer = "paused" <=> verdict[1][1] = 2))]_vars
NoSpuriousRefusal == [][
    /\ (phase = 3 /\ phase' = 4 => consumer'.expiry = GateValue)
    /\ (phase = 5 /\ phase' = 6 =>
        (consumer'.writer = "refused" <=> consumer.expiry = 2 /\ GateValue = 2))
    /\ (phase = 7 /\ phase' = 8 =>
        (saga'.status = "refused" <=> saga.status = "waiting" /\ SagaValue = 2))]_vars
ThrottledNeverRefuses == [][
    /\ (phase = 2 /\ phase' = 3 => consumer'.pace = (GateValue = 1))
    /\ (phase = 9 /\ phase' = 10 => consumer'.shed = (LogicalValue(1) = 2))]_vars
SagaRefusalPreservesProgress == [][(phase = 7 /\ phase' = 8 =>
    saga'.next = saga.next /\ saga'.aborts = 0
        /\ (saga.status = "waiting" /\ SagaValue = 2 => saga'.status = "refused"))]_vars
SagaQuiesceWinsDispatch == saga.wait <= DispatchBudget
SagaScopedToCapturedPartitions == [][(phase = 6 /\ phase' = 7 =>
    (saga'.status = "waiting" <=> SagaValue = 2))]_vars
RoutingSnapshotFresh == [][(phase = 8 /\ phase' = 9 =>
    saga'.capturedEpoch = epoch' /\ (epoch' # epoch => saga'.status = "Execute"))]_vars
ReliefNeverGated == /\ \A actor \in ReliefActors : \A g \in Gates : <<actor,g>> \notin waitsOn
    /\ (phase = 12 => \A actor \in ReliefActors \ reliefDone : ENABLED Relief(actor))
NoBackPressureCycle == \A a \in Actors : <<a,a>> \notin Closure(waitsOn)
CauseAttributed == [][(IsSampleStep =>
    (\A t \in Trees : sampledCause'[t] = Cause(t)))]_vars
RefusalSourceAttributed == [][
    /\ (phase = 5 /\ phase' = 6 =>
        (consumer'.writer = "refused" => consumer'.source = "WalAdmission"))
    /\ (phase = 7 /\ phase' = 8 =>
        (saga'.status = "refused" => consumer'.source = "AtomicWriteSaga"))
    /\ (phase = 9 /\ phase' = 10 =>
        (consumer'.shed => consumer'.source = "SnapshotCursorOpen"))]_vars
ShutdownTerminal == [][(~shuttingDown /\ shuttingDown' =>
    consumer'.writer = "shutdown" /\ consumer'.source = "Shutdown" /\ waitsOn' = {})]_vars
AliasedTreeUnshielded == [][(aliased /\ phase = 10 /\ phase' = 11 =>
    consumer'.receiver = FullBatch)]_vars
ConsumerBatchesRespond == [][
    /\ (phase = 10 /\ phase' = 11 => consumer'.receiver = ReceiverBatch(LogicalValue(1)))
    /\ (phase = 11 /\ phase' = 12 => consumer'.view = ViewBatch(treeVerdict[1]))]_vars
NoFlapping == \A t \in Trees :
    /\ (RecoveryWindow = 0 /\ sampledCause[t] = "None" => treeVerdict[t] = 0)
    /\ (RecoveryWindow \in 1..98 =>
        /\ Cardinality({i \in 1..Len(history[t]) : history[t][i]}) <= 1
        /\ (recovery[t] > 0 => treeVerdict[t] >= 1))
StickyFloor == RecoveryWindow = 99 => (\A t \in Trees :
    recovery[t] > 0 => treeVerdict[t] >= 1)

\* Fair sampler/caller opportunities do not imply bounded wall-clock latency.
\* Consumers are one finite callback pass, not independently fair gate choices.
CallbackPass == GateCheck \/ Pace \/ ParkOrRefuse \/ ReReadAfterExpiry
    \/ SagaQuiesce \/ SagaRefuse \/ SagaReRoute \/ ShedOpen
    \/ ReceiverShrinkBatch \/ ViewShrinkBatch \/ ReliefComplete \/ ShutdownDrain \/ NextRound
Opportunity == Sample \/ CallbackPass \/ (\E actor \in ReliefActors : Relief(actor))
Spec == Init /\ [][Next]_vars /\ WF_vars(Opportunity)
BackPressureClears == (<>[](\A t \in Trees :
        input[t].causes \cap {"Dispatch", "Provider", "Flush"} = {}) /\ []~shuttingDown)
    => /\ <>[](\A t \in Trees : treeVerdict[t] # 2)
       /\ ((RecoveryWindow # 99 /\ <>[](\A t \in Trees :
               input[t].causes \cap {"Lag", "Pin"} = {}))
           => <>[](\A t \in Trees : treeVerdict[t] = 0))
OfferedLoadResponds ==
    /\ []((phase = 1 /\ GateValue = 2) ~>
        (consumer.writer \in {"paused","refused","shutdown"} \/ shuttingDown))
    /\ []((phase = 9 /\ LogicalValue(1) = 2) ~> (consumer.shed \/ shuttingDown))
    /\ []((phase = 10 /\ LogicalValue(1) = 2) ~> (consumer.receiver = 1 \/ shuttingDown))
    /\ []((phase = 11 /\ treeVerdict[1] = 2) ~> (consumer.view = 1 \/ shuttingDown))
SagaEventuallyCompletesOrRefuses == [](saga.status = "waiting" ~>
    saga.status \in {"refused","Execute","Decided"} \/ shuttingDown)
====
