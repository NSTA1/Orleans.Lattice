---- MODULE TurnOccupancy ----
EXTENDS Naturals, FiniteSets

\* Like SplitLinkTurns and SplitGateCycles, distinguish the exclusive turn,
\* queued requests and the operation whose completion is awaited. Dispatch
\* releases the turn; completing that operation is a separate environment step.
CONSTANTS Grains, Bound
VARIABLES turn, queued, served, operation, timer, age
vars == <<turn, queued, served, operation, timer, age>>

HoldTurn(g, who) == [turn EXCEPT ![g] = who]
ReleaseTurn(g) == [turn EXCEPT ![g] = "idle"]
TurnServedWithin(g, b) == queued[g] => age[g] <= b

TypeOK ==
    /\ turn \in [Grains -> {"idle", "timer", "call"}]
    /\ queued \in [Grains -> BOOLEAN]
    /\ served \in [Grains -> BOOLEAN]
    /\ operation \in [Grains -> {"none", "running", "done"}]
    /\ timer \in [Grains -> {"new", "fired", "dispatch", "done"}]
    /\ age \in [Grains -> 0..(Bound + 1)]

ExclusiveTurn == \A g \in Grains : turn[g] = "call" => ~queued[g]
NoAwaitedDrive == \A g \in Grains : operation[g] = "running" => turn[g] # "timer"
TurnServedWithinBound == \A g \in Grains : TurnServedWithin(g, Bound)
EveryCallServed == \A g \in Grains : queued[g] ~> served[g]

Init ==
    /\ turn = [g \in Grains |-> "idle"]
    /\ queued = [g \in Grains |-> FALSE]
    /\ served = [g \in Grains |-> FALSE]
    /\ operation = [g \in Grains |-> "none"]
    /\ timer = [g \in Grains |-> "new"]
    /\ age = [g \in Grains |-> 0]

CallQueued(g) ==
    /\ ~queued[g] /\ ~served[g] /\ turn[g] # "call"
    /\ queued' = [queued EXCEPT ![g] = TRUE]
    /\ UNCHANGED <<turn, served, operation, timer, age>>

TimerFire(g) ==
    /\ timer[g] = "new" /\ turn[g] = "idle" /\ ~queued[g]
    /\ turn' = HoldTurn(g, "timer")
    /\ timer' = [timer EXCEPT ![g] = "fired"]
    /\ UNCHANGED <<queued, served, operation, age>>

\* The timer's local admission work, not the external drive itself.
AwaitInTurn(g) ==
    /\ timer[g] = "fired" /\ turn[g] = "timer"
    /\ timer' = [timer EXCEPT ![g] = "dispatch"]
    /\ age' = [age EXCEPT ![g] = IF queued[g] THEN @ + 1 ELSE @]
    /\ UNCHANGED <<turn, queued, served, operation>>

Dispatch(g) ==
    /\ timer[g] = "dispatch" /\ turn[g] = "timer"
    /\ operation' = [operation EXCEPT ![g] = "running"]
    /\ turn' = ReleaseTurn(g)
    /\ timer' = [timer EXCEPT ![g] = "done"]
    /\ age' = [age EXCEPT ![g] = IF queued[g] THEN @ + 1 ELSE @]
    /\ UNCHANGED <<queued, served>>

CallServe(g) ==
    /\ queued[g] /\ turn[g] = "idle"
    /\ turn' = HoldTurn(g, "call")
    /\ queued' = [queued EXCEPT ![g] = FALSE]
    /\ served' = [served EXCEPT ![g] = TRUE]
    /\ UNCHANGED <<operation, timer, age>>

CallReturn(g) ==
    /\ turn[g] = "call"
    /\ turn' = ReleaseTurn(g)
    /\ UNCHANGED <<queued, served, operation, timer, age>>

Complete(g) ==
    /\ operation[g] = "running"
    /\ operation' = [operation EXCEPT ![g] = "done"]
    /\ UNCHANGED <<turn, queued, served, timer, age>>

Stutter == UNCHANGED vars
Next ==
    \/ \E g \in Grains : CallQueued(g)
    \/ \E g \in Grains : TimerFire(g)
    \/ \E g \in Grains : AwaitInTurn(g)
    \/ \E g \in Grains : Dispatch(g)
    \/ \E g \in Grains : CallServe(g)
    \/ \E g \in Grains : CallReturn(g)
    \/ \E g \in Grains : Complete(g)
    \/ Stutter

\* Runtime opportunities are an environment premise, not storage fairness.
\* There is one finite timer episode per grain. Even if Complete never runs,
\* the local turn drains. No bound on stuttering, wall time or other grains.
RuntimeStep(g) ==
    AwaitInTurn(g) \/ Dispatch(g) \/ CallServe(g) \/ CallReturn(g)
Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A g \in Grains : WF_vars(RuntimeStep(g))
====
