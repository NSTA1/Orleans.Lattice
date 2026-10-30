---- MODULE SchemaDurableOperation ----
EXTENDS Naturals, FiniteSets

\* Tracked schema operations (remediation, migration, advance-and-migrate) over
\* one tree: admission before a handle, the durable operation record, the
\* in-process runner on the accepting silo, the tree's durable sliced
\* remediation coordinator, cooperative cancellation, terminal reporting, and
\* the loss of a runner's silo with a fresh start resuming the remediation.

CONSTANTS OpCount, SiloCount, ParamCount, NValues, MaxCrashes, MaxStalls, MaxRejects

Ops == 1..OpCount
Silos == 1..SiloCount
Params == 1..ParamCount
NoOp == 0
States == {"Absent", "Queued", "Running", "Succeeded", "Failed", "Cancelled"}
Outcomes == {"None", "Completed", "Aborted", "Cancelled", "Superseded", "Fault", "Lost"}
RemOutcomes == {"None", "Completed", "Aborted", "Cancelled"}
Phases == {"Idle", "DryRun", "Build", "Cutover"}

VARIABLES alive, crashes, stalls, rejects, handles, launches,
          opState, opRunner, opParams, opCancel, opWork,
          opFollows, opSeen, opOutcome, opResultRem,
          bad, remInProgress, remOwner, remParams, remPhase, remCursor,
          remLast, remResult, cutOver
vars == <<alive, crashes, stalls, rejects, handles, launches,
          opState, opRunner, opParams, opCancel, opWork,
          opFollows, opSeen, opOutcome, opResultRem,
          bad, remInProgress, remOwner, remParams, remPhase, remCursor,
          remLast, remResult, cutOver>>
opVars == <<opState, opRunner, opParams, opCancel, opWork,
            opFollows, opSeen, opOutcome, opResultRem>>
remVars == <<bad, remInProgress, remOwner, remParams, remPhase, remCursor,
             remLast, remResult, cutOver>>

Terminal(o) == opState[o] \in {"Succeeded", "Failed", "Cancelled"}
Live(o) == opState[o] \in {"Queued", "Running"}
StateOf(out) == CASE out = "Completed" -> "Succeeded"
                  [] out = "Cancelled" -> "Cancelled"
                  [] OTHER -> "Failed"
PhaseRank(p) == CASE p = "DryRun" -> 0 [] p = "Build" -> 1 [] OTHER -> 2
Rank == PhaseRank(remPhase) * (NValues + 1) + remCursor

TypeOK ==
    /\ alive \in [Silos -> BOOLEAN]
    /\ crashes \in 0..MaxCrashes /\ stalls \in 0..MaxStalls /\ rejects \in 0..MaxRejects
    /\ handles \subseteq Ops
    /\ launches \in [Ops -> 0..2]
    /\ opState \in [Ops -> States]
    /\ opRunner \in [Ops -> Silos]
    /\ opParams \in [Ops -> Params]
    /\ opCancel \in [Ops -> BOOLEAN]
    /\ opWork \in [Ops -> {"None", "Accepting", "Driving", "Reporting", "Done"}]
    /\ opFollows \in [Ops -> Ops \cup {NoOp}]
    /\ opSeen \in [Ops -> Ops \cup {NoOp}]
    /\ opOutcome \in [Ops -> Outcomes]
    /\ opResultRem \in [Ops -> Ops \cup {NoOp}]
    /\ bad \in BOOLEAN
    /\ remInProgress \in BOOLEAN
    /\ remOwner \in Ops \cup {NoOp}
    /\ remParams \in Params
    /\ remPhase \in Phases
    /\ remCursor \in 0..NValues
    /\ remLast \in Ops \cup {NoOp}
    /\ remResult \in [Ops -> RemOutcomes]
    /\ cutOver \subseteq Ops

\* A handle names an operation that exists.
HandleHasRecord == \A o \in handles : opState[o] # "Absent"

\* One operation id runs its work at most once.
OneExecutionPerId == \A o \in Ops : launches[o] <= 1

\* An operation recorded as succeeded cut its remediation over.
SucceededMeansCutOver ==
    \A o \in Ops : opState[o] = "Succeeded" =>
        /\ opResultRem[o] \in cutOver
        /\ remResult[opResultRem[o]] = "Completed"

\* A remediation that aborted or was cancelled never cut the tree over.
NothingCutOverUnlessCompleted ==
    \A r \in cutOver : remResult[r] \in {"None", "Completed"}

\* A recorded remediation outcome is the outcome of the remediation it names,
\* and the operation's state is the one that outcome maps to.
OutcomeMatchesRemediation ==
    \A o \in Ops : opOutcome[o] \in {"Completed", "Aborted", "Cancelled"} =>
        /\ remResult[opResultRem[o]] = opOutcome[o]
        /\ opState[o] = StateOf(opOutcome[o])

\* An operation reports the outcome of the remediation it accepted or joined,
\* or that it was superseded, naming that remediation; never another's.
OutcomeOfFollowed ==
    \A o \in Ops : opOutcome[o] \in {"Completed", "Aborted", "Cancelled", "Superseded"} =>
        opResultRem[o] = opFollows[o]

\* A remediation is cancelled only for an operation that follows it and whose
\* cancel was requested or whose record was already declared lost.
CancelledOnlyOnRequest ==
    \A r \in Ops : remResult[r] = "Cancelled" =>
        \E o \in Ops : opFollows[o] = r /\ (opCancel[o] \/ opOutcome[o] = "Lost")

\* A start refused at admission creates no record, handle, work or remediation.
RejectedStartCreatesNothing ==
    [][rejects' # rejects =>
        UNCHANGED <<opState, handles, launches, remInProgress, remOwner>>]_vars

\* A terminal operation record never changes again.
TerminalIsFinal ==
    [][\A o \in Ops : Terminal(o) =>
        (opState'[o] = opState[o] /\ opOutcome'[o] = opOutcome[o])]_vars

\* An in-flight remediation keeps its identity and parameters, and its durable
\* progress never moves backwards, whoever drives it.
InFlightRemediationKept ==
    [][(remInProgress /\ remInProgress') =>
        /\ remOwner' = remOwner /\ remParams' = remParams
        /\ Rank' >= Rank]_vars

\* A remediation advances only while a live driver follows it: no driver
\* slices a remediation it does not follow.
DrivenOnlyByFollowers ==
    [][(remInProgress /\ (remPhase' # remPhase \/ remCursor' # remCursor \/ cutOver' # cutOver))
        => \E o \in Ops : /\ opWork[o] = "Driving" /\ alive[opRunner[o]]
                          /\ opFollows[o] = remOwner]_vars

\* An operation whose runner's silo is lost eventually reads as failed.
LostRunnerReadsFailed ==
    \A o \in Ops : (Live(o) /\ ~alive[opRunner[o]]) ~> opState[o] = "Failed"

\* Every accepted operation reaches a terminal state.
OperationTerminates == \A o \in Ops : Live(o) ~> Terminal(o)

Init ==
    /\ alive = [s \in Silos |-> TRUE]
    /\ crashes = 0 /\ stalls = 0 /\ rejects = 0
    /\ handles = {}
    /\ launches = [o \in Ops |-> 0]
    /\ opState = [o \in Ops |-> "Absent"]
    /\ opRunner = [o \in Ops |-> 1]
    /\ opParams = [o \in Ops |-> 1]
    /\ opCancel = [o \in Ops |-> FALSE]
    /\ opWork = [o \in Ops |-> "None"]
    /\ opFollows = [o \in Ops |-> NoOp]
    /\ opSeen = [o \in Ops |-> NoOp]
    /\ opOutcome = [o \in Ops |-> "None"]
    /\ opResultRem = [o \in Ops |-> NoOp]
    /\ bad \in BOOLEAN
    /\ remInProgress = FALSE /\ remOwner = NoOp /\ remParams = 1
    /\ remPhase = "Idle" /\ remCursor = 0
    /\ remLast = NoOp
    /\ remResult = [o \in Ops |-> "None"]
    /\ cutOver = {}

\* A start that fails admission (caller authority, installed support, an
\* uncompilable policy, a missing version config or descriptor, a target that
\* does not advance, an id held by another kind or scope, or recovery that
\* cannot be registered) throws before any record exists.
StartRejected(o, s) ==
    /\ alive[s] /\ rejects < MaxRejects
    /\ rejects' = rejects + 1
    /\ UNCHANGED <<alive, crashes, stalls, handles, launches>>
    /\ UNCHANGED opVars /\ UNCHANGED remVars

\* An admitted start on silo s creates the queued record, returns its handle
\* and launches the work in that silo's process. Ids are taken in order.
Begin(o, s, p) ==
    /\ alive[s] /\ opState[o] = "Absent"
    /\ \A q \in 1..(o - 1) : opState[q] # "Absent"
    /\ opState' = [opState EXCEPT ![o] = "Queued"]
    /\ opRunner' = [opRunner EXCEPT ![o] = s]
    /\ opParams' = [opParams EXCEPT ![o] = p]
    /\ opWork' = [opWork EXCEPT ![o] = "Accepting"]
    /\ launches' = [launches EXCEPT ![o] = @ + 1]
    /\ handles' = handles \cup {o}
    /\ UNCHANGED <<alive, crashes, stalls, rejects, opCancel,
                   opFollows, opSeen, opOutcome, opResultRem>>
    /\ UNCHANGED remVars

\* A retried start with an existing id returns the existing operation and
\* starts nothing.
Retry(o) ==
    /\ opState[o] # "Absent"
    /\ handles' = handles \cup {o}
    /\ UNCHANGED <<alive, crashes, stalls, rejects, launches>>
    /\ UNCHANGED opVars /\ UNCHANGED remVars

\* The work reports its first phase, then accepts on the tree's coordinator:
\* it records a new remediation, follows the one in flight with the same
\* parameters, or fails in-band against one with different parameters.
Accept(o) ==
    /\ opWork[o] = "Accepting" /\ alive[opRunner[o]]
    /\ IF ~remInProgress
       THEN /\ remInProgress' = TRUE /\ remOwner' = o
            /\ remParams' = opParams[o]
            /\ remPhase' = "DryRun" /\ remCursor' = 0
            /\ opFollows' = [opFollows EXCEPT ![o] = o]
            /\ opWork' = [opWork EXCEPT ![o] = "Driving"]
            /\ opState' = [opState EXCEPT ![o] = IF Terminal(o) THEN @ ELSE "Running"]
            /\ UNCHANGED <<opOutcome>>
       ELSE IF remParams = opParams[o]
       THEN /\ opFollows' = [opFollows EXCEPT ![o] = remOwner]
            /\ opWork' = [opWork EXCEPT ![o] = "Driving"]
            /\ opState' = [opState EXCEPT ![o] = IF Terminal(o) THEN @ ELSE "Running"]
            /\ UNCHANGED <<opOutcome, remInProgress, remOwner, remParams,
                           remPhase, remCursor>>
       ELSE /\ opWork' = [opWork EXCEPT ![o] = "Done"]
            /\ opState' = [opState EXCEPT ![o] = IF Terminal(o) THEN @ ELSE "Failed"]
            /\ opOutcome' = [opOutcome EXCEPT ![o] = IF Terminal(o) THEN @ ELSE "Fault"]
            /\ UNCHANGED <<opFollows, remInProgress, remOwner, remParams,
                           remPhase, remCursor>>
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches,
                   opRunner, opParams, opCancel, opSeen, opResultRem,
                   bad, remLast, remResult, cutOver>>

\* A cancel request is recorded durably on the operation, from any silo.
RequestCancel(o) ==
    /\ Live(o) /\ ~opCancel[o]
    /\ opCancel' = [opCancel EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches,
                   opState, opRunner, opParams, opWork, opFollows,
                   opSeen, opOutcome, opResultRem>>
    /\ UNCHANGED remVars

\* The runner learns of a cancel request, or of its own record already being
\* terminal, at its next report or heartbeat, which cancels the work's token.
Stop(o) == opCancel[o] \/ Terminal(o)

\* The coordinator accepts a cancel only for the remediation it names, and
\* only in its dry run or build.
CancelAccepted(o) ==
    remOwner = opFollows[o] /\ remPhase \in {"DryRun", "Build"}

\* A driver hands its loop the report a coordinator call returned: a report
\* that is no longer in flight ends the loop with that remediation's outcome.
Report(o, r) ==
    /\ opWork' = [opWork EXCEPT ![o] = "Reporting"]
    /\ opSeen' = [opSeen EXCEPT ![o] = r]

\* With its token cancelled the driver cancels the remediation it follows
\* before a slice; only a dry run or build accepts, discarding the build.
DriveCancel(o) ==
    /\ opWork[o] = "Driving" /\ alive[opRunner[o]] /\ Stop(o)
    /\ remInProgress /\ CancelAccepted(o)
    /\ remInProgress' = FALSE /\ remPhase' = "Idle" /\ remCursor' = 0
    /\ remLast' = remOwner
    /\ remResult' = [remResult EXCEPT ![remOwner] = "Cancelled"]
    /\ Report(o, remOwner)
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches>>
    /\ UNCHANGED <<opState, opRunner, opParams, opCancel, opFollows,
                   opOutcome, opResultRem>>
    /\ UNCHANGED <<bad, remOwner, remParams, cutOver>>

\* The driver runs one bounded slice of the remediation it follows, while that
\* one is in flight: a dry-run value (the last one is unremediable when bad),
\* a build value, the cutover swap, or the completion after it. Each slice
\* persists its cursor, and a slice that ends the remediation returns its
\* terminal report.
Slice(o) ==
    /\ opWork[o] = "Driving" /\ alive[opRunner[o]] /\ remInProgress
    /\ remOwner = opFollows[o]
    /\ (~Stop(o) \/ ~CancelAccepted(o))
    /\ CASE remPhase = "DryRun" /\ remCursor < NValues /\ bad /\ remCursor + 1 = NValues ->
                /\ remInProgress' = FALSE /\ remPhase' = "Idle" /\ remCursor' = 0
                /\ remLast' = remOwner
                /\ remResult' = [remResult EXCEPT ![remOwner] = "Aborted"]
                /\ Report(o, remOwner)
                /\ UNCHANGED cutOver
         [] remPhase = "DryRun" /\ remCursor < NValues ->
                /\ remCursor' = remCursor + 1
                /\ UNCHANGED <<remInProgress, remPhase, remLast, remResult, cutOver,
                               opWork, opSeen>>
         [] remPhase = "DryRun" ->
                /\ remPhase' = "Build" /\ remCursor' = 0
                /\ UNCHANGED <<remInProgress, remLast, remResult, cutOver,
                               opWork, opSeen>>
         [] remPhase = "Build" /\ remCursor < NValues ->
                /\ remCursor' = remCursor + 1
                /\ UNCHANGED <<remInProgress, remPhase, remLast, remResult, cutOver,
                               opWork, opSeen>>
         [] remPhase = "Build" ->
                /\ remPhase' = "Cutover" /\ remCursor' = 0
                /\ UNCHANGED <<remInProgress, remLast, remResult, cutOver,
                               opWork, opSeen>>
         [] remOwner \notin cutOver ->
                /\ cutOver' = cutOver \cup {remOwner}
                /\ UNCHANGED <<remInProgress, remPhase, remCursor, remLast, remResult,
                               opWork, opSeen>>
         [] OTHER ->
                /\ remInProgress' = FALSE /\ remPhase' = "Idle" /\ remCursor' = 0
                /\ remLast' = remOwner
                /\ remResult' = [remResult EXCEPT ![remOwner] = "Completed"]
                /\ Report(o, remOwner)
                /\ UNCHANGED cutOver
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches>>
    /\ UNCHANGED <<opState, opRunner, opParams, opCancel, opFollows,
                   opOutcome, opResultRem>>
    /\ UNCHANGED <<bad, remOwner, remParams>>

\* A driver whose next coordinator call finds the remediation it follows no
\* longer in flight (another driver ended it) is returned that remediation's
\* report; when another remediation has since been accepted, the followed one
\* is superseded and the driver reports no remediation outcome at all.
Observe(o) ==
    /\ opWork[o] = "Driving" /\ alive[opRunner[o]]
    /\ (~remInProgress \/ remOwner # opFollows[o])
    /\ Report(o, IF ~remInProgress /\ remLast = opFollows[o] THEN remLast ELSE NoOp)
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches>>
    /\ UNCHANGED <<opState, opRunner, opParams, opCancel, opFollows,
                   opOutcome, opResultRem>>
    /\ UNCHANGED remVars

\* The runner maps the report its loop ended with to the completion; a record
\* already terminal keeps its outcome.
Finish(o) ==
    /\ opWork[o] = "Reporting" /\ alive[opRunner[o]]
    /\ opWork' = [opWork EXCEPT ![o] = "Done"]
    /\ IF Terminal(o)
       THEN UNCHANGED <<opState, opOutcome, opResultRem>>
       ELSE IF opSeen[o] = NoOp
       THEN /\ opOutcome' = [opOutcome EXCEPT ![o] = "Superseded"]
            /\ opState' = [opState EXCEPT ![o] = "Failed"]
            /\ opResultRem' = [opResultRem EXCEPT ![o] = opFollows[o]]
       ELSE /\ opOutcome' = [opOutcome EXCEPT ![o] = remResult[opSeen[o]]]
            /\ opState' = [opState EXCEPT ![o] = StateOf(remResult[opSeen[o]])]
            /\ opResultRem' = [opResultRem EXCEPT ![o] = opSeen[o]]
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches,
                   opRunner, opParams, opCancel, opFollows, opSeen>>
    /\ UNCHANGED remVars

\* Environment: a silo dies, losing every runner in its process. One silo
\* always survives to observe and restart.
Crash(s) ==
    /\ alive[s] /\ crashes < MaxCrashes
    /\ Cardinality({t \in Silos : alive[t]}) > 1
    /\ alive' = [alive EXCEPT ![s] = FALSE]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<stalls, rejects, handles, launches>>
    /\ UNCHANGED opVars /\ UNCHANGED remVars

\* A read of a live record whose runner's silo is dead records it as failed.
DetectLoss(o) ==
    /\ Live(o) /\ ~alive[opRunner[o]]
    /\ opState' = [opState EXCEPT ![o] = "Failed"]
    /\ opOutcome' = [opOutcome EXCEPT ![o] = "Lost"]
    /\ UNCHANGED <<alive, crashes, stalls, rejects, handles, launches,
                   opRunner, opParams, opCancel, opWork, opFollows,
                   opSeen, opResultRem>>
    /\ UNCHANGED remVars

\* Environment: a runner stops heartbeating past the lease while its silo
\* lives, so a read records the operation failed although its work goes on.
LeaseExpire(o) ==
    /\ Live(o) /\ alive[opRunner[o]] /\ stalls < MaxStalls
    /\ stalls' = stalls + 1
    /\ opState' = [opState EXCEPT ![o] = "Failed"]
    /\ opOutcome' = [opOutcome EXCEPT ![o] = "Lost"]
    /\ UNCHANGED <<alive, crashes, rejects, handles, launches,
                   opRunner, opParams, opCancel, opWork, opFollows,
                   opSeen, opResultRem>>
    /\ UNCHANGED remVars

Stutter == UNCHANGED vars

Runner(o) ==
    Accept(o) \/ DriveCancel(o) \/ Slice(o) \/ Observe(o) \/ Finish(o)

Next ==
    \/ \E o \in Ops, s \in Silos : StartRejected(o, s)
    \/ \E o \in Ops, s \in Silos, p \in Params : Begin(o, s, p)
    \/ \E o \in Ops : Retry(o)
    \/ \E o \in Ops : Accept(o)
    \/ \E o \in Ops : RequestCancel(o)
    \/ \E o \in Ops : DriveCancel(o)
    \/ \E o \in Ops : Slice(o)
    \/ \E o \in Ops : Observe(o)
    \/ \E o \in Ops : Finish(o)
    \/ \E s \in Silos : Crash(s)
    \/ \E o \in Ops : DetectLoss(o)
    \/ \E o \in Ops : LeaseExpire(o)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A o \in Ops : WF_vars(Runner(o))
    /\ \A o \in Ops : WF_vars(DetectLoss(o))
====
