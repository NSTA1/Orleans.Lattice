---- MODULE SagaReadProgress ----
EXTENDS Naturals, Sequences, FiniteSets

\* AtomicCommit supplies acknowledged prepares and durable, immutable decisions.
\* WalBackPressure supplies the cached scoped verdict, not storage capacity.
CONSTANTS Leaves, Retries, Lease, MayExpire
VARIABLES walState, saga, registryVersion, drained, materialised,
          reader, snapshot, observed, attempts, crossed, held, leaseLeft,
          checkpointed, completed, terminalWitness
vars == <<walState, saga, registryVersion, drained, materialised,
          reader, snapshot, observed, attempts, crossed, held, leaseLeft,
          checkpointed, completed, terminalWitness>>

Init ==
    /\ walState \in 0..2 /\ saga = "idle"
    /\ registryVersion = 0 /\ drained = Leaves
    /\ materialised = [leaf \in Leaves |-> 0]
    /\ reader = "capture" /\ snapshot = 0 /\ observed = <<>>
    /\ attempts = 0 /\ crossed = FALSE /\ held = FALSE /\ leaseLeft = 0
    /\ checkpointed = FALSE /\ completed = FALSE /\ terminalWitness = FALSE

Capture ==
    /\ reader = "capture"
    /\ snapshot' = registryVersion /\ observed' = <<>>
    /\ crossed' = FALSE /\ reader' = "read"
    /\ UNCHANGED <<attempts, held, leaseLeft>>

Acquire ==
    /\ reader = "acquire"
    /\ held' = TRUE /\ leaseLeft' = Lease
    /\ snapshot' = registryVersion /\ observed' = <<>>
    /\ crossed' = FALSE /\ reader' = "read"
    /\ UNCHANGED attempts

ReadLeaf ==
    /\ reader = "read"
    /\ LET leaf == Len(observed) + 1 IN
       observed' = Append(observed,
           IF leaf \in drained THEN materialised[leaf] ELSE snapshot)
    /\ reader' = IF Len(observed') = Cardinality(Leaves) THEN "validate" ELSE "read"
    /\ UNCHANGED <<snapshot, attempts, crossed, held, leaseLeft>>

Validate ==
    /\ reader = "validate"
    /\ reader' = IF held /\ leaseLeft = 0 THEN "failed"
        ELSE IF ~crossed THEN "done"
        ELSE IF attempts < Retries THEN "capture" ELSE "acquire"
    /\ attempts' = IF crossed /\ attempts < Retries THEN attempts + 1 ELSE attempts
    /\ held' = FALSE /\ leaseLeft' = 0
    /\ UNCHANGED <<snapshot, observed, crossed>>

ReaderIdle ==
    /\ reader \in {"done", "failed"}
    /\ UNCHANGED <<reader, snapshot, observed, attempts, crossed, held, leaseLeft>>

\* An opportunity is a delivered continuation, not fairness on a policy choice.
\* The reader's guards are disjoint; the bounded retry selects Acquire.
ReaderOpportunity ==
    /\ (Capture \/ Acquire \/ ReadLeaf \/ Validate \/ ReaderIdle)
    /\ UNCHANGED <<walState, saga, registryVersion, drained, materialised,
                  checkpointed, completed, terminalWitness>>

Prepare ==
    /\ saga = "idle" /\ walState # 2
    /\ saga' = "prepared" /\ drained' = {}
    /\ checkpointed' = FALSE /\ completed' = FALSE /\ terminalWitness' = FALSE
    /\ UNCHANGED <<registryVersion, crossed>>

Decide ==
    /\ saga = "prepared" /\ (~held \/ leaseLeft = 0)
    /\ saga' = "decided" /\ registryVersion' = 1 - registryVersion
    /\ crossed' = IF reader \in {"read", "validate"} THEN TRUE ELSE crossed
    /\ UNCHANGED <<drained, checkpointed, completed, terminalWitness>>

WriterIdle ==
    /\ (saga = "idle" /\ walState = 2)
       \/ (saga = "prepared" /\ held /\ leaseLeft > 0)
       \/ saga \in {"decided", "completing", "acknowledging"}
    /\ UNCHANGED <<saga, registryVersion, drained, crossed, checkpointed, completed, terminalWitness>>

WriterOpportunity ==
    /\ (Prepare \/ Decide \/ WriterIdle)
    /\ UNCHANGED <<walState, materialised, reader, snapshot, observed, attempts, held, leaseLeft>>

Terminal ==
    /\ saga = "decided"
    /\ IF drained # Leaves
       THEN \E leaf \in Leaves \ drained :
            /\ drained' = drained \cup {leaf}
            /\ materialised' = [materialised EXCEPT ![leaf] = registryVersion]
            /\ saga' = IF drained' = Leaves THEN "completing" ELSE saga
            /\ terminalWitness' = (drained' = Leaves)
       ELSE /\ saga' = "completing" /\ terminalWitness' = TRUE
            /\ UNCHANGED <<drained, materialised>>
    /\ UNCHANGED <<checkpointed, completed>>

Checkpoint ==
    /\ saga = "completing"
    /\ saga' = "acknowledging" /\ checkpointed' = TRUE
    /\ UNCHANGED <<drained, materialised, completed, terminalWitness>>

Complete ==
    /\ saga = "acknowledging"
    /\ saga' = "idle" /\ completed' = TRUE
    /\ UNCHANGED <<drained, materialised, checkpointed, terminalWitness>>

TerminalIdle ==
    /\ saga \in {"idle", "prepared"}
    /\ UNCHANGED <<saga, drained, materialised, checkpointed, completed, terminalWitness>>

TerminalOpportunity ==
    /\ (Terminal \/ Checkpoint \/ Complete \/ TerminalIdle)
    /\ UNCHANGED <<walState, registryVersion, reader, snapshot, observed,
                  attempts, crossed, held, leaseLeft>>

Sample ==
    /\ walState' \in 0..2
    /\ UNCHANGED <<saga, registryVersion, drained, materialised, reader,
                  snapshot, observed, attempts, crossed, held, leaseLeft, checkpointed, completed, terminalWitness>>

Expire ==
    /\ MayExpire /\ held /\ leaseLeft > 0
    /\ leaseLeft' = 0
    /\ UNCHANGED <<walState, saga, registryVersion, drained, materialised,
                  reader, snapshot, observed, attempts, crossed, held, checkpointed, completed, terminalWitness>>

Stutter == UNCHANGED vars
Next ==
    \/ ReaderOpportunity
    \/ WriterOpportunity
    \/ TerminalOpportunity
    \/ Sample
    \/ Expire
    \/ Stutter
Spec == Init /\ [][Next]_vars
    /\ WF_vars(ReaderOpportunity) /\ WF_vars(WriterOpportunity) /\ WF_vars(TerminalOpportunity)

TypeOK ==
    /\ walState \in 0..2
    /\ saga \in {"idle", "prepared", "decided", "completing", "acknowledging"}
    /\ registryVersion \in 0..1 /\ drained \subseteq Leaves
    /\ materialised \in [Leaves -> 0..1]
    /\ reader \in {"capture", "read", "validate", "acquire", "done", "failed"}
    /\ snapshot \in 0..1 /\ observed \in Seq(0..1)
    /\ Len(observed) <= Cardinality(Leaves) /\ attempts \in 0..Retries
    /\ crossed \in BOOLEAN /\ held \in BOOLEAN /\ leaseLeft \in 0..Lease
    /\ checkpointed \in BOOLEAN /\ completed \in BOOLEAN /\ terminalWitness \in BOOLEAN

ReadSnapshotConsistent == reader = "done" => \A i \in 1..Len(observed) : observed[i] = snapshot
CallerOnlyAfterTerminals == completed => terminalWitness /\ drained = Leaves /\ checkpointed
QuiesceNeverAdmitsDuringSaturation == [][(walState = 2 /\ saga = "idle" => saga' # "prepared")]_vars
NoDecisionUnderReadGate == [][(held /\ leaseLeft > 0 => registryVersion' = registryVersion)]_vars
ReaderEventuallyAnswers == <> (reader = "done")
ReaderEventuallyResolves == <> (reader \in {"done", "failed"})
SagaCompletionVisible == [](saga = "decided" => <> completed)
====
