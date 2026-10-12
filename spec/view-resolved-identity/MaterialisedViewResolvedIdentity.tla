------------------ MODULE MaterialisedViewResolvedIdentity ------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Target, Failure
Logs == {1,2}
Tx == 1
Targets == IF Target = "Same" THEN {1} ELSE IF Target = "New" THEN {2} ELSE Logs
Outcomes == IF Failure = "All" THEN {"success","uncommitted","lost"}
            ELSE {Failure}
VARIABLES tail, owner, resolved, order, prepared, terminal, stagedOwner,
          read, heads, offsets, projections, liveBinding, durableBinding,
          candidate, capturedHead, outcome, phase
vars == <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
          read,heads,offsets,projections,liveBinding,durableBinding,
          candidate,capturedHead,outcome,phase>>
TypeOK ==
    /\ tail \in Logs /\ owner \in Logs /\ stagedOwner \in Logs
    /\ liveBinding \in Logs /\ durableBinding \in Logs /\ candidate \in Logs
    /\ resolved \in BOOLEAN /\ prepared \in BOOLEAN /\ terminal \in BOOLEAN
    /\ order \in {<<>>,<<Tx>>}
    /\ read \in [Logs -> 0..4] /\ heads \in [Logs -> 0..4]
    /\ offsets \in [Logs -> 0..4] /\ projections \in [Logs -> 0..2]
    /\ capturedHead \in 0..2
    /\ outcome \in {"none","success","uncommitted","lost"}
    /\ phase \in {"before","capturing","captured","returned","entered"}
Init ==
    /\ tail = 1 /\ owner = 1 /\ stagedOwner = 1
    /\ resolved = FALSE /\ order = <<>> /\ prepared = FALSE /\ terminal = FALSE
    /\ read = [l \in Logs |-> 0] /\ offsets = read /\ projections = read
    /\ heads = [l \in Logs |-> IF l = 1 THEN 2 ELSE 0]
    /\ liveBinding = 1 /\ durableBinding = 1 /\ candidate = 1
    /\ capturedHead = 0 /\ outcome = "none" /\ phase = "before"

StartRebuild ==
    /\ phase = "before" /\ projections[1] = 1 /\ resolved
    /\ candidate' \in Targets /\ phase' = "capturing"
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,heads,offsets,projections,liveBinding,durableBinding,capturedHead,outcome>>

Capture ==
    /\ phase = "capturing"
    /\ capturedHead' = IF candidate = 1 THEN 2 ELSE 0
    /\ phase' = "captured"
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,heads,offsets,projections,liveBinding,durableBinding,candidate,outcome>>

\* The reused new-log transaction and late old-log duplicates arrive after
\* capture. Thus the candidate's head cannot already cover the new transaction.
Deliver ==
    /\ phase \in {"captured","returned","entered"} /\ heads[1] = 2
    /\ heads' = [l \in Logs |-> IF l = 1 THEN 4 ELSE IF candidate = 2 THEN 2 ELSE 0]
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,offsets,projections,liveBinding,durableBinding,candidate,capturedHead,outcome,phase>>

\* Failed persistence can leave the live checkpoint object naming the candidate,
\* without changing the physical subscriber tail or its cache ownership.
PublishCheckpoint ==
    /\ phase = "captured"
    /\ outcome' \in Outcomes
    /\ liveBinding' = candidate
    /\ durableBinding' = IF outcome' = "uncommitted" THEN durableBinding ELSE candidate
    /\ offsets' = [offsets EXCEPT ![candidate] = capturedHead]
    /\ phase' = "returned"
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,heads,projections,candidate,capturedHead>>

RetryCheckpoint ==
    /\ phase = "returned" /\ outcome = "uncommitted"
    /\ durableBinding' = candidate
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,heads,offsets,projections,liveBinding,candidate,capturedHead,outcome,phase>>

\* A failed or lost response permits an already-selected original-bound retry.
\* Its duplicates must still be suppressed before the actual new tail begins.
EnterTail ==
    /\ phase = "returned" /\ durableBinding = candidate
    /\ (outcome = "success" \/ read[1] = 4)
    /\ tail' = candidate /\ owner' = candidate /\ stagedOwner' = candidate
    /\ resolved' = IF candidate = tail THEN resolved ELSE FALSE
    /\ order' = IF candidate = tail THEN order ELSE <<>>
    /\ prepared' = FALSE /\ terminal' = FALSE
    /\ read' = [read EXCEPT ![candidate] = IF @ > capturedHead THEN @ ELSE capturedHead]
    /\ phase' = "entered"
    /\ UNCHANGED <<heads,offsets,projections,liveBinding,durableBinding,
                   candidate,capturedHead,outcome>>

Read ==
    /\ read[tail] < heads[tail] /\ ~terminal
    /\ LET i == read[tail]+1
       IN /\ read' = [read EXCEPT ![tail] = i]
          /\ prepared' = IF ~resolved /\ i \in {1,3} THEN TRUE ELSE prepared
          /\ terminal' = IF ~resolved /\ i \in {2,4} THEN TRUE ELSE terminal
          /\ stagedOwner' = IF ~resolved THEN tail ELSE stagedOwner
    /\ UNCHANGED <<tail,owner,resolved,order,heads,offsets,projections,
                   liveBinding,durableBinding,candidate,capturedHead,outcome,phase>>

Flush ==
    /\ prepared /\ terminal
    /\ projections' = [projections EXCEPT ![tail] = @+1]
    /\ resolved' = TRUE /\ order' = <<Tx>>
    /\ prepared' = FALSE /\ terminal' = FALSE
    /\ UNCHANGED <<tail,owner,stagedOwner,read,heads,offsets,
                   liveBinding,durableBinding,candidate,capturedHead,outcome,phase>>

Checkpoint ==
    /\ ~prepared /\ ~terminal /\ offsets[tail] < read[tail]
    /\ offsets' = [offsets EXCEPT ![tail] = read[tail]]
    /\ UNCHANGED <<tail,owner,resolved,order,prepared,terminal,stagedOwner,
        read,heads,projections,liveBinding,durableBinding,candidate,capturedHead,outcome,phase>>

Stutter == UNCHANGED vars

Next ==
    \/ StartRebuild
    \/ Capture
    \/ Deliver
    \/ PublishCheckpoint
    \/ RetryCheckpoint
    \/ EnterTail
    \/ Read
    \/ Flush
    \/ Checkpoint
    \/ Stutter

Spec == Init /\ [][Next]_vars
    /\ WF_vars(StartRebuild) /\ WF_vars(Capture) /\ WF_vars(Deliver)
    /\ WF_vars(PublishCheckpoint) /\ WF_vars(RetryCheckpoint) /\ WF_vars(EnterTail)
    /\ WF_vars(Read) /\ WF_vars(Flush) /\ WF_vars(Checkpoint)
CacheOwned == owner = tail
QueueSound == resolved = (order = <<Tx>>)
StageOwned == prepared \/ terminal => stagedOwner = tail
ProjectionOnce == \A l \in Logs : projections[l] <= 1
CheckpointSafe == \A l \in Logs : offsets[l] >= 2 => projections[l] = 1
ResolvedRetained == [][resolved /\ tail' = tail => resolved']_vars
Converges == <>[](phase = "entered" /\ tail = candidate /\
    projections[1] = 1 /\ (candidate = 2 => projections[2] = 1) /\
    offsets[tail] = heads[tail])
=============================================================================
