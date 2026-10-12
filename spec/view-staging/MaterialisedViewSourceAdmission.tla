------------------ MODULE MaterialisedViewSourceAdmission ------------------
EXTENDS Naturals, FiniteSets
CONSTANTS InitiallyNewer, Mode, CrossTree, MaxCrashes, OverlayMode
Keys == {1,2}
Participants == IF CrossTree THEN {1,2} ELSE {1}
Row(h,v,d) == [stamp |-> h, bytes |-> v, deleted |-> d]
Empty == Row(0,0,FALSE)
Prepared == [k \in Keys |-> IF k = 1 THEN Row(20,7,FALSE) ELSE Row(40,0,FALSE)]
Newer == [k \in Keys |-> IF k = 1 THEN Row(50,7,FALSE) ELSE Row(60,0,TRUE)]
Twin == [Newer EXCEPT ![2] = Row(40,0,TRUE)]
Blank == [k \in Keys |-> Empty]
Older == [Blank EXCEPT ![1] = Row(10,3,FALSE)]
Undrained == Prepared
SourceChoices ==
    IF OverlayMode = "Only" THEN {Undrained}
    ELSE (IF InitiallyNewer THEN {Newer,Twin,Blank,Older} ELSE {Blank,Older}) \cup
         (IF OverlayMode = "Both" THEN {Undrained} ELSE {})
RevisionIds == {<<1,k>> : k \in Keys}
Classify(row,k) == IF row = Prepared[k] THEN "prepared"
    ELSE IF row.stamp > Prepared[k].stamp THEN "replacement" ELSE "pending"

\* bytes=7 at stamps20/50 is intentionally identical. The second key's newer
\* row is a tombstone with the same empty bytes as its older live revision.
\* A source read admits an actual prepare or proves supersession; it does NOT
\* synthesize a projection of an unseen source replacement.
VARIABLES accumulative, pending, pendingCommitted, samplePending, sampleDecision,
          decision, source, installed, appended, read, prepares, terminals,
          sampled, classes, phase, horizon, applied, dropped, registered,
          done, value, history, ckpt, crashes
vars == <<accumulative,pending,pendingCommitted,samplePending,sampleDecision,
          decision,source,installed,appended,read,prepares,terminals,
          sampled,classes,phase,horizon,applied,dropped,registered,
          done,value,history,ckpt,crashes>>
\* One bounded leaf capture carries raw row, pending B revision and B's captured
\* decision together. This is not a transaction-atomic multi-leaf snapshot.
VisibleSource == IF pending /\ pendingCommitted
    THEN [source EXCEPT ![1] = Newer[1]] ELSE source
Ready == prepares = Keys /\ terminals = Keys
Eligible == \A k \in Keys : classes[k] = "prepared" \/
                                 (classes[k] = "replacement" /\ k \in applied)
AtHorizon == \A k \in Keys : read[k] >= horizon[k]
Admitted == accumulative \/ (Eligible /\ AtHorizon)
Rows == {Empty,Twin[2],Older[1]} \cup {Prepared[k] : k \in Keys} \cup {Newer[k] : k \in Keys}
TypeOK ==
    /\ accumulative \in BOOLEAN
    /\ pending \in BOOLEAN /\ pendingCommitted \in BOOLEAN
    /\ samplePending \in BOOLEAN /\ sampleDecision \in BOOLEAN
    /\ decision \in BOOLEAN /\ done \in BOOLEAN
    /\ source \in [Keys -> Rows] /\ sampled \in [Keys -> Rows] /\ value \in [Keys -> Rows]
    /\ installed \subseteq Keys /\ prepares \subseteq Keys /\ terminals \subseteq Keys
    /\ appended \subseteq Keys /\ applied \subseteq Keys /\ dropped \subseteq Keys
    /\ registered \subseteq Participants /\ history \subseteq RevisionIds
    /\ read \in [Keys -> 0..3] /\ ckpt \in [Keys -> 0..3] /\ horizon \in [Keys -> 0..3]
    /\ classes \in [Keys -> {"prepared","replacement","pending"}]
    /\ phase \in {"idle","sampled"} /\ crashes \in 0..MaxCrashes
Init ==
    /\ accumulative \in (IF Mode = "Both" THEN BOOLEAN ELSE {Mode = "History"})
    /\ source \in SourceChoices
    /\ decision = (source = Undrained) /\ done = FALSE
    /\ installed = IF source = Undrained THEN Keys ELSE {}
    /\ appended = {}
    /\ pending = (source = Undrained)
    /\ pendingCommitted \in (IF pending THEN BOOLEAN ELSE {FALSE})
    /\ samplePending = FALSE /\ sampleDecision = FALSE
    /\ value \in {Blank,source} /\ sampled = Blank /\ classes = [k \in Keys |-> "pending"]
    /\ read = [k \in Keys |-> 0] /\ ckpt = read /\ horizon = read
    /\ prepares = {} /\ terminals = {} /\ applied = {} /\ dropped = {}
    /\ registered = {} /\ history = {} /\ phase = "idle" /\ crashes = 0

Decide ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ ~decision /\ decision' = TRUE
    /\ UNCHANGED <<source,installed,appended,read,prepares,terminals,sampled,classes,
        phase,horizon,applied,dropped,registered,done,value,history,ckpt,crashes>>

SupersedeTwin ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ source[2] = Twin[2]
    /\ source' = [source EXCEPT ![2] = Newer[2]]
    /\ UNCHANGED <<decision,installed,appended,read,prepares,terminals,sampled,classes,
        phase,horizon,applied,dropped,registered,done,value,history,ckpt,crashes>>

Install(k) ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ decision /\ k \notin installed
    /\ installed' = installed \cup {k}
    /\ source' = [source EXCEPT ![k] = IF @.stamp < Prepared[k].stamp THEN Prepared[k] ELSE @]
    /\ UNCHANGED <<decision,appended,read,prepares,terminals,sampled,classes,
        phase,horizon,applied,dropped,registered,done,value,history,ckpt,crashes>>

\* An observed commit decision and a WAL terminal's arrival are different steps.
AppendTerminal(k) ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ decision /\ k \notin appended
    /\ appended' = appended \cup {k}
    /\ UNCHANGED <<decision,source,installed,read,prepares,terminals,sampled,classes,
        phase,horizon,applied,dropped,registered,done,value,history,ckpt,crashes>>

Read(k) ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ phase = "idle" /\ read[k] < 3
    /\ (read[k] = 0 \/ (read[k] = 1 /\ k \in appended) \/
        (read[k] = 2 /\ VisibleSource[k] = Newer[k]))
    /\ read' = [read EXCEPT ![k] = @+1]
    /\ prepares' = IF read[k] = 0 /\ ~done THEN prepares \cup {k} ELSE prepares
    /\ terminals' = IF read[k] = 1 /\ ~done THEN terminals \cup {k} ELSE terminals
    /\ dropped' = IF read[k] = 2 THEN dropped \cup {k} ELSE dropped
    /\ UNCHANGED <<decision,source,installed,appended,sampled,classes,
        phase,horizon,applied,registered,done,value,history,ckpt,crashes>>

Confirm ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted>>
    /\ phase = "idle" /\ Ready /\ ~done
    /\ sampled' = VisibleSource
    /\ samplePending' = pending /\ sampleDecision' = pendingCommitted
    /\ classes' = [k \in Keys |-> Classify(VisibleSource[k],k)]
    /\ horizon' = [k \in Keys |-> IF VisibleSource[k] = Newer[k] THEN 3 ELSE 2]
    /\ phase' = "sampled"
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,
        applied,dropped,registered,done,value,history,ckpt,crashes>>

Retry ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ phase = "sampled" /\ ~Admitted
    /\ phase' = "idle"
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,
        sampled,classes,horizon,applied,dropped,registered,done,value,history,ckpt,crashes>>

\* Reading a replacement event does not establish its actual projected effect.
\* An ordinary record may have been dropped by U; atomic B's own publication
\* closure is a ReadyClosure rely-guarantee, never a copy of the sampled row.
Repair(k) ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ phase = "idle" /\ read[k] = 3 /\ VisibleSource[k] = Newer[k] /\ k \notin applied
    /\ value' = [value EXCEPT ![k] = Newer[k]]
    /\ applied' = applied \cup {k}
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,sampled,
        classes,phase,horizon,dropped,registered,done,history,ckpt,crashes>>

Register(p) ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ p \in Participants /\ p \notin registered /\ decision
    /\ IF p = 1 THEN phase = "sampled" /\ Ready /\ Admitted ELSE installed = Keys
    /\ registered' = registered \cup {p}
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,sampled,
        classes,phase,horizon,applied,dropped,done,value,history,ckpt,crashes>>

Publish ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ phase = "sampled" /\ ~done /\ Ready /\ decision /\ Admitted
    /\ registered = Participants
    /\ value' = IF accumulative THEN value ELSE
        [k \in Keys |-> IF classes[k] = "prepared" THEN Prepared[k] ELSE value[k]]
    /\ history' = IF accumulative THEN RevisionIds ELSE history
    /\ done' = TRUE /\ phase' = "idle"
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,sampled,
        classes,horizon,applied,dropped,registered,ckpt,crashes>>

Checkpoint ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted,samplePending,sampleDecision>>
    /\ done /\ ckpt # read
    /\ ckpt' = read
    /\ UNCHANGED <<decision,source,installed,appended,read,prepares,terminals,sampled,
        classes,phase,horizon,applied,dropped,registered,done,value,history,crashes>>

Crash ==
    /\ UNCHANGED <<accumulative,pending,pendingCommitted>>
    /\ crashes < MaxCrashes
    /\ read' = ckpt /\ prepares' = {} /\ terminals' = {} /\ sampled' = Blank
    /\ samplePending' = FALSE /\ sampleDecision' = FALSE
    /\ classes' = [k \in Keys |-> "pending"] /\ horizon' = [k \in Keys |-> 0]
    /\ phase' = "idle" /\ registered' = {} /\ crashes' = crashes+1
    /\ UNCHANGED <<decision,source,installed,appended,applied,dropped,done,value,history,ckpt>>

CommitPending ==
    /\ pending /\ ~pendingCommitted
    /\ pendingCommitted' = TRUE
    /\ UNCHANGED <<accumulative,pending,samplePending,sampleDecision,decision,source,
        installed,appended,read,prepares,terminals,sampled,classes,phase,horizon,
        applied,dropped,registered,done,value,history,ckpt,crashes>>

MaterialisePending ==
    /\ pending /\ pendingCommitted
    /\ source' = [source EXCEPT ![1] = Newer[1]] /\ pending' = FALSE
    /\ UNCHANGED <<accumulative,pendingCommitted,samplePending,sampleDecision,decision,
        installed,appended,read,prepares,terminals,sampled,classes,phase,horizon,
        applied,dropped,registered,done,value,history,ckpt,crashes>>

Stutter == UNCHANGED vars

Next ==
    \/ Decide
    \/ SupersedeTwin
    \/ \E k \in Keys : Install(k)
    \/ \E k \in Keys : AppendTerminal(k)
    \/ \E k \in Keys : Read(k)
    \/ Confirm
    \/ Retry
    \/ \E k \in Keys : Repair(k)
    \/ \E p \in Participants : Register(p)
    \/ Publish
    \/ Checkpoint
    \/ Crash
    \/ CommitPending
    \/ MaterialisePending
    \/ Stutter

Spec == Init /\ [][Next]_vars /\ WF_vars(Decide) /\ WF_vars(SupersedeTwin) /\ WF_vars(Confirm) /\
    WF_vars(Retry) /\ WF_vars(Publish) /\ WF_vars(Checkpoint)
    /\ WF_vars(CommitPending) /\ WF_vars(MaterialisePending)
    /\ (\A k \in Keys : WF_vars(Install(k)) /\ WF_vars(AppendTerminal(k)) /\
                        SF_vars(Read(k)) /\ SF_vars(Repair(k)))
    /\ (\A p \in Participants : WF_vars(Register(p)))
CheckpointHeld == ~done => ckpt = [k \in Keys |-> 0]
TerminalSound == appended # {} => decision
HistoryRetained == done /\ accumulative => history = RevisionIds
SampleCoherent == [][phase = "idle" /\ phase' = "sampled" =>
    sampled' = VisibleSource /\ samplePending' = pending /\
    sampleDecision' = pendingCommitted]_vars
VersionIdentity == phase = "sampled" =>
    \A k \in Keys : (classes[k] = "prepared") = (sampled[k] = Prepared[k])
ReplacementEntitled == [][~done /\ done' /\ ~accumulative =>
    \A k \in Keys : classes[k] = "replacement" =>
        k \in applied /\ value[k] = sampled[k] /\ AtHorizon]_vars
AdmissionComplete == [][~done /\ done' /\ ~accumulative =>
    \A k \in Keys : sampled[k] = Prepared[k] \/
        (sampled[k].stamp > Prepared[k].stamp /\ k \in applied /\ AtHorizon)]_vars
RegistrationSound == \A p \in registered :
    decision /\ (IF p = 1 THEN Ready /\ Admitted ELSE installed = Keys)
ParticipantsClosed == [][~done /\ done' => registered = Participants /\ Ready /\ decision]_vars
ProjectionMonotone == [][\A k \in Keys : value'[k].stamp >= value[k].stamp]_vars
DecisionEntitled == [][~done /\ done' /\ ~accumulative /\ samplePending /\ sampleDecision =>
    sampled[1] = Newer[1] /\ value[1] = Newer[1] /\ 1 \in applied /\ AtHorizon]_vars
LogicalVisibilityMonotone == [][\A k \in Keys :
    VisibleSource'[k].stamp >= VisibleSource[k].stamp]_vars
Converges == <>[](done /\ (accumulative => history = RevisionIds) /\
    (pending => pendingCommitted) /\ (pendingCommitted /\ ~accumulative => value[1] = Newer[1]))
=============================================================================
