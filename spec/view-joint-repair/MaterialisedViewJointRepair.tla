-------------------- MODULE MaterialisedViewJointRepair --------------------
EXTENDS Naturals, FiniteSets
CONSTANTS ViewCount, MaxFaults, TargetMode, LegacyMode
Views == 1..ViewCount
Targets == {0,1}
Zero == [v \in Views |-> 0]
One == [v \in Views |-> 1]
NoTarget == [v \in Views |-> 0]

\* One original operation, followed by one fresh repair operation. Versions
\* represent coherent admitted source tuples, not equal bytes or WAL offsets.
\* Capture relies on the maintainer's post-observation source admission and
\* replacement closure. It is NOT inferred from copying projected view rows.
VARIABLES registered, decided, applied, degraded, source, bound, rows,
          captured, target, first, firstTarget, repairDecided, repairApplied,
          reported, stage, observed, retired, acknowledged, cleared, faults,
          ordinarySeen, snapshotRows, snapshotOrdinary, legacy
vars == <<registered,decided,applied,degraded,source,bound,rows,captured,target,
          first,firstTarget,repairDecided,repairApplied,reported,stage,observed,
          retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary,legacy>>
Coordinator == <<legacy,registered,decided,applied,degraded,captured,target,first,
                 firstTarget,repairDecided,repairApplied,acknowledged,cleared>>
Snapshots == <<observed,snapshotRows,snapshotOrdinary>>
Fresh == {v \in Views : captured[v] # 0}
Live == ~cleared /\ ~degraded /\ ~legacy
ActiveRows == [v \in Views |-> rows[v][bound[v]]]

TypeOK ==
    /\ registered \subseteq Views /\ retired \subseteq Views /\ acknowledged \subseteq Views
    /\ decided \in BOOLEAN /\ applied \in BOOLEAN /\ degraded \in BOOLEAN
    /\ source \in [Views -> {1,2}] /\ bound \in [Views -> Targets]
    /\ rows \in [Views -> [Targets -> 0..2]]
    /\ captured \in [Views -> 0..2] /\ first \in [Views -> 0..2]
    /\ target \in [Views -> Targets] /\ firstTarget \in [Views -> Targets]
    /\ repairDecided \in BOOLEAN /\ repairApplied \in BOOLEAN
    /\ reported \in [ {"original","repair"} -> BOOLEAN]
    /\ stage \in [Views -> 0..2] /\ observed \in [Views -> 0..2]
    /\ cleared \in BOOLEAN /\ faults \in 0..MaxFaults
    /\ ordinarySeen \subseteq Views /\ snapshotOrdinary \subseteq Views
    /\ snapshotRows \in [Views -> 0..2]
    /\ legacy \in BOOLEAN

InitCurrent ==
    /\ registered = {} /\ decided = FALSE /\ applied = FALSE /\ degraded = FALSE
    /\ source = One
    /\ bound \in [Views -> (IF TargetMode = "Both" THEN Targets ELSE {0})]
    /\ rows = [v \in Views |-> [t \in Targets |-> 0]]
    /\ captured = Zero /\ first = Zero /\ target = NoTarget /\ firstTarget = NoTarget
    /\ repairDecided = FALSE /\ repairApplied = FALSE
    /\ reported = [k \in {"original","repair"} |-> FALSE]
    /\ stage = Zero /\ observed = Zero
    /\ retired = {} /\ acknowledged = {} /\ cleared = FALSE /\ faults = 0
    /\ ordinarySeen = {} /\ snapshotRows = Zero /\ snapshotOrdinary = {}

\* A released, already-retired original publication, not new repair evidence.
\* Its absent protocol marker cannot create fresh holds on historical callers.
InitLegacy ==
    /\ registered = Views /\ decided = TRUE /\ applied = TRUE /\ degraded = FALSE
    /\ source = One
    /\ bound \in [Views -> (IF TargetMode = "Both" THEN Targets ELSE {0})]
    /\ rows = [v \in Views |-> [t \in Targets |-> IF t = bound[v] THEN 1 ELSE 0]]
    /\ captured = Zero /\ first = Zero /\ target = NoTarget /\ firstTarget = NoTarget
    /\ repairDecided = FALSE /\ repairApplied = FALSE
    /\ reported = [k \in {"original","repair"} |-> k = "original"]
    /\ stage = One /\ observed = Zero
    /\ retired = Views /\ acknowledged = {} /\ cleared = FALSE /\ faults = 0
    /\ ordinarySeen = {} /\ snapshotRows = Zero /\ snapshotOrdinary = {}

Init ==
    /\ legacy \in (CASE LegacyMode = 0 -> {FALSE}
                      [] LegacyMode = 1 -> {TRUE}
                      [] LegacyMode = 2 -> {FALSE,TRUE})
    /\ IF legacy THEN InitLegacy ELSE InitCurrent

Register(v) ==
    /\ Live /\ ~decided /\ v \notin registered
    /\ registered' = registered \cup {v}
    /\ UNCHANGED <<legacy,decided,applied,degraded,source,bound,rows,captured,target,first,
                   firstTarget,repairDecided,repairApplied,reported,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

DecideOriginal ==
    /\ Live /\ ~decided /\ registered = Views
    /\ decided' = TRUE
    /\ UNCHANGED <<legacy,registered,applied,degraded,source,bound,rows,captured,target,first,
                   firstTarget,repairDecided,repairApplied,reported,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

PublishOriginal ==
    /\ Live /\ decided /\ ~applied /\ stage = Zero
    /\ rows' = [v \in Views |-> [rows[v] EXCEPT ![bound[v]] = 1]]
    /\ stage' = One
    /\ UNCHANGED <<Coordinator,source,bound,reported,Snapshots,retired,faults,ordinarySeen>>

CompleteOriginal ==
    /\ Live /\ decided /\ stage = One /\ ~applied
    /\ applied' = TRUE
    /\ reported' = [reported EXCEPT !["original"] = TRUE]
    /\ UNCHANGED <<legacy,registered,decided,degraded,source,bound,rows,captured,target,first,
                   firstTarget,repairDecided,repairApplied,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* Timeout is an environment step, not a fair obligation to degrade.
\* A persisted original decision closes the option of per-slice fallback.
Timeout ==
    /\ Live /\ ~decided
    /\ degraded' = TRUE
    /\ UNCHANGED <<legacy,registered,decided,applied,source,bound,rows,captured,target,first,
                   firstTarget,repairDecided,repairApplied,reported,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

Advance(v) ==
    /\ source[v] = 1
    /\ source' = [source EXCEPT ![v] = 2]
    /\ UNCHANGED <<Coordinator,bound,rows,reported,stage,Snapshots,retired,faults,ordinarySeen>>

\* Every participant supplies fresh current source data. Its first submission
\* fixes both payload and physical target, before the global repair decision.
Capture(v) ==
    /\ Live /\ applied /\ captured[v] = 0
    /\ captured' = [captured EXCEPT ![v] = source[v]]
    /\ target' = [target EXCEPT ![v] = bound[v]]
    /\ first' = [first EXCEPT ![v] = source[v]]
    /\ firstTarget' = [firstTarget EXCEPT ![v] = bound[v]]
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,
                   repairDecided,repairApplied,reported,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* Incoming retry payloads/targets may differ, but do not replace first capture.
Retry(v) ==
    /\ Live /\ v \in Fresh
    /\ UNCHANGED vars

DecideRepair ==
    /\ Live /\ applied /\ ~repairDecided /\ Fresh = Views
    /\ repairDecided' = TRUE
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,captured,
                   target,first,firstTarget,repairApplied,reported,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* The repair has a different operation identity. The backing saga makes a
\* retry after a lost response idempotent, including physical target identity.
PublishRepair ==
    /\ Live /\ repairDecided /\ ~repairApplied /\ stage = One
    /\ rows' = [v \in Views |-> [rows[v] EXCEPT ![target[v]] = captured[v]]]
    /\ stage' = [v \in Views |-> 2]
    /\ UNCHANGED <<Coordinator,source,bound,reported,Snapshots,retired,faults,ordinarySeen>>

CompleteRepair ==
    /\ Live /\ repairDecided /\ stage = [v \in Views |-> 2] /\ ~repairApplied
    /\ repairApplied' = TRUE
    /\ reported' = [reported EXCEPT !["repair"] = TRUE]
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,captured,
                   target,first,firstTarget,repairDecided,stage,observed,
                   retired,acknowledged,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* One bounded failed completion persistence / lost result. It must not report
\* an unpersisted Applied flag as success. Publication effect remains installed.
PersistFailure ==
    /\ Live /\ faults < MaxFaults
    /\ ((decided /\ ~applied /\ stage = One) \/
        (repairDecided /\ ~repairApplied /\ stage = [v \in Views |-> 2]))
    /\ faults' = faults + 1
    /\ reported' = [reported EXCEPT !["original"] = applied, !["repair"] = repairApplied]
    /\ UNCHANGED <<Coordinator,source,bound,rows,stage,Snapshots,retired,ordinarySeen>>

Crash ==
    /\ Live /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ reported' = [reported EXCEPT !["original"] = applied, !["repair"] = repairApplied]
    /\ UNCHANGED <<Coordinator,source,bound,rows,stage,Snapshots,retired,ordinarySeen>>

\* A durable participant checkpoint write precedes acknowledgement and release.
Retire(v) ==
    /\ Live /\ repairApplied /\ v \notin retired
    /\ retired' = retired \cup {v}
    /\ UNCHANGED <<Coordinator,source,bound,rows,reported,stage,Snapshots,faults,ordinarySeen>>

Acknowledge(v) ==
    /\ Live /\ repairApplied /\ v \in retired /\ v \notin acknowledged
    /\ acknowledged' = acknowledged \cup {v}
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,captured,
                   target,first,firstTarget,repairDecided,repairApplied,reported,
                   stage,observed,retired,cleared,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* Model terminal cleanup by a marker, retaining ghost evidence for assertions.
Cleanup ==
    /\ Live /\ repairApplied /\ acknowledged = Views
    /\ cleared' = TRUE
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,captured,
                   target,first,firstTarget,repairDecided,repairApplied,reported,
                   stage,observed,retired,acknowledged,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* Historical original-contract TTL, never fresh repair certification.
CleanupLegacy ==
    /\ legacy /\ applied /\ ~cleared
    /\ cleared' = TRUE
    /\ UNCHANGED <<legacy,registered,decided,applied,degraded,source,bound,rows,captured,
                   target,first,firstTarget,repairDecided,repairApplied,reported,
                   stage,observed,retired,acknowledged,faults,ordinarySeen,snapshotRows,snapshotOrdinary>>

\* A legitimately newer ordinary peer can advance after repair completion.
\* An applied coordinator retry must never resurrect its first captured value.
Ordinary(v) ==
    /\ repairApplied /\ source[v] > rows[v][bound[v]]
    /\ rows' = [rows EXCEPT ![v][bound[v]] = source[v]]
    /\ ordinarySeen' = ordinarySeen \cup {v}
    /\ UNCHANGED <<Coordinator,source,bound,reported,stage,Snapshots,retired,faults>>

Rebind(v) ==
    /\ v \in retired /\ bound[v] = 0
    /\ rows' = [rows EXCEPT ![v][1] = rows[v][0]]
    /\ bound' = [bound EXCEPT ![v] = 1]
    /\ UNCHANGED <<Coordinator,source,reported,stage,Snapshots,retired,faults,ordinarySeen>>

\* A conceptual joint-publication cut, not a shared multi-view cursor API.
\* Independent consumer opens may straddle either joint commit. Runtime
\* evidence must hold/capture the actual joint write to establish this witness.
Observe ==
    /\ observed' = stage
    /\ snapshotRows' = ActiveRows
    /\ snapshotOrdinary' = ordinarySeen
    /\ UNCHANGED <<Coordinator,source,bound,rows,reported,stage,retired,faults,ordinarySeen>>

Stutter == UNCHANGED vars
Next ==
    \/ \E v \in Views : Register(v)
    \/ DecideOriginal
    \/ PublishOriginal
    \/ CompleteOriginal
    \/ Timeout
    \/ \E v \in Views : Advance(v)
    \/ \E v \in Views : Capture(v)
    \/ \E v \in Views : Retry(v)
    \/ DecideRepair
    \/ PublishRepair
    \/ CompleteRepair
    \/ PersistFailure
    \/ Crash
    \/ \E v \in Views : Retire(v)
    \/ \E v \in Views : Acknowledge(v)
    \/ Cleanup
    \/ CleanupLegacy
    \/ \E v \in Views : Ordinary(v)
    \/ \E v \in Views : Rebind(v)
    \/ Observe
    \/ Stutter

Spec == Init /\ [][Next]_vars
    /\ (\A v \in Views : WF_vars(Register(v)) /\ WF_vars(Capture(v)) /\
                           WF_vars(Retire(v)) /\ WF_vars(Acknowledge(v)))
    /\ WF_vars(DecideOriginal) /\ WF_vars(PublishOriginal) /\ WF_vars(CompleteOriginal)
    /\ WF_vars(DecideRepair) /\ WF_vars(PublishRepair) /\ WF_vars(CompleteRepair)
    /\ WF_vars(Cleanup)
    /\ WF_vars(CleanupLegacy)

OriginalIntentHeld == decided => ~degraded
OriginalEntitled == decided => registered = Views
FreshEntitled == repairDecided => applied /\ Fresh = Views
CaptureFrozen == captured = first /\ target = firstTarget
PhysicalHeld == \A v \in Fresh \ retired : bound[v] = target[v]
JointVisible == \A a,b \in Views : stage[a] = stage[b] /\ observed[a] = observed[b]
CompletionDurable == (reported["original"] => applied) /\ (reported["repair"] => repairApplied)
CheckpointHeld == ~legacy /\ retired # {} => repairApplied
PublicationFenced == ordinarySeen # {} => repairApplied
AckEntitled == acknowledged \subseteq retired
CleanupEntitled == cleared => IF legacy THEN applied ELSE repairApplied /\ acknowledged = Views
LegacyNeverCertified == legacy =>
    ~repairDecided /\ ~repairApplied /\ captured = Zero /\ acknowledged = {}
NewerPeerPreserved == \A v \in ordinarySeen : rows[v][bound[v]] = 2
EffectExact == \A v \in Views :
    rows[v][bound[v]] =
        (IF v \in ordinarySeen THEN 2
         ELSE IF stage[v] = 0 THEN 0
         ELSE IF stage[v] = 1 THEN 1 ELSE captured[v])
SnapshotExact == \A v \in Views :
    snapshotRows[v] =
        (IF v \in snapshotOrdinary THEN 2
         ELSE IF observed[v] = 0 THEN 0
         ELSE IF observed[v] = 1 THEN 1 ELSE captured[v])
FreshCaptureCoherent == [] [(\A v \in Views :
    (captured[v] = 0 /\ captured'[v] # 0) =>
        (captured'[v] = source[v] /\ target'[v] = bound[v]))]_vars
RepairClosed == decided ~> cleared
LifecycleEnds == <> (degraded \/ cleared)
=============================================================================
