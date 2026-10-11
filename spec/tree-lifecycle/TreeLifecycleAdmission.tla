----------------------- MODULE TreeLifecycleAdmission -----------------------
\* The publish-before-read interlock shared by resize versus shard migration
\* or a legacy in-place bootstrap, and persisted replication undo clearance.
\* Companion TreeLifecycle models the copy, fence, redirect and swap phases.
EXTENDS Naturals

MaxFaults == 1
InitiallyReplicated == FALSE
Phases == {"idle", "announced", "active", "done"}

VARIABLES resize, competitor, resizeUsed, competitorUsed, alias, undo,
          cleared, replicated, enabledReplication, running, faults, compensated

vars == <<resize, competitor, resizeUsed, competitorUsed, alias, undo,
          cleared, replicated, enabledReplication, running, faults, compensated>>

Init ==
    /\ resize = "idle" /\ competitor = "idle"
    /\ resizeUsed = FALSE /\ competitorUsed = FALSE
    /\ alias = "old" /\ undo = "idle" /\ cleared = FALSE
    /\ replicated = InitiallyReplicated /\ enabledReplication = FALSE
    /\ running = TRUE /\ faults = 0 /\ compensated = FALSE

\* Each side publishes durable intent before probing the other's durable
\* record. A later intent must therefore see the already-admitted work.
ResizeAnnounce ==
    /\ running /\ ~resizeUsed
    /\ resize' = "announced" /\ resizeUsed' = TRUE
    /\ UNCHANGED <<competitor, competitorUsed, alias, undo, cleared, replicated,
                   enabledReplication, running, faults, compensated>>

ResizeAdmit ==
    /\ running /\ resize = "announced"
    /\ competitor \notin {"announced", "active"}
    /\ resize' = "active"
    /\ UNCHANGED <<competitor, resizeUsed, competitorUsed, alias, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

ResizeRefuse ==
    /\ running /\ resize = "announced"
    /\ competitor \in {"announced", "active"}
    /\ resize' = "done"
    /\ UNCHANGED <<competitor, resizeUsed, competitorUsed, alias, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

CompetitorAnnounce ==
    /\ running /\ ~competitorUsed
    /\ competitor' = "announced" /\ competitorUsed' = TRUE
    /\ UNCHANGED <<resize, resizeUsed, alias, undo, cleared, replicated,
                   enabledReplication, running, faults, compensated>>

CompetitorAdmit ==
    /\ running /\ competitor = "announced"
    /\ resize \notin {"announced", "active"} /\ undo \in {"idle", "done"}
    /\ competitor' = "active"
    /\ UNCHANGED <<resize, resizeUsed, competitorUsed, alias, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

CompetitorRefuse ==
    /\ running /\ competitor = "announced"
    /\ resize \in {"announced", "active"} \/ undo \notin {"idle", "done"}
    /\ competitor' = "done"
    /\ UNCHANGED <<resize, resizeUsed, competitorUsed, alias, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

ResizeFinish ==
    /\ running /\ resize = "active"
    /\ alias' = "new" /\ resize' = "done"
    /\ UNCHANGED <<competitor, resizeUsed, competitorUsed, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

CompetitorFinish ==
    /\ running /\ competitor = "active"
    /\ competitor' = "done"
    /\ UNCHANGED <<resize, resizeUsed, competitorUsed, alias, undo, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

\* A request is acknowledged separately from its replication decision.
UndoRequest ==
    /\ running /\ resize = "done" /\ alias = "new" /\ undo = "idle"
    /\ undo' = "checking"
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

UndoClearance ==
    /\ running /\ undo = "checking" /\ ~replicated
    /\ cleared' = TRUE /\ undo' = "armed"
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias,
                   replicated, enabledReplication, running, faults, compensated>>

UndoRefuse ==
    /\ running /\ undo = "checking" /\ replicated
    /\ undo' = "done"
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

\* Replication may become enabled after clearance. Once compensation may
\* have begun, the durable clearance must let retry finish rather than strand
\* an armed serving copy. This models policy at that seam, not shipping.
EnableReplication ==
    /\ ~enabledReplication /\ ~replicated
    /\ replicated' = TRUE /\ enabledReplication' = TRUE
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, undo,
                   cleared, running, faults, compensated>>

UndoCompensate ==
    /\ running /\ undo = "armed" /\ cleared
    /\ compensated' = TRUE /\ undo' = "swapping"
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, cleared,
                   replicated, enabledReplication, running, faults>>

UndoFinish ==
    /\ running /\ undo = "swapping" /\ cleared
    /\ alias' = "old" /\ undo' = "done"
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, cleared,
                   replicated, enabledReplication, running, faults, compensated>>

Crash ==
    /\ running /\ faults < MaxFaults
    /\ resize \in {"announced", "active"} \/ competitor \in {"announced", "active"}
        \/ undo \in {"checking", "armed", "swapping"}
    /\ running' = FALSE /\ faults' = faults + 1
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, undo,
                   cleared, replicated, enabledReplication, compensated>>

Retry ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED <<resize, competitor, resizeUsed, competitorUsed, alias, undo,
                   cleared, replicated, enabledReplication, faults, compensated>>

Stutter == UNCHANGED vars

Next ==
    \/ ResizeAnnounce
    \/ ResizeAdmit
    \/ ResizeRefuse
    \/ CompetitorAnnounce
    \/ CompetitorAdmit
    \/ CompetitorRefuse
    \/ ResizeFinish
    \/ CompetitorFinish
    \/ UndoRequest
    \/ UndoClearance
    \/ UndoRefuse
    \/ EnableReplication
    \/ UndoCompensate
    \/ UndoFinish
    \/ Crash
    \/ Retry
    \/ Stutter

Progress ==
    \/ ResizeAdmit \/ ResizeRefuse \/ CompetitorAdmit \/ CompetitorRefuse
    \/ ResizeFinish \/ CompetitorFinish
    \/ UndoClearance \/ UndoRefuse \/ UndoCompensate \/ UndoFinish \/ Retry

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
    /\ resize \in Phases /\ competitor \in Phases
    /\ resizeUsed \in BOOLEAN /\ competitorUsed \in BOOLEAN
    /\ alias \in {"old", "new"}
    /\ undo \in {"idle", "checking", "armed", "swapping", "done"}
    /\ cleared \in BOOLEAN /\ replicated \in BOOLEAN
    /\ enabledReplication \in BOOLEAN /\ running \in BOOLEAN
    /\ faults \in 0..MaxFaults /\ compensated \in BOOLEAN

ExclusiveWork ==
    ~(resize = "active" /\ competitor = "active")

UndoExcludesCompetitor ==
    [][(undo \in {"checking", "armed", "swapping"})
          => ~(competitor # "active" /\ competitor' = "active")]_vars

ClearanceBeforeCompensation ==
    (compensated \/ undo \in {"armed", "swapping"}) => cleared

RefusedUndoLeavesAlias ==
    undo = "done" /\ ~cleared => alias = "new"

ClearanceWasUnreplicated ==
    [][(~cleared /\ cleared') => ~replicated]_vars

DurableIntentSurvives ==
    [][(~running \/ ~running') => /\ resize' = resize /\ competitor' = competitor
                                 /\ undo' = undo /\ cleared' = cleared]_vars

AdmissionsSettle ==
    (resize = "announced" \/ competitor = "announced")
        ~> (resize \notin {"announced", "active"} /\ competitor \notin {"announced", "active"})

UndoSettles == (undo \in {"checking", "armed", "swapping"}) ~> (undo = "done")
=============================================================================
