---- MODULE BPlusReclaimRecovery ----
EXTENDS Naturals

CONSTANT MaxCrashes

VARIABLES linked, routed, backLinkCorrect, victimExists, retired, retirementGranted, pending, ready, crashes

vars ==
    <<linked, routed, backLinkCorrect, victimExists, retired, retirementGranted, pending, ready, crashes>>

TypeOK ==
    /\ linked \in BOOLEAN
    /\ routed \in BOOLEAN
    /\ backLinkCorrect \in BOOLEAN
    /\ victimExists \in BOOLEAN
    /\ retired \in BOOLEAN
    /\ retirementGranted \in BOOLEAN
    /\ pending \in {"none", "victim"}
    /\ ready \in BOOLEAN
    /\ crashes \in 0..MaxCrashes

RetirementGrantIsLatched == retirementGranted => retired

UnlinkedVictimHasMarker == ~linked /\ victimExists => pending = "victim"

NoRouteToClearedVictim == ~victimExists => ~routed

ClearedVictimHasRepairedBackLink == ~victimExists => backLinkCorrect

CompletedReclaimCleared == pending = "none" /\ ~linked => ~victimExists

Init ==
    /\ linked = TRUE
    /\ routed = TRUE
    /\ backLinkCorrect = TRUE
    /\ victimExists = TRUE
    /\ retired = FALSE
    /\ retirementGranted = FALSE
    /\ pending = "none"
    /\ ready = TRUE
    /\ crashes = 0

BeginRetirement ==
    /\ ready
    /\ linked
    /\ routed
    /\ victimExists
    /\ ~retirementGranted
    /\ retired' = TRUE
    /\ retirementGranted' = TRUE
    /\ UNCHANGED <<linked, routed, backLinkCorrect, victimExists, pending, ready, crashes>>

UnlinkAndRecord ==
    /\ ready
    /\ linked
    /\ retirementGranted
    /\ retired
    /\ pending = "none"
    /\ linked' = FALSE
    /\ backLinkCorrect' = FALSE
    /\ pending' = "victim"
    /\ UNCHANGED <<routed, victimExists, retired, retirementGranted, ready, crashes>>

RetireRoute ==
    /\ ready
    /\ pending = "victim"
    /\ ~linked
    /\ routed
    /\ routed' = FALSE
    /\ UNCHANGED <<linked, backLinkCorrect, victimExists, retired, retirementGranted, pending, ready, crashes>>

RepairBackLink ==
    /\ ready
    /\ pending = "victim"
    /\ ~linked
    /\ ~backLinkCorrect
    /\ backLinkCorrect' = TRUE
    /\ UNCHANGED <<linked, routed, victimExists, retired, retirementGranted, pending, ready, crashes>>

ClearVictim ==
    /\ ready
    /\ pending = "victim"
    /\ ~linked
    /\ ~routed
    /\ backLinkCorrect
    /\ victimExists
    /\ victimExists' = FALSE
    /\ UNCHANGED <<linked, routed, backLinkCorrect, retired, retirementGranted, pending, ready, crashes>>

Complete ==
    /\ ready
    /\ pending = "victim"
    /\ ~linked
    /\ ~routed
    /\ backLinkCorrect
    /\ ~victimExists
    /\ pending' = "none"
    /\ UNCHANGED <<linked, routed, backLinkCorrect, victimExists, retired, retirementGranted, ready, crashes>>

Crash ==
    /\ ready
    /\ crashes < MaxCrashes
    /\ ready' = FALSE
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<linked, routed, backLinkCorrect, victimExists, retired, retirementGranted, pending>>

Recover ==
    /\ ~ready
    /\ ready' = TRUE
    /\ UNCHANGED <<linked, routed, backLinkCorrect, victimExists, retired, retirementGranted, pending, crashes>>

Stutter == UNCHANGED vars

Next ==
    \/ BeginRetirement
    \/ UnlinkAndRecord
    \/ RetireRoute
    \/ RepairBackLink
    \/ ClearVictim
    \/ Complete
    \/ Crash
    \/ Recover
    \/ Stutter

PendingEventuallyCompletes == pending = "victim" ~> pending = "none"

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(RetireRoute)
    /\ WF_vars(RepairBackLink)
    /\ WF_vars(ClearVictim)
    /\ WF_vars(Complete)
    /\ WF_vars(Recover)
====
