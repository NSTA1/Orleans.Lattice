----------------------- MODULE ReplicationAdmission -----------------------
(***************************************************************************)
(* The replication receiver's applier gate (issue #6053, epic #6046): what *)
(* it does with a run that reaches it from any receive path, under any    *)
(* peer behaviour. A run is dropped when its tree is not enrolled here,    *)
(* dead-lettered when the locally resolved mode does not admit its write   *)
(* class, and otherwise applied; inbound per-peer contact is recorded only *)
(* for an admitted run, under a row cap. The callers that reach the        *)
(* applier are admitted by ReplicationOriginBinding in this directory.     *)
(*                                                                         *)
(* The merge mode is an open set of write classes: Admits(mode, class) is  *)
(* an abstract operator, bound in the cfg to ModeAdmits (a mode admits     *)
(* exactly its own class). A future tree whose resolved mode admits only   *)
(* decided consensus writes inherits ModeReResolvedLocally unchanged.      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANT Admits(_, _)

\* "t1" is enrolled and its mode changes; "t2" is enrolled with a fixed mode;
\* "rogue" is a peer-chosen tree id this cluster never enrolls (a sys- tree,
\* another tenant's tree, or an arbitrary name).
Trees == {"t1", "t2", "rogue"}
Enrollable == {"t1", "t2"}
NotEnrolled == "none"
Modes == {"Lww", "Crdt"}
WriteClasses == {"Lww", "Crdt", "Decided"}
ModeAdmits(mode, cls) == mode = cls
RunOrigins == {"peerA", "stranger"}
\* The receive paths that funnel a run into the applier: a gRPC push, and a
\* replay (the causal-buffer drain or a dead-letter replay).
Paths == {"push", "replay"}
Cap == 1

VARIABLES enrolled, dlq, contact, obs
vars == <<enrolled, dlq, contact, obs>>

NoObs == [kind |-> "none"]
ApplyObsRecs ==
    [kind : {"apply"}, tree : Trees, cls : WriteClasses, origin : RunOrigins, path : Paths,
     mode : Modes \cup {NotEnrolled}, result : {"applied", "deadLettered", "dropped"}, recorded : BOOLEAN]

LocalMode(t) == IF t \in Enrollable THEN enrolled[t] ELSE NotEnrolled

-----------------------------------------------------------------------------
Init ==
    /\ enrolled = [t \in Enrollable |-> IF t = "t1" THEN "Lww" ELSE "Crdt"]
    /\ dlq = {}
    /\ contact = {}
    /\ obs = NoObs

\* A run of entries for tree t, of write class cls, from origin o, reaches the
\* applier along path p. The applier classifies it against the local
\* enrollment, then records inbound contact for an admitted run under the cap.
ApplyRun(t, cls, o, p) ==
    LET mode == LocalMode(t)
        result == IF mode = NotEnrolled THEN "dropped" ELSE IF Admits(mode, cls) THEN "applied" ELSE "deadLettered"
        record == result = "applied" /\ (<<t, o>> \in contact \/ Cardinality(contact) < Cap)
    IN /\ dlq' = IF result = "deadLettered" THEN dlq \cup {t} ELSE dlq
       /\ contact' = IF record THEN contact \cup {<<t, o>>} ELSE contact
       /\ obs' = [kind |-> "apply", tree |-> t, cls |-> cls, origin |-> o, path |-> p,
                  mode |-> mode, result |-> result, recorded |-> record]
       /\ UNCHANGED enrolled

\* The environment: an operator enrolls t1 under a mode, changes its mode, or
\* disables it.
SetEnrollment(mode) ==
    /\ mode # enrolled["t1"]
    /\ enrolled' = [enrolled EXCEPT !["t1"] = mode]
    /\ obs' = NoObs
    /\ UNCHANGED <<dlq, contact>>

Next ==
    \/ \E t \in Trees, cls \in WriteClasses, o \in RunOrigins, p \in Paths : ApplyRun(t, cls, o, p)
    \/ \E mode \in Modes \cup {NotEnrolled} : SetEnrollment(mode)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ enrolled \in [Enrollable -> Modes \cup {NotEnrolled}]
    /\ dlq \subseteq Trees
    /\ contact \subseteq Trees \X RunOrigins
    /\ obs \in {NoObs} \cup ApplyObsRecs

IsApply == obs.kind = "apply"

\* NonEnrolledDropped: a run for a tree not enrolled here is dropped, never
\* dead-lettered, whichever receive path delivers it, so dead-letter
\* activations stay within the enrolled trees under any peer behaviour.
NonEnrolledDropped ==
    /\ (IsApply /\ obs.mode = NotEnrolled) => obs.result = "dropped"
    /\ dlq \subseteq Enrollable

\* ModeReResolvedLocally: an enrolled tree's run is applied only when the
\* locally resolved mode admits its write class, and a disagreement is
\* dead-lettered. The wire mode is never trusted.
ModeReResolvedLocally ==
    /\ (IsApply /\ obs.result = "applied") => obs.mode # NotEnrolled /\ Admits(obs.mode, obs.cls)
    /\ (IsApply /\ obs.mode # NotEnrolled /\ ~Admits(obs.mode, obs.cls)) => obs.result = "deadLettered"

\* ContactOnlyWhenAdmitted: inbound per-peer contact is recorded only for an
\* admitted run, and the inbound rows stay capped.
ContactOnlyWhenAdmitted ==
    /\ (IsApply /\ obs.recorded) => obs.result = "applied"
    /\ Cardinality(contact) <= Cap
    /\ \A r \in contact : r[1] \in Enrollable
=============================================================================
