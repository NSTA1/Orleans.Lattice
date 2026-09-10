------------------- MODULE AtomicCommitDecisionExpiry -------------------
(***************************************************************************)
(* MUTANT of AtomicCommit.tla. Its whole purpose is to FAIL.               *)
(*                                                                         *)
(* Paired property: MonotonicVisibility.                                   *)
(* Expected outcome: VIOLATED, at depth 4.                                 *)
(*                                                                         *)
(* A green model check proves nothing until you have shown it can go red.  *)
(* Every property in AtomicCommit.cfg is meant to ship with a named        *)
(* mutation under which it fires, checked in as a cfg beside the mutant, so *)
(* that CI requires the violation rather than merely permitting it. This   *)
(* module is the first such pair; see README.md in this directory.         *)
(*                                                                         *)
(* THE MUTATION, in full (issue #2320). The base specification cannot      *)
(* express the production hazard of issue #2318 at all, because no         *)
(* variable stands between the recorded decision and the reader: in the    *)
(* base module `decision[t]` IS the registry, a total function assigned    *)
(* once and read directly by SurfaceViaGate, and a registry cannot         *)
(* misreport itself when it is the variable being read. In production the  *)
(* two are distinct - TxRegistryGrain.cs:428-435 masks an aged-out         *)
(* decision AHEAD of the Decisions lookup at :436, so the decision record  *)
(* is still present and only the REPORTED status lies.                     *)
(*                                                                         *)
(* So the mutation interposes exactly one variable:                        *)
(*                                                                         *)
(*   1. `expired`, a per-saga flag, added to the state.                    *)
(*   2. `ObservedDecision(t)`, the reported status, which diverges from    *)
(*      the stored `decision[t]` once the flag is set.                     *)
(*   3. `SurfaceViaGate` redirected to read the reported status instead of *)
(*      the stored one. This is the whole of the behavioural change; the   *)
(*      gate rule itself is untouched.                                     *)
(*   4. `DecisionExpire(t)`, a five-line unfair action that sets the flag. *)
(*                                                                         *)
(* Everything else is AtomicCommit.tla verbatim, except that each action's *)
(* UNCHANGED tuple grows the new variable.                                 *)
(*                                                                         *)
(* NOTE WHAT IS *NOT* HERE. No clock, no timeout, no TTL constant, and no  *)
(* failure action. A clock was never required: MonotonicVisibility is an   *)
(* action property, so it constrains consecutive state PAIRS and is        *)
(* structurally incapable of observing a duration. It is sensitive only to *)
(* the ORDER of the divergence event, and a nondeterministic action        *)
(* explores every ordering - strictly stronger than any chosen timeout.    *)
(* Renaming `expired` to `unreachable` would model the failure-triggered   *)
(* route (a swallowed dial failure, with no expiry involved at all) with   *)
(* the identical five lines.                                               *)
(*                                                                         *)
(* Nor does the counterexample need a failure action: the violating trace  *)
(* is Init -> PrepareTx -> DecideTx -> DecisionExpire, because the base    *)
(* specification already reaches "decided, terminal not yet delivered" as  *)
(* an ordinary transient state, and safety fires there regardless of       *)
(* whether fairness would eventually deliver the terminal.                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS t1, t2, k1, k2, k3

Txns == {t1, t2}
Keys == {k1, k2, k3}
TxWrites == (t1 :> {k1, k2}) @@ (t2 :> {k2, k3})

Written(t) == TxWrites[t]

(***************************************************************************)
(* State. As AtomicCommit.tla, plus:                                       *)
(*                                                                         *)
(*  expired[t]    whether the registry has stopped REPORTING saga t's      *)
(*                recorded decision. The decision itself is UNCHANGED by   *)
(*                the action that sets this, matching production, whose    *)
(*                own comment confirms the decision "is not physically     *)
(*                purged here."                                            *)
(***************************************************************************)
VARIABLES phase, vote, decision, terminal, pend, orphanDone, revision, expired

vars == <<phase, vote, decision, terminal, pend, orphanDone, revision, expired>>

Phases == {"init", "prepared", "committing", "aborting", "done"}

TypeOK ==
    /\ phase \in [Txns -> Phases]
    /\ vote \in [Txns -> [Keys -> {"none", "ack", "nack"}]]
    /\ decision \in [Txns -> {"inflight", "committed", "aborted"}]
    /\ terminal \in [Txns -> [Keys -> {"none", "commit", "abort"}]]
    /\ pend \in [Txns -> [Keys -> {"none", "pending"}]]
    /\ orphanDone \in [Txns -> [Keys -> BOOLEAN]]
    /\ revision \in 0..Cardinality(Txns)
    /\ expired \in [Txns -> BOOLEAN]

AlreadyTerminal(t, k)  == terminal[t][k] # "none"
ProjectedPrepared(t, k) == terminal[t][k] = "commit"

(***************************************************************************)
(* THE MUTATION (1 of 2): the reported status, interposed between the      *)
(* stored decision and the reader. In AtomicCommit.tla SurfaceViaGate      *)
(* reads `decision[t]` directly; here it reads what the registry SAYS.     *)
(***************************************************************************)
ObservedDecision(t) == IF expired[t] THEN "inflight" ELSE decision[t]

SurfaceViaGate(t, k) == ObservedDecision(t) = "committed" /\ ~AlreadyTerminal(t, k)

ObservedPrepared(t, k) ==
    IF pend[t][k] = "pending"
    THEN IF SurfaceViaGate(t, k) THEN TRUE ELSE ProjectedPrepared(t, k)
    ELSE ProjectedPrepared(t, k)

Init ==
    /\ phase = [t \in Txns |-> "init"]
    /\ vote = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ decision = [t \in Txns |-> "inflight"]
    /\ terminal = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ pend = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ orphanDone = [t \in Txns |-> [k \in Keys |-> FALSE]]
    /\ revision = 0
    /\ expired = [t \in Txns |-> FALSE]

PrepareTx(t) ==
    /\ phase[t] = "init"
    /\ \E ackSet \in SUBSET Written(t) :
         vote' = [vote EXCEPT ![t] =
                    [k \in Keys |-> IF k \in Written(t)
                                    THEN (IF k \in ackSet THEN "ack" ELSE "nack")
                                    ELSE "none"]]
    /\ pend' = [pend EXCEPT ![t] =
                  [k \in Keys |-> IF k \in Written(t) THEN "pending" ELSE pend[t][k]]]
    /\ phase' = [phase EXCEPT ![t] = "prepared"]
    /\ UNCHANGED <<decision, terminal, orphanDone, revision, expired>>

AllAcked(t) == \A k \in Written(t) : vote[t][k] = "ack"

DecideTx(t) ==
    /\ phase[t] = "prepared"
    /\ decision' = [decision EXCEPT ![t] = IF AllAcked(t) THEN "committed" ELSE "aborted"]
    /\ phase' = [phase EXCEPT ![t] = IF AllAcked(t) THEN "committing" ELSE "aborting"]
    /\ revision' = revision + 1
    /\ UNCHANGED <<vote, terminal, pend, orphanDone, expired>>

BroadcastStep(t, k) ==
    /\ k \in Written(t)
    /\ terminal[t][k] = "none"
    /\ phase[t] \in {"committing", "aborting"}
    /\ LET kind == IF phase[t] = "committing" THEN "commit" ELSE "abort"
           nterm == [terminal[t] EXCEPT ![k] = kind]
           allDone == \A j \in Written(t) : nterm[j] # "none"
       IN /\ terminal' = [terminal EXCEPT ![t] = nterm]
          /\ pend' = [pend EXCEPT ![t][k] = "none"]
          /\ phase' = [phase EXCEPT ![t] = IF allDone THEN "done" ELSE phase[t]]
    /\ UNCHANGED <<vote, decision, orphanDone, revision, expired>>

ShadowForwardOrphan(t, k) ==
    /\ phase[t] = "done"
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "none"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "pending"]
    /\ UNCHANGED <<phase, vote, decision, terminal, orphanDone, revision, expired>>

OrphanDrain(t, k) ==
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "pending"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "none"]
    /\ orphanDone' = [orphanDone EXCEPT ![t][k] = TRUE]
    /\ UNCHANGED <<phase, vote, decision, terminal, revision, expired>>

(***************************************************************************)
(* THE MUTATION (2 of 2): the registry stops reporting a recorded          *)
(* decision. `decision` is deliberately UNCHANGED - the record survives    *)
(* and only its reported status diverges, which is exactly the production  *)
(* shape. Deliberately NOT fair: the hazard is a safety violation, so it   *)
(* must fire on an ordinary transient state and must not need a fairness   *)
(* assumption to be reached.                                               *)
(***************************************************************************)
DecisionExpire(t) ==
    /\ decision[t] # "inflight"
    /\ ~expired[t]
    /\ expired' = [expired EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<phase, vote, decision, terminal, pend, orphanDone, revision>>

FullyQuiesced ==
    /\ \A t \in Txns : phase[t] = "done"
    /\ \A t \in Txns : \A k \in Written(t) : pend[t][k] = "none" /\ orphanDone[t][k]

Stutter == FullyQuiesced /\ UNCHANGED vars

Next ==
    \/ \E t \in Txns : PrepareTx(t)
    \/ \E t \in Txns : DecideTx(t)
    \/ \E t \in Txns : \E k \in Keys : BroadcastStep(t, k)
    \/ \E t \in Txns : \E k \in Keys : ShadowForwardOrphan(t, k)
    \/ \E t \in Txns : \E k \in Keys : OrphanDrain(t, k)
    \/ \E t \in Txns : DecisionExpire(t)
    \/ Stutter

TxProgress(t) ==
    \/ PrepareTx(t)
    \/ DecideTx(t)
    \/ \E k \in Keys : BroadcastStep(t, k)

Spec == Init /\ [][Next]_vars /\ \A t \in Txns : WF_vars(TxProgress(t))

(***************************************************************************)
(* The paired property, verbatim from AtomicCommit.tla:287-290. NOTHING    *)
(* NEW HAS TO BE WRITTEN to detect the hazard - the property was already   *)
(* live, correctly worded and load-bearing, and it fires the instant the   *)
(* state becomes expressible.                                              *)
(***************************************************************************)

\* Monotonic visibility: once a key is post-saga-visible it stays visible
\* (a committed value never reverts to pre-saga, even across a reshard).
MonotonicVisibility ==
    [][ \A t \in Txns : \A k \in Written(t) :
          ObservedPrepared(t, k) => ObservedPrepared(t, k)' ]_vars

=============================================================================
