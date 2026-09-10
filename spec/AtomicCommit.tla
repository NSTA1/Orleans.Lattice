-------------------------- MODULE AtomicCommit --------------------------
(***************************************************************************)
(* An abstract TLA+ specification of the Orleans.Lattice distributed       *)
(* atomic-commit protocol: the multi-leaf prepare / commit / abort saga,   *)
(* the per-tree transaction-registry decision, and reader visibility.      *)
(*                                                                         *)
(* This models the protocol DESIGN, not the code. It is deliberately       *)
(* abstract: keys, participant leaves, and a transaction status, with no   *)
(* serialization, no timers, no HLC, no WAL. It exists so TLC can check    *)
(* the safety and liveness properties of the protocol exhaustively over    *)
(* small bounded instances, catching design-level defects that a           *)
(* code-shaped model would not surface. See Refinement.md for the mapping  *)
(* from each variable / action here to its counterpart in the extracted    *)
(* Coyote cores (AtomicVisibilityGate / TxDecisionView and the coordinator *)
(* / registry / orphan-guard pieces landing across level-C phases 1-4).    *)
(*                                                                         *)
(* Level-C epic #1588, Phase 7 (#1596), lever (c).                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* Model instance. The five model values below are supplied by            *)
(* AtomicCommit.cfg. Txns is the set of concurrent sagas; Keys is the      *)
(* keyspace; each participant leaf is identified with the key it holds.    *)
(* TxWrites[t] is the fixed set of keys saga t writes (its participant     *)
(* set). The default instance is 2 sagas over 3 keys, each saga touching   *)
(* 2 keys and overlapping on k2 - i.e. 2 participants per saga, 2          *)
(* concurrent sagas, 3 keys - plus a bounded reshard orphan step per key.  *)
(***************************************************************************)
CONSTANTS t1, t2, k1, k2, k3

Txns == {t1, t2}
Keys == {k1, k2, k3}
TxWrites == (t1 :> {k1, k2}) @@ (t2 :> {k2, k3})

Written(t) == TxWrites[t]

(***************************************************************************)
(* State.                                                                  *)
(*                                                                         *)
(*  phase[t]      coordinator saga phase (AtomicWriteGrain / AtomicWritePhase). *)
(*  vote[t][k]    participant leaf's prepare vote (ack = prepared ok,      *)
(*                nack = precondition / write failure).                    *)
(*  decision[t]   the per-tree TxRegistry recorded outcome. This single    *)
(*                variable is the tree-wide linearization point            *)
(*                (TxRegistryGrain.Decisions / TxDecisionView).            *)
(*  terminal[t][k] the terminal mark a participant leaf has applied for    *)
(*                this saga (none = not yet broadcast). terminal # "none"  *)
(*                is the leaf's alreadyTerminal / orphan-guard flag.       *)
(*  pend[t][k]    whether a prepared (hidden) pending bucket currently     *)
(*                shadows the key on the leaf (leaf _pendingTx).           *)
(*  orphanDone[t][k]  a used-once budget so a reshard shadow-forward       *)
(*                orphan is modelled at most once per key (keeps the state *)
(*                space finite).                                           *)
(*  revision      monotonic registry revision (DecisionsRevision), bumped  *)
(*                on every decision write.                                 *)
(***************************************************************************)
VARIABLES phase, vote, decision, terminal, pend, orphanDone, revision

vars == <<phase, vote, decision, terminal, pend, orphanDone, revision>>

Phases == {"init", "prepared", "committing", "aborting", "done"}

TypeOK ==
    /\ phase \in [Txns -> Phases]
    /\ vote \in [Txns -> [Keys -> {"none", "ack", "nack"}]]
    /\ decision \in [Txns -> {"inflight", "committed", "aborted"}]
    /\ terminal \in [Txns -> [Keys -> {"none", "commit", "abort"}]]
    /\ pend \in [Txns -> [Keys -> {"none", "pending"}]]
    /\ orphanDone \in [Txns -> [Keys -> BOOLEAN]]
    /\ revision \in 0..Cardinality(Txns)

(***************************************************************************)
(* Reader visibility - the per-key gate.                                   *)
(*                                                                         *)
(* AlreadyTerminal(t,k) mirrors AtomicVisibilityGate's alreadyTerminal     *)
(* input; ProjectedPrepared(t,k) is the leaf's materialised (visible)      *)
(* projection for the key under this saga. Gate(t,k) is the exact rule of  *)
(* AtomicVisibilityGate.ResolveKey (minus the tombstone/TTL "hidden" case, *)
(* which the issue puts out of scope for the abstract model): a pending    *)
(* bucket surfaces its prepared value iff the saga committed and this leaf *)
(* has not already applied a terminal (so a late shadow-forward orphan     *)
(* bucket falls through to the authoritative projection instead of         *)
(* shadowing it).                                                          *)
(*                                                                         *)
(* ObservedPrepared(t,k) is what a single snapshot read of the key         *)
(* resolves to: TRUE = the post-saga (prepared) value, FALSE = the         *)
(* pre-saga value. Resolving every key of a fan-out against the SAME       *)
(* decision[t] is the linearization that makes a saga all-or-nothing       *)
(* visible (TxDecisionView).                                               *)
(***************************************************************************)
AlreadyTerminal(t, k)  == terminal[t][k] # "none"
ProjectedPrepared(t, k) == terminal[t][k] = "commit"

SurfaceViaGate(t, k) == decision[t] = "committed" /\ ~AlreadyTerminal(t, k)

ObservedPrepared(t, k) ==
    IF pend[t][k] = "pending"
    THEN IF SurfaceViaGate(t, k) THEN TRUE ELSE ProjectedPrepared(t, k)
    ELSE ProjectedPrepared(t, k)

(***************************************************************************)
(* Initial state: nothing started, every txid resolves to InFlight (the    *)
(* strict-isolation default: a txid absent from the registry view is       *)
(* InFlight).                                                              *)
(***************************************************************************)
Init ==
    /\ phase = [t \in Txns |-> "init"]
    /\ vote = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ decision = [t \in Txns |-> "inflight"]
    /\ terminal = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ pend = [t \in Txns |-> [k \in Keys |-> "none"]]
    /\ orphanDone = [t \in Txns |-> [k \in Keys |-> FALSE]]
    /\ revision = 0

(***************************************************************************)
(* PrepareTx(t): the coordinator's prepare fan-out. Every written key gets *)
(* a hidden pending bucket, and each participant votes ack or nack         *)
(* (nondeterministic: models a per-key precondition-guard miss or write    *)
(* failure). All prepared buckets are invisible to readers until the       *)
(* registry decision is recorded, so the whole fan-out is one action.      *)
(***************************************************************************)
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
    /\ UNCHANGED <<decision, terminal, orphanDone, revision>>

AllAcked(t) == \A k \in Written(t) : vote[t][k] = "ack"

(***************************************************************************)
(* DecideTx(t): the coordinator records the single terminal decision in    *)
(* the per-tree registry BEFORE any per-leaf terminal is broadcast. Commit *)
(* iff every participant acked; otherwise abort. This is the commit-side / *)
(* abort-side linearization point (RecordTerminalDecisionAsync ->          *)
(* MarkCommittedAsync / MarkAbortedAsync). The revision counter bumps with *)
(* the decision write.                                                     *)
(***************************************************************************)
DecideTx(t) ==
    /\ phase[t] = "prepared"
    /\ decision' = [decision EXCEPT ![t] = IF AllAcked(t) THEN "committed" ELSE "aborted"]
    /\ phase' = [phase EXCEPT ![t] = IF AllAcked(t) THEN "committing" ELSE "aborting"]
    /\ revision' = revision + 1
    /\ UNCHANGED <<vote, terminal, pend, orphanDone>>

(***************************************************************************)
(* BroadcastStep(t,k): one participant leaf applies the saga's terminal    *)
(* (BroadcastTerminalsAsync fan-out, one leaf at a time - the interleaving *)
(* that a split-view bug would exploit). Applying the terminal consumes    *)
(* the pending bucket and sets the leaf's alreadyTerminal flag. When the   *)
(* last written key is applied the saga is done.                           *)
(***************************************************************************)
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
    /\ UNCHANGED <<vote, decision, orphanDone, revision>>

(***************************************************************************)
(* Reshard / migration interplay (abstract, the #1584 class at design      *)
(* level). After a saga is fully broadcast, an online shard-split sweep     *)
(* can shadow-forward a stale prepared write onto a leaf that has ALREADY   *)
(* applied the saga's terminal, re-installing a pending bucket. The orphan  *)
(* guard (Gate's AlreadyTerminal) makes this late bucket fall through to    *)
(* the authoritative projection rather than shadow it. OrphanDrain models   *)
(* the sweep's own post-sweep cleanup pass draining the orphan. The used-   *)
(* once orphanDone budget keeps the model finite.                          *)
(***************************************************************************)
ShadowForwardOrphan(t, k) ==
    /\ phase[t] = "done"
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "none"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "pending"]
    /\ UNCHANGED <<phase, vote, decision, terminal, orphanDone, revision>>

OrphanDrain(t, k) ==
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "pending"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "none"]
    /\ orphanDone' = [orphanDone EXCEPT ![t][k] = TRUE]
    /\ UNCHANGED <<phase, vote, decision, terminal, revision>>

(***************************************************************************)
(* A fully quiesced terminal state has an explicit stuttering successor so *)
(* natural termination is not reported as a deadlock. Before full          *)
(* quiescence some real action is always enabled.                          *)
(***************************************************************************)
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
    \/ Stutter

(***************************************************************************)
(* Fairness: each saga makes progress (prepare -> decide -> broadcast every *)
(* leaf) so every saga terminates. The reshard orphan / drain actions are  *)
(* deliberately NOT fair - they model an optional environment event, and   *)
(* every safety property must hold whether or not they fire.               *)
(***************************************************************************)
TxProgress(t) ==
    \/ PrepareTx(t)
    \/ DecideTx(t)
    \/ \E k \in Keys : BroadcastStep(t, k)

Spec == Init /\ [][Next]_vars /\ \A t \in Txns : WF_vars(TxProgress(t))

(***************************************************************************)
(* Safety invariants (the property catalogue, lever (b) / #1595).          *)
(***************************************************************************)

\* Atomicity / all-or-nothing visibility: within one saga every written
\* key resolves identically for a snapshot reader - never a split view.
AllOrNothing ==
    \A t \in Txns : \A a, b \in Written(t) : ObservedPrepared(t, a) = ObservedPrepared(t, b)

\* Sharpened form: a key is post-saga-visible for a reader exactly when the
\* tree-wide registry decision is committed. Implies AllOrNothing and
\* StrictIsolation; a broadcast-before-decision bug violates it.
VisibilityMatchesDecision ==
    \A t \in Txns : \A k \in Written(t) : ObservedPrepared(t, k) = (decision[t] = "committed")

\* Strict-isolation default: an in-flight or aborted saga is never surfaced
\* as committed to a reader.
StrictIsolation ==
    \A t \in Txns : \A k \in Written(t) : ObservedPrepared(t, k) => decision[t] = "committed"

\* Commit integrity: a committed decision implies every participant acked
\* its prepare; an aborted decision implies at least one nack.
CommitIntegrity ==
    /\ \A t \in Txns : decision[t] = "committed" => \A k \in Written(t) : vote[t][k] = "ack"
    /\ \A t \in Txns : decision[t] = "aborted"   => \E k \in Written(t) : vote[t][k] = "nack"

\* Linearized terminals: no leaf applies a commit terminal before the
\* registry recorded commit, nor an abort terminal before it recorded
\* abort. This is the load-bearing ordering invariant (decision-before-
\* broadcast).
LinearizedTerminals ==
    \A t \in Txns : \A k \in Written(t) :
        /\ terminal[t][k] = "commit" => decision[t] = "committed"
        /\ terminal[t][k] = "abort"  => decision[t] = "aborted"

\* No mixed terminals: a single saga never applies a commit terminal on one
\* leaf and an abort terminal on another (never both commit and abort).
NoMixedTerminals ==
    \A t \in Txns :
        ~(/\ \E a \in Written(t) : terminal[t][a] = "commit"
          /\ \E b \in Written(t) : terminal[t][b] = "abort")

(***************************************************************************)
(* Liveness / progress (temporal). DecisionDurability, MonotonicVisibility *)
(* and RevisionMonotonic are action (safety) properties expressed as       *)
(* box-of-action formulas; Termination and EveryCommittedKeyReadable need  *)
(* the fairness assumption in Spec.                                        *)
(***************************************************************************)

\* Decision durability: once terminal, the registry decision never flips.
DecisionDurability ==
    [][ \A t \in Txns :
          /\ (decision[t] = "committed" => decision'[t] = "committed")
          /\ (decision[t] = "aborted"   => decision'[t] = "aborted") ]_vars

\* Monotonic visibility: once a key is post-saga-visible it stays visible
\* (a committed value never reverts to pre-saga, even across a reshard).
MonotonicVisibility ==
    [][ \A t \in Txns : \A k \in Written(t) :
          ObservedPrepared(t, k) => ObservedPrepared(t, k)' ]_vars

\* The registry revision counter never decreases.
RevisionMonotonic == [][ revision' >= revision ]_vars

\* Every saga terminates.
Termination == \A t \in Txns : <>(phase[t] = "done")

\* Every committed saga's keys are eventually all readable at the post-saga
\* value.
EveryCommittedKeyReadable ==
    \A t \in Txns :
        (decision[t] = "committed") ~> (\A k \in Written(t) : ObservedPrepared(t, k))

=============================================================================
