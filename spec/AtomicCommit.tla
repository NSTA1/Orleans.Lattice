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
(*  forgotten[t]  whether the saga's post-fan-out cleanup has retired the  *)
(*                registry row (ForgetAsync, the lazy PruneExpired purge   *)
(*                behind it, or the zero-retention branch). This is        *)
(*                deliberately a separate variable rather than a write     *)
(*                back to decision[t]: retiring the row does not un-commit *)
(*                the saga, it removes the tree's ability to answer for    *)
(*                it. decision[t] therefore keeps the outcome, and         *)
(*                RegistryView(t) is what a reader actually resolves - the *)
(*                same split the Coyote model makes between its recorded   *)
(*                outcome and a live read of the registry core.            *)
(*  revision      monotonic registry revision (DecisionsRevision), bumped  *)
(*                on every decision write and on the cleanup that retires  *)
(*                one (both change the surface a reader probes).           *)
(***************************************************************************)
VARIABLES phase, vote, decision, terminal, pend, orphanDone, forgotten, revision

vars == <<phase, vote, decision, terminal, pend, orphanDone, forgotten, revision>>

Phases == {"init", "prepared", "committing", "aborting", "done"}

TypeOK ==
   /\ phase \in [Txns -> Phases]
   /\ vote \in [Txns -> [Keys -> {"none", "ack", "nack"}]]
   /\ decision \in [Txns -> {"inflight", "committed", "aborted"}]
   /\ terminal \in [Txns -> [Keys -> {"none", "commit", "abort"}]]
   /\ pend \in [Txns -> [Keys -> {"none", "pending"}]]
   /\ orphanDone \in [Txns -> [Keys -> BOOLEAN]]
   /\ forgotten \in [Txns -> BOOLEAN]
   /\ revision \in 0..(2 * Cardinality(Txns))

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

(***************************************************************************)
(* What a reader's GetStatusAsync actually resolves for the saga. A txid    *)
(* absent from the registry view is InFlight (:104 below), so once the      *)
(* cleanup has retired the row the view reverts to "inflight" even though   *)
(* the saga's outcome (decision[t]) is unchanged. Only the gate consults    *)
(* this; the invariants below are stated against decision[t], the outcome.  *)
(***************************************************************************)
RegistryView(t) == IF forgotten[t] THEN "inflight" ELSE decision[t]

SurfaceViaGate(t, k) == RegistryView(t) = "committed" /\ ~AlreadyTerminal(t, k)

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
    /\ forgotten = [t \in Txns |-> FALSE]
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
    /\ UNCHANGED <<decision, terminal, orphanDone, forgotten, revision>>

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
    /\ UNCHANGED <<vote, terminal, pend, orphanDone, forgotten>>

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
    /\ UNCHANGED <<vote, decision, orphanDone, forgotten, revision>>

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
    /\ UNCHANGED <<phase, vote, decision, terminal, orphanDone, forgotten, revision>>

OrphanDrain(t, k) ==
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "pending"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "none"]
    /\ orphanDone' = [orphanDone EXCEPT ![t][k] = TRUE]
    /\ UNCHANGED <<phase, vote, decision, terminal, forgotten, revision>>

(***************************************************************************)
(* ForgetDecision(t): the saga's post-fan-out cleanup retires the registry  *)
(* row (ITxRegistryGrain.ForgetAsync, the lazy PruneExpired purge behind    *)
(* it, and the TxDecisionRetention = 0 branch that drops the row outright). *)
(* After it the txid resolves to "inflight" again, because a txid absent    *)
(* from the registry view is InFlight.                                     *)
(*                                                                         *)
(* The enabling conditions are the whole safety argument, and they are      *)
(* production's, not a modelling convenience: ForgetAsync's own contract    *)
(* states that after the call the txid resolves to in-flight "by which      *)
(* point no leaf has the txid in its pending bucket anymore, so that        *)
(* observation is consistent with the absence of any pending mutation".     *)
(* Every written key must therefore have applied its terminal and hold no   *)
(* pending bucket - including an orphan a shadow-forward sweep re-installed *)
(* after the fan-out, which is exactly what the tombstone retention window  *)
(* exists to cover. Drop those conjuncts and DecisionDurability fails: a    *)
(* leaf that has not drained resolves the retired txid to in-flight, the    *)
(* gate falls its value through to the pre-saga value, and a committed      *)
(* saga becomes invisible. That is the unset the property forbids, and it   *)
(* is reachable without any flip, any late terminal, or any re-delivery.    *)
(*                                                                         *)
(* WHAT THIS ACTION IS NOT, AND WHY THAT MATTERS. It models the *ordered*   *)
(* cleanup path only. The *unordered* one - a retention window aging out    *)
(* and masking a decision row while a prepared bucket is still live - is a  *)
(* different event with no ordering guarantee behind it, and it is #2320's  *)
(* to add, deliberately not added here. Adding it is known to violate       *)
(* MonotonicVisibility and VisibilityMatchesDecision, which is the finding  *)
(* #2320 exists to record; this action does not, because its conjuncts make *)
(* every observation independent of the decision before it fires. Reading   *)
(* the guarded action as evidence that the unguarded hazard is absent would *)
(* invert both results, so do not treat #2320 as discharged by this.        *)
(***************************************************************************)
ForgetDecision(t) ==
    /\ decision[t] # "inflight"
    /\ ~forgotten[t]
    /\ \A k \in Written(t) : terminal[t][k] # "none"
    /\ \A k \in Written(t) : pend[t][k] = "none"
    /\ forgotten' = [forgotten EXCEPT ![t] = TRUE]
    /\ revision' = revision + 1
    /\ UNCHANGED <<phase, vote, decision, terminal, pend, orphanDone>>

(***************************************************************************)
(* A fully quiesced terminal state has an explicit stuttering successor so *)
(* natural termination is not reported as a deadlock. Before full          *)
(* quiescence some real action is always enabled.                          *)
(***************************************************************************)
FullyQuiesced ==
    /\ \A t \in Txns : phase[t] = "done"
    /\ \A t \in Txns : forgotten[t]
    /\ \A t \in Txns : \A k \in Written(t) : pend[t][k] = "none" /\ orphanDone[t][k]

Stutter == FullyQuiesced /\ UNCHANGED vars

Next ==
    \/ \E t \in Txns : PrepareTx(t)
    \/ \E t \in Txns : DecideTx(t)
    \/ \E t \in Txns : \E k \in Keys : BroadcastStep(t, k)
    \/ \E t \in Txns : \E k \in Keys : ShadowForwardOrphan(t, k)
    \/ \E t \in Txns : \E k \in Keys : OrphanDrain(t, k)
    \/ \E t \in Txns : ForgetDecision(t)
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

\* Decision durability: once the registry records a terminal decision it never
\* flips to the other terminal, and its row is never retired while a
\* participant still holds an undrained prepared bucket. The first two
\* conjuncts are the original formula, unchanged and unscoped. The third is
\* the unset half: absent is not "committed", so a reading that only forbids
\* a flip is strictly weaker than what durability has to mean. Retiring the
\* row is not itself a violation - it is ordinary cleanup, and ForgetDecision
\* performs it on every run - but retiring it early loses a decision a reader
\* still depends on just as effectively as flipping it.
DecisionDurability ==
    [][ \A t \in Txns :
          /\ (decision[t] = "committed" => decision'[t] = "committed")
          /\ (decision[t] = "aborted"   => decision'[t] = "aborted")
          /\ (   /\ decision[t] # "inflight"
                 /\ ~forgotten[t]
                 /\ \E k \in Written(t) : terminal[t][k] = "none"
              => ~forgotten'[t] ) ]_vars

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
