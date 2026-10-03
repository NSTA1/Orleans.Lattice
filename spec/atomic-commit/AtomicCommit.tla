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
(* Coyote cores (AtomicVisibilityGate / TxDecisionView, and the            *)
(* SagaCoordinatorCore / TxRegistryDecisionCore / MigrationTerminalCore    *)
(* pieces extracted across level-C phases 1-4, which landed with this      *)
(* specification in #1597).                                                *)
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
(*  masked[t]     whether the registry is currently declining to report    *)
(*                the saga's outcome (TxStatus.Indeterminate): the         *)
(*                tombstone retention window has elapsed on a row that is  *)
(*                still stored, a snapshot pin has not yet re-exposed it,  *)
(*                or a delegated cross-tree coordinator cannot be dialled. *)
(*                Unlike forgotten[t] it carries no ordering guarantee     *)
(*                against the participants, nor against the decision - see *)
(*                RegistryMask below.                                      *)
(*  revision      monotonic registry revision (DecisionsRevision), bumped  *)
(*                on every decision write and on the cleanup that retires  *)
(*                one (both change the surface a reader probes).           *)
(***************************************************************************)
VARIABLES phase, vote, decision, terminal, pend, orphanDone, forgotten, masked, revision

vars == <<phase, vote, decision, terminal, pend, orphanDone, forgotten, masked, revision>>

Phases == {"init", "prepared", "committing", "aborting", "done"}

TypeOK ==
   /\ phase \in [Txns -> Phases]
   /\ vote \in [Txns -> [Keys -> {"none", "ack", "nack"}]]
   /\ decision \in [Txns -> {"inflight", "committed", "aborted"}]
   /\ terminal \in [Txns -> [Keys -> {"none", "commit", "abort"}]]
   /\ pend \in [Txns -> [Keys -> {"none", "pending"}]]
   /\ orphanDone \in [Txns -> [Keys -> BOOLEAN]]
   /\ forgotten \in [Txns -> BOOLEAN]
   /\ masked \in [Txns -> BOOLEAN]
   /\ revision \in 0..(2 * Cardinality(Txns))

(***************************************************************************)
(* Reader visibility - the per-key gate.                                   *)
(*                                                                         *)
(* AlreadyTerminal(t,k) mirrors AtomicVisibilityGate's alreadyTerminal     *)
(* input; ProjectedPrepared(t,k) is the leaf's materialised (visible)      *)
(* projection for the key under this saga. SurfaceViaGate(t,k) is the      *)
(* committed arm of AtomicVisibilityGate.ResolveKey (minus the             *)
(* tombstone/TTL "hidden" case on the prepared VALUE, which the issue puts *)
(* out of scope for the abstract model): a pending bucket surfaces its     *)
(* prepared value iff the saga committed and this leaf has not already     *)
(* applied a terminal (so a late shadow-forward orphan bucket falls        *)
(* through to the authoritative projection instead of shadowing it).      *)
(*                                                                         *)
(* Observed(t,k) is what a single snapshot read of the key resolves to,    *)
(* and it has three values, not two:                                       *)
(*   "post"   - the post-saga (prepared) value;                            *)
(*   "pre"    - the pre-saga value;                                        *)
(*   "hidden" - no value: the gate's Indeterminate arm, taken when the     *)
(*              registry declines to report the saga's outcome.            *)
(* "hidden" is not a value, and the properties below treat it as one at    *)
(* their peril: it asserts nothing about the saga, which is exactly why    *)
(* the gate prefers it to falling through. Reading it as "pre" is the      *)
(* defect AtomicVisibilityGate exists to refuse, so every property that    *)
(* constrains a pre-saga observation names "pre" explicitly.               *)
(*                                                                         *)
(* Resolving every key of a fan-out against the SAME registry view is the  *)
(* linearization that makes a saga all-or-nothing visible (TxDecisionView).*)
(***************************************************************************)
AlreadyTerminal(t, k)  == terminal[t][k] # "none"
ProjectedPrepared(t, k) == terminal[t][k] = "commit"

(***************************************************************************)
(* What a reader's GetStatusAsync actually resolves for the saga. A txid    *)
(* absent from the registry view is InFlight, so once the cleanup has       *)
(* retired the row the view reverts to "inflight" even though the saga's    *)
(* outcome (decision[t]) is unchanged. A saga whose outcome the registry    *)
(* declines to report - a stored row it has stopped reporting, or a         *)
(* delegated txid it cannot resolve - resolves to "indeterminate". Only the *)
(* gate consults this; the invariants below are stated against decision[t], *)
(* the outcome.                                                             *)
(***************************************************************************)
RegistryView(t) ==
    IF forgotten[t] THEN "inflight"
    ELSE IF masked[t] THEN "indeterminate"
    ELSE decision[t]

SurfaceViaGate(t, k) == RegistryView(t) = "committed" /\ ~AlreadyTerminal(t, k)

Projected(t, k) == IF ProjectedPrepared(t, k) THEN "post" ELSE "pre"

\* The Indeterminate arm is tested FIRST, ahead of the orphan guard, because
\* AtomicVisibilityGate.ResolveKey tests it first: under an outcome the
\* registry declined to report there is no basis to prefer the projection
\* over the prepared value, so neither is served.
Observed(t, k) ==
    IF pend[t][k] = "pending"
    THEN IF RegistryView(t) = "indeterminate" THEN "hidden"
         ELSE IF SurfaceViaGate(t, k) THEN "post"
         ELSE Projected(t, k)
    ELSE Projected(t, k)

ObservedPrepared(t, k) == Observed(t, k) = "post"

ObservedPreSaga(t, k) == Observed(t, k) = "pre"

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
    /\ masked = [t \in Txns |-> FALSE]
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
    /\ UNCHANGED <<decision, terminal, orphanDone, forgotten, masked, revision>>

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
    /\ UNCHANGED <<vote, terminal, pend, orphanDone, forgotten, masked>>

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
    /\ UNCHANGED <<vote, decision, orphanDone, forgotten, masked, revision>>

(***************************************************************************)
(* Reshard / migration interplay (abstract, the #1584 class at design      *)
(* level). After a saga is fully broadcast, an online shard-split sweep     *)
(* can shadow-forward a stale prepared write onto a leaf that has ALREADY   *)
(* applied the saga's terminal, re-installing a pending bucket. The orphan  *)
(* guard (Gate's AlreadyTerminal) makes this late bucket fall through to    *)
(* the authoritative projection rather than shadow it. OrphanDrain models  *)
(* the leaf discarding that orphan bucket because the saga's terminal has  *)
(* already landed there (MigrationTerminalCore's DiscardOrphan, applied by *)
(* BPlusLeafGrain.ApplyTxTerminalAsync). It is not the split coordinator's *)
(* post-sweep cleanup, which applies a first terminal where none has       *)
(* landed. The used-once orphanDone budget keeps the model finite.         *)
(***************************************************************************)
ShadowForwardOrphan(t, k) ==
    /\ phase[t] = "done"
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "none"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "pending"]
    /\ UNCHANGED <<phase, vote, decision, terminal, orphanDone, forgotten, masked, revision>>

OrphanDrain(t, k) ==
    /\ k \in Written(t)
    /\ terminal[t][k] # "none"
    /\ pend[t][k] = "pending"
    /\ ~orphanDone[t][k]
    /\ pend' = [pend EXCEPT ![t][k] = "none"]
    /\ orphanDone' = [orphanDone EXCEPT ![t][k] = TRUE]
    /\ UNCHANGED <<phase, vote, decision, terminal, forgotten, masked, revision>>

(***************************************************************************)
(* ForgetDecision(t): the saga's post-fan-out cleanup retires the registry  *)
(* row (ITxRegistryGrain.ForgetAsync, the lazy PruneExpired purge behind    *)
(* it, and the TxDecisionRetention = 0 branch that drops the row outright). *)
(* After it the txid resolves to "inflight" again, because a txid absent    *)
(* from the registry view is InFlight.                                     *)
(*                                                                         *)
(* The enabling conditions are the whole safety argument, and they are      *)
(* production's, not a modelling convenience: ForgetAsync's own contract   *)
(* is that it is "Called after every touched leaf has applied its          *)
(* terminal". The terminal conjunct states that argument. Drop BOTH        *)
(* conjuncts (DecisionDurabilityEarlyForget) and DecisionDurability fails: *)
(* a leaf that has not yet applied its terminal resolves the retired txid  *)
(* to in-flight, the gate falls its value through to the pre-saga value,   *)
(* and a committed saga becomes invisible. That is the unset the property  *)
(* forbids, and it is reachable without any flip, any late terminal, or    *)
(* any re-delivery.                                                        *)
(*                                                                         *)
(* Either conjunct alone suffices here, and TLC confirms both directions.  *)
(* Removing only the terminal conjunct changes nothing at all - the state  *)
(* graph is identical - because in this model a decided saga's key holds  *)
(* its bucket until its terminal lands and both orphan actions require the *)
(* terminal, so "no bucket" already implies "terminal applied". Removing   *)
(* only the pend conjunct leaves every invariant and property clean too:   *)
(* once every terminal is applied, the only bucket left is an orphan a     *)
(* shadow-forward re-installed, and the orphan guard makes it fall through *)
(* to the projection whether or not the row is retired. So the pend        *)
(* conjunct is not load-bearing. It stands for the tombstone retention     *)
(* window's intent - that the row outlives a late orphan - which           *)
(* production does not guarantee either; it is kept because it is         *)
(* harmless, and no conclusion about the retention window may rest on it.  *)
(*                                                                         *)
(* WHAT THIS ACTION IS NOT, AND WHY THAT MATTERS. It models the *ordered*   *)
(* cleanup path only. The *unordered* one - the registry declining to      *)
(* report a decision while a prepared bucket is still live - has no        *)
(* ordering guarantee behind it and is modelled separately, by             *)
(* RegistryMask below. This action passes every property because its       *)
(* conjuncts make every observation independent of the decision before it  *)
(* fires; reading that pass as evidence about the unordered event would    *)
(* invert what RegistryMask's mutation shows.                              *)
(***************************************************************************)
ForgetDecision(t) ==
    /\ decision[t] # "inflight"
    /\ ~forgotten[t]
    /\ \A k \in Written(t) : terminal[t][k] # "none"
    /\ \A k \in Written(t) : pend[t][k] = "none"
    /\ forgotten' = [forgotten EXCEPT ![t] = TRUE]
    /\ revision' = revision + 1
    /\ UNCHANGED <<phase, vote, decision, terminal, pend, orphanDone, masked>>

(***************************************************************************)
(* RegistryMask(t): the registry stops reporting a saga's outcome, or      *)
(* starts reporting it again. In production a stored row resolves to       *)
(* TxStatus.Indeterminate once TxDecisionRetention has elapsed on a        *)
(* tombstone that PruneExpired has not yet purged, and a snapshot pin       *)
(* covering that tombstone re-exposes it; both are toggles of one flag     *)
(* here. A delegated cross-tree txid whose coordinator cannot be dialled   *)
(* reports the same status (TxRegistryGrain.ReadStatusAsync ->             *)
(* ResolveAnyDelegatedAsync), with no clock involved and - unlike the      *)
(* tombstone - with NO local decision behind it: the dial fails whether or *)
(* not the coordinator has decided.                                        *)
(*                                                                         *)
(* THE GUARD IS DELIBERATELY WEAKER THAN PRODUCTION'S. In code a tombstone *)
(* exists only after ForgetAsync, which follows the terminal fan-out, so   *)
(* the retention mask is ordinarily reached late. Nothing makes that       *)
(* ordering hold for a prepared bucket the fan-out never saw - a           *)
(* shadow-forward onto a split destination the participant query passed  *)
(* over - and a dial failure has no ordering at all, not even against the *)
(* decision. So the action may fire at ANY point before the row is         *)
(* retired: before the decision, while every bucket is still live, or     *)
(* after the fan-out. That is an over-approximation of what a reader can   *)
(* be told: every Indeterminate answer production can give is an answer    *)
(* this model can give, so a safety property that holds here holds for     *)
(* the ordered case too. Cross-tree delegation itself is still not         *)
(* modelled as a mechanism (see Refinement.md); only its observable effect *)
(* on this tree - an Indeterminate answer at any time - is.                *)
(*                                                                         *)
(* The one guard kept, ~forgotten[t], costs no generality: RegistryView    *)
(* tests forgotten[t] first, so toggling the flag on a retired row could   *)
(* not change any observation, and keeping it stops TLC enumerating those  *)
(* indistinguishable states.                                               *)
(*                                                                         *)
(* This is the variable issue #2320 found missing. Before it the stored    *)
(* decision WAS the registry, so the registry could not misreport itself.  *)
(* The interposition is what matters, not the clock: reporting the masked  *)
(* row as "inflight" instead - the defect production had before            *)
(* AtomicVisibilityGate learnt the Indeterminate arm - reverts a committed  *)
(* key to its pre-saga value four steps from the initial state (prepare,   *)
(* decide, one broadcast step, the mask), which the paired mutation keeps  *)
(* demonstrating. The revision is left alone: production's comparison      *)
(* token does move on a mask, through terms the spec abstracts away (see   *)
(* RevisionMonotonic in Refinement.md).                                    *)
(***************************************************************************)
RegistryMask(t) ==
    /\ ~forgotten[t]
    /\ masked' = [masked EXCEPT ![t] = ~masked[t]]
    /\ UNCHANGED <<phase, vote, decision, terminal, pend, orphanDone, forgotten, revision>>

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
    \/ \E t \in Txns : RegistryMask(t)
    \/ Stutter

(***************************************************************************)
(* Fairness: each saga makes progress (prepare -> decide -> broadcast every *)
(* leaf) so every saga terminates. The reshard orphan / drain actions and  *)
(* RegistryMask are deliberately NOT fair - they model optional            *)
(* environment events, and every safety property must hold whether or not  *)
(* they fire.                                                              *)
(***************************************************************************)
TxProgress(t) ==
    \/ PrepareTx(t)
    \/ DecideTx(t)
    \/ \E k \in Keys : BroadcastStep(t, k)

Spec == Init /\ [][Next]_vars /\ \A t \in Txns : WF_vars(TxProgress(t))

(***************************************************************************)
(* Safety invariants (the property catalogue, lever (b) / #1595).          *)
(***************************************************************************)

\* Atomicity / all-or-nothing visibility: within one saga no snapshot reader
\* sees one written key at its post-saga value and another at its pre-saga
\* value - never a split view. A "hidden" key is compatible with either: it
\* is the gate declining to answer, not an answer.
AllOrNothing ==
    \A t \in Txns : \A a, b \in Written(t) : ~(ObservedPrepared(t, a) /\ ObservedPreSaga(t, b))

\* Sharpened form: a key is post-saga-visible for a reader only when the
\* tree-wide registry decision is committed, and pre-saga-visible only when
\* it is not. Implies AllOrNothing and StrictIsolation; a
\* broadcast-before-decision bug violates it, and so does a gate that serves
\* the pre-saga value for a saga that did commit.
VisibilityMatchesDecision ==
    \A t \in Txns : \A k \in Written(t) :
        /\ ObservedPrepared(t, k) => decision[t] = "committed"
        /\ ObservedPreSaga(t, k)  => decision[t] # "committed"

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
(* Action and temporal properties. DecisionDurability and RevisionMonotonic *)
(* are box-of-action formulas over a single step. MonotonicVisibility is a  *)
(* safety property over a whole behaviour (a nested [] formula), because    *)
(* the three-valued observation makes a single-step statement of it too    *)
(* weak - see its comment. Termination, EveryCommittedKeyReadable and      *)
(* NoStrandedPrepare are liveness properties and need the fairness          *)
(* assumption in Spec.                                                      *)
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

\* Monotonic visibility: once a key has been observed post-saga, it is never
\* observed at its pre-saga value at any later state (a committed value never
\* reverts, even across a reshard or a registry that stops reporting the
\* decision). Going hidden is not a reversion: the reader is refused an
\* answer, not given a stale one. But hidden must not launder one either, and
\* that is why this is stated over the whole behaviour rather than over one
\* step. The single-step form, [][ObservedPrepared => ~ObservedPreSaga']_vars,
\* was "once post, always post" by induction only while Observed had two
\* values. With three it admits post -> hidden -> pre, which is exactly
\* production's retention > 0 hazard: the tombstone ages out (Indeterminate,
\* so hidden), then PruneExpired purges it while a stranded prepare is still
\* resident (absent, so InFlight, so pre). The paired mutation
\* MonotonicVisibilityPurgeAfterMask keeps that hazard as a standing check;
\* when it was added, the single-step form was measured clean on it and this
\* one fired.
\*
\* A ghost history variable (seenPost[t][k]) with a single-step check would
\* say the same thing. It was not chosen because it would add a variable
\* every action must carry in its UNCHANGED tuple and that every mutation
\* anchored on such a tuple would drift against, for no gain in what is
\* checked. The cost of the nested-[] form is diagnostic, not semantic: TLC
\* reports its violation as "Temporal properties were violated." without
\* naming it, like the liveness properties below.
MonotonicVisibility ==
    \A t \in Txns : \A k \in Written(t) :
        [](ObservedPrepared(t, k) => [](~ObservedPreSaga(t, k)))

\* The registry revision counter never decreases.
RevisionMonotonic == [][ revision' >= revision ]_vars

\* Every saga terminates. Under the fairness Spec asserts this fails on a
\* protocol defect, not only without fairness: a broadcast whose completion
\* test ranges over every key rather than the saga's own participants never
\* declares the saga done, though every participant has been told
\* (TerminationCompletionOverAllKeys). NoStrandedPrepare and
\* EveryCommittedKeyReadable both stay clean on that defect.
Termination == \A t \in Txns : <>(phase[t] = "done")

\* Every committed saga's keys are eventually all materialised at their
\* post-saga value on their own leaf: the projection, not merely the gate,
\* holds the committed write.
\*
\* An earlier statement asked for every key to be eventually observed
\* post-saga or hidden. Observed has exactly three values and
\* VisibilityMatchesDecision already forbids "pre" at every committed state,
\* so that target held in every state of every behaviour satisfying the
\* invariant, and a leads-to whose target is already true asserts nothing.
\* This form is not entailed by any invariant: on
\* EveryCommittedKeyReadableCommitFanOutStops, where the commit fan-out
\* stops after its first participant, every invariant holds and
\* ([]VisibilityMatchesDecision /\ []AllOrNothing) => EveryCommittedKeyReadable
\* is violated, because a key the fan-out skipped is served post-saga by the
\* gate forever and never materialised.
\*
\* Its overlap with NoStrandedPrepare is total for a committed saga, and is
\* stated rather than hidden. Here a leaf's projection IS its commit
\* terminal (ProjectedPrepared), so given LinearizedTerminals and that no
\* action clears a terminal once applied, NoStrandedPrepare implies this
\* property; and given VisibilityMatchesDecision, a committed key can only
\* fail to materialise by never receiving its terminal, so for a committed
\* saga the converse holds too. It is kept because it states the guarantee
\* a reader depends on rather than the mechanism that delivers it, and the
\* two come apart the moment materialisation stops being the terminal. Its
\* paired mutation fires NoStrandedPrepare as well, by construction.
\*
\* What it does not promise is that the gate SERVES the materialised value.
\* A late orphan bucket on a leaf that has already applied the commit is
\* hidden for as long as the registry reports Indeterminate, because the
\* gate tests that arm ahead of the orphan guard, and neither event that
\* ends it (the registry answering again, or the orphan being discarded) is
\* guaranteed to happen. "Every key is eventually observed post-saga" fails
\* on exactly that behaviour, and it is a behaviour production has too.
EveryCommittedKeyReadable ==
    \A t \in Txns :
        (decision[t] = "committed")
            ~> (\A k \in Written(t) : ProjectedPrepared(t, k))

\* No stranded prepare: every participant of a decided saga eventually
\* applies the saga's terminal, which is what consumes its prepared bucket.
\* This is the liveness property the catalogue was missing (issue #2321).
\* Termination cannot stand in for it - a saga can reach "done" with a
\* participant never told - and EveryCommittedKeyReadable cannot either,
\* because an aborted saga is outside it entirely: a compensation fan-out
\* that skips the participants whose prepare failed strands their buckets
\* while every saga terminates and every committed key materialises
\* (NoStrandedPrepareCompensationSkipsNacked). All three liveness properties
\* fail on protocol defects under the fairness Spec asserts. This one and
\* Termination each have one the other two miss; EveryCommittedKeyReadable's
\* is also caught here, because for a committed saga the two coincide.
NoStrandedPrepare ==
    \A t \in Txns : \A k \in Written(t) :
        (decision[t] # "inflight") ~> (terminal[t][k] # "none")
=============================================================================
