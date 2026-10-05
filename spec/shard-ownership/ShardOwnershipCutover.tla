------------------------ MODULE ShardOwnershipCutover ------------------------
(***************************************************************************)
(* An atomic-write saga bound to one physical copy of a tree, across a     *)
(* local shadow-cutover restore and its revert: the alias swap onto the    *)
(* shadow, the retained redirect armed on the previous copy, the swap back *)
(* and the redirect fix-up. The saga re-binds when the tree moves off its  *)
(* bound copy and the copy it left mirrors nowhere (unlike a resize        *)
(* source, #4369), so the prepares it already took there would be         *)
(* orphaned; a revert, or a stale reader before the redirect is armed,     *)
(* would then serve its batch torn (#4689). The design checked here        *)
(* discards them before the decision, through a call the redirect admits, *)
(* and the leaf remembers the discard so a prepare still on the wire is    *)
(* refused.                                                                *)
(*                                                                         *)
(* The alias and map moving together, stale routing healing, the alias     *)
(* reservation and a crash at any step of the restore or the revert are   *)
(* BackupCutover's (spec/backup/); this module takes the restore's steps   *)
(* as its environment and adds the saga. Values are reduced to the        *)
(* pre-saga value and the saga's, so a reader sees a batch whole or torn.  *)
(*                                                                         *)
(* See README.md for the instance and RefinementCutover.md for the         *)
(* mapping. Epic #4430, issues #4434 and #4440.                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS P, S, k1, k2

\* P is the copy the alias names before the restore; S is the shadow the
\* restore builds from a backup taken before the saga. One key per shard.
Copies == {P, S}
Keys == {k1, k2}
None == "none"
Old == "old"
New == "new"

VARIABLES
    alias,    \* registry: the physical copy the logical tree resolves to
    pub,      \* copies whose (copy, map) pair the registry has published; a stale router may hold any
    redir,    \* redir[c]: where copy c's retained redirect sends routed traffic, or None
    rp,       \* the restore and its revert: idle -> swapped -> committed -> revertSwapped -> reverted
    row,      \* row[c][k]: the committed projection value of k on copy c
    pend,     \* pend[c][k]: the saga's prepared bucket for k on copy c
    term,     \* term[c][k]: the leaf for k on copy c remembers a terminal or a discard of the saga
    late,     \* routed prepares still on the wire after the saga's own call gave up on them
    sg,       \* saga phase: dispatch -> checked -> decided -> done
    bound,    \* the copy the saga is bound to
    prepped,  \* keys whose prepare under the current binding was acknowledged
    left,     \* copies the saga re-bound away from, whose prepares it owes a discard
    disc,     \* copies whose discard the saga has had acknowledged
    dec,      \* the registry's recorded decision for the saga
    told      \* keys of the bound copy the terminal broadcast has delivered

vars == <<alias, pub, redir, rp, row, pend, term, late, sg, bound, prepped, left, disc, dec, told>>

restoreVars == <<alias, pub, redir, rp>>
sagaVars == <<row, pend, term, late, sg, bound, prepped, left, disc, dec, told>>

TypeOK ==
    /\ alias \in Copies
    /\ pub \subseteq Copies
    /\ redir \in [Copies -> Copies \cup {None}]
    /\ rp \in {"idle", "swapped", "committed", "revertSwapped", "reverted"}
    /\ row \in [Copies -> [Keys -> {Old, New}]]
    /\ pend \in [Copies -> [Keys -> BOOLEAN]]
    /\ term \in [Copies -> [Keys -> BOOLEAN]]
    /\ late \subseteq (Copies \X Keys)
    /\ sg \in {"dispatch", "checked", "decided", "done"}
    /\ bound \in Copies
    /\ prepped \subseteq Keys
    /\ left \subseteq Copies
    /\ disc \subseteq Copies
    /\ dec \in {"none", "committed", "aborted"}
    /\ told \subseteq Keys

-----------------------------------------------------------------------------
(* Derived state                                                           *)

\* Whether copy c serves a call routed through the logical alias: a retained
\* redirect refuses it (ShardRootGrain.ThrowIfRetainedRedirect). A call
\* addressed to the physical copy directly carries no routed-logical stamp and
\* is always admitted.
RoutedAdmits(c) == redir[c] = None

\* Whether some reader can be served by copy c: a fresh reader is served by
\* the copy the alias names, and a stale routing activation by any copy the
\* registry published that does not redirect it.
Servable(c) == c = alias \/ (c \in pub /\ RoutedAdmits(c))

\* The leaf read gate: a bucket of a committed saga surfaces unless the leaf
\* remembers applying its terminal or discarding it.
Surfaced(c, k) == pend[c][k] /\ dec = "committed" /\ ~term[c][k]

Vis(c, k) == IF Surfaced(c, k) THEN New ELSE row[c][k]

-----------------------------------------------------------------------------
Init ==
    /\ alias = P
    /\ pub = {P}
    /\ redir = [c \in Copies |-> None]
    /\ rp = "idle"
    /\ row = [c \in Copies |-> [k \in Keys |-> Old]]
    /\ pend = [c \in Copies |-> [k \in Keys |-> FALSE]]
    /\ term = [c \in Copies |-> [k \in Keys |-> FALSE]]
    /\ late = {}
    /\ sg = "dispatch"
    /\ bound = P
    /\ prepped = {}
    /\ left = {}
    /\ disc = {}
    /\ dec = "none"
    /\ told = {}

(***************************************************************************)
(* The restore and its revert (the environment; BackupCutover checks them). *)
(***************************************************************************)

\* The cutover moves the alias and the map onto the shadow in one registry
\* write (AliasCutoverShardMaps.SwapCutoverAsync). Not fair: a restore is a
\* caller's choice.
Swap ==
    /\ rp = "idle"
    /\ alias' = S
    /\ pub' = pub \cup {S}
    /\ rp' = "swapped"
    /\ UNCHANGED <<redir>>
    /\ UNCHANGED sagaVars

\* The previous copy is armed to redirect routed traffic onto the shadow
\* (LatticeBackupRestoreService.MarkRetainedTreeRedirectAsync). Fair:
\* BackupCutover's RestoreReturns.
Arm ==
    /\ rp = "swapped"
    /\ redir' = [redir EXCEPT ![P] = S]
    /\ rp' = "committed"
    /\ UNCHANGED <<alias, pub>>
    /\ UNCHANGED sagaVars

\* The revert moves the alias and the map back (AliasCutoverShardMaps.RevertAsync).
\* Not fair: a revert is a caller's choice.
RevertSwap ==
    /\ rp = "committed"
    /\ alias' = P
    /\ rp' = "revertSwapped"
    /\ UNCHANGED <<pub, redir>>
    /\ UNCHANGED sagaVars

\* The revert clears the previous copy's redirect and arms the shadow to
\* redirect back. Fair: BackupCutover's RevertReturns.
RevertArm ==
    /\ rp = "revertSwapped"
    /\ redir' = [redir EXCEPT ![P] = None, ![S] = P]
    /\ rp' = "reverted"
    /\ UNCHANGED <<alias, pub>>
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* The saga.                                                               *)
(***************************************************************************)

\* One key's prepare, routed through an activation still holding the bound
\* copy's pair and admitted there. Its first attempt may have given up while
\* the call stayed on the wire, which the retry then acknowledges: the
\* straggler is left in late (at most one at a time).
Prepare(k) ==
    /\ sg = "dispatch"
    /\ k \notin prepped
    /\ RoutedAdmits(bound)
    /\ pend' = [pend EXCEPT ![bound][k] = TRUE]
    /\ prepped' = prepped \cup {k}
    /\ late' \in IF late = {} THEN {late, {<<bound, k>>}} ELSE {late}
    /\ UNCHANGED <<row, term, sg, bound, left, disc, dec, told>>
    /\ UNCHANGED restoreVars

\* A straggling routed prepare reaches its shard. A retained redirect refuses
\* it, and so does a leaf that remembers the saga's terminal or discard.
\* Not fair: a straggler may be lost.
Land(c, k) ==
    /\ <<c, k>> \in late
    /\ late' = late \ {<<c, k>>}
    /\ pend' = IF RoutedAdmits(c) /\ ~term[c][k] THEN [pend EXCEPT ![c][k] = TRUE] ELSE pend
    /\ UNCHANGED <<row, term, sg, bound, prepped, left, disc, dec, told>>
    /\ UNCHANGED restoreVars

\* The routing tier refuses part of the dispatch because the tree resolves
\* elsewhere, and the bound copy mirrors nowhere, so the saga re-binds to the
\* resolved copy and re-dispatches the whole batch, recording the copy it
\* leaves (unless it is re-binding back onto it).
RebindOnRefusal ==
    /\ sg = "dispatch"
    /\ prepped # Keys
    /\ alias # bound
    /\ bound' = alias
    /\ prepped' = {}
    /\ left' = (left \cup {bound}) \ {alias}
    /\ UNCHANGED <<row, pend, term, late, sg, disc, dec, told>>
    /\ UNCHANGED restoreVars

\* The batch is fully dispatched but the tree has moved: the pre-decision
\* check re-binds, records the copy it leaves, and re-dispatches.
RebindBeforeDecision ==
    /\ sg = "dispatch"
    /\ prepped = Keys
    /\ alias # bound
    /\ bound' = alias
    /\ prepped' = {}
    /\ left' = (left \cup {bound}) \ {alias}
    /\ UNCHANGED <<row, pend, term, late, sg, disc, dec, told>>
    /\ UNCHANGED restoreVars

\* The pre-decision check finds the tree still resolves to the bound copy.
Check ==
    /\ sg = "dispatch"
    /\ prepped = Keys
    /\ alias = bound
    /\ sg' = "checked"
    /\ UNCHANGED <<row, pend, term, late, bound, prepped, left, disc, dec, told>>
    /\ UNCHANGED restoreVars

\* The saga discards its prepares on a copy it left, addressed to that
\* physical copy directly so a retained redirect admits it; the leaf drops
\* the bucket and remembers the discard as it remembers a terminal (#4689).
Discard(c) ==
    /\ sg = "checked"
    /\ c \in left \ disc
    /\ pend' = [pend EXCEPT ![c] = [k \in Keys |-> FALSE]]
    /\ term' = [term EXCEPT ![c] = [k \in Keys |-> TRUE]]
    /\ disc' = disc \cup {c}
    /\ UNCHANGED <<row, late, sg, bound, prepped, left, dec, told>>
    /\ UNCHANGED restoreVars

\* The commit decision, once every copy the saga left has acknowledged its
\* discard.
Decide ==
    /\ sg = "checked"
    /\ left \subseteq disc
    /\ dec' = "committed"
    /\ sg' = "decided"
    /\ UNCHANGED <<row, pend, term, late, bound, prepped, left, disc, told>>
    /\ UNCHANGED restoreVars

\* The batch fails and the saga compensates. Not fair.
Abort ==
    /\ sg = "dispatch"
    /\ dec' = "aborted"
    /\ sg' = "decided"
    /\ UNCHANGED <<row, pend, term, late, bound, prepped, left, disc, told>>
    /\ UNCHANGED restoreVars

\* One shard of the terminal broadcast, addressed to the bound copy directly
\* (no routed-logical stamp, so a retained redirect admits it). A commit
\* drains the bucket, or installs the committed value as the backstop; the
\* broadcast completes with the last shard.
Terminal(k) ==
    /\ sg = "decided"
    /\ k \notin told
    /\ row' = IF dec = "committed" THEN [row EXCEPT ![bound][k] = New] ELSE row
    /\ pend' = [pend EXCEPT ![bound][k] = FALSE]
    /\ term' = [term EXCEPT ![bound][k] = TRUE]
    /\ told' = told \cup {k}
    /\ sg' = IF told \cup {k} = Keys THEN "done" ELSE "decided"
    /\ UNCHANGED <<late, bound, prepped, left, disc, dec>>
    /\ UNCHANGED restoreVars

\* Quiescence: the saga is done and nothing is on the wire.
Stutter ==
    /\ sg = "done"
    /\ late = {}
    /\ UNCHANGED vars

Next ==
    \/ Swap
    \/ Arm
    \/ RevertSwap
    \/ RevertArm
    \/ \E k \in Keys : Prepare(k)
    \/ \E c \in Copies, k \in Keys : Land(c, k)
    \/ RebindOnRefusal
    \/ RebindBeforeDecision
    \/ Check
    \/ \E c \in Copies : Discard(c)
    \/ Decide
    \/ Abort
    \/ \E k \in Keys : Terminal(k)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Arm) /\ WF_vars(RevertArm)
    /\ WF_vars(\E k \in Keys : Prepare(k))
    /\ WF_vars(RebindOnRefusal) /\ WF_vars(RebindBeforeDecision) /\ WF_vars(Check)
    /\ WF_vars(\E c \in Copies : Discard(c)) /\ WF_vars(Decide)
    /\ WF_vars(\E k \in Keys : Terminal(k))

-----------------------------------------------------------------------------
(* Properties                                                              *)

\* No reader, fresh or stale, is served a copy that holds the saga's batch on
\* one key and not the other: not across the cutover, not between its swap
\* and its redirect, and not after a revert.
AtomicAcrossCutover ==
    \A c \in Copies : Servable(c) => Vis(c, k1) = Vis(c, k2)

\* Once committed, the saga holds prepared buckets only on its bound copy:
\* every copy it left has been discarded, and no straggler recreated a bucket
\* there.
CommittedBatchOnBoundCopy ==
    dec = "committed" => \A c \in Copies \ {bound} : \A k \in Keys : ~pend[c][k]

\* The saga settles, however the restore and its revert interleave with it.
SagaSettles == <>(sg = "done")
=============================================================================
