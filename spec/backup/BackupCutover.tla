-------------------------- MODULE BackupCutover --------------------------
(***************************************************************************)
(* An abstract TLA+ specification of a local shadow-cutover restore and    *)
(* its revert on one cluster (LatticeBackupRestoreService.RestoreAsync with *)
(* LatticeRestoreMode.ShadowCutover, RevertRestoreAsync): the registry     *)
(* alias and shard map that move together, the redirect armed on the copy *)
(* the alias moved off so a stale routing activation self-heals, the alias *)
(* reservation that refuses a delete while the tree's copies are in        *)
(* motion, and a crash at any step of the restore or of its revert with   *)
(* an idempotent retry (issue #4441 F9).                                  *)
(*                                                                         *)
(* It models the DESIGN, not the code. The tree has two physical copies:   *)
(* "prev", which the alias resolves to before the restore, and "shadow",   *)
(* the copy the restore builds. A reader reaches the tree through a cached *)
(* routing snapshot (a StatelessWorker LatticeGrain activation) that may   *)
(* be arbitrarily stale. See Refinement.md for the mapping.                *)
(*                                                                         *)
(* Epic #4430, issues #4440 and #4441.                                     *)
(***************************************************************************)
EXTENDS Naturals, TLC

Copies == {"prev", "shadow"}
Other(p) == IF p = "prev" THEN "shadow" ELSE "prev"

(***************************************************************************)
(* State.                                                                  *)
(*  alias      the copy the registry alias resolves the tree to.          *)
(*  map        the copy whose shard map the registry holds under the tree. *)
(*  route      a routing activation's cached [alias, map] pair.            *)
(*  redir[p]   where copy p forwards logical-alias traffic, or "none"      *)
(*             (MarkRetainedTreeRedirectAsync).                            *)
(*  rp         the operation: idle -> built -> swapped -> committed, then  *)
(*             optionally revertBegun -> reverting -> reverted; failed or  *)
(*             revertFailed after a crash.                                 *)
(*  reserved   the tree's alias reservation is held                        *)
(*             (ITreeDeletionGrain.BeginAliasChangeAsync / End...).        *)
(*  deleted    the tree has been deleted.                                  *)
(*  crashed    a crash has happened - the fault budget is one, so a       *)
(*             crash cannot repeat forever and starve the retry.           *)
(***************************************************************************)
VARIABLES alias, map, route, redir, rp, reserved, deleted, crashed

vars == <<alias, map, route, redir, rp, reserved, deleted, crashed>>

Phases == {"idle", "built", "swapped", "committed", "revertBegun", "reverting", "reverted",
           "failed", "revertFailed"}

TypeOK ==
    /\ alias \in Copies
    /\ map \in Copies
    /\ route \in [alias : Copies, map : Copies]
    /\ redir \in [Copies -> Copies \cup {"none"}]
    /\ rp \in Phases
    /\ reserved \in BOOLEAN
    /\ deleted \in BOOLEAN
    /\ crashed \in BOOLEAN

\* The copy a read through the cached route is served by: a redirect armed
\* on the routed copy forwards it, otherwise the routed copy answers.
Served == IF redir[route.alias] # "none" THEN redir[route.alias] ELSE route.alias

\* The routed copy is read with the routed map unless a redirect forwards
\* the read, which the forwarded-to copy then routes itself.
Coherent == redir[route.alias] # "none" \/ route.map = route.alias

Init ==
    /\ alias = "prev"
    /\ map = "prev"
    /\ route = [alias |-> "prev", map |-> "prev"]
    /\ redir = [p \in Copies |-> "none"]
    /\ rp = "idle"
    /\ reserved = FALSE
    /\ deleted = FALSE
    /\ crashed = FALSE

(***************************************************************************)
(* Build: the restore registers the shadow with restore provenance, takes  *)
(* the alias reservation, and builds the shadow (BuildShadowCoreAsync).    *)
(* Refused once the tree is deleted, or while another operation holds the  *)
(* reservation.                                                            *)
(***************************************************************************)
Build ==
    /\ rp = "idle"
    /\ ~deleted
    /\ ~reserved
    /\ reserved' = TRUE
    /\ rp' = "built"
    /\ UNCHANGED <<alias, map, route, redir, deleted, crashed>>

(***************************************************************************)
(* Swap: the cutover moves the alias AND the shard map onto the shadow in  *)
(* one registry write (AliasCutoverShardMaps.SwapCutoverAsync, #4336).     *)
(***************************************************************************)
Swap ==
    /\ rp = "built"
    /\ alias' = "shadow"
    /\ map' = "shadow"
    /\ rp' = "swapped"
    /\ UNCHANGED <<route, redir, reserved, deleted, crashed>>

(***************************************************************************)
(* ArmRedirect: the retained previous copy is armed to forward            *)
(* logical-alias traffic onto the shadow, then the reservation is          *)
(* released and the restore returns.                                       *)
(***************************************************************************)
ArmRedirect ==
    /\ rp = "swapped"
    /\ redir' = [redir EXCEPT !["prev"] = "shadow"]
    /\ reserved' = FALSE
    /\ rp' = "committed"
    /\ UNCHANGED <<alias, map, route, deleted, crashed>>

(***************************************************************************)
(* RevertBegin: a revert takes the alias reservation                       *)
(* (ITreeDeletionGrain.BeginAliasChangeAsync). Optional: not fair.         *)
(***************************************************************************)
RevertBegin ==
    /\ rp = "committed"
    /\ ~deleted
    /\ ~reserved
    /\ reserved' = TRUE
    /\ rp' = "revertBegun"
    /\ UNCHANGED <<alias, map, route, redir, deleted, crashed>>

(***************************************************************************)
(* RevertSwap: the revert moves the alias and the map back onto the        *)
(* previous copy in one registry write (AliasCutoverShardMaps.RevertAsync). *)
(***************************************************************************)
RevertSwap ==
    /\ rp = "revertBegun"
    /\ alias' = "prev"
    /\ map' = "prev"
    /\ rp' = "reverting"
    /\ UNCHANGED <<route, redir, reserved, deleted, crashed>>

(***************************************************************************)
(* RevertRedirect: the revert clears the previous copy's redirect and arms *)
(* the shadow - which the alias just moved off - to forward onto the       *)
(* previous copy, then releases the reservation.                           *)
(***************************************************************************)
RevertRedirect ==
    /\ rp = "reverting"
    /\ redir' = [redir EXCEPT !["prev"] = "none", !["shadow"] = "prev"]
    /\ reserved' = FALSE
    /\ rp' = "reverted"
    /\ UNCHANGED <<alias, map, route, deleted, crashed>>

(***************************************************************************)
(* Refresh: a routing activation re-resolves its route from the registry, *)
(* reading the alias and the map together. Not fair: a stale activation    *)
(* may never refresh.                                                      *)
(***************************************************************************)
Refresh ==
    /\ route' = [alias |-> alias, map |-> map]
    /\ UNCHANGED <<alias, map, redir, rp, reserved, deleted, crashed>>

(***************************************************************************)
(* Crash: the restore or its revert fails part-way. The reservation it    *)
(* took is KEPT, and the shadow stays registered, until the same request  *)
(* is retried to completion. Between a revert's swap and its redirect     *)
(* fix-up a stale reader is still forwarded to the shadow; the retry       *)
(* completes the fix-up, so RevertNeverServesRestored holds once the       *)
(* revert returns and RevertReturns guarantees it does. Not fair.         *)
(***************************************************************************)
Crash ==
    /\ rp \in {"built", "swapped", "revertBegun", "reverting"}
    /\ ~crashed
    /\ crashed' = TRUE
    /\ rp' = IF rp \in {"built", "swapped"} THEN "failed" ELSE "revertFailed"
    /\ UNCHANGED <<alias, map, route, redir, reserved, deleted>>

(***************************************************************************)
(* Retry: the same request retried resumes where the failure left it: the  *)
(* deterministic operation id reuses the shadow and the reservation it     *)
(* already holds. A retried revert (RevertRestoreAsync again, with the     *)
(* same result) re-runs the swap, a no-op once the alias is back, and the  *)
(* whole redirect fix-up.                                                  *)
(***************************************************************************)
Retry ==
    /\ rp \in {"failed", "revertFailed"}
    /\ rp' = CASE rp = "failed" /\ alias = "shadow" -> "swapped"
               [] rp = "failed" -> "built"
               [] alias = "prev" -> "reverting"
               [] OTHER -> "revertBegun"
    /\ UNCHANGED <<alias, map, route, redir, reserved, deleted, crashed>>

(***************************************************************************)
(* Delete: the tree is deleted, which the reservation refuses.             *)
(* Not fair.                                                               *)
(***************************************************************************)
Delete ==
    /\ ~deleted
    /\ ~reserved
    /\ deleted' = TRUE
    /\ UNCHANGED <<alias, map, route, redir, rp, reserved, crashed>>

\* Refresh is always enabled, so the model never deadlocks and needs no
\* stuttering action.
Next ==
    \/ Build
    \/ Swap
    \/ ArmRedirect
    \/ RevertBegin
    \/ RevertSwap
    \/ RevertRedirect
    \/ Refresh
    \/ Crash
    \/ Retry
    \/ Delete

Progress == Swap \/ ArmRedirect \/ RevertSwap \/ RevertRedirect \/ Retry

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* A reader never pairs one copy with another copy's shard map, which would
\* read most of its keys as absent: a restored tree is never served torn.
RestoreNeverTorn == Coherent

\* Once the restore has returned, every reader - however stale its route -
\* is served by the restored copy.
CutoverServesRestored == rp = "committed" => Served = "shadow"

\* Once a revert has returned, no reader is served by the restored copy.
RevertNeverServesRestored == rp = "reverted" => Served = "prev"

\* The tree is never deleted while a restore or revert holds it in motion.
DeleteNeverMidCutover == deleted => rp \in {"idle", "committed", "reverted"}

(***************************************************************************)
(* Liveness: a restore whose shadow is built eventually returns.           *)
(***************************************************************************)
RestoreReturns == (rp = "built") ~> (rp \in {"committed", "revertBegun", "reverting", "reverted"})

\* A revert that has taken its reservation eventually returns, a crash
\* included (the same request is retried).
RevertReturns == (rp = "revertBegun") ~> (rp = "reverted")
=============================================================================
