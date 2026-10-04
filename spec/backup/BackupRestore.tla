-------------------------- MODULE BackupRestore --------------------------
(***************************************************************************)
(* An abstract TLA+ specification of a coordinated restore of a replicated *)
(* tree across regions (the restore saga RestoreSagaDispatcher runs, with  *)
(* RestoreParticipant on every cluster), its per-record restore admission, *)
(* and the replication that resumes after it.                              *)
(*                                                                         *)
(* It models the protocol DESIGN, not the code. A tree's content is a set  *)
(* of write ids, merged by union (last-writer-wins over distinct keys).    *)
(* Each cluster holds two physical copies of the tree: "old", the copy the *)
(* alias resolves to before the restore, and "new", the restore shadow.    *)
(* Replication ships each cluster's locally authored writes from the copy  *)
(* its shipper is bound to. A backup SET restores as one group whose        *)
(* aliases one participant step swaps together, so the tree here stands    *)
(* for the group. See Refinement.md for the mapping to production symbols. *)
(*                                                                         *)
(* Epic #4430, issue #4440.                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* The bounded instance. Two clusters replicate the tree. w0 is the write  *)
(* the backup captured; wx is a backup record outside the restoring        *)
(* tenant's namespace. w1 (authored on B) and w2 (authored on A) are       *)
(* written at any time - before a cluster's cutover they land on its old  *)
(* copy and the restore must discard them; after it they land on the       *)
(* restored copy and must survive.                                         *)
(***************************************************************************)
Clusters == {"A", "B"}
Peer(c) == IF c = "A" THEN "B" ELSE "A"
Copies == {"old", "new"}
Writes == {"w1", "w2"}
Author == ("w1" :> "B") @@ ("w2" :> "A")
Ids == {"w0", "wx"} \cup Writes
Backup == {"w0", "wx"}
Admitted == {"w0"}

(***************************************************************************)
(* State.                                                                  *)
(*  alias[c]     the copy cluster c's alias resolves to.                   *)
(*  data[c][p]   the writes copy p on cluster c holds.                     *)
(*  log[c][p]    the writes copy p's WAL holds that c ships: its locally   *)
(*               authored writes, and every record the restore built into  *)
(*               the shadow (the build writes through the merge seams).    *)
(*  bound[c]     the copy c's shipper is bound to                          *)
(*               (ReplicationShipperState.BoundPhysicalTreeId).            *)
(*  sent[c]      the writes of the bound log c has shipped and had         *)
(*               acknowledged (its partition cursors).                     *)
(*  shipOn[c]    c's outbound shipping is not paused.                      *)
(*  recvOn[c]    c's inbound apply is not paused (ITreeReceiveFenceGrain). *)
(*  rphase       the saga: prepare -> commit | abort -> done.              *)
(*  vote[c]      c's prepare vote: none, yes, no (it compensated itself),  *)
(*               or comp (compensated on the abort decision).              *)
(*  pre          writes that landed on an old copy (pre-restore).          *)
(*  post         writes that landed on a restored copy.                    *)
(***************************************************************************)
VARIABLES alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post

vars == <<alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post>>

TypeOK ==
    /\ alias \in [Clusters -> Copies]
    /\ data \in [Clusters -> [Copies -> SUBSET Ids]]
    /\ log \in [Clusters -> [Copies -> SUBSET Ids]]
    /\ bound \in [Clusters -> Copies]
    /\ sent \in [Clusters -> SUBSET Ids]
    /\ shipOn \in [Clusters -> BOOLEAN]
    /\ recvOn \in [Clusters -> BOOLEAN]
    /\ rphase \in {"prepare", "commit", "abort", "done"}
    /\ vote \in [Clusters -> {"none", "yes", "no", "comp"}]
    /\ pre \subseteq Writes
    /\ post \subseteq Writes

Served(c) == data[c][alias[c]]

Init ==
    /\ alias = [c \in Clusters |-> "old"]
    /\ data = [c \in Clusters |-> [p \in Copies |-> IF p = "old" THEN {"w0"} ELSE {}]]
    /\ log = [c \in Clusters |-> [p \in Copies |-> {}]]
    /\ bound = [c \in Clusters |-> "old"]
    /\ sent = [c \in Clusters |-> {}]
    /\ shipOn = [c \in Clusters |-> TRUE]
    /\ recvOn = [c \in Clusters |-> TRUE]
    /\ rphase = "prepare"
    /\ vote = [c \in Clusters |-> "none"]
    /\ pre = {}
    /\ post = {}

(***************************************************************************)
(* Write(w): an application write, made on its author cluster's served     *)
(* copy. Not fair: it may never happen. The write fence makes the alias    *)
(* swap one step (Commit), so no write interleaves it.                     *)
(***************************************************************************)
Write(w) ==
    /\ w \notin pre \cup post
    /\ LET c == Author[w]
           p == alias[c]
       IN /\ data' = [data EXCEPT ![c][p] = @ \cup {w}]
          /\ log' = [log EXCEPT ![c][p] = @ \cup {w}]
          /\ IF p = "old" THEN pre' = pre \cup {w} /\ UNCHANGED post
                          ELSE post' = post \cup {w} /\ UNCHANGED pre
    /\ UNCHANGED <<alias, bound, sent, shipOn, recvOn, rphase, vote>>

(***************************************************************************)
(* Ship(c): c's shipper sends one unshipped write of its bound log, and    *)
(* the peer applies it to the copy the peer's alias resolves to. A send to *)
(* a peer whose receive is paused is deferred - not acknowledged, so the   *)
(* cursor does not move - which is the guard on recvOn. The shipper ships *)
(* only from the copy the alias resolves to: it re-resolves its source     *)
(* before shipping again after a fence. That last conjunct is the intended *)
(* design; production checks its binding only on the push notification or *)
(* the backstop interval (RestoredCutNotReAdvancedResumeShipsRetiredLog).  *)
(***************************************************************************)
Ship(c) ==
    /\ shipOn[c]
    /\ recvOn[Peer(c)]
    /\ bound[c] = alias[c]
    /\ \E w \in log[c][bound[c]] \ sent[c] :
         /\ sent' = [sent EXCEPT ![c] = @ \cup {w}]
         /\ data' = [data EXCEPT ![Peer(c)][alias[Peer(c)]] = @ \cup {w}]
    /\ UNCHANGED <<alias, log, bound, shipOn, recvOn, rphase, vote, pre, post>>

(***************************************************************************)
(* Rebind(c): the shipper re-resolves its source and finds the alias moved *)
(* (the backstop, ShipSourceIdentityBackstopInterval): it binds to the new *)
(* copy and resets its cursors, re-shipping from the new log's start.      *)
(***************************************************************************)
Rebind(c) ==
    /\ bound[c] # alias[c]
    /\ bound' = [bound EXCEPT ![c] = alias[c]]
    /\ sent' = [sent EXCEPT ![c] = {}]
    /\ UNCHANGED <<alias, data, log, shipOn, recvOn, rphase, vote, pre, post>>

(***************************************************************************)
(* Build(c): c's participant prepares: the admission pre-flight, then the  *)
(* unfenced, resumable shadow build, which writes every ADMITTED record of *)
(* the backup into the shadow (IBackupRestoreAdmission dead-letters the    *)
(* rest). It votes yes, or no when the probe or the build fails - a        *)
(* failed build garbage-collects its partial shadow itself.                *)
(***************************************************************************)
Build(c) ==
    /\ rphase = "prepare"
    /\ vote[c] = "none"
    /\ \E v \in {"yes", "no"} :
         /\ vote' = [vote EXCEPT ![c] = v]
         /\ data' = [data EXCEPT ![c]["new"] = IF v = "yes" THEN Admitted ELSE {}]
         /\ log' = [log EXCEPT ![c]["new"] = IF v = "yes" THEN Admitted ELSE {}]
    /\ UNCHANGED <<alias, bound, sent, shipOn, recvOn, rphase, pre, post>>

(***************************************************************************)
(* Decide: the coordinator's single global decision, once every vote is    *)
(* in. Commit needs every vote yes. Abort is always possible: a vote no,   *)
(* a prepare retried past its hour, or the coordinator lost before it      *)
(* decided (the participants then auto-compensate on their fence timer).   *)
(***************************************************************************)
Decide ==
    /\ rphase = "prepare"
    /\ \A c \in Clusters : vote[c] # "none"
    /\ \E d \in {"commit", "abort"} :
         /\ d = "commit" => \A c \in Clusters : vote[c] = "yes"
         /\ rphase' = d
    /\ UNCHANGED <<alias, data, log, bound, sent, shipOn, recvOn, vote, pre, post>>

(***************************************************************************)
(* Commit(c): c's participant engages the write fence (pausing writes,    *)
(* shipping and receiving), swaps the alias to the shadow, and unblocks   *)
(* local writes. Shipping and receiving stay paused. The swap pushes a     *)
(* source-identity change to the shipper, which may be lost (it is best-   *)
(* effort); a delivered push rebinds and resets the cursors.               *)
(***************************************************************************)
Commit(c) ==
    /\ rphase = "commit"
    /\ alias[c] = "old"
    /\ alias' = [alias EXCEPT ![c] = "new"]
    /\ shipOn' = [shipOn EXCEPT ![c] = FALSE]
    /\ recvOn' = [recvOn EXCEPT ![c] = FALSE]
    /\ \E pushed \in BOOLEAN :
         /\ bound' = IF pushed THEN [bound EXCEPT ![c] = "new"] ELSE bound
         /\ sent' = IF pushed THEN [sent EXCEPT ![c] = {}] ELSE sent
    /\ UNCHANGED <<data, log, rphase, vote, pre, post>>

(***************************************************************************)
(* Abort(c): on the abort decision a participant that prepared reverts and *)
(* garbage-collects its shadow, leaving the pre-restore tree untouched.    *)
(***************************************************************************)
Abort(c) ==
    /\ rphase = "abort"
    /\ vote[c] = "yes"
    /\ vote' = [vote EXCEPT ![c] = "comp"]
    /\ data' = [data EXCEPT ![c]["new"] = {}]
    /\ log' = [log EXCEPT ![c]["new"] = {}]
    /\ UNCHANGED <<alias, bound, sent, shipOn, recvOn, rphase, pre, post>>

(***************************************************************************)
(* Complete: the saga completes globally - every cluster has cut over, or  *)
(* every prepared cluster has compensated.                                 *)
(***************************************************************************)
Complete ==
    /\ \/ rphase = "commit" /\ \A c \in Clusters : alias[c] = "new"
       \/ rphase = "abort" /\ \A c \in Clusters : vote[c] \in {"no", "comp"}
    /\ rphase' = "done"
    /\ UNCHANGED <<alias, data, log, bound, sent, shipOn, recvOn, vote, pre, post>>

(***************************************************************************)
(* Resume(c): c's fence grain observes global completion and resumes       *)
(* shipping and receiving together (SagaWriteFenceGrain release point 2). *)
(***************************************************************************)
Resume(c) ==
    /\ rphase = "done"
    /\ ~shipOn[c] \/ ~recvOn[c]
    /\ shipOn' = [shipOn EXCEPT ![c] = TRUE]
    /\ recvOn' = [recvOn EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<alias, data, log, bound, sent, rphase, vote, pre, post>>

Quiesced ==
    /\ rphase = "done"
    /\ pre \cup post = Writes
    /\ \A c \in Clusters :
         /\ shipOn[c] /\ recvOn[c]
         /\ bound[c] = alias[c]
         /\ log[c][bound[c]] \subseteq sent[c]

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E w \in Writes : Write(w)
    \/ \E c \in Clusters : Ship(c)
    \/ \E c \in Clusters : Rebind(c)
    \/ \E c \in Clusters : Build(c)
    \/ Decide
    \/ \E c \in Clusters : Commit(c)
    \/ \E c \in Clusters : Abort(c)
    \/ Complete
    \/ \E c \in Clusters : Resume(c)
    \/ Stutter

(***************************************************************************)
(* Fairness: the saga and replication make progress. Writes are not fair.  *)
(***************************************************************************)
Progress ==
    \/ \E c \in Clusters : Ship(c) \/ Rebind(c) \/ Build(c) \/ Commit(c) \/ Abort(c) \/ Resume(c)
    \/ Decide
    \/ Complete

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)
        /\ \A c \in Clusters : WF_vars(Ship(c)) /\ WF_vars(Rebind(c)) /\ WF_vars(Resume(c))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* All-or-nothing: no cluster serves the restored copy unless every cluster
\* voted yes and none compensated.
RestoreAllOrNothing ==
    (\E c \in Clusters : alias[c] = "new") => \A d \in Clusters : vote[d] = "yes"

\* No write made before a cluster's cutover ever reaches a restored copy:
\* nothing re-advances the restored cut.
RestoredCutNotReAdvanced ==
    \A c \in Clusters : data[c]["new"] \cap pre = {}

\* A restore never installs a record outside the restoring tenant's
\* namespace.
RestoreAdmitsOnlyNamespace ==
    \A c \in Clusters : \A p \in Copies : "wx" \notin data[c][p]

\* A write made on a restored copy stays served by its author.
AckedWritesServed ==
    \A w \in post : w \in Served(Author[w])

(***************************************************************************)
(* Liveness: a restore followed by resumed replication converges - every   *)
(* cluster eventually serves the same content, for good.                  *)
(***************************************************************************)
RestoreConverges == <>[](Served("A") = Served("B"))
=============================================================================
