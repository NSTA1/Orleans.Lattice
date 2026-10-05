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
(* Delivery is split into admission and landing (issue #4593). A shipped   *)
(* write is admitted by the receiver's CACHED receive gate, which can be    *)
(* stale, and lands later - possibly after the receiver's restore. The     *)
(* restored copy is born closed and records the receive-fence epoch of the *)
(* restore's pause; a landing that routes to it is refused while it is     *)
(* closed, or when it was admitted under an older epoch. A refused live    *)
(* delivery is deferred (the sender re-ships it); an admitted write may    *)
(* instead be parked in the receiver's causal-apply buffer, stamped with   *)
(* the fence epoch read uncached at park time, and drained later; a parked *)
(* entry admitted under an older epoch than the restored copy's is         *)
(* discarded.                                                              *)
(*                                                                         *)
(* Epic #4430, issues #4440 and #4593.                                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* The bounded instance. Two clusters replicate the tree. w0 is the write  *)
(* the backup captured; wx is a backup record outside the restoring        *)
(* tenant's namespace. w1 (authored on B) and w2 (authored on A) are       *)
(* written at any time - before a cluster's cutover they land on its old  *)
(* copy and the restore must discard them; after it they land on the       *)
(* restored copy and must survive. One saga runs, so each cluster's        *)
(* receive-fence epoch moves from 0 to 1 at most once.                     *)
(***************************************************************************)
Clusters == {"A", "B"}
Peer(c) == IF c = "A" THEN "B" ELSE "A"
Copies == {"old", "new"}
Writes == {"w1", "w2"}
Author == ("w1" :> "B") @@ ("w2" :> "A")
Ids == {"w0", "wx"} \cup Writes
Backup == {"w0", "wx"}
Admitted == {"w0"}
Epochs == 0..1

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
(*  seen[c]      c's CACHED receive-gate answer: whether inbound apply is  *)
(*               open, and the fence epoch it was read under               *)
(*               (ReplicationReceiveGate).                                 *)
(*  epoch[c]     c's receive-fence epoch; every pause bumps it.            *)
(*  closed[c]    c's copies a restore holds receive-closed                 *)
(*               (ICopyReceiveFenceGrain).                                 *)
(*  floor[c][p]  copy p's minimum admission epoch.                         *)
(*  inflight     admitted deliveries that have not landed yet: the sender, *)
(*               the write, and the epoch the admission was stamped with.  *)
(*  parked[c]    entries parked in c's causal-apply buffer, each with the  *)
(*               fence epoch read when it was parked.                      *)
(***************************************************************************)
VARIABLES alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post,
          seen, epoch, closed, floor, inflight, parked

vars == <<alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post,
          seen, epoch, closed, floor, inflight, parked>>

\* The receive-fence variables, held unchanged by the restore's own steps.
fence == <<seen, epoch, closed, floor, inflight, parked>>

Delivery == [from : Clusters, w : Ids, ep : Epochs]
ParkedEntry == [w : Ids, ep : Epochs]

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
    /\ seen \in [Clusters -> [open : BOOLEAN, ep : Epochs]]
    /\ epoch \in [Clusters -> Epochs]
    /\ closed \in [Clusters -> SUBSET Copies]
    /\ floor \in [Clusters -> [Copies -> Epochs]]
    /\ inflight \subseteq Delivery
    /\ parked \in [Clusters -> SUBSET ParkedEntry]

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
    /\ seen = [c \in Clusters |-> [open |-> TRUE, ep |-> 0]]
    /\ epoch = [c \in Clusters |-> 0]
    /\ closed = [c \in Clusters |-> {}]
    /\ floor = [c \in Clusters |-> [p \in Copies |-> 0]]
    /\ inflight = {}
    /\ parked = [c \in Clusters |-> {}]

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
    /\ UNCHANGED fence

(***************************************************************************)
(* Refresh(c): c's receive-gate cache re-reads the durable fence. The      *)
(* cache can stay stale for any number of steps in between - production    *)
(* bounds it by time, which this model deliberately does not rely on.      *)
(***************************************************************************)
Refresh(c) ==
    /\ seen' = [seen EXCEPT ![c] = [open |-> recvOn[c], ep |-> epoch[c]]]
    /\ UNCHANGED <<alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post>>
    /\ UNCHANGED <<epoch, closed, floor, inflight, parked>>

(***************************************************************************)
(* Ship(c): c's shipper sends one unshipped write of its bound log, and    *)
(* the peer's CACHED gate admits it, stamping it with the epoch the cached *)
(* answer carries (ReplicationApplier). A send to a peer whose cached      *)
(* answer is paused is deferred - not acknowledged, so the cursor does not *)
(* move. The shipper ships only from the copy the alias resolves to: it    *)
(* re-resolves its source before shipping again after a fence (#4490).     *)
(***************************************************************************)
Ship(c) ==
    /\ shipOn[c]
    /\ seen[Peer(c)].open
    /\ bound[c] = alias[c]
    /\ \E w \in log[c][bound[c]] \ sent[c] :
         /\ \A r \in inflight : r.from # c \/ r.w # w
         /\ inflight' = inflight \cup {[from |-> c, w |-> w, ep |-> seen[Peer(c)].ep]}
    /\ UNCHANGED <<alias, data, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, epoch, closed, floor, parked>>

(***************************************************************************)
(* Land(r): an admitted delivery reaches the peer's tree and routes to the *)
(* copy the peer's alias resolves to NOW. The apply seam refuses it when   *)
(* that copy is closed, or when the delivery was admitted under an epoch   *)
(* below the copy's floor; a refusal is deferred, so the sender's cursor   *)
(* does not move and it re-ships. Otherwise it is applied and acked.       *)
(***************************************************************************)
Land(r) ==
    /\ r \in inflight
    /\ inflight' = inflight \ {r}
    /\ IF LET d == Peer(r.from)
              p == alias[d]
          IN \/ p \in closed[d]
             \/ r.ep < floor[d][p]
         THEN UNCHANGED <<data, sent>>
         ELSE /\ data' = [data EXCEPT ![Peer(r.from)][alias[Peer(r.from)]] = @ \cup {r.w}]
              /\ sent' = [sent EXCEPT ![r.from] = @ \cup {r.w}]
    /\ UNCHANGED <<alias, log, bound, shipOn, recvOn, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, epoch, closed, floor, parked>>

(***************************************************************************)
(* Park(r): the delivery's causal dependencies are unmet, so the receiver  *)
(* parks it in its durable causal-apply buffer and acknowledges it. The    *)
(* park re-reads the fence uncached. If the fence is in fact paused, or a  *)
(* pause has happened since the delivery was admitted, the delivery is    *)
(* deferred instead (the sender re-ships it and a fresh admission stamps  *)
(* it again); otherwise the entry is parked under the fence epoch it read. *)
(* Not fair: a delivery whose dependencies are met is never parked.        *)
(***************************************************************************)
Park(r) ==
    /\ r \in inflight
    /\ inflight' = inflight \ {r}
    /\ LET d == Peer(r.from)
       IN IF ~recvOn[d] \/ r.ep < epoch[d]
            THEN UNCHANGED <<sent, parked>>
            ELSE /\ parked' = [parked EXCEPT ![d] = @ \cup {[w |-> r.w, ep |-> epoch[d]]}]
                 /\ sent' = [sent EXCEPT ![r.from] = @ \cup {r.w}]
    /\ UNCHANGED <<alias, data, log, bound, shipOn, recvOn, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, epoch, closed, floor>>

(***************************************************************************)
(* Drain(d, q): the parked entry's dependencies are now met. It routes to  *)
(* the copy d's alias resolves to; while that copy is closed the entry     *)
(* stays parked. An entry parked under an epoch below the copy's floor was *)
(* parked before the restore's pause: no peer ships a post-cutover write   *)
(* before the saga completes, so it is a pre-cutover write and is          *)
(* discarded. Otherwise it is applied.                                     *)
(***************************************************************************)
Drain(d, q) ==
    /\ q \in parked[d]
    /\ alias[d] \notin closed[d]
    /\ parked' = [parked EXCEPT ![d] = @ \ {q}]
    /\ IF q.ep < floor[d][alias[d]]
         THEN UNCHANGED data
         ELSE data' = [data EXCEPT ![d][alias[d]] = @ \cup {q.w}]
    /\ UNCHANGED <<alias, log, bound, sent, shipOn, recvOn, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, epoch, closed, floor, inflight>>

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
    /\ UNCHANGED fence

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
    /\ UNCHANGED fence

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
    /\ UNCHANGED fence

(***************************************************************************)
(* Commit(c): c's participant engages the fence: it pauses receiving      *)
(* (bumping the fence epoch), closes the restored copy with that epoch as  *)
(* its floor, pauses writes and shipping, swaps the alias to the shadow,  *)
(* and unblocks local writes. Shipping and receiving stay paused. The      *)
(* swap pushes a source-identity change to the shipper, which may be lost *)
(* (it is best-effort); a delivered push rebinds and resets the cursors.   *)
(***************************************************************************)
Commit(c) ==
    /\ rphase = "commit"
    /\ alias[c] = "old"
    /\ alias' = [alias EXCEPT ![c] = "new"]
    /\ shipOn' = [shipOn EXCEPT ![c] = FALSE]
    /\ recvOn' = [recvOn EXCEPT ![c] = FALSE]
    /\ epoch' = [epoch EXCEPT ![c] = @ + 1]
    /\ closed' = [closed EXCEPT ![c] = @ \cup {"new"}]
    /\ floor' = [floor EXCEPT ![c]["new"] = epoch[c] + 1]
    /\ \E pushed \in BOOLEAN :
         /\ bound' = IF pushed THEN [bound EXCEPT ![c] = "new"] ELSE bound
         /\ sent' = IF pushed THEN [sent EXCEPT ![c] = {}] ELSE sent
    /\ UNCHANGED <<data, log, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, inflight, parked>>

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
    /\ UNCHANGED fence

(***************************************************************************)
(* Complete: the saga completes globally - every cluster has cut over, or  *)
(* every prepared cluster has compensated.                                 *)
(***************************************************************************)
Complete ==
    /\ \/ rphase = "commit" /\ \A c \in Clusters : alias[c] = "new"
       \/ rphase = "abort" /\ \A c \in Clusters : vote[c] \in {"no", "comp"}
    /\ rphase' = "done"
    /\ UNCHANGED <<alias, data, log, bound, sent, shipOn, recvOn, vote, pre, post>>
    /\ UNCHANGED fence

(***************************************************************************)
(* Resume(c): c's fence grain observes global completion and resumes       *)
(* shipping and receiving together, and opens the restored copies it      *)
(* closed (SagaWriteFenceGrain release point 2). The floors stay.         *)
(***************************************************************************)
Resume(c) ==
    /\ rphase = "done"
    /\ ~shipOn[c] \/ ~recvOn[c]
    /\ shipOn' = [shipOn EXCEPT ![c] = TRUE]
    /\ closed' = [closed EXCEPT ![c] = {}]
    /\ recvOn' = [recvOn EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<alias, data, log, bound, sent, rphase, vote, pre, post>>
    /\ UNCHANGED <<seen, epoch, floor, inflight, parked>>

Quiesced ==
    /\ rphase = "done"
    /\ pre \cup post = Writes
    /\ inflight = {}
    /\ \A c \in Clusters :
         /\ shipOn[c] /\ recvOn[c]
         /\ bound[c] = alias[c]
         /\ log[c][bound[c]] \subseteq sent[c]
         /\ parked[c] = {}

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E w \in Writes : Write(w)
    \/ \E c \in Clusters : Refresh(c)
    \/ \E c \in Clusters : Ship(c)
    \/ \E r \in inflight : Land(r)
    \/ \E r \in inflight : Park(r)
    \/ \E c \in Clusters : \E q \in parked[c] : Drain(c, q)
    \/ \E c \in Clusters : Rebind(c)
    \/ \E c \in Clusters : Build(c)
    \/ Decide
    \/ \E c \in Clusters : Commit(c)
    \/ \E c \in Clusters : Abort(c)
    \/ Complete
    \/ \E c \in Clusters : Resume(c)
    \/ Stutter

(***************************************************************************)
(* Fairness: the saga and replication make progress, the gate cache        *)
(* refreshes, admitted deliveries land, and parked entries drain. Writes   *)
(* and parking are not fair.                                               *)
(***************************************************************************)
Progress ==
    \/ \E c \in Clusters : Ship(c) \/ Rebind(c) \/ Build(c) \/ Commit(c) \/ Abort(c) \/ Resume(c)
    \/ Decide
    \/ Complete

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)
        /\ WF_vars(\E r \in inflight : Land(r))
        /\ \A c \in Clusters : /\ WF_vars(Ship(c)) /\ WF_vars(Rebind(c)) /\ WF_vars(Resume(c))
                               /\ WF_vars(Refresh(c))
                               /\ WF_vars(\E q \in parked[c] : Drain(c, q))

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