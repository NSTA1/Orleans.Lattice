----------------------- MODULE AtomicCommitCrossCluster -----------------------
(***************************************************************************)
(* An abstract TLA+ specification of Orleans.Lattice cross-cluster atomic  *)
(* visibility: a saga committed on an ORIGIN cluster and replicated to a   *)
(* RECEIVER cluster, and the receiver-side protocol that keeps the         *)
(* replicated saga all-or-nothing visible there.                           *)
(*                                                                         *)
(* The origin runs the single-cluster saga of AtomicCommit.tla unchanged:  *)
(* this module INSTANCEs that module and calls its PrepareTx, DecideTx,    *)
(* BroadcastStep and ForgetDecision actions for one saga, t1, so a change  *)
(* to the single-cluster protocol reaches this module without a copy to    *)
(* drift. What this module adds is everything after the origin's WAL:      *)
(*                                                                         *)
(*   - replication of each prepare and each per-source-shard terminal over *)
(*     a transport that can drop, duplicate and reorder;                   *)
(*   - the receiver's per-source-shard terminal tally (the registry's      *)
(*     RecordTerminalArrivalAsync, driven from the replication apply       *)
(*     grain's ApplyTxTerminalAsync), including the legacy "no count"      *)
(*     path that marks on the first terminal;                              *)
(*   - the cross-tree receiver barrier (LatticeCrossTreeReceiverGrain) and *)
(*     the receiver registry's delegation to it, with the dial-back that   *)
(*     resolves to Indeterminate when the barrier cannot be reached;       *)
(*   - the two delegation maps (ExternalAuthorities on the authoring side, *)
(*     ReceiverDecisionAuthorities on the receiving side) and their        *)
(*     disjointness;                                                       *)
(*   - bootstrap of a fresh receiver from a snapshot export, including the *)
(*     aged-out committed export vocabulary (issues #2328 and #2318).      *)
(*                                                                         *)
(* Every property here is a claim about the RECEIVER. The origin's own     *)
(* properties are AtomicCommit.tla's, checked there; nothing below should  *)
(* be read as re-checking them, and nothing in AtomicCommit.tla covers     *)
(* the receiver (issue #2324). See Refinement.md in this directory, the    *)
(* cross-cluster note, for the mapping to production code and tests.       *)
(*                                                                         *)
(* Epic #4430, issue #4436.                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* The origin's model values. AtomicCommit fixes its instance (two sagas   *)
(* over three keys); this module drives only t1, whose participant set is  *)
(* {k1, k2}. Each written key is identified with the source shard that     *)
(* holds it, so a "source-shard terminal" is a terminal for one key. t2    *)
(* never leaves "init": the cross-saga interactions on a shared key are    *)
(* AtomicCommit's concern, and one saga is all the receiver protocol needs *)
(* to exhibit a split view, a reversion or a stranded prepare.             *)
(***************************************************************************)
CONSTANTS t1, t2, k1, k2, k3

\* AtomicCommit's state, carried under its own names so the INSTANCE below
\* substitutes it implicitly.
VARIABLES phase, vote, decision, terminal, pend, orphanDone, forgotten, masked, revision

Origin == INSTANCE AtomicCommit

T == t1
XKeys == Origin!Written(T)

(***************************************************************************)
(* Cross-cluster state.                                                    *)
(*                                                                         *)
(*  xtree       the shape of the replicated saga, fixed for a behaviour:   *)
(*              FALSE is a single-tree saga over two source shards of one  *)
(*              tree; TRUE is a cross-tree saga whose two keys live on two *)
(*              trees, one shard each, under one cross-tree operation.     *)
(*  oext[tr]    the ORIGIN registry of tree tr holds an ExternalAuthorities*)
(*              row for the saga (a cross-tree sub-saga delegating its     *)
(*              status to the authoring coordinator until its local mark). *)
(*  orcv[tr]    the ORIGIN registry of tree tr holds a                     *)
(*              ReceiverDecisionAuthorities row for the saga. Only the     *)
(*              public apply seam, handed the origin's own terminal, could *)
(*              write one; see ForeignOriginClaim.                         *)
(*  outbox      replication records the origin has written to its WAL and *)
(*              the receiver has not acknowledged. The sender keeps a      *)
(*              record until it is acked, so a lost delivery is a record   *)
(*              still here, and a lost ack is a duplicate delivery.        *)
(*  dlv         records delivered at least once (the history the per-shard *)
(*              ordering assumption is stated against).                    *)
(*  rconn       the receiver is attached to the stream. A receiver that    *)
(*              starts detached joins through Bootstrap.                   *)
(*  rpend[k]    a receiver leaf holds the saga's prepared bucket for k.    *)
(*  rterm[k]    the terminal a receiver leaf has applied for the saga (its *)
(*              recently-terminal memory: the late-prepare refusal's and   *)
(*              the orphan guard's input).                                 *)
(*  rproj[k]    the receiver leaf's materialised projection of k: "post"   *)
(*              once the saga's write is in it, "pre" otherwise. Kept      *)
(*              apart from rterm because a terminal applied to a leaf with *)
(*              no bucket records the terminal and materialises nothing.   *)
(*  rarr[tr]    the receiver registry's tally of source shards whose       *)
(*              terminal has arrived (TxRegistryState.TerminalArrivals).   *)
(*  rexp[tr]    the merged expected count (ExpectedTerminals); 0 = none.   *)
(*  rdec[tr]    the receiver registry's local decision for the saga.       *)
(*  rout[tr]    the outcome tree tr's tally completed with, carried into   *)
(*              the cross-tree barrier.                                    *)
(*  rstage[tr]  where tree tr is in the cross-tree hand-off: "register"    *)
(*              (tally final, delegation not yet written), "notify"        *)
(*              (delegation written, barrier not yet told), "done".        *)
(*  rdeleg[tr]  the receiver registry holds a ReceiverDecisionAuthorities  *)
(*              row delegating the saga's status to the barrier.           *)
(*  rdial[tr]   the receiver registry currently cannot reach the barrier.  *)
(*  carr[tr]    the barrier's recorded arrival for tree tr.                *)
(*  cdec        the barrier's durable, published decision.                 *)
(*  rfin        trees the barrier has told to finalise and that have not   *)
(*              yet marked their registry.                                 *)
(*  rtodo       receiver leaves still owed the saga's terminal by a        *)
(*              finalised registry (the post-gate fan-out).                *)
(***************************************************************************)
VARIABLES xtree, oext, orcv, outbox, dlv, rconn,
          rpend, rterm, rproj,
          rarr, rexp, rdec, rout, rstage, rdeleg, rdial,
          carr, cdec, rfin, rtodo

originVars == <<phase, vote, decision, terminal, pend, orphanDone, forgotten, masked, revision>>

netVars == <<oext, orcv, outbox, dlv, rconn>>

leafVars == <<rpend, rterm, rproj>>

registryVars == <<rarr, rexp, rdec, rout, rstage, rdeleg, rdial>>

barrierVars == <<carr, cdec, rfin, rtodo>>

xvars == <<xtree, netVars, leafVars, registryVars, barrierVars>>

vars == <<originVars, xvars>>

Trees == {"A", "B"}

\* Single-tree: both source shards belong to tree A. Cross-tree: k1 is
\* tree A's only participant and k2 is tree B's.
TreeOf(k) == IF xtree /\ k = k2 THEN "B" ELSE "A"

KeysOf(tr) == {k \in XKeys : TreeOf(k) = tr}

\* The participant trees the barrier waits for. ReplicationApplier scopes it
\* to the participants replicated on the receiver; both are, here.
WaitSet == {TreeOf(k) : k \in XKeys}

\* The touched-shard count AtomicWriteGrain stamps on every terminal of a
\* (sub-)saga: the number of source shards that sub-saga touched.
ShardCount(tr) == Cardinality(KeysOf(tr))

Max(a, b) == IF a >= b THEN a ELSE b

Outcome(c) == IF c THEN "committed" ELSE "aborted"

Kind(o) == IF o = "committed" THEN "commit" ELSE "abort"

(***************************************************************************)
(* Replication records. A prepare carries its key; a terminal carries its  *)
(* source shard (its key, here), its outcome and the touched-shard count   *)
(* stamped on it (0 = unstamped, the legacy path). Every record has the    *)
(* same fields so the set is homogeneous.                                  *)
(***************************************************************************)
Prep(k) == [type |-> "prep", key |-> k, commit |-> FALSE, count |-> 0]

Term(k, c, n) == [type |-> "term", key |-> k, commit |-> c, count |-> n]

MsgSet == {Prep(k) : k \in XKeys} \cup {Term(k, c, n) : k \in XKeys, c \in BOOLEAN, n \in 0..2}

TypeOK ==
    /\ Origin!TypeOK
    /\ xtree \in BOOLEAN
    /\ oext \in [Trees -> BOOLEAN]
    /\ orcv \in [Trees -> BOOLEAN]
    /\ outbox \subseteq MsgSet
    /\ dlv \subseteq MsgSet
    /\ rconn \in BOOLEAN
    /\ rpend \in [XKeys -> {"none", "pending"}]
    /\ rterm \in [XKeys -> {"none", "commit", "abort"}]
    /\ rproj \in [XKeys -> {"pre", "post"}]
    /\ rarr \in [Trees -> SUBSET XKeys]
    /\ rexp \in [Trees -> 0..2]
    /\ rdec \in [Trees -> {"inflight", "committed", "aborted"}]
    /\ rout \in [Trees -> {"none", "committed", "aborted"}]
    /\ rstage \in [Trees -> {"idle", "register", "notify", "done"}]
    /\ rdeleg \in [Trees -> BOOLEAN]
    /\ rdial \in [Trees -> BOOLEAN]
    /\ carr \in [Trees -> {"none", "committed", "aborted"}]
    /\ cdec \in {"inflight", "committed", "aborted"}
    /\ rfin \subseteq Trees
    /\ rtodo \subseteq XKeys

(***************************************************************************)
(* Receiver reader visibility.                                             *)
(*                                                                         *)
(* RView(tr) is what tree tr's receiver registry answers for the saga      *)
(* (TxRegistryGrain.GetStatusAsync): its local decision if it has one;     *)
(* otherwise, if the saga is delegated to the cross-tree barrier, the      *)
(* barrier's published decision - or "indeterminate" when the barrier      *)
(* cannot be dialled (ResolveReceiverDelegatedAsync's catch); otherwise    *)
(* "inflight", the answer for a txid the registry has never heard of.      *)
(*                                                                         *)
(* RObserved(k) is the receiver leaf's read through AtomicVisibilityGate,  *)
(* exactly as AtomicCommit's Observed: a pending bucket is hidden under    *)
(* Indeterminate, surfaces post-saga under Committed unless the leaf has   *)
(* already applied a terminal, and otherwise falls through to the          *)
(* projection.                                                             *)
(***************************************************************************)
RView(tr) ==
    IF rdec[tr] # "inflight" THEN rdec[tr]
    ELSE IF rdeleg[tr] THEN (IF rdial[tr] THEN "indeterminate" ELSE cdec)
    ELSE "inflight"

RObserved(k) ==
    IF rpend[k] = "pending"
    THEN IF RView(TreeOf(k)) = "indeterminate" THEN "hidden"
         ELSE IF RView(TreeOf(k)) = "committed" /\ rterm[k] = "none" THEN "post"
         ELSE rproj[k]
    ELSE rproj[k]

Init ==
    /\ Origin!Init
    /\ xtree \in BOOLEAN
    \* A receiver either follows the stream from the saga's start or joins
    \* later through a bootstrap snapshot. Bootstrap is modelled for the
    \* single-tree shape only (see Bootstrap).
    /\ rconn \in IF xtree THEN {TRUE} ELSE BOOLEAN
    /\ oext = [tr \in Trees |-> FALSE]
    /\ orcv = [tr \in Trees |-> FALSE]
    /\ outbox = {}
    /\ dlv = {}
    /\ rpend = [k \in XKeys |-> "none"]
    /\ rterm = [k \in XKeys |-> "none"]
    /\ rproj = [k \in XKeys |-> "pre"]
    /\ rarr = [tr \in Trees |-> {}]
    /\ rexp = [tr \in Trees |-> 0]
    /\ rdec = [tr \in Trees |-> "inflight"]
    /\ rout = [tr \in Trees |-> "none"]
    /\ rstage = [tr \in Trees |-> "idle"]
    /\ rdeleg = [tr \in Trees |-> FALSE]
    /\ rdial = [tr \in Trees |-> FALSE]
    /\ carr = [tr \in Trees |-> "none"]
    /\ cdec = "inflight"
    /\ rfin = {}
    /\ rtodo = {}

(***************************************************************************)
(* ORIGIN ACTIONS. Each is AtomicCommit's action for t1, plus what that    *)
(* step writes to the origin's WAL for replication.                        *)
(***************************************************************************)

\* The prepare fan-out. Every written key's prepared write is a WAL record
\* (IsPrepared, carrying the transaction id) and so a replication record. A
\* cross-tree sub-saga registers its ExternalAuthorities row before it
\* prepares, delegating its tree's status to the authoring coordinator.
OriginPrepare ==
    /\ Origin!PrepareTx(T)
    /\ outbox' = outbox \cup {Prep(k) : k \in XKeys}
    /\ oext' = IF xtree THEN [tr \in Trees |-> tr \in WaitSet] ELSE oext
    /\ UNCHANGED <<xtree, orcv, dlv, rconn, leafVars, registryVars, barrierVars>>

\* The origin records its single decision. Nothing is replicated by the
\* decision itself: a receiver learns the outcome only from terminals.
OriginDecide ==
    /\ Origin!DecideTx(T)
    /\ UNCHANGED xvars

\* One source shard applies the saga's terminal, and its terminal record -
\* stamped with the outcome and the touched-shard count - is written to the
\* origin WAL (ShardRootGrain.AppendTxTerminalAsync appends it before the
\* leaf fan-out). A cross-tree sub-saga marks its own registry before its
\* terminals, which drops its ExternalAuthorities row.
OriginBroadcast(k) ==
    /\ Origin!BroadcastStep(T, k)
    /\ outbox' = outbox \cup {Term(k, phase[T] = "committing", ShardCount(TreeOf(k)))}
    /\ oext' = IF xtree THEN [oext EXCEPT ![TreeOf(k)] = FALSE] ELSE oext
    /\ UNCHANGED <<xtree, orcv, dlv, rconn, leafVars, registryVars, barrierVars>>

\* The origin's post-fan-out cleanup retires the registry row, under
\* AtomicCommit's own ordering: only once every written key has applied its
\* terminal. Its only consequence for the receiver is what a later bootstrap
\* export reports for the saga.
OriginForget ==
    /\ Origin!ForgetDecision(T)
    /\ UNCHANGED xvars

(***************************************************************************)
(* TRANSPORT AND RECEIVER APPLY.                                           *)
(*                                                                         *)
(* The transport may deliver any un-acknowledged record at any time        *)
(* (reorder), may lose a delivery (the record stays in the outbox and is   *)
(* shipped again), and may lose the acknowledgement of a delivery that did *)
(* apply (the record stays and is delivered again: a duplicate). It is     *)
(* ASSUMED never to lose a record outright: the sender keeps it until it   *)
(* is applied and acked. Production violates that assumption: the         *)
(* receiver acknowledges a saga record its dead-letter applier parked     *)
(* (issue #4591), and the WAL can trim an entry the shipper has not read   *)
(* (issues #4579 and #4534). A lost prepare whose terminal still ships     *)
(* splits the receiver (RAllOrNothingPrepareAckedUnapplied); a lost        *)
(* terminal strands its buckets (RNoStrandedPrepareShipperDropsTerminal).  *)
(* A prepare the shipper itself dead-letters poisons its saga instead      *)
(* (issue #4494, fixed by #4570), which keeps the receiver safe but not    *)
(* live.                                                                   *)
(*                                                                         *)
(* One ordering is assumed, and it is the only constraint on reordering:   *)
(* a source shard's terminal is not delivered before every prepare that    *)
(* shard wrote and that is still outstanding. Production provides a        *)
(* stronger, saga-wide form: the shipper holds every terminal it reads     *)
(* until the peer has acknowledged every prepare of that saga the WAL      *)
(* holds (ReplicationShipperGrain's terminal hold, issue #4480), so a      *)
(* terminal never reaches the receiver ahead of a prepare of its saga.     *)
(* The guard is stated over OUTSTANDING records, so a prepare that left    *)
(* the outbox cannot hold its terminal back for ever                       *)
(* (RNoStrandedPrepareHoldWaitsOnUnshippedPrepare). That is safe only      *)
(* under the no-loss assumption above: a prepare that left the outbox      *)
(* without being applied releases its terminal over a key with no bucket.  *)
(* Lifting the guard is RAllOrNothingTerminalOvertakesPrepare, what        *)
(* production did before the hold.                                         *)
(***************************************************************************)

\* What the receiver's prepare seam reads before it stages
\* (LatticeGrain.TrySettleReplicatedPrepareAsync): the registry's status,
\* read through to the recorded decision when it answers Indeterminate.
SettleView(k) ==
    IF RView(TreeOf(k)) = "indeterminate" THEN rdec[TreeOf(k)] ELSE RView(TreeOf(k))

\* A prepare reaches the receiver (ReplicationApplier ->
\* IReplicationApplyGrain.ApplyPreparedSetAsync). The seam first settles it
\* against the receiver registry (issue #4482's fix): under a recorded commit
\* it is applied as a committed write, under an abort it is dropped, and only
\* an undecided saga's prepare is staged in a pending bucket. A prepare
\* arriving at a leaf that has already applied the saga's terminal is refused
\* (BPlusLeafGrain.IsLatePrepareForTerminalTransactionAsync), so a duplicate
\* trailing its terminal cannot install an orphan. The receiver registry never
\* forgets here, so that refusal sits behind the settle: a leaf has a
\* terminal only once its registry has decided.
DeliverPrepare(m) ==
    /\ rconn
    /\ m \in outbox
    /\ m.type = "prep"
    /\ dlv' = dlv \cup {m}
    /\ \/ outbox' = outbox \ {m}
       \/ outbox' = outbox
    /\ rpend' = IF SettleView(m.key) = "inflight" /\ rterm[m.key] = "none"
                THEN [rpend EXCEPT ![m.key] = "pending"] ELSE rpend
    /\ rproj' = IF SettleView(m.key) = "committed" THEN [rproj EXCEPT ![m.key] = "post"] ELSE rproj
    /\ UNCHANGED <<originVars, xtree, oext, orcv, rconn, rterm, registryVars, barrierVars>>

\* A source-shard terminal reaches the receiver: IReplicationApplyGrain.
\* ApplyTxTerminalAsync records it against the tree's registry tally
\* (ITxRegistryGrain.RecordTerminalArrivalAsync). A count of 0 is the legacy
\* path: final at once, no tally state. Otherwise the arrival is added to the
\* tally and the expected count merged upward (TerminalArrivalTally), and the
\* tally is final once every expected source shard has arrived. A conflicting
\* outcome against a recorded decision throws (TerminalDecisionGuard), which
\* leaves the record un-acked; no behaviour of the base reaches it.
\*
\* On a final tally a single-tree saga marks the receiver registry and owes
\* the terminal to every observed source shard's leaves. A cross-tree saga
\* instead hands off to the barrier: ReceiverRegister, then ReceiverNotify.
DeliverTerminal(m) ==
    /\ rconn
    /\ m \in outbox
    /\ m.type = "term"
    /\ \A p \in outbox : (p.type = "prep" /\ p.key = m.key) => p \in dlv
    /\ LET tr == TreeOf(m.key)
           o == Outcome(m.commit)
           legacy == m.count = 0
           arr2 == IF legacy THEN rarr[tr] ELSE rarr[tr] \cup {m.key}
           exp2 == IF legacy THEN rexp[tr]
                   ELSE IF rexp[tr] = 0 THEN m.count ELSE Max(rexp[tr], m.count)
           final == legacy \/ Cardinality(arr2) >= exp2
           observed == IF legacy THEN {m.key} ELSE arr2
       IN /\ rdec[tr] \in {"inflight", o}
          /\ rarr' = [rarr EXCEPT ![tr] = arr2]
          /\ rexp' = [rexp EXCEPT ![tr] = exp2]
          /\ IF ~final
             THEN UNCHANGED <<rdec, rout, rstage, rtodo>>
             ELSE IF ~xtree
             THEN /\ rdec' = [rdec EXCEPT ![tr] = o]
                  /\ rtodo' = rtodo \cup observed
                  /\ UNCHANGED <<rout, rstage>>
             ELSE /\ rout' = [rout EXCEPT ![tr] = o]
                  /\ rstage' = IF rstage[tr] = "idle" THEN [rstage EXCEPT ![tr] = "register"] ELSE rstage
                  /\ UNCHANGED <<rdec, rtodo>>
    /\ dlv' = dlv \cup {m}
    /\ \/ outbox' = outbox \ {m}
       \/ outbox' = outbox
    /\ UNCHANGED <<originVars, xtree, oext, orcv, rconn, leafVars, rdeleg, rdial, carr, cdec, rfin>>

(***************************************************************************)
(* THE CROSS-TREE RECEIVER BARRIER.                                        *)
(***************************************************************************)

\* Step (a) of the hand-off, strictly first: the tree's receiver registry
\* delegates the saga's status to the barrier
\* (RegisterReceiverDecisionAuthorityAsync). A local decision already
\* recorded supersedes the delegation, so registration is then a no-op.
ReceiverRegister(tr) ==
    /\ rstage[tr] = "register"
    /\ rdeleg' = IF rdec[tr] = "inflight" THEN [rdeleg EXCEPT ![tr] = TRUE] ELSE rdeleg
    /\ rstage' = [rstage EXCEPT ![tr] = "notify"]
    /\ UNCHANGED <<originVars, xtree, netVars, leafVars, rarr, rexp, rdec, rout, rdial, barrierVars>>

\* Step (b): the tree tells the barrier its terminal
\* (LatticeCrossTreeReceiverGrain.NotifyTerminalAsync). The barrier records
\* the arrival, and once every wait-set tree has arrived it decides - commit
\* iff every arrival committed - persists, publishes, and returns the whole
\* finalise set, every tree of which then marks its registry.
ReceiverNotify(tr) ==
    /\ rstage[tr] = "notify"
    /\ LET carr2 == [carr EXCEPT ![tr] = rout[tr]]
           complete == \A w \in WaitSet : carr2[w] # "none"
       IN /\ carr' = carr2
          /\ IF complete
             THEN /\ cdec' = IF \A w \in WaitSet : carr2[w] = "committed" THEN "committed" ELSE "aborted"
                  /\ rfin' = rfin \cup WaitSet
             ELSE UNCHANGED <<cdec, rfin>>
    /\ rstage' = [rstage EXCEPT ![tr] = "done"]
    /\ UNCHANGED <<originVars, xtree, netVars, leafVars, rarr, rexp, rdec, rout, rdeleg, rdial, rtodo>>

\* A tree the barrier decided for materialises its slice
\* (FinalizeCrossTreeTerminalCoreAsync): it marks its registry with the
\* barrier's verdict - which drops the delegation row - and owes the terminal
\* to its observed source shards' leaves.
ReceiverFinalize(tr) ==
    /\ tr \in rfin
    /\ rdec' = [rdec EXCEPT ![tr] = cdec]
    /\ rdeleg' = [rdeleg EXCEPT ![tr] = FALSE]
    /\ rdial' = [rdial EXCEPT ![tr] = FALSE]
    /\ rtodo' = rtodo \cup KeysOf(tr)
    /\ rfin' = rfin \ {tr}
    /\ UNCHANGED <<originVars, xtree, netVars, leafVars, rarr, rexp, rout, rstage, carr, cdec>>

(***************************************************************************)
(* THE POST-GATE FAN-OUT.                                                  *)
(***************************************************************************)

\* One receiver leaf applies the saga's terminal, as its registry recorded it
\* (ApplyTerminalPostGateAsync -> AppendTxTerminalAsync -> the leaf's
\* ApplyTxTerminalAsync). A commit drains the bucket into the projection; an
\* abort discards it. A leaf with no bucket records the terminal and
\* materialises nothing. A leaf that already applied it is a no-op.
ReceiverFanOut(k) ==
    /\ k \in rtodo
    /\ rdec[TreeOf(k)] # "inflight"
    /\ IF rterm[k] # "none"
       THEN UNCHANGED leafVars
       ELSE /\ rterm' = [rterm EXCEPT ![k] = Kind(rdec[TreeOf(k)])]
            /\ rproj' = IF rdec[TreeOf(k)] = "committed" /\ rpend[k] = "pending"
                        THEN [rproj EXCEPT ![k] = "post"]
                        ELSE rproj
            /\ rpend' = [rpend EXCEPT ![k] = "none"]
    /\ rtodo' = rtodo \ {k}
    /\ UNCHANGED <<originVars, xtree, netVars, registryVars, carr, cdec, rfin>>

(***************************************************************************)
(* ENVIRONMENT.                                                            *)
(***************************************************************************)

\* The receiver registry's dial to the barrier fails, or recovers. Unfair,
\* and enabled whenever the saga is delegated: a failed grain call needs no
\* ordering against anything. While it is failing the delegated status reads
\* "indeterminate" and the gate hides the saga's keys.
DialFault(tr) ==
    /\ xtree
    /\ rdeleg[tr]
    /\ rdial' = [rdial EXCEPT ![tr] = ~rdial[tr]]
    /\ UNCHANGED <<originVars, xtree, netVars, leafVars, rarr, rexp, rdec, rout, rstage, rdeleg, barrierVars>>

\* A caller of the public apply seam hands an ORIGIN tree its own saga's
\* cross-tree terminal (a local origin, which ReplicationApplier drops but
\* the seam does not). The origin registry would register a receiver
\* delegation for a txid it already delegates as an authoring sub-saga;
\* TxRegistryGrain.ThrowIfWouldCoexist refuses it. Enabled for the whole
\* window in which the authoring row exists: before it there is no saga, and
\* after it the tree's local decision supersedes any registration.
ForeignOriginClaim(tr) ==
    /\ xtree
    /\ oext[tr]
    /\ orcv' = IF oext[tr] THEN orcv ELSE [orcv EXCEPT ![tr] = TRUE]
    /\ UNCHANGED <<originVars, xtree, oext, outbox, dlv, rconn, leafVars, registryVars, barrierVars>>

(***************************************************************************)
(* BOOTSTRAP. A fresh receiver joins through a snapshot export             *)
(* (LatticeSnapshotProvider), taken atomically here: the export freezes    *)
(* the origin registry's view of the saga (snap0) and ships                *)
(*   - for a saga snap0 has decided: the committed projection, so every    *)
(*     key post-saga on a commit and pre-saga on an abort, a bucket still   *)
(*     resident included, and a decision row the drain records in the      *)
(*     receiver registry (LatticeBootstrapCoordinatorGrain.                *)
(*     ApplySettledDecisionAsync, which never forgets it);                 *)
(*   - for a saga snap0 has as InFlight or absent: each origin bucket as a *)
(*     prepared row, and each drained key's projection.                    *)
(* A forgotten saga whose row is still stored is exported by its recorded  *)
(* verdict, aged out or not (issue #4481's fix). Its row is purged only    *)
(* once the origin WAL can no longer re-ship a prepare of the saga (issue  *)
(* #4508's fix), so the absent answer is possible only when no retained    *)
(* record is a prepare of the saga.                                        *)
(*                                                                         *)
(* The handoff to the incremental stream is at-least-once: the shipper     *)
(* resumes from its own cursors, so any subset of the records the origin   *)
(* WAL still retains from before the cut is shipped again (kept). A        *)
(* re-shipped prepare is settled against the decision row by               *)
(* DeliverPrepare, which is what keeps it from being stranded (issue       *)
(* #4482's fix). RNoStrandedPrepareBootstrapReshipsPreCut,                 *)
(* RNoStrandedPrepareDedupeOverPurgedDecision and                          *)
(* RAllOrNothingExportOverStrandedPrepare restore what production did      *)
(* before each of those fixes, and stand as their regression checks. No    *)
(* floor of any kind is modelled, for non-saga records or saga records:    *)
(* the receiver reads none (#4476), and this module ships only saga        *)
(* records.                                                                *)
(*                                                                         *)
(* Modelled for the single-tree shape only: the cross-tree barrier adds    *)
(* nothing a bootstrap changes, and the state space is kept for it.        *)
(***************************************************************************)
Snap0 ==
    IF forgotten[T] THEN {decision[T], "inflight"} ELSE {Origin!RegistryView(T)}

ExportRow(snap, k) ==
    IF snap = "committed" THEN "post"
    ELSE IF snap = "aborted" THEN "pre"
    ELSE IF pend[T][k] = "none" /\ terminal[T][k] = "commit" THEN "post"
    ELSE "pre"

ExportsPrepared(snap, k) ==
    snap \in {"inflight", "indeterminate"} /\ pend[T][k] = "pending"

Bootstrap ==
    /\ ~rconn
    /\ \E kept \in SUBSET outbox :
       \E snap \in Snap0 :
         \* A forgotten saga's row is purged only once no retained prepare of it
         \* remains to be shipped again (issue #4508's fix).
         /\ (forgotten[T] /\ snap = "inflight") => ~\E p \in kept : p.type = "prep"
         /\ rproj' = [k \in XKeys |-> ExportRow(snap, k)]
         /\ rpend' = [k \in XKeys |-> IF ExportsPrepared(snap, k) THEN "pending" ELSE "none"]
         /\ rdec' = [rdec EXCEPT !["A"] = IF snap \in {"committed", "aborted"} THEN snap ELSE @]
         /\ outbox' = kept
    /\ rconn' = TRUE
    /\ UNCHANGED <<originVars, xtree, oext, orcv, dlv, rterm, rarr, rexp, rout, rstage, rdeleg, rdial, barrierVars>>

(***************************************************************************)
(* Quiescence: the origin saga is done, every record has been acked, and   *)
(* the receiver owes nothing. Optional events (OriginForget, the           *)
(* environment) are not required to have run.                             *)
(***************************************************************************)
Quiesced ==
    /\ phase[T] = "done"
    /\ rconn
    /\ outbox = {}
    /\ rfin = {}
    /\ rtodo = {}
    /\ \A tr \in Trees : rstage[tr] \in {"idle", "done"}

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ OriginPrepare
    \/ OriginDecide
    \/ \E k \in XKeys : OriginBroadcast(k)
    \/ OriginForget
    \/ \E m \in MsgSet : DeliverPrepare(m)
    \/ \E m \in MsgSet : DeliverTerminal(m)
    \/ \E tr \in Trees : ReceiverRegister(tr)
    \/ \E tr \in Trees : ReceiverNotify(tr)
    \/ \E tr \in Trees : ReceiverFinalize(tr)
    \/ \E k \in XKeys : ReceiverFanOut(k)
    \/ \E tr \in Trees : DialFault(tr)
    \/ \E tr \in Trees : ForeignOriginClaim(tr)
    \/ Bootstrap
    \/ Stutter

(***************************************************************************)
(* Fairness. The origin saga progresses (AtomicCommit's own assumption).   *)
(* The transport is fair: a record that stays deliverable is eventually    *)
(* delivered AND acknowledged - losses and lost acks happen, but not       *)
(* forever. The receiver's own obligations (register, notify, finalise,    *)
(* fan out) and a pending bootstrap eventually run. OriginForget, the dial *)
(* faults and the foreign claim are unfair environment events.             *)
(***************************************************************************)
OriginProgress == OriginPrepare \/ OriginDecide \/ \E k \in XKeys : OriginBroadcast(k)

AckedDelivery ==
    \E m \in MsgSet : (DeliverPrepare(m) \/ DeliverTerminal(m)) /\ m \notin outbox'

ReceiverStep ==
    \/ \E tr \in Trees : ReceiverRegister(tr) \/ ReceiverNotify(tr) \/ ReceiverFinalize(tr)
    \/ \E k \in XKeys : ReceiverFanOut(k)

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(OriginProgress)
    /\ WF_vars(AckedDelivery)
    /\ WF_vars(ReceiverStep)
    /\ WF_vars(Bootstrap)

(***************************************************************************)
(* Safety invariants - claims about the receiver.                          *)
(***************************************************************************)

\* Receiver all-or-nothing visibility: no receiver reader sees one of the
\* saga's keys post-saga and another pre-saga, across trees as well as
\* within one. "hidden" is compatible with either, as in AtomicCommit.
RAllOrNothing ==
    ~\E a, b \in XKeys : RObserved(a) = "post" /\ RObserved(b) = "pre"

\* The receiver never surfaces a saga the origin did not commit: a key is
\* observed post-saga on the receiver only once the origin's decision is
\* committed.
RStrictIsolation ==
    \A k \in XKeys : RObserved(k) = "post" => decision[T] = "committed"

\* A receiver leaf applies a terminal only after its registry recorded the
\* saga's outcome, and only that outcome, which is the origin's.
RLinearizedTerminals ==
    \A k \in XKeys :
        rterm[k] # "none" =>
            /\ rdec[TreeOf(k)] = decision[T]
            /\ rterm[k] = Kind(decision[T])

\* No registry holds both delegation rows for the saga (issue #2353's
\* premise). The receiver registry's ExternalAuthorities map has no row for
\* a replicated txid - it authored nothing - so the claim with content is
\* the origin's: it never also holds a ReceiverDecisionAuthorities row.
DelegationsDisjoint ==
    \A tr \in Trees : ~(oext[tr] /\ orcv[tr])

(***************************************************************************)
(* Temporal properties.                                                    *)
(***************************************************************************)

\* Once a receiver reader has seen a key post-saga it never sees it
\* pre-saga again, through any number of hidden observations in between
\* (stated over the whole behaviour for the reason AtomicCommit's
\* MonotonicVisibility is).
RMonotonicVisibility ==
    \A k \in XKeys :
        [](RObserved(k) = "post" => [](RObserved(k) # "pre"))

\* Under a fair transport every replicated committed saga eventually becomes
\* visible on the receiver: every key materialised post-saga in the
\* receiver's projection. Stated over materialisation, as AtomicCommit's
\* EveryCommittedKeyReadable is, because a dial fault may hide a delegated
\* key for as long as it lasts and nothing bounds that.
RCommittedEventuallyVisible ==
    (decision[T] = "committed") ~> (\A k \in XKeys : rproj[k] = "post")

\* No prepared bucket is stranded on the receiver: every bucket the receiver
\* stages is eventually consumed by a terminal, committed or aborted.
RNoStrandedPrepare ==
    \A k \in XKeys : (rpend[k] = "pending") ~> (rpend[k] = "none")
=============================================================================
