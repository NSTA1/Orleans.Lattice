------------------------ MODULE ReplicationCausalDelivery ------------------------
(***************************************************************************)
(* A focused companion to Replication.tla: causal-dependency delivery to a *)
(* receiver whose shippers block at the head of the line. Replication.tla  *)
(* checks the whole pipeline over two writes; this module checks the one   *)
(* question that needs more - whether a receiver can deadlock waiting for  *)
(* dependencies when every shipper stops at an entry the receiver will not *)
(* acknowledge. A cross-origin cycle needs four writes:                    *)
(*                                                                         *)
(*   b writes b1, learns a1, writes b2 depending on a1;                    *)
(*   a writes a1, learns b1, writes a2 depending on b1;                    *)
(*   b ships b2 before b1 and a ships a2 before a1.                        *)
(*                                                                         *)
(* Shipping order may differ from authoring order because production       *)
(* merges several key-hashed WAL partitions by per-leaf HLC, and nothing   *)
(* orders two leaves' clocks (#1060). A shipper delivers one entry at a    *)
(* time and moves on only when it is acknowledged: a not-accepted          *)
(* acknowledgement makes ReplicationShipperGrain back off and re-ship the  *)
(* same batch, so a deferred entry stalls everything behind it.            *)
(*                                                                         *)
(* The intended design (issue #4464) acknowledges a parked entry, keeps it *)
(* in a durable buffer and drains it once its dependency is met, so the    *)
(* cycle above converges. The alternative of withholding the ack until the *)
(* entry can be applied deadlocks on it; that stands as the mutation       *)
(* EventualConvergenceDeferredParkStalls.                                  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS a, b, c

Writers == {a, b}
Receiver == c
MaxHlc == 2
Hlcs == 1..MaxHlc
WritesPerWriter == 2
Other(o) == IF o = a THEN b ELSE a

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A n \in S : n <= m

(***************************************************************************)
(* A write: its origin, its leaf HLC, and an optional dependency on the    *)
(* other writer's frontier (the highest HLC of it the author had applied), *)
(* as production's WalRecord.VectorClock carries one. NoDep is the null    *)
(* frontier.                                                               *)
(***************************************************************************)
NoDep == [o |-> c, h |-> 0]
DepDomain == {NoDep} \cup [o : Writers, h : Hlcs]
Writes == [o : Writers, h : Hlcs, d : DepDomain]

VARIABLES
    authored,   \* every write ever authored
    known,      \* known[x]: the writes cluster x holds (its own and those it learned)
    log,        \* log[s]: s's writes in shipping order
    cursor,     \* cursor[s]: entries of log[s] the receiver has acknowledged
    applied,    \* the writes merged at the receiver
    hwm,        \* hwm[o]: the receiver's high-water mark for origin o
    buf,        \* the receiver's causal buffer
    wake        \* a drain of the receiver's buffer is pending

vars == <<authored, known, log, cursor, applied, hwm, buf, wake>>

DepsOk(w) == IF w.d = NoDep THEN TRUE ELSE hwm[w.d.o] >= w.d.h

DepChoices(o) ==
    LET seen == {v.h : v \in {u \in known[o] : u.o = Other(o)}}
    IN IF seen = {} THEN {NoDep} ELSE {NoDep, [o |-> Other(o), h |-> Max(seen)]}

TypeOK ==
    /\ authored \subseteq Writes
    /\ known \in [Writers -> SUBSET Writes]
    /\ \A s \in Writers : log[s] \in Seq(Writes) /\ Len(log[s]) <= WritesPerWriter
    /\ cursor \in [Writers -> 0..WritesPerWriter]
    /\ applied \subseteq Writes
    /\ hwm \in [Writers -> 0..MaxHlc]
    /\ buf \subseteq Writes
    /\ wake \in BOOLEAN

Init ==
    /\ authored = {}
    /\ known = [x \in Writers |-> {}]
    /\ log = [s \in Writers |-> <<>>]
    /\ cursor = [s \in Writers |-> 0]
    /\ applied = {}
    /\ hwm = [o \in Writers |-> 0]
    /\ buf = {}
    /\ wake = FALSE

(***************************************************************************)
(* Author(o, h, d, p): o commits a write with an HLC its own writes have   *)
(* not used and an optional dependency on the other writer's frontier it   *)
(* holds, and the write takes shipping position p - anywhere the shipper   *)
(* has not yet passed, because merging partitions by HLC can ship it ahead *)
(* of an earlier write on a slower leaf.                                   *)
(***************************************************************************)
Author(o, h, d, p) ==
    /\ Len(log[o]) < WritesPerWriter
    /\ h \notin {w.h : w \in {v \in authored : v.o = o}}
    /\ d \in DepChoices(o)
    /\ p \in (cursor[o] + 1)..(Len(log[o]) + 1)
    /\ LET w == [o |-> o, h |-> h, d |-> d]
       IN /\ authored' = authored \cup {w}
          /\ known' = [known EXCEPT ![o] = @ \cup {w}]
          /\ log' = [log EXCEPT ![o] = SubSeq(@, 1, p - 1) \o <<w>> \o SubSeq(@, p, Len(@))]
    /\ UNCHANGED <<cursor, applied, hwm, buf, wake>>

(***************************************************************************)
(* Learn(x, w): a writer applies the other writer's write over their own   *)
(* link. That link is not this module's subject - Replication.tla checks   *)
(* it - so a learn is a single step; it exists so dependencies can form.   *)
(***************************************************************************)
Learn(x, w) ==
    /\ w \in authored
    /\ w.o # x
    /\ w \notin known[x]
    /\ known' = [known EXCEPT ![x] = @ \cup {w}]
    /\ UNCHANGED <<authored, log, cursor, applied, hwm, buf, wake>>

(***************************************************************************)
(* Deliver(s): s's shipper delivers the entry at the head of its line and  *)
(* the receiver runs its pipeline: an entry it holds already is a          *)
(* duplicate; one whose dependency is met is merged and the high-water     *)
(* mark advances; one whose dependency is not met is parked. Every outcome *)
(* is acknowledged, which moves the head of the line - the intended        *)
(* design, under which a parked entry is held durably. A shipper sends     *)
(* one entry at a time: a larger batch could carry an entry past one the   *)
(* receiver defers, and the bounded instance excludes that rescue.         *)
(***************************************************************************)
Deliver(s) ==
    LET i == cursor[s] + 1
        w == log[s][i]
    IN /\ i <= Len(log[s])
       /\ \/ /\ w \in applied \cup buf
             /\ cursor' = [cursor EXCEPT ![s] = i]
             /\ UNCHANGED <<applied, hwm, buf, wake>>
          \/ /\ w \notin applied \cup buf
             /\ DepsOk(w)
             /\ applied' = applied \cup {w}
             /\ hwm' = [hwm EXCEPT ![w.o] = Max({@, w.h})]
             /\ wake' = (wake \/ (w.h > hwm[w.o] /\ buf # {}))
             /\ cursor' = [cursor EXCEPT ![s] = i]
             /\ UNCHANGED buf
          \/ /\ w \notin applied \cup buf
             /\ ~DepsOk(w)
             /\ buf' = buf \cup {w}
             /\ wake' = TRUE
             /\ cursor' = [cursor EXCEPT ![s] = i]
             /\ UNCHANGED <<applied, hwm>>
       /\ UNCHANGED <<authored, known, log>>

(***************************************************************************)
(* Drain: DrainBufferAsync, one released entry per step, while a drain is  *)
(* pending.                                                                *)
(***************************************************************************)
Drain ==
    /\ wake
    /\ IF \E r \in buf : DepsOk(r)
       THEN \E r \in buf :
               /\ DepsOk(r)
               /\ buf' = buf \ {r}
               /\ applied' = applied \cup {r}
               /\ hwm' = [hwm EXCEPT ![r.o] = Max({@, r.h})]
               /\ UNCHANGED wake
       ELSE /\ wake' = FALSE
            /\ UNCHANGED <<buf, applied, hwm>>
    /\ UNCHANGED <<authored, known, log, cursor>>

Quiesced ==
    /\ \A s \in Writers : cursor[s] = Len(log[s])
    /\ buf = {}
    /\ ~wake

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E o \in Writers, h \in Hlcs, d \in DepDomain, p \in 1..WritesPerWriter : Author(o, h, d, p)
    \/ \E x \in Writers, w \in Writes : Learn(x, w)
    \/ \E s \in Writers : Deliver(s)
    \/ Drain
    \/ Stutter

(***************************************************************************)
(* Fairness: shippers keep delivering and a pending drain runs. Authoring  *)
(* and learning are environment events.                                    *)
(***************************************************************************)
Spec == Init /\ [][Next]_vars /\ \A s \in Writers : WF_vars(Deliver(s)) /\ WF_vars(Drain)

(* EventualConvergence: every write is eventually merged at the receiver. *)
EventualConvergence == <>[](authored \subseteq applied)

=============================================================================
