-------------------------- MODULE ReplicationLowWatermark --------------------------
(***************************************************************************)
(* The causal-dependency check of #4586's fix, "(a'')": a downward-closed  *)
(* low watermark built by refusing late stamps, plus #4603's dead-letter   *)
(* backpressure and lost marks.                                            *)
(*                                                                         *)
(* One origin a ships its writes over several WAL partitions to receiver   *)
(* c; a second writer b ships writes that each depend on one write of a    *)
(* (the named write (a, t), the dependency semantics #4586 states). The    *)
(* receiver merges a dependent only when its dependency is met:            *)
(*                                                                         *)
(*   - on the exact identity it remembers (a bounded, evicting record); or *)
(*   - when t < S_a, the low watermark a ships, and (a, t) is not one of   *)
(*     the acknowledged a-writes held back unapplied (parked,              *)
(*     dead-lettered, or discarded as a lost mark).                        *)
(*                                                                         *)
(* S_a is downward-closed because each partition seals a floor F and       *)
(* refuses a fresh stamp below it; (F, O) is published atomically, and S_p *)
(* is F once the ACKED cursor passes O. S_a is the minimum over every      *)
(* (tree, partition) shipper, clamped below acked prepares without an      *)
(* acked terminal. The floor is enforced only after a capability gate has  *)
(* latched open on every silo; before that the check is identity-only.     *)
(*                                                                         *)
(* The base checks two partitions with no sagas, loss or bootstrap and one *)
(* dead-letter; each variant configuration enables one feature on one      *)
(* partition. See README.md.                                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

Parts == {1, 2}
MaxHlc == 2
Hlcs == 1..MaxHlc
Top == MaxHlc + 1
AWrites == 2
BWrites == 1
Sagas == FALSE
MaxLoss == 0
DlqCap == 1
MaxDeadLetters == 1
Bootstraps == FALSE
Kinds == {"w", "prep", "term"}

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A n \in S : n <= m
Min(S) == CHOOSE m \in S : \A n \in S : m <= n
NoPub == <<0, 0>>

VARIABLES
    aAuth, aCom,          \* origin a: HLCs authored, and committed (visible)
    alog, adr, aack,      \* a's WAL partitions; drained and acked cursors
    floor, pub, spart,    \* per partition: sealed floor, held publication, S_p
    capable, floorOn,     \* capable[p]: p's silo is floor-capable; the gate, latched
    bdeps, back,          \* b's dependent writes (each names an a-HLC); acked
    rApplied, ids,        \* receiver: a-writes merged; identities remembered
    dlq, lost, dls,       \* receiver: dead-lettered, lost marks, dead-letters so far
    bApplied, bBuf, bDead, \* receiver: b-writes merged, parked, dead-lettered
    srcv, wake,           \* receiver: recorded S from a (0: none); a drain is pending
    losses, booted

vars == <<aAuth, aCom, alog, adr, aack, floor, pub, spart, capable, floorOn,
          bdeps, back, rApplied, ids, dlq, lost, dls, bApplied, bBuf, bDead,
          srcv, wake, losses, booted>>

AckedEntries == UNION {{alog[p][i] : i \in 1..aack[p]} : p \in Parts}
AllEntries == UNION {{alog[p][i] : i \in 1..Len(alog[p])} : p \in Parts}
OpenIn(E) == {e.h : e \in {x \in E : x.k = "prep" /\ [k |-> "term", h |-> x.h] \notin E}}

(* S_a: the min over every (tree, partition) shipper of the floor of a   *)
(* publication its acked cursor passed, clamped below acked prepares     *)
(* whose terminal is not acked. Every a-write with HLC < S_a is acked.   *)
SOf == Min({spart[p] : p \in Parts} \cup OpenIn(AckedEntries))

Held == dlq \cup lost
MinHeld == IF Held = {} THEN Top ELSE Min(Held)

(* A dependency (a, t) names one write. It is met on its exact identity, *)
(* or once a's S has been received, t is below it - so the write is      *)
(* acknowledged - and the write is not one of the acknowledged ones held  *)
(* back unapplied.                                                        *)
DepsOk(t) ==
    \/ t \in ids
    \/ t < srcv /\ t \notin Held

TypeOK ==
    /\ aAuth \subseteq Hlcs /\ aCom \subseteq aAuth
    /\ \A p \in Parts : /\ alog[p] \in Seq([k : Kinds, h : Hlcs])
                        /\ aack[p] <= adr[p] /\ adr[p] <= Len(alog[p])
                        /\ floor[p] \in 0..Top /\ spart[p] \in 0..Top
    /\ bdeps \in Seq(Hlcs) /\ Len(bdeps) <= BWrites
    /\ rApplied \subseteq Hlcs /\ ids \subseteq rApplied
    /\ dlq \subseteq Hlcs /\ Cardinality(dlq) <= DlqCap
    /\ lost \subseteq Hlcs
    /\ srcv \in 0..Top

Init ==
    /\ aAuth = {} /\ aCom = {}
    /\ alog = [p \in Parts |-> <<>>]
    /\ adr = [p \in Parts |-> 0] /\ aack = [p \in Parts |-> 0]
    /\ floor = [p \in Parts |-> 0] /\ pub = [p \in Parts |-> NoPub] /\ spart = [p \in Parts |-> 0]
    /\ capable = [p \in Parts |-> FALSE] /\ floorOn = FALSE
    /\ bdeps = <<>> /\ back = 0
    /\ rApplied = {} /\ ids = {} /\ dlq = {} /\ lost = {} /\ dls = 0
    /\ bApplied = {} /\ bBuf = {} /\ bDead = {}
    /\ srcv = 0 /\ wake = FALSE /\ losses = 0 /\ booted = FALSE

Origin == <<aAuth, aCom, alog, floor, pub, spart, capable, floorOn>>
Wire == <<adr, aack, bdeps, back, losses>>
Rcv == <<rApplied, ids, dlq, lost, dls, bApplied, bBuf, bDead, srcv, wake, booted>>

(* The floor guard: once the gate has latched, a floor-capable silo      *)
(* refuses a fresh stamp below the partition's floor and the leaf         *)
(* re-stamps above it. A silo without the build cannot refuse.           *)
StampOk(p, h) == floorOn /\ capable[p] => h >= floor[p]

AuthorA(p, h, saga) ==
    /\ Cardinality(aAuth) < AWrites
    /\ h \notin aAuth
    /\ StampOk(p, h)
    /\ saga \in (IF Sagas THEN BOOLEAN ELSE {FALSE})
    /\ aAuth' = aAuth \cup {h}
    /\ alog' = [alog EXCEPT ![p] = Append(@, [k |-> IF saga THEN "prep" ELSE "w", h |-> h])]
    /\ aCom' = IF saga THEN aCom ELSE aCom \cup {h}
    /\ UNCHANGED <<floor, pub, spart, capable, floorOn>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

(* A saga's terminal: re-stamped above any floor, the write keeps its    *)
(* prepare's HLC (#4566).                                                *)
CommitA(h, p) ==
    /\ h \in aAuth \ aCom
    /\ aCom' = aCom \cup {h}
    /\ alog' = [alog EXCEPT ![p] = Append(@, [k |-> "term", h |-> h])]
    /\ UNCHANGED <<aAuth, floor, pub, spart, capable, floorOn>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

Upgrade(p) ==
    /\ ~capable[p] /\ capable' = [capable EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<aAuth, aCom, alog, floor, pub, spart, floorOn>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

(* The gate opens only once every active silo advertises the marker. *)
EnableFloor ==
    /\ \A p \in Parts : capable[p]
    /\ ~floorOn /\ floorOn' = TRUE
    /\ UNCHANGED <<aAuth, aCom, alog, floor, pub, spart, capable>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

(* AdvanceFloor(p): the floor follows the wall clock. It is persisted, then *)
(* ReadShippingAsync publishes (F, O) atomically under the append gate; the *)
(* shipper holds the latest publication until its acked cursor passes O.   *)
AdvanceFloor(p) ==
    /\ floorOn /\ floor[p] < Top
    /\ floor' = [floor EXCEPT ![p] = @ + 1]
    /\ pub' = [pub EXCEPT ![p] = <<floor[p] + 1, Len(alog[p])>>]
    /\ UNCHANGED <<aAuth, aCom, alog, spart, capable, floorOn>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

(* The shipper's acked cursor has passed the held publication's O. *)
Pass(p) ==
    /\ pub[p] # NoPub /\ pub[p][2] <= aack[p]
    /\ spart' = [spart EXCEPT ![p] = pub[p][1]]
    /\ pub' = [pub EXCEPT ![p] = NoPub]
    /\ UNCHANGED <<aAuth, aCom, alog, floor, capable, floorOn>> /\ UNCHANGED Wire /\ UNCHANGED Rcv

ShipA(p) ==
    /\ adr[p] < Len(alog[p])
    /\ adr' = [adr EXCEPT ![p] = @ + 1]
    /\ UNCHANGED Origin /\ UNCHANGED <<aack, bdeps, back, losses>> /\ UNCHANGED Rcv

LoseA(p) ==
    /\ losses < MaxLoss /\ aack[p] < adr[p]
    /\ adr' = [adr EXCEPT ![p] = aack[p]]
    /\ losses' = losses + 1
    /\ UNCHANGED Origin /\ UNCHANGED <<aack, bdeps, back>> /\ UNCHANGED Rcv

Rearm == wake' = (wake \/ bBuf # {})

(* ReceiveA(p): the receiver runs the next in-flight a-entry and acks it. *)
(* A visible write either merges or, on an apply failure, is              *)
(* dead-lettered; a full queue refuses it and the entry stays un-acked    *)
(* (#4603 backpressure).                                                  *)
ReceiveA(p) ==
    LET i == aack[p] + 1
        e == alog[p][i]
    IN /\ i <= adr[p]
       /\ \/ /\ e.k = "prep" \/ e.h \in rApplied \cup dlq \cup lost
             /\ aack' = [aack EXCEPT ![p] = i]
             /\ UNCHANGED Rcv
          \/ /\ e.k # "prep" /\ e.h \notin rApplied \cup dlq \cup lost
             /\ aack' = [aack EXCEPT ![p] = i]
             /\ rApplied' = rApplied \cup {e.h} /\ ids' = ids \cup {e.h}
             /\ Rearm
             /\ UNCHANGED <<dlq, lost, dls, bApplied, bBuf, bDead, srcv, booted>>
          \/ /\ e.k # "prep" /\ e.h \notin rApplied \cup dlq \cup lost
             /\ dls < MaxDeadLetters
             /\ Cardinality(dlq) < DlqCap
             /\ aack' = [aack EXCEPT ![p] = i]
             /\ dlq' = dlq \cup {e.h} /\ dls' = dls + 1
             /\ UNCHANGED <<rApplied, ids, lost, bApplied, bBuf, bDead, srcv, wake, booted>>
       /\ UNCHANGED Origin /\ UNCHANGED <<adr, bdeps, back, losses>>

Replay(h) ==
    /\ h \in dlq
    /\ dlq' = dlq \ {h}
    /\ rApplied' = rApplied \cup {h} /\ ids' = ids \cup {h}
    /\ Rearm
    /\ UNCHANGED <<lost, dls, bApplied, bBuf, bDead, srcv, booted>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

(* Discard(h): an operator drops a dead-letter; it becomes a lost mark. *)
Discard(h) ==
    /\ h \in dlq
    /\ dlq' = dlq \ {h}
    /\ lost' = lost \cup {h}
    /\ Rearm
    /\ UNCHANGED <<rApplied, ids, dls, bApplied, bBuf, bDead, srcv, booted>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

Forget(h) ==
    /\ h \in ids /\ ids' = ids \ {h}
    /\ UNCHANGED <<rApplied, dlq, lost, dls, bApplied, bBuf, bDead, srcv, wake, booted>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

(* b authors a write depending on an a-write it holds. *)
AuthorB(t) ==
    /\ Len(bdeps) < BWrites /\ t \in aCom
    /\ bdeps' = Append(bdeps, t)
    /\ UNCHANGED Origin /\ UNCHANGED <<adr, aack, back, losses>> /\ UNCHANGED Rcv

(* ReceiveB: a dependent whose dependency is lost dead-letters as         *)
(* dependency_lost; one whose dependency is met merges; else it parks.   *)
Settle(i, t) ==
    IF t \in lost THEN /\ bDead' = bDead \cup {i} /\ UNCHANGED bApplied
    ELSE /\ bApplied' = bApplied \cup {i} /\ UNCHANGED bDead

ReceiveB ==
    LET i == back + 1
        t == bdeps[i]
    IN /\ i <= Len(bdeps)
       /\ back' = i
       /\ IF t \in lost \/ DepsOk(t)
             THEN Settle(i, t) /\ UNCHANGED <<bBuf, wake>>
             ELSE bBuf' = bBuf \cup {i} /\ wake' = TRUE /\ UNCHANGED <<bApplied, bDead>>
       /\ UNCHANGED <<rApplied, ids, dlq, lost, dls, srcv, booted>>
       /\ UNCHANGED Origin /\ UNCHANGED <<adr, aack, bdeps, losses>>

Drain ==
    /\ wake
    /\ IF \E i \in bBuf : bdeps[i] \in lost \/ DepsOk(bdeps[i])
       THEN \E i \in bBuf :
               /\ bdeps[i] \in lost \/ DepsOk(bdeps[i])
               /\ bBuf' = bBuf \ {i}
               /\ Settle(i, bdeps[i])
               /\ UNCHANGED wake
       ELSE wake' = FALSE /\ UNCHANGED <<bBuf, bApplied, bDead>>
    /\ UNCHANGED <<rApplied, ids, dlq, lost, dls, srcv, booted>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

(* Heartbeat: an envelope carrying S_a; recording it re-arms a drain. *)
Heartbeat ==
    /\ floorOn
    /\ SOf > srcv
    /\ srcv' = SOf
    /\ Rearm
    /\ UNCHANGED <<rApplied, ids, dlq, lost, dls, bApplied, bBuf, bDead, booted>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

(* Bootstrap: the receiver installs a snapshot from a source that holds   *)
(* every committed a-write (a peer of a; a's own export omits its own     *)
(* watermark, #4586 part 2b-2). The export carries the source's applied  *)
(* low watermark for a - every a-write below it is reflected, none is    *)
(* held back - and the pin installs it as a pointwise maximum            *)
(* (ReplicationTreeFrontierGrain.PinAsync). The source's applied          *)
(* watermark is downward-closed for the same reason a's is: the minimum   *)
(* floor, clamped below every prepare without a terminal.                 *)
SelfS == Min({floor[p] : p \in Parts} \cup OpenIn(AllEntries))
Bootstrap ==
    /\ Bootstraps /\ ~booted /\ floorOn
    /\ booted' = TRUE
    /\ rApplied' = rApplied \cup aCom
    /\ srcv' = Max({srcv, SelfS})
    /\ Rearm
    /\ UNCHANGED <<ids, dlq, lost, dls, bApplied, bBuf, bDead>>
    /\ UNCHANGED Origin /\ UNCHANGED Wire

Quiesced ==
    /\ \A p \in Parts : aack[p] = Len(alog[p])
    /\ back = Len(bdeps) /\ ~wake

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E p \in Parts, h \in Hlcs, s \in BOOLEAN : AuthorA(p, h, s)
    \/ \E h \in Hlcs, p \in Parts : CommitA(h, p)
    \/ (\E p \in Parts : Upgrade(p)) \/ EnableFloor \/ Heartbeat \/ Bootstrap
    \/ \E p \in Parts : AdvanceFloor(p) \/ Pass(p) \/ ShipA(p) \/ LoseA(p) \/ ReceiveA(p)
    \/ \E h \in Hlcs : Replay(h) \/ Discard(h) \/ Forget(h)
    \/ \E t \in Hlcs : AuthorB(t)
    \/ ReceiveB \/ Drain
    \/ Stutter

(* Fairness: the gate opens once a is upgraded; floors follow the clock, *)
(* are published, passed and shipped; shippers ship and receivers         *)
(* receive; sagas terminate; dead-letters are replayed or discarded; a    *)
(* pending drain runs. Authoring, forgetting, loss and bootstrap are      *)
(* environment events.                                                    *)
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(EnableFloor) /\ WF_vars(Heartbeat)
    /\ \A p \in Parts : /\ WF_vars(Upgrade(p)) /\ WF_vars(AdvanceFloor(p)) /\ WF_vars(Pass(p))
                        /\ WF_vars(ShipA(p)) /\ WF_vars(ReceiveA(p))
    /\ WF_vars(\E h \in Hlcs, p \in Parts : CommitA(h, p))
    /\ WF_vars(\E h \in Hlcs : Replay(h) \/ Discard(h))
    /\ WF_vars(ReceiveB) /\ WF_vars(Drain)

CausalOrder == \A i \in bApplied : bdeps[i] \in rApplied

EventualConvergence ==
    <>[](/\ \A h \in aCom : h \in rApplied \cup lost
         /\ \A i \in 1..Len(bdeps) : i \in bApplied \/ (bdeps[i] \in lost /\ i \in bDead))

=============================================================================
