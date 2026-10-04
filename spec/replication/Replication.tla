----------------------------- MODULE Replication -----------------------------
(***************************************************************************)
(* TLA+ specification of plain (non-saga) cross-cluster replication in     *)
(* Orleans.Lattice: clusters authoring writes under per-leaf HLCs, the     *)
(* shipper's per-partition cursor and its local-origin cycle-break, the    *)
(* receiver's apply pipeline (receiver-side cycle-break, snapshot-pinned   *)
(* floor, shadow-forward identity cache, causal buffer, dead-letter        *)
(* queue), and bootstrap from a snapshot followed by the stream handoff,   *)
(* over a transport that loses, duplicates, reorders and delays.           *)
(*                                                                         *)
(* A replica's state for a key is the SET of writes merged into it - the   *)
(* free join-semilattice - and each merge mode is a join-homomorphism      *)
(* from that set to the mode's own lattice (ValueAt): last-writer-wins     *)
(* takes the greatest write under (HLC, origin rank); the grow-only        *)
(* counter takes, per origin, the highest HLC that origin contributed      *)
(* (isomorphic to the per-replica count, since a leaf's HLC rises with     *)
(* every write it commits). Properties compare VALUES, never sets, so      *)
(* dropping a write whose effect a replica already holds is not a loss,    *)
(* and dropping one whose effect it lacks is.                              *)
(*                                                                         *)
(* Where production deviated from a design that satisfies the properties   *)
(* below, the module models the intended design and the deviation stands   *)
(* as a paired mutation that reproduces the former production shape,       *)
(* linked to the issue that fixed it. Refinement.md lists every such row.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS a, b, c, k1, k2

(***************************************************************************)
(* The bounded instance. a and b author writes and ship to each other (a   *)
(* two-cluster cycle the cycle-break must cut), and both ship to c, a      *)
(* receive-only cluster. a -> b -> c is a multi-hop path production        *)
(* deliberately does not use: b never re-ships a's writes, so c takes them *)
(* from a directly. c may also bootstrap, once, from a snapshot of b,      *)
(* including in place over a copy it already holds. k1 is a                *)
(* last-writer-wins key, which b may also delete, and k2 a                 *)
(* grow-only-counter key.                                                  *)
(***************************************************************************)
Clusters == {a, b, c}
Writers == {a, b}
Keys == {k1, k2}
Peers == {<<a, b>>, <<b, a>>, <<a, c>>, <<b, c>>}
Mode(k) == IF k = k1 THEN "lww" ELSE "gcounter"
Rank(x) == IF x = a THEN 1 ELSE IF x = b THEN 2 ELSE 3
MaxHlc == 2
Hlcs == 1..MaxHlc
MaxWrites == 2
MaxFaults == 1
BootSource == b
BootTarget == c
BootEdge == <<BootSource, BootTarget>>
FaultSite == c

Max(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A n \in S : n <= m

(***************************************************************************)
(* A write is identified by its origin, key and HLC - production's         *)
(* shadow-forward identity tuple (origin, hlc, key, op) with op fixed. It  *)
(* may carry one causal dependency: a foreign origin and the HLC of that   *)
(* origin the author had applied (production's WalRecord.VectorClock, a    *)
(* frontier the application stamps). NoDep is the null frontier of an      *)
(* ordinary local write. A delete of a last-writer-wins key is a write     *)
(* whose del flag is set: production's tombstone, an LwwValue that merges  *)
(* by its HLC like any other write. Only BootSource deletes, and only a    *)
(* key its replica holds (deleting an absent key changes no replica): the  *)
(* case a snapshot bootstrap must carry (#4504), in a small instance.      *)
(***************************************************************************)
NoDep == [o |-> c, h |-> 0]
DepDomain == {NoDep} \cup [o : Writers, h : Hlcs]
Writes == [o : Writers, k : Keys, h : Hlcs, d : DepDomain, del : BOOLEAN]
Idents == Writers \X Keys \X Hlcs
Ident(w) == <<w.o, w.k, w.h>>

(***************************************************************************)
(* A delivery is tagged with the shipper partition entry it came from,     *)
(* <<edge, index>>, so its acknowledgement advances the right cursor.     *)
(* An operator replay of a dead letter is a local call with no shipper.    *)
(***************************************************************************)
Tags == Peers \X (1..MaxWrites)
Parked == [w : Writes, t : Tags, replay : BOOLEAN]

VARIABLES
    authored,    \* every write ever authored (history, for the properties)
    wal,         \* wal[x]: cluster x's WAL partition
    cursor,      \* cursor[e]: shipper e's acknowledged position in its source's WAL
    val,         \* val[x][k]: the writes merged into x's replica of k
    hwm,         \* hwm[x][o]: x's per-origin high-water vector (max applied HLC)
    pinned,      \* pinned[x][o]: x's snapshot-pinned drop floor
    cache,       \* cache[x]: x's shadow-forward identity cache (volatile)
    parking,     \* parking[x]: entries x decided to park, not yet buffered
    buf,         \* buf[x]: x's causal apply buffer
    wake,        \* wake[x]: a drain of x's buffer is pending
    dlq,         \* dlq[x]: x's dead-letter queue
    faults,      \* environment fault budget (the fairness ceiling)
    booted       \* BootTarget has bootstrapped from BootSource

vars == <<authored, wal, cursor, val, hwm, pinned, cache, parking,
          buf, wake, dlq, faults, booted>>

Range(s) == {s[i] : i \in 1..Len(s)}

(***************************************************************************)
(* The merge modes, as join-homomorphisms from a set of writes. The        *)
(* observer x is a parameter so a merge whose result depends on WHERE it   *)
(* is evaluated is expressible (issue #2891); the base ranks every write   *)
(* by its authoring origin alone, so the observer is ignored.              *)
(***************************************************************************)
LwwRank(x, w) == Rank(w.o)
LwwLeq(x, v, w) == v.h < w.h \/ (v.h = w.h /\ LwwRank(x, v) <= LwwRank(x, w))
LwwTop(x, S) == CHOOSE w \in S : \A v \in S : LwwLeq(x, v, w)

ValueAt(x, k, S) ==
    IF Mode(k) = "lww"
    THEN IF S = {} THEN [h |-> 0, o |-> c, del |-> FALSE]
         ELSE [h |-> LwwTop(x, S).h, o |-> LwwTop(x, S).o, del |-> LwwTop(x, S).del]
    ELSE [o \in Writers |-> Max({w.h : w \in {v \in S : v.o = o}})]

(* The highest HLC merged into x's replica of k: the leaf's version stamp. *)
Ver(x, k) == Max({w.h : w \in val[x][k]})

(* Applying w at x would change nothing x's replica of w's key reports.    *)
Subsumed(x, w) == ValueAt(x, w.k, val[x][w.k] \cup {w}) = ValueAt(x, w.k, val[x][w.k])

InFlight(x, w) == \E p \in parking[x] : p.w = w

(***************************************************************************)
(* x has absorbed w: its value already reflects it, or w is held where x   *)
(* will still apply it from - an in-progress park, the causal buffer, or   *)
(* the dead-letter queue awaiting operator replay.                         *)
(***************************************************************************)
Absorbed(x, w) ==
    \/ Subsumed(x, w)
    \/ w \in buf[x]
    \/ InFlight(x, w)
    \/ w \in dlq[x]

(* CausalApplyBuffer.DependenciesSatisfied: the entry's own origin and the *)
(* receiver's own cluster are exempt; any other coordinate must be met.    *)
DepsOk(x, w) ==
    IF w.d = NoDep \/ w.d.o = w.o \/ w.d.o = x THEN TRUE ELSE hwm[x][w.d.o] >= w.d.h

(* ReplicationShipperGrain.ShouldShip's origin clauses. *)
ShouldShip(s, w) == w.o = s

DepChoices(o) == {NoDep} \cup {[o |-> p, h |-> hwm[o][p]] : p \in {q \in Writers \ {o} : hwm[o][q] > 0}}
DelChoices(o, k) == IF o = BootSource /\ Mode(k) = "lww" /\ val[o][k] # {} THEN BOOLEAN ELSE {FALSE}

Merged(x, w) == [val EXCEPT ![x][w.k] = @ \cup {w}]
Logged(x, w) ==
    IF x \notin Writers THEN wal
    ELSE [wal EXCEPT ![x] = IF w \in Range(@) THEN @ ELSE Append(@, w)]
Advanced(x, w) == [hwm EXCEPT ![x][w.o] = IF w.h > @ THEN w.h ELSE @]

TypeOK ==
    /\ authored \subseteq Writes
    /\ \A x \in Clusters : wal[x] \in Seq(Writes) /\ Len(wal[x]) <= MaxWrites
    /\ cursor \in [Peers -> 0..MaxWrites]
    /\ val \in [Clusters -> [Keys -> SUBSET Writes]]
    /\ hwm \in [Clusters -> [Writers -> 0..MaxHlc]]
    /\ pinned \in [Clusters -> [Writers -> 0..MaxHlc]]
    /\ cache \in [Clusters -> SUBSET Idents]
    /\ parking \in [Clusters -> SUBSET Parked]
    /\ buf \in [Clusters -> SUBSET Writes]
    /\ wake \in [Clusters -> BOOLEAN]
    /\ dlq \in [Clusters -> SUBSET Writes]
    /\ faults \in 0..MaxFaults
    /\ booted \in BOOLEAN

Init ==
    /\ authored = {}
    /\ wal = [x \in Clusters |-> <<>>]
    /\ cursor = [e \in Peers |-> 0]
    /\ val = [x \in Clusters |-> [k \in Keys |-> {}]]
    /\ hwm = [x \in Clusters |-> [o \in Writers |-> 0]]
    /\ pinned = [x \in Clusters |-> [o \in Writers |-> 0]]
    /\ cache = [x \in Clusters |-> {}]
    /\ parking = [x \in Clusters |-> {}]
    /\ buf = [x \in Clusters |-> {}]
    /\ wake = [x \in Clusters |-> FALSE]
    /\ dlq = [x \in Clusters |-> {}]
    /\ faults = 0
    /\ booted = FALSE

(***************************************************************************)
(* Author(o, k, h, d): cluster o commits a local write to k. Its HLC is    *)
(* strictly above every HLC already merged into o's replica of k: the leaf *)
(* clock ticks for a local write and is advanced past every merged         *)
(* timestamp (BPlusLeafGrain's merge path; AdvanceClockOrOverride for the  *)
(* CRDT and atomic apply paths). Nothing relates the clocks of different   *)
(* keys or different clusters, which is exactly production's guarantee, so *)
(* an origin's HLCs are non-monotonic in delivery order (#1060). The write *)
(* may declare a dependency on a foreign frontier o has applied. Writes    *)
(* are optional, so the action is not fair.                                *)
(***************************************************************************)
Author(o, k, h, d) ==
    /\ Cardinality(authored) < MaxWrites
    /\ h > Ver(o, k)
    /\ d \in DepChoices(o)
    /\ \E del \in DelChoices(o, k) :
       LET w == [o |-> o, k |-> k, h |-> h, d |-> d, del |-> del]
       IN /\ authored' = authored \cup {w}
          /\ val' = [val EXCEPT ![o][k] = @ \cup {w}]
          /\ wal' = [wal EXCEPT ![o] = Append(@, w)]
    /\ UNCHANGED <<cursor, hwm, pinned, cache, parking, buf, wake, dlq, faults, booted>>

(***************************************************************************)
(* ShipSkip(e): the shipper's merge consumes the next entry of its        *)
(* source's WAL that it must not ship - one whose origin is not the local   *)
(* cluster - and folds the partition cursor past it with no ack, as        *)
(* FoldFilteredOnlyConsumedCursorsAsync does. The scalar HLC cursor is NOT *)
(* a skip criterion: the legacy-migration HLC filter is off once any       *)
(* partition cursor is saved, because a below-cursor entry is routinely    *)
(* genuinely new (#1060, ReplicationShipperGrain's merge loop).            *)
(***************************************************************************)
ShipSkip(e) ==
    LET s == e[1]
        i == cursor[e] + 1
    IN /\ i <= Len(wal[s])
       /\ ~ShouldShip(s, wal[s][i])
       /\ cursor' = [cursor EXCEPT ![e] = i]
       /\ UNCHANGED <<authored, wal, val, hwm, pinned, cache, parking, buf, wake, dlq, faults, booted>>

(***************************************************************************)
(* Deliver(e, i): the shipper sends entry i of its source's WAL - any      *)
(* entry beyond its acknowledged cursor - the receiver runs it through the *)
(* apply pipeline, and the acknowledgement returns. Sending any entry      *)
(* above the cursor covers pipelined batches, a retry after a lost batch   *)
(* or ack, and the crash replay of the deferred cursor-flush window;       *)
(* delivering in any order covers reordering; and because only the ack of  *)
(* the very next entry moves the cursor, an entry delivered out of order   *)
(* stays above the cursor and is delivered again, which covers             *)
(* duplication - the same re-delivery a lost ack causes. A lost batch is a *)
(* delivery that has not happened yet, and a network partition is a run   *)
(* of them. The ack is folded into the delivery step: the cursor it moves  *)
(* is read only by this shipper's own steps, so delaying it past other     *)
(* steps reaches no further receiver state.                                *)
(*                                                                         *)
(* Each cluster has ONE WAL partition holding every key. Production hashes *)
(* keys over several partitions, each with its own cursor, but one         *)
(* partition already interleaves independent per-leaf clocks (production's *)
(* own #1060 note says so), and a single cursor advances no faster than    *)
(* per-partition cursors would, so every entry production may still       *)
(* (re)deliver, the model may too.                                         *)
(*                                                                         *)
(* The cursor advances only when the ack is for the very next entry: acks  *)
(* are consumed in FIFO batch order and a batch behind a failed one does   *)
(* not advance the cursor (ReplicationShipperGrain's pipelined ack path).  *)
(*                                                                         *)
(* The send guard is the local-origin cycle-break. Its second disjunct is  *)
(* an over-approximation: a peer running another build, or a hand-built    *)
(* apply pipeline (which ReplicationApplier's own comment names), may hand *)
(* the destination its OWN write back, so the receiver-side cycle-break is *)
(* load-bearing rather than decorative.                                    *)
(*                                                                         *)
(* The pipeline is ReplicationApplier.ApplyAsync's, in order: the          *)
(* receiver-side cycle-break (an own-origin entry is a dedup no-op), the   *)
(* pinned floor (always zero: production installs none since #4476), the  *)
(* identity cache (TryAdd), the dependency                                 *)
(* check (which hands the entry to Park), and the merge, after which the   *)
(* HWM advances (monotone max) and, if it advanced while entries are       *)
(* buffered, a drain is scheduled. Every outcome but the park is           *)
(* acknowledged at once: production acks a dedup, a cycle-break rejection *)
(* and an apply alike as Accepted. A duplicate of an entry that is still   *)
(* being parked is not processed until the park completes - the intended  *)
(* design (#4465); see CursorNeverSkipsUnshippedDuplicateOfParkingAcked.   *)
(***************************************************************************)
Deliver(e, i) ==
    LET s == e[1]
        x == e[2]
        w == wal[s][i]
        acked == IF i = cursor[e] + 1 THEN [cursor EXCEPT ![e] = i] ELSE cursor
    IN /\ i > cursor[e]
       /\ i <= Len(wal[s])
       /\ \/ ShouldShip(s, w)
          \/ w.o = x
       /\ ~InFlight(x, w)
       /\ \/ /\ w.o = x
             /\ cursor' = acked
             /\ UNCHANGED <<val, wal, hwm, cache, parking, wake>>
          \/ /\ w.o # x
             /\ w.h <= pinned[x][w.o]
             /\ cursor' = acked
             /\ UNCHANGED <<val, wal, hwm, cache, parking, wake>>
          \/ /\ w.o # x
             /\ w.h > pinned[x][w.o]
             /\ Ident(w) \in cache[x]
             /\ cursor' = acked
             /\ UNCHANGED <<val, wal, hwm, cache, parking, wake>>
          \/ /\ w.o # x
             /\ w.h > pinned[x][w.o]
             /\ Ident(w) \notin cache[x]
             /\ ~DepsOk(x, w)
             /\ cache' = [cache EXCEPT ![x] = @ \cup {Ident(w)}]
             /\ parking' = [parking EXCEPT ![x] = @ \cup {[w |-> w, t |-> <<e, i>>, replay |-> FALSE]}]
             /\ UNCHANGED <<cursor, val, wal, hwm, wake>>
          \/ /\ w.o # x
             /\ w.h > pinned[x][w.o]
             /\ Ident(w) \notin cache[x]
             /\ DepsOk(x, w)
             /\ cache' = [cache EXCEPT ![x] = @ \cup {Ident(w)}]
             /\ val' = Merged(x, w)
             /\ wal' = Logged(x, w)
             /\ hwm' = Advanced(x, w)
             /\ wake' = [wake EXCEPT ![x] = @ \/ (w.h > hwm[x][w.o] /\ buf[x] # {})]
             /\ cursor' = acked
             /\ UNCHANGED parking
       /\ UNCHANGED <<authored, pinned, buf, dlq, faults, booted>>

(***************************************************************************)
(* Park(x, p): the parked entry lands in the causal buffer and the call    *)
(* returns, so a shipped entry is acknowledged. The dependency check and   *)
(* the buffer insert are separate steps because production separates them *)
(* by awaits, and an apply on another call can advance the HWM and run its *)
(* drain in between. Parking re-arms a drain, as CausalApplyBufferGrain's  *)
(* park has re-checked and drained in the same turn since #4464; the       *)
(* EventualConvergenceParkLostWakeup mutation is the former shape.         *)
(***************************************************************************)
Park(x, p) ==
    /\ p \in parking[x]
    /\ parking' = [parking EXCEPT ![x] = @ \ {p}]
    /\ buf' = [buf EXCEPT ![x] = @ \cup {p.w}]
    /\ cursor' = IF p.replay \/ p.t[2] # cursor[p.t[1]] + 1
                 THEN cursor
                 ELSE [cursor EXCEPT ![p.t[1]] = p.t[2]]
    /\ wake' = [wake EXCEPT ![x] = TRUE]
    /\ UNCHANGED <<authored, wal, val, hwm, pinned, cache, dlq, faults, booted>>

(***************************************************************************)
(* Drain(x): DrainBufferAsync, one released entry per step. While a drain  *)
(* is pending it applies any buffered entry whose dependencies are now     *)
(* met; when none is, the drain completes. One entry per step is finer     *)
(* than production's pass, which only adds interleavings.                  *)
(***************************************************************************)
Drain(x) ==
    /\ wake[x]
    /\ IF \E r \in buf[x] : DepsOk(x, r)
       THEN \E r \in buf[x] :
               /\ DepsOk(x, r)
               /\ buf' = [buf EXCEPT ![x] = @ \ {r}]
               /\ val' = Merged(x, r)
               /\ wal' = Logged(x, r)
               /\ hwm' = Advanced(x, r)
               /\ UNCHANGED wake
       ELSE /\ wake' = [wake EXCEPT ![x] = FALSE]
            /\ UNCHANGED <<buf, val, wal, hwm>>
    /\ UNCHANGED <<authored, cursor, pinned, cache, parking, dlq, faults, booted>>

(***************************************************************************)
(* ApplyFails(e, i): delivering entry i, the merge throws until         *)
(* DeadLetterTrackingReplicationApplier exhausts its retry budget and      *)
(* parks the entry on the dead-letter queue, acknowledging it. The         *)
(* identity-cache reservation was rolled back by ReplicationApplier's      *)
(* catch, so the cache is unchanged.                                       *)
(***************************************************************************)
ApplyFails(e, i) ==
    LET x == e[2]
        w == wal[e[1]][i]
    IN /\ faults < MaxFaults
       /\ x = FaultSite
       /\ i > cursor[e]
       /\ i <= Len(wal[e[1]])
       /\ ShouldShip(e[1], w)
       /\ w.h > pinned[x][w.o]
       /\ Ident(w) \notin cache[x]
       /\ ~InFlight(x, w)
       /\ dlq' = [dlq EXCEPT ![x] = @ \cup {w}]
       /\ cursor' = IF i = cursor[e] + 1 THEN [cursor EXCEPT ![e] = i] ELSE cursor
       /\ faults' = faults + 1
       /\ UNCHANGED <<authored, wal, val, hwm, pinned, cache, parking, buf, wake, booted>>

(***************************************************************************)
(* Evict(x, w): the bounded causal buffer displaces an entry to the        *)
(* dead-letter queue (CausalApplyBuffer.TryAdd over its caps). ParkAsync   *)
(* releases the displaced entry's identity-cache reservation so a replay   *)
(* is applied rather than dropped as a duplicate.                          *)
(***************************************************************************)
Evict(x, w) ==
    /\ faults < MaxFaults
    /\ x = FaultSite
    /\ w \in buf[x]
    /\ buf' = [buf EXCEPT ![x] = @ \ {w}]
    /\ dlq' = [dlq EXCEPT ![x] = @ \cup {w}]
    /\ cache' = [cache EXCEPT ![x] = @ \ {Ident(w)}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<authored, wal, cursor, val, hwm, pinned, parking, wake, booted>>

(***************************************************************************)
(* Replay(x, r): an operator replays a dead letter                         *)
(* (ILatticeReplicationDeadLetters.ReplayAsync), running it back through   *)
(* the same pipeline as Deliver bar the cycle-break, which no dead letter  *)
(* needs (only foreign entries are dead-lettered). Production has no       *)
(* automatic replay; the liveness property assumes the operator eventually *)
(* replays, and that assumption is the only one it makes about operators. *)
(***************************************************************************)
Replay(x, r) ==
    /\ r \in dlq[x]
    /\ dlq' = [dlq EXCEPT ![x] = @ \ {r}]
    /\ \/ /\ r.h <= pinned[x][r.o]
          /\ UNCHANGED <<val, wal, hwm, cache, parking, wake>>
       \/ /\ r.h > pinned[x][r.o]
          /\ Ident(r) \in cache[x]
          /\ UNCHANGED <<val, wal, hwm, cache, parking, wake>>
       \/ /\ r.h > pinned[x][r.o]
          /\ Ident(r) \notin cache[x]
          /\ ~DepsOk(x, r)
          /\ cache' = [cache EXCEPT ![x] = @ \cup {Ident(r)}]
          /\ parking' = [parking EXCEPT ![x] = @ \cup {[w |-> r, t |-> <<<<r.o, x>>, 1>>, replay |-> TRUE]}]
          /\ UNCHANGED <<val, wal, hwm, wake>>
       \/ /\ r.h > pinned[x][r.o]
          /\ Ident(r) \notin cache[x]
          /\ DepsOk(x, r)
          /\ cache' = [cache EXCEPT ![x] = @ \cup {Ident(r)}]
          /\ val' = Merged(x, r)
          /\ wal' = Logged(x, r)
          /\ hwm' = Advanced(x, r)
          /\ wake' = [wake EXCEPT ![x] = @ \/ (r.h > hwm[x][r.o] /\ buf[x] # {})]
          /\ UNCHANGED <<parking>>
    /\ UNCHANGED <<authored, cursor, pinned, buf, faults, booted>>

(***************************************************************************)
(* Restart(x): the receiving silo restarts. Volatile state is lost: the    *)
(* identity cache, and every call in progress (an unacknowledged park is   *)
(* re-sent by its shipper; a replay in progress returns to the dead-letter *)
(* queue). The causal buffer survives and a drain is re-armed on           *)
(* activation, as CausalApplyBufferGrain has persisted it since #4464;     *)
(* the EventualConvergenceVolatileCausalBuffer mutation is the former      *)
(* in-memory buffer.                                                       *)
(***************************************************************************)
Restart(x) ==
    /\ faults < MaxFaults
    /\ x = FaultSite
    /\ cache' = [cache EXCEPT ![x] = {}]
    /\ parking' = [parking EXCEPT ![x] = {}]
    /\ dlq' = [dlq EXCEPT ![x] = @ \cup {p.w : p \in {q \in parking[x] : q.replay}}]
    /\ wake' = [wake EXCEPT ![x] = buf[x] # {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<authored, wal, cursor, val, hwm, pinned, buf, booted>>

(***************************************************************************)
(* Bootstrap: BootTarget bootstraps from a snapshot of BootSource. The     *)
(* export reads its frontier (LatticeSnapshotProvider.ExportAsync): no     *)
(* production caller reports a vector to the WAL cursor registry, so       *)
(* GetCausalStableAsync returns null and the frontier is the source's own  *)
(* HWM vector, which carries no coordinate for the source itself. The      *)
(* export streams every row, tombstones included (the design; see below), *)
(* the drain applies them at BootTarget                                   *)
(* under LatticeBootstrapApplyContext - the floor is bypassed and the HWM  *)
(* does not advance, but each row still passes through the identity cache, *)
(* stamped with the SOURCE as its origin and the row's version HLC - and   *)
(* LatticeBootstrapCoordinatorGrain.PinAndCompleteAsync pins the handoff: *)
(* the source's coordinate is sealed at the cut (the highest coordinate in *)
(* the frontier and the highest row HLC applied) and the HWM vector takes  *)
(* the result. Optional and at most once, so not fair.                     *)
(*                                                                         *)
(* Production reads the frontier first and each row at its own instant     *)
(* before the pin; here all of it happens at once. With no drop floor that *)
(* loses nothing: a row only adds to BootTarget a value BootSource held,   *)
(* every write in which BootTarget also receives over its own peer edges, *)
(* so when a row is read changes how early a write arrives, never whether; *)
(* a row's seeded identity <<source, key, version>> can only name a write *)
(* of the source's that the row itself contains, because the source's     *)
(* later writes to the key carry higher HLCs; and an earlier frontier is a *)
(* smaller one, which only satisfies fewer dependencies.                   *)
(*                                                                         *)
(* The design pins NO drop floor, because no HLC is a cut below which     *)
(* every write of an origin is provably in the snapshot: per-leaf clocks   *)
(* are unordered. Production pinned the floor at the frontier until #4476  *)
(* (mutation BootstrapHandoffLosesNothingPinnedFloor). The intended design *)
(* also takes the pointwise maximum with the vector already held, where    *)
(* production replaced it until #4464 (mutation                            *)
(* EventualConvergencePinRegressesVector), and re-arms a drain, because    *)
(* the pin can satisfy a parked entry's dependency (mutation               *)
(* EventualConvergencePinSkipsDrain).                                      *)
(*                                                                         *)
(* The bootstrap may run in place over a copy BootTarget already holds,    *)
(* and BootSource may have trimmed its log past BootTarget's cursor first: *)
(* that is what makes LatticeFallOffLogDetector request it. The stream    *)
(* then resumes at the trim point t, any position from the cursor to the   *)
(* log's end, and the entries in between reach BootTarget only through the *)
(* snapshot. So the snapshot must carry every row BootSource holds,        *)
(* tombstones included. Until #4544 (the fix for #4504) production's      *)
(* export skipped a tombstoned key, and the drain does not clear the       *)
(* receiver's copy, so a delete behind the trim point was never delivered  *)
(* (mutation EventualConvergenceSnapshotDropsDeletes). No tombstone is     *)
(* reaped here; ReplicationReBootstrap.tla covers a reaped one (#4537).    *)
(***************************************************************************)
Bootstrap ==
    /\ ~booted
    /\ booted' = TRUE
    /\ val' = [val EXCEPT ![BootTarget] = [k \in Keys |-> @[k] \cup val[BootSource][k]]]
    /\ cache' = [cache EXCEPT ![BootTarget] =
                    @ \cup {<<BootSource, k, Ver(BootSource, k)>> : k \in {j \in Keys : val[BootSource][j] # {}}}]
    /\ LET frontier == [o \in Writers |-> IF o = BootSource THEN 0 ELSE hwm[BootSource][o]]
           cut == Max({frontier[o] : o \in Writers} \cup {Ver(BootSource, k) : k \in Keys})
           front == [frontier EXCEPT ![BootSource] = Max({@, cut})]
       IN /\ hwm' = [hwm EXCEPT ![BootTarget] = [o \in Writers |-> Max({@[o], front[o]})]]
          /\ pinned' = [pinned EXCEPT ![BootTarget] = [o \in Writers |-> 0]]
    /\ wake' = [wake EXCEPT ![BootTarget] = @ \/ buf[BootTarget] # {}]
    /\ \E t \in cursor[BootEdge]..Len(wal[BootSource]) : cursor' = [cursor EXCEPT ![BootEdge] = t]
    /\ UNCHANGED <<authored, wal, parking, buf, dlq, faults>>

(***************************************************************************)
(* A fully quiesced state has an explicit stuttering successor so natural  *)
(* termination is not reported as a deadlock. Before full quiescence some  *)
(* real action is always enabled.                                          *)
(***************************************************************************)
Quiesced ==
    /\ \A x \in Clusters : parking[x] = {} /\ buf[x] = {} /\ dlq[x] = {} /\ ~wake[x]
    /\ \A e \in Peers : cursor[e] = Len(wal[e[1]])

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E o \in Writers, k \in Keys, h \in Hlcs, d \in DepDomain : Author(o, k, h, d)
    \/ \E e \in Peers : ShipSkip(e)
    \/ \E e \in Peers, i \in 1..MaxWrites : Deliver(e, i)
    \/ \E x \in Clusters : \E p \in parking[x] : Park(x, p)
    \/ \E x \in Clusters : Drain(x)
    \/ \E e \in Peers, i \in 1..MaxWrites : ApplyFails(e, i)
    \/ \E x \in Clusters : \E w \in buf[x] : Evict(x, w)
    \/ \E x \in Clusters : \E r \in dlq[x] : Replay(x, r)
    \/ \E x \in Clusters : Restart(x)
    \/ Bootstrap
    \/ Stutter

(***************************************************************************)
(* Fairness: the protocol makes progress over a fair transport. Every      *)
(* filtered entry is skipped; every entry beyond a cursor is eventually    *)
(* delivered, per (edge, partition, index) so no entry starves another;    *)
(* parks complete; a pending drain runs; an  *)
(* operator eventually replays a dead letter; a started bootstrap          *)
(* completes. Authoring, faults and starting a bootstrap are environment   *)
(* events and are not fair: every safety property must hold whether or    *)
(* not they happen.                                                        *)
(***************************************************************************)
Fairness ==
    /\ \A e \in Peers : WF_vars(ShipSkip(e))
    /\ \A e \in Peers, i \in 1..MaxWrites : WF_vars(Deliver(e, i))
    /\ \A x \in Clusters : WF_vars(\E p \in parking[x] : Park(x, p))
    /\ \A x \in Clusters : WF_vars(Drain(x))
    /\ \A x \in Clusters : WF_vars(\E r \in dlq[x] : Replay(x, r))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Safety properties.                                                      *)
(***************************************************************************)

(* NoRelay: a cluster ships only writes it authored. The one exception is *)
(* the over-approximated echo of a destination's own write, which is the  *)
(* receiver-side cycle-break's business (NoReflection), so no delivery     *)
(* ever carries a third cluster's write: a -> b -> c is never a path. An  *)
(* action property, because in this topology a relayed write is also      *)
(* delivered directly, so no reachable STATE distinguishes a relay.        *)
NoRelay ==
    [][\A e \in Peers, i \in 1..MaxWrites :
          Deliver(e, i) => wal[e[1]][i].o \in {e[1], e[2]}]_vars

(* NoReflection: no cluster admits its own write, received from a peer,   *)
(* into its apply pipeline. Every entry the pipeline admits is reserved   *)
(* in the identity cache first, so the cache is where admission shows.    *)
NoReflection == \A x \in Clusters : \A id \in cache[x] : id[1] # x

(* CursorNeverSkipsUnshipped: every ship-worthy entry at or below a       *)
(* shipper's acknowledged cursor has been absorbed by the destination.    *)
CursorNeverSkipsUnshipped ==
    \A e \in Peers :
        \A i \in 1..cursor[e] :
            ShouldShip(e[1], wal[e[1]][i]) => Absorbed(e[2], wal[e[1]][i])

(* DedupNeverDropsNew: a receiver drops an entry as a duplicate - by the  *)
(* pinned floor or the identity cache - only if, once dropped, it is still *)
(* absorbed. A drop is a pipeline step that leaves the receiver's replica, *)
(* its identity cache and its parking untouched for a foreign entry. The   *)
(* #1060 class.                                                            *)
DropStep(x, w) == w.o # x /\ UNCHANGED <<val, cache, parking>>

DedupNeverDropsNew ==
    [][/\ \A e \in Peers, i \in 1..MaxWrites :
             (Deliver(e, i) /\ DropStep(e[2], wal[e[1]][i]))
                 => Absorbed(e[2], wal[e[1]][i])'
       /\ \A x \in Clusters, r \in Writes :
             (Replay(x, r) /\ DropStep(x, r)) => Absorbed(x, r)']_vars

(* BootstrapHandoffLosesNothing: once the handoff is pinned, every write  *)
(* BootTarget did not author is either absorbed there or still on its     *)
(* way - beyond its shipper's cursor, above the pinned floor, and not     *)
(* reserved in the identity cache - so it will be accepted on arrival.    *)
Deliverable(w) ==
    LET e == <<w.o, BootTarget>>
    IN /\ w.h > pinned[BootTarget][w.o]
       /\ Ident(w) \notin cache[BootTarget]
       /\ \E i \in 1..Len(wal[w.o]) : wal[w.o][i] = w /\ i > cursor[e]

BootstrapHandoffLosesNothing ==
    booted =>
        \A w \in authored : w.o # BootTarget => (Absorbed(BootTarget, w) \/ Deliverable(w))

(***************************************************************************)
(* Liveness.                                                               *)
(***************************************************************************)
Target(k) == {w \in authored : w.k = k}

Converged ==
    /\ \A k \in Keys, x \in Clusters : ValueAt(x, k, val[x][k]) = ValueAt(x, k, Target(k))
    /\ \A k \in Keys, x, y \in Clusters : ValueAt(x, k, val[x][k]) = ValueAt(y, k, val[y][k])

(* EventualConvergence: under a fair transport, once writing stops every  *)
(* replica of every key reaches the same value - the value of every write *)
(* to that key. Fails on protocol defects under this fairness, not only   *)
(* without it: see EventualConvergenceParkLostWakeup and                  *)
(* EventualConvergenceObserverRelativeTieBreak.                           *)
EventualConvergence == <>[]Converged

=============================================================================
