----------------------- MODULE ReplicationReBootstrap -----------------------
(***************************************************************************)
(* An in-place re-bootstrap after the source reaped a delete (#4537), the  *)
(* focused companion of Replication.tla.                                   *)
(*                                                                         *)
(* A receiver r that fell off the source s's log is re-bootstrapped over   *)
(* the copy it already holds. The export carries every row s still holds,  *)
(* tombstones included (#4504), but s garbage-collects tombstones          *)
(* (BPlusLeafGrain.CompactTombstonesAsync after TombstoneGracePeriod), so  *)
(* a delete that is both behind the trim point and reaped reaches r by no  *)
(* path at all, and r keeps the deleted value for ever.                    *)
(*                                                                         *)
(* The design reconciles it on the receiver. Before the export opens, r    *)
(* pre-captures its live entry (s, t) if s is its origin, so s held it.   *)
(* If the export does not carry the key, absence at s means s deleted it  *)
(* at some HLC t_del >= t, so r                                            *)
(* applies a delete attributed to s at t itself: a tombstone wins an HLC   *)
(* tie, so it removes the captured value and only what s's own delete     *)
(* dominates. The reconcile fabricates a write, so it is gated on   *)
(* every way a key can be absent from an export without a delete: the     *)
(* export's scope, a reshard during the scan, and a source restore, purge  *)
(* or alias rebind since r's copy was aligned with s.                      *)
(*                                                                         *)
(* A key r holds under another origin - its own write, here - cannot be   *)
(* reconciled: r cannot prove s held that write before the scan. That      *)
(* residual is stated exactly (Residual) and excused from convergence.    *)
(*                                                                         *)
(* One last-writer-wins key, two clusters, at most three writes. Only s    *)
(* deletes. Delivery is FIFO and exactly once per edge: Replication.tla    *)
(* checks loss, duplication and reordering.                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS s, r

Clusters == {s, r}
Edges == {<<s, r>>, <<r, s>>}
Rank(x) == IF x = s THEN 2 ELSE 1
MaxHlc == 3
Hlcs == 1..MaxHlc
MaxWrites == 3
AlignedGen == 0

Max2(m, n) == IF m > n THEN m ELSE n

(***************************************************************************)
(* A write is its origin, HLC and tombstone flag. A replica's register is  *)
(* the last-writer-wins winner it holds, or Absent once a tombstone is    *)
(* reaped. fab marks a tombstone the reconcile fabricated, for the safety  *)
(* property only; production does not record it.                          *)
(***************************************************************************)
Writes == [o : Clusters, h : Hlcs, del : BOOLEAN]
Regs == [present : BOOLEAN, o : Clusters, h : 0..(MaxHlc + 1), del : BOOLEAN, fab : BOOLEAN]
Absent == [present |-> FALSE, o |-> s, h |-> 0, del |-> FALSE, fab |-> FALSE]
AsReg(w) == [present |-> TRUE, o |-> w.o, h |-> w.h, del |-> w.del, fab |-> FALSE]

(* LwwValue.Merge: the HLC, then a tombstone wins the tie, then the value. *)
(* The origin's rank stands for the value bytes.                           *)
Beats(v, u) ==
    IF ~u.present THEN TRUE
    ELSE IF v.h # u.h THEN v.h > u.h
    ELSE IF v.del # u.del THEN v.del
    ELSE Rank(v.o) > Rank(u.o)
Merge(u, v) == IF v.present /\ Beats(v, u) THEN v ELSE u

Exports == [phase : {"idle", "full", "scoped"}, scope : BOOLEAN, precap : Regs,
            scanned : BOOLEAN, carried : BOOLEAN, skipped : BOOLEAN,
            topo0 : 0..1, gen0 : 0..1]
IdleExport == [phase |-> "idle", scope |-> FALSE, precap |-> Absent,
               scanned |-> FALSE, carried |-> FALSE, skipped |-> FALSE,
               topo0 |-> 0, gen0 |-> 0]

VARIABLES
    authored,    \* every write ever authored (history, for the properties)
    wal,         \* wal[x]: x's own writes, in its WAL
    cursor,      \* cursor[e]: shipper e's acknowledged position
    reg,         \* reg[x]: x's replica of the key
    clk,         \* clk[x]: x's leaf clock, which a reap does not reset
    reaped,      \* the highest HLC of a reaped tombstone
    fellOff,     \* s trimmed its log past r's cursor
    booted,      \* the full re-bootstrap that fall-off requests has completed
    scopedDone,  \* the one range-scoped export has run
    topo,        \* s's shard-map version
    gen,         \* s's tree generation: restore epoch and physical identity
    restored,    \* a restore, purge or rebind removed the key at s
    ex           \* the export in progress

vars == <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone,
          topo, gen, restored, ex>>

(* r may pre-capture its entry: live, and s is its origin, so s held it. *)
Eligible(u) == u.present /\ ~u.del /\ u.o = s

TypeOK ==
    /\ authored \subseteq Writes
    /\ \A x \in Clusters : wal[x] \in Seq(Writes) /\ Len(wal[x]) <= MaxWrites
    /\ cursor \in [Edges -> 0..MaxWrites]
    /\ reg \in [Clusters -> Regs]
    /\ clk \in [Clusters -> 0..(MaxHlc + 1)]
    /\ reaped \in 0..MaxHlc
    /\ fellOff \in BOOLEAN
    /\ booted \in BOOLEAN
    /\ scopedDone \in BOOLEAN
    /\ topo \in 0..1
    /\ gen \in 0..1
    /\ restored \in BOOLEAN
    /\ ex \in Exports

Init ==
    /\ authored = {}
    /\ wal = [x \in Clusters |-> <<>>]
    /\ cursor = [e \in Edges |-> 0]
    /\ reg = [x \in Clusters |-> Absent]
    /\ clk = [x \in Clusters |-> 0]
    /\ reaped = 0
    /\ fellOff = FALSE
    /\ booted = FALSE
    /\ scopedDone = FALSE
    /\ topo = 0
    /\ gen = 0
    /\ restored = FALSE
    /\ ex = IdleExport

(***************************************************************************)
(* Write(o, h, d): o commits a write, or (s only) deletes the key it holds. *)
(* The HLC is above o's leaf clock and above every reaped tombstone: the   *)
(* grace period is longer than any clock skew, so a write authored after a *)
(* reap stamps above it.                                                   *)
(***************************************************************************)
Write(o, h, d) ==
    /\ Cardinality(authored) < MaxWrites
    /\ h > clk[o]
    /\ h > reaped
    /\ d => (o = s /\ reg[s].present /\ ~reg[s].del)
    /\ LET w == [o |-> o, h |-> h, del |-> d]
       IN /\ authored' = authored \cup {w}
          /\ wal' = [wal EXCEPT ![o] = Append(@, w)]
          /\ reg' = [reg EXCEPT ![o] = AsReg(w)]
          /\ clk' = [clk EXCEPT ![o] = w.h]
    /\ UNCHANGED <<cursor, reaped, fellOff, booted, scopedDone, topo, gen, restored, ex>>

(***************************************************************************)
(* Deliver(e): the shipper on edge e delivers its next entry and the       *)
(* receiver merges it, advancing its leaf clock.                           *)
(***************************************************************************)
Deliver(e) ==
    /\ cursor[e] < Len(wal[e[1]])
    /\ LET w == wal[e[1]][cursor[e] + 1]
       IN /\ reg' = [reg EXCEPT ![e[2]] = Merge(@, AsReg(w))]
          /\ clk' = [clk EXCEPT ![e[2]] = Max2(@, w.h)]
    /\ cursor' = [cursor EXCEPT ![e] = @ + 1]
    /\ UNCHANGED <<authored, wal, reaped, fellOff, booted, scopedDone, topo, gen, restored, ex>>

(***************************************************************************)
(* Trim: s trims its log past entries r has not received. Those entries   *)
(* will never be shipped; the stream resumes after them, and              *)
(* LatticeFallOffLogDetector requests a full re-bootstrap.                *)
(***************************************************************************)
Trim ==
    /\ ~fellOff
    /\ cursor[<<s, r>>] < Len(wal[s])
    /\ \E t \in (cursor[<<s, r>>] + 1)..Len(wal[s]) : cursor' = [cursor EXCEPT ![<<s, r>>] = t]
    /\ fellOff' = TRUE
    /\ UNCHANGED <<authored, wal, reg, clk, reaped, booted, scopedDone, topo, gen, restored, ex>>

(***************************************************************************)
(* Reap: s compacts a tombstone away once its grace period has passed, by *)
(* which time no write it beats is still on its way to s.                 *)
(***************************************************************************)
Reap ==
    /\ reg[s].present
    /\ reg[s].del
    /\ \A i \in (cursor[<<r, s>>] + 1)..Len(wal[r]) : wal[r][i].h > reg[s].h
    /\ reg' = [reg EXCEPT ![s] = Absent]
    /\ reaped' = Max2(reaped, reg[s].h)
    /\ UNCHANGED <<authored, wal, cursor, clk, fellOff, booted, scopedDone, topo, gen, restored, ex>>

(***************************************************************************)
(* Restore: s loses the key without a delete - a restore or revert, a     *)
(* purge and recreate, or an alias rebind to another physical tree - and  *)
(* its tree generation changes. At most once.                             *)
(***************************************************************************)
Restore ==
    /\ ~restored
    /\ reg[s].present
    /\ ~reg[s].del
    /\ reg' = [reg EXCEPT ![s] = Absent]
    /\ gen' = 1
    /\ restored' = TRUE
    /\ UNCHANGED <<authored, wal, cursor, clk, reaped, fellOff, booted, scopedDone, topo, ex>>

(***************************************************************************)
(* Reshard: s's shard map changes, at most once. A scan in progress may   *)
(* pass the key without seeing it.                                        *)
(***************************************************************************)
Reshard ==
    /\ topo = 0
    /\ topo' = 1
    /\ \E sk \in BOOLEAN :
          ex' = IF ex.phase # "idle" /\ ~ex.scanned THEN [ex EXCEPT !.skipped = sk] ELSE ex
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, gen, restored>>

(***************************************************************************)
(* BeginExport(kind): an export opens. "full" is the in-place             *)
(* re-bootstrap fall-off requests; "scoped" is a range-scoped export that  *)
(* does not cover the key, as the anti-entropy bootstrap fallback's is.   *)
(* r pre-captures its entry if s provably held it, and the export records *)
(* s's shard-map version and tree generation at open.                     *)
(***************************************************************************)
BeginExport(kind) ==
    /\ ex.phase = "idle"
    /\ IF kind = "full" THEN fellOff /\ ~booted ELSE ~scopedDone
    /\ ex' = [phase |-> kind, scope |-> kind = "full",
              precap |-> IF Eligible(reg[r]) THEN reg[r] ELSE Absent,
              scanned |-> FALSE, carried |-> FALSE, skipped |-> FALSE,
              topo0 |-> topo, gen0 |-> gen]
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, topo, gen, restored>>

(***************************************************************************)
(* ExportRow: the scan reaches the key. Whatever s holds, tombstone        *)
(* included, is carried and merged at r, unless a reshard made the scan    *)
(* pass it.                                                               *)
(***************************************************************************)
ExportRow ==
    /\ ex.phase # "idle"
    /\ ex.scope
    /\ ~ex.scanned
    /\ IF ex.skipped \/ ~reg[s].present
       THEN /\ ex' = [ex EXCEPT !.scanned = TRUE]
            /\ UNCHANGED <<reg, clk>>
       ELSE /\ ex' = [ex EXCEPT !.scanned = TRUE, !.carried = TRUE]
            /\ reg' = [reg EXCEPT ![r] = Merge(@, reg[s])]
            /\ clk' = [clk EXCEPT ![r] = Max2(@, reg[s].h)]
    /\ UNCHANGED <<authored, wal, cursor, reaped, fellOff, booted, scopedDone, topo, gen, restored>>

(***************************************************************************)
(* EndExport: the drain completes. If the key was pre-captured, is in the  *)
(* export's scope and was not carried, and s's shard map did not change   *)
(* during the export and its tree generation is the one r's copy is       *)
(* aligned with at both ends of the export, r applies a delete attributed *)
(* to s at the captured HLC t. A full re-bootstrap whose shard map changed is *)
(* requested again, so the skipped reconcile is retried.                  *)
(***************************************************************************)
EndExport ==
    /\ ex.phase # "idle"
    /\ ex.scanned \/ ~ex.scope
    /\ LET ok == /\ ex.scope
                 /\ ex.precap.present
                 /\ ~ex.carried
                 /\ topo = ex.topo0
                 /\ gen = ex.gen0
                 /\ ex.gen0 = AlignedGen
           fab == [present |-> TRUE, o |-> s, h |-> ex.precap.h, del |-> TRUE, fab |-> TRUE]
       IN /\ reg' = IF ok THEN [reg EXCEPT ![r] = Merge(@, fab)] ELSE reg
          /\ clk' = IF ok THEN [clk EXCEPT ![r] = Max2(@, fab.h)] ELSE clk
    /\ booted' = (booted \/ (ex.phase = "full" /\ topo = ex.topo0))
    /\ scopedDone' = (scopedDone \/ ex.phase = "scoped")
    /\ ex' = IdleExport
    /\ UNCHANGED <<authored, wal, cursor, reaped, fellOff, topo, gen, restored>>

Quiesced ==
    /\ ex.phase = "idle"
    /\ fellOff => booted
    /\ \A e \in Edges : cursor[e] = Len(wal[e[1]])

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E o \in Clusters, h \in Hlcs, d \in BOOLEAN : Write(o, h, d)
    \/ \E e \in Edges : Deliver(e)
    \/ Trim
    \/ Reap
    \/ Restore
    \/ Reshard
    \/ \E kind \in {"full", "scoped"} : BeginExport(kind)
    \/ ExportRow
    \/ EndExport
    \/ Stutter

(***************************************************************************)
(* Fairness: delivery is fair, a requested full re-bootstrap starts, and  *)
(* an export in progress completes. Writes, trims, reaps, restores,       *)
(* reshards and scoped exports are environment events.                    *)
(***************************************************************************)
Fairness ==
    /\ \A e \in Edges : WF_vars(Deliver(e))
    /\ WF_vars(BeginExport("full"))
    /\ WF_vars(ExportRow)
    /\ WF_vars(EndExport)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* ReconcileDeletesOnlyDeleted: a tombstone the reconcile fabricated is   *)
(* dominated by a delete s really authored. Absence for any other reason  *)
(* must not be turned into a delete: a fabricated tombstone beats the     *)
(* value for good, and nothing re-ships a value behind the trim point.    *)
(***************************************************************************)
ReconcileDeletesOnlyDeleted ==
    reg[r].fab => \E d \in authored : d.o = s /\ d.del /\ d.h >= reg[r].h

(***************************************************************************)
(* Liveness.                                                               *)
(***************************************************************************)
Read(u) == IF u.present /\ ~u.del THEN [live |-> TRUE, o |-> u.o, h |-> u.h]
           ELSE [live |-> FALSE, o |-> s, h |-> 0]

Winner == IF authored = {} THEN Absent
          ELSE AsReg(CHOOSE w \in authored : \A v \in authored : v = w \/ Beats(AsReg(w), AsReg(v)))

Converged == Read(reg[s]) = Read(Winner) /\ Read(reg[r]) = Read(Winner)

(* Residual: r holds a live value of another origin that a delete of s's *)
(* dominates, and s has reaped that delete. Neither the stream (trimmed)  *)
(* nor the export (reaped) carries it, and the reconcile cannot prove s   *)
(* held the value.                                                         *)
Residual ==
    /\ reg[r].present /\ ~reg[r].del /\ reg[r].o # s
    /\ ~reg[s].present
    /\ \E d \in authored : d.o = s /\ d.del /\ Beats(AsReg(d), reg[r])

(* EventualConvergence: once writing stops, both replicas hold the value  *)
(* of every write, the deletes included, except in the Residual and when  *)
(* s lost the key without a delete, which no replication protocol undoes. *)
EventualConvergence == <>[](restored \/ Residual \/ Converged)

=============================================================================
