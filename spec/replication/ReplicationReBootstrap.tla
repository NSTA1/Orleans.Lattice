----------------------- MODULE ReplicationReBootstrap -----------------------
(***************************************************************************)
(* An in-place re-bootstrap after the source reaped a delete, the focused  *)
(* companion of Replication.tla.                                           *)
(*                                                                         *)
(* A receiver r that fell off the source s's log is re-bootstrapped over   *)
(* the copy it already holds. The export carries every row s still holds,  *)
(* tombstones included (#4504), but s garbage-collects tombstones, so a    *)
(* delete that is both behind the trim point and reaped reaches r by no    *)
(* path at all. The receiver reconciles it: before the export opens, r     *)
(* pre-captures its live entry, and if the export does not carry the key,  *)
(* r applies a delete attributed to s at the captured HLC t, which wins   *)
(* the tie at t and dominates only what s's own delete dominates.          *)
(*                                                                         *)
(* A captured entry s authored is reconciled when r is aligned with s's    *)
(* current lineage (#4537, built by #4647). An entry of another origin -   *)
(* r's own write, here - is reconciled when s provably applied it: its HLC *)
(* is below the low watermark s holds for that origin at export open       *)
(* (#4549, on #4586's watermark). The reconcile fabricates a write, so it  *)
(* is gated on every way a key can be absent from an export without a      *)
(* delete: the export's scope, a reshard or resize during the scan, a      *)
(* soft-deleted source tree, and a source restore, purge or alias rebind,  *)
(* which re-stamps the lineage.                                            *)
(*                                                                         *)
(* A tombstone is reaped only once no write it beats is still on its way   *)
(* to s (#4615). A receiver restore re-stamps r's lineage, which every     *)
(* sender treats as a gap and answers with a re-seed (#4586). A unilateral *)
(* source restore is the one behaviour convergence is not claimed for: r   *)
(* keeps what the restore dropped, and a coordinated restore converges     *)
(* both clusters (the source-restore contract).                            *)
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

Exports == [phase : {"idle", "full", "scoped", "reconcile"}, scope : BOOLEAN, precap : Regs,
            scanned : BOOLEAN, carried : BOOLEAN, skipped : BOOLEAN,
            topo0 : 0..1, gen0 : 0..1, phys0 : 0..1, del0 : BOOLEAN, dep0 : BOOLEAN,
            lwm0 : 0..(MaxHlc + 2)]
IdleExport == [phase |-> "idle", scope |-> FALSE, precap |-> Absent,
               scanned |-> FALSE, carried |-> FALSE, skipped |-> FALSE,
               topo0 |-> 0, gen0 |-> 0, phys0 |-> 0, del0 |-> FALSE, dep0 |-> FALSE,
               lwm0 |-> 0]

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
    gen,         \* s's lineage token on the logical tree's registry entry
    restored,    \* a restore, purge or rebind removed the key at s
    phys,        \* s's physical tree behind the alias
    deleted,     \* s's tree is soft-deleted
    delDone,     \* s's soft-delete epoch: the one soft delete has happened
    owed,        \* a reconcile a mid-export topology change skipped is owed
    aligned,     \* the source lineage r's copy is aligned with
    restoredR,   \* a receiver restore removed the key at r
    coordDone,   \* a coordinated restore has run
    lostR,       \* r-writes s had applied when its lineage was re-stamped
    oldS,        \* s's log entries written before its lineage was re-stamped
    detached,    \* r is detached as a peer of s's tree, until its re-seed
    ex           \* the export in progress

vars == <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone,
          topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(* At most one disruption per behaviour: a source restore, reshard,       *)
(* resize or soft delete, or a receiver restore. Each gate is checked      *)
(* against the disruption it exists for; the bound keeps the instance     *)
(* small.                                                                  *)
Undisrupted == topo = 0 /\ phys = 0 /\ ~restored /\ ~delDone /\ ~restoredR

(* r pre-captures its live entry if s is its origin, so s held it. *)
Eligible(u) == u.present /\ ~u.del /\ u.o = s

(* The low watermark s holds for r's writes in its current lineage: every *)
(* r-write with an HLC below it is applied at s in that lineage. r's       *)
(* writes stamp in increasing HLC order and reach s in order, so the next  *)
(* undelivered one bounds it; once a restore has re-stamped s's lineage,   *)
(* the r-writes s had applied before it are not in the new contents, and   *)
(* the lowest of them bounds it (#4586: the watermark is per lineage).     *)
(* With nothing undelivered, r's next write stamps above r's clock.        *)
SrcLwm ==
    IF lostR > 0 THEN wal[r][1].h
    ELSE IF cursor[<<r, s>>] = Len(wal[r]) THEN clk[r] + 1
    ELSE wal[r][cursor[<<r, s>>] + 1].h

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
    /\ phys \in 0..1
    /\ deleted \in BOOLEAN
    /\ delDone \in BOOLEAN
    /\ owed \in BOOLEAN
    /\ aligned \in 0..1
    /\ restoredR \in BOOLEAN
    /\ coordDone \in BOOLEAN
    /\ lostR \in 0..MaxWrites
    /\ oldS \in 0..MaxWrites
    /\ detached \in BOOLEAN
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
    /\ phys = 0
    /\ deleted = FALSE
    /\ delDone = FALSE
    /\ owed = FALSE
    /\ aligned = 0
    /\ restoredR = FALSE
    /\ coordDone = FALSE
    /\ lostR = 0
    /\ oldS = 0
    /\ detached = FALSE
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
    /\ o = s => ~deleted
    /\ LET w == [o |-> o, h |-> h, del |-> d]
       IN /\ authored' = authored \cup {w}
          /\ wal' = [wal EXCEPT ![o] = Append(@, w)]
          /\ reg' = [reg EXCEPT ![o] = AsReg(w)]
          /\ clk' = [clk EXCEPT ![o] = w.h]
    /\ UNCHANGED <<cursor, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* Deliver(e): the shipper on edge e delivers its next entry and the       *)
(* receiver merges it, advancing its leaf clock. An entry s logged before  *)
(* its lineage was re-stamped, reaching an r already aligned with the new  *)
(* lineage, is acknowledged without being applied: the batch carries the   *)
(* source lineage it was read under, and an aligned receiver refuses       *)
(* another (#4673), so a write the restore dropped cannot reappear at r    *)
(* under the new lineage and be taken for a write s later deleted.         *)
(***************************************************************************)
StaleLineage(e) ==
    e = <<s, r>> /\ cursor[e] + 1 <= oldS /\ restored /\ aligned = gen

Deliver(e) ==
    /\ cursor[e] < Len(wal[e[1]])
    /\ LET w == wal[e[1]][cursor[e] + 1]
       IN IF StaleLineage(e) THEN UNCHANGED <<reg, clk>>
          ELSE /\ reg' = [reg EXCEPT ![e[2]] = Merge(@, AsReg(w))]
               /\ clk' = [clk EXCEPT ![e[2]] = Max2(@, w.h)]
    /\ cursor' = [cursor EXCEPT ![e] = @ + 1]
    /\ UNCHANGED <<authored, wal, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* Trim: s trims its log past entries r has not received. Those entries   *)
(* will never be shipped; the stream resumes after them, and s's shipper,  *)
(* whose read finds the gap, requests a full re-bootstrap (#4599).         *)
(***************************************************************************)
Trim ==
    /\ ~fellOff
    /\ cursor[<<s, r>>] < Len(wal[s])
    /\ \E t \in (cursor[<<s, r>>] + 1)..Len(wal[s]) : cursor' = [cursor EXCEPT ![<<s, r>>] = t]
    /\ fellOff' = TRUE
    /\ UNCHANGED <<authored, wal, reg, clk, reaped, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* Reap: s compacts a tombstone away once no write it beats is still on   *)
(* its way to s - its receive frontier D is above it - and every attached  *)
(* peer has it - its vouched watermark P is above it, which a peer owed a  *)
(* re-seed holds back until the re-seed's export, which carries the       *)
(* tombstone, completes (#4615). A detached peer does not hold it back.   *)
(* Production waits only for the wall-clock grace period (mutation         *)
(* EventualConvergenceReapInsideGrace).                                    *)
(***************************************************************************)
DeleteShipped ==
    \E i \in 1..cursor[<<s, r>>] : wal[s][i] = [o |-> s, h |-> reg[s].h, del |-> TRUE]

Reap ==
    /\ reg[s].present
    /\ reg[s].del
    /\ \A i \in (cursor[<<r, s>>] + 1)..Len(wal[r]) : wal[r][i].h > reg[s].h
    /\ detached \/ (DeleteShipped /\ ~(fellOff /\ ~booted))
    /\ reg' = [reg EXCEPT ![s] = Absent]
    /\ reaped' = Max2(reaped, reg[s].h)
    /\ UNCHANGED <<authored, wal, cursor, clk, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* Detach: an operator detaches r as a peer of s's tree (#4534-B), and     *)
(* later re-attaches it, which requests a full re-seed. While detached r   *)
(* holds back no reap, so a delete it missed can be reaped before its re-  *)
(* seed, which the reconcile repairs. It can follow an earlier re-seed:    *)
(* only one in progress blocks it.                                         *)
(***************************************************************************)
Detach ==
    /\ fellOff => booted
    /\ ~detached
    /\ cursor' = [cursor EXCEPT ![<<s, r>>] = Len(wal[s])]
    /\ detached' = TRUE
    /\ fellOff' = TRUE
    /\ booted' = FALSE
    /\ UNCHANGED <<authored, wal, reg, clk, reaped, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, ex>>

(***************************************************************************)
(* Restore: a unilateral source restore. s loses the key without a delete *)
(* - a restore or revert, a purge and recreate, or an alias rebind to      *)
(* another physical tree - and its lineage is re-stamped. It is the one    *)
(* disruption (Undisrupted). r keeps what s lost: the reconcile fabricates *)
(* no delete for it (ReconcileDeletesOnlyDeleted), and convergence is not *)
(* claimed until a coordinated restore runs (the source-restore contract). *)
(***************************************************************************)
Restore ==
    /\ Undisrupted
    /\ reg[s].present
    /\ ~reg[s].del
    /\ reg' = [reg EXCEPT ![s] = Absent]
    /\ gen' = 1
    /\ restored' = TRUE
    /\ lostR' = cursor[<<r, s>>]
    /\ oldS' = Len(wal[s])
    /\ UNCHANGED <<authored, wal, cursor, clk, reaped, fellOff, booted, scopedDone, topo, phys, deleted, delDone, owed, aligned, restoredR, coordDone, detached, ex>>

(***************************************************************************)
(* CoordinatedRestore: the remedy for a unilateral source restore. Every   *)
(* cluster's receive fence is held and every cluster cuts over to the same *)
(* restore point c; the streams resume after the cut, so nothing in flight *)
(* from before it is re-applied. r is aligned with the new lineage, and   *)
(* every write before the cut is reflected by it or superseded, so s's    *)
(* watermark is re-derived from the cut (the re-stamp's forced re-seed).   *)
(* Not fair: an operator runs it.                                          *)
(***************************************************************************)
CoordinatedRestore ==
    /\ restored /\ ~coordDone
    /\ ex.phase = "idle"
    /\ \E c \in {Absent} \cup {AsReg(w) : w \in authored} :
          /\ reg' = [x \in Clusters |-> c]
          /\ clk' = [x \in Clusters |-> Max2(clk[x], c.h)]
    /\ cursor' = [e \in Edges |-> Len(wal[e[1]])]
    /\ aligned' = gen
    /\ coordDone' = TRUE
    /\ lostR' = 0
    /\ UNCHANGED <<authored, wal, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, restoredR, oldS, detached, ex>>

(***************************************************************************)
(* RestoreR: a receiver restore removes the key at r and re-stamps r's    *)
(* lineage. Its next acknowledgement carries the new lineage, which every *)
(* sender treats as a forced gap: it requests a re-seed and rewinds after  *)
(* the echo (#4586). As the one disruption. It is taken once r's own     *)
(* writes have reached s: a restore that destroys r's unshipped writes is  *)
(* a unilateral restore of their source, which the source-restore contract *)
(* covers as Restore does for s.                                           *)
(***************************************************************************)
RestoreR ==
    /\ Undisrupted
    /\ ex.phase = "idle"
    /\ cursor[<<r, s>>] = Len(wal[r])
    /\ reg[r].present
    /\ reg' = [reg EXCEPT ![r] = Absent]
    /\ restoredR' = TRUE
    /\ fellOff' = TRUE
    /\ booted' = FALSE
    /\ UNCHANGED <<authored, wal, cursor, clk, reaped, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* Reshard: s's shard map changes, as the one disruption. A scan in        *)
(* progress may pass the key without seeing it.                            *)
(***************************************************************************)
Reshard ==
    /\ Undisrupted
    /\ topo' = 1
    /\ \E sk \in BOOLEAN :
          ex' = IF ex.phase # "idle" /\ ~ex.scanned THEN [ex EXCEPT !.skipped = ex.skipped \/ sk] ELSE ex
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached>>

(***************************************************************************)
(* Resize: s copies its tree into a new physical tree and swaps the alias, *)
(* as the one disruption. Content is preserved, so the lineage token is    *)
(* unchanged, but a scan in progress may pass the key without seeing it.   *)
(***************************************************************************)
Resize ==
    /\ Undisrupted
    /\ phys' = 1
    /\ \E sk \in BOOLEAN :
          ex' = IF ex.phase # "idle" /\ ~ex.scanned THEN [ex EXCEPT !.skipped = ex.skipped \/ sk] ELSE ex
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, topo, gen, restored, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached>>

(***************************************************************************)
(* SoftDelete / Recover: s's tree is soft-deleted, as the one              *)
(* disruption, and recovered.                                              *)
(* While deleted it serves nothing, so an export carries no row; recovery *)
(* brings every key back. Recovery is fair: a soft delete ends in recovery *)
(* or a purge, and a purge unregisters the tree, which the export reports *)
(* as an unknown generation and the reconcile skips.                      *)
(***************************************************************************)
SoftDelete ==
    /\ Undisrupted
    /\ deleted' = TRUE
    /\ delDone' = TRUE
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

Recover ==
    /\ deleted
    /\ deleted' = FALSE
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached, ex>>

(***************************************************************************)
(* BeginExport(kind): an export opens. "full" is the in-place             *)
(* re-bootstrap fall-off requests; "scoped" is a range-scoped export that  *)
(* does not cover the key, as the anti-entropy bootstrap fallback's is;   *)
(* "reconcile" is the retry pass an owed reconcile runs: a re-bootstrap's *)
(* drain again, rows and all, with its own pre-capture.                   *)
(* r pre-captures its live entry, and the export records s's shard-map    *)
(* version, tree generation and low watermark for r's writes at open.     *)
(***************************************************************************)
BeginExport(kind) ==
    /\ ex.phase = "idle"
    /\ CASE kind = "full" -> fellOff /\ ~booted
          [] kind = "reconcile" -> booted /\ owed
          [] OTHER -> ~scopedDone
    /\ ex' = [phase |-> kind, scope |-> kind # "scoped",
              precap |-> IF Eligible(reg[r]) THEN reg[r] ELSE Absent,
              scanned |-> FALSE, carried |-> FALSE, skipped |-> FALSE,
              topo0 |-> topo, gen0 |-> gen, phys0 |-> phys, del0 |-> deleted, dep0 |-> delDone,
              lwm0 |-> SrcLwm]
    /\ UNCHANGED <<authored, wal, cursor, reg, clk, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached>>

(***************************************************************************)
(* ExportRow: the scan reaches the key. Whatever s holds, tombstone        *)
(* included, is carried and merged at r, unless a reshard or resize made  *)
(* the scan pass it or the tree is soft-deleted. A retry pass merges its  *)
(* rows too: the pass it repays may have carried nothing.                 *)
(***************************************************************************)
ExportRow ==
    /\ ex.phase # "idle"
    /\ ex.scope
    /\ ~ex.scanned
    /\ IF ex.skipped \/ deleted \/ ~reg[s].present
       THEN /\ ex' = [ex EXCEPT !.scanned = TRUE]
            /\ UNCHANGED <<reg, clk>>
       ELSE /\ ex' = [ex EXCEPT !.scanned = TRUE, !.carried = TRUE]
            /\ reg' = [reg EXCEPT ![r] = Merge(@, reg[s])]
            /\ clk' = [clk EXCEPT ![r] = Max2(@, reg[s].h)]
    /\ UNCHANGED <<authored, wal, cursor, reaped, fellOff, booted, scopedDone, topo, gen, restored, phys, deleted, delDone, owed, aligned, restoredR, coordDone, lostR, oldS, detached>>

(***************************************************************************)
(* EndExport: the drain completes. If the key was pre-captured, is in the  *)
(* export's scope and was not carried; s's shard map and physical tree did *)
(* not change during the export; the tree was not soft-deleted at either  *)
(* end, nor deleted and recovered in between (its soft-delete epoch is    *)
(* unchanged); and the lineage is the same at both ends, r applies a       *)
(* delete attributed to s at the captured HLC t - for an entry s authored  *)
(* that r pre-captured, when r's copy is aligned with that lineage, and    *)
(* for an entry of another origin r holds at the end of the drain, when    *)
(* its HLC is below the low watermark s held at open. A                    *)
(* stable pass that orphans no source entry r holds, captured at open or   *)
(* taken during the drain, aligns r with the export's lineage. A pass that was unstable - the shard map or       *)
(* physical tree changed, or the tree was soft-deleted at any point -      *)
(* leaves the pass owed, which a retry repays, and so does an orphaned    *)
(* entry of another origin not yet below the watermark: s's watermark      *)
(* rises as delivery proceeds. A lineage mismatch with an orphaned source  *)
(* entry is permanent, so it is not retried.                               *)
(***************************************************************************)
EndExport ==
    /\ ex.phase # "idle"
    /\ ex.scanned \/ ~ex.scope
    /\ LET stable == /\ topo = ex.topo0
                     /\ phys = ex.phys0
                     /\ ~ex.del0
                     /\ ~deleted
                     /\ delDone = ex.dep0
                     /\ gen = ex.gen0
           orphan == ex.scope /\ ex.precap.present /\ ~ex.carried
           \* An entry of another origin r holds at the end of the drain that
           \* the export did not carry (the foreign reconcile's end scan).
           foreign == ex.scope /\ ~ex.carried /\ reg[r].present /\ ~reg[r].del /\ reg[r].o # s
           okSrc == orphan /\ stable /\ ex.gen0 = aligned
           okFor == foreign /\ stable /\ reg[r].h < ex.lwm0
           ok == okSrc \/ okFor
           fab == [present |-> TRUE, o |-> s, h |-> IF okSrc THEN ex.precap.h ELSE reg[r].h,
                   del |-> TRUE, fab |-> TRUE]
           \* A source entry r took during the drain that the export did not carry.
           LateOrphan == ex.scope /\ ~ex.carried /\ reg[r].present /\ ~reg[r].del /\ reg[r].o = s
       IN /\ reg' = IF ok THEN [reg EXCEPT ![r] = Merge(@, fab)] ELSE reg
          /\ clk' = IF ok THEN [clk EXCEPT ![r] = Max2(@, fab.h)] ELSE clk
          /\ aligned' = IF ex.scope /\ stable /\ ~(orphan /\ ex.precap.o = s) /\ ~LateOrphan
                         THEN gen ELSE aligned
    /\ booted' = (booted \/ ex.phase = "full")
    /\ detached' = (detached /\ ex.phase # "full")
    /\ scopedDone' = (scopedDone \/ ex.phase = "scoped")
    /\ owed' = IF ex.phase = "scoped" THEN owed
               ELSE \/ topo # ex.topo0 \/ phys # ex.phys0 \/ ex.del0 \/ deleted \/ delDone # ex.dep0
                    \/ /\ ex.scope /\ ~ex.carried /\ reg[r].present /\ ~reg[r].del
                       /\ reg[r].o # s /\ reg[r].h >= ex.lwm0
    /\ ex' = IdleExport
    /\ UNCHANGED <<authored, wal, cursor, reaped, fellOff, topo, gen, restored, phys, deleted, delDone, restoredR, coordDone, lostR, oldS>>

Quiesced ==
    /\ ex.phase = "idle"
    /\ fellOff => booted
    /\ ~owed
    /\ ~deleted
    /\ \A e \in Edges : cursor[e] = Len(wal[e[1]])

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E o \in Clusters, h \in Hlcs, d \in BOOLEAN : Write(o, h, d)
    \/ \E e \in Edges : Deliver(e)
    \/ Trim
    \/ Detach
    \/ Reap
    \/ Restore
    \/ CoordinatedRestore
    \/ RestoreR
    \/ Reshard
    \/ Resize
    \/ SoftDelete
    \/ Recover
    \/ \E kind \in {"full", "scoped", "reconcile"} : BeginExport(kind)
    \/ ExportRow
    \/ EndExport
    \/ Stutter

(***************************************************************************)
(* Fairness: delivery is fair, a requested full re-bootstrap and an owed   *)
(* reconcile start, an export in progress completes, and a soft-deleted   *)
(* tree is recovered. Writes, trims, reaps, restores (unilateral,         *)
(* coordinated or at the receiver), reshards, resizes, soft deletes and   *)
(* scoped exports are environment events.                                  *)
(***************************************************************************)
Fairness ==
    /\ \A e \in Edges : WF_vars(Deliver(e))
    /\ WF_vars(BeginExport("full"))
    /\ WF_vars(BeginExport("reconcile"))
    /\ WF_vars(Recover)
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

(* Agreed: both replicas read the same value. *)
Agreed == Read(reg[s]) = Read(reg[r])

(* EventualConvergence: once writing stops, both replicas hold the value  *)
(* of every write, the deletes included, on every behaviour with no        *)
(* unilateral source restore; after one, they agree once a coordinated     *)
(* restore has run (the source-restore contract). A behaviour that leaves *)
(* a unilateral source restore unremedied is the contract's one exclusion. *)
EventualConvergence ==
    <>[](IF ~restored THEN Converged ELSE IF coordDone THEN Agreed ELSE TRUE)

=============================================================================
