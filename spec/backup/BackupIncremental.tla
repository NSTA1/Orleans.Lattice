------------------------- MODULE BackupIncremental -------------------------
(***************************************************************************)
(* An abstract TLA+ specification of an incremental backup racing an       *)
(* atomic saga (issue #4589): the forward WAL drain from the base's        *)
(* recorded frontier, the saga's prepared writes and per-shard terminals   *)
(* in the delta window, their resolution against the #4485 decision        *)
(* snapshot (d0), the frontier held back for an unsettled saga, the sagas  *)
(* a link resolved as undecided and hands on, and the fall back to a full  *)
(* backup when a committed batch straddles the window.                     *)
(*                                                                         *)
(* It models the protocol DESIGN, not the code. One saga s writes keys a   *)
(* and b. As in production, a prepare is routed to its key's WAL partition *)
(* and a shard's terminal to its shard's partition, which here is the      *)
(* other one, so a saga's prepares and terminals never share a partition.  *)
(* See RefinementIncremental.md for the mapping to production symbols.     *)
(*                                                                         *)
(* Epic #4430, issue #4589.                                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

Keys == {"a", "b"}
Parts == {1, 2}
KeyPart == [k \in Keys |-> IF k = "a" THEN 1 ELSE 2]
TermPart == [k \in Keys |-> IF k = "a" THEN 2 ELSE 1]
MaxChain == 3
Decisions == {"none", "committed", "aborted"}

(***************************************************************************)
(* State.                                                                  *)
(*  wal      each partition's records, in offset order. A record is a      *)
(*           prepare of key x, or a commit / abort terminal of shard x     *)
(*           (each key is its own shard).                                  *)
(*  dec      the saga's recorded decision (the tree's local Mark).         *)
(*  phase    the capture engine: idle, between the full capture's head    *)
(*           read and its snapshot, or holding the gate for an increment.  *)
(*  pending  the WAL heads a full capture read before its snapshot.        *)
(*  gated    the #4485 decision gate is held: no decision may be recorded. *)
(*  d0       the decision snapshot taken when the gate was acquired.       *)
(*  chain    the backup chain. Each link is a full capture or an           *)
(*           increment and records: the per-partition frontier the next    *)
(*           increment resumes from (heads), the saga keys it holds post-  *)
(*           saga (emit), whether its decision snapshot held the saga      *)
(*           committed (dc), whether it is a full backup taken because an  *)
(*           increment fell back (fb), and whether it holds the saga as    *)
(*           undecided for the next increment to look up (und).            *)
(***************************************************************************)
VARIABLES wal, dec, phase, pending, gated, d0, chain

vars == <<wal, dec, phase, pending, gated, d0, chain>>
sagaVars == <<wal, dec>>

Rec == [k : {"prep", "commit", "abort"}, x : Keys]
Heads == [Parts -> 0..4]
Link == [kind : {"full", "incr"}, heads : Heads, emit : SUBSET Keys, dc : BOOLEAN, fb : BOOLEAN, und : BOOLEAN]

TypeOK ==
    /\ wal \in [Parts -> Seq(Rec)]
    /\ \A p \in Parts : Len(wal[p]) <= 4
    /\ dec \in Decisions
    /\ phase \in {"idle", "fullsnap", "drain"}
    /\ pending \in Heads
    /\ gated \in BOOLEAN
    /\ d0 \in Decisions
    /\ chain \in Seq(Link)
    /\ Len(chain) <= MaxChain

Lens == [p \in Parts |-> Len(wal[p])]
Has(kind, x) == \E p \in Parts : \E i \in 1..Len(wal[p]) : wal[p][i] = [k |-> kind, x |-> x]
Prepared == {x \in Keys : Has("prep", x)}
Terminated == {x \in Keys : Has("commit", x) \/ Has("abort", x)}
Min(S) == CHOOSE m \in S : \A n \in S : m <= n
Max(S) == CHOOSE m \in S : \A n \in S : n <= m

Init ==
    /\ wal = [p \in Parts |-> << >>]
    /\ dec = "none"
    /\ phase = "idle"
    /\ pending = [p \in Parts |-> 0]
    /\ gated = FALSE
    /\ d0 = "none"
    /\ chain = << >>

(***************************************************************************)
(* The saga (environment). Prepare stages one key's prepared write; Decide *)
(* records the decision, which the gate refuses while it is held; Terminal *)
(* appends one shard's terminal, only after the decision (invariant I1).   *)
(***************************************************************************)
Prepare(x) ==
    /\ dec = "none"
    /\ x \notin Prepared
    /\ wal' = [wal EXCEPT ![KeyPart[x]] = Append(@, [k |-> "prep", x |-> x])]
    /\ UNCHANGED <<dec, phase, pending, gated, d0, chain>>

Decide(o) ==
    /\ dec = "none"
    /\ ~gated
    /\ o = "committed" => Prepared = Keys
    /\ dec' = o
    /\ UNCHANGED <<wal, phase, pending, gated, d0, chain>>

Terminal(x) ==
    /\ dec # "none"
    /\ x \notin Terminated
    /\ wal' = [wal EXCEPT ![TermPart[x]] =
                 Append(@, [k |-> IF dec = "committed" THEN "commit" ELSE "abort", x |-> x])]
    /\ UNCHANGED <<dec, phase, pending, gated, d0, chain>>

(***************************************************************************)
(* The full capture that roots the chain, in two steps as in production:  *)
(* it reads the WAL heads, then opens its snapshot under the decision      *)
(* gate, where every pending bucket resolves against the decision (#4485). *)
(* A saga pending and undecided at the snapshot is recorded as undecided.  *)
(***************************************************************************)
FullLink(h, fb) ==
    [kind |-> "full", heads |-> h,
     emit |-> IF dec = "committed" THEN Keys ELSE {},
     dc |-> dec = "committed", fb |-> fb,
     und |-> dec = "none" /\ Prepared # {}]

FullHeads ==
    /\ phase = "idle"
    /\ chain = << >>
    /\ pending' = Lens
    /\ phase' = "fullsnap"
    /\ UNCHANGED <<gated, d0, chain>>
    /\ UNCHANGED sagaVars

FullSnap ==
    /\ phase = "fullsnap"
    /\ chain' = << FullLink(pending, FALSE) >>
    /\ phase' = "idle"
    /\ UNCHANGED <<pending, gated, d0>>
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* GateAcquire: an increment takes the decision gate and its snapshot d0   *)
(* before it drains. Writes - prepares and terminals of an already decided *)
(* saga - go on; only a new decision waits.                                *)
(***************************************************************************)
GateAcquire ==
    /\ phase = "idle"
    /\ chain # << >>
    /\ Len(chain) < MaxChain
    /\ gated' = TRUE
    /\ d0' = dec
    /\ phase' = "drain"
    /\ UNCHANGED <<pending, chain>>
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* The delta window: every record after the last link's frontier.          *)
(***************************************************************************)
Last == chain[Len(chain)]
Window(p) == {i \in 1..Len(wal[p]) : i > Last.heads[p]}
WinRecs == UNION {{wal[p][i] : i \in Window(p)} : p \in Parts}
InWindow == WinRecs # {}
SeenPrep == {r.x : r \in {q \in WinRecs : q.k = "prep"}}
CommitShards == {r.x : r \in {q \in WinRecs : q.k = "commit"}}
AbortSeen == \E r \in WinRecs : r.k = "abort"
Complete == SeenPrep = Keys
TermsComplete == CommitShards = Keys

(***************************************************************************)
(* The staging rule (IncrementalSagaStaging). A batch committed in d0 is   *)
(* emitted whole once its prepares are complete; an aborted one is         *)
(* dropped; an undecided one is dropped and holds the frontier back to its *)
(* earliest record on each partition, as does a committed one whose shard  *)
(* terminals are still to come. A saga the base link holds as undecided is *)
(* looked up in d0 even when none of its records is in the window; one    *)
(* still undecided is handed on. A committed batch the window does not     *)
(* hold whole - or a commit terminal d0 does not explain - falls back.     *)
(***************************************************************************)
Tracked == InWindow \/ Last.und

Emit == IF ~AbortSeen /\ d0 = "committed" /\ Complete THEN Keys ELSE {}

Held ==
    /\ InWindow
    /\ ~AbortSeen
    /\ \/ d0 = "none"
       \/ d0 = "committed" /\ Complete /\ ~TermsComplete

FallsBack ==
    /\ Tracked
    /\ ~AbortSeen
    /\ \/ d0 = "committed" /\ ~Complete
       \/ d0 # "committed" /\ CommitShards # {}

NewHeads == [p \in Parts |-> IF Held /\ Window(p) # {} THEN Min(Window(p)) - 1 ELSE Len(wal[p])]

(***************************************************************************)
(* Drain: the increment reads its window, settles it, and releases the     *)
(* gate. A fall back takes a fresh full backup instead, still under the    *)
(* gate, so its snapshot resolves against the same decision.               *)
(***************************************************************************)
Drain ==
    /\ phase = "drain"
    /\ chain' = Append(chain,
         IF FallsBack
         THEN FullLink(Lens, TRUE)
         ELSE [kind |-> "incr", heads |-> NewHeads, emit |-> Emit,
               dc |-> d0 = "committed", fb |-> FALSE,
               und |-> Last.und /\ d0 = "none"])
    /\ gated' = FALSE
    /\ phase' = "idle"
    /\ UNCHANGED <<pending, d0>>
    /\ UNCHANGED sagaVars

\* Natural termination once the chain is full and the saga has settled.
Stutter == UNCHANGED vars

Next ==
    \/ \E x \in Keys : Prepare(x)
    \/ \E o \in {"committed", "aborted"} : Decide(o)
    \/ \E x \in Keys : Terminal(x)
    \/ FullHeads
    \/ FullSnap
    \/ GateAcquire
    \/ Drain
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties. Restored(n) is what restoring the chain up to link n        *)
(* holds of the saga: the keys any link since the latest full one holds    *)
(* post-saga (a chain restore folds its links last-writer-wins).           *)
(***************************************************************************)
Restored(n) ==
    LET f == Max({j \in 1..n : chain[j].kind = "full"})
    IN UNION {chain[j].emit : j \in f..n}

\* No restore of the chain holds the batch partially.
BackupSagaConsistent ==
    \A n \in 1..Len(chain) : Restored(n) \in {{}, Keys}

\* No restore of the chain holds a write of a saga that did not commit.
CaptureStrictIsolation ==
    \A n \in 1..Len(chain) : Restored(n) # {} => dec = "committed"

\* A link whose decision snapshot holds the saga committed restores it whole.
ChainCoversCommitted ==
    \A n \in 1..Len(chain) : chain[n].dc => Restored(n) = Keys

\* An increment falls back to a full backup only for a batch that straddles
\* the frontier of the full capture the chain grew from: the held-back
\* frontier means no increment strands a batch it has already read part of.
SagaFallbackOnlyAcrossFull ==
    \A n \in 2..Len(chain) :
        chain[n].fb =>
            LET f == Max({j \in 1..(n - 1) : chain[j].kind = "full"})
            IN \E p \in Parts : \E i \in 1..Len(wal[p]) :
                   i <= chain[f].heads[p] /\ wal[p][i].k = "prep"
=============================================================================
