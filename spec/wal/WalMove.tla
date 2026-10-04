------------------------------ MODULE WalMove ------------------------------
(***************************************************************************)
(* An abstract TLA+ specification of a WAL shard move in Orleans.Lattice:  *)
(* the stream is fenced against new appends, quiesced (every in-flight     *)
(* append drained), copied to its new home, and switched over, while       *)
(* appends, out-of-order flushes, a cursor-advancing consumer and the GC   *)
(* run concurrently and the shard may crash at any step.                   *)
(*                                                                         *)
(* The leaf lifecycle is specified in WalDurability.tla; a move touches    *)
(* none of its leaf state, so the two are checked separately. Here one     *)
(* consumer stands for every reader of the stream (leaf replay, the        *)
(* replication shipper, view maintainers), and the GC trims only below     *)
(* what it has consumed.                                                   *)
(*                                                                         *)
(* See Refinement.md for the mapping to WalMoveFenceCore, WalMoveResumeCore *)
(* and the move coordinator, and for what this model does NOT cover.       *)
(*                                                                         *)
(* Epic #4430, issue #4432.                                                *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, TLC

MaxOff == 3
Offs == 0..(MaxOff - 1)
MaxCrashes == 1
MaxMoves == 1

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

(***************************************************************************)
(* State.                                                                  *)
(*  next      the next offset the allocator hands out.                     *)
(*  inflight  offsets assigned to an append whose flush has not completed. *)
(*  durable   offsets the stream's current home holds (trim removes them). *)
(*  tail      the oldest readable offset.                                  *)
(*  acked     offsets acknowledged to their writer (only once durable).    *)
(*  cons      the consumer's read position, -1 before it has read.         *)
(*  move      "idle", "fenced" (appends refused, draining) or "copied".    *)
(*  moveCopy  the offsets the move copied to the target home.              *)
(*  moves     moves started so far; crashes  shard crashes so far.         *)
(***************************************************************************)
VARIABLES next, inflight, durable, tail, acked, cons, move, moveCopy, moves, crashes

vars == <<next, inflight, durable, tail, acked, cons, move, moveCopy, moves, crashes>>

TypeOK ==
   /\ next \in 0..MaxOff
   /\ inflight \subseteq Offs
   /\ durable \subseteq Offs
   /\ tail \in 0..MaxOff
   /\ acked \subseteq Offs
   /\ cons \in -1..(MaxOff - 1)
   /\ move \in {"idle", "fenced", "copied"}
   /\ moveCopy \subseteq Offs
   /\ moves \in 0..MaxMoves
   /\ crashes \in 0..MaxCrashes

Watermark == IF inflight = {} THEN next ELSE Min(inflight)

Init ==
    /\ next = 0
    /\ inflight = {}
    /\ durable = {}
    /\ tail = 0
    /\ acked = {}
    /\ cons = -1
    /\ move = "idle"
    /\ moveCopy = {}
    /\ moves = 0
    /\ crashes = 0

(***************************************************************************)
(* Append: the allocator hands the next offset to a new write, atomically  *)
(* with the move-fence check (WalMoveFenceCore.IsAppendAdmitted under the  *)
(* shard's state gate, WalOffsetAllocationCore.Assign).                    *)
(***************************************************************************)
Append ==
    /\ move = "idle"
    /\ next < MaxOff
    /\ inflight' = inflight \cup {next}
    /\ next' = next + 1
    /\ UNCHANGED <<durable, tail, acked, cons, move, moveCopy, moves, crashes>>

(***************************************************************************)
(* FlushAck(o): an in-flight append's flush lands on the stream's current  *)
(* home, in any order, and the write is acknowledged.                      *)
(***************************************************************************)
FlushAck(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<next, tail, cons, move, moveCopy, moves, crashes>>

(***************************************************************************)
(* Consume: the consumer reads the next offset below the durable-contiguous *)
(* watermark (WalShippingWatermark); a read below the tail lands on it.    *)
(***************************************************************************)
ConsumeFrom == IF cons + 1 < tail THEN tail ELSE cons + 1

Consume ==
    /\ ConsumeFrom < Watermark
    /\ cons' = ConsumeFrom
    /\ UNCHANGED <<next, inflight, durable, tail, acked, move, moveCopy, moves, crashes>>

(***************************************************************************)
(* GcTrim: the GC trims the prefix the consumer has read.                  *)
(***************************************************************************)
GcTrim ==
    /\ \E t \in (tail + 1)..next :
         /\ t - 1 <= cons
         /\ tail' = t
         /\ durable' = {o \in durable : o >= t}
    /\ UNCHANGED <<next, inflight, acked, cons, move, moveCopy, moves, crashes>>

(***************************************************************************)
(* MoveFence: an operator starts a move; the fence refuses new appends.    *)
(* MoveCopy: once the stream has quiesced - no append in flight, the stale *)
(* observation WalMoveFenceCore aborts on - the copy is taken.             *)
(* MoveSwitch: the stream switches to the copy and the fence is lowered.   *)
(***************************************************************************)
MoveFence ==
    /\ move = "idle"
    /\ moves < MaxMoves
    /\ move' = "fenced"
    /\ moves' = moves + 1
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, moveCopy, crashes>>

MoveCopy ==
    /\ move = "fenced"
    /\ inflight = {}
    /\ moveCopy' = durable
    /\ move' = "copied"
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, moves, crashes>>

MoveSwitch ==
    /\ move = "copied"
    /\ durable' = {o \in moveCopy : o >= tail}
    /\ move' = "idle"
    /\ moveCopy' = {}
    /\ UNCHANGED <<next, inflight, tail, acked, cons, moves, crashes>>

(***************************************************************************)
(* ShardCrash: the shard's activation is lost at any step. Unflushed       *)
(* appends are lost (none was acknowledged), an unfinished move is         *)
(* abandoned with its fence, and the allocator recovers one past the       *)
(* highest stored offset, never below the tail.                            *)
(***************************************************************************)
ShardCrash ==
    /\ crashes < MaxCrashes
    /\ inflight' = {}
    /\ next' = Max({tail} \cup {o + 1 : o \in durable})
    /\ move' = "idle"
    /\ moveCopy' = {}
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<durable, tail, acked, cons, moves>>

(***************************************************************************)
(* A fully quiesced stream has an explicit stuttering successor so natural *)
(* termination is not reported as a deadlock.                              *)
(***************************************************************************)
Quiesced ==
    /\ next = MaxOff
    /\ inflight = {}
    /\ move = "idle"
    /\ tail = MaxOff

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ Append
    \/ \E o \in Offs : FlushAck(o)
    \/ Consume
    \/ GcTrim
    \/ MoveFence
    \/ MoveCopy
    \/ MoveSwitch
    \/ ShardCrash
    \/ Stutter

(***************************************************************************)
(* Fairness: appends, flushes, the consumer, the GC and an in-progress move *)
(* make progress. Starting a move and crashing the shard are environment   *)
(* events, bounded by MaxMoves and MaxCrashes and deliberately not fair.   *)
(***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Append)
    /\ \A o \in Offs : WF_vars(FlushAck(o))
    /\ WF_vars(Consume)
    /\ WF_vars(GcTrim)
    /\ WF_vars(MoveCopy)
    /\ WF_vars(MoveSwitch)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* Every acknowledged write survives every move: it is still on the stream's
\* current home, or the consumer has read it and the GC has trimmed it.
MovedStreamKeepsAckedWrites ==
    \A o \in acked : o \in durable \/ (o < tail /\ o <= cons)

\* A move copies only a quiesced stream: once copied, nothing is in flight,
\* so no append can land on the old home after the copy was taken.
CopyTakenQuiesced ==
    move = "copied" => inflight = {}

\* The consumer never reads past an append that is still in flight.
ReaderNeverPassesHole ==
    \A o \in inflight : cons < o

\* No acknowledged offset is handed out again, and every assigned offset is
\* below the allocator.
AllocatorNeverReissues ==
    /\ acked \cap inflight = {}
    /\ \A o \in acked \cup inflight : o < next

(***************************************************************************)
(* Liveness.                                                               *)
(***************************************************************************)

\* Every append eventually completes, whatever moves and crashes intervene: a
\* move's fence is always eventually lowered, so the allocator reaches the end
\* of the stream with nothing left in flight. (An append a crash lost was never
\* acknowledged, and its writer retries at a fresh offset, so the hole it leaves
\* is not owed a completion.)
StreamEventuallyComplete ==
    <>(next = MaxOff /\ inflight = {})

=============================================================================
