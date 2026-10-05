------------------------------ MODULE WalMove ------------------------------
(***************************************************************************)
(* An abstract TLA+ specification of a WAL shard move in Orleans.Lattice:  *)
(* the stream is fenced against new appends, quiesced (every in-flight     *)
(* append drained), copied to its new home, and switched over, while       *)
(* appends, out-of-order flushes, a cursor-advancing consumer and the GC   *)
(* run concurrently, and the shard and the move's coordinator may each     *)
(* crash at any step.                                                      *)
(*                                                                         *)
(* The fence has two halves. The DURABLE fence is a record in the WAL      *)
(* placement pin, raised by the coordinator before it quiesces the source, *)
(* held under a lease, and cleared by the flip, by an abort, or - once the *)
(* lease has lapsed - by a source activation that finds it. The            *)
(* ACTIVATION fence is the source shard's in-memory refusal of appends; it *)
(* is lost with the activation, and every new activation re-derives it     *)
(* from the durable record (issue #4525).                                  *)
(*                                                                         *)
(* The leaf lifecycle is specified in WalDurability.tla; a move touches    *)
(* none of its leaf state, so the two are checked separately. Here one     *)
(* consumer stands for every reader of the stream (leaf replay, the        *)
(* replication shipper, view maintainers), and the GC trims only below     *)
(* what it has consumed.                                                   *)
(*                                                                         *)
(* See MoveRefinement.md for the mapping to WalMoveFenceCore,              *)
(* WalMoveResumeCore and the move coordinator, and for what this model     *)
(* does NOT cover.                                                         *)
(*                                                                         *)
(* Epic #4430, issues #4432 and #4433.                                     *)
(***************************************************************************)
EXTENDS Naturals, Integers, FiniteSets, TLC

MaxOff == 3
Offs == 0..(MaxOff - 1)
MaxCrashes == 1
MaxCoordCrashes == 1

\* The moves an operator may start, one coordinator each. The base checks one;
\* the TwoMoves variant configuration checks two, which can contend for the
\* same partition: a move may take over another move's lapsed fence.
MoveIds == {"m1"}
NoMove == "none"

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
(*  move[m]   move m's coordinator phase: "idle" (not started), "fenced",  *)
(*            "copied", or "done" (flipped, aborted or abandoned).         *)
(*  moveCopy[m] the offsets move m copied to its target home.              *)
(*  dfence    the move whose durable fence the placement pin holds, or     *)
(*            NoMove.                                                      *)
(*  lapsed    the durable fence's lease has lapsed.                        *)
(*  afence    the source activation's in-memory fence.                     *)
(*  crashes, coordCrashes  crashes so far.                                 *)
(***************************************************************************)
VARIABLES next, inflight, durable, tail, acked, cons, move, moveCopy,
          dfence, lapsed, afence, crashes, coordCrashes

vars == <<next, inflight, durable, tail, acked, cons, move, moveCopy,
          dfence, lapsed, afence, crashes, coordCrashes>>

TypeOK ==
   /\ next \in 0..MaxOff
   /\ inflight \subseteq Offs
   /\ durable \subseteq Offs
   /\ tail \in 0..MaxOff
   /\ acked \subseteq Offs
   /\ cons \in -1..(MaxOff - 1)
   /\ move \in [MoveIds -> {"idle", "fenced", "copied", "done"}]
   /\ moveCopy \in [MoveIds -> SUBSET Offs]
   /\ dfence \in MoveIds \cup {NoMove}
   /\ lapsed \in BOOLEAN
   /\ afence \in BOOLEAN
   /\ crashes \in 0..MaxCrashes
   /\ coordCrashes \in 0..MaxCoordCrashes

Watermark == IF inflight = {} THEN next ELSE Min(inflight)

Init ==
    /\ next = 0
    /\ inflight = {}
    /\ durable = {}
    /\ tail = 0
    /\ acked = {}
    /\ cons = -1
    /\ move = [m \in MoveIds |-> "idle"]
    /\ moveCopy = [m \in MoveIds |-> {}]
    /\ dfence = NoMove
    /\ lapsed = FALSE
    /\ afence = FALSE
    /\ crashes = 0
    /\ coordCrashes = 0

(***************************************************************************)
(* Append: the allocator hands the next offset to a new write, atomically  *)
(* with the activation's fence check (WalMoveFenceCore.IsAppendAdmitted    *)
(* under the shard's state gate, WalOffsetAllocationCore.Assign).          *)
(***************************************************************************)
Append ==
    /\ ~afence
    /\ next < MaxOff
    /\ inflight' = inflight \cup {next}
    /\ next' = next + 1
    /\ UNCHANGED <<durable, tail, acked, cons, move, moveCopy, dfence, lapsed, afence, crashes, coordCrashes>>

(***************************************************************************)
(* FlushAck(o): an in-flight append's flush lands on the stream's current  *)
(* home, in any order, and the write is acknowledged.                      *)
(***************************************************************************)
FlushAck(o) ==
    /\ o \in inflight
    /\ inflight' = inflight \ {o}
    /\ durable' = durable \cup {o}
    /\ acked' = acked \cup {o}
    /\ UNCHANGED <<next, tail, cons, move, moveCopy, dfence, lapsed, afence, crashes, coordCrashes>>

(***************************************************************************)
(* Consume: the consumer reads the next offset below the durable-contiguous *)
(* watermark (WalShippingWatermark); a read below the tail lands on it.    *)
(***************************************************************************)
ConsumeFrom == IF cons + 1 < tail THEN tail ELSE cons + 1

Consume ==
    /\ ConsumeFrom < Watermark
    /\ cons' = ConsumeFrom
    /\ UNCHANGED <<next, inflight, durable, tail, acked, move, moveCopy, dfence, lapsed, afence, crashes, coordCrashes>>

(***************************************************************************)
(* GcTrim: the GC trims the prefix the consumer has read.                  *)
(***************************************************************************)
GcTrim ==
    /\ \E t \in (tail + 1)..next :
         /\ t - 1 <= cons
         /\ tail' = t
         /\ durable' = {o \in durable : o >= t}
    /\ UNCHANGED <<next, inflight, acked, cons, move, moveCopy, dfence, lapsed, afence, crashes, coordCrashes>>

(***************************************************************************)
(* MoveFence(m): an operator starts move m. Its coordinator raises the    *)
(* durable fence (a compare-and-swap on the placement pin, under a fresh   *)
(* lease) and then quiesces the source, which fences its activation.       *)
(* MoveCopy: once the stream has quiesced - no append in flight, the stale *)
(* observation WalMoveFenceCore aborts on - and the coordinator has        *)
(* renewed its durable fence, the copy is taken.                           *)
(* MoveSwitch: the flip. Its compare-and-swap requires the durable fence   *)
(* still held by this move and clears it atomically with the placement     *)
(* change; the source is deactivated, and the next activation serves the   *)
(* target, unfenced.                                                       *)
(* MoveAbort: the coordinator abandons the move (a refused flip, a stale   *)
(* quiesce, a cancellation), releases its durable fence if it still holds  *)
(* it, and deactivates the source, which resumes unfenced.                 *)
(***************************************************************************)
MoveFence(m) ==
    /\ move[m] = "idle"
    \* WalMoveFenceCore.EvaluateRaise: no fence is held, or another move's
    \* lapsed fence is taken over; another move's live fence refuses the raise.
    /\ \/ dfence = NoMove
       \/ dfence # m /\ lapsed
    /\ move' = [move EXCEPT ![m] = "fenced"]
    /\ dfence' = m
    /\ lapsed' = FALSE
    /\ afence' = TRUE
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, moveCopy, crashes, coordCrashes>>

MoveCopy(m) ==
    /\ move[m] = "fenced"
    /\ afence
    /\ inflight = {}
    /\ dfence = m
    /\ moveCopy' = [moveCopy EXCEPT ![m] = durable]
    /\ move' = [move EXCEPT ![m] = "copied"]
    /\ lapsed' = FALSE
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, dfence, afence, crashes, coordCrashes>>

MoveSwitch(m) ==
    /\ move[m] = "copied"
    /\ dfence = m
    /\ durable' = {o \in moveCopy[m] : o >= tail}
    /\ move' = [move EXCEPT ![m] = "done"]
    /\ moveCopy' = [moveCopy EXCEPT ![m] = {}]
    /\ dfence' = NoMove
    /\ lapsed' = FALSE
    /\ afence' = FALSE
    /\ UNCHANGED <<next, inflight, tail, acked, cons, crashes, coordCrashes>>

\* The aborting coordinator releases only its own fence
\* (WalMoveFenceCore.IsReleaseAdmitted with its move id), then deactivates the
\* source; the next activation re-derives its fence from what the pin holds.
MoveAbort(m) ==
    /\ move[m] \in {"fenced", "copied"}
    /\ move' = [move EXCEPT ![m] = "done"]
    /\ moveCopy' = [moveCopy EXCEPT ![m] = {}]
    /\ dfence' = IF dfence = m THEN NoMove ELSE dfence
    /\ lapsed' = IF dfence = m THEN FALSE ELSE lapsed
    /\ afence' = (dfence' # NoMove)
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, crashes, coordCrashes>>

(***************************************************************************)
(* LeaseLapse: time passes and the durable fence's lease lapses. Abstract  *)
(* and unconditional: safety must hold however the clocks fall, so the     *)
(* lease may lapse even while the coordinator is renewing it.              *)
(* Release: a source activation that finds the durable fence lapsed        *)
(* releases it in the registry (a fenced activation deactivates at its     *)
(* lease's expiry, and the next one releases) and serves unfenced.         *)
(***************************************************************************)
LeaseLapse ==
    /\ dfence # NoMove
    /\ ~lapsed
    /\ lapsed' = TRUE
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, move, moveCopy, dfence, afence, crashes, coordCrashes>>

Release ==
    /\ dfence # NoMove
    /\ lapsed
    /\ dfence' = NoMove
    /\ lapsed' = FALSE
    /\ afence' = FALSE
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, move, moveCopy, crashes, coordCrashes>>

(***************************************************************************)
(* ShardCrash: the shard's activation is lost at any step. Unflushed       *)
(* appends are lost (none was acknowledged) and the allocator recovers one *)
(* past the highest stored offset, never below the tail. The in-memory     *)
(* fence is lost with the activation; the next activation re-derives it    *)
(* from the durable fence - fenced while the lease holds, and releasing it *)
(* once the lease has lapsed. The coordinator is a different grain, so the *)
(* move survives the crash.                                                *)
(* CoordinatorCrash(m): move m's coordinator is lost; the move is          *)
(* abandoned and its durable fence is left in place for its lease to       *)
(* govern.                                                                 *)
(***************************************************************************)
ShardCrash ==
    /\ crashes < MaxCrashes
    /\ inflight' = {}
    /\ next' = Max({tail} \cup {o + 1 : o \in durable})
    /\ afence' = (dfence # NoMove /\ ~lapsed)
    /\ dfence' = IF lapsed THEN NoMove ELSE dfence
    /\ lapsed' = FALSE
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<durable, tail, acked, cons, move, moveCopy, coordCrashes>>

CoordinatorCrash(m) ==
    /\ coordCrashes < MaxCoordCrashes
    /\ move[m] \in {"fenced", "copied"}
    /\ move' = [move EXCEPT ![m] = "done"]
    /\ moveCopy' = [moveCopy EXCEPT ![m] = {}]
    /\ coordCrashes' = coordCrashes + 1
    /\ UNCHANGED <<next, inflight, durable, tail, acked, cons, dfence, lapsed, afence, crashes>>
(***************************************************************************)
(* A fully quiesced stream has an explicit stuttering successor so natural *)
(* termination is not reported as a deadlock.                              *)
(***************************************************************************)
Quiesced ==
    /\ next = MaxOff
    /\ inflight = {}
    /\ \A m \in MoveIds : move[m] \in {"idle", "done"}
    /\ dfence = NoMove
    /\ tail = MaxOff

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ Append
    \/ \E o \in Offs : FlushAck(o)
    \/ Consume
    \/ GcTrim
    \/ \E m \in MoveIds : MoveFence(m)
    \/ \E m \in MoveIds : MoveCopy(m)
    \/ \E m \in MoveIds : MoveSwitch(m)
    \/ \E m \in MoveIds : MoveAbort(m)
    \/ LeaseLapse
    \/ Release
    \/ ShardCrash
    \/ \E m \in MoveIds : CoordinatorCrash(m)
    \/ Stutter

(***************************************************************************)
(* Fairness: appends, flushes, the consumer, the GC, an in-progress move   *)
(* (copy, flip or abort), the lease and its release make progress.         *)
(* Starting a move and crashing the shard or the coordinator are           *)
(* environment events, bounded and deliberately not fair.                  *)
(***************************************************************************)
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(Append)
    /\ \A o \in Offs : WF_vars(FlushAck(o))
    /\ WF_vars(Consume)
    /\ WF_vars(GcTrim)
    /\ \A m \in MoveIds : WF_vars(MoveCopy(m))
    /\ \A m \in MoveIds : WF_vars(MoveSwitch(m))
    /\ \A m \in MoveIds : WF_vars(MoveAbort(m))
    /\ WF_vars(LeaseLapse)
    /\ WF_vars(Release)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Every acknowledged write survives every move: it is still on the stream's
\* current home, or the consumer has read it and the GC has trimmed it.
MovedStreamKeepsAckedWrites ==
    \A o \in acked : o \in durable \/ (o < tail /\ o <= cons)

\* A move copies only a quiesced stream: once copied, and for as long as the
\* move still holds its durable fence, nothing is in flight, so no append can
\* land on the old home between the copy and a flip that is allowed to happen.
\* (Once the lease lapses and a source activation releases the fence, appends
\* resume and the flip is refused instead.)
CopyTakenQuiesced ==
    \A m \in MoveIds : move[m] = "copied" /\ dfence = m => inflight = {}

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

\* A durable fence is never held for ever: the flip, an abort, or the lapse of
\* its lease and a release always follows, including after the coordinator is
\* lost.
FenceEventuallyReleased ==
    [](dfence # NoMove => <>(dfence = NoMove))

=============================================================================