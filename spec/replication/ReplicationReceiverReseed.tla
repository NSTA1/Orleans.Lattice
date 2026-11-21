------------------------ MODULE ReplicationReceiverReseed ------------------------
(***************************************************************************)
(* A focused companion to Replication.tla: the forced gap a shipper marks  *)
(* when its peer's lineage of the tree moves, and how a bootstrap the     *)
(* receiver started on its own settles it (#5794).                        *)
(*                                                                         *)
(* One sender log (one partition: its positions are 1..head), one shipper  *)
(* and one receiver. The receiver may bootstrap from one of the sender's   *)
(* exports without being asked - it fell off the log, or the tree is new - *)
(* which moves its lineage. The shipper sees the move on the next          *)
(* acknowledgement and cannot tell what the new contents hold, so it takes *)
(* the peer off the log and marks a re-seed: the gap settles only when the *)
(* receiver echoes a bootstrap epoch above the marker, and the shipper     *)
(* then rewinds to the retained point it recorded.                         *)
(*                                                                         *)
(* Before the fix the marker was the current export epoch, so the echo of *)
(* the bootstrap that caused the move (an epoch at or below it) could not *)
(* settle it, and the receiver was re-seeded a second time for nothing.   *)
(* Exports now record the head they opened at (a cut). The marker is one  *)
(* below the earliest export from which every export up to the current    *)
(* one recorded a cut at or above the log's retained floor: such an       *)
(* export's contents plus a rewind to that floor leave nothing out. An    *)
(* export with no cut on this log - taken before the purge hold, on       *)
(* another physical log, or whose cut write failed - is not trusted, and *)
(* breaks the run, so the conservative marker stands.                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS MaxWrites, MaxExports

Positions == 1..MaxWrites
Epochs == 1..MaxExports
NoCut == -1
NoMarker == -1

VARIABLES
    head,       \* writes 1..head exist on the sender's log
    trim,       \* writes 1..trim have been purged from the log
    epoch,      \* exports taken so far; the current export epoch
    cut,        \* cut[e]: the head export e recorded it opened at, or NoCut
    snap,       \* snap[e]: export e contains writes 1..snap[e]
    trusted,    \* trusted[e]: export e opened under this log's purge hold
    recv,       \* the writes the receiver holds
    cursor,     \* writes 1..cursor are acknowledged by the receiver
    moved,      \* the receiver's lineage moved and the shipper has not seen it
    moves,      \* receiver-initiated bootstraps so far
    echo,       \* the receiver's completed bootstrap epoch, as its acks echo it
    marker,     \* the shipper's re-seed marker, or NoMarker while on the log
    retainFrom, \* the point the rewind restarts from when the gap settles
    redundant   \* a re-seed ran although the echoed export covered the gap

vars == <<head, trim, epoch, cut, snap, trusted, recv, cursor, moved, moves,
          echo, marker, retainFrom, redundant>>

Upto(n) == 1..n

(***************************************************************************)
(* Covers(k): export k opened on this log at or above the retained floor, *)
(* and so did every export after it up to the current one. The shipper's *)
(* marker is one below the earliest such k, else the current epoch.       *)
(***************************************************************************)
Covers(k) == \A j \in k..epoch : cut[j] # NoCut /\ trim <= cut[j]

CoveringMarker ==
    IF \E k \in 1..epoch : Covers(k)
    THEN (CHOOSE k \in 1..epoch : Covers(k) /\ \A j \in 1..(k - 1) : ~Covers(j)) - 1
    ELSE epoch

(***************************************************************************)
(* The reference for NoRedundantReseed, stated over the ghost trusted     *)
(* rather than the recorded cuts: the export the receiver holds, and      *)
(* every later one, opened on this log, and it holds every write the log  *)
(* no longer retains.                                                      *)
(***************************************************************************)
EchoCovers ==
    /\ echo \in Epochs
    /\ \A j \in echo..epoch : trusted[j]
    /\ trim <= snap[echo]

Floor == IF marker = NoMarker THEN cursor ELSE retainFrom

TypeOK ==
    /\ head \in 0..MaxWrites
    /\ trim \in 0..head
    /\ epoch \in 0..MaxExports
    /\ cut \in [Epochs -> {NoCut} \cup (0..MaxWrites)]
    /\ snap \in [Epochs -> 0..MaxWrites]
    /\ trusted \in [Epochs -> BOOLEAN]
    /\ recv \subseteq Positions
    /\ cursor \in 0..head
    /\ moved \in BOOLEAN
    /\ moves \in 0..1
    /\ echo \in 0..MaxExports
    /\ marker \in {NoMarker} \cup (0..MaxExports)
    /\ retainFrom \in 0..MaxWrites
    /\ redundant \in BOOLEAN

Init ==
    /\ head = 0
    /\ trim = 0
    /\ epoch = 0
    /\ cut = [e \in Epochs |-> NoCut]
    /\ snap = [e \in Epochs |-> 0]
    /\ trusted = [e \in Epochs |-> FALSE]
    /\ recv = {}
    /\ cursor = 0
    /\ moved = FALSE
    /\ moves = 0
    /\ echo = 0
    /\ marker = NoMarker
    /\ retainFrom = 0
    /\ redundant = FALSE

(* Write: the sender commits the next write to its log. *)
Write ==
    /\ head < MaxWrites
    /\ head' = head + 1
    /\ UNCHANGED <<trim, epoch, cut, snap, trusted, recv, cursor, moved, moves,
                   echo, marker, retainFrom, redundant>>

(***************************************************************************)
(* Purge: the log trims its oldest write, never past what the receiver    *)
(* acknowledged, nor past the rewind point while a re-seed is marked.     *)
(***************************************************************************)
Purge ==
    /\ trim < Floor
    /\ trim' = trim + 1
    /\ UNCHANGED <<head, epoch, cut, snap, trusted, recv, cursor, moved, moves,
                   echo, marker, retainFrom, redundant>>

(***************************************************************************)
(* Export: the sender takes an export of the tree. A trusted one opens    *)
(* under the purge hold, records the head it opened at, and holds every   *)
(* write below it. An untrusted one records no cut on this log and may    *)
(* hold any prefix. One export number is kept back for a re-seed.         *)
(***************************************************************************)
Export ==
    /\ marker = NoMarker
    /\ epoch < MaxExports - 1
    /\ \E t \in BOOLEAN, s \in 0..head :
          /\ t => s = head
          /\ epoch' = epoch + 1
          /\ trusted' = [trusted EXCEPT ![epoch + 1] = t]
          /\ cut' = [cut EXCEPT ![epoch + 1] = IF t THEN head ELSE NoCut]
          /\ snap' = [snap EXCEPT ![epoch + 1] = s]
    /\ UNCHANGED <<head, trim, recv, cursor, moved, moves, echo, marker,
                   retainFrom, redundant>>

(* Ship: the shipper delivers the next retained write and it is acknowledged. *)
Ship ==
    /\ ~moved
    /\ marker = NoMarker
    /\ cursor < head
    /\ cursor >= trim
    /\ recv' = recv \cup {cursor + 1}
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<head, trim, epoch, cut, snap, trusted, moved, moves, echo,
                   marker, retainFrom, redundant>>

(***************************************************************************)
(* Bootstrap(e): the receiver bootstraps from export e on its own         *)
(* initiative. Its contents become the export's and its lineage moves.    *)
(***************************************************************************)
Bootstrap(e) ==
    /\ moves = 0
    /\ ~moved
    /\ marker = NoMarker
    /\ e \in 1..epoch
    /\ recv' = Upto(snap[e])
    /\ echo' = e
    /\ moved' = TRUE
    /\ moves' = 1
    /\ UNCHANGED <<head, trim, epoch, cut, snap, trusted, cursor, marker,
                   retainFrom, redundant>>

(***************************************************************************)
(* Notice: the acknowledgement reports the new lineage and the shipper     *)
(* takes the peer off the log (RemarkReseedRequiredAsync). The marker is  *)
(* the covering one; a covering export rewinds from the retained floor.    *)
(***************************************************************************)
Notice ==
    /\ moved
    /\ marker = NoMarker
    /\ LET m == CoveringMarker
       IN /\ marker' = m
          /\ retainFrom' = IF m < epoch THEN trim ELSE cursor
    /\ moved' = FALSE
    /\ UNCHANGED <<head, trim, epoch, cut, snap, trusted, recv, cursor, moves,
                   echo, redundant>>

(***************************************************************************)
(* Settle: the receiver's echo exceeds the marker, so the gap settles and *)
(* the shipper rewinds to the retained point and resumes the log.         *)
(***************************************************************************)
Settle ==
    /\ marker # NoMarker
    /\ echo > marker
    /\ cursor' = retainFrom
    /\ marker' = NoMarker
    /\ UNCHANGED <<head, trim, epoch, cut, snap, trusted, recv, moved, moves,
                   echo, retainFrom, redundant>>

(***************************************************************************)
(* Reseed: the echo does not settle the gap, so the receiver is re-seeded *)
(* from a fresh export, taken under the hold after the marker.            *)
(***************************************************************************)
Reseed ==
    /\ marker # NoMarker
    /\ echo <= marker
    /\ epoch < MaxExports
    /\ epoch' = epoch + 1
    /\ trusted' = [trusted EXCEPT ![epoch + 1] = TRUE]
    /\ cut' = [cut EXCEPT ![epoch + 1] = head]
    /\ snap' = [snap EXCEPT ![epoch + 1] = head]
    /\ recv' = Upto(head)
    /\ echo' = epoch + 1
    /\ redundant' = (redundant \/ EchoCovers)
    /\ UNCHANGED <<head, trim, cursor, moved, moves, marker, retainFrom>>

Quiesced ==
    /\ ~moved
    /\ marker = NoMarker
    /\ cursor = head

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ Write
    \/ Purge
    \/ Export
    \/ Ship
    \/ \E e \in Epochs : Bootstrap(e)
    \/ Notice
    \/ Settle
    \/ Reseed
    \/ Stutter

(***************************************************************************)
(* Fairness: the shipper keeps shipping, sees the move, and the gap is    *)
(* settled or re-seeded. Writes, purges, exports and the receiver's own   *)
(* bootstrap are environment events.                                       *)
(***************************************************************************)
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Ship) /\ WF_vars(Notice) /\ WF_vars(Settle) /\ WF_vars(Reseed)

(* NoLostWrite: while the link is on the log, the receiver holds every    *)
(* write it acknowledged.                                                  *)
NoLostWrite ==
    (~moved /\ marker = NoMarker) => Upto(cursor) \subseteq recv

(* NoRedundantReseed: no re-seed runs when the bootstrap the receiver     *)
(* already ran covers the gap.                                             *)
NoRedundantReseed == ~redundant

(* Converges: the link eventually returns to the log and catches up.      *)
Converges == <>[](~moved /\ marker = NoMarker /\ cursor = head)

=============================================================================
