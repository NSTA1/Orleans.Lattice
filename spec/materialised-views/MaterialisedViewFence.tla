----------------------- MODULE MaterialisedViewFence -----------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS MaxCrashes, Window, OrdinaryHlc

\* Fixed adversarial prefix: TWO two-key batches, followed by lower-HLC
\* ordinary writes at HIGHER offsets. Offset order is not source LWW order.
Keys == {"a", "b"}
Tx == {"t1", "t2"}
Log == <<[tx |-> "t1", k |-> "a", op |-> "prepare", h |-> 5],
         [tx |-> "t1", k |-> "b", op |-> "prepare", h |-> 6],
         [tx |-> "t1", k |-> "a", op |-> "commit", h |-> 7],
         [tx |-> "t2", k |-> "a", op |-> "prepare", h |-> 20],
         [tx |-> "t2", k |-> "b", op |-> "prepare", h |-> 21],
         [tx |-> "t2", k |-> "a", op |-> "commit", h |-> 22],
         [tx |-> "none", k |-> "a", op |-> "ordinary", h |-> OrdinaryHlc],
         [tx |-> "none", k |-> "b", op |-> "ordinary", h |-> OrdinaryHlc]>>
WalHead == Len(Log)
MinHead(n) == IF n < WalHead THEN n ELSE WalHead
Zero == [k \in Keys |-> 0]
Max(a,b) == IF a >= b THEN a ELSE b
Expected == [k \in Keys |-> Max(IF k = "a" THEN 20 ELSE 21, OrdinaryHlc)]
Sup == [k \in Keys |-> Max(IF k = "a" THEN 20 ELSE 21, OrdinaryHlc)]

VARIABLES established, wal, fenceWal, value, ckpt, pos, livePos, fence,
          armed, fenceStaged, prepared, terminal, staged, resolved,
          supersession, phase, peer, crashes, rebound
vars == <<established, wal, fenceWal, value, ckpt, pos, livePos, fence,
          armed, fenceStaged, prepared, terminal, staged, resolved,
          supersession, phase, peer, crashes, rebound>>

TypeOK ==
    /\ established \in BOOLEAN /\ rebound \in BOOLEAN /\ peer \in BOOLEAN
    /\ wal \in {"P1","P2"} /\ fenceWal \in {"P1","P2","none"}
    /\ value \in [Keys -> 0..Max(OrdinaryHlc,21)]
    /\ ckpt \in 0..WalHead /\ pos \in 0..WalHead /\ livePos \in 0..WalHead
    /\ fence \in 0..WalHead /\ armed \in 0..WalHead
    /\ fenceStaged \subseteq Tx /\ staged \subseteq Tx /\ resolved \subseteq Tx
    /\ prepared \in [Tx -> SUBSET Keys] /\ terminal \subseteq Tx
    /\ supersession \in [Keys -> 0..Max(OrdinaryHlc,21)]
    /\ phase \in {"idle","reading","flushing","persisting"}
    /\ crashes \in 0..MaxCrashes

Init ==
    /\ established = FALSE /\ rebound = FALSE /\ peer = FALSE
    /\ wal = "P1" /\ fenceWal = "none" /\ value = Zero
    /\ ckpt = 0 /\ pos = 0 /\ livePos = 0 /\ fence = 0 /\ armed = 0
    /\ fenceStaged = {} /\ staged = {} /\ resolved = {}
    /\ prepared = [t \in Tx |-> {}] /\ terminal = {}
    /\ supersession = Zero /\ phase = "idle" /\ crashes = 0

\* The first activation reads the complete fixed prefix. t1 waits on its
\* cross-tree peer, t2 flushes atomically, ordinary writes are folded before
\* the flush. Persisted hold-back is below t1. Subsequent passes are bounded.
\* This macro is a stated prefix, not a proof of arbitrary initial staging.
Establish ==
    /\ ~established
    /\ established' = TRUE /\ value' = Expected
    /\ pos' = WalHead /\ livePos' = WalHead /\ ckpt' = 0
    /\ fence' = WalHead /\ fenceWal' = wal /\ fenceStaged' = {"t1"}
    /\ staged' = {"t1"} /\ prepared' = [t \in Tx |-> Keys]
    /\ terminal' = Tx /\ resolved' = {"t2"} /\ supersession' = Sup
    /\ UNCHANGED <<wal, armed, phase, peer, crashes, rebound>>

Crash ==
    /\ established /\ ~rebound /\ crashes < MaxCrashes
    /\ phase' = "idle" /\ pos' = ckpt /\ livePos' = 0 /\ armed' = 0
    /\ staged' = {} /\ prepared' = [t \in Tx |-> {}]
    /\ terminal' = {} /\ resolved' = {} /\ supersession' = Zero
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<established, wal, fenceWal, value, ckpt, fence,
                   fenceStaged, peer, rebound>>

PeerReady ==
    /\ established /\ ~peer
    /\ peer' = TRUE
    /\ UNCHANGED <<established, wal, fenceWal, value, ckpt, pos, livePos,
                   fence, armed, fenceStaged, prepared, terminal, staged,
                   resolved, supersession, phase, crashes, rebound>>

Begin ==
    /\ established /\ ~rebound /\ phase = "idle"
    /\ (pos < WalHead \/ staged # {})
    /\ pos' = IF staged = {} THEN ckpt ELSE livePos
    /\ armed' = IF staged = {} /\ fenceWal = wal THEN fence ELSE armed
    /\ supersession' = IF staged = {} /\ fence > 0 /\ fenceWal = wal
                       THEN Sup ELSE supersession
    /\ phase' = "reading"
    /\ UNCHANGED <<established, wal, fenceWal, value, ckpt, livePos, fence,
                   fenceStaged, prepared, terminal, staged, resolved,
                   peer, crashes, rebound>>

\* A single bounded subscription window. Resolved fenced transactions are
\* skipped, not re-staged; an ordinary fenced write never applies again.
Read ==
    /\ phase = "reading"
    /\ \E stop \in (IF pos = WalHead THEN {WalHead} ELSE (pos+1)..MinHead(pos+Window)) :
        /\ LET entries == (pos+1)..stop
               replayed == {i \in entries : Log[i].tx \in Tx /\
                            (i > armed \/ Log[i].tx \in fenceStaged) /\
                            Log[i].tx \notin resolved}
               skipped == {Log[i].tx : i \in {j \in entries :
                            Log[j].tx \in Tx /\ j <= armed /\
                            Log[j].tx \notin fenceStaged}}
           IN /\ prepared' = [t \in Tx |-> prepared[t] \cup
                         {Log[i].k : i \in {j \in replayed :
                          Log[j].tx = t /\ Log[j].op = "prepare"}}]
              /\ terminal' = terminal \cup {Log[i].tx : i \in
                         {j \in replayed : Log[j].op = "commit"}}
              /\ staged' = staged \cup {Log[i].tx : i \in replayed}
              /\ resolved' = resolved \cup skipped
              /\ value' = [k \in Keys |-> IF \E i \in entries :
                            Log[i].op = "ordinary" /\ Log[i].k = k /\ i > armed
                          THEN OrdinaryHlc ELSE value[k]]
        /\ pos' = stop
    /\ phase' = "flushing"
    /\ UNCHANGED <<established, wal, fenceWal, ckpt, livePos, fence, armed,
                   fenceStaged, supersession, peer, crashes, rebound>>

\* The persisted supersession remains available even if another batch's
\* release removed the live refcount. Source HLC, never WAL offset, wins.
Flush ==
    /\ phase = "flushing"
    /\ LET ready == {t \in staged : prepared[t] = Keys /\ t \in terminal /\
                                  (t = "t2" \/ peer)}
       IN /\ value' = [k \in Keys |-> IF "t1" \in ready /\
                            (IF k = "a" THEN 5 ELSE 6) >= supersession[k]
                          THEN (IF k = "a" THEN 5 ELSE 6) ELSE value[k]]
          /\ staged' = staged \ ready /\ resolved' = resolved \cup ready
    /\ phase' = "persisting"
    /\ UNCHANGED <<established, wal, fenceWal, ckpt, pos, livePos, fence,
                   armed, fenceStaged, prepared, terminal, supersession,
                   peer, crashes, rebound>>

Persist ==
    /\ phase = "persisting"
    /\ LET unpassed == armed > pos
               clear == staged = {} /\ ~unpassed
       IN /\ fence' = IF clear THEN 0 ELSE Max(fence,pos)
          /\ fenceWal' = IF clear THEN "none" ELSE wal
          /\ fenceStaged' = IF clear THEN {} ELSE staged
          /\ armed' = IF clear \/ ~unpassed THEN 0 ELSE armed
    /\ ckpt' = IF staged = {} THEN pos ELSE 0
    /\ livePos' = pos /\ phase' = "idle"
    /\ UNCHANGED <<established, wal, value, pos, prepared, terminal, staged,
                   resolved, supersession, peer, crashes, rebound>>

\* A physical identity change invalidates the old log's entire replay scope.
\* The new log and scan are empty; rebind itself is a tail-model obligation.
Rebind ==
    /\ established /\ ~rebound /\ phase = "idle"
    /\ wal' = "P2" /\ rebound' = TRUE /\ value' = Zero
    /\ ckpt' = 0 /\ pos' = 0 /\ livePos' = 0 /\ fence' = 0
    /\ armed' = 0 /\ fenceWal' = "none" /\ fenceStaged' = {}
    /\ staged' = {} /\ prepared' = [t \in Tx |-> {}] /\ terminal' = {}
    /\ resolved' = {} /\ supersession' = Zero
    /\ UNCHANGED <<established, phase, peer, crashes>>

Stutter == UNCHANGED vars

Next ==
    \/ Establish
    \/ Crash
    \/ PeerReady
    \/ Begin
    \/ Read
    \/ Flush
    \/ Persist
    \/ Rebind
    \/ Stutter

Spec == Init /\ [][Next]_vars /\ WF_vars(Establish) /\ WF_vars(PeerReady)
        /\ WF_vars(Begin) /\ WF_vars(Read) /\ WF_vars(Flush) /\ WF_vars(Persist)

QuiescentExact == established /\ ~rebound /\ phase = "idle" /\ pos = WalHead /\
                  staged = {} => value = Expected
FenceCoversUnread == established /\ ~rebound /\ phase = "idle" /\
                    crashes > 0 /\ pos < WalHead => fence = WalHead
ScopeExact == rebound => fence = 0 /\ armed = 0 /\ fenceWal = "none" /\ supersession = Zero
NoFencedRegression == [][established /\ ~rebound /\ wal' = wal /\ armed > 0
                         => \A k \in Keys : value'[k] >= value[k]]_vars
Converges == <>[](rebound \/ (established /\ phase = "idle" /\ pos = WalHead /\
                             ckpt = WalHead /\ staged = {} /\ value = Expected /\ fence = 0))
=============================================================================
