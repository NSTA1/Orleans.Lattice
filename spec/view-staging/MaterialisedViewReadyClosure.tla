-------------------- MODULE MaterialisedViewReadyClosure --------------------
EXTENDS Naturals, FiniteSets
CONSTANTS BatchCount, ConsumerCount, MaxCrashes, Mode
Keys == 1..BatchCount
Tx == Keys
Consumers == 1..ConsumerCount
NextKey(t) == IF t = BatchCount THEN 1 ELSE t+1
Previous(k) == IF k = 1 THEN BatchCount ELSE k-1
TxKeys(t) == {t,NextKey(t)}
Stamp(t,k) == IF t = k THEN 30 ELSE 10
Winner == [k \in Keys |-> 30]
Zero == [k \in Keys |-> 0]
Highest(g,k) ==
    IF {t \in g : k \in TxKeys(t)} = {} THEN 0
    ELSE CHOOSE h \in {Stamp(t,k) : t \in {u \in g : k \in TxKeys(u)}} :
        \A t \in {u \in g : k \in TxKeys(u)} : h >= Stamp(t,k)
Fold(g) == [k \in Keys |-> Highest(g,k)]
LogTx(p,i) == IF i \in {1,3} THEN p ELSE Previous(p)
Head == 4
Revisions == UNION {{<<t,k>> : k \in TxKeys(t)} : t \in Tx}

\* Fixed reachable source prefix: every ring batch has committed and installed.
\* Every key's two prepares precede its two distinct shard terminals. The high
\* stamp is on a different key in each batch: no scalar batch order can compose
\* the winner cut. Decision/install is not inferred from local terminal arrival.
VARIABLES accumulative, deferred, read, ckpt, prepares, terminals, done, value, history, horizon,
          snapshots, historySnapshots, crashes
vars == <<accumulative,deferred,read,ckpt,prepares,terminals,done,value,history,horizon,
          snapshots,historySnapshots,crashes>>
Positions == [Consumers -> [Keys -> 0..Head]]
Ready(c,t) == prepares[c][t] = TxKeys(t) /\ terminals[c][t] = TxKeys(t)
Pending(c) == {t \in Tx : t \notin done[c] /\
                              (prepares[c][t] # {} \/ terminals[c][t] # {})}
Complete(c) == {t \in Tx : t \notin done[c] /\ Ready(c,t) /\ ~(deferred /\ t = 2)}
\* Each omitted low-stamp key depends on the next batch's winning revision.
\* Closing that dependency transitively around the ring requires every batch.
Closed(c,g) == \A t \in g : NextKey(t) \in g \cup done[c]
AtHorizon(c) == horizon[c] = Head /\ \A p \in Keys : read[c][p] = Head
Floor(c,p) ==
    IF \E i \in 1..read[c][p] : LogTx(p,i) \notin done[c]
    THEN CHOOSE n \in 0..Head :
        n+1 \in {i \in 1..read[c][p] : LogTx(p,i) \notin done[c]} /\
        \A j \in {i \in 1..read[c][p] : LogTx(p,i) \notin done[c]} : n+1 <= j
    ELSE read[c][p]

TypeOK ==
    /\ accumulative \in BOOLEAN
    /\ deferred \in BOOLEAN
    /\ read \in Positions /\ ckpt \in Positions
    /\ prepares \in [Consumers -> [Tx -> SUBSET Keys]]
    /\ terminals \in [Consumers -> [Tx -> SUBSET Keys]]
    /\ done \in [Consumers -> SUBSET Tx]
    /\ value \in [Consumers -> [Keys -> {0,10,30}]]
    /\ snapshots \in [Consumers -> [Keys -> {0,10,30}]]
    /\ history \in [Consumers -> SUBSET Revisions]
    /\ historySnapshots \in [Consumers -> SUBSET Revisions]
    /\ horizon \in [Consumers -> {0,Head}]
    /\ crashes \in 0..MaxCrashes
Init ==
    /\ accumulative \in (IF Mode = "Both" THEN BOOLEAN ELSE {Mode = "History"})
    /\ deferred = ~accumulative
    /\ read = [c \in Consumers |-> [p \in Keys |-> 0]] /\ ckpt = read
    /\ prepares = [c \in Consumers |-> [t \in Tx |-> {}]] /\ terminals = prepares
    /\ done = [c \in Consumers |-> {}] /\ value = [c \in Consumers |-> Zero]
    /\ snapshots = value /\ history = [c \in Consumers |-> {}]
    /\ historySnapshots = history /\ horizon = [c \in Consumers |-> 0] /\ crashes = 0

Read(c,p) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ read[c][p] < Head
    /\ LET i == read[c][p]+1
           t == LogTx(p,i)
       IN /\ read' = [read EXCEPT ![c][p] = i]
          /\ prepares' = IF i <= 2 /\ t \notin done[c]
                         THEN [prepares EXCEPT ![c][t] = @ \cup {p}] ELSE prepares
          /\ terminals' = IF i > 2 /\ t \notin done[c]
                          THEN [terminals EXCEPT ![c][t] = @ \cup {p}] ELSE terminals
    /\ UNCHANGED <<ckpt,done,value,history,horizon,snapshots,historySnapshots,crashes>>

Capture(c) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ ~accumulative /\ Complete(c) # {} /\ horizon[c] = 0
    /\ horizon' = [horizon EXCEPT ![c] = Head]
    /\ UNCHANGED <<read,ckpt,prepares,terminals,done,value,history,
                   snapshots,historySnapshots,crashes>>

Publish(c) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ ~accumulative /\ Complete(c) # {} /\ AtHorizon(c)
    /\ Closed(c,Complete(c))
    /\ value' = [value EXCEPT ![c] = Fold(Complete(c) \cup done[c])]
    /\ done' = [done EXCEPT ![c] = @ \cup Complete(c)]
    /\ UNCHANGED <<read,ckpt,prepares,terminals,history,horizon,
                   snapshots,historySnapshots,crashes>>

\* History's projection key is revision identity, not current-state source key.
HistoryPublish(c,t) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ accumulative /\ t \in Complete(c)
    /\ history' = [history EXCEPT ![c] = @ \cup {<<t,k>> : k \in TxKeys(t)}]
    /\ done' = [done EXCEPT ![c] = @ \cup {t}]
    /\ UNCHANGED <<read,ckpt,prepares,terminals,value,horizon,
                   snapshots,historySnapshots,crashes>>

Checkpoint(c) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ ckpt[c] # [p \in Keys |-> Floor(c,p)]
    /\ ckpt' = [ckpt EXCEPT ![c] = [p \in Keys |-> Floor(c,p)]]
    /\ UNCHANGED <<read,prepares,terminals,done,value,history,horizon,
                   snapshots,historySnapshots,crashes>>

Crash(c) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ crashes < MaxCrashes
    /\ read' = [read EXCEPT ![c] = ckpt[c]]
    /\ prepares' = [prepares EXCEPT ![c] = [t \in Tx |-> {}]]
    /\ terminals' = [terminals EXCEPT ![c] = [t \in Tx |-> {}]]
    /\ horizon' = [horizon EXCEPT ![c] = 0] /\ crashes' = crashes+1
    /\ UNCHANGED <<ckpt,done,value,history,snapshots,historySnapshots>>

Observe(c) ==
    /\ UNCHANGED <<accumulative,deferred>>
    /\ snapshots' = [snapshots EXCEPT ![c] = value[c]]
    /\ historySnapshots' = [historySnapshots EXCEPT ![c] = history[c]]
    /\ UNCHANGED <<read,ckpt,prepares,terminals,done,value,history,horizon,crashes>>

\* Batch 2's external admission hold can outlast consumption of every WAL head.
Release ==
    /\ deferred /\ \E c \in Consumers : AtHorizon(c)
    /\ deferred' = FALSE
    /\ UNCHANGED <<accumulative,read,ckpt,prepares,terminals,done,value,history,horizon,
                   snapshots,historySnapshots,crashes>>

Stutter == UNCHANGED vars

Next ==
    \/ \E c \in Consumers, p \in Keys : Read(c,p)
    \/ \E c \in Consumers : Capture(c)
    \/ \E c \in Consumers : Publish(c)
    \/ \E c \in Consumers, t \in Tx : HistoryPublish(c,t)
    \/ \E c \in Consumers : Checkpoint(c)
    \/ \E c \in Consumers : Crash(c)
    \/ \E c \in Consumers : Observe(c)
    \/ Release
    \/ Stutter

Spec == Init /\ [][Next]_vars
    /\ WF_vars(Release)
    /\ (\A c \in Consumers, p \in Keys : WF_vars(Read(c,p)))
    /\ (\A c \in Consumers : WF_vars(Capture(c)) /\ WF_vars(Publish(c)) /\
                              WF_vars(Checkpoint(c)))
    /\ (\A c \in Consumers, t \in Tx : WF_vars(HistoryPublish(c,t)))
CheckpointHeld == \A c \in Consumers, p \in Keys : ckpt[c][p] <= Floor(c,p)
SnapshotExact == \A c \in Consumers : value[c] \in {Zero,Winner} /\
                                            snapshots[c] \in {Zero,Winner}
HistoryExact == \A c \in Consumers : history[c] =
    (IF accumulative THEN UNION {{<<t,k>> : k \in TxKeys(t)} : t \in done[c]} ELSE {})
HistoryReadAtomic == \A c \in Consumers, t \in Tx :
    {<<t,k>> : k \in TxKeys(t)} \subseteq historySnapshots[c] \/
    {<<t,k>> : k \in TxKeys(t)} \cap historySnapshots[c] = {}
PublicationEntitled == [][~accumulative =>
    \A c \in Consumers : done'[c] # done[c] =>
        AtHorizon(c) /\ Closed(c,Complete(c)) /\ done'[c] = done[c] \cup Complete(c)]_vars
Converges == <>[](\A c \in Consumers :
    done[c] = Tx /\ ckpt[c] = [p \in Keys |-> Head] /\
    (IF accumulative THEN history[c] = Revisions ELSE value[c] = Winner))
=============================================================================
