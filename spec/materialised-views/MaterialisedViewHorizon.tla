---------------------- MODULE MaterialisedViewHorizon ----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS MaxCrashes, MaxRebuilds, MaxBackstops, ScanLimit, HistoryBudget
Partitions == {0,1,2}
Keys == {0,2}
TerminalPartitions == {0,1}
HistoryEntries == 1..4
LastOffset(p) == IF p = 0 \/ p = 2 THEN 2 ELSE 1
IsPrepare(p,offset) == p \in Keys /\ offset = 1
IsTerminal(p,offset) == p \in TerminalPartitions /\ offset = LastOffset(p)
HistoryPartition(i) == CASE i = 1 -> 0 [] i = 2 -> 0 [] i = 3 -> 1 [] i = 4 -> 2
HistoryOffset(i) == CASE i = 1 -> 1 [] i = 2 -> 2 [] i = 3 -> 1 [] i = 4 -> 1
HistoryIsPrepare(i) == i \in {1,4}
HistoryIsTerminal(i) == i \in {2,3}
Zero == [p \in Partitions |-> 0]
Values == [p \in Partitions |-> IF p = 0 THEN 20 ELSE IF p = 2 THEN 21 ELSE 0]
Max(a,b) == IF a >= b THEN a ELSE b

\* Two keys on p0/p2; distinct shard terminals on p0/p1. A consumed terminal
\* on p1 does not prove that the key prepare on p2 has been read.
\* Length is an exclusive head: p0 prepare/commit, p1 commit, p2 prepare.
\* Production uses zero-based offsets and head-1; this model translates both
\* together to consumed-prefix lengths. HLCs 20/21 are NOT offsets 1/2.
\* One retained prepare with a positive batch size witnesses cardinality; it
\* need not recover every prepare payload. The source decision is separate from
\* shard-terminal arrival and gates whether the rebuild can see prepared rows.
VARIABLES length, installed, value, read, ckpt, prepares, terminals, staged, abandoned,
          cardinalityKnown, historyRetained, sourceDecisionCommitted,
          historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
          horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
          crashes, rebuilds, backstops, requested, recovery
vars == <<length, installed, value, read, ckpt, prepares, terminals, staged, abandoned,
          cardinalityKnown, historyRetained, sourceDecisionCommitted,
          historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
          horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
          crashes, rebuilds, backstops, requested, recovery>>
RawSource == [p \in Partitions |-> IF (p = 0 /\ 0 \in installed) \/
                    (p = 2 /\ 1 \in installed) THEN Values[p] ELSE 0]
Source == IF sourceDecisionCommitted /\ installed = TerminalPartitions
          THEN Values ELSE Zero
AllCommitted(h) == \A p \in TerminalPartitions : h[p] = LastOffset(p)
AppendedTerminals == {p \in TerminalPartitions : length[p] = LastOffset(p)}
Missing == prepares # Keys
AtHorizon == \A p \in Partitions : read[p] >= horizon[p]
KnownPrepares == prepares \cup historyPrepares
KnownTerminals == terminals \cup historyTerminals
PrepareCardinalityKnown == cardinalityKnown
CompleteTerminalSet == KnownTerminals = TerminalPartitions
HistoryEntitled == cardinalityKnown /\ CompleteTerminalSet
HistoryItemBelowCheckpoint(i) ==
    i \in historyRetained /\ HistoryOffset(i) <= ckpt[HistoryPartition(i)]

TypeOK ==
    /\ length \in [Partitions -> 0..2] /\ installed \subseteq TerminalPartitions
    /\ read \in [Partitions -> 0..2]
    /\ ckpt \in [Partitions -> 0..2] /\ horizon \in [Partitions -> 0..2]
    /\ heads \in [Partitions -> 0..2] /\ floor \in [Partitions -> 0..2]
    /\ value \in [Partitions -> {0,20,21}] /\ shadow \in [Partitions -> {0,20,21}]
    /\ prepares \subseteq Keys /\ terminals \subseteq TerminalPartitions
    /\ cardinalityKnown \in BOOLEAN /\ historyRetained \subseteq HistoryEntries
    /\ sourceDecisionCommitted \in BOOLEAN
    /\ historyPrepares \subseteq Keys /\ historyTerminals \subseteq TerminalPartitions
    /\ historyCursor \in 0..Cardinality(HistoryEntries)
    /\ historyBudgetUsed \in 0..HistoryBudget
    /\ staged \in BOOLEAN /\ abandoned \in BOOLEAN /\ planned \in BOOLEAN
    /\ requested \in BOOLEAN /\ recovery \in BOOLEAN
    /\ wal = "physical-source" /\ horizonWal \in {wal,"none"}
    /\ phase \in {"idle","captured","planned","scanned"}
    /\ crashes \in 0..MaxCrashes /\ rebuilds \in 0..MaxRebuilds
    /\ backstops \in 0..MaxBackstops

NormalInit ==
    /\ length = Zero /\ installed = {} /\ value = Zero /\ read = Zero /\ ckpt = Zero
    /\ prepares = {} /\ terminals = {} /\ staged = FALSE /\ abandoned = FALSE
    /\ cardinalityKnown = FALSE /\ historyRetained = HistoryEntries
    /\ sourceDecisionCommitted = FALSE
    /\ historyPrepares = {} /\ historyTerminals = {}
    /\ historyCursor = 0 /\ historyBudgetUsed = 0
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

\* A fresh physical tail can expose one terminal before this transaction's
\* own prepares become readable on their partitions. This is an initial
\* observed prefix, not a claim that the producer commits before preparing.
FreshTerminalOnly ==
    /\ length = [p \in Partitions |-> IF p = 1 THEN 1 ELSE 0]
    /\ installed = {1} /\ value = Zero
    /\ read = [p \in Partitions |-> IF p = 1 THEN 1 ELSE 0]
    /\ ckpt = Zero
    /\ prepares = {} /\ terminals = {1} /\ staged = TRUE /\ abandoned = FALSE
    /\ cardinalityKnown = FALSE /\ historyRetained = HistoryEntries
    /\ sourceDecisionCommitted = FALSE
    /\ historyPrepares = {} /\ historyTerminals = {}
    /\ historyCursor = 0 /\ historyBudgetUsed = 0
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

\* The p2 prepare row is trimmed, but a retained p0 prepare still witnesses the
\* declared batch size. A distinct p1 terminal is retained below its checkpoint;
\* recovery must also consume the p2 suffix through the captured horizon.
RetainedHistoryOrphan ==
    /\ length = [p \in Partitions |-> LastOffset(p)]
    /\ installed = TerminalPartitions /\ value = Zero
    /\ read = [p \in Partitions |-> IF p = 2 THEN 1 ELSE LastOffset(p)]
    /\ ckpt = [p \in Partitions |-> 1]
    /\ prepares = {} /\ terminals = {0} /\ staged = TRUE /\ abandoned = FALSE
    /\ cardinalityKnown = FALSE /\ historyRetained = {1,2,3}
    /\ sourceDecisionCommitted = TRUE
    /\ historyPrepares = {} /\ historyTerminals = {}
    /\ historyCursor = 0 /\ historyBudgetUsed = 0
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

AbandonedHistoryOrphan ==
    /\ length = [p \in Partitions |-> LastOffset(p)]
    /\ installed = TerminalPartitions /\ value = Zero
    /\ read = [p \in Partitions |-> IF p = 2 THEN 1 ELSE LastOffset(p)]
    /\ ckpt = [p \in Partitions |-> 1]
    /\ prepares = {} /\ terminals = {0} /\ staged = FALSE /\ abandoned = TRUE
    /\ cardinalityKnown = FALSE /\ historyRetained = {}
    /\ sourceDecisionCommitted = TRUE
    /\ historyPrepares = {} /\ historyTerminals = {}
    /\ historyCursor = 0 /\ historyBudgetUsed = 0
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

RetainedHistoryWithoutCardinality ==
    /\ length = [p \in Partitions |-> LastOffset(p)]
    /\ installed = TerminalPartitions /\ value = Zero
    /\ read = [p \in Partitions |-> IF p = 2 THEN 1 ELSE LastOffset(p)]
    /\ ckpt = [p \in Partitions |-> 1]
    /\ prepares = {} /\ terminals = {0} /\ staged = TRUE /\ abandoned = FALSE
    /\ cardinalityKnown = FALSE /\ historyRetained = {2,3}
    /\ sourceDecisionCommitted = TRUE
    /\ historyPrepares = {} /\ historyTerminals = {}
    /\ historyCursor = 0 /\ historyBudgetUsed = 0
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

Init == NormalInit \/ FreshTerminalOnly \/ RetainedHistoryOrphan
        \/ RetainedHistoryWithoutCardinality \/ AbandonedHistoryOrphan

AppendPrepare(p) ==
    /\ p \in Keys /\ length[p] = 0
    /\ length' = [length EXCEPT ![p] = 1]
    /\ UNCHANGED <<installed, value, read, ckpt, prepares, terminals, staged, abandoned,
                   cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

\* All prepares have landed before ANY terminal; terminals remain separate
\* actions on distinct partitions, and one terminal cannot decide the batch.
InstallSource(p) ==
    /\ p \in TerminalPartitions /\ \A q \in Keys : length[q] >= 1
    /\ p \notin installed
    /\ installed' = installed \cup {p}
    /\ UNCHANGED <<length, value, read, ckpt, prepares, terminals, staged, abandoned,
                   cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

ConfirmSourceDecision ==
    /\ ~sourceDecisionCommitted
    /\ \A q \in Keys : length[q] >= 1
    /\ installed = TerminalPartitions
    /\ sourceDecisionCommitted' = TRUE
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained,
                   historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

AppendCommit(p) ==
    /\ p \in TerminalPartitions /\ \A q \in Keys : length[q] >= 1
    /\ length[p] = LastOffset(p)-1 /\ p \in installed
    /\ (Cardinality(AppendedTerminals) < Cardinality(TerminalPartitions)-1
        \/ sourceDecisionCommitted)
    /\ length' = [length EXCEPT ![p] = LastOffset(p)]
    /\ UNCHANGED <<installed, value, read, ckpt, prepares, terminals, staged, abandoned,
                   cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Consume(p) ==
    /\ phase = "idle" /\ ~requested /\ read[p] < length[p]
    /\ LET offset == read[p]+1
       IN /\ read' = [read EXCEPT ![p] = offset]
          /\ prepares' = IF IsPrepare(p,offset) /\ ~abandoned THEN prepares \cup {p} ELSE prepares
          /\ terminals' = IF IsTerminal(p,offset) THEN terminals \cup {p} ELSE terminals
          /\ cardinalityKnown' = (cardinalityKnown \/ (IsPrepare(p,offset) /\ ~abandoned))
          /\ historyPrepares' = IF IsPrepare(p,offset) /\ abandoned
                                  THEN historyPrepares \cup {p} ELSE historyPrepares
          /\ staged' = (staged \/ ~abandoned)
    /\ UNCHANGED <<length, installed, value, ckpt, abandoned, historyRetained,
                sourceDecisionCommitted, horizon, horizonWal, wal, phase,
                historyTerminals, historyCursor, historyBudgetUsed,
                heads, floor, shadow, planned, crashes, rebuilds, backstops,
                requested, recovery>>

\* One examined retained entry consumes the bounded per-drain search budget.
\* The cursor survives another pass but is volatile across activation loss.
\* Every history record here belongs to the one selected physical WAL (wal);
\* retention gaps and physical-log rebinding are separate model scopes.
HistorySearch ==
    /\ phase = "idle" /\ ~requested /\ staged
    /\ KnownTerminals # {} /\ ~HistoryEntitled
    /\ historyCursor < Cardinality(HistoryEntries)
    /\ LET i == historyCursor+1
           p == HistoryPartition(i)
           below == HistoryItemBelowCheckpoint(i)
       IN /\ (~below \/ historyBudgetUsed < HistoryBudget)
          /\ historyCursor' = i
          /\ cardinalityKnown' = (cardinalityKnown \/ (below /\ HistoryIsPrepare(i)))
          /\ historyPrepares' = IF below /\ HistoryIsPrepare(i)
                                   THEN historyPrepares \cup {p} ELSE historyPrepares
          /\ historyTerminals' = IF below /\ HistoryIsTerminal(i)
                                    THEN historyTerminals \cup {p} ELSE historyTerminals
          /\ historyBudgetUsed' = IF below THEN historyBudgetUsed+1 ELSE historyBudgetUsed
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, historyRetained, sourceDecisionCommitted,
                   horizon, horizonWal, wal, phase, heads, floor, shadow,
                   planned, crashes, rebuilds, backstops, requested, recovery>>

ResetHistoryBudget ==
    /\ phase = "idle" /\ ~requested /\ staged
    /\ KnownTerminals # {} /\ ~HistoryEntitled
    /\ historyBudgetUsed = HistoryBudget
    /\ historyCursor < Cardinality(HistoryEntries)
    /\ HistoryItemBelowCheckpoint(historyCursor+1)
    /\ historyBudgetUsed' = 0
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

\* Capture is permitted only once retained same-log evidence has recovered
\* both prepare cardinality and every distinct terminal shard.
CaptureHorizon ==
    /\ phase = "idle" /\ ~requested /\ staged /\ Missing
    /\ KnownTerminals # {} /\ Missing
    /\ PrepareCardinalityKnown /\ CompleteTerminalSet
    /\ horizonWal = "none"
    /\ horizon' = length /\ horizonWal' = wal
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   historyBudgetUsed, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Flush ==
    /\ phase = "idle" /\ ~requested /\ staged
    /\ prepares = Keys /\ KnownTerminals = TerminalPartitions /\ sourceDecisionCommitted
    /\ value' = Values /\ ckpt' = read
    /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ cardinalityKnown' = FALSE
    /\ historyPrepares' = {} /\ historyTerminals' = {}
    /\ historyCursor' = 0 /\ historyBudgetUsed' = 0
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ UNCHANGED <<length, installed, read, abandoned, historyRetained,
                   sourceDecisionCommitted, wal, phase, heads, floor, shadow,
                   planned, crashes, rebuilds, backstops, requested, recovery>>

\* Regular orphan recovery requires cardinality, every distinct terminal and
\* consumption through the captured horizon. Abandoned batches instead request
\* a rebuild for each observed commit terminal; the source-decision visibility
\* gate, not retained-history entitlement, keeps that rebuild atomic.
Recover ==
    /\ phase = "idle" /\ ~requested /\ KnownTerminals # {}
    /\ staged /\ Missing /\ HistoryEntitled /\ sourceDecisionCommitted
    /\ horizonWal = wal /\ AtHorizon
    /\ abandoned' = TRUE /\ staged' = FALSE
    /\ prepares' = {} /\ terminals' = {} /\ requested' = TRUE /\ recovery' = TRUE
    /\ cardinalityKnown' = FALSE
    /\ historyPrepares' = {} /\ historyTerminals' = {}
    /\ historyCursor' = 0 /\ historyBudgetUsed' = 0
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ UNCHANGED <<length, installed, value, read, ckpt, historyRetained,
                   sourceDecisionCommitted, wal, phase, heads, floor, shadow,
                   planned, crashes, rebuilds, backstops>>

RecoverAbandoned ==
    /\ phase = "idle" /\ ~requested /\ abandoned /\ KnownTerminals # {}
    /\ requested' = TRUE /\ recovery' = FALSE
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor, historyBudgetUsed,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops>>

Backstop ==
    /\ phase = "idle" /\ ~requested /\ staged /\ backstops < MaxBackstops
    /\ abandoned' = TRUE /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ cardinalityKnown' = FALSE
    /\ historyPrepares' = {} /\ historyTerminals' = {}
    /\ historyCursor' = 0 /\ historyBudgetUsed' = 0
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ backstops' = backstops+1 /\ requested' = TRUE
    /\ UNCHANGED <<length, installed, value, read, ckpt, historyRetained,
                   sourceDecisionCommitted, wal, phase,
                   heads, floor, shadow, planned, crashes, rebuilds, recovery>>

RequestRebuild ==
    /\ phase = "idle" /\ ~requested /\ rebuilds < MaxRebuilds
    /\ requested' = TRUE /\ rebuilds' = rebuilds+1
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   historyBudgetUsed, horizon, horizonWal, wal, phase, heads, floor,
                   shadow, planned, crashes, backstops, recovery>>

Capture ==
    /\ phase = "idle" /\ requested
    /\ heads' = length /\ floor' = length /\ planned' = FALSE
    /\ phase' = "captured"
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   historyBudgetUsed, horizon, horizonWal, wal, shadow, crashes,
                   rebuilds, backstops, requested, recovery>>

\* The bounded preparation-window scan may miss a prepare on one partition.
\* Lower that partition's floor only where an undecided, unabandoned prepare
\* was actually discovered; if missed, post-terminal horizon recovery is needed.
Plan ==
    /\ phase = "captured"
    /\ floor' = [p \in Partitions |-> IF heads[p] > 0 /\ p < ScanLimit /\
                      ~AllCommitted(heads) /\ ~abandoned THEN 0 ELSE heads[p]]
    /\ planned' = TRUE /\ phase' = "planned"
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   historyBudgetUsed, horizon, horizonWal, wal, heads, shadow, crashes,
                   rebuilds, backstops, requested, recovery>>

Scan ==
    /\ phase = "planned"
    /\ shadow' = Source /\ phase' = "scanned"
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, cardinalityKnown, historyRetained, sourceDecisionCommitted,
                   historyPrepares, historyTerminals, historyCursor,
                   historyBudgetUsed, horizon, horizonWal, wal, heads, floor, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Swap ==
    /\ phase = "scanned"
    /\ value' = shadow /\ ckpt' = floor /\ read' = floor
    /\ abandoned' = (abandoned /\ ~AllCommitted(heads))
    /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ cardinalityKnown' = FALSE
    /\ historyPrepares' = {} /\ historyTerminals' = {}
    /\ historyCursor' = 0 /\ historyBudgetUsed' = 0
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ phase' = "idle" /\ requested' = FALSE /\ recovery' = FALSE
    /\ UNCHANGED <<length, installed, wal, historyRetained, sourceDecisionCommitted,
                   heads, floor, shadow, planned, crashes,
                   rebuilds, backstops>>

\* Durable abandoned identity survives; lost staging and horizon are re-created
\* from held-back WAL. A crashed rebuild retries without charging a new request.
Crash ==
    /\ crashes < MaxCrashes
    /\ read' = ckpt /\ prepares' = {} /\ terminals' = {} /\ staged' = FALSE
    /\ cardinalityKnown' = FALSE
    /\ historyPrepares' = {} /\ historyTerminals' = {}
    /\ historyCursor' = 0 /\ historyBudgetUsed' = 0
    /\ horizon' = Zero /\ horizonWal' = "none" /\ phase' = "idle"
    /\ requested' = (requested \/ phase # "idle")
    /\ crashes' = crashes+1
    /\ UNCHANGED <<length, installed, value, ckpt, abandoned, historyRetained,
                   sourceDecisionCommitted, wal, heads, floor, shadow,
                   planned, rebuilds, backstops, recovery>>

Stutter == UNCHANGED vars

Next ==
    \/ \E p \in Partitions : AppendPrepare(p)
    \/ \E p \in Partitions : InstallSource(p)
    \/ ConfirmSourceDecision
    \/ \E p \in Partitions : AppendCommit(p)
    \/ \E p \in Partitions : Consume(p)
    \/ HistorySearch
    \/ ResetHistoryBudget
    \/ CaptureHorizon
    \/ Flush
    \/ Recover
    \/ RecoverAbandoned
    \/ Backstop
    \/ RequestRebuild
    \/ Capture
    \/ Plan
    \/ Scan
    \/ Swap
    \/ Crash
    \/ Stutter

Spec == Init /\ [][Next]_vars
        /\ (\A p \in Partitions : WF_vars(AppendPrepare(p)) /\ WF_vars(AppendCommit(p)) /\ WF_vars(Consume(p)))
        /\ (\A p \in Partitions : WF_vars(InstallSource(p)))
        /\ WF_vars(ConfirmSourceDecision)
        /\ WF_vars(HistorySearch) /\ WF_vars(ResetHistoryBudget)
        /\ WF_vars(CaptureHorizon) /\ WF_vars(Flush) /\ WF_vars(Recover)
        /\ WF_vars(RecoverAbandoned)
        /\ WF_vars(Capture) /\ WF_vars(Plan) /\ WF_vars(Scan) /\ WF_vars(Swap)

NoPrematureFlush == [][(phase = "idle" /\ value' = Values /\ value # Values /\
                        phase' = "idle") => KnownTerminals = TerminalPartitions /\
                        prepares = Keys /\ sourceDecisionCommitted]_vars
PublishedBatchAtomic == value = Zero \/ value = Values
SourceDecisionPrerequisite == [][~sourceDecisionCommitted /\ sourceDecisionCommitted' =>
    (\A q \in Keys : length[q] >= 1) /\ installed = TerminalPartitions]_vars
NoPrematureRetirement == [][abandoned /\ ~abandoned' => AllCommitted(heads)]_vars
RecoveryEntitled == [][~requested /\ requested' /\ recovery' =>
                        HistoryEntitled /\ horizonWal = wal /\ AtHorizon]_vars
HorizonRequiresHistoryEntitlement == [][horizonWal = "none" /\ horizonWal' # "none" =>
                        HistoryEntitled]_vars
HistorySearchProgress == [][HistorySearch => historyCursor' > historyCursor]_vars
HorizonAfterTerminal == horizonWal # "none" =>
    /\ (KnownTerminals # {} \/ requested \/ phase # "idle")
    /\ HistoryEntitled
    /\ \A p \in Keys : horizon[p] >= 1
    /\ \A p \in Partitions : horizon[p] <= length[p]
RebuildReplayEntitled == [][phase = "captured" /\ phase' = "planned" =>
    \A p \in Partitions : (heads[p] > 0 /\ p < ScanLimit /\
        ~AllCommitted(heads) /\ ~abandoned) => floor'[p] = 0]_vars
CheckpointHeld == staged => \A p \in prepares \cup terminals : ckpt[p] < read[p]
QuiescentExact == AllCommitted(length) /\ phase = "idle" /\ ~requested /\
                  ~staged /\ ~abandoned /\ read = length => value = Values
TerminallyQuiescent == value = Values /\ ckpt = length /\ ~staged /\ ~abandoned /\
                       ~requested /\ phase = "idle"
NoCardinalityRetentionGap ==
    historyRetained = {2,3} /\ staged /\ ~cardinalityKnown /\ ~abandoned
Converges == NoCardinalityRetentionGap \/ <>[]TerminallyQuiescent
=============================================================================
