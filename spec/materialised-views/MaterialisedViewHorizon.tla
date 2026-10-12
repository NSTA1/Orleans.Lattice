---------------------- MODULE MaterialisedViewHorizon ----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS MaxCrashes, MaxRebuilds, MaxBackstops, ScanLimit
Partitions == {0,1,2}
Keys == {0,2}
TerminalPartitions == {0,1}
LastOffset(p) == IF p = 0 THEN 2 ELSE 1
IsPrepare(p,offset) == p \in Keys /\ offset = 1
IsTerminal(p,offset) == p \in TerminalPartitions /\ offset = LastOffset(p)
Zero == [p \in Partitions |-> 0]
Values == [p \in Partitions |-> IF p = 0 THEN 20 ELSE IF p = 2 THEN 21 ELSE 0]
Max(a,b) == IF a >= b THEN a ELSE b

\* Two keys on p0/p2; distinct shard terminals on p0/p1. A consumed terminal
\* on p1 does not prove that the key prepare on p2 has been read.
\* Length is an exclusive head: p0 prepare/commit, p1 commit, p2 prepare.
\* Production uses zero-based offsets and head-1; this model translates both
\* together to consumed-prefix lengths. HLCs 20/21 are NOT offsets 1/2.
VARIABLES length, installed, value, read, ckpt, prepares, terminals, staged, abandoned,
          horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
          crashes, rebuilds, backstops, requested, recovery
vars == <<length, installed, value, read, ckpt, prepares, terminals, staged, abandoned,
          horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
          crashes, rebuilds, backstops, requested, recovery>>
Source == [p \in Partitions |-> IF (p = 0 /\ 0 \in installed) \/
                 (p = 2 /\ 1 \in installed) THEN Values[p] ELSE 0]
AllCommitted(h) == \A p \in TerminalPartitions : h[p] = LastOffset(p)
Missing == prepares # Keys
AtHorizon == \A p \in Partitions : read[p] >= horizon[p]

TypeOK ==
    /\ length \in [Partitions -> 0..2] /\ installed \subseteq TerminalPartitions
    /\ read \in [Partitions -> 0..2]
    /\ ckpt \in [Partitions -> 0..2] /\ horizon \in [Partitions -> 0..2]
    /\ heads \in [Partitions -> 0..2] /\ floor \in [Partitions -> 0..2]
    /\ value \in [Partitions -> {0,20,21}] /\ shadow \in [Partitions -> {0,20,21}]
    /\ prepares \subseteq Keys /\ terminals \subseteq TerminalPartitions
    /\ staged \in BOOLEAN /\ abandoned \in BOOLEAN /\ planned \in BOOLEAN
    /\ requested \in BOOLEAN /\ recovery \in BOOLEAN
    /\ wal = "physical-source" /\ horizonWal \in {wal,"none"}
    /\ phase \in {"idle","captured","planned","scanned"}
    /\ crashes \in 0..MaxCrashes /\ rebuilds \in 0..MaxRebuilds
    /\ backstops \in 0..MaxBackstops

Init ==
    /\ length = Zero /\ installed = {} /\ value = Zero /\ read = Zero /\ ckpt = Zero
    /\ prepares = {} /\ terminals = {} /\ staged = FALSE /\ abandoned = FALSE
    /\ horizon = Zero /\ horizonWal = "none" /\ wal = "physical-source"
    /\ phase = "idle" /\ heads = Zero /\ floor = Zero /\ shadow = Zero
    /\ planned = FALSE /\ crashes = 0 /\ rebuilds = 0 /\ backstops = 0
    /\ requested = FALSE /\ recovery = FALSE

AppendPrepare(p) ==
    /\ p \in Keys /\ length[p] = 0
    /\ length' = [length EXCEPT ![p] = 1]
    /\ UNCHANGED <<installed, value, read, ckpt, prepares, terminals, staged, abandoned,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

\* All prepares have landed before ANY terminal; terminals remain separate
\* actions on distinct partitions, and one terminal cannot decide the batch.
InstallSource(p) ==
    /\ p \in TerminalPartitions /\ \A q \in Keys : length[q] >= 1
    /\ p \notin installed
    /\ installed' = installed \cup {p}
    /\ UNCHANGED <<length, value, read, ckpt, prepares, terminals, staged, abandoned,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

AppendCommit(p) ==
    /\ p \in TerminalPartitions /\ \A q \in Keys : length[q] >= 1
    /\ length[p] = LastOffset(p)-1 /\ p \in installed
    /\ length' = [length EXCEPT ![p] = LastOffset(p)]
    /\ UNCHANGED <<installed, value, read, ckpt, prepares, terminals, staged, abandoned,
                   horizon, horizonWal, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Consume(p) ==
    /\ phase = "idle" /\ ~requested /\ read[p] < length[p]
    /\ LET offset == read[p]+1
       IN /\ read' = [read EXCEPT ![p] = offset]
          /\ prepares' = IF IsPrepare(p,offset) /\ ~abandoned THEN prepares \cup {p} ELSE prepares
          /\ terminals' = IF IsTerminal(p,offset) THEN terminals \cup {p} ELSE terminals
          /\ staged' = (staged \/ ~abandoned)
    /\ UNCHANGED <<length, installed, value, ckpt, abandoned, horizon, horizonWal, wal, phase,
                   heads, floor, shadow, planned, crashes, rebuilds, backstops,
                   requested, recovery>>

\* Captured only AFTER a terminal is observed. Later ordinary appends are
\* irrelevant to prepare absence: source ordered all prepares before terminals.
CaptureHorizon ==
    /\ phase = "idle" /\ ~requested /\ staged /\ terminals # {} /\ Missing
    /\ horizonWal = "none"
    /\ horizon' = length /\ horizonWal' = wal
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, wal, phase, heads, floor, shadow, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Flush ==
    /\ phase = "idle" /\ ~requested /\ staged
    /\ prepares = Keys /\ terminals = TerminalPartitions
    /\ value' = Values /\ ckpt' = read
    /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ UNCHANGED <<length, installed, read, abandoned, wal, phase, heads, floor, shadow,
                   planned, crashes, rebuilds, backstops, requested, recovery>>

\* Either an orphan has consumed EVERY partition below a post-terminal head,
\* or an abandoned batch has supplied a later terminal. Recovery rebuilds
\* current source state; retirement still requires ALL shard terminals.
Recover ==
    /\ phase = "idle" /\ ~requested /\ terminals # {}
    /\ (abandoned \/ (staged /\ Missing /\ horizonWal = wal /\ AtHorizon))
    /\ abandoned' = TRUE /\ staged' = FALSE
    /\ prepares' = {} /\ terminals' = {} /\ requested' = TRUE /\ recovery' = TRUE
    /\ UNCHANGED <<length, installed, value, read, ckpt, horizon, horizonWal, wal, phase,
                   heads, floor, shadow, planned, crashes, rebuilds, backstops>>

Backstop ==
    /\ phase = "idle" /\ ~requested /\ staged /\ backstops < MaxBackstops
    /\ abandoned' = TRUE /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ backstops' = backstops+1 /\ requested' = TRUE
    /\ UNCHANGED <<length, installed, value, read, ckpt, horizon, horizonWal, wal, phase,
                   heads, floor, shadow, planned, crashes, rebuilds, recovery>>

RequestRebuild ==
    /\ phase = "idle" /\ ~requested /\ rebuilds < MaxRebuilds
    /\ requested' = TRUE /\ rebuilds' = rebuilds+1
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, horizon, horizonWal, wal, phase, heads, floor,
                   shadow, planned, crashes, backstops, recovery>>

Capture ==
    /\ phase = "idle" /\ requested
    /\ heads' = length /\ floor' = length /\ planned' = FALSE
    /\ phase' = "captured"
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, horizon, horizonWal, wal, shadow, crashes,
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
                   abandoned, horizon, horizonWal, wal, heads, shadow, crashes,
                   rebuilds, backstops, requested, recovery>>

Scan ==
    /\ phase = "planned"
    /\ shadow' = Source /\ phase' = "scanned"
    /\ UNCHANGED <<length, installed, value, read, ckpt, prepares, terminals, staged,
                   abandoned, horizon, horizonWal, wal, heads, floor, planned,
                   crashes, rebuilds, backstops, requested, recovery>>

Swap ==
    /\ phase = "scanned"
    /\ value' = shadow /\ ckpt' = floor /\ read' = floor
    /\ abandoned' = (abandoned /\ ~AllCommitted(heads))
    /\ staged' = FALSE /\ prepares' = {} /\ terminals' = {}
    /\ horizon' = Zero /\ horizonWal' = "none"
    /\ phase' = "idle" /\ requested' = FALSE /\ recovery' = FALSE
    /\ UNCHANGED <<length, installed, wal, heads, floor, shadow, planned, crashes,
                   rebuilds, backstops>>

\* Durable abandoned identity survives; lost staging and horizon are re-created
\* from held-back WAL. A crashed rebuild retries without charging a new request.
Crash ==
    /\ crashes < MaxCrashes
    /\ read' = ckpt /\ prepares' = {} /\ terminals' = {} /\ staged' = FALSE
    /\ horizon' = Zero /\ horizonWal' = "none" /\ phase' = "idle"
    /\ requested' = (requested \/ phase # "idle")
    /\ crashes' = crashes+1
    /\ UNCHANGED <<length, installed, value, ckpt, abandoned, wal, heads, floor, shadow,
                   planned, rebuilds, backstops, recovery>>

Stutter == UNCHANGED vars

Next ==
    \/ \E p \in Partitions : AppendPrepare(p)
    \/ \E p \in Partitions : InstallSource(p)
    \/ \E p \in Partitions : AppendCommit(p)
    \/ \E p \in Partitions : Consume(p)
    \/ CaptureHorizon
    \/ Flush
    \/ Recover
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
        /\ WF_vars(CaptureHorizon) /\ WF_vars(Flush) /\ WF_vars(Recover)
        /\ WF_vars(Capture) /\ WF_vars(Plan) /\ WF_vars(Scan) /\ WF_vars(Swap)

NoPrematureFlush == [][(phase = "idle" /\ value' = Values /\ value # Values /\
                        phase' = "idle") => terminals = TerminalPartitions /\ prepares = Keys]_vars
NoPrematureRetirement == [][abandoned /\ ~abandoned' => AllCommitted(heads)]_vars
RecoveryEntitled == [][~requested /\ requested' /\ recovery' =>
                        abandoned \/ (terminals # {} /\ Missing /\ horizonWal = wal /\ AtHorizon)]_vars
HorizonAfterTerminal == horizonWal # "none" =>
    /\ (terminals # {} \/ requested \/ phase # "idle")
    /\ \A p \in Keys : horizon[p] >= 1
    /\ \A p \in Partitions : horizon[p] <= length[p]
RebuildReplayEntitled == [][phase = "captured" /\ phase' = "planned" =>
    \A p \in Partitions : (heads[p] > 0 /\ p < ScanLimit /\
        ~AllCommitted(heads) /\ ~abandoned) => floor'[p] = 0]_vars
CheckpointHeld == staged => \A p \in prepares \cup terminals : ckpt[p] < read[p]
QuiescentExact == AllCommitted(length) /\ phase = "idle" /\ ~requested /\
                  ~staged /\ ~abandoned /\ read = length => value = Values
Converges == <>[](value = Values /\ ckpt = length /\ ~staged /\ ~abandoned /\ ~requested /\ phase = "idle")
=============================================================================
