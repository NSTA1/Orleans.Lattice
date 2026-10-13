---------------- MODULE MaterialisedViewRebind ----------------
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS MaxMoves, MaxCrashes
Physical == {1, 2}
Events == {1, 2}
VARIABLES stamps, alias, logs, source, txOrigin, outcome, moves,
          bound, generation, view, read, checkpoint, staged, stageOrigin,
          phase, candidate, floor, shadow, held, trim, crashes, rebuilt
vars == <<stamps, alias, logs, source, txOrigin, outcome, moves,
          bound, generation, view, read, checkpoint, staged, stageOrigin,
          phase, candidate, floor, shadow, held, trim, crashes, rebuilt>>
Stamp(e) == IF e = 0 THEN 0 ELSE stamps[e]
Winner(a, b) == IF Stamp(a) >= Stamp(b) THEN a ELSE b
Written(e) == \E p \in Physical : e \in {logs[p][i] : i \in 1..Len(logs[p])}
Idle == phase = "idle"
Required(p) == IF ~Idle /\ p = candidate THEN floor
               ELSE IF p = bound THEN checkpoint ELSE Len(logs[p])

Init ==
    /\ stamps \in [Events -> 1..2] /\ stamps[1] # stamps[2]
    /\ alias = 1 /\ logs = [p \in Physical |-> <<>>]
    /\ source = [p \in Physical |-> 0]
    /\ txOrigin = 0 /\ outcome = "pending" /\ moves = 0
    /\ bound = 1 /\ generation = 0 /\ view = 0
    /\ read = 0 /\ checkpoint = 0 /\ staged = FALSE /\ stageOrigin = 0
    /\ phase = "idle" /\ candidate = 1 /\ floor = 0 /\ shadow = 0
    /\ held = [p \in Physical |-> IF p = 1 THEN 0 ELSE -1]
    /\ trim = [p \in Physical |-> 0]
    /\ crashes = 0 /\ rebuilt = FALSE

Write ==
    /\ ~Written(1)
    /\ logs' = [logs EXCEPT ![alias] = Append(@, 1)]
    /\ source' = [source EXCEPT ![alias] = Winner(@, 1)]
    /\ UNCHANGED <<stamps, alias, txOrigin, outcome, moves, bound,
         generation, view, read, checkpoint, staged, stageOrigin, phase,
         candidate, floor, shadow, held, trim, crashes, rebuilt>>

Prepare ==
    /\ ~Written(2)
    /\ txOrigin' = alias
    /\ logs' = [logs EXCEPT ![alias] = Append(@, 2)]
    /\ UNCHANGED <<stamps, alias, source, outcome, moves, bound, generation,
         view, read, checkpoint, staged, stageOrigin, phase, candidate,
         floor, shadow, held, trim, crashes, rebuilt>>

Decide ==
    /\ txOrigin \in Physical /\ outcome = "pending"
    /\ outcome' \in {"commit", "abort"}
    /\ source' = IF outcome' = "commit"
                  THEN [source EXCEPT ![txOrigin] = Winner(@, 2)] ELSE source
    /\ UNCHANGED <<stamps, alias, logs, txOrigin, moves, bound, generation,
         view, read, checkpoint, staged, stageOrigin, phase, candidate,
         floor, shadow, held, trim, crashes, rebuilt>>

SourceMove ==
    /\ moves < MaxMoves
    \* TreeLifecycle guarantees no unresolved source transaction is copied.
    /\ txOrigin = 0 \/ outcome # "pending"
    /\ alias' = 3 - alias
    /\ source' = [source EXCEPT ![alias'] = source[alias]]
    /\ logs' = [logs EXCEPT ![alias'] = <<>>]
    /\ trim' = [trim EXCEPT ![alias'] = 0]
    /\ moves' = moves + 1
    /\ UNCHANGED <<stamps, txOrigin, outcome, bound, generation, view,
         read, checkpoint, staged, stageOrigin, phase, candidate, floor,
         shadow, held, crashes, rebuilt>>

ReadOrdinary ==
    /\ Idle /\ bound = alias /\ read < Len(logs[bound])
    /\ logs[bound][read + 1] = 1
    /\ view' = IF source[bound] = 1 THEN Winner(view, 1) ELSE view
    /\ read' = read + 1
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, checkpoint, staged, stageOrigin, phase,
         candidate, floor, shadow, held, trim, crashes, rebuilt>>

Stage ==
    /\ Idle /\ bound = alias /\ read < Len(logs[bound])
    /\ logs[bound][read + 1] = 2
    /\ staged' = TRUE /\ stageOrigin' = bound /\ read' = read + 1
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, checkpoint, phase, candidate, floor,
         shadow, held, trim, crashes, rebuilt>>

Flush ==
    /\ Idle /\ bound = alias /\ staged /\ outcome # "pending"
    /\ stageOrigin = bound
    /\ view' = IF outcome = "commit" /\ source[bound] = 2
               THEN Winner(view, 2) ELSE view
    /\ staged' = FALSE /\ stageOrigin' = 0
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, read, checkpoint, phase, candidate, floor,
         shadow, held, trim, crashes, rebuilt>>

Checkpoint ==
    /\ Idle /\ bound = alias /\ ~staged /\ checkpoint # read
    /\ checkpoint' = read /\ held' = [held EXCEPT ![bound] = read]
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, staged, stageOrigin, phase,
         candidate, floor, shadow, trim, crashes, rebuilt>>

BeginRebuild ==
    /\ Idle /\ (bound # alias \/ ~rebuilt)
    /\ candidate' = alias
    /\ floor' = IF candidate' = bound THEN checkpoint ELSE trim[candidate']
    /\ held' = [held EXCEPT ![candidate'] = floor']
    /\ phase' = "captured"
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         shadow, trim, crashes, rebuilt>>

Scan ==
    /\ phase = "captured"
    /\ shadow' = source[candidate] /\ phase' = "scanned"
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         candidate, floor, held, trim, crashes, rebuilt>>

Validate ==
    /\ phase = "scanned" /\ candidate = alias
    /\ phase' = "validated"
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         candidate, floor, shadow, held, trim, crashes, rebuilt>>

Install ==
    \* Alias movement while publication awaits persistence does not relabel
    \* the captured tuple. A subsequent rebind heals that late movement.
    /\ phase = "validated"
    /\ bound' = candidate /\ view' = shadow /\ generation' = generation + 1
    /\ read' = floor /\ checkpoint' = floor
    /\ staged' = FALSE /\ stageOrigin' = 0
    /\ held' = [held EXCEPT ![candidate] = floor]
    /\ phase' = "idle" /\ rebuilt' = TRUE
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         candidate, floor, shadow, trim, crashes>>

RejectStale ==
    /\ ~Idle /\ candidate # alias
    /\ phase' = "idle"
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         candidate, floor, shadow, held, trim, crashes, rebuilt>>

ReleaseRetired ==
    \E p \in Physical :
    /\ p # bound /\ (Idle \/ p # candidate) /\ held[p] # -1
    /\ held' = [held EXCEPT ![p] = -1]
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         phase, candidate, floor, shadow, trim, crashes, rebuilt>>

CursorTrim ==
    \E p \in Physical :
    /\ trim[p] < Len(logs[p])
    /\ ~(outcome = "pending" /\ logs[p][trim[p] + 1] = 2)
    /\ held[p] = -1 \/ trim[p] < held[p]
    /\ trim' = [trim EXCEPT ![p] = @ + 1]
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, read, checkpoint, staged, stageOrigin,
         phase, candidate, floor, shadow, held, crashes, rebuilt>>

Crash ==
    /\ crashes < MaxCrashes
    /\ read' = checkpoint /\ staged' = FALSE /\ stageOrigin' = 0
    /\ phase' = "idle" /\ crashes' = crashes + 1
    /\ held' = [held EXCEPT ![bound] = checkpoint]
    /\ UNCHANGED <<stamps, alias, logs, source, txOrigin, outcome, moves,
         bound, generation, view, checkpoint, candidate, floor, shadow,
         trim, rebuilt>>

Stutter == UNCHANGED vars

Next ==
    \/ Write
    \/ Prepare
    \/ Decide
    \/ SourceMove
    \/ ReadOrdinary
    \/ Stage
    \/ Flush
    \/ Checkpoint
    \/ BeginRebuild
    \/ Scan
    \/ Validate
    \/ Install
    \/ RejectStale
    \/ ReleaseRetired
    \/ CursorTrim
    \/ Crash
    \/ Stutter

Spec == Init /\ [][Next]_vars
     /\ WF_vars(ReadOrdinary) /\ WF_vars(Stage) /\ WF_vars(Flush)
     /\ WF_vars(Checkpoint) /\ WF_vars(BeginRebuild) /\ WF_vars(Scan)
     /\ WF_vars(Validate) /\ WF_vars(Install) /\ WF_vars(RejectStale) /\ WF_vars(ReleaseRetired)

TypeOK ==
    /\ bound \in Physical /\ alias \in Physical /\ candidate \in Physical
    /\ view \in 0..2 /\ staged \in BOOLEAN /\ stageOrigin \in 0..2
    /\ phase \in {"idle", "captured", "scanned", "validated"}
StagingNamesBoundLog == staged => stageOrigin = bound
PrepareNamesPhysicalLog == txOrigin \in Physical =>
    2 \in {logs[txOrigin][i] : i \in 1..Len(logs[txOrigin])}
ResumeNamesBoundLog == Idle /\ bound = alias => checkpoint <= Len(logs[bound])
BindingHeld == held[bound] # -1
CandidateHeld == ~Idle => held[candidate] # -1 /\ held[candidate] <= floor
CursorTrimEntitled == \A p \in Physical :
    (p = bound) \/ (~Idle /\ p = candidate) =>
       trim[p] <= Required(p)
NoAbortedPrepare == outcome # "commit" => view # 2
CutoverKeepsCommittedSnapshot ==
    [][moves' # moves => source'[alias'] = source[alias]]_vars
InstallUsesCapturedIdentity ==
    [][(generation' # generation) =>
       (bound' = candidate /\ view' = shadow
        /\ checkpoint' = floor /\ ~staged')]_vars
BindingConverges == <>[](bound = alias /\ view = source[alias])
RetiredPinsReleased == <>[](\A p \in Physical : p # bound => held[p] = -1)
==============================================================
