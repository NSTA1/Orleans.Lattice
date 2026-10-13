-------------------------- MODULE TreeLifecycle --------------------------
\* Control-plane composition of reshard grow/shrink, resize/undo and local
\* shadow cutover/revert. Data-plane ownership and atomic visibility are
\* checked by ShardOwnership*, not assumed to follow from this module.
EXTENDS Naturals, FiniteSets

Copies == {"source", "resized", "restored"}
Small == <<0, 2, 2>>
Large == <<0, 2, 3>>
Maps == {Small, Large}
Owners(m) == {m[i] : i \in 1..3}
MaxFaults == 1
InitialMap == Small

VARIABLES alias, maps, operation, phase, source, captured, copied, fenced,
          redirect, mirror, undoable, migration, target, migrationCopy,
          running, faults, deleted, resizeUsed, restoreUsed, reshardUsed

vars == <<alias, maps, operation, phase, source, captured, copied, fenced,
          redirect, mirror, undoable, migration, target, migrationCopy,
          running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

Init ==
    /\ alias = "source"
    /\ maps = [c \in Copies |-> InitialMap]
    /\ operation = "none"
    /\ phase = "idle"
    /\ source = "source"
    /\ captured = {}
    /\ copied = {}
    /\ fenced = {}
    /\ redirect = [c \in Copies |-> "none"]
    /\ mirror = FALSE
    /\ undoable = FALSE
    /\ migration = "idle"
    /\ target = Small
    /\ migrationCopy = "source"
    /\ running = TRUE
    /\ faults = 0
    /\ deleted = FALSE
    /\ resizeUsed = FALSE
    /\ restoreUsed = FALSE
    /\ reshardUsed = FALSE

ResizeBegin ==
    /\ running /\ ~deleted /\ ~resizeUsed /\ ~restoreUsed
    /\ operation = "none" /\ migration \in {"idle", "done"}
    /\ ~undoable
    /\ operation' = "resize" /\ phase' = "copy"
    /\ source' = alias
    /\ captured' = Owners(maps[alias])
    /\ copied' = {} /\ fenced' = {}
    /\ maps' = [maps EXCEPT !["resized"] = maps[alias]]
    /\ mirror' = TRUE /\ resizeUsed' = TRUE
    /\ UNCHANGED <<alias, redirect, undoable, migration, target,
                   migrationCopy, running, faults, deleted, restoreUsed, reshardUsed>>

CopyShard(s) ==
    /\ running /\ operation = "resize" /\ phase = "copy"
    /\ s \in captured \ copied
    /\ copied' = copied \cup {s}
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

CopyComplete ==
    /\ running /\ operation = "resize" /\ phase = "copy"
    /\ copied = captured
    /\ phase' = "fence"
    /\ UNCHANGED <<alias, maps, operation, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

FenceShard(s) ==
    /\ running /\ operation = "resize" /\ phase = "fence"
    /\ s \in captured \ fenced
    /\ fenced' = fenced \cup {s}
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

ResizeSwap ==
    /\ running /\ operation = "resize" /\ phase = "fence"
    /\ fenced = captured
    /\ alias' = "resized" /\ phase' = "complete"
    /\ undoable' = TRUE
    /\ UNCHANGED <<maps, operation, source, captured, copied, fenced,
                   redirect, mirror, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

\* Persisting completion and releasing its alias reservation are distinct:
\* a reminder must recover a completion interrupted between those writes.
ReleaseReservation ==
    /\ running /\ operation # "none" /\ phase = "complete"
    /\ operation' = "none" /\ phase' = "idle"
    /\ UNCHANGED <<alias, maps, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

UndoBegin ==
    /\ running /\ operation = "none" /\ undoable /\ ~deleted
    /\ operation' = "undo" /\ phase' = "redirect"
    /\ UNCHANGED <<alias, maps, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

UndoRedirect ==
    /\ running /\ operation = "undo" /\ phase = "redirect"
    /\ redirect' = [redirect EXCEPT !["resized"] = source]
    /\ phase' = "swap"
    /\ UNCHANGED <<alias, maps, operation, source, captured, copied, fenced,
                   mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

UndoSwap ==
    /\ running /\ operation = "undo" /\ phase = "swap"
    /\ alias' = source /\ phase' = "clear"
    /\ UNCHANGED <<maps, operation, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

UndoClear ==
    /\ running /\ operation = "undo" /\ phase = "clear"
    /\ fenced' = {} /\ mirror' = FALSE /\ undoable' = FALSE
    /\ phase' = "complete"
    /\ UNCHANGED <<alias, maps, operation, source, captured, copied,
                   redirect, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

ExpireUndo ==
    /\ running /\ operation = "none" /\ undoable
    /\ undoable' = FALSE /\ mirror' = FALSE
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

\* Grow and shrink both publish a whole map. The migration's internal
\* sweep/freeze/terminal protocol is the ShardOwnership and B+ tree seam.
ReshardBegin ==
    /\ running /\ ~deleted /\ ~reshardUsed
    /\ operation = "none" /\ ~mirror /\ ~undoable
    /\ migration = "idle"
    /\ migration' = "moving"
    /\ migrationCopy' = alias
    /\ target' = IF maps[alias] = Small THEN Large ELSE Small
    /\ reshardUsed' = TRUE
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, running, faults, deleted,
                   resizeUsed, restoreUsed>>

MigrationPublish ==
    /\ running /\ migration = "moving"
    /\ operation \notin {"resize", "undo"} /\ alias = migrationCopy
    /\ maps' = [maps EXCEPT ![migrationCopy] = target]
    /\ migration' = "published"
    /\ UNCHANGED <<alias, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

ReshardComplete ==
    /\ running /\ migration = "published"
    /\ alias = migrationCopy
    /\ maps[migrationCopy] = target
    /\ migration' = "done"
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

MigrationRetarget ==
    /\ running /\ migration \in {"moving", "published"}
    /\ alias # migrationCopy
    /\ migrationCopy' = alias /\ migration' = "moving"
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, target, running, faults, deleted,
                   resizeUsed, restoreUsed, reshardUsed>>

\* A cutover can overtake an active split/fold. Its physical-copy commit
\* fence refuses the obsolete diff; the reshard's next tick plans afresh.
RestoreBegin ==
    /\ running /\ ~deleted /\ ~restoreUsed
    /\ operation = "none" /\ ~undoable
    /\ operation' = "restore" /\ phase' = "swap"
    /\ source' = alias
    /\ maps' = [maps EXCEPT !["restored"] = maps[alias]]
    /\ restoreUsed' = TRUE
    /\ UNCHANGED <<alias, captured, copied, fenced, redirect, mirror, undoable,
                   migration, target, migrationCopy, running, faults, deleted,
                   resizeUsed, reshardUsed>>

RestoreSwap ==
    /\ running /\ operation = "restore" /\ phase = "swap"
    /\ alias' = "restored" /\ phase' = "redirect"
    /\ UNCHANGED <<maps, operation, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

RestoreRedirect ==
    /\ running /\ operation = "restore" /\ phase = "redirect"
    /\ redirect' = [redirect EXCEPT ![source] = "restored"]
    /\ phase' = "complete"
    /\ UNCHANGED <<alias, maps, operation, source, captured, copied, fenced,
                   mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

RevertBegin ==
    /\ running /\ ~deleted /\ operation = "none"
    /\ alias = "restored"
    /\ operation' = "revert" /\ phase' = "swap"
    /\ UNCHANGED <<alias, maps, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

RevertSwap ==
    /\ running /\ operation = "revert" /\ phase = "swap"
    /\ alias' = source /\ phase' = "redirect"
    /\ UNCHANGED <<maps, operation, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

RevertRedirect ==
    /\ running /\ operation = "revert" /\ phase = "redirect"
    /\ redirect' = [redirect EXCEPT ![source] = "none", !["restored"] = source]
    /\ phase' = "complete"
    /\ UNCHANGED <<alias, maps, operation, source, captured, copied, fenced,
                   mirror, undoable, migration, target, migrationCopy,
                   running, faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

Crash ==
    /\ running /\ faults < MaxFaults
    /\ operation # "none" \/ migration \in {"moving", "published"}
    /\ running' = FALSE /\ faults' = faults + 1
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   deleted, resizeUsed, restoreUsed, reshardUsed>>

Retry ==
    /\ ~running
    /\ running' = TRUE
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   faults, deleted, resizeUsed, restoreUsed, reshardUsed>>

Delete ==
    /\ running /\ ~deleted /\ operation = "none"
    /\ migration \in {"idle", "done"}
    /\ ~undoable
    /\ deleted' = TRUE
    /\ UNCHANGED <<alias, maps, operation, phase, source, captured, copied, fenced,
                   redirect, mirror, undoable, migration, target, migrationCopy,
                   running, faults, resizeUsed, restoreUsed, reshardUsed>>

Stutter == UNCHANGED vars

Next ==
    \/ ResizeBegin
    \/ \E s \in {0, 2, 3} : CopyShard(s)
    \/ CopyComplete
    \/ \E s \in {0, 2, 3} : FenceShard(s)
    \/ ResizeSwap
    \/ ReleaseReservation
    \/ UndoBegin
    \/ UndoRedirect
    \/ UndoSwap
    \/ UndoClear
    \/ ExpireUndo
    \/ ReshardBegin
    \/ MigrationPublish
    \/ ReshardComplete
    \/ MigrationRetarget
    \/ RestoreBegin
    \/ RestoreSwap
    \/ RestoreRedirect
    \/ RevertBegin
    \/ RevertSwap
    \/ RevertRedirect
    \/ Crash
    \/ Retry
    \/ Delete
    \/ Stutter

Progress ==
    \/ \E s \in {0, 2, 3} : CopyShard(s)
    \/ CopyComplete
    \/ \E s \in {0, 2, 3} : FenceShard(s)
    \/ ResizeSwap \/ ReleaseReservation
    \/ UndoRedirect \/ UndoSwap \/ UndoClear
    \/ MigrationPublish \/ ReshardComplete \/ MigrationRetarget
    \/ RestoreSwap \/ RestoreRedirect
    \/ RevertSwap \/ RevertRedirect \/ Retry

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
    /\ alias \in Copies /\ source \in Copies /\ migrationCopy \in Copies
    /\ maps \in [Copies -> Maps] /\ target \in Maps
    /\ operation \in {"none", "resize", "undo", "restore", "revert"}
    /\ phase \in {"idle", "copy", "fence", "swap", "redirect", "clear", "complete"}
    /\ captured \subseteq 0..3 /\ copied \subseteq 0..3
    /\ fenced \subseteq 0..3
    /\ redirect \in [Copies -> Copies \cup {"none"}]
    /\ migration \in {"idle", "moving", "published", "done"}
    /\ faults \in 0..MaxFaults
    /\ running \in BOOLEAN /\ deleted \in BOOLEAN /\ mirror \in BOOLEAN
    /\ undoable \in BOOLEAN /\ resizeUsed \in BOOLEAN
    /\ restoreUsed \in BOOLEAN /\ reshardUsed \in BOOLEAN

CopyCoversRoutedShards ==
    operation = "resize" => captured = Owners(maps[source])

SwapOnlyAfterCopyAndFence ==
    operation = "resize" /\ phase = "complete" =>
        copied = captured /\ fenced = captured

UndoRedirectBeforeSwap ==
    operation = "undo" /\ phase \in {"clear", "complete"} =>
        redirect["resized"] = source

UndoReturnsToSource ==
    operation = "undo" /\ phase \in {"clear", "complete"} => alias = source

SettledUndoUnfenced ==
    operation = "undo" /\ phase = "complete" =>
        fenced = {} /\ ~mirror /\ ~undoable

CutoverMatchesPhase ==
    /\ (operation = "restore" /\ phase \in {"redirect", "complete"} =>
            alias = "restored")
    /\ (operation = "revert" /\ phase \in {"redirect", "complete"} =>
            alias = source)

RedirectInstalledOnCompletion ==
    /\ (operation = "restore" /\ phase = "complete" =>
            redirect[source] = "restored")
    /\ (operation = "revert" /\ phase = "complete" =>
            redirect[source] = "none" /\ redirect["restored"] = source)

ExpiredMirrorReleased ==
    operation = "none" /\ ~undoable /\ resizeUsed /\ alias = "resized"
        /\ ~restoreUsed => ~mirror

NoMigrationDuringResize ==
    operation \in {"resize", "undo"} =>
        migration \in {"idle", "done"}

NoReshardWhileMirroring ==
    migration \in {"moving", "published"} => ~mirror /\ ~undoable

DeleteOutsideReservation ==
    deleted => operation = "none"

PublishedMigrationMatchesTarget ==
    migration \in {"published", "done"} => maps[migrationCopy] = target

ReservationSurvivesCrash ==
    [][((~running \/ ~running') /\ operation # "none") => operation' = operation]_vars

OperationReturns == (operation # "none") ~> (operation = "none")
ReshardReturns == (migration = "moving") ~> (migration = "done")
CrashRecovers == (~running) ~> running
=============================================================================
