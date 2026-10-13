----------------------------- MODULE AliasRevert -----------------------------
\* Physical-identity revert fences, including administrative moves during awaits.
EXTENDS Naturals
CONSTANT MaxMoves
Copies == {"a", "b", "s", "c"}
VARIABLES alias, map, previous, phase, reservation, lineage, observed,
          moves, deleted, migration, migrationDone, staleRevert, staleMigration
vars == <<alias, map, previous, phase, reservation, lineage, observed,
          moves, deleted, migration, migrationDone, staleRevert, staleMigration>>

Init ==
    /\ previous \in {"a", "b"}
    /\ alias = "s" /\ map = "s" /\ phase = "new"
    /\ reservation = FALSE /\ lineage = 0 /\ observed = "s"
    /\ moves = 0 /\ deleted = FALSE
    /\ migration = "none" /\ migrationDone = FALSE
    /\ staleRevert = FALSE /\ staleMigration = FALSE

Validate ==
    /\ phase = "new"
    /\ phase' = IF alias \in {"s", previous} THEN "validated" ELSE "refused"
    /\ UNCHANGED <<alias, map, previous, reservation, lineage, observed, moves,
                   deleted, migration, migrationDone, staleRevert, staleMigration>>

Reserve ==
    /\ phase = "validated" /\ ~deleted
    /\ reservation' = TRUE /\ phase' = "reserved"
    /\ UNCHANGED <<alias, map, previous, lineage, observed, moves,
                   deleted, migration, migrationDone, staleRevert, staleMigration>>

Restamp ==
    /\ phase = "reserved"
    /\ lineage' = lineage + 1 /\ phase' = "stamped"
    /\ UNCHANGED <<alias, map, previous, reservation, observed, moves,
                   deleted, migration, migrationDone, staleRevert, staleMigration>>

\* The helper rechecks after service-side admission/lineage. A failure here
\* retains the reservation, as production does; it is not a neutral refusal.
ReadBinding ==
    /\ phase = "stamped"
    /\ phase' = IF alias = previous THEN "reverted"
                ELSE IF alias = "s" THEN "ready" ELSE "failed"
    /\ observed' = alias
    /\ UNCHANGED <<alias, map, previous, reservation, lineage, moves,
                   deleted, migration, migrationDone, staleRevert, staleMigration>>

Revert ==
    /\ phase = "ready"
    /\ phase' = IF alias = observed THEN "reverted" ELSE "failed"
    /\ alias' = IF alias = observed THEN previous ELSE alias
    /\ map' = IF alias = observed THEN previous ELSE map
    /\ staleRevert' = (staleRevert \/ (alias # observed /\ map' # map))
    /\ UNCHANGED <<previous, reservation, lineage, observed, moves,
                   deleted, migration, migrationDone, staleMigration>>

Finish ==
    /\ phase = "reverted"
    /\ reservation' = FALSE /\ phase' = "done"
    /\ UNCHANGED <<alias, map, previous, lineage, observed, moves,
                   deleted, migration, migrationDone, staleRevert, staleMigration>>

\* Administrative alias writes check deletion, but do not take this saga's
\* reservation. ABA is deliberately possible and NOT epoch-fenced by a revert.
MoveAlias ==
    \E p \in Copies :
        /\ ~deleted /\ p # alias /\ moves < MaxMoves
        /\ alias' = p /\ map' = p /\ moves' = moves + 1
        /\ UNCHANGED <<previous, phase, reservation, lineage, observed, deleted,
                       migration, migrationDone, staleRevert, staleMigration>>

Delete ==
    /\ ~deleted /\ ~reservation
    /\ deleted' = TRUE
    /\ UNCHANGED <<alias, map, previous, phase, reservation, lineage, observed,
                   moves, migration, migrationDone, staleRevert, staleMigration>>

BindMigration ==
    /\ migration = "none" /\ ~deleted
    /\ migration' = alias
    /\ UNCHANGED <<alias, map, previous, phase, reservation, lineage, observed,
                   moves, deleted, migrationDone, staleRevert, staleMigration>>

PublishMigration ==
    /\ migration # "none" /\ ~migrationDone
    /\ migrationDone' = TRUE
    /\ map' = map
    /\ staleMigration' = (staleMigration \/ (migration # alias /\ map' # map))
    /\ UNCHANGED <<alias, previous, phase, reservation, lineage, observed,
                   moves, deleted, migration, staleRevert>>

Stutter == UNCHANGED vars
Next ==
    \/ Validate
    \/ Reserve
    \/ Restamp
    \/ ReadBinding
    \/ Revert
    \/ Finish
    \/ MoveAlias
    \/ Delete
    \/ BindMigration
    \/ PublishMigration
    \/ Stutter
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ alias \in Copies /\ map \in Copies /\ previous \in {"a", "b"}
    /\ phase \in {"new", "validated", "refused", "reserved", "stamped",
                   "ready", "reverted", "failed", "done"}
    /\ reservation \in BOOLEAN /\ lineage \in 0..1 /\ observed \in Copies
    /\ moves \in 0..MaxMoves /\ deleted \in BOOLEAN
    /\ migration \in Copies \cup {"none"} /\ migrationDone \in BOOLEAN
    /\ staleRevert \in BOOLEAN /\ staleMigration \in BOOLEAN
CoherentRow == alias = map
NoStaleRevert == ~staleRevert
BoundMigration == ~staleMigration
FinishedReservationReleased == phase = "done" => ~reservation
EarlyRefusalNeutral ==
    [][(phase = "new" /\ phase' = "refused") =>
        /\ alias' = alias /\ map' = map
        /\ lineage' = lineage /\ reservation' = reservation]_vars
RevertedBinding ==
    [][(phase = "ready" /\ phase' = "reverted") =>
        /\ alias' = previous /\ map' = previous]_vars
ReservationBlocksDelete ==
    [][reservation => deleted' = deleted]_vars
DeletionBlocksReservation ==
    [][deleted => reservation' = reservation]_vars
CapturedBinding ==
    [][(phase = "stamped" /\ phase' = "ready") => observed' = alias]_vars
CapturedMigration ==
    [][(migration = "none" /\ migration' # "none") => migration' = alias]_vars
LineageBeforeRevert ==
    [][(phase = "ready" /\ phase' = "reverted") => lineage = 1]_vars
=============================================================================
