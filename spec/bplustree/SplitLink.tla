---- MODULE SplitLink ----
\* Leaf split followed by the shard root linking the new sibling into its parent
\* (issue #4795). The leaf completes the split (birth, chain, row transfer,
\* SplitInFlight cleared) and returns a SplitResult in memory. The shard root
\* persists a PendingChildLink intent only afterwards, then links.
\*
\* The fix: completing the split also persists an unacknowledged-split marker on
\* the donor. Recovery entry points re-surface the result while the marker is
\* set, and the root acknowledges it only once the link intent is durable.
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, SplitKey, MaxCrashes

VARIABLES rowsD, rowsS, acked, phase, born, chained, held, pending, linked, marker, crashes

vars == <<rowsD, rowsS, acked, phase, born, chained, held, pending, linked, marker, crashes>>

Moving == {k \in Keys : k >= SplitKey}

TypeOK ==
    /\ rowsD \subseteq Keys /\ rowsS \subseteq Keys /\ acked \subseteq Keys
    /\ phase \in {"idle", "intent", "moving", "done"}
    /\ born \in BOOLEAN /\ chained \in BOOLEAN
    /\ held \in BOOLEAN /\ pending \in BOOLEAN /\ linked \in BOOLEAN
    /\ marker \in BOOLEAN
    /\ crashes \in 0..MaxCrashes

\* No acknowledged key is ever lost or duplicated across the two halves.
NoKeyLostOrDuplicated ==
    /\ acked = rowsD \cup rowsS
    /\ rowsD \cap rowsS = {}

\* Once the split is complete and unlinked, some DURABLE record of the
\* obligation exists. The in-memory result (held) does not count.
LinkObligationDurable ==
    (phase = "done" /\ ~linked) => (pending \/ marker)

\* The root records a link intent only for a split the donor has completed, and
\* retires it when the link lands.
IntentOnlyForCompleteSplit == pending => phase = "done"
LinkedClearsIntent == linked => ~pending

\* The sibling is born before it is chained, and only for a split in flight.
ChainedImpliesBorn == chained => born
BornImpliesIntent == born => phase # "idle"

\* Only a leaf holding an acknowledged key divides.
SplitOnlyOfNonEmptyLeaf == phase # "idle" => acked # {}

Init ==
    /\ rowsD = {} /\ rowsS = {} /\ acked = {}
    /\ phase = "idle" /\ born = FALSE /\ chained = FALSE
    /\ held = FALSE /\ pending = FALSE /\ linked = FALSE /\ marker = FALSE
    /\ crashes = 0

Write(k) ==
    /\ k \notin acked
    /\ acked' = acked \cup {k}
    /\ IF linked /\ k >= SplitKey
          THEN /\ rowsS' = rowsS \cup {k} /\ UNCHANGED rowsD
          ELSE /\ rowsD' = rowsD \cup {k} /\ UNCHANGED rowsS
    /\ UNCHANGED <<phase, born, chained, held, pending, linked, marker, crashes>>

SplitIntent ==
    /\ phase = "idle" /\ rowsD # {}
    /\ phase' = "intent"
    /\ UNCHANGED <<rowsD, rowsS, acked, born, chained, held, pending, linked, marker, crashes>>

Birth ==
    /\ phase = "intent" /\ ~born
    /\ born' = TRUE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, chained, held, pending, linked, marker, crashes>>

Chain ==
    /\ phase = "intent" /\ born /\ ~chained
    /\ chained' = TRUE /\ phase' = "moving"
    /\ UNCHANGED <<rowsD, rowsS, acked, born, held, pending, linked, marker, crashes>>

MoveRow(k) ==
    /\ phase = "moving" /\ k \in rowsD /\ k \in Moving
    /\ rowsS' = rowsS \cup {k} /\ rowsD' = rowsD \ {k}
    /\ UNCHANGED <<acked, phase, born, chained, held, pending, linked, marker, crashes>>

FinishSplit ==
    /\ phase = "moving" /\ \A k \in rowsD : k \notin Moving
    /\ phase' = "done" /\ held' = TRUE /\ marker' = TRUE
    /\ UNCHANGED <<rowsD, rowsS, acked, born, chained, pending, linked, crashes>>

\* The root persists the link intent for a result it holds.
RecordPending ==
    /\ phase = "done" /\ held /\ ~pending /\ ~linked
    /\ pending' = TRUE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, born, chained, held, linked, marker, crashes>>

\* The root acknowledges the donor once the intent is durable (or the link is done).
Acknowledge ==
    /\ marker /\ (pending \/ linked)
    /\ marker' = FALSE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, born, chained, held, pending, linked, crashes>>

\* A recovery entry point on the donor hands a still-marked result back to the root.
Resurface ==
    /\ marker /\ ~held /\ ~pending /\ ~linked
    /\ held' = TRUE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, born, chained, pending, linked, marker, crashes>>

Link ==
    /\ phase = "done" /\ (pending \/ held) /\ ~linked
    /\ linked' = TRUE /\ pending' = FALSE /\ held' = FALSE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, born, chained, marker, crashes>>

\* The shard root loses its in-memory result. Durable state survives.
Crash ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1 /\ held' = FALSE
    /\ UNCHANGED <<rowsD, rowsS, acked, phase, born, chained, pending, linked, marker>>

Stutter == acked = Keys /\ phase = "done" /\ linked /\ ~marker /\ UNCHANGED vars

Next ==
    \/ \E k \in Keys : Write(k)
    \/ SplitIntent
    \/ Birth
    \/ Chain
    \/ \E k \in Keys : MoveRow(k)
    \/ FinishSplit
    \/ RecordPending
    \/ Acknowledge
    \/ Resurface
    \/ Link
    \/ Crash
    \/ Stutter

Spec == Init /\ [][Next]_vars
        /\ WF_vars(Birth) /\ WF_vars(Chain) /\ WF_vars(FinishSplit)
        /\ WF_vars(RecordPending) /\ WF_vars(Acknowledge)
        /\ WF_vars(Resurface) /\ WF_vars(Link)
        /\ \A k \in Keys : WF_vars(MoveRow(k))

SplitEventuallyLinked == (phase = "done") ~> linked
====
