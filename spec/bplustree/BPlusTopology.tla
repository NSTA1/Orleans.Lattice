---- MODULE BPlusTopology ----
\* A bounded leaf split and fold with writes interleaved at every protocol
\* boundary. The routing bit is published only after the new leaf is born and
\* both directions of the sibling chain agree.
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, SplitKey, MaxCrashes

VARIABLES
    donorRows, siblingRows, acked,
    phase, born, donorNext, siblingPrev, successorPrev, linked, childParent,
    pending, marker, held, crashes

vars ==
    <<donorRows, siblingRows, acked, phase, born, donorNext, siblingPrev,
      successorPrev, linked, childParent, pending, marker, held, crashes>>

Moving == {k \in Keys : k >= SplitKey}

TypeOK ==
    /\ donorRows \subseteq Keys
    /\ siblingRows \subseteq Keys
    /\ acked \subseteq Keys
    /\ phase \in {"idle", "splitting", "ready", "merging", "done"}
    /\ born \in BOOLEAN
    /\ donorNext \in BOOLEAN
    /\ siblingPrev \in BOOLEAN
    /\ successorPrev \in BOOLEAN
    /\ linked \in BOOLEAN
    /\ childParent \in BOOLEAN
    /\ pending \in BOOLEAN
    /\ marker \in BOOLEAN
    /\ held \in BOOLEAN
    /\ crashes \in 0..MaxCrashes

\* An acknowledged key remains present on at least one leaf at every step.
NoKeyLost == acked = donorRows \cup siblingRows

\* A published sibling can never be unborn or have a half-written chain.
ParentSiblingConsistency ==
    linked =>
        /\ born
        /\ donorNext
        /\ siblingPrev
        /\ successorPrev
        /\ childParent

\* A completed split with no parent route is not an unrecoverable orphan.
OrphanRecoverable ==
    (born /\ ~linked) =>
        (phase = "splitting" \/ pending \/ marker)

PendingOnlyForCompleteSplit ==
    pending => phase \in {"ready", "merging"}

SplitRequiresData == phase # "idle" => acked # {}

BornImpliesSplit == born => phase \in {"splitting", "ready", "merging"}

\* Copies can overlap while transfer is in flight, but not at a stable topology.
StableNoDuplicate ==
    phase \in {"ready", "done"} => donorRows \cap siblingRows = {}

Init ==
    /\ donorRows = {}
    /\ siblingRows = {}
    /\ acked = {}
    /\ phase = "idle"
    /\ born = FALSE
    /\ donorNext = FALSE
    /\ siblingPrev = FALSE
    /\ successorPrev = FALSE
    /\ linked = FALSE
    /\ childParent = FALSE
    /\ pending = FALSE
    /\ marker = FALSE
    /\ held = FALSE
    /\ crashes = 0

Write(k) ==
    /\ k \in Keys
    /\ k \notin acked
    /\ (phase # "ready" \/ linked \/ k \notin Moving)
    /\ acked' = acked \cup {k}
    /\ IF phase = "merging" /\ k \in Moving
          THEN
              /\ donorRows' = donorRows \cup {k}
              /\ siblingRows' = siblingRows \cup {k}
          ELSE IF linked /\ k \in Moving
              THEN
                  /\ donorRows' = donorRows
                  /\ siblingRows' = siblingRows \cup {k}
              ELSE
                  /\ donorRows' = donorRows \cup {k}
                  /\ siblingRows' = siblingRows
    /\ UNCHANGED <<phase, born, donorNext, siblingPrev, successorPrev, linked,
                   childParent, pending, marker, held, crashes>>

SplitIntent ==
    /\ phase = "idle"
    /\ \E k \in Keys : k \in donorRows /\ k \in Moving
    /\ phase' = "splitting"
    /\ UNCHANGED <<donorRows, siblingRows, acked, born, donorNext, siblingPrev,
                   successorPrev, linked, childParent, pending, marker, held, crashes>>

Birth ==
    /\ phase = "splitting"
    /\ ~born
    /\ born' = TRUE
    /\ siblingPrev' = TRUE
    /\ phase' = "splitting"
    /\ UNCHANGED <<donorRows, siblingRows, acked, donorNext, successorPrev,
                   linked, childParent, pending, marker, held, crashes>>

ChainDonor ==
    /\ phase = "splitting"
    /\ born
    /\ ~donorNext
    /\ donorNext' = TRUE
    /\ linked' = FALSE
    /\ UNCHANGED <<donorRows, siblingRows, acked, phase, born, siblingPrev,
                   successorPrev, childParent, pending, marker, held, crashes>>

ChainSuccessor ==
    /\ phase = "splitting"
    /\ donorNext
    /\ ~successorPrev
    /\ successorPrev' = TRUE
    /\ linked' = FALSE
    /\ UNCHANGED <<donorRows, siblingRows, acked, phase, born, donorNext,
                   siblingPrev, childParent, pending, marker, held, crashes>>

Move(k) ==
    /\ phase = "splitting"
    /\ born
    /\ k \in donorRows
    /\ k \in Moving
    /\ donorRows' = donorRows \ {k}
    /\ siblingRows' = siblingRows \cup {k}
    /\ UNCHANGED <<acked, phase, born, donorNext, siblingPrev, successorPrev,
                   linked, childParent, pending, marker, held, crashes>>

FinishSplit ==
    /\ phase = "splitting"
    /\ born
    /\ donorNext
    /\ siblingPrev
    /\ successorPrev
    /\ donorRows \cap Moving = {}
    /\ phase' = "ready"
    /\ donorRows' = donorRows
    /\ marker' = TRUE
    /\ held' = TRUE
    /\ UNCHANGED <<siblingRows, acked, born, donorNext, siblingPrev,
                   successorPrev, linked, childParent, pending, crashes>>

RecordPending ==
    /\ phase = "ready"
    /\ held \/ marker
    /\ ~pending
    /\ pending' = TRUE
    /\ phase' = "ready"
    /\ UNCHANGED <<donorRows, siblingRows, acked, born, donorNext, siblingPrev,
                   successorPrev, linked, childParent, marker, held, crashes>>

Link ==
    /\ phase = "ready"
    /\ (pending \/ held \/ marker)
    /\ donorNext
    /\ siblingPrev
    /\ successorPrev
    /\ linked' = TRUE
    /\ childParent' = TRUE
    /\ pending' = FALSE
    /\ marker' = FALSE
    /\ held' = FALSE
    /\ UNCHANGED <<donorRows, siblingRows, acked, phase, born, donorNext,
                   siblingPrev, successorPrev, crashes>>

Crash ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ held' = FALSE
    /\ marker' = marker
    /\ UNCHANGED <<donorRows, siblingRows, acked, phase, born, donorNext,
                   siblingPrev, successorPrev, linked, childParent, pending>>

MergeBegin ==
    /\ phase = "ready"
    /\ linked
    /\ phase' = "merging"
    /\ linked' = TRUE
    /\ UNCHANGED <<donorRows, siblingRows, acked, born, donorNext, siblingPrev,
                   successorPrev, childParent, pending, marker, held, crashes>>

Merge(k) ==
    /\ phase = "merging"
    /\ k \in siblingRows
    /\ donorRows' = donorRows \cup {k}
    /\ siblingRows' = siblingRows \ {k}
    /\ UNCHANGED <<acked, phase, born, donorNext, siblingPrev, successorPrev,
                   linked, childParent, pending, marker, held, crashes>>

MergeFinish ==
    /\ phase = "merging"
    /\ siblingRows = {}
    /\ phase' = "done"
    /\ born' = FALSE
    /\ donorNext' = FALSE
    /\ siblingPrev' = FALSE
    /\ successorPrev' = FALSE
    /\ linked' = FALSE
    /\ childParent' = FALSE
    /\ pending' = FALSE
    /\ marker' = FALSE
    /\ held' = FALSE
    /\ UNCHANGED <<donorRows, siblingRows, acked, crashes>>

Stutter ==
    /\ phase = "done"
    /\ acked = Keys
    /\ UNCHANGED vars

Next ==
    \/ \E k \in Keys : Write(k)
    \/ SplitIntent
    \/ Birth
    \/ ChainDonor
    \/ ChainSuccessor
    \/ \E k \in Keys : Move(k)
    \/ FinishSplit
    \/ RecordPending
    \/ Link
    \/ Crash
    \/ MergeBegin
    \/ \E k \in Keys : Merge(k)
    \/ MergeFinish
    \/ Stutter

Spec == Init /\ [][Next]_vars
====
