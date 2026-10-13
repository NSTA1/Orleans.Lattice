---- MODULE SplitLinkTurns ----
EXTENDS Naturals

\* Grain turns and the wait-for graph of a capture-path split link (#4914).
\*
\* The donor leaf links its own split while still inside its capture turn: it
\* calls the shard root, which records the link under its link gate and hands
\* it to the parent internal node. A parent already at its fan-out limit splits
\* too, and the new internal sibling seeds the children it took over by calling
\* back into each of them - including the donor leaf whose turn started the
\* chain. If those callbacks queue behind the leaf's turn, the chain waits on
\* itself and only the call timeout breaks it, failing the capture after the
\* write it was carrying had been acknowledged.
\*
\* The model also tracks the leaf's storage writes. Interleaving the callbacks
\* lets their persist overlap a persist from a turn that holds no split gate
\* (a checkpoint flush), and lets it race the clear of a reclaimed leaf.

VARIABLES
    leafTurn,   \* "idle", "capture" (the donor's exclusive turn), "done"
    chain,      \* progress of the capture link through root and parent
    seed,       \* the new internal sibling's callbacks into the donor
    linkGate,   \* the shard root's link gate
    linked,     \* the split is linked into the parent
    lost,       \* an acknowledged write was failed by the call timeout
    gate,       \* the leaf's persist-write gate holder
    writes,     \* storage writes in flight on the leaf
    flush,      \* an ungated persist from an interleaved turn
    cleared,    \* the leaf's state was cleared
    resurrected \* a write landed on cleared state

vars == <<leafTurn, chain, seed, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* The #4914 fix: the seeding callbacks carry [AlwaysInterleave], so the
\* runtime admits them while the capture turn is still running.
SetParentInterleaves == TRUE
DigestInterleaves == TRUE

Linking == chain \in {"parent", "seed", "return"}

TypeOK ==
    /\ leafTurn \in {"idle", "capture", "done"}
    /\ chain \in {"none", "root", "parent", "seed", "return", "done"}
    /\ seed \in {"none", "queued", "admitted", "blocked", "writing",
                 "digestQueued", "digest", "digestBlocked", "done"}
    /\ linkGate \in BOOLEAN
    /\ linked \in BOOLEAN
    /\ lost \in BOOLEAN
    /\ gate \in {"free", "setparent", "flush"}
    /\ writes \in 0..2
    /\ flush \in {"idle", "writing", "done"}
    /\ cleared \in BOOLEAN
    /\ resurrected \in BOOLEAN

\* No call is queued behind the turn that is itself waiting on that call.
NoTurnWaitCycle == seed \notin {"blocked", "digestBlocked"}

NoAcknowledgedWriteLost == ~lost

NoConcurrentLeafWrites == writes <= 1

NoResurrectionAfterClear == ~resurrected

LinkGateMatchesLinking == linkGate <=> Linking

LinkedOnlyAfterSeed == linked => seed = "done"

Init ==
    /\ leafTurn = "idle"
    /\ chain = "none"
    /\ seed = "none"
    /\ linkGate = FALSE
    /\ linked = FALSE
    /\ lost = FALSE
    /\ gate = "free"
    /\ writes = 0
    /\ flush = "idle"
    /\ cleared = FALSE
    /\ resurrected = FALSE

\* The capture turn overflows the leaf, splits it, and starts the link.
BeginCapture ==
    /\ leafTurn = "idle"
    /\ ~cleared
    /\ leafTurn' = "capture"
    /\ chain' = "root"
    /\ UNCHANGED <<seed, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* The shard root records the link under its link gate.
RootRecordLink ==
    /\ chain = "root"
    /\ ~linkGate
    /\ linkGate' = TRUE
    /\ chain' = "parent"
    /\ UNCHANGED <<leafTurn, seed, linked, lost, gate, writes, flush, cleared, resurrected>>

\* The full parent accepts the split, splits, and its new sibling seeds the
\* children it took over, the donor among them.
ParentOverflow ==
    /\ chain = "parent"
    /\ chain' = "seed"
    /\ seed' = "queued"
    /\ UNCHANGED <<leafTurn, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* The runtime admits SetParentAsync, or queues it behind the leaf's turn.
AdmitSetParent ==
    /\ seed = "queued"
    /\ seed' = IF SetParentInterleaves \/ leafTurn # "capture" THEN "admitted" ELSE "blocked"
    /\ UNCHANGED <<leafTurn, chain, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* SetParentAsync takes the persist-write gate for its write.
BeginSetParent ==
    /\ seed = "admitted"
    /\ gate = "free"
    /\ gate' = "setparent"
    /\ writes' = writes + 1
    /\ seed' = "writing"
    /\ UNCHANGED <<leafTurn, chain, linkGate, linked, lost, flush, cleared, resurrected>>

FinishSetParent ==
    /\ seed = "writing"
    /\ gate' = "free"
    /\ writes' = writes - 1
    /\ resurrected' = (resurrected \/ cleared)
    /\ seed' = "digestQueued"
    /\ UNCHANGED <<leafTurn, chain, linkGate, linked, lost, flush, cleared>>

\* The runtime admits GetChildDigestSnapshotAsync, or queues it.
AdmitDigest ==
    /\ seed = "digestQueued"
    /\ seed' = IF DigestInterleaves \/ leafTurn # "capture" THEN "digest" ELSE "digestBlocked"
    /\ UNCHANGED <<leafTurn, chain, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* GetChildDigestSnapshotAsync answers the sibling's digest read.
ServeDigest ==
    /\ seed = "digest"
    /\ seed' = "done"
    /\ chain' = "return"
    /\ UNCHANGED <<leafTurn, linkGate, linked, lost, gate, writes, flush, cleared, resurrected>>

\* The link returns to the root, which releases its gate; the capture turn ends.
ReturnLink ==
    /\ chain = "return"
    /\ linked' = TRUE
    /\ linkGate' = FALSE
    /\ chain' = "done"
    /\ leafTurn' = "done"
    /\ UNCHANGED <<seed, lost, gate, writes, flush, cleared, resurrected>>

\* An interleaved turn holding no split gate persists (a checkpoint flush).
\* It re-checks the clear latch once it holds the persist-write gate.
BeginFlush ==
    /\ flush = "idle"
    /\ gate = "free"
    /\ ~cleared
    /\ gate' = "flush"
    /\ writes' = writes + 1
    /\ flush' = "writing"
    /\ UNCHANGED <<leafTurn, chain, seed, linkGate, linked, lost, cleared, resurrected>>

FinishFlush ==
    /\ flush = "writing"
    /\ gate' = "free"
    /\ writes' = writes - 1
    /\ resurrected' = (resurrected \/ cleared)
    /\ flush' = "done"
    /\ UNCHANGED <<leafTurn, chain, seed, linkGate, linked, lost, cleared>>

\* The reclaim clears the leaf's state under the persist-write gate.
ClearLeaf ==
    /\ leafTurn # "capture"
    /\ ~cleared
    /\ gate = "free"
    /\ cleared' = TRUE
    /\ UNCHANGED <<leafTurn, chain, seed, linkGate, linked, lost, gate, writes, flush, resurrected>>

\* The call timeout breaks a wait cycle by failing the capture.
Timeout ==
    /\ seed \in {"blocked", "digestBlocked"}
    /\ lost' = TRUE
    /\ seed' = "done"
    /\ chain' = "done"
    /\ linkGate' = FALSE
    /\ leafTurn' = "done"
    /\ UNCHANGED <<linked, gate, writes, flush, cleared, resurrected>>

Stutter == UNCHANGED vars

Next ==
    \/ BeginCapture
    \/ RootRecordLink
    \/ ParentOverflow
    \/ AdmitSetParent
    \/ BeginSetParent
    \/ FinishSetParent
    \/ AdmitDigest
    \/ ServeDigest
    \/ ReturnLink
    \/ BeginFlush
    \/ FinishFlush
    \/ ClearLeaf
    \/ Timeout
    \/ Stutter

CaptureEventuallyLinked == leafTurn = "capture" ~> linked

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(RootRecordLink)
    /\ WF_vars(ParentOverflow)
    /\ WF_vars(AdmitSetParent)
    /\ WF_vars(BeginSetParent)
    /\ WF_vars(FinishSetParent)
    /\ WF_vars(AdmitDigest)
    /\ WF_vars(ServeDigest)
    /\ WF_vars(ReturnLink)
    /\ WF_vars(FinishFlush)
    /\ WF_vars(Timeout)
====
