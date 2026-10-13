---- MODULE SplitGateCycles ----
EXTENDS Naturals, FiniteSets

\* Wait-for cycles between leaf split gates, leaf turns, and the split links
\* that travel through the shard root and back into the leaves.
\*
\* SplitLinkTurns checks one capture link against one donor leaf. This module
\* checks what that model abstracts away: two adjacent leaves B and C, a
\* foreground split on B that holds B's split gate while it calls C's
\* SetPrevSiblingAsync, concurrent captures on both leaves, a capture nested
\* inside the foreground split's checkpoint-hint path, the root's link gate with
\* its defer-and-drain path for a busy capture link, a parent overflow whose new
\* sibling seeds both leaves, a cascade one level further up, a consolidation
\* turn that takes C's split gate, and deactivation of either leaf.
\*
\* A turn waits on a split gate (SetParentAsync, SetPrevSiblingAsync, the
\* foreground split), on admission into a leaf (a non-interleaving call queues
\* behind every running request, and a deactivating activation admits nothing),
\* or on a link chain it started. NoWaitCycle requires that whenever work is
\* in flight some step of that work can still be taken: a state in which
\* every in-flight turn waits on another in-flight turn is a cycle that only
\* the call timeout breaks.

Leaves == {"B", "C"}
Origins == {"fg", "nest", "capB", "capC"}
CaptureOrigins == {"nest", "capB", "capC"}

\* The split-link fixes this module holds in place. Each is TRUE in the shipped
\* code; the mutations set them FALSE to reproduce the cycle each one removes.
\* #4914: the seeding callbacks are admitted into a busy leaf.
SetParentInterleaves == TRUE
DigestInterleaves == TRUE
\* The sibling setters are admitted into a busy leaf, so a split holding its own
\* gate never queues behind the neighbour's capture turn.
SiblingSettersInterleave == TRUE
\* A capture nested under a held split gate leaves the unlinked-split marker for
\* a later turn instead of calling the root while the gate is held.
NestedCaptureDefersLink == TRUE
\* The root defers a capture link that finds its link gate busy.
RootDefersBusyLink == TRUE
\* Orleans collects only an idle activation; the leaf's test-only
\* ForceDeactivateAsync seam is called on a quiescent leaf.
DeactivationOnlyWhenIdle == TRUE

VARIABLES
    fg,         \* B's foreground split, inside an interleaving SetAsync request
    cap,        \* each leaf's capture turn (non-interleaving)
    cons,       \* a consolidation turn on C (non-interleaving)
    gate,       \* each leaf's split gate holder
    sib,        \* the SetPrevSiblingAsync call a split of B makes into C
    ch,         \* each link chain's progress through the root and the parents
    lg,         \* the shard root's link gate holder
    seed,       \* the new internal sibling's callbacks into each leaf
    overflowed, \* the parent has divided once
    cascaded,   \* the division has climbed one level further
    linked,     \* each chain's split is linked
    dep,        \* each leaf's activation state
    deacts      \* deactivations taken per leaf

vars == <<fg, cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

FgRunning == {"gate", "sib", "awaitSib", "nested", "awaitNest", "release"}
FgHolding == {"sib", "awaitSib", "nested", "awaitNest", "release"}
CapRunning == {"split", "splitting", "awaitSib", "link"}
CapHolding == {"splitting", "awaitSib"}
ChainActive == {"parent", "seed", "grand", "cascade", "return"}

\* Every request currently running on a leaf, interleaving or not.
Running(l) ==
    \/ l = "B" /\ fg \in FgRunning
    \/ cap[l] \in CapRunning
    \/ l = "C" /\ cons \in {"running", "holding"}
    \/ l = "C" /\ sib \in {"admitted", "writing"}
    \/ seed[l] \in {"admitted", "writing"}

\* A call is admitted only into a live activation; a non-interleaving call
\* also waits for every running request to finish.
Admits(l, interleaves) ==
    /\ dep[l] = "live"
    /\ interleaves \/ ~Running(l)

\* The leaf whose split sends the SetPrevSiblingAsync call: whoever holds B's gate.
SibOwner == IF fg \in FgHolding THEN "fg" ELSE "capB"

TypeOK ==
    /\ fg \in {"idle", "gate"} \cup FgHolding \cup {"done"}
    /\ cap \in [Leaves -> {"idle", "done"} \cup CapRunning]
    /\ cons \in {"idle", "queued", "running", "holding", "done"}
    /\ gate \in [Leaves -> {"free", "fg", "cap", "sib", "seed", "cons"}]
    /\ sib \in {"none", "queued", "admitted", "writing", "done"}
    /\ ch \in [Origins -> {"none", "root", "deferred", "done"} \cup ChainActive]
    /\ lg \in Origins \cup {"none"}
    /\ seed \in [Leaves -> {"none", "queued", "admitted", "writing", "digest", "done"}]
    /\ overflowed \in BOOLEAN
    /\ cascaded \in BOOLEAN
    /\ linked \in [Origins -> BOOLEAN]
    /\ dep \in [Leaves -> {"live", "deactivating"}]
    /\ deacts \in [Leaves -> 0..1]

\* Each split gate is held only by the turn that took it.
GateOwnership ==
    /\ fg \in FgHolding <=> gate["B"] = "fg"
    /\ \A l \in Leaves : cap[l] \in CapHolding <=> gate[l] = "cap"
    /\ cons = "holding" <=> gate["C"] = "cons"
    /\ sib = "writing" <=> gate["C"] = "sib"
    /\ \A l \in Leaves : seed[l] = "writing" <=> gate[l] = "seed"

\* At most one link mutates the parents at a time.
SingleParentMutation == Cardinality({o \in Origins : ch[o] \in ChainActive}) <= 1

\* The root holds its link gate exactly for the chain that is past it.
LinkGateMatchesLinking ==
    \A o \in Origins : ch[o] \in ChainActive => lg = o

\* A chain carries the division upward only once the seeding it caused has
\* finished in both leaves.
ClimbsOnlyAfterSeed ==
    \A o \in Origins :
        ch[o] \in {"grand", "cascade", "return"} => \A l \in Leaves : seed[l] \in {"none", "done"}

\* A deferred link is a capture's, and is never left holding the link gate.
DeferredOnlyForCapture ==
    \A o \in Origins : ch[o] = "deferred" => o \in CaptureOrigins /\ lg # o

Settled ==
    /\ fg \in {"idle", "done"}
    /\ \A l \in Leaves : cap[l] \in {"idle", "done"}
    /\ cons \in {"idle", "done"}
    /\ sib = "none"
    /\ \A o \in Origins : ch[o] \in {"none", "done"}
    /\ \A l \in Leaves : seed[l] \in {"none", "done"}
    /\ \A l \in Leaves : dep[l] = "live"

Init ==
    /\ fg = "idle"
    /\ cap = [l \in Leaves |-> "idle"]
    /\ cons = "idle"
    /\ gate = [l \in Leaves |-> "free"]
    /\ sib = "none"
    /\ ch = [o \in Origins |-> "none"]
    /\ lg = "none"
    /\ seed = [l \in Leaves |-> "none"]
    /\ overflowed = FALSE
    /\ cascaded = FALSE
    /\ linked = [o \in Origins |-> FALSE]
    /\ dep = [l \in Leaves |-> "live"]
    /\ deacts = [l \in Leaves |-> 0]

\* ---------------------------------------------------------------------------
\* B's foreground split: an interleaving SetAsync that overflows the leaf.

BeginForeground ==
    /\ fg = "idle"
    /\ Admits("B", TRUE)
    /\ fg' = "gate"
    /\ UNCHANGED <<cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* SplitAsync waits for B's split gate.
ForegroundTakeGate ==
    /\ fg = "gate"
    /\ gate["B"] = "free"
    /\ gate' = [gate EXCEPT !["B"] = "fg"]
    /\ fg' = "sib"
    /\ UNCHANGED <<cap, cons, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* Under the gate, the split calls the old successor's SetPrevSiblingAsync.
ForegroundCallSibling ==
    /\ fg = "sib"
    /\ sib = "none"
    /\ sib' = "queued"
    /\ fg' = "awaitSib"
    /\ UNCHANGED <<cap, cons, gate, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

ForegroundSiblingDone ==
    /\ fg = "awaitSib"
    /\ sib = "done"
    /\ sib' = "none"
    /\ fg' = "nested"
    /\ UNCHANGED <<cap, cons, gate, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The checkpoint-hint loop reaches no capture, or a capture that finds the
\* unlinked-split marker and leaves it for a later turn.
ForegroundSkipNested ==
    /\ fg = "nested"
    /\ fg' = "release"
    /\ UNCHANGED <<cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The hint loop's capture drains the marker by calling the root while the
\* split gate is still held.
ForegroundNestedLink ==
    /\ fg = "nested"
    /\ ~NestedCaptureDefersLink
    /\ ch["nest"] = "none"
    /\ ch' = [ch EXCEPT !["nest"] = "root"]
    /\ fg' = "awaitNest"
    /\ UNCHANGED <<cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

ForegroundNestedDone ==
    /\ fg = "awaitNest"
    /\ ch["nest"] \in {"done", "deferred"}
    /\ fg' = "release"
    /\ UNCHANGED <<cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The split releases the gate and returns its result; the root links it in
\* its own turn.
ForegroundRelease ==
    /\ fg = "release"
    /\ gate' = [gate EXCEPT !["B"] = "free"]
    /\ fg' = "done"
    /\ ch' = [ch EXCEPT !["fg"] = "root"]
    /\ UNCHANGED <<cap, cons, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* ---------------------------------------------------------------------------
\* Capture turns. A capture splits only if its split gate is free (Wait(0)),
\* releases the gate, then links the split through the root.

BeginCapture(l) ==
    /\ cap[l] = "idle"
    /\ Admits(l, FALSE)
    /\ cap' = [cap EXCEPT ![l] = "split"]
    /\ UNCHANGED <<fg, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

CaptureTakeGate(l) ==
    /\ cap[l] = "split"
    /\ gate[l] = "free"
    /\ gate' = [gate EXCEPT ![l] = "cap"]
    /\ cap' = [cap EXCEPT ![l] = "splitting"]
    /\ UNCHANGED <<fg, cons, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

CaptureGateBusy(l) ==
    /\ cap[l] = "split"
    /\ gate[l] # "free"
    /\ cap' = [cap EXCEPT ![l] = "done"]
    /\ UNCHANGED <<fg, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* A split of B calls C's SetPrevSiblingAsync under B's gate.
CaptureCallSibling ==
    /\ cap["B"] = "splitting"
    /\ sib = "none"
    /\ sib' = "queued"
    /\ cap' = [cap EXCEPT !["B"] = "awaitSib"]
    /\ UNCHANGED <<fg, cons, gate, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

CaptureSiblingDone ==
    /\ cap["B"] = "awaitSib"
    /\ sib = "done"
    /\ sib' = "none"
    /\ gate' = [gate EXCEPT !["B"] = "free"]
    /\ cap' = [cap EXCEPT !["B"] = "link"]
    /\ ch' = [ch EXCEPT !["capB"] = "root"]
    /\ UNCHANGED <<fg, cons, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* C is the last leaf, so its split has no successor to call.
CaptureSplitC ==
    /\ cap["C"] = "splitting"
    /\ gate' = [gate EXCEPT !["C"] = "free"]
    /\ cap' = [cap EXCEPT !["C"] = "link"]
    /\ ch' = [ch EXCEPT !["capC"] = "root"]
    /\ UNCHANGED <<fg, cons, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The capture turn ends when its link lands or the root defers it.
CaptureLinkReturns(l) ==
    /\ cap[l] = "link"
    /\ ch[IF l = "B" THEN "capB" ELSE "capC"] \in {"done", "deferred"}
    /\ cap' = [cap EXCEPT ![l] = "done"]
    /\ UNCHANGED <<fg, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* ---------------------------------------------------------------------------
\* SetPrevSiblingAsync on C: admitted, then waits for C's split gate.

AdmitSibling ==
    /\ sib = "queued"
    /\ Admits("C", SiblingSettersInterleave)
    /\ sib' = "admitted"
    /\ UNCHANGED <<fg, cap, cons, gate, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

SiblingTakeGate ==
    /\ sib = "admitted"
    /\ gate["C"] = "free"
    /\ gate' = [gate EXCEPT !["C"] = "sib"]
    /\ sib' = "writing"
    /\ UNCHANGED <<fg, cap, cons, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

SiblingFinish ==
    /\ sib = "writing"
    /\ gate' = [gate EXCEPT !["C"] = "free"]
    /\ sib' = "done"
    /\ UNCHANGED <<fg, cap, cons, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* ---------------------------------------------------------------------------
\* Consolidation marks C moved away: a non-interleaving turn that takes C's
\* split gate, persists, and calls nothing else.

BeginConsolidation ==
    /\ cons = "idle"
    /\ cons' = "queued"
    /\ UNCHANGED <<fg, cap, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

AdmitConsolidation ==
    /\ cons = "queued"
    /\ Admits("C", FALSE)
    /\ cons' = "running"
    /\ UNCHANGED <<fg, cap, gate, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

ConsolidationTakeGate ==
    /\ cons = "running"
    /\ gate["C"] = "free"
    /\ gate' = [gate EXCEPT !["C"] = "cons"]
    /\ cons' = "holding"
    /\ UNCHANGED <<fg, cap, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

ConsolidationFinish ==
    /\ cons = "holding"
    /\ gate' = [gate EXCEPT !["C"] = "free"]
    /\ cons' = "done"
    /\ UNCHANGED <<fg, cap, sib, ch, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* ---------------------------------------------------------------------------
\* The shard root and the parents.

RootTakeLinkGate(o) ==
    /\ ch[o] = "root"
    /\ lg = "none"
    /\ lg' = o
    /\ ch' = [ch EXCEPT ![o] = "parent"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, seed, overflowed, cascaded, linked, dep, deacts>>

\* A capture link that finds the link gate busy is parked and the capture
\* returns at once.
RootDeferLink(o) ==
    /\ ch[o] = "root"
    /\ RootDefersBusyLink
    /\ o \in CaptureOrigins
    /\ lg \notin {"none", o}
    /\ ch' = [ch EXCEPT ![o] = "deferred"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The root drains a parked link once the gate is free.
RootDrainDeferred(o) ==
    /\ ch[o] = "deferred"
    /\ lg = "none"
    /\ ch' = [ch EXCEPT ![o] = "root"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The parent accepts the split; a full parent divides once, and its new
\* sibling seeds both leaves.
ParentAccept(o) ==
    /\ ch[o] = "parent"
    /\ ~overflowed
    /\ overflowed' = TRUE
    /\ seed' = [l \in Leaves |-> "queued"]
    /\ ch' = [ch EXCEPT ![o] = "seed"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, cascaded, linked, dep, deacts>>

ParentAcceptNoOverflow(o) ==
    /\ ch[o] = "parent"
    /\ ch' = [ch EXCEPT ![o] = "return"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The seeding is complete; the root carries the parent's division up.
SeedComplete(o) ==
    /\ ch[o] = "seed"
    /\ \A l \in Leaves : seed[l] = "done"
    /\ ch' = [ch EXCEPT ![o] = "grand"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The grandparent accepts the division and divides too; its new sibling
\* reparents the internal nodes it took over, never the leaves.
GrandparentCascade(o) ==
    /\ ch[o] = "grand"
    /\ ~cascaded
    /\ cascaded' = TRUE
    /\ ch' = [ch EXCEPT ![o] = "cascade"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, linked, dep, deacts>>

GrandparentAbsorbs(o) ==
    /\ ch[o] = "grand"
    /\ ch' = [ch EXCEPT ![o] = "return"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

CascadeSeeded(o) ==
    /\ ch[o] = "cascade"
    /\ ch' = [ch EXCEPT ![o] = "return"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, lg, seed, overflowed, cascaded, linked, dep, deacts>>

\* The link lands and the root releases its link gate.
ReturnLink(o) ==
    /\ ch[o] = "return"
    /\ lg' = "none"
    /\ ch' = [ch EXCEPT ![o] = "done"]
    /\ linked' = [linked EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, seed, overflowed, cascaded, dep, deacts>>

\* ---------------------------------------------------------------------------
\* The new internal sibling's callbacks into each leaf.

AdmitSetParent(l) ==
    /\ seed[l] = "queued"
    /\ Admits(l, SetParentInterleaves)
    /\ seed' = [seed EXCEPT ![l] = "admitted"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, ch, lg, overflowed, cascaded, linked, dep, deacts>>

\* SetParentAsync waits for the leaf's split gate before it persists.
SetParentTakeGate(l) ==
    /\ seed[l] = "admitted"
    /\ gate[l] = "free"
    /\ gate' = [gate EXCEPT ![l] = "seed"]
    /\ seed' = [seed EXCEPT ![l] = "writing"]
    /\ UNCHANGED <<fg, cap, cons, sib, ch, lg, overflowed, cascaded, linked, dep, deacts>>

SetParentFinish(l) ==
    /\ seed[l] = "writing"
    /\ gate' = [gate EXCEPT ![l] = "free"]
    /\ seed' = [seed EXCEPT ![l] = "digest"]
    /\ UNCHANGED <<fg, cap, cons, sib, ch, lg, overflowed, cascaded, linked, dep, deacts>>

ServeDigest(l) ==
    /\ seed[l] = "digest"
    /\ Admits(l, DigestInterleaves)
    /\ seed' = [seed EXCEPT ![l] = "done"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, ch, lg, overflowed, cascaded, linked, dep, deacts>>

\* ---------------------------------------------------------------------------
\* Deactivation. A deactivating activation admits no call, interleaving or not,
\* and finishes once its running requests have; the next call activates afresh.

Deactivate(l) ==
    /\ dep[l] = "live"
    /\ deacts[l] = 0
    /\ DeactivationOnlyWhenIdle => ~Running(l)
    /\ dep' = [dep EXCEPT ![l] = "deactivating"]
    /\ deacts' = [deacts EXCEPT ![l] = 1]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked>>

FinishDeactivation(l) ==
    /\ dep[l] = "deactivating"
    /\ ~Running(l)
    /\ dep' = [dep EXCEPT ![l] = "live"]
    /\ UNCHANGED <<fg, cap, cons, gate, sib, ch, lg, seed, overflowed, cascaded, linked, deacts>>

Stutter == UNCHANGED vars

\* Every step of work already in flight; the Begin actions and Deactivate are
\* the environment.
InFlightStep ==
    \/ ForegroundTakeGate
    \/ ForegroundCallSibling
    \/ ForegroundSiblingDone
    \/ ForegroundSkipNested
    \/ ForegroundNestedLink
    \/ ForegroundNestedDone
    \/ ForegroundRelease
    \/ \E l \in Leaves : CaptureTakeGate(l) \/ CaptureGateBusy(l) \/ CaptureLinkReturns(l)
    \/ CaptureCallSibling
    \/ CaptureSiblingDone
    \/ CaptureSplitC
    \/ AdmitSibling
    \/ SiblingTakeGate
    \/ SiblingFinish
    \/ AdmitConsolidation
    \/ ConsolidationTakeGate
    \/ ConsolidationFinish
    \/ \E o \in Origins :
        \/ RootTakeLinkGate(o)
        \/ RootDeferLink(o)
        \/ RootDrainDeferred(o)
        \/ ParentAccept(o)
        \/ ParentAcceptNoOverflow(o)
        \/ SeedComplete(o)
        \/ GrandparentCascade(o)
        \/ GrandparentAbsorbs(o)
        \/ CascadeSeeded(o)
        \/ ReturnLink(o)
    \/ \E l \in Leaves :
        \/ AdmitSetParent(l)
        \/ SetParentTakeGate(l)
        \/ SetParentFinish(l)
        \/ ServeDigest(l)
        \/ FinishDeactivation(l)

\* Whenever work is in flight, some of it can move: no set of turns waits only
\* on itself.
NoWaitCycle == ~Settled => ENABLED InFlightStep

\* The environment (the Begin actions and Deactivate) together with InFlightStep,
\* listed one action per line.
Next ==
    \/ BeginForeground
    \/ ForegroundTakeGate
    \/ ForegroundCallSibling
    \/ ForegroundSiblingDone
    \/ ForegroundSkipNested
    \/ ForegroundNestedLink
    \/ ForegroundNestedDone
    \/ ForegroundRelease
    \/ \E l \in Leaves : BeginCapture(l)
    \/ \E l \in Leaves : CaptureTakeGate(l)
    \/ \E l \in Leaves : CaptureGateBusy(l)
    \/ CaptureCallSibling
    \/ CaptureSiblingDone
    \/ CaptureSplitC
    \/ \E l \in Leaves : CaptureLinkReturns(l)
    \/ AdmitSibling
    \/ SiblingTakeGate
    \/ SiblingFinish
    \/ BeginConsolidation
    \/ AdmitConsolidation
    \/ ConsolidationTakeGate
    \/ ConsolidationFinish
    \/ \E o \in Origins : RootTakeLinkGate(o)
    \/ \E o \in Origins : RootDeferLink(o)
    \/ \E o \in Origins : RootDrainDeferred(o)
    \/ \E o \in Origins : ParentAccept(o)
    \/ \E o \in Origins : ParentAcceptNoOverflow(o)
    \/ \E o \in Origins : SeedComplete(o)
    \/ \E o \in Origins : GrandparentCascade(o)
    \/ \E o \in Origins : GrandparentAbsorbs(o)
    \/ \E o \in Origins : CascadeSeeded(o)
    \/ \E o \in Origins : ReturnLink(o)
    \/ \E l \in Leaves : AdmitSetParent(l)
    \/ \E l \in Leaves : SetParentTakeGate(l)
    \/ \E l \in Leaves : SetParentFinish(l)
    \/ \E l \in Leaves : ServeDigest(l)
    \/ \E l \in Leaves : Deactivate(l)
    \/ \E l \in Leaves : FinishDeactivation(l)
    \/ Stutter

\* Every split that reaches the root is eventually linked.
EveryLinkLands == \A o \in Origins : ch[o] = "root" ~> linked[o]

\* Every progressing step lies in InFlightStep, and the only behaviours that do
\* not settle are stuttering ones: no in-flight action can repeat forever, because
\* each chain, turn and deactivation runs once. NoWaitCycle keeps InFlightStep
\* enabled while work is unsettled, so weak fairness on it alone drives every
\* link home.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(InFlightStep)
====
