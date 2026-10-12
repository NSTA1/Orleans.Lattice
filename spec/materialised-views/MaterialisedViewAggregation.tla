---------------- MODULE MaterialisedViewAggregation ----------------
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

\* The admission contract is checked separately by staging/source-confirmation
\* detectors. Numeric replacement is not the accumulative/history contract.
CONSTANTS OrdinaryDeletes, MaxCrashes
Keys == {1, 2}
Groups == {1, 2}
Events == {1, 2, 3}
Batch == {2, 3}
Key(e) == IF e = 3 THEN 2 ELSE 1
Group(e) == IF e = 2 THEN 2 ELSE 1
Value(e) == IF e = 0 THEN 1 ELSE IF e = 2 THEN 2 ELSE 3
Present(e, k) == IF e = 0 THEN k = 1
                 ELSE ~(e = 1 /\ OrdinaryDeletes)
EmptyMembers == [k \in Keys |-> 0]

VARIABLES stamps, log, outcome, source, read, checkpoint, staged,
          members, counts, sums, materialised, dirty, history, seen,
          seed, rebuilding, shadow, floor, crashes, rebuilt, omissionSafe
vars == <<stamps, log, outcome, source, read, checkpoint, staged,
          members, counts, sums, materialised, dirty, history, seen,
          seed, rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

Stamp(e) == IF e = 0 THEN 0 ELSE stamps[e]
Count(m, g) == Cardinality({k \in Keys :
    Present(m[k], k) /\ Group(m[k]) = g})
Sum(m, g) == (IF Present(m[1], 1) /\ Group(m[1]) = g
             THEN Value(m[1]) ELSE 0)
          + (IF Present(m[2], 2) /\ Group(m[2]) = g
             THEN Value(m[2]) ELSE 0)
Counts(m) == [g \in Groups |-> Count(m, g)]
Sums(m) == [g \in Groups |-> Sum(m, g)]
Replace(m, e) == [m EXCEPT ![Key(e)] = e]
Eligible(e, m) == e = source[Key(e)] /\ Stamp(e) >= Stamp(m[Key(e)])
FoldOne(m, e) == IF Eligible(e, m) THEN Replace(m, e) ELSE m
FoldBatch(m) == FoldOne(FoldOne(m, 2), 3)
CountDelta(m, e, g) ==
    (IF Present(e, Key(e)) /\ Group(e) = g THEN 1 ELSE 0)
  - (IF Present(m[Key(e)], Key(e)) /\ Group(m[Key(e)]) = g THEN 1 ELSE 0)
SumDelta(m, e, g) ==
    (IF Present(e, Key(e)) /\ Group(e) = g THEN Value(e) ELSE 0)
  - (IF Present(m[Key(e)], Key(e)) /\ Group(m[Key(e)]) = g
     THEN Value(m[Key(e)]) ELSE 0)
NextCounts(m, c, e) == IF Eligible(e, m)
    THEN [g \in Groups |-> c[g] + CountDelta(m, e, g)] ELSE c
NextSums(m, s, e) == IF Eligible(e, m)
    THEN [g \in Groups |-> s[g] + SumDelta(m, e, g)] ELSE s
ReplacementVisible == \A e \in Batch :
    Eligible(e, members) \/ (members[Key(e)] = source[Key(e)]
                            /\ materialised = Sums(members))
Touched(m, e) == IF Eligible(e, m)
    THEN {Group(e)} \cup (IF Present(m[Key(e)], Key(e))
                          THEN {Group(m[Key(e)])} ELSE {})
    ELSE {}
Seed(s) == {<<k, s[k]>> : k \in {j \in Keys : Present(s[j], j)}}
Revision(es) == {<<Key(e), e>> : e \in es}
Written == {log[i] : i \in 1..Len(log)}
ExpectedSource(k) ==
    LET candidates == {0} \cup {e \in Written :
        Key(e) = k /\ (e = 1 \/ outcome = "commit")}
    IN CHOOSE e \in candidates : \A other \in candidates : Stamp(e) >= Stamp(other)

Init ==
    /\ stamps \in [Events -> 1..3]
    /\ \A a, b \in Events : a # b => stamps[a] # stamps[b]
    /\ log = <<>> /\ outcome = "pending" /\ source = EmptyMembers
    /\ read = 0 /\ checkpoint = 0 /\ staged = {}
    /\ members = EmptyMembers /\ counts = Counts(members)
    /\ sums = Sums(members) /\ materialised = sums /\ dirty = {}
    /\ seed = Seed(source) /\ history = seed /\ seen = {}
    /\ rebuilding = FALSE /\ shadow = source /\ floor = 0
    /\ crashes = 0 /\ rebuilt = FALSE /\ omissionSafe = TRUE

AppendOrdinary ==
    /\ 1 \notin Written
    /\ log' = Append(log, 1)
    /\ source' = IF Stamp(1) > Stamp(source[1])
                  THEN Replace(source, 1) ELSE source
    /\ UNCHANGED <<stamps, outcome, read, checkpoint, staged, members,
         counts, sums, materialised, dirty, history, seen, seed,
         rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

AppendPrepare ==
    \E e \in Batch :
    /\ e \notin Written
    /\ log' = Append(log, e)
    /\ UNCHANGED <<stamps, outcome, source, read, checkpoint, staged,
         members, counts, sums, materialised, dirty, history, seen, seed,
         rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

Decide ==
    /\ Batch \subseteq Written /\ outcome = "pending"
    /\ outcome' \in {"commit", "abort"}
    /\ source' = IF outcome' = "commit"
       THEN [k \in Keys |-> LET e == IF k = 1 THEN 2 ELSE 3 IN
          IF Stamp(e) > Stamp(source[k]) THEN e ELSE source[k]]
       ELSE source
    /\ UNCHANGED <<stamps, log, read, checkpoint, staged, members, counts,
         sums, materialised, dirty, history, seen, seed, rebuilding,
         shadow, floor, crashes, rebuilt, omissionSafe>>

ReadOrdinary ==
    /\ ~rebuilding /\ read < Len(log) /\ log[read + 1] = 1 /\ dirty = {}
    /\ members' = FoldOne(members, 1)
    /\ counts' = NextCounts(members, counts, 1)
    /\ sums' = NextSums(members, sums, 1)
    /\ dirty' = Touched(members, 1)
    /\ history' = history \cup Revision({1})
    /\ seen' = seen \cup {1} /\ read' = read + 1
    /\ UNCHANGED <<stamps, log, outcome, source, checkpoint, staged,
         materialised, seed, rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

Stage ==
    /\ ~rebuilding /\ read < Len(log) /\ log[read + 1] \in Batch
    /\ staged' = staged \cup {log[read + 1]} /\ read' = read + 1
    /\ UNCHANGED <<stamps, log, outcome, source, checkpoint, members,
         counts, sums, materialised, dirty, history, seen, seed,
         rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

Flush ==
    /\ ~rebuilding /\ staged = Batch /\ outcome # "pending" /\ dirty = {}
    /\ outcome = "abort" \/ ReplacementVisible
    /\ members' = IF outcome = "commit" THEN FoldBatch(members) ELSE members
    /\ counts' = IF outcome = "commit"
       THEN NextCounts(FoldOne(members, 2), NextCounts(members, counts, 2), 3)
       ELSE counts
    /\ sums' = IF outcome = "commit"
       THEN NextSums(FoldOne(members, 2), NextSums(members, sums, 2), 3)
       ELSE sums
    /\ omissionSafe' = (outcome = "abort" \/ ReplacementVisible)
    /\ materialised' = IF outcome = "commit" THEN Sums(members') ELSE materialised
    /\ dirty' = {}
    /\ history' = IF outcome = "commit"
                  THEN history \cup Revision(Batch) ELSE history
    /\ seen' = IF outcome = "commit" THEN seen \cup Batch ELSE seen
    /\ staged' = {}
    /\ UNCHANGED <<stamps, log, outcome, source, read, checkpoint,
         seed, rebuilding, shadow, floor, crashes, rebuilt>>

Materialise ==
    \E g \in Groups :
    /\ ~rebuilding /\ g \in dirty
    /\ materialised' = [materialised EXCEPT ![g] = sums[g]]
    /\ dirty' = dirty \ {g}
    /\ UNCHANGED <<stamps, log, outcome, source, read, checkpoint, staged,
         members, counts, sums, history, seen, seed, rebuilding, shadow,
         floor, crashes, rebuilt, omissionSafe>>

Checkpoint ==
    /\ ~rebuilding /\ dirty = {} /\ staged = {} /\ checkpoint # read
    /\ checkpoint' = read
    /\ UNCHANGED <<stamps, log, outcome, source, read, staged, members,
         counts, sums, materialised, dirty, history, seen, seed,
         rebuilding, shadow, floor, crashes, rebuilt, omissionSafe>>

Crash ==
    /\ crashes < MaxCrashes
    /\ read' = checkpoint /\ staged' = {} /\ rebuilding' = FALSE
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<stamps, log, outcome, source, checkpoint, members,
         counts, sums, materialised, dirty, history, seen, seed,
         shadow, floor, rebuilt, omissionSafe>>

StartRebuild ==
    /\ ~rebuilding /\ ~rebuilt
    \* Capture before scanning. A prepare may be invisible to the scan.
    /\ floor' = checkpoint
    /\ shadow' = source /\ rebuilding' = TRUE
    /\ UNCHANGED <<stamps, log, outcome, source, read, checkpoint, staged,
         members, counts, sums, materialised, dirty, history, seen, seed,
         crashes, rebuilt, omissionSafe>>

InstallRebuild ==
    /\ rebuilding
    /\ members' = shadow /\ counts' = Counts(shadow) /\ sums' = Sums(shadow)
    /\ materialised' = Sums(shadow) /\ dirty' = {}
    /\ seed' = Seed(shadow) /\ history' = seed' /\ seen' = {}
    /\ read' = floor /\ checkpoint' = floor /\ staged' = {}
    /\ rebuilding' = FALSE /\ rebuilt' = TRUE
    /\ UNCHANGED <<stamps, log, outcome, source, shadow, floor, crashes, omissionSafe>>

Stutter == UNCHANGED vars

Next ==
    \/ AppendOrdinary
    \/ AppendPrepare
    \/ Decide
    \/ ReadOrdinary
    \/ Stage
    \/ Flush
    \/ Materialise
    \/ Checkpoint
    \/ Crash
    \/ StartRebuild
    \/ InstallRebuild
    \/ Stutter

Spec == Init /\ [][Next]_vars
     /\ WF_vars(ReadOrdinary) /\ WF_vars(Stage) /\ WF_vars(Flush)
     /\ WF_vars(Materialise) /\ WF_vars(Checkpoint)
     /\ WF_vars(InstallRebuild)

TypeOK ==
    /\ source \in [Keys -> 0..3] /\ members \in [Keys -> 0..3]
    /\ read \in 0..Len(log) /\ checkpoint \in 0..read
    /\ staged \subseteq Batch /\ seen \subseteq Events
    /\ dirty \subseteq Groups
    /\ counts \in [Groups -> Int] /\ sums \in [Groups -> Int]
MembershipAccumulatorAtomic == counts = Counts(members) /\ sums = Sums(members)
SourceMatchesDecision == source = [k \in Keys |-> ExpectedSource(k)]
NoNegativeCounts == \A g \in Groups : counts[g] >= 0
HistoryKeepsSuperseded == history = seed \cup Revision(seen)
NoAbortedContribution == outcome = "abort" =>
    (\A k \in Keys : members[k] \notin Batch) /\ seen \cap Batch = {}
CheckpointRetainsStage == staged # {} =>
    \A i \in 1..Len(log) : log[i] \in staged => checkpoint < i
MaterialisedWhenClean == dirty = {} => materialised = sums
OmittedReplacementAlreadyVisible == omissionSafe
AggregateConverges ==
    (Written = Events /\ outcome # "pending") ~>
       (members = source /\ materialised = Sums(source))
HistoryConverges ==
    (Written = Events /\ outcome # "pending" /\ rebuilt) ~>
       (\A e \in (IF outcome = "commit" THEN Events ELSE {1}) :
          <<Key(e), e>> \in history \/ e \in {log[i] : i \in 1..floor})
==============================================================
