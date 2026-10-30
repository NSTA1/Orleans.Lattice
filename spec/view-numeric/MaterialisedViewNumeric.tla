---------------- MODULE MaterialisedViewNumeric ----------------
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

\* An independent numeric (Count/Sum) projection over a fixed source script.
\* The script exercises create, in-place update, regroup, reserved-group
\* rejection, filter exit, a zero-valued Sum member and delete, through
\* ordinary writes and two overlapping two-key atomic batches. The environment
\* decides each batch, changes the projection definition, swaps the source's
\* physical copy, loses responses or crashes the maintainer, and asks for a
\* reconcile rebuild. No schema component is installed.
CONSTANTS Kind, Topology, MaxFaults, MaxReconciles

\* Topology selects the bounded topology change: the projection's filter
\* definition changes (FilterChange) or the source's physical copy is swapped
\* (CopySwap).
FilterChange == 1
CopySwap == 2
Keys == {1, 2}
Groups == {"A", "B"}
Units == 1..5
Batches == {3, 5}
\* Unit 1 creates key 1 in A; unit 2 creates key 2 in A; batch 3 regroups
\* key 1 to B and updates key 2 in place; unit 4 moves key 2 to a reserved
\* group; batch 5 deletes key 1 and recreates key 2 in B with numeric 0.
UnitEvents(u) == CASE u = 1 -> {1} [] u = 2 -> {2} [] u = 3 -> {3, 4}
                   [] u = 4 -> {5} [] OTHER -> {6, 7}
IsBatch(u) == u \in Batches
UnitOf(e) == CHOOSE u \in Units : e \in UnitEvents(u)
Key(e) == IF e \in {1, 3, 6} THEN 1 ELSE 2
EventFor(u, k) == CHOOSE e \in UnitEvents(u) : Key(e) = k
OnlyEvent(u) == CHOOSE e \in UnitEvents(u) : TRUE
Op(e) == IF e = 6 THEN "del" ELSE "set"
Grp(e) == CASE e \in {1, 2, 4} -> "A" [] e \in {3, 7} -> "B" [] OTHER -> "R"
Num(e) == CASE e = 1 -> 2 [] e = 2 -> 3 [] e = 3 -> 4 [] e = 4 -> 1
            [] e = 5 -> 2 [] OTHER -> 0
\* Definition 1 admits every value; definition 2 filters out numeric 1.
Pass(e, d) == d = 1 \/ Num(e) # 1
None == <<"none", 0>>
\* Extraction from the event's own kind and bytes. A delete, a reserved group
\* key or a filter exit contributes nothing, so it retracts any prior member.
Extract(e, d) == IF e = 0 \/ Op(e) = "del" \/ Grp(e) = "R" \/ ~Pass(e, d)
                 THEN None ELSE <<Grp(e), Num(e)>>
Hlc(e) == IF e = 0 THEN 0 ELSE UnitOf(e)
Absent == -1
Contrib(m, k, g) == IF m[k][1] = g THEN 1 ELSE 0
Agg(m) == [g \in Groups |->
    [cnt |-> Contrib(m, 1, g) + Contrib(m, 2, g),
     sum |-> Contrib(m, 1, g) * m[1][2] + Contrib(m, 2, g) * m[2][2]]]
Enc(s) == IF s.cnt = 0 THEN Absent ELSE IF Kind = "Count" THEN s.cnt ELSE s.sum
Pub(s) == [g \in Groups |-> Enc(s[g])]
Retract(s, c) == IF c = None THEN s
    ELSE [s EXCEPT ![c[1]] = [cnt |-> @.cnt - 1, sum |-> @.sum - c[2]]]
Add(s, c) == IF c = None THEN s
    ELSE [s EXCEPT ![c[1]] = [cnt |-> @.cnt + 1, sum |-> @.sum + c[2]]]
Touched(old, new) == {c[1] : c \in {old, new} \ {None}}
EmptyMembers == [k \in Keys |-> None]
NoIntent == [k \in Keys |-> {}]
OrdinaryId(ep, k, h) == <<"o", ep, k, h>>
TxId(u) == <<"t", 0, 0, u>>

VARIABLES appended, logs, phys, decision, src, defn,
          bound, viewDefn, epoch, read, checkpoint, phase,
          mem, slots, pub, intent, registry, conflict, faults, reconciles
vars == <<appended, logs, phys, decision, src, defn,
          bound, viewDefn, epoch, read, checkpoint, phase,
          mem, slots, pub, intent, registry, conflict, faults, reconciles>>

Registered(id) == \E r \in registry : r.id = id
BL == logs[bound]
Live == bound = phys /\ viewDefn = defn
Current == BL[read + 1]
Hi == IF read < Len(BL) THEN read + 1 ELSE read
\* Source keys of the units the current pass has read since the checkpoint,
\* the current unit included: the keys whose durable obligation it repairs.
KeysRead == {Key(e) : e \in UNION {UnitEvents(BL[i]) : i \in (checkpoint + 1)..Hi}}
Expected == [k \in Keys |-> Extract(src[k], defn)]
\* The source already holds a newer last-writer for the event's key, so its
\* contribution is dropped rather than regressing the member (#4885).
Superseded(e) == Hlc(src[Key(e)]) > Hlc(e)
Quiescent == Live /\ phase = "idle" /\ read = Len(BL) /\ intent = NoIntent

\* The scan applies each key's current winner under the rebuilt generation's
\* identity. A contribution whose operation id a retained saga already holds
\* is deduplicated away, so the scan would lose it.
Scanned(ep) == [k \in Keys |->
    IF Extract(src[k], defn) = None \/ Registered(OrdinaryId(ep, k, Hlc(src[k])))
    THEN None ELSE Extract(src[k], defn)]
Rebuild(ep) ==
    LET m == Scanned(ep) IN
    /\ epoch' = ep
    /\ mem' = m /\ slots' = Agg(m) /\ pub' = Pub(Agg(m)) /\ intent' = NoIntent
    /\ registry' = registry \cup
         {[id |-> OrdinaryId(ep, k, Hlc(src[k])), old |-> None, new |-> m[k]] :
             k \in {j \in Keys : m[j] # None}}
    /\ read' = Len(logs[phys]) /\ checkpoint' = Len(logs[phys])
    /\ bound' = phys /\ viewDefn' = defn /\ phase' = "idle"

Init ==
    /\ appended = 0 /\ logs = [p \in 1..2 |-> <<>>] /\ phys = 1
    /\ decision = [u \in Batches |-> "none"] /\ src = [k \in Keys |-> 0]
    /\ defn = 1
    /\ bound = 1 /\ viewDefn = 1 /\ epoch = 0 /\ read = 0 /\ checkpoint = 0
    /\ phase = "idle" /\ mem = EmptyMembers /\ slots = Agg(EmptyMembers)
    /\ pub = Pub(Agg(EmptyMembers)) /\ intent = NoIntent /\ registry = {}
    /\ conflict = FALSE /\ faults = 0 /\ reconciles = 0

\* The source commits the next ordinary write, or decides the next atomic
\* batch; only a committed batch changes the last-writer winners.
Write ==
    /\ appended < 5
    /\ LET u == appended + 1 IN
       /\ appended' = u
       /\ logs' = [logs EXCEPT ![phys] = Append(@, u)]
       /\ IF IsBatch(u)
          THEN \E d \in {"commit", "abort"} :
                 /\ decision' = [decision EXCEPT ![u] = d]
                 /\ src' = IF d = "commit"
                           THEN [k \in Keys |-> EventFor(u, k)] ELSE src
          ELSE /\ decision' = decision
               /\ src' = [src EXCEPT ![Key(OnlyEvent(u))] = OnlyEvent(u)]
    /\ UNCHANGED <<phys, defn, bound, viewDefn, epoch, read, checkpoint, phase,
         mem, slots, pub, intent, registry, conflict, faults, reconciles>>

\* The operator changes the projection's filter, which changes its version.
ChangeFilter ==
    /\ Topology = FilterChange /\ defn = 1 /\ defn' = 2
    /\ UNCHANGED <<appended, logs, phys, decision, src, bound, viewDefn, epoch,
         read, checkpoint, phase, mem, slots, pub, intent, registry, conflict,
         faults, reconciles>>

\* A resize, reshard or restore swaps the source's physical copy. The copy
\* holds the same rows; later writes reach the new copy's log.
Swap ==
    /\ Topology = CopySwap /\ phys = 1 /\ phys' = 2
    /\ UNCHANGED <<appended, logs, decision, src, defn, bound, viewDefn, epoch,
         read, checkpoint, phase, mem, slots, pub, intent, registry, conflict,
         faults, reconciles>>

\* A lost flip response, a failed group write, a failed intent retirement or
\* a crash abandons the pass. Durable rows survive; the pass replays from the
\* checkpoint.
Fail ==
    /\ faults < MaxFaults /\ (read # checkpoint \/ phase # "idle")
    /\ read' = checkpoint /\ phase' = "idle" /\ faults' = faults + 1
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, checkpoint, mem, slots, pub, intent, registry, conflict, reconciles>>

\* Apply one ordinary contribution: the key's prior obligation is complete, a
\* source-superseded write or an exactly installed membership is skipped,
\* otherwise membership, both slots and the affected-group intent flip together
\* under a deterministic operation id.
Flip ==
    /\ Live /\ phase = "idle" /\ read < Len(BL) /\ ~IsBatch(Current)
    /\ LET e == OnlyEvent(Current)
           k == Key(e)
           new == Extract(e, viewDefn)
           old == mem[k]
           id == OrdinaryId(epoch, k, Hlc(e))
       IN /\ intent[k] = {}
          /\ IF Superseded(e) \/ old = new
             THEN UNCHANGED <<mem, slots, intent, registry, conflict>>
             ELSE IF Registered(id)
                  THEN /\ conflict' = (conflict \/ ~\E r \in registry :
                                         r.id = id /\ r.old = old /\ r.new = new)
                       /\ UNCHANGED <<mem, slots, intent, registry>>
                  ELSE /\ mem' = [mem EXCEPT ![k] = new]
                       /\ slots' = Add(Retract(slots, old), new)
                       /\ intent' = [intent EXCEPT ![k] = Touched(old, new)]
                       /\ registry' = registry \cup
                            {[id |-> id, old |-> old, new |-> new]}
                       /\ UNCHANGED conflict
    /\ phase' = "flipped"
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, read, checkpoint, pub, faults, reconciles>>

\* Rematerialise one affected group from its slots, for a key this pass read.
Publish(k, g) ==
    /\ Live /\ k \in KeysRead /\ g \in intent[k]
    /\ pub' = [pub EXCEPT ![g] = Enc(slots[g])]
    /\ intent' = [intent EXCEPT ![k] = @ \ {g}]
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, read, checkpoint, phase, mem, slots, registry, conflict, faults,
         reconciles>>

\* Move past the ordinary unit once its key's obligation is retired.
Advance ==
    /\ Live /\ phase = "flipped" /\ intent[Key(OnlyEvent(Current))] = {}
    /\ read' = read + 1 /\ phase' = "idle"
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, checkpoint, mem, slots, pub, intent, registry, conflict, faults,
         reconciles>>

\* Flip a decided batch's captured net slice - membership, slots and public
\* groups of both keys, prior obligations included - as one atomic write keyed
\* by the batch. A member the source already superseded keeps its current
\* membership. An aborted or already-flipped batch changes nothing.
FlushBatch ==
    /\ Live /\ phase = "idle" /\ read < Len(BL) /\ IsBatch(Current)
    /\ LET u == Current
           m1 == [k \in Keys |-> IF Superseded(EventFor(u, k)) THEN mem[k]
                                 ELSE Extract(EventFor(u, k), viewDefn)]
           s1 == Add(Retract(Add(Retract(slots, mem[1]), m1[1]), mem[2]), m1[2])
           aff == UNION {Touched(mem[k], m1[k]) \cup intent[k] : k \in Keys}
       IN IF decision[u] = "commit" /\ ~Registered(TxId(u))
          THEN /\ mem' = m1 /\ slots' = s1
               /\ pub' = [g \in Groups |-> IF g \in aff THEN Enc(s1[g]) ELSE pub[g]]
               /\ intent' = NoIntent
               /\ registry' = registry \cup
                    {[id |-> TxId(u), old |-> None, new |-> None]}
          ELSE UNCHANGED <<mem, slots, pub, intent, registry>>
    /\ read' = read + 1
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, checkpoint, phase, conflict, faults, reconciles>>

\* Bank the pass only after every affected group is republished.
Checkpoint ==
    /\ Live /\ phase = "idle" /\ checkpoint # read /\ intent = NoIntent
    /\ checkpoint' = read
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, bound, viewDefn,
         epoch, read, phase, mem, slots, pub, intent, registry, conflict, faults,
         reconciles>>

\* A changed projection version or a swapped physical copy rebuilds from the
\* current source under a fresh generation, bound to the current copy.
Rebind ==
    /\ ~Live
    /\ Rebuild(epoch + 1)
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, conflict, faults,
         reconciles>>

\* An operator reconcile (or fall-off, or range delete) rebuilds a live view.
Reconcile ==
    /\ Live /\ reconciles < MaxReconciles
    /\ Rebuild(epoch + 1)
    /\ reconciles' = reconciles + 1
    /\ UNCHANGED <<appended, logs, phys, decision, src, defn, conflict, faults>>

Stutter == UNCHANGED vars

Next ==
    \/ Write
    \/ ChangeFilter
    \/ Swap
    \/ Fail
    \/ Flip
    \/ \E k \in Keys, g \in Groups : Publish(k, g)
    \/ Advance
    \/ FlushBatch
    \/ Checkpoint
    \/ Rebind
    \/ Reconcile
    \/ Stutter

Spec == Init /\ [][Next]_vars
     /\ WF_vars(Flip) /\ WF_vars(\E k \in Keys, g \in Groups : Publish(k, g))
     /\ WF_vars(Advance) /\ WF_vars(FlushBatch) /\ WF_vars(Checkpoint)
     /\ WF_vars(Rebind)

TypeOK ==
    /\ appended \in 0..5 /\ phys \in 1..2 /\ bound \in 1..2
    /\ defn \in 1..2 /\ viewDefn \in 1..2
    /\ decision \in [Batches -> {"none", "commit", "abort"}]
    /\ src \in [Keys -> 0..7] /\ epoch \in Nat
    /\ read \in 0..Len(BL) /\ checkpoint \in 0..read
    /\ phase \in {"idle", "flipped"}
    /\ mem \in [Keys -> {None} \cup (Groups \X (0..4))]
    /\ slots \in [Groups -> [cnt : 0..2, sum : 0..8]]
    /\ intent \in [Keys -> SUBSET Groups]
    /\ conflict \in BOOLEAN /\ faults \in 0..MaxFaults
\* Contribution arithmetic: the slots are the retract/add fold of membership.
SlotsMatchMembership == slots = Agg(mem)
\* Publication obligation: a public group that differs from its slots is
\* covered by a durable intent naming it.
IntentCoversStale == \A g \in Groups :
    pub[g] # Enc(slots[g]) => \E k \in Keys : g \in intent[k]
\* Idempotency identity: no operation id is resubmitted with another payload.
NoIdentityConflict == ~conflict
\* Extraction and encoding: once a pass catches up, membership is the
\* extraction of each key's source winner and every group is published as its
\* Count or Sum, absent exactly when it has no member.
QuiescentExact == Quiescent => mem = Expected /\ pub = Pub(Agg(Expected))
\* An aborted batch never reaches the view.
AbortedContributesNothing == \A u \in Batches :
    decision[u] = "abort" => ~Registered(TxId(u))
\* The checkpoint never advances past an unretired publication obligation.
CheckpointAfterPublication ==
    [][checkpoint' # checkpoint => intent' = NoIntent]_vars
\* Once the script is written and faults and topology changes are spent, the
\* view converges on the extraction of the final source state.
Converges == appended = 5 ~> (Quiescent /\ mem = Expected
                              /\ pub = Pub(Agg(Expected)))
\* Every durable publication obligation is eventually retired.
ObligationsRetire == \A k \in Keys, g \in Groups :
    g \in intent[k] ~> g \notin intent[k]
==============================================================
