---------------- MODULE SchemaViewComposition ----------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* The bounded composition of the schema write path, version family and
\* rewrite (an eager migration or a transform remediation) with an
\* independent Count/Sum projection, by rely-guarantee rather than INSTANCE.
\* The schema side is modelled as the environment its checked modules
\* guarantee: a local raw write is judged before the WAL append and stamped at
\* the target; a refused write appends nothing; a target advance rewrites
\* nothing; a value written below the check keeps its source stamp; a rewrite
\* re-stamps every value through the destination copy's ordinary write path
\* and cuts over by moving the alias, or aborts touching nothing. The view
\* side is modelled at the level the numeric companion guarantees: per-key
\* replace membership, a rebuild from the bound copy on a changed definition
\* or copy, and replay from the checkpoint. What this module adds is the
\* projection input contract between them: Project receives the
\* envelope-stripped body at its stamped version, never upcast, on every
\* derivation path.
CONSTANTS Rewrite, StrictIngest, MaxRetries, MaxFaults, MaxReconciles

\* Rewrite selects the bounded rewrite: an eager version migration to the
\* target captured when it begins (Migrate), or a transform remediation that
\* decodes each value to the live target, transforms it and stamps it there
\* (Remediate).
Migrate == 1
Remediate == 2
Keys == {1, 2}
Groups == {"A", "B"}
Copies == {1, 2}
Versions == {1, 2}
Bodies == {"none", "p", "q", "r", "s", "x", "env"}
\* Units 1 to 4 are the scripted writes; the others are environment writes.
Units == 1..7
RetryUnit == 5
ReplicaUnit == 6
RewriteUnit == 7
Absent == [ver |-> 0, body |-> "none"]
None == <<"none", 0>>

\* A body token keeps its identity across versions and the upcaster is total;
\* what a version changes is the meaning of the shape. Version 2 regroups "p",
\* rescales the numeric of "q" and puts "r" outside the filter, so an upcast
\* changes a contribution's group, its numeric or its filter membership.
V1(b) == CASE b = "p" -> <<"A", 1>> [] b = "q" -> <<"A", 2>>
           [] b = "r" -> <<"B", 1>> [] b = "s" -> <<"B", 2>>
           [] OTHER -> <<"A", 1>>
V2(b) == CASE b = "p" -> <<"B", 1>> [] b = "q" -> <<"A", 3>>
           [] b = "s" -> <<"B", 2>> [] b = "r" -> None
           [] OTHER -> <<"A", 1>>
\* Projection definition 1 reads only the version-1 shape; definition 2 reads
\* both. A value it cannot read (absent, envelope bytes, or a version-2 body
\* under definition 1) contributes nothing.
Proj(d, v) == CASE v.ver = 1 -> V1(v.body)
                [] v.ver = 2 /\ d = 2 -> V2(v.body)
                [] OTHER -> None

\* The policy's accepting transform, the remediation transform, and validity.
Fix(b) == IF b = "s" THEN "q" ELSE b
T(b) == CASE b = "p" -> "q" [] b = "q" -> "q" [] OTHER -> "p"
Valid(b) == b # "x"
BatchBody(k) == IF k = 1 THEN "r" ELSE "p"
Decodable(v, t) == v.ver <= t

Entry(u, ks, vals, atomic, ok) ==
    [unit |-> u, keys |-> ks, vals |-> vals, atomic |-> atomic, commit |-> ok]
One(k, v) == [j \in Keys |-> IF j = k THEN v ELSE Absent]

Contrib(m, k, g) == IF m[k][1] = g THEN 1 ELSE 0
Agg(m) == [g \in Groups |->
    [cnt |-> Contrib(m, 1, g) + Contrib(m, 2, g),
     sum |-> Contrib(m, 1, g) * m[1][2] + Contrib(m, 2, g) * m[2][2]]]
Retract(s, c) == IF c = None THEN s
    ELSE [s EXCEPT ![c[1]] = [cnt |-> @.cnt - 1, sum |-> @.sum - c[2]]]
Add(s, c) == IF c = None THEN s
    ELSE [s EXCEPT ![c[1]] = [cnt |-> @.cnt + 1, sum |-> @.sum + c[2]]]
NoMembers == [k \in Keys |-> None]

VARIABLES appended, target, defn, phys, stored, wal, intended, submitted,
          mustRefuse, replicated, retried, rw, cap, promised,
          bound, viewDefn, genCopy, read, checkpoint, mem, slots, applied,
          faults, reconciles, barrier
vars == <<appended, target, defn, phys, stored, wal, intended, submitted,
          mustRefuse, replicated, retried, rw, cap, promised,
          bound, viewDefn, genCopy, read, checkpoint, mem, slots, applied,
          faults, reconciles, barrier>>
source == <<appended, target, defn, phys, stored, wal, intended, submitted,
            mustRefuse, replicated, retried, rw, cap, promised>>
view == <<bound, viewDefn, genCopy, read, checkpoint, mem, slots, applied,
          faults, reconciles, barrier>>

\* The projection input: the envelope-stripped body at its stamped version.
Input(v) == v
\* What a decoding client read returns: the body upcast to the live target.
Decode(v) == IF v.ver = 0 THEN v ELSE [ver |-> target, body |-> v.body]
\* The stored bytes with the version envelope still on them.
EnvelopeBytes(v) == IF v.ver = 0 THEN v ELSE [ver |-> 0, body |-> "env"]

BL == wal[bound]
Live == bound = phys /\ viewDefn = defn
\* A rewrite is building or awaiting cutover: writes are quiesced.
Busy == rw \in {"building", "built"}
Quiescent == Live /\ read = Len(BL)
Expected == [k \in Keys |-> Proj(defn, intended[k])]
Scan(c) == [k \in Keys |-> Proj(defn, Input(stored[c][k]))]
Rewritten(v, at) == IF v.ver = 0 THEN v
    ELSE IF Rewrite = Migrate THEN [ver |-> at, body |-> v.body]
    ELSE [ver |-> at, body |-> T(v.body)]
RewriteOf(v) == IF v.ver = 0 THEN v
    ELSE IF Rewrite = Migrate THEN [ver |-> promised, body |-> v.body]
    ELSE [ver |-> promised, body |-> T(v.body)]
RewriteEntry(k, at) ==
    IF stored[1][k].ver = 0 THEN <<>>
    ELSE <<Entry(RewriteUnit, {k}, One(k, Rewritten(stored[1][k], at)), FALSE, TRUE)>>

Init ==
    /\ appended = 0 /\ target = 1 /\ defn = 1 /\ phys = 1
    /\ stored = [c \in Copies |-> [k \in Keys |-> Absent]]
    /\ wal = [c \in Copies |-> <<>>]
    /\ intended = [k \in Keys |-> Absent]
    /\ submitted = [k \in Keys |-> "none"]
    /\ mustRefuse = {} /\ replicated = FALSE /\ retried = 0
    /\ rw = "idle" /\ cap = 1 /\ promised = 1
    /\ bound = 1 /\ viewDefn = 1 /\ genCopy = 1 /\ read = 0 /\ checkpoint = 0
    /\ mem = NoMembers /\ slots = Agg(NoMembers) /\ applied = {}
    /\ faults = 0 /\ reconciles = 0 /\ barrier = "none"

\* A local raw write of body b to key k: judged before the append, so a
\* refused body appends nothing; an accepted one is transformed by the
\* policy and stamped at the target.
Submit(u, k, b) ==
    /\ submitted' = [submitted EXCEPT ![k] = b]
    /\ mustRefuse' = IF Valid(b) THEN mustRefuse ELSE mustRefuse \cup {u}
    /\ IF Valid(b)
       THEN /\ wal' = [wal EXCEPT ![phys] = Append(@,
                 Entry(u, {k}, One(k, [ver |-> target, body |-> Fix(b)]), FALSE, TRUE))]
            /\ stored' = [stored EXCEPT ![phys][k] = [ver |-> target, body |-> Fix(b)]]
            /\ intended' = [intended EXCEPT ![k] = [ver |-> target, body |-> Fix(b)]]
       ELSE UNCHANGED <<wal, stored, intended>>

\* A two-key atomic batch stamped at the target and decided by the
\* environment; only a committed batch changes the last-writer winners.
Batch(u, ok) ==
    LET vals == [k \in Keys |-> [ver |-> target, body |-> BatchBody(k)]] IN
    /\ wal' = [wal EXCEPT ![phys] = Append(@, Entry(u, Keys, vals, TRUE, ok))]
    /\ stored' = IF ok THEN [stored EXCEPT ![phys] = vals] ELSE stored
    /\ intended' = IF ok THEN vals ELSE intended
    /\ UNCHANGED <<submitted, mustRefuse>>

\* The source's scripted writes: key 1 gets "p"; key 2 gets "s", which the
\* policy transforms, or "x", which it refuses; then a two-key atomic batch.
\* No write lands while a rewrite is building.
Write ==
    /\ appended < 3 /\ ~Busy
    /\ appended' = appended + 1
    /\ CASE appended = 0 -> Submit(1, 1, "p")
         [] appended = 1 -> \E b \in {"s", "x"} : Submit(2, 2, b)
         [] OTHER -> \E ok \in BOOLEAN : Batch(3, ok)
    /\ UNCHANGED <<target, defn, phys, replicated, retried, rw, cap, promised>>
    /\ UNCHANGED view

\* A writer whose response was lost re-submits its last body for a key; the
\* re-submission is judged and stamped afresh at the target current then.
Retry ==
    /\ retried < MaxRetries /\ ~Busy
    /\ retried' = retried + 1
    /\ \E k \in Keys : submitted[k] # "none" /\ Submit(RetryUnit, k, submitted[k])
    /\ UNCHANGED <<appended, target, defn, phys, replicated, rw, cap, promised>>
    /\ UNCHANGED view

\* A replicated entry stamped at version 2 by its source arrives below the
\* local check. Under strict ingest it is judged on its decoded body, so a
\* stamp past the target is diverted; otherwise it is stored verbatim with
\* its source stamp.
Replicate ==
    /\ ~replicated /\ ~Busy
    /\ replicated' = TRUE
    /\ LET v == [ver |-> 2, body |-> "q"]
           diverted == StrictIngest /\ ~Decodable(v, target)
       IN /\ mustRefuse' = IF diverted THEN mustRefuse \cup {ReplicaUnit} ELSE mustRefuse
          /\ IF diverted
             THEN UNCHANGED <<wal, stored, intended>>
             ELSE /\ wal' = [wal EXCEPT ![phys] = Append(@,
                       Entry(ReplicaUnit, {1}, One(1, v), FALSE, TRUE))]
                  /\ stored' = [stored EXCEPT ![phys][1] = v]
                  /\ intended' = [intended EXCEPT ![1] = v]
    /\ UNCHANGED <<appended, target, defn, phys, submitted, retried, rw, cap, promised>>
    /\ UNCHANGED view

\* The operator advances the target to the registered version 2. Nothing is
\* rewritten and no view is touched.
Advance ==
    /\ target = 1 /\ target' = 2
    /\ UNCHANGED <<appended, defn, phys, stored, wal, intended, submitted,
         mustRefuse, replicated, retried, rw, cap, promised>>
    /\ UNCHANGED view

\* The operator replaces the projection with one that reads version 2,
\* which changes its version.
ChangeDefinition ==
    /\ defn = 1 /\ defn' = 2
    /\ UNCHANGED <<appended, target, phys, stored, wal, intended, submitted,
         mustRefuse, replicated, retried, rw, cap, promised>>
    /\ UNCHANGED view

\* A rewrite of copy 1 begins; a migration captures the target it re-stamps
\* at.
BeginRewrite ==
    /\ rw = "idle" /\ phys = 1
    /\ rw' = "building"
    /\ cap' = target
    /\ promised' = target
    /\ UNCHANGED <<appended, target, defn, phys, stored, wal, intended,
         submitted, mustRefuse, replicated, retried>>
    /\ UNCHANGED view

\* The rewrite reads every stored value of the captured copy in its stored
\* form. A value it cannot decode to its version aborts it with nothing
\* changed; otherwise every value is written through copy 2's ordinary write
\* path, re-stamped at that version.
BuildRewrite ==
    /\ rw = "building"
    /\ LET at == IF Rewrite = Migrate THEN cap ELSE target
       IN IF \E k \in Keys : ~Decodable(stored[1][k], at)
          THEN /\ rw' = "aborted"
               /\ UNCHANGED <<stored, wal, phys, intended, cap, promised>>
          ELSE /\ rw' = "built"
               /\ stored' = [stored EXCEPT ![2] = [k \in Keys |-> Rewritten(stored[1][k], at)]]
               /\ wal' = [wal EXCEPT ![2] = RewriteEntry(1, at) \o RewriteEntry(2, at)]
               /\ cap' = at
               /\ promised' = IF Rewrite = Migrate THEN promised ELSE target
               /\ UNCHANGED <<phys, intended>>
    /\ UNCHANGED <<appended, target, defn, submitted, mustRefuse, replicated, retried>>
    /\ UNCHANGED view

\* Cutover moves the alias to copy 2 only while it still names the captured
\* copy; readers then see every value at the rewrite's stamp.
Cutover ==
    /\ rw = "built" /\ phys = 1
    /\ phys' = 2 /\ rw' = "done"
    /\ intended' = [k \in Keys |-> RewriteOf(intended[k])]
    /\ UNCHANGED <<appended, target, defn, stored, wal, submitted, mustRefuse,
         replicated, retried, cap, promised>>
    /\ UNCHANGED view

\* Incremental apply of one ordinary entry: the key's membership is replaced
\* by the projection of the entry's input, and its slots move with it.
Apply ==
    /\ Live /\ read < Len(BL) /\ ~BL[read + 1].atomic
    /\ LET e == BL[read + 1]
           k == CHOOSE j \in e.keys : TRUE
           new == Proj(viewDefn, Input(e.vals[k]))
       IN /\ mem' = [mem EXCEPT ![k] = new]
          /\ slots' = Add(Retract(slots, mem[k]), new)
          /\ applied' = applied \cup {e.unit}
    /\ read' = read + 1
    /\ UNCHANGED <<bound, viewDefn, genCopy, checkpoint, faults, reconciles, barrier>>
    /\ UNCHANGED source

\* Atomic publication of a decided batch's net slice; an aborted batch
\* contributes nothing.
ApplyBatch ==
    /\ Live /\ read < Len(BL) /\ BL[read + 1].atomic
    /\ LET e == BL[read + 1]
           m1 == [k \in Keys |-> Proj(viewDefn, Input(e.vals[k]))]
       IN IF e.commit
          THEN /\ mem' = m1
               /\ slots' = Add(Retract(Add(Retract(slots, mem[1]), m1[1]), mem[2]), m1[2])
               /\ applied' = applied \cup {e.unit}
          ELSE UNCHANGED <<mem, slots, applied>>
    /\ read' = read + 1
    /\ UNCHANGED <<bound, viewDefn, genCopy, checkpoint, faults, reconciles, barrier>>
    /\ UNCHANGED source

\* A crash or lost response abandons the pass, which replays the retained
\* entries from the checkpoint at their own stamps. The checkpoint here is
\* the generation's floor; banking a pass further is the numeric companion's
\* obligation.
Fail ==
    /\ faults < MaxFaults /\ read # checkpoint
    /\ read' = checkpoint /\ faults' = faults + 1
    /\ UNCHANGED <<bound, viewDefn, genCopy, checkpoint, mem, slots, applied,
         reconciles, barrier>>
    /\ UNCHANGED source

\* A rebuild from one copy under the current definition: membership, slots,
\* cursor, checkpoint, binding and generation all name that copy.
Rebuild(m) ==
    /\ mem' = m
    /\ slots' = Agg(m)
    /\ read' = Len(wal[phys]) /\ checkpoint' = Len(wal[phys])
    /\ bound' = phys /\ viewDefn' = defn /\ genCopy' = phys
    /\ UNCHANGED <<applied, barrier>>

\* A changed definition or a moved alias rebuilds from the current copy.
Rebind ==
    /\ ~Live
    /\ Rebuild(Scan(phys))
    /\ UNCHANGED <<faults, reconciles>>
    /\ UNCHANGED source

\* An operator reconcile (or rebuild) of a live view.
Reconcile ==
    /\ Live /\ reconciles < MaxReconciles
    /\ Rebuild(Scan(phys))
    /\ reconciles' = reconciles + 1
    /\ UNCHANGED faults
    /\ UNCHANGED source

\* A reader's head barrier returns once the view is bound to the copy the
\* alias names, under the current definition, and has applied that copy's
\* every entry; the reader then sees the view as it stands. The barrier is
\* driven at most once, after a cutover, where the source identity matters;
\* elsewhere its result is QuiescentAtStampedVersion.
WaitHead ==
    /\ barrier = "none" /\ rw = "done"
    /\ bound = phys /\ viewDefn = defn /\ read = Len(wal[phys])
    /\ barrier' = IF mem = [k \in Keys |-> Proj(defn, stored[phys][k])]
                  THEN "exact" ELSE "stale"
    /\ UNCHANGED <<bound, viewDefn, genCopy, read, checkpoint, mem, slots, applied,
         faults, reconciles>>
    /\ UNCHANGED source

Stutter == UNCHANGED vars

Next ==
    \/ Write
    \/ Retry
    \/ Replicate
    \/ Advance
    \/ ChangeDefinition
    \/ BeginRewrite
    \/ BuildRewrite
    \/ Cutover
    \/ Apply
    \/ ApplyBatch
    \/ Fail
    \/ Rebind
    \/ Reconcile
    \/ WaitHead
    \/ Stutter

Spec == Init /\ [][Next]_vars
     /\ WF_vars(BuildRewrite) /\ WF_vars(Cutover)
     /\ WF_vars(Apply) /\ WF_vars(ApplyBatch)
     /\ WF_vars(Rebind)

Values == [ver : 0..2, body : Bodies]
Members == {None} \cup (Groups \X (1..3))
TypeOK ==
    /\ appended \in 0..3 /\ target \in Versions /\ defn \in 1..2
    /\ phys \in Copies
    /\ stored \in [Copies -> [Keys -> Values]]
    /\ intended \in [Keys -> Values]
    /\ \A c \in Copies : Len(wal[c]) <= 8
         /\ \A i \in 1..Len(wal[c]) : wal[c][i].unit \in Units
    /\ submitted \in [Keys -> Bodies]
    /\ mustRefuse \subseteq Units /\ applied \subseteq Units
    /\ replicated \in BOOLEAN /\ retried \in 0..MaxRetries
    /\ rw \in {"idle", "building", "built", "done", "aborted"}
    /\ cap \in Versions /\ promised \in Versions
    /\ bound \in Copies /\ genCopy \in Copies /\ viewDefn \in 1..2
    /\ read \in 0..8 /\ checkpoint \in 0..8
    /\ mem \in [Keys -> Members]
    /\ slots \in [Groups -> [cnt : 0..8, sum : 0..30]]
    /\ faults \in 0..MaxFaults /\ reconciles \in 0..MaxReconciles
    /\ barrier \in {"none", "exact", "stale"}

\* A head barrier that returns shows the reader the projection of the source
\* as it stands, whatever copy the view was bound to when the barrier began.
BarrierExact == barrier # "stale"

\* No source key is counted twice: the slots are the fold of membership,
\* across replays, retries, rebuilds and the cutover.
NoDuplicateContribution == slots = Agg(mem)
\* A write the schema layer must refuse (a refused local write or retry, or a
\* strict-ingest diversion) never reaches the view.
RejectedContributesNothing == applied \cap mustRefuse = {}
\* Every derivation path projects the stored value at its stamped version:
\* once caught up, membership is the projection of each key's stored winner
\* on the bound copy, never of an upcast or of the envelope bytes.
QuiescentAtStampedVersion ==
    Quiescent => mem = [k \in Keys |-> Proj(viewDefn, stored[bound][k])]
\* End to end: once caught up, each member is the projection of what the
\* write path promised - the accepted, transformed body at the stamp it was
\* given, or after a rewrite the rewritten body at the rewrite's stamp.
IntendedContribution == Quiescent => mem = Expected
\* Binding, generation, cursor and checkpoint name one physical copy.
BindingConsistent == genCopy = bound /\ checkpoint <= read /\ read <= Len(BL)
\* A target advance appends nothing, rewrites nothing and re-projects
\* nothing.
AdvanceReprojectsNothing ==
    [][target' # target => UNCHANGED <<wal, stored, mem, slots, read, checkpoint>>]_vars
\* An aborted rewrite changes no copy, no alias and no view.
AbortTouchesNothing ==
    [][rw' = "aborted" /\ rw # "aborted"
        => UNCHANGED <<wal, stored, phys, intended, mem, slots>>]_vars
Settled == appended = 3 /\ ~Busy
\* Once the script is written and no rewrite is in flight, the view
\* converges on the intended contributions.
Converges == Settled ~> (Quiescent /\ mem = Expected)
==============================================================
