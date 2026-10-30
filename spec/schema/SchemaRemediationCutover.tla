---- MODULE SchemaRemediationCutover ----
EXTENDS Naturals, FiniteSets

\* The copy-and-cutover protocol beneath a schema remediation (transform mode)
\* and an eager version migration: the durable coordinator reserves the tree's
\* alias, captures the physical copy the tree resolves to, runs a read-only dry
\* run, builds a fresh destination copy that inherits the source's layout, and
\* cuts over by installing the target policy, swapping the alias together with
\* the destination's map, and arming every source shard to redirect stale
\* routers. Phases are durable and slices bank their progress, so a crash
\* anywhere resumes idempotently. An abort or cancel publishes nothing; a
\* cutover whose captured source has been superseded refuses to publish.

CONSTANTS KeyCount, Versioned, MaxRems, MaxWrites, MaxCrashes

Keys == 1..KeyCount
Shards == {1, 2}
\* Copy 0 is the logical tree's own copy; copy 1 is an unrelated tree an
\* administrator may alias the tree onto; fresh copies are numbered from 2.
Orig == 0
Other == 1
MaxCopy == MaxRems + 2
Copies == 0..MaxCopy
NoCopy == MaxCopy + 1

\* A stored value: Abs is absent; otherwise a stamp (None for an unversioned
\* tree, Old or Tgt for a versioned one), whether the remediation can rewrite
\* it (an un-upcastable, untransformable or policy-violating value cannot),
\* and whether a transform has rewritten it.
Val(s, o, r) == [st |-> s, ok |-> o, rw |-> r]
Abs == [st |-> "Abs", ok |-> TRUE, rw |-> FALSE]
Stamps == {"None", "Old", "Tgt"}
Values == {Abs} \cup {Val(s, o, r) : s \in Stamps, o \in BOOLEAN, r \in BOOLEAN}
TargetStamp == IF Versioned THEN "Tgt" ELSE "None"
OldStamp == IF Versioned THEN "Old" ELSE "None"
\* What an ordinary write lands: stamped at the target by the write stage.
Landing == {Abs, Val(TargetStamp, TRUE, FALSE), Val(TargetStamp, FALSE, FALSE)}
Seed == {Abs, Val(OldStamp, TRUE, FALSE), Val(OldStamp, FALSE, FALSE)}

\* The per-value rewrite both modes apply: re-stamped at the target, and
\* rewritten by the transform in transform mode (#5243).
Rewrite(v, m) ==
    IF v = Abs THEN Abs ELSE Val(TargetStamp, TRUE, v.rw \/ m = "Transform")
Offends(v) == v # Abs /\ ~v.ok

Phases == {"Idle", "DryRun", "Build", "Cutover"}
Reports == {"None", "Completed", "Aborted", "Cancelled", "Superseded"}

VARIABLES
    phase, mode, cursor, pend, cstep, begin, prevPol,
    dest, srcPhys, destReg, inherited,
    reserved, alias, mapOf, shards, armed, policy, data,
    nextId, discarded, retired, report, abortKey,
    rems, writes, crashes, moved, resharded, explicit,
    dirty, late, last

vars == <<phase, mode, cursor, pend, cstep, begin, prevPol,
          dest, srcPhys, destReg, inherited,
          reserved, alias, mapOf, shards, armed, policy, data,
          nextId, discarded, retired, report, abortKey,
          rems, writes, crashes, moved, resharded, explicit,
          dirty, late, last>>

coord == <<phase, mode, cursor, pend, cstep, begin, prevPol, dest, srcPhys, destReg,
           inherited, report, abortKey>>
env == <<rems, writes, crashes, moved, resharded, explicit>>

\* The copy a write routed at copy c and shard s lands on: a redirect armed on
\* that shard forwards it, transitively, and the target routes it by its own
\* layout (a key on shard 2 of a two-shard copy is on shard 1 of a one-shard one).
RECURSIVE FollowN(_, _, _)
FollowN(c, s, n) ==
    IF n = 0 THEN c
    ELSE IF \E t \in Copies : <<c, s, t>> \in armed
         THEN LET t == CHOOSE t \in Copies : <<c, s, t>> \in armed IN
              FollowN(t, IF s \in shards[t] THEN s ELSE 1, n - 1)
         ELSE c
Follow(c, s) == FollowN(c, s, MaxCopy + 1)

TypeOK ==
    /\ phase \in Phases
    /\ mode \in {"None", "Transform", "Migrate"}
    /\ cursor \in 0..KeyCount
    /\ pend \in BOOLEAN
    /\ cstep \in 0..3
    /\ begin \in BOOLEAN
    /\ prevPol \in {"Old", "New"}
    /\ dest \in Copies \cup {NoCopy}
    /\ srcPhys \in Copies
    /\ destReg \in BOOLEAN
    /\ inherited \in SUBSET Shards
    /\ reserved \in {"None", "Rem"}
    /\ alias \in Copies /\ mapOf \in Copies
    /\ shards \in [Copies -> SUBSET Shards]
    /\ armed \subseteq Copies \X Shards \X Copies
    /\ policy \in {"Old", "New"}
    /\ data \in [Copies -> [Keys -> Values]]
    /\ nextId \in 2..(MaxCopy + 1)
    /\ discarded \subseteq Copies
    /\ retired \subseteq Copies
    /\ report \in Reports
    /\ abortKey \in 0..KeyCount
    /\ rems \in 0..MaxRems /\ writes \in 0..MaxWrites /\ crashes \in 0..MaxCrashes
    /\ moved \in BOOLEAN /\ resharded \in BOOLEAN
    /\ explicit \in {"None", "Admitted", "Done"}
    /\ dirty \in BOOLEAN /\ late \in BOOLEAN
    /\ last \in {"Init", "Reserve", "Initiate", "Dry", "Build", "Bank", "Policy",
                 "Swap", "Arm", "Complete", "Abort", "Cancel", "Supersede",
                 "Idle", "Crash", "Write", "Stale", "Move", "Reshard", "Admit",
                 "Refuse", "Publish"}

\* The alias reservation is held for the whole of a remediation.
ReservationHeldWhileInFlight == phase # "Idle" => reserved = "Rem"

\* The destination is a fresh physical copy: when the remediation begins it is
\* empty, has no layout, and is not the source, a discarded destination or a
\* copy any cutover retired.
FreshDestination ==
    phase = "DryRun"
      => /\ dest \notin {Orig, Other, srcPhys, alias} \cup retired \cup discarded
         /\ shards[dest] = {}
         /\ \A k \in Keys : data[dest][k] = Abs

\* A registered destination lays keys out exactly as the source did until it
\* goes live (a later reshard of the live tree may grow it).
DestinationInheritsTopology ==
    (phase \in {"Build", "Cutover"} /\ destReg /\ alias = srcPhys)
      => shards[dest] = inherited

\* The logical row's map always describes the copy its alias names.
AliasMapCoherent == mapOf = alias

\* A transform remediation's destination is never live without its policy.
EnforcedBeforeLive ==
    (phase = "Cutover" /\ mode = "Transform" /\ alias = dest) => policy = "New"

\* Every value the destination holds is stamped at the target.
DestinationStampedAtTarget ==
    (phase \in {"Build", "Cutover"} /\ destReg)
      => \A k \in Keys : data[dest][k] = Abs \/ data[dest][k].st = TargetStamp

\* No write routed at a copy a cutover superseded ever lands on it.
SupersededCopyTakesNoWrite == ~late

Init ==
    /\ phase = "Idle" /\ mode = "None" /\ cursor = 0 /\ pend = FALSE
    /\ cstep = 0 /\ begin = FALSE /\ prevPol = "Old"
    /\ dest = NoCopy /\ srcPhys = Orig /\ destReg = FALSE /\ inherited = {}
    /\ reserved = "None" /\ alias = Orig /\ mapOf = Orig
    /\ shards = [c \in Copies |-> IF c \in {Orig, Other} THEN {1} ELSE {}]
    /\ armed = {}
    /\ policy = "Old"
    /\ data \in {[c \in Copies |-> IF c = Orig THEN f
                                   ELSE IF c = Other
                                        THEN [k \in Keys |-> Val(TargetStamp, TRUE, FALSE)]
                                        ELSE [k \in Keys |-> Abs]] :
                 f \in [Keys -> Seed]}
    /\ nextId = 2 /\ discarded = {} /\ retired = {}
    /\ report = "None" /\ abortKey = 0
    /\ rems = 0 /\ writes = 0 /\ crashes = 0
    /\ moved = FALSE /\ resharded = FALSE /\ explicit = "None"
    /\ dirty = FALSE /\ late = FALSE /\ last = "Init"

\* The coordinator takes the alias reservation before it persists any intent.
Reserve ==
    /\ phase = "Idle" /\ reserved = "None" /\ ~begin /\ rems < MaxRems
    /\ reserved' = "Rem" /\ begin' = TRUE /\ rems' = rems + 1
    /\ last' = "Reserve"
    /\ UNCHANGED <<phase, mode, cursor, pend, cstep, dest, srcPhys, destReg,
                   inherited, alias, mapOf, shards, armed, policy, data, nextId,
                   discarded, retired, report, abortKey, writes, crashes, moved,
                   resharded, explicit, dirty, late, prevPol>>

\* Intent is persisted: the mode, a fresh destination id, and the physical copy
\* the tree resolves to now, captured before any alias swap.
Initiate(m) ==
    /\ phase = "Idle" /\ begin /\ reserved = "Rem"
    /\ phase' = "DryRun" /\ mode' = m /\ cursor' = 0 /\ pend' = FALSE
    /\ cstep' = 0 /\ begin' = FALSE
    /\ dest' = nextId /\ nextId' = nextId + 1
    /\ srcPhys' = alias /\ destReg' = FALSE /\ inherited' = {}
    /\ report' = "None" /\ abortKey' = 0
    /\ dirty' = FALSE /\ last' = "Initiate"
    /\ UNCHANGED <<reserved, alias, mapOf, shards, armed, policy, data,
                   discarded, retired, env, late, prevPol>>

\* Ends the remediation without publishing: the reservation is released.
Finish(r, k) ==
    /\ phase' = "Idle" /\ report' = r /\ abortKey' = k
    /\ reserved' = "None" /\ cursor' = 0 /\ pend' = FALSE /\ cstep' = 0

\* One dry-run value: an offender aborts with no destination; otherwise the
\* durable cursor advances, and the exhausted source moves to the build.
DryStep(k) ==
    /\ phase = "DryRun" /\ k = cursor + 1
    /\ IF Offends(data[alias][k])
       THEN /\ Finish("Aborted", k) /\ last' = "Abort"
            /\ UNCHANGED <<mode, begin, dest, srcPhys, destReg, inherited, prevPol>>
       ELSE /\ IF k = KeyCount
               THEN phase' = "Build" /\ cursor' = 0
               ELSE phase' = phase /\ cursor' = k
            /\ last' = "Dry"
            /\ UNCHANGED <<mode, pend, cstep, begin, dest, srcPhys, destReg,
                           inherited, reserved, report, abortKey, prevPol>>
    /\ UNCHANGED <<alias, mapOf, shards, armed, policy, data, nextId,
                   discarded, retired, env, dirty, late, prevPol>>

\* One build value: the first slice registers the destination with the
\* source's layout; an offender discards the destination and aborts;
\* otherwise the rewritten value is written to the destination, unbanked.
BuildStep(k) ==
    /\ phase = "Build" /\ k = cursor + 1 /\ ~pend
    /\ LET lay == IF destReg THEN shards[dest] ELSE shards[alias] IN
       /\ destReg' = TRUE
       /\ inherited' = IF destReg THEN inherited ELSE shards[alias]
       /\ IF Offends(data[alias][k])
          THEN /\ Finish("Aborted", k) /\ last' = "Abort"
               /\ discarded' = discarded \cup {dest}
               /\ shards' = [shards EXCEPT ![dest] = lay]
               /\ UNCHANGED data
          ELSE /\ data' = [data EXCEPT ![dest][k] = Rewrite(data[alias][k], mode)]
               /\ shards' = [shards EXCEPT ![dest] = lay]
               /\ pend' = TRUE /\ last' = "Build"
               /\ UNCHANGED <<phase, cursor, cstep, reserved, report, abortKey,
                              discarded, prevPol>>
    /\ UNCHANGED <<mode, begin, dest, srcPhys, alias, mapOf, armed, policy,
                   nextId, retired, env, dirty, late, prevPol>>

\* The slice banks its progress; the exhausted source moves to the cutover.
Bank ==
    /\ phase = "Build" /\ pend
    /\ pend' = FALSE
    /\ IF cursor + 1 = KeyCount
       THEN phase' = "Cutover" /\ cursor' = 0
       ELSE phase' = phase /\ cursor' = cursor + 1
    /\ last' = "Bank"
    /\ UNCHANGED <<mode, cstep, begin, dest, srcPhys, destReg, inherited,
                   reserved, alias, mapOf, shards, armed, policy, data, nextId,
                   discarded, retired, report, abortKey, env, dirty, late, prevPol>>

\* The cutover publishes only over the captured source (or, resumed after its
\* swap, over its own destination); otherwise it refuses: no swap, the
\* destination discarded, the reservation released, and a policy this attempt
\* installed before its fenced swap was refused put back (#5289).
Supersede ==
    /\ phase = "Cutover" /\ cstep \in {0, 1}
    /\ alias \notin {srcPhys, dest}
    /\ Finish("Superseded", 0) /\ last' = "Supersede"
    /\ discarded' = discarded \cup {dest}
    /\ policy' = IF cstep = 1 THEN prevPol ELSE policy
    /\ UNCHANGED <<mode, begin, prevPol, dest, srcPhys, destReg, inherited, alias,
                   mapOf, shards, armed, data, nextId, retired, env, dirty, late>>

\* A transform remediation installs its target policy before the swap; a
\* migration keeps the tree's policy.
CutPolicy ==
    /\ phase = "Cutover" /\ cstep = 0 /\ alias \in {srcPhys, dest}
    /\ policy' = IF mode = "Transform" THEN "New" ELSE policy
    /\ prevPol' = policy
    /\ cstep' = 1 /\ last' = "Policy"
    /\ UNCHANGED <<phase, mode, cursor, pend, begin, dest, srcPhys, destReg,
                   inherited, reserved, alias, mapOf, shards, armed, data,
                   nextId, discarded, retired, report, abortKey, env, dirty, late>>

\* The alias and the destination's map move together in one registry write; a
\* swap resumed after it landed changes nothing.
CutSwap ==
    /\ phase = "Cutover" /\ cstep = 1 /\ alias \in {srcPhys, dest}
    /\ alias' = dest /\ mapOf' = dest
    /\ cstep' = 2 /\ last' = "Swap"
    /\ UNCHANGED <<phase, mode, cursor, pend, begin, dest, srcPhys, destReg,
                   inherited, reserved, shards, armed, policy, data, nextId,
                   discarded, retired, report, abortKey, env, dirty, late, prevPol>>

\* Every shard of the captured source, at its current layout, is armed to
\* redirect logical traffic to the destination.
CutArm ==
    /\ phase = "Cutover" /\ cstep = 2
    /\ armed' = armed \cup {<<srcPhys, s, dest>> : s \in shards[srcPhys]}
    /\ cstep' = 3 /\ last' = "Arm"
    /\ UNCHANGED <<phase, mode, cursor, pend, begin, dest, srcPhys, destReg,
                   inherited, reserved, alias, mapOf, shards, policy, data,
                   nextId, discarded, retired, report, abortKey, env, dirty, late, prevPol>>

\* The terminal report is recorded and the reservation released.
Complete ==
    /\ phase = "Cutover" /\ cstep = 3
    /\ Finish("Completed", 0) /\ last' = "Complete"
    /\ retired' = retired \cup {srcPhys}
    /\ UNCHANGED <<mode, begin, dest, srcPhys, destReg, inherited, alias, mapOf,
                   shards, armed, policy, data, nextId, discarded, env, dirty, late, prevPol>>

\* A cancel before the cutover discards any destination; cutover declines it.
Cancel ==
    /\ phase \in {"DryRun", "Build"}
    /\ Finish("Cancelled", 0) /\ last' = "Cancel"
    /\ discarded' = IF destReg THEN discarded \cup {dest} ELSE discarded
    /\ UNCHANGED <<mode, begin, dest, srcPhys, destReg, inherited, alias, mapOf,
                   shards, armed, policy, data, nextId, retired, env, dirty, late, prevPol>>

\* An idle pass releases a reservation whose intent was never persisted.
IdlePass ==
    /\ phase = "Idle" /\ reserved = "Rem" /\ ~begin
    /\ reserved' = "None" /\ last' = "Idle"
    /\ UNCHANGED <<phase, mode, cursor, pend, cstep, begin, dest, srcPhys,
                   destReg, inherited, alias, mapOf, shards, armed, policy, data,
                   nextId, discarded, retired, report, abortKey, env, dirty, late, prevPol>>

\* The coordinator's activation is lost: in-turn progress goes, durable state
\* (phase, cursor, reservation, destination writes) stays.
Crash ==
    /\ crashes < MaxCrashes
    /\ (begin \/ pend \/ cstep > 0)
    /\ crashes' = crashes + 1
    /\ begin' = FALSE /\ pend' = FALSE /\ cstep' = 0 /\ last' = "Crash"
    /\ UNCHANGED <<phase, mode, cursor, dest, srcPhys, destReg, inherited,
                   reserved, alias, mapOf, shards, armed, policy, data, nextId,
                   discarded, retired, report, abortKey, rems, writes, moved,
                   resharded, explicit, dirty, late, prevPol>>

\* Environment: an ordinary write through the logical tree lands on the copy
\* the alias names (or the one its armed redirect forwards to).
Write(k, v) ==
    /\ writes < MaxWrites
    /\ LET c == Follow(alias, 1) IN data' = [data EXCEPT ![c][k] = v]
    /\ writes' = writes + 1
    /\ dirty' = (dirty \/ phase # "Idle")
    /\ last' = "Write"
    /\ UNCHANGED <<coord, reserved, alias, mapOf, shards, armed, policy,
                   nextId, discarded, retired, rems, crashes, moved, resharded,
                   explicit, late>>

\* Environment: a router still caching a superseded copy routes a write at
\* one of its shards; an armed redirect forwards it to the live copy.
StaleWrite(c, s, k) ==
    /\ writes < MaxWrites /\ c \in retired /\ s \in shards[c]
    /\ LET t == Follow(c, s) IN
       /\ data' = [data EXCEPT ![t][k] = Val(TargetStamp, TRUE, FALSE)]
       /\ late' = (late \/ t \in retired)
    /\ writes' = writes + 1 /\ last' = "Stale"
    /\ dirty' = (dirty \/ phase # "Idle")
    /\ UNCHANGED <<coord, reserved, alias, mapOf, shards, armed, policy,
                   nextId, discarded, retired, rems, crashes, moved, resharded,
                   explicit>>

\* Environment: a resize, restore or revert takes the reservation, moves the
\* tree onto a fresh copy of its contents and arms the old copy, then releases.
Move ==
    /\ ~moved /\ reserved = "None" /\ nextId <= MaxCopy
    /\ LET n == nextId IN
       /\ data' = [data EXCEPT ![n] = data[alias]]
       /\ shards' = [shards EXCEPT ![n] = shards[alias]]
       /\ armed' = armed \cup {<<alias, s, n>> : s \in shards[alias]}
       /\ alias' = n /\ mapOf' = n /\ nextId' = n + 1
       /\ retired' = retired \cup {alias}
    /\ moved' = TRUE /\ last' = "Move"
    /\ UNCHANGED <<coord, reserved, policy, discarded, rems, writes, crashes,
                   resharded, explicit, dirty, late>>

\* Environment: an online reshard of the copy the tree resolves to grows its
\* layout; it takes no reservation.
Reshard ==
    /\ ~resharded /\ shards[alias] = {1}
    /\ shards' = [shards EXCEPT ![alias] = Shards]
    /\ resharded' = TRUE /\ last' = "Reshard"
    /\ UNCHANGED <<coord, reserved, alias, mapOf, armed, policy, data, nextId,
                   discarded, retired, rems, writes, crashes, moved, explicit,
                   dirty, late>>

\* Environment: an explicit alias set is admitted only while no alias
\* reservation is held (#5289), and refused otherwise.
ExplicitAdmit ==
    /\ explicit = "None"
    /\ IF reserved = "None"
       THEN explicit' = "Admitted" /\ last' = "Admit"
       ELSE explicit' = explicit /\ last' = "Refuse"
    /\ UNCHANGED <<coord, reserved, alias, mapOf, shards, armed, policy, data,
                   nextId, discarded, retired, rems, writes, crashes, moved,
                   resharded, dirty, late>>

\* Environment: an admitted explicit alias set publishes onto the other tree,
\* arming the copy it replaces. It may land after a remediation reserved.
ExplicitPublish ==
    /\ explicit = "Admitted" /\ alias # Other
    /\ alias' = Other /\ mapOf' = Other
    /\ armed' = armed \cup {<<alias, s, Other>> : s \in shards[alias]}
    /\ retired' = retired \cup {alias}
    /\ explicit' = "Done" /\ last' = "Publish"
    /\ UNCHANGED <<coord, reserved, shards, policy, data, nextId, discarded,
                   rems, writes, crashes, moved, resharded, dirty, late>>

Stutter == UNCHANGED vars

Next ==
    \/ Reserve
    \/ \E m \in {"Transform", "Migrate"} : Initiate(m)
    \/ \E k \in Keys : DryStep(k)
    \/ \E k \in Keys : BuildStep(k)
    \/ Bank
    \/ Supersede
    \/ CutPolicy
    \/ CutSwap
    \/ CutArm
    \/ Complete
    \/ Cancel
    \/ IdlePass
    \/ Crash
    \/ \E k \in Keys, v \in Landing : Write(k, v)
    \/ \E c \in Copies, s \in Shards, k \in Keys : StaleWrite(c, s, k)
    \/ Move
    \/ Reshard
    \/ ExplicitAdmit
    \/ ExplicitPublish
    \/ Stutter

\* An abort or cancel publishes nothing: the alias and policy are untouched,
\* any destination is discarded, the reservation is released, and an abort
\* names a value the remediation genuinely could not rewrite.
AbortPublishesNothing ==
    [][(phase # "Idle" /\ report' \in {"Aborted", "Cancelled"} /\ phase' = "Idle")
       => /\ alias' = alias /\ policy' = policy /\ reserved' = "None"
          /\ (destReg => dest \in discarded')
          /\ (report' = "Aborted" => Offends(data[alias][abortKey']))]_vars

\* A cutover whose captured source was superseded publishes nothing over the
\* publication that superseded it.
SupersededPublicationRefused ==
    [][(alias' = dest /\ alias # dest /\ phase = "Cutover") => alias = srcPhys]_vars

\* A superseded cutover ends with no swap, the destination discarded, the
\* reservation released, and any policy it installed in the same attempt put back.
SupersededLeavesNothing ==
    [][(phase = "Cutover" /\ report' = "Superseded")
       => /\ alias' = alias /\ mapOf' = mapOf /\ dest \in discarded'
          /\ reserved' = "None" /\ phase' = "Idle"
          /\ (cstep = 1 => policy' = prevPol)]_vars

\* A migration never changes the tree's policy.
MigrationKeepsPolicy == [][policy' # policy => mode = "Transform"]_vars

\* Durable progress within a phase never moves backwards.
ProgressNeverRegresses ==
    [][(phase' = phase /\ phase # "Idle") => cursor' >= cursor]_vars

\* Every completed cutover has armed every shard of the source it replaced.
RedirectsCoverSource ==
    [][(phase = "Cutover" /\ report' = "Completed")
       => \A s \in shards[srcPhys] : <<srcPhys, s, dest>> \in armed]_vars

\* No alias change is admitted while the remediation holds the reservation.
ReservationExcludesAliasChanges ==
    [][last' \in {"Move", "Admit"} => reserved = "None"]_vars

\* With no write during the remediation, the published copy is exactly the
\* rewrite of every source value.
QuiescentCutoverComplete ==
    [][(phase = "Cutover" /\ report' = "Completed" /\ ~dirty)
       => \A k \in Keys : data[dest][k] = Rewrite(data[srcPhys][k], mode)]_vars

\* Every remediation ends: completed, aborted, cancelled or superseded.
RemediationTerminates == phase # "Idle" ~> phase = "Idle"

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(\E k \in Keys : DryStep(k))
    /\ WF_vars(\E k \in Keys : BuildStep(k))
    /\ WF_vars(Bank)
    /\ WF_vars(Supersede)
    /\ WF_vars(CutPolicy)
    /\ WF_vars(CutSwap)
    /\ WF_vars(CutArm)
    /\ WF_vars(Complete)
====
