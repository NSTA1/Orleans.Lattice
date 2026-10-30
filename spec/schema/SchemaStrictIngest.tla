---- MODULE SchemaStrictIngest ----
EXTENDS Naturals

\* Strict ingest at the receiver of a replicated stream. Each entry reaches the
\* LatticeGrain write choke point as a system-origin point write, is judged by
\* the policy its silo holds, and is either committed or diverted to the tree's
\* dead-letter queue. The policy lookup, the dead-letter append and the commit
\* can each fail, be cancelled or lose their response. The replication applier
\* re-throws a failure so the sender re-delivers, and quarantines an entry after
\* repeated failures; the transport can lose an acknowledgement, so a handled
\* entry is delivered again. The configuration is the supported composition with
\* strict ingest on (SchemaStageOrdering owns every other combination).

CONSTANTS N, MaxFailures, MaxLost

Entries == 1..N

\* Shared vocabulary (spec/schema/README.md), restated. "Legacy" is a body the
\* previous policy accepts and the current one refuses; "Orphan" is a tag the
\* read decoder cannot serve at the target (newer, or no upcast path).
Bodies == {"Good", "Legacy", "Bad"}
Raw(b) == [form |-> "Raw", body |-> b, tag |-> "None"]
Env(b, t) == [form |-> "Env", body |-> b, tag |-> t]
Values ==
    {Raw(b) : b \in Bodies} \cup {Env(b, "Target") : b \in Bodies}
        \cup {Env("Good", "Orphan")}

\* Policy 1 is the previous policy and 2 the current one after a single change.
Policies == {1, 2}

\* The outcome of a durable step: done and answered, failed (or cancelled)
\* before anything became durable, or durable with its response lost.
Results == {"Ok", "Failed", "Lost"}

\* The #5239 rule: a policy judges the body a reader is served, so an envelope
\* the decoder cannot serve fails closed, and a valid body inside an envelope
\* is never refused for its header.
Refused(v, p) ==
    \/ v.form = "Env" /\ v.tag = "Orphan"
    \/ v.body = "Bad"
    \/ v.body = "Legacy" /\ p = 2

\* The most dead-letter records an entry can accumulate: one per delivery, and
\* every acknowledgement but the last can be lost.
Bound == (MaxLost + 1) * MaxFailures

VARIABLES val, late, pol, seen, sstate, cur, rphase, judged, appended, reported,
          fails, lostLeft, stored, storedUnder, dlq, parked
vars == <<val, late, pol, seen, sstate, cur, rphase, judged, appended, reported,
          fails, lostLeft, stored, storedUnder, dlq, parked>>

TypeOK ==
    /\ val \in [Entries -> Values]
    /\ late \in [Entries -> BOOLEAN]
    /\ pol \in Policies /\ seen \in Policies
    /\ sstate \in [Entries -> {"Pending", "InFlight", "Acked"}]
    /\ cur \in Entries \cup {0}
    /\ rphase \in {"Idle", "Judging", "DeadLettering", "Committing", "Failed"}
    /\ judged \in {0} \cup Policies
    /\ appended \in BOOLEAN /\ reported \in BOOLEAN
    /\ fails \in [Entries -> 0..(MaxFailures - 1)]
    /\ lostLeft \in 0..MaxLost
    /\ stored \subseteq Entries
    /\ storedUnder \in [Entries -> {0} \cup Policies]
    /\ dlq \in [Entries -> 0..Bound]
    /\ parked \subseteq Entries

\* Strict ingest stores no entry the policy its silo held at judgement refuses,
\* and nothing is stored that was never judged, whatever the order in which the
\* entry and its batch's decision arrived.
StoredValidWhenJudged ==
    \A e \in stored : storedUnder[e] \in Policies /\ ~Refused(val[e], storedUnder[e])

\* An acknowledged entry was committed, diverted with a durable record, or
\* quarantined by the replication applier: a failed append is never taken for
\* a diversion.
AckedResolved ==
    \A e \in Entries :
        sstate[e] = "Acked" => (e \in stored \/ dlq[e] > 0 \/ e \in parked)

\* A delivery appends at most one dead-letter record: duplicates come only from
\* re-delivery.
OneRecordPerDelivery ==
    [][\A e \in Entries :
          dlq'[e] # dlq[e] => (e = cur /\ dlq'[e] = dlq[e] + 1 /\ ~appended)]_vars

\* A diversion is acknowledged only by the step that made its own record
\* durable, and a record never replaces an earlier one.
DiversionAckedOnlyWhenRecorded ==
    [][(rphase = "DeadLettering" /\ cur # 0 /\ sstate'[cur] = "Acked")
       => dlq'[cur] = dlq[cur] + 1]_vars

\* A diversion the stage reported is acknowledged at once, never re-surfaced as a
\* failure for re-delivery, so an entry is diverted exactly once per delivery.
DiversionSettles ==
    [][(rphase = "DeadLettering" /\ cur # 0 /\ reported') => sstate'[cur] = "Acked"]_vars

\* Only an entry the judging policy refuses is dead-lettered.
DeadLettersOnlyRefused ==
    [][\A e \in Entries : dlq'[e] > dlq[e] => Refused(val[e], judged)]_vars

\* Every delivery is judged afresh by the policy its silo holds at that moment.
JudgedByHeldPolicy ==
    [][(rphase = "Judging" /\ judged' # judged) => judged' = seen]_vars

\* The applier quarantines an entry only when a failed attempt reaches the
\* retry threshold.
ParkOnlyAfterRepeatedFailure ==
    [][\A e \in Entries :
          (e \notin parked /\ e \in parked')
              => (rphase = "Failed" /\ e = cur /\ fails[e] = MaxFailures - 1)]_vars

\* Every entry is eventually acknowledged for good.
EveryEntryAcknowledged == \A e \in Entries : <>[](sstate[e] = "Acked")

Init ==
    /\ val \in [Entries -> Values]
    /\ late \in [Entries -> BOOLEAN]
    /\ pol = 1 /\ seen = 1
    /\ sstate = [e \in Entries |-> "Pending"]
    /\ cur = 0 /\ rphase = "Idle" /\ judged = 0
    /\ appended = FALSE /\ reported = FALSE
    /\ fails = [e \in Entries |-> 0]
    /\ lostLeft = MaxLost
    /\ stored = {} /\ storedUnder = [e \in Entries |-> 0]
    /\ dlq = [e \in Entries |-> 0] /\ parked = {}

\* The receiver finishes the entry in hand and acknowledges it.
Acknowledge(e) ==
    /\ sstate' = [sstate EXCEPT ![e] = "Acked"]
    /\ fails' = [fails EXCEPT ![e] = 0]
    /\ rphase' = "Idle" /\ cur' = 0

\* The attempt fails; the applier settles it next.
FailAttempt == rphase' = "Failed" /\ UNCHANGED <<sstate, fails, cur>>

\* The sender delivers an entry it holds no acknowledgement for. An entry of a
\* replicated atomic batch whose decision the receiver already recorded (late)
\* is settled against that decision, but still reaches the strict stages.
Deliver(e) ==
    /\ rphase = "Idle" /\ sstate[e] = "Pending"
    /\ cur' = e /\ rphase' = "Judging" /\ judged' = 0
    /\ appended' = FALSE /\ reported' = FALSE
    /\ sstate' = [sstate EXCEPT ![e] = "InFlight"]
    /\ UNCHANGED <<val, late, pol, seen, fails, lostLeft, stored, storedUnder, dlq, parked>>

\* The strict stages look the policy up and judge the entry's served body; a
\* lookup that throws or is cancelled fails the attempt.
Judge(r) ==
    /\ rphase = "Judging"
    /\ IF r = "Ok"
       THEN /\ judged' = seen
            /\ rphase' = IF Refused(val[cur], seen) THEN "DeadLettering" ELSE "Committing"
       ELSE /\ rphase' = "Failed"
            /\ UNCHANGED judged
    /\ UNCHANGED <<val, late, pol, seen, sstate, cur, appended, reported, fails, lostLeft, stored, storedUnder, dlq, parked>>

\* The refusing stage appends a dead-letter record and then reports the
\* diversion; a failed append is not a diversion.
DeadLetter(r) ==
    /\ rphase = "DeadLettering"
    /\ IF r = "Failed"
       THEN UNCHANGED <<dlq, appended>>
       ELSE /\ dlq' = [dlq EXCEPT ![cur] = @ + 1]
            /\ appended' = TRUE
    /\ reported' = (r = "Ok")
    /\ IF r = "Ok" THEN Acknowledge(cur) ELSE FailAttempt
    /\ UNCHANGED <<val, late, pol, seen, judged, lostLeft, stored, storedUnder, parked>>

\* The choke point appends the admitted entry to the WAL.
Commit(r) ==
    /\ rphase = "Committing"
    /\ IF r = "Failed"
       THEN UNCHANGED <<stored, storedUnder>>
       ELSE /\ stored' = stored \cup {cur}
            /\ storedUnder' = [storedUnder EXCEPT ![cur] = judged]
    /\ IF r = "Ok" THEN Acknowledge(cur) ELSE FailAttempt
    /\ UNCHANGED <<val, late, pol, seen, judged, appended, reported, lostLeft, dlq, parked>>

\* The replication applier settles a failed attempt: below the threshold it
\* re-throws so the sender re-delivers; at it, it parks the entry.
Settle ==
    /\ rphase = "Failed"
    /\ IF fails[cur] + 1 < MaxFailures
       THEN /\ fails' = [fails EXCEPT ![cur] = @ + 1]
            /\ sstate' = [sstate EXCEPT ![cur] = "Pending"]
            /\ rphase' = "Idle" /\ cur' = 0
            /\ UNCHANGED parked
       ELSE /\ parked' = parked \cup {cur}
            /\ Acknowledge(cur)
    /\ UNCHANGED <<val, late, pol, seen, judged, appended, reported, lostLeft, stored, storedUnder, dlq>>

\* Environment: the transport loses an acknowledgement, so the sender delivers
\* the entry again.
LoseAck(e) ==
    /\ sstate[e] = "Acked" /\ lostLeft > 0
    /\ sstate' = [sstate EXCEPT ![e] = "Pending"]
    /\ lostLeft' = lostLeft - 1
    /\ UNCHANGED <<val, late, pol, seen, cur, rphase, judged, appended, reported, fails, stored, storedUnder, dlq, parked>>

\* An administrator replaces the policy.
ChangePolicy ==
    /\ pol = 1 /\ pol' = 2
    /\ UNCHANGED <<val, late, seen, sstate, cur, rphase, judged, appended, reported, fails, lostLeft, stored, storedUnder, dlq, parked>>

\* The change reaches the receiving silo, which evicts its cached policy.
Propagate ==
    /\ seen # pol /\ seen' = pol
    /\ UNCHANGED <<val, late, pol, sstate, cur, rphase, judged, appended, reported, fails, lostLeft, stored, storedUnder, dlq, parked>>

Stutter == UNCHANGED vars

Next ==
    \/ \E e \in Entries : Deliver(e)
    \/ \E r \in {"Ok", "Failed"} : Judge(r)
    \/ \E r \in Results : DeadLetter(r)
    \/ \E r \in Results : Commit(r)
    \/ Settle
    \/ \E e \in Entries : LoseAck(e)
    \/ ChangePolicy
    \/ Propagate
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A e \in Entries : WF_vars(Deliver(e))
    /\ WF_vars(\E r \in {"Ok", "Failed"} : Judge(r))
    /\ WF_vars(\E r \in Results : DeadLetter(r))
    /\ WF_vars(\E r \in Results : Commit(r))
    /\ WF_vars(Settle)
====
