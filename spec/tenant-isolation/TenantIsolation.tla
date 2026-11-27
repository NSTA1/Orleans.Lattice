---- MODULE TenantIsolation ----
\* Tenant-policy freshness across silos: a registry write commits on one silo,
\* the committing silo publishes a new cluster epoch through the epoch grain,
\* and every silo holds its compiled snapshot authoritative only while it holds
\* a live lease and has compiled the latest epoch it has seen. A silo that
\* cannot know it is current confirms against the registry, or denies.
\*
\* The guarantee checked is deliberately bounded. Once a write has RETURNED
\* after a successful publication, no silo trusts a snapshot older than it
\* (NoStaleAuthority). Two windows stay open and are modelled rather than
\* assumed away: a write whose publication fails returns while peers may still
\* trust their old snapshots until a background re-publish lands, and a silo
\* that crashes after committing leaves peers unaware until membership declares
\* it dead. Decisions already taken are never revoked retroactively.
\*
\* Membership can also be wrong: it may falsely declare a live silo dead. That
\* silo keeps serving on its snapshot and lease until it learns it is dead, so
\* the epoch grain's fresh-activation grace may not count a death as proof the
\* silo stopped. The grace ends early only once every member has leased from
\* the activation, or has been seen dead by the activation for one lease plus
\* the margin (NoEarlyGraceRelease).
EXTENDS Naturals

CONSTANTS NumSilos, MaxWrites, MaxCrashes, MaxRestarts, MaxFailures, MaxFalseDeaths

Silos == 1..NumSilos

VARIABLES
    regVer, coveredVer, returnedVer,
    wstate, wsilo, owes,
    epoch, graceOpen, graceWait,
    lease, pend,
    snapVer, stale, scanning, scanDirty, knows,
    alive, awaiting,
    dec,
    crashes, restarts, failures,
    falseDead, seen, suspicions

vars == <<regVer, coveredVer, returnedVer, wstate, wsilo, owes,
          epoch, graceOpen, graceWait, lease, pend,
          snapVer, stale, scanning, scanDirty, knows,
          alive, awaiting, dec, crashes, restarts, failures,
          falseDead, seen, suspicions>>

\* The false-death state most actions leave alone.
suspectVars == <<falseDead, seen, suspicions>>

\* Epochs are <<incarnation, version>>; the first epoch grain activation is
\* incarnation 1. Epochs only ever grow, so whether a silo's latest observed
\* epoch is superseded by the current one is tracked as knows[s]: the silo has
\* observed the current epoch. A fresh process has observed nothing.
\*
\* lease[s] joins the grain's lease table and the silo's own lease clock:
\* "live" before the recorded deadline, "margin" past the deadline on the
\* grain's clock but inside the clock-rate margin (a slower silo may still
\* trust it), "old" granted by a previous grain activation, "none" lapsed.
\*
\* falseDead[s]: membership reports s dead while its process still runs.
\* seen[s] is the epoch grain's record of s's death during its grace: "no" not
\* seen, "fresh" first seen by this activation, "aged" seen for one lease plus
\* the margin. suspicions counts false declarations against MaxFalseDeaths.

\* Membership lists t as dead: a crash it has declared, or a false declaration
\* while t's process still runs.
MemberDead(t) == ~awaiting[t] /\ (~alive[t] \/ falseDead[t])

\* t has leased from the current epoch grain activation.
LeasedHere(t) == alive[t] /\ lease[t] \in {"live", "margin"}

Writing == wstate \in {"committed", "advancing", "failed", "retrying"}

Authoritative(s) ==
    /\ alive[s]
    /\ lease[s] # "none"
    /\ ~stale[s]
    /\ ~owes[s]

Decisions == [kind : {"none", "trust", "confirm", "deny"}, auth : BOOLEAN, reach : BOOLEAN]

TypeOK ==
    /\ NumSilos \in Nat \ {0}
    /\ regVer \in 0..MaxWrites
    /\ coveredVer \in 0..regVer
    /\ returnedVer \in 0..regVer
    /\ wstate \in {"idle", "committed", "advancing", "failed", "retrying", "crashed"}
    /\ wsilo \in Silos
    /\ owes \in [Silos -> BOOLEAN]
    /\ epoch[1] \in 1..(MaxRestarts + 1)
    /\ epoch[2] \in 0..(MaxWrites + MaxFailures)
    /\ graceOpen \in BOOLEAN /\ graceWait \in BOOLEAN
    /\ lease \in [Silos -> {"none", "live", "margin", "old"}]
    /\ pend \in [Silos -> {"no", "tied", "open", "lapsed"}]
    /\ snapVer \in [Silos -> 0..MaxWrites]
    /\ stale \in [Silos -> BOOLEAN]
    /\ scanning \in [Silos -> BOOLEAN]
    /\ scanDirty \in [Silos -> BOOLEAN]
    /\ knows \in [Silos -> BOOLEAN]
    /\ alive \in [Silos -> BOOLEAN]
    /\ awaiting \in [Silos -> BOOLEAN]
    /\ dec \in Decisions
    /\ crashes \in 0..MaxCrashes
    /\ restarts \in 0..MaxRestarts
    /\ failures \in 0..MaxFailures
    /\ falseDead \in [Silos -> BOOLEAN]
    /\ seen \in [Silos -> {"no", "fresh", "aged"}]
    /\ suspicions \in 0..MaxFalseDeaths

\* Once a write has returned after its publication completed (or once the
\* crashed writer's death has been declared), no silo trusts an older snapshot.
NoStaleAuthority ==
    \A s \in Silos : Authoritative(s) => snapVer[s] >= coveredVer

\* The committing silo never trusts a snapshot that misses its own write.
WriterReadsOwnWrite ==
    (Writing /\ Authoritative(wsilo)) => snapVer[wsilo] >= regVer

\* A write may return ahead of its coverage only through a failed publication
\* that is still owed a background re-publish, or a writer that crashed.
PublicationWindowTracked ==
    returnedVer > coveredVer => wstate \in {"failed", "retrying", "crashed"}

\* A decision trusts the snapshot only when it is authoritative, confirms only
\* when the registry is reachable, and otherwise denies.
UnconfirmableDenies ==
    /\ dec.kind = "trust" => dec.auth
    /\ dec.kind = "confirm" => (~dec.auth /\ dec.reach)
    /\ (dec.kind # "none" /\ ~dec.auth /\ ~dec.reach) => dec.kind = "deny"

\* Epochs never repeat: a version only grows within an incarnation, and a new
\* incarnation is never one seen before.
EpochMonotonic ==
    [][/\ epoch'[1] = epoch[1] => epoch'[2] >= epoch[2]
       /\ epoch'[1] # epoch[1] => epoch'[1] > epoch[1]]_epoch

\* The fresh-activation grace ends only once no silo can still serve on a lease
\* a previous activation granted: each has leased from this activation, or has
\* been seen dead by it for one lease plus the margin, which a silo falsely
\* declared dead may still be trusting its old lease within.
NoEarlyGraceRelease ==
    ~graceOpen => \A t \in Silos : lease[t] # "old"

Init ==
    /\ regVer = 0 /\ coveredVer = 0 /\ returnedVer = 0
    /\ wstate = "idle" /\ wsilo = CHOOSE s \in Silos : TRUE
    /\ owes = [s \in Silos |-> FALSE]
    /\ epoch = <<1, 0>>
    /\ graceOpen = TRUE /\ graceWait = FALSE
    /\ lease = [s \in Silos |-> "none"]
    /\ pend = [s \in Silos |-> "no"]
    /\ snapVer = [s \in Silos |-> 0]
    /\ stale = [s \in Silos |-> TRUE]
    /\ scanning = [s \in Silos |-> FALSE]
    /\ scanDirty = [s \in Silos |-> FALSE]
    /\ knows = [s \in Silos |-> FALSE]
    /\ alive = [s \in Silos |-> TRUE]
    /\ awaiting = [s \in Silos |-> FALSE]
    /\ dec = [kind |-> "none", auth |-> FALSE, reach |-> FALSE]
    /\ crashes = 0 /\ restarts = 0 /\ failures = 0
    /\ falseDead = [s \in Silos |-> FALSE]
    /\ seen = [s \in Silos |-> "no"]
    /\ suspicions = 0

\* Silo s has not observed the current epoch: learning it supersedes what s
\* compiled, so the snapshot stops being current and a rebuild is scheduled.
Superseded(s) == ~knows[s]

\* The leases an advance captures: every table entry whose deadline plus the
\* clock-rate margin has not passed.
Capture(t) == IF lease[t] \in {"live", "margin"} THEN "tied" ELSE "no"

\* A registry write commits on silo s. The mutation hook then schedules the
\* local rebuild and marks an advance in flight before publishing.
CommitWrite(s) ==
    /\ wstate = "idle" /\ alive[s] /\ regVer < MaxWrites
    /\ regVer' = regVer + 1
    /\ wstate' = "committed" /\ wsilo' = s
    /\ stale' = [stale EXCEPT ![s] = TRUE]
    /\ scanDirty' = [scanDirty EXCEPT ![s] = scanning[s] \/ scanDirty[s]]
    /\ owes' = [owes EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<coveredVer, returnedVer, epoch, graceOpen, graceWait, lease, pend,
                   snapVer, scanning, knows, alive, awaiting, dec, crashes,
                   restarts, failures>>
    /\ UNCHANGED suspectVars

\* The epoch grain bumps the version and captures the leases to push to.
StartAdvance ==
    /\ wstate = "committed" /\ alive[wsilo]
    /\ wstate' = "advancing"
    /\ epoch' = <<epoch[1], epoch[2] + 1>>
    /\ knows' = [t \in Silos |-> FALSE]
    /\ pend' = [t \in Silos |-> Capture(t)]
    /\ graceWait' = graceOpen
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wsilo, owes, graceOpen, lease,
                   snapVer, stale, scanning, scanDirty, alive, awaiting, dec,
                   crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* The push reaches silo t and is acknowledged.
Deliver(t) ==
    /\ pend[t] \in {"tied", "open"} /\ alive[t]
    /\ pend' = [pend EXCEPT ![t] = "no"]
    /\ knows' = [knows EXCEPT ![t] = TRUE]
    /\ stale' = [stale EXCEPT ![t] = stale[t] \/ Superseded(t)]
    /\ scanDirty' = [scanDirty EXCEPT ![t] = scanDirty[t] \/ (scanning[t] /\ Superseded(t))]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, snapVer, scanning, alive, awaiting,
                   dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* Time passes for silo t's lease on the epoch grain's clock: past the recorded
\* deadline the lease enters the margin, where a slower silo may still trust
\* it, and then lapses everywhere. A captured deadline the silo has not
\* renewed past ages with it.
LeaseAges(t) ==
    /\ lease[t] \in {"live", "margin"}
    /\ LET next == IF lease[t] = "live" THEN "margin" ELSE "none"
       IN /\ lease' = [lease EXCEPT ![t] = next]
          /\ pend' = [pend EXCEPT ![t] =
                IF pend[t] = "tied" /\ next = "none" THEN "lapsed" ELSE pend[t]]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, snapVer, stale, scanning, scanDirty, knows,
                   alive, awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* A captured deadline the silo has since renewed past passes on its own, plus
\* the margin, while the renewed deadline still stands.
CaptureAges(t) ==
    /\ pend[t] = "open"
    /\ pend' = [pend EXCEPT ![t] = "lapsed"]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, snapVer, stale, scanning, scanDirty,
                   knows, alive, awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* Every captured silo acknowledged or had its lease plus margin pass, and the
\* fresh-incarnation grace is over: the write returns.
CompleteWrite ==
    /\ wstate = "advancing" /\ alive[wsilo]
    /\ \A t \in Silos : pend[t] \in {"no", "lapsed"}
    /\ ~graceWait
    /\ wstate' = "idle"
    /\ coveredVer' = regVer /\ returnedVer' = regVer
    /\ owes' = [owes EXCEPT ![wsilo] = FALSE]
    /\ pend' = [t \in Silos |-> "no"]
    /\ UNCHANGED <<regVer, wsilo, epoch, graceOpen, graceWait, lease, snapVer, stale,
                   scanning, scanDirty, knows, alive, awaiting, dec, crashes,
                   restarts, failures>>
    /\ UNCHANGED suspectVars

\* The publication fails (the grain is unreachable or faults). The write is
\* durable and returns anyway; the silo stays non-authoritative and owes a
\* background re-publish.
PublishFails ==
    /\ wstate \in {"committed", "advancing", "retrying"} /\ alive[wsilo]
    /\ failures < MaxFailures
    /\ failures' = failures + 1
    /\ wstate' = "failed"
    /\ returnedVer' = regVer
    /\ pend' = [t \in Silos |-> "no"]
    /\ graceWait' = FALSE
    /\ UNCHANGED <<regVer, coveredVer, wsilo, owes, epoch, graceOpen, lease, snapVer,
                   stale, scanning, scanDirty, knows, alive, awaiting, dec, crashes,
                   restarts>>
    /\ UNCHANGED suspectVars

\* The background loop re-publishes the owed advance.
RetryAdvance ==
    /\ wstate = "failed" /\ alive[wsilo] /\ owes[wsilo]
    /\ wstate' = "retrying"
    /\ epoch' = <<epoch[1], epoch[2] + 1>>
    /\ knows' = [t \in Silos |-> FALSE]
    /\ pend' = [t \in Silos |-> Capture(t)]
    /\ graceWait' = graceOpen
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wsilo, owes, graceOpen, lease,
                   snapVer, stale, scanning, scanDirty, alive, awaiting, dec,
                   crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* The re-publish completes and repairs the debt.
RetrySucceeds ==
    /\ wstate = "retrying" /\ alive[wsilo]
    /\ \A t \in Silos : pend[t] \in {"no", "lapsed"}
    /\ ~graceWait
    /\ wstate' = "idle"
    /\ coveredVer' = regVer
    /\ owes' = [owes EXCEPT ![wsilo] = FALSE]
    /\ pend' = [t \in Silos |-> "no"]
    /\ UNCHANGED <<regVer, returnedVer, wsilo, epoch, graceOpen, graceWait, lease,
                   snapVer, stale, scanning, scanDirty, knows, alive, awaiting, dec,
                   crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* A scheduled rebuild scans the registry as it stands now. The snapshot it
\* will publish is fixed by this scan; the silo stays out of date meanwhile.
RebuildStart(s) ==
    /\ alive[s] /\ stale[s] /\ ~scanning[s]
    /\ scanning' = [scanning EXCEPT ![s] = TRUE]
    /\ snapVer' = [snapVer EXCEPT ![s] = regVer]
    /\ scanDirty' = [scanDirty EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, pend, stale, knows, alive, awaiting,
                   dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* The rebuild publishes. It is current only if nothing invalidated the silo
\* while it scanned; otherwise the follow-up rebuild is still owed.
RebuildFinish(s) ==
    /\ alive[s] /\ scanning[s]
    /\ scanning' = [scanning EXCEPT ![s] = FALSE]
    /\ stale' = [stale EXCEPT ![s] = scanDirty[s]]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, pend, snapVer, scanDirty, knows,
                   alive, awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* Silo s renews its lease: the grain records a fresh deadline and the lease
\* carries the current epoch, which the silo observes.
Renew(s) ==
    /\ alive[s]
    /\ lease' = [lease EXCEPT ![s] = "live"]
    /\ pend' = [pend EXCEPT ![s] = IF pend[s] = "tied" THEN "open" ELSE pend[s]]
    /\ knows' = [knows EXCEPT ![s] = TRUE]
    /\ stale' = [stale EXCEPT ![s] = stale[s] \/ Superseded(s)]
    /\ scanDirty' = [scanDirty EXCEPT ![s] = scanDirty[s] \/ (scanning[s] /\ Superseded(s))]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, snapVer, scanning, alive, awaiting, dec,
                   crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* A consumer on silo s decides a request: from the snapshot when it is
\* authoritative, by confirming against the registry when that is reachable,
\* and otherwise by denying.
Decide(s, r) ==
    /\ alive[s]
    /\ dec' = IF Authoritative(s) THEN [kind |-> "trust", auth |-> TRUE, reach |-> FALSE]
              ELSE IF r THEN [kind |-> "confirm", auth |-> FALSE, reach |-> TRUE]
              ELSE [kind |-> "deny", auth |-> FALSE, reach |-> FALSE]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, pend, snapVer, stale, scanning,
                   scanDirty, knows, alive, awaiting, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* Silo s crashes and loses its process state. An unfinished write or owed
\* re-publish on it is lost with it. The grain's recorded deadline for s still
\* passes, so a capture tied to s no longer holds an advance open. A silo
\* falsely declared dead stops the same way once it learns it is dead; membership
\* already reports it dead, so no declaration is awaited.
SiloCrash(s) ==
    /\ alive[s] /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ alive' = [alive EXCEPT ![s] = FALSE]
    /\ awaiting' = [awaiting EXCEPT ![s] = ~falseDead[s]]
    /\ falseDead' = [falseDead EXCEPT ![s] = FALSE]
    /\ wstate' = IF wsilo = s /\ Writing THEN "crashed" ELSE wstate
    /\ pend' = IF wsilo = s /\ Writing THEN [t \in Silos |-> "no"]
               ELSE [pend EXCEPT ![s] = IF pend[s] = "no" THEN "no" ELSE "lapsed"]
    /\ graceWait' = IF wsilo = s /\ Writing THEN FALSE ELSE graceWait
    /\ owes' = [owes EXCEPT ![s] = FALSE]
    /\ lease' = [lease EXCEPT ![s] = "none"]
    /\ stale' = [stale EXCEPT ![s] = TRUE]
    /\ scanning' = [scanning EXCEPT ![s] = FALSE]
    /\ scanDirty' = [scanDirty EXCEPT ![s] = FALSE]
    /\ snapVer' = [snapVer EXCEPT ![s] = 0]
    /\ knows' = [knows EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wsilo, epoch, graceOpen, dec,
                   restarts, failures>>
    /\ UNCHANGED <<seen, suspicions>>

\* Cluster membership declares the crashed silo s dead. Every surviving silo
\* treats its snapshot as out of date, which covers any write s committed and
\* never published. An epoch grain in its grace records when it first sees the
\* death.
DeclareDead(s) ==
    /\ awaiting[s]
    /\ awaiting' = [awaiting EXCEPT ![s] = FALSE]
    /\ stale' = [t \in Silos |-> stale[t] \/ alive[t]]
    /\ scanDirty' = [t \in Silos |-> scanDirty[t] \/ (alive[t] /\ scanning[t])]
    /\ IF wstate = "crashed" /\ wsilo = s
          THEN /\ wstate' = "idle" /\ coveredVer' = regVer
          ELSE UNCHANGED <<wstate, coveredVer>>
    /\ seen' = [seen EXCEPT ![s] = IF graceOpen /\ ~alive[s] THEN "fresh" ELSE seen[s]]
    /\ UNCHANGED <<regVer, returnedVer, wsilo, owes, epoch, graceOpen, graceWait,
                   lease, pend, snapVer, scanning, knows, alive, dec, crashes,
                   restarts, failures>>
    /\ UNCHANGED <<falseDead, suspicions>>

\* Membership falsely declares the live silo s dead. Its process keeps running,
\* serving on its snapshot and whatever lease it holds, until it learns it is
\* dead and stops (SiloCrash). Survivors treat their snapshots as out of date,
\* as for any declaration; the epoch ledger still pushes to and waits out every
\* lease it captured, whatever membership says; and an epoch grain in its grace
\* records when it first sees the death.
DeclareFalselyDead(s) ==
    /\ alive[s] /\ ~falseDead[s] /\ ~awaiting[s]
    /\ suspicions < MaxFalseDeaths
    /\ suspicions' = suspicions + 1
    /\ falseDead' = [falseDead EXCEPT ![s] = TRUE]
    /\ seen' = [seen EXCEPT ![s] = IF graceOpen THEN "fresh" ELSE "no"]
    /\ stale' = [t \in Silos |-> stale[t] \/ (alive[t] /\ t # s)]
    /\ scanDirty' = [t \in Silos |-> scanDirty[t] \/ (alive[t] /\ t # s /\ scanning[t])]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, pend, snapVer, scanning, knows,
                   alive, awaiting, dec, crashes, restarts, failures>>

\* One lease plus the margin passes after the epoch grain first saw t dead. The
\* sighting came after the grain activated, so every lease a previous activation
\* granted has lapsed by now too, on any clock within the margin - including the
\* lease a silo falsely declared dead may still be trusting.
SightingAges(t) ==
    /\ seen[t] = "fresh"
    /\ seen' = [seen EXCEPT ![t] = "aged"]
    /\ lease' = [u \in Silos |-> IF lease[u] = "old" THEN "none" ELSE lease[u]]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, pend, snapVer, stale, scanning, scanDirty,
                   knows, alive, awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED <<falseDead, suspicions>>

\* A fresh process starts on silo s with no snapshot and no observed epoch.
Recover(s) ==
    /\ ~alive[s]
    /\ alive' = [alive EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   graceOpen, graceWait, lease, pend, snapVer, stale, scanning,
                   scanDirty, knows, awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED suspectVars

\* The epoch grain re-activates with a fresh incarnation and an empty lease
\* table. Leases the old activation granted are still live on the silos, so a
\* grace period opens. An advance in flight on the old activation fails. The
\* new activation's first sight of every silo membership already lists dead is
\* its activation: it cannot vouch for any earlier moment.
EpochRestart ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ epoch' = <<epoch[1] + 1, 0>>
    /\ knows' = [t \in Silos |-> FALSE]
    /\ lease' = [t \in Silos |-> IF lease[t] = "none" THEN "none" ELSE "old"]
    /\ pend' = [t \in Silos |-> "no"]
    /\ graceOpen' = TRUE /\ graceWait' = FALSE
    /\ IF wstate \in {"advancing", "retrying"}
          THEN /\ wstate' = "failed" /\ returnedVer' = regVer
          ELSE UNCHANGED <<wstate, returnedVer>>
    /\ seen' = [t \in Silos |-> IF MemberDead(t) THEN "fresh" ELSE "no"]
    /\ UNCHANGED <<regVer, coveredVer, wsilo, owes, snapVer, stale, scanning,
                   scanDirty, alive, awaiting, dec, crashes, failures>>
    /\ UNCHANGED <<falseDead, suspicions>>

\* One lease plus the margin passes after activation: every lease the previous
\* activation granted has lapsed.
GraceElapse ==
    /\ graceOpen
    /\ graceOpen' = FALSE /\ graceWait' = FALSE
    /\ lease' = [t \in Silos |-> IF lease[t] = "old" THEN "none" ELSE lease[t]]
    /\ seen' = [t \in Silos |-> "no"]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   pend, snapVer, stale, scanning, scanDirty, knows, alive,
                   awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED <<falseDead, suspicions>>

\* The grace ends early once every silo membership lists has leased from this
\* activation, or has been seen dead by this activation for one lease plus the
\* margin. A death alone releases nothing: a silo falsely declared dead may
\* still be serving on a lease from the previous activation.
ReleaseGrace ==
    /\ graceOpen
    /\ \A t \in Silos : ~awaiting[t] /\ (LeasedHere(t) \/ (MemberDead(t) /\ seen[t] = "aged"))
    /\ graceOpen' = FALSE /\ graceWait' = FALSE
    /\ seen' = [t \in Silos |-> "no"]
    /\ UNCHANGED <<regVer, coveredVer, returnedVer, wstate, wsilo, owes, epoch,
                   lease, pend, snapVer, stale, scanning, scanDirty, knows, alive,
                   awaiting, dec, crashes, restarts, failures>>
    /\ UNCHANGED <<falseDead, suspicions>>

Stutter == UNCHANGED vars

Next ==
    \/ \E s \in Silos : CommitWrite(s)
    \/ StartAdvance
    \/ \E t \in Silos : Deliver(t)
    \/ \E t \in Silos : LeaseAges(t)
    \/ \E t \in Silos : CaptureAges(t)
    \/ CompleteWrite
    \/ PublishFails
    \/ RetryAdvance
    \/ RetrySucceeds
    \/ \E s \in Silos : RebuildStart(s)
    \/ \E s \in Silos : RebuildFinish(s)
    \/ \E s \in Silos : Renew(s)
    \/ \E s \in Silos, r \in BOOLEAN : Decide(s, r)
    \/ \E s \in Silos : SiloCrash(s)
    \/ \E s \in Silos : DeclareDead(s)
    \/ \E s \in Silos : DeclareFalselyDead(s)
    \/ \E t \in Silos : SightingAges(t)
    \/ \E s \in Silos : Recover(s)
    \/ EpochRestart
    \/ GraceElapse
    \/ ReleaseGrace
    \/ Stutter

\* Fairness is what production guarantees without help: the hook always calls
\* the grain, clocks advance, the advance waits complete, the background loop
\* keeps re-publishing while the writer lives, and membership eventually
\* declares a crashed silo dead. Deliveries, renewals, rebuilds, failures,
\* crashes, false declarations, sightings aging and restarts carry no fairness.
Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(StartAdvance)
    /\ \A t \in Silos : WF_vars(LeaseAges(t))
    /\ \A t \in Silos : WF_vars(CaptureAges(t))
    /\ WF_vars(CompleteWrite)
    /\ WF_vars(RetryAdvance)
    /\ WF_vars(RetrySucceeds)
    /\ \A s \in Silos : WF_vars(DeclareDead(s))
    /\ WF_vars(GraceElapse)

\* Every committed write is eventually covered: published to every silo, or
\* covered by membership declaring its crashed writer dead.
EveryCommitEventuallyCovered ==
    \A v \in 1..MaxWrites : (regVer >= v) ~> (coveredVer >= v)
====