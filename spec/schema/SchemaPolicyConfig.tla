---- MODULE SchemaPolicyConfig ----
EXTENDS Naturals

\* One governed tree's schema setting (its enforcement policy, or equally its
\* version config): the durable reserved-tree row, a per-silo cache with a
\* null sentinel, eviction on change, and the writes the cached setting judges.

CONSTANTS SiloCount, MaxChanges, MaxWrites

Silos == 1..SiloCount
\* "None" is a tree with no setting (never set, or cleared). "Bad" is a policy
\* that does not compile and must never be stored.
Policies == {"None", "A", "B"}
Values == {1, 2}
Valid(p, v) == p = "None" \/ (p = "A" /\ v = 1) \/ (p = "B" /\ v = 2)

VARIABLES stored, rev, admin, adminAt, cache, cacheRev, loading, loadVal,
          loadRev, overtaken, pending, data, commits, writes, judge, outcome
vars == <<stored, rev, admin, adminAt, cache, cacheRev, loading, loadVal,
          loadRev, overtaken, pending, data, commits, writes, judge, outcome>>

TypeOK ==
    /\ stored \in Policies \cup {"Bad"}
    /\ rev \in 0..MaxChanges
    /\ admin \in {"Idle", "Committed"} /\ adminAt \in Silos
    /\ cache \in [Silos -> Policies \cup {"Bad", "Miss"}]
    /\ cacheRev \in [Silos -> 0..MaxChanges]
    /\ loading \in [Silos -> BOOLEAN]
    /\ loadVal \in [Silos -> Policies \cup {"Bad"}]
    /\ loadRev \in [Silos -> 0..MaxChanges]
    /\ overtaken \in [Silos -> BOOLEAN]
    /\ pending \in [Silos -> BOOLEAN]
    /\ data \in {0} \cup Values
    /\ commits \in 0..MaxWrites /\ writes \in 0..MaxWrites
    /\ judge \in Policies \cup {"Bad", "Miss"}
    /\ outcome \in {"None", "Accepted", "Rejected"}

\* A policy that does not compile is never durable.
StoredPolicyCompiles == stored # "Bad"

\* A cached setting is current, or an eviction for it is still on its way.
CacheCurrentOrPending ==
    \A s \in Silos : cache[s] # "Miss" => (cacheRev[s] = rev \/ pending[s])

\* The change's own silo has evicted before the admin call returns, and no load
\* it had in flight can install the pre-change setting.
AdminSiloCurrentAtAck ==
    [][(admin = "Committed" /\ admin' = "Idle")
       => /\ (cache'[adminAt] = "Miss" \/ cacheRev'[adminAt] = rev')
          /\ ~pending'[adminAt]
          /\ (loading'[adminAt] => (overtaken'[adminAt] \/ loadRev'[adminAt] = rev'))]_vars

\* A write is committed only when the setting that judged it accepts it.
AcceptedWriteValid == [][commits' # commits => Valid(judge', data')]_vars

\* A rejected write changes no data.
RejectLeavesNoMutation ==
    [][(writes' # writes /\ outcome' = "Rejected")
       => (data' = data /\ commits' = commits)]_vars

\* Every silo's stale setting is eventually evicted or refreshed.
StaleEventuallyEvicted ==
    \A s \in Silos :
        (cache[s] # "Miss" /\ cacheRev[s] # rev) ~> (cache[s] = "Miss" \/ cacheRev[s] = rev)

Init ==
    /\ stored = "None" /\ rev = 0
    /\ admin = "Idle" /\ adminAt = 1
    /\ cache = [s \in Silos |-> "Miss"]
    /\ cacheRev = [s \in Silos |-> 0]
    /\ loading = [s \in Silos |-> FALSE]
    /\ loadVal = [s \in Silos |-> "None"]
    /\ loadRev = [s \in Silos |-> 0]
    /\ overtaken = [s \in Silos |-> FALSE]
    /\ pending = [s \in Silos |-> FALSE]
    /\ data = 0 /\ commits = 0 /\ writes = 0
    /\ judge = "Miss" /\ outcome = "None"

\* The admin on silo s commits a set (A, B) or a clear (None) to the reserved
\* tree; the change is owed to every silo's cache.
Change(s, p) ==
    /\ admin = "Idle" /\ rev < MaxChanges
    /\ stored' = p /\ rev' = rev + 1
    /\ admin' = "Committed" /\ adminAt' = s
    /\ pending' = [t \in Silos |-> TRUE]
    /\ UNCHANGED <<cache, cacheRev, loading, loadVal, loadRev, overtaken,
                   data, commits, writes, judge, outcome>>

\* A policy that does not compile is refused before the durable write.
ChangeRefused ==
    /\ admin = "Idle"
    /\ UNCHANGED vars

\* The admin evicts its own silo's entry, then returns.
Acknowledge ==
    /\ admin = "Committed"
    /\ admin' = "Idle"
    /\ cache' = [cache EXCEPT ![adminAt] = "Miss"]
    /\ pending' = [pending EXCEPT ![adminAt] = FALSE]
    /\ overtaken' = [overtaken EXCEPT ![adminAt] = @ \/ loading[adminAt]]
    /\ UNCHANGED <<stored, rev, adminAt, cacheRev, loading, loadVal, loadRev,
                   data, commits, writes, judge, outcome>>

\* Environment: the change reaches silo s and evicts its entry, overtaking any
\* load it has in flight.
Propagate(s) ==
    /\ pending[s]
    /\ pending' = [pending EXCEPT ![s] = FALSE]
    /\ cache' = [cache EXCEPT ![s] = "Miss"]
    /\ overtaken' = [overtaken EXCEPT ![s] = @ \/ loading[s]]
    /\ UNCHANGED <<stored, rev, admin, adminAt, cacheRev, loading, loadVal,
                   loadRev, data, commits, writes, judge, outcome>>

\* A cache miss on silo s reads the durable setting.
BeginLoad(s) ==
    /\ cache[s] = "Miss" /\ ~loading[s]
    /\ loading' = [loading EXCEPT ![s] = TRUE]
    /\ loadVal' = [loadVal EXCEPT ![s] = stored]
    /\ loadRev' = [loadRev EXCEPT ![s] = rev]
    /\ overtaken' = [overtaken EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<stored, rev, admin, adminAt, cache, cacheRev, pending,
                   data, commits, writes, judge, outcome>>

\* The load completes; it installs its result unless an eviction overtook it.
InstallLoad(s) ==
    /\ loading[s]
    /\ loading' = [loading EXCEPT ![s] = FALSE]
    /\ cache' = IF overtaken[s] THEN cache ELSE [cache EXCEPT ![s] = loadVal[s]]
    /\ cacheRev' = IF overtaken[s] THEN cacheRev ELSE [cacheRev EXCEPT ![s] = loadRev[s]]
    /\ UNCHANGED <<stored, rev, admin, adminAt, loadVal, loadRev, overtaken,
                   pending, data, commits, writes, judge, outcome>>

\* A local write through silo s is judged by that silo's cached setting before
\* it reaches the WAL.
Write(s, v) ==
    /\ writes < MaxWrites /\ cache[s] # "Miss"
    /\ writes' = writes + 1 /\ judge' = cache[s]
    /\ IF Valid(cache[s], v)
       THEN /\ data' = v /\ commits' = commits + 1 /\ outcome' = "Accepted"
       ELSE /\ outcome' = "Rejected"
            /\ UNCHANGED <<data, commits>>
    /\ UNCHANGED <<stored, rev, admin, adminAt, cache, cacheRev, loading,
                   loadVal, loadRev, overtaken, pending>>

Stutter == UNCHANGED vars

Next ==
    \/ \E s \in Silos, p \in Policies : Change(s, p)
    \/ ChangeRefused
    \/ Acknowledge
    \/ \E s \in Silos : Propagate(s)
    \/ \E s \in Silos : BeginLoad(s)
    \/ \E s \in Silos : InstallLoad(s)
    \/ \E s \in Silos, v \in Values : Write(s, v)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Acknowledge)
    /\ \A s \in Silos : WF_vars(Propagate(s))
====
