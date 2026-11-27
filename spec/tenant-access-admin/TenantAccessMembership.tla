----------------------- MODULE TenantAccessMembership -----------------------
(***************************************************************************)
(* The concurrency half of delegated tenant-access administration (epic   *)
(* #4154, issue #6191): concurrent admin-set and member-set removals and   *)
(* tenant group deletion through the tenant registry's optimistic          *)
(* compare-and-set loop, removals stamped by silos whose clocks are        *)
(* skewed, and admin-set, member-set and role-binding entries that arrive  *)
(* by replication or restore, including entries that name another         *)
(* tenant's group.                                                         *)
(*                                                                         *)
(* Authorization is no more than "the caller is a tenant admin of T":      *)
(* every operation here is one the tenant-access facades have already      *)
(* authorized, so the module has no caller dimension. The authorization    *)
(* half is the sibling module TenantAccessAuthorization in this directory. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

\* ---- The administered tenant A and its entries ----
\* "u1" and "u2" are users, "cg" a cluster group, "t/A/g" tenant A's own
\* group, "t/B/g" tenant B's group and "t/bad" a malformed t/ id. A user or
\* cluster group, or A's own well-formed group, may count for A; another
\* tenant's group and a malformed t/ id never may (TenantAccessEntries).
Entries == {"u1", "u2", "cg", "t/A/g", "t/B/g", "t/bad"}
OwnGroup == "t/A/g"
IsTier(e) == e \in {"t/A/g", "t/B/g", "t/bad"}
Admissible(e) == ~IsTier(e) \/ e = OwnGroup
Sets == {"admin", "member"}
Silos == {"lag", "lead"}
Procs == {"p1", "p2"}
\* Removals run on the lagging silo and adds on the leading one, whose clock
\* starts ahead; a peer region stamps arrivals ahead of both. That is the
\* skew a removal must survive: a slot written ahead of the remover's clock.
InitClock == [s \in Silos |-> IF s = "lag" THEN 0 ELSE 3]
PeerStamp == 5
MaxStamp == 8
MaxWrites == 3

Slot(p, ts) == [p |-> p, ts |-> ts]
Absent == Slot(FALSE, 0)
Slots == [p : BOOLEAN, ts : 0..MaxStamp]
\* TenantSubjectSlot.Merge: the later stamp wins. A tie is broken by writer
\* id in production; the model gives it to the incumbent, the outcome a
\* removal can least afford.
MergeSlot(incumbent, incoming) == IF incoming.ts > incumbent.ts THEN incoming ELSE incumbent

LiveCount(set) == Cardinality({e \in Entries : set[e].p})
AdmissibleCount(set) == Cardinality({e \in Entries : set[e].p /\ Admissible(e)})

\* TenantRemovalStamp: strictly later than both the local clock and the
\* slot it supersedes.
RemovalStamp(now, slot) == IF now + 1 > slot.ts THEN now + 1 ELSE slot.ts + 1

\* Role bindings of an app installed in tenant A. AppRoleCompiler refuses
\* the whole compilation when any binding names a group outside A, so a
\* mismatched binding confers no role at all.
BindGroups == {"cg", "t/A/g", "t/B/g", "t/bad"}
Bindable(g) == Admissible(g)

\* ---- Observations of the last step ----
NoOp == [kind |-> "none"]
Kinds == {"removeAdmin", "removeMember", "deleteGroup"}
Results == {"unchanged", "refused", "ok"}
\* Arriving slots: another tenant's group in either set, and the tenant's
\* own entries re-stamped ahead of both silos. A malformed t/ id is judged
\* by the same admissibility test as another tenant's group, so the
\* role-binding arrivals carry it instead.
ArrivalSlots ==
    {<<"admin", "u1">>, <<"admin", OwnGroup>>, <<"admin", "t/B/g">>,
     <<"member", "u2">>, <<"member", OwnGroup>>, <<"member", "t/B/g">>}
\* Membership probes <<subject id, one resolved group>>: an ordinary user
\* in the tenant's own group or in another tenant's group, and a subject
\* whose own id is shaped like another tenant's group.
Probes == {<<"u1", "t/A/g">>, <<"u1", "t/B/g">>, <<"t/B/g", "cg">>}
Obs ==
    {NoOp}
    \cup [kind : {"call"}, op : Kinds, result : Results, wrote : BOOLEAN]
    \cup [kind : {"count"}, conferred : SUBSET (Entries \cup BindGroups)]
Pending ==
    [kind : Kinds, entry : Entries, sets : SUBSET Sets,
     stamp : [Sets -> 0..MaxStamp], seen : [Sets -> 0..MaxStamp]]
None == [kind |-> "idle"]

VARIABLES slots, clock, groupExists, bindings, pending, removed, budget, obs
vars == <<slots, clock, groupExists, bindings, pending, removed, budget, obs>>

Init ==
    /\ slots = [x \in Sets |-> [e \in Entries |->
                   IF (x = "admin" /\ e \in {"u1", OwnGroup}) \/ (x = "member" /\ e \in {"u2", OwnGroup})
                   THEN Slot(TRUE, 1) ELSE Absent]]
    /\ clock = InitClock
    /\ groupExists = TRUE
    /\ bindings = {}
    /\ pending = [p \in Procs |-> None]
    /\ removed = {}
    /\ budget = MaxWrites
    /\ obs = NoOp

Answer(op, result) ==
    /\ obs' = [kind |-> "call", op |-> op, result |-> result, wrote |-> FALSE]
    /\ UNCHANGED <<slots, clock, groupExists, bindings, pending, removed, budget>>

\* LatticeTenantAccessAdmin.RemoveAdminSubjectAsync up to the commit: read
\* the record, the idempotent no-op, the last-admin pre-check, then a
\* removal stamped by TenantRemovalStamp.ForAdminEntry on silo s.
BeginRemoveAdmin(p, s, e) ==
    /\ pending[p].kind = "idle"
    /\ budget > 0
    /\ IF ~slots["admin"][e].p THEN Answer("removeAdmin", "unchanged")
       ELSE IF Admissible(e) /\ AdmissibleCount(slots["admin"]) <= 1 THEN Answer("removeAdmin", "refused")
       ELSE /\ pending' = [pending EXCEPT ![p] =
                  [kind |-> "removeAdmin", entry |-> e, sets |-> {"admin"},
                   stamp |-> [x \in Sets |-> IF x = "admin" THEN RemovalStamp(clock[s], slots["admin"][e]) ELSE 0],
                   seen |-> [x \in Sets |-> IF x = "admin" THEN slots["admin"][e].ts ELSE 0]]]
            /\ clock' = [clock EXCEPT ![s] = @ + 1]
            /\ budget' = budget - 1
            /\ obs' = NoOp
            /\ UNCHANGED <<slots, groupExists, bindings, removed>>

\* LatticeTenantDirectoryAdmin.RemoveMemberAsync up to the commit: the
\* member-set removal is stamped by TenantRemovalStamp.ForMemberEntry.
BeginRemoveMember(p, s, e) ==
    /\ pending[p].kind = "idle"
    /\ budget > 0
    /\ IF ~slots["member"][e].p THEN Answer("removeMember", "unchanged")
       ELSE /\ pending' = [pending EXCEPT ![p] =
                  [kind |-> "removeMember", entry |-> e, sets |-> {"member"},
                   stamp |-> [x \in Sets |-> IF x = "member" THEN RemovalStamp(clock[s], slots["member"][e]) ELSE 0],
                   seen |-> [x \in Sets |-> IF x = "member" THEN slots["member"][e].ts ELSE 0]]]
            /\ clock' = [clock EXCEPT ![s] = @ + 1]
            /\ budget' = budget - 1
            /\ obs' = NoOp
            /\ UNCHANGED <<slots, groupExists, bindings, removed>>

\* LatticeTenantDirectoryAdmin.RemoveGroupAsync up to the commit: the group
\* must exist, a group that is the last admin entry is refused, and its
\* member-set and admin-set entries are removed with superseding stamps.
BeginDeleteGroup(p, s) ==
    LET inSets == {x \in Sets : slots[x][OwnGroup].p}
    IN /\ pending[p].kind = "idle"
       /\ budget > 0
       /\ IF ~groupExists THEN Answer("deleteGroup", "unchanged")
          ELSE IF "admin" \in inSets /\ AdmissibleCount(slots["admin"]) <= 1 THEN Answer("deleteGroup", "refused")
          ELSE /\ pending' = [pending EXCEPT ![p] =
                     [kind |-> "deleteGroup", entry |-> OwnGroup, sets |-> inSets,
                      stamp |-> [x \in Sets |-> IF x \in inSets THEN RemovalStamp(clock[s], slots[x][OwnGroup]) ELSE 0],
                      seen |-> [x \in Sets |-> IF x \in inSets THEN slots[x][OwnGroup].ts ELSE 0]]]
               /\ clock' = [clock EXCEPT ![s] = @ + 1]
               /\ budget' = budget - 1
               /\ obs' = NoOp
               /\ UNCHANGED <<slots, groupExists, bindings, removed>>

\* One attempt of the registry's compare-and-set loop that commits: re-read
\* the stored record, merge this call's tombstones into it, and, when the
\* admin set is touched, re-apply the last-admin guard to the merged record
\* before the conditional write (TenantAdminSetCommit over
\* IGuardedTenantRegistry.PutGuardedAsync). A refused commit writes nothing.
Commit(p) ==
    LET op == pending[p]
        merged == [x \in Sets |-> IF x \in op.sets
                       THEN [slots[x] EXCEPT ![op.entry] = MergeSlot(@, Slot(FALSE, op.stamp[x]))]
                       ELSE slots[x]]
    IN /\ op.kind # "idle"
       /\ IF "admin" \in op.sets /\ AdmissibleCount(merged["admin"]) = 0
          THEN /\ pending' = [pending EXCEPT ![p] = None]
               /\ obs' = [kind |-> "call", op |-> op.kind, result |-> "refused", wrote |-> FALSE]
               /\ UNCHANGED <<slots, clock, groupExists, bindings, removed, budget>>
          ELSE /\ slots' = merged
               /\ removed' = removed \cup {[set |-> x, entry |-> op.entry, ts |-> op.seen[x]] : x \in op.sets}
               /\ groupExists' = IF op.kind = "deleteGroup" THEN FALSE ELSE groupExists
               /\ pending' = [pending EXCEPT ![p] = None]
               /\ obs' = [kind |-> "call", op |-> op.kind, result |-> "ok", wrote |-> TRUE]
               /\ UNCHANGED <<clock, bindings, budget>>

\* An admin-set or member-set add through a facade on silo s: an
\* admissible entry, stamped by the local clock, merged in one put.
AddEntry(s, x, e) ==
    /\ budget > 0
    /\ ~slots[x][e].p
    /\ e = OwnGroup => groupExists
    /\ slots' = [slots EXCEPT ![x][e] = MergeSlot(@, Slot(TRUE, clock[s] + 1))]
    /\ clock' = [clock EXCEPT ![s] = @ + 1]
    /\ budget' = budget - 1
    /\ obs' = NoOp
    /\ UNCHANGED <<groupExists, bindings, pending, removed>>

\* A record arriving by replication or restore, merged around the facades:
\* it may carry any entry, another tenant's group and a malformed id
\* included, stamped by a peer whose clock runs ahead.
ArriveEntry(x, e) ==
    /\ budget > 0
    /\ slots' = [slots EXCEPT ![x][e] = MergeSlot(@, Slot(TRUE, PeerStamp))]
    /\ budget' = budget - 1
    /\ obs' = NoOp
    /\ UNCHANGED <<clock, groupExists, bindings, pending, removed>>

\* An app registry record arriving by replication or restore with a role
\* binding the install facade would have refused.
ArriveBinding(g) ==
    /\ budget > 0
    /\ bindings' = bindings \cup {g}
    /\ budget' = budget - 1
    /\ obs' = NoOp
    /\ UNCHANGED <<slots, clock, groupExists, pending, removed>>

\* One membership decision for a subject carrying resolved groups: which
\* entries confer admin authority (TenantRecord.IsAdmin), membership
\* (TenantRecord.IsMember) or an app role (AppRoleCompiler).
Count(subj, groups) ==
    LET live(e) == slots["admin"][e].p \/ slots["member"][e].p
        exact == {e \in Entries : e = subj /\ ~IsTier(subj) /\ live(e)}
        viaGroup == {e \in Entries : e \in groups /\ Admissible(e) /\ live(e)}
        roles == IF \A b \in bindings : Bindable(b) THEN bindings \cap groups ELSE {}
    IN /\ obs' = [kind |-> "count", conferred |-> exact \cup viaGroup \cup roles]
       /\ UNCHANGED <<slots, clock, groupExists, bindings, pending, removed, budget>>

Next ==
    \/ \E p \in Procs, e \in {"u1", OwnGroup, "t/B/g"} : BeginRemoveAdmin(p, "lag", e)
    \/ \E p \in Procs, e \in {"u2", OwnGroup} : BeginRemoveMember(p, "lag", e)
    \/ \E p \in Procs : BeginDeleteGroup(p, "lag")
    \/ \E p \in Procs : Commit(p)
    \/ \E a \in {<<"admin", "u2">>, <<"member", "u1">>} : AddEntry("lead", a[1], a[2])
    \/ \E a \in ArrivalSlots : ArriveEntry(a[1], a[2])
    \/ \E g \in BindGroups : ArriveBinding(g)
    \/ \E q \in Probes : Count(q[1], {q[2]})

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ slots \in [Sets -> [Entries -> Slots]]
    /\ clock \in [Silos -> 0..MaxStamp]
    /\ groupExists \in BOOLEAN
    /\ bindings \subseteq BindGroups
    /\ pending \in [Procs -> Pending \cup {None}]
    /\ removed \subseteq [set : Sets, entry : Entries, ts : 0..MaxStamp]
    /\ budget \in 0..MaxWrites
    /\ obs \in Obs

\* No interleaving of concurrent removals, through the access facade or a
\* group deletion, leaves the tenant without an admin entry that counts for
\* it, and a refused removal writes nothing. An entry that never counts
\* (another tenant's group that arrived by replication) does not hold the
\* set open.
LastAdminGuard ==
    /\ AdmissibleCount(slots["admin"]) >= 1
    /\ (obs.kind = "call" /\ obs.result = "refused") => ~obs.wrote

\* A committed removal supersedes the slot it read, even when it was stamped
\* by the lagging silo and the slot by a silo or peer ahead of it.
RemovalWinsUnderSkew ==
    \A r \in removed : slots[r.set][r.entry].ts > r.ts

\* No admin-set, member-set or role-binding entry naming another tenant's
\* group, or a malformed t/ id, ever confers anything, whether it was written
\* by a facade or arrived by replication or restore; and a subject whose id
\* is shaped like a tenant group never matches an entry by exact id.
NoCrossTenantMembership ==
    obs.kind = "count" => \A e \in obs.conferred : Admissible(e)
=============================================================================
