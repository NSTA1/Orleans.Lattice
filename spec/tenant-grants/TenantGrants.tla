---------------------------- MODULE TenantGrants ----------------------------
(***************************************************************************)
(* The cross-tenant grant protocol (issue #6050): the two-party agreement *)
(* behind ILatticeTenantGrantAdmin - an offer by the granting tenant, an  *)
(* approval or rejection by the grantee, a revocation by either party or  *)
(* an operator - and its consumer, the tenancy engine's cross-tenant read  *)
(* resolution at the auth gate and the gate's existence probe.             *)
(*                                                                         *)
(* Every caller's own authority is the abstract operator                   *)
(* Allowed(caller, op, scope) of the F1 contract (spec/auth-decision,      *)
(* contract version 1). The cfg binds it to ContractAllowed, the           *)
(* contract's minimal grant-set definition.                                *)
(*                                                                         *)
(* Snapshot freshness is TenantIsolation's epoch and lease protocol        *)
(* (spec/tenant-isolation), in the form that module guarantees: a registry *)
(* write returns only once a peer silo that held its snapshot              *)
(* authoritative has been told or has let its lease lapse, and a failed    *)
(* publication returns early and owes a re-publish. That owed re-publish   *)
(* is the only staleness window a read admitted after an acknowledged      *)
(* revocation may observe.                                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANT Allowed(_, _, _)

\* ---- F1 contract vocabulary (contract version 1), the subset used here ----
NoKey == "none"
NoTenant == "none"
Tenants == {"A", "B"}
Admins == {"op", "adminA", "adminB"}
Readers == {"rB", "rN", "rZ"}
Callers == Admins \cup Readers
BootstrapAdministrators == {}
AdminOp == "Admin"
ReadOp == "Read"
Tree(kind, t) == [kind |-> kind, tenant |-> t]
PolicyTree == Tree("reserved", NoTenant)
TenantCapability(t) == Tree("tenantCapability", t)
Scope(tree, key) == [tree |-> tree, key |-> key]
\* A point read of a key in one of tenant A's trees.
ReadScope == Scope(Tree("tenant", "A"), "k1")

\* The platform operator holds Admin on the policy tree; each tenant's
\* delegated admin holds Admin on its own capability scope. rB and rZ hold an
\* operator-layer Read on tenant A's trees; rN holds nothing. No bootstrap
\* administrator, so every allow comes from a grant.
Grants ==
    { [caller |-> "op", op |-> AdminOp, scope |-> Scope(PolicyTree, NoKey)],
      [caller |-> "adminA", op |-> AdminOp, scope |-> Scope(TenantCapability("A"), NoKey)],
      [caller |-> "adminB", op |-> AdminOp, scope |-> Scope(TenantCapability("B"), NoKey)],
      [caller |-> "rB", op |-> ReadOp, scope |-> ReadScope],
      [caller |-> "rZ", op |-> ReadOp, scope |-> ReadScope] }
ContractAllowed(c, op, s) ==
    c \in BootstrapAdministrators \/ [caller |-> c, op |-> op, scope |-> s] \in Grants

IsOperator(c) == Allowed(c, AdminOp, Scope(PolicyTree, NoKey))
HoldsAdmin(c, t) == Allowed(c, AdminOp, Scope(TenantCapability(t), NoKey))
\* The side each step belongs to: tenant A grants, tenant B receives.
GranterSide(c) == IsOperator(c) \/ HoldsAdmin(c, "A")
GranteeSide(c) == IsOperator(c) \/ HoldsAdmin(c, "B")

\* ---- The instance ----
\* The members of B that active-tenant validation admits. rZ holds the same
\* Read as rB but is no member of B, so it may not act as B.
Members == {"rB", "rN"}
\* The facades run, and every registry write commits, on the first silo,
\* whose decisions confirm against the registry; the second is the peer
\* whose compiled snapshot can lag.
Silos == {"s1", "s2"}
Peer == "s2"
\* Tree names under t/A/. The grant's scope is orders; other is a sibling
\* tree the scope does not cover.
Names == {"orders", "other"}
MaxWrites == 3
MaxGen == MaxWrites
MaxInc == 1
MaxFailures == 1

\* ---- The grant slot and the record join ----
States == {"pending", "active", "rejected", "revoked", "removed"}
Rank(st) ==
    CASE st = "pending" -> 0 [] st = "active" -> 1 [] st = "rejected" -> 2
      [] st = "revoked" -> 3 [] st = "removed" -> 4
Slot(gen, st) == [ex |-> TRUE, gen |-> gen, st |-> st]
NoSlot == [ex |-> FALSE, gen |-> 0, st |-> "removed"]
SlotSet == {NoSlot} \cup [ex : {TRUE}, gen : 0..MaxGen, st : States]
Live(x) == x.ex /\ x.st # "removed"
\* The later agreement generation wins outright; within one generation the
\* more restrictive state wins, and a removal outranks every state.
Merge(a, b) ==
    IF ~a.ex THEN b
    ELSE IF ~b.ex THEN a
    ELSE IF a.gen # b.gen THEN (IF a.gen > b.gen THEN a ELSE b)
    ELSE IF Rank(a.st) >= Rank(b.st) THEN a ELSE b
\* Every offer that writes begins a new agreement generation.
OfferSlot(x) == IF x.ex THEN Slot(x.gen + 1, "pending") ELSE Slot(0, "pending")
Legal(from, to) ==
    <<from, to>> \in {<<"pending", "active">>, <<"pending", "rejected">>, <<"active", "revoked">>}

Status == {"active", "absent"}

\* ---- Observations of the last step ----
\* An observation carries each property's verdict on the step that made it,
\* judged by the operators below from what the step did and from who the
\* caller and what the view were, never from the decision under test.
\* Keeping only the verdicts stops refusals and reads multiplying states.
Verbs == {"offer", "approve", "reject", "revoke"}
AnswerVerbs == {"approve", "reject", "revoke"}
NoObs == [kind |-> "none"]
NoCall == [verb |-> "none"]
Obs ==
    {NoObs}
    \cup [kind : {"call"}, scoped : BOOLEAN, discreet : BOOLEAN, untrapped : BOOLEAN]
    \cup [kind : {"read"}, requires : BOOLEAN, effect : BOOLEAN, discreet : BOOLEAN]
    \cup [kind : {"probe"}, discreet : BOOLEAN]
Calls ==
    {NoCall}
    \cup [verb : Verbs, caller : Admins, auth : {"ok"}, present : BOOLEAN, preActive : BOOLEAN,
          cp : SlotSet, target : States]

VARIABLES status, incB, grant, seen, trusted, wstate, owed, captured, ack, closed,
          offers, approvals, fl, budget, failures, obs
vars == <<status, incB, grant, seen, trusted, wstate, owed, captured, ack, closed,
          offers, approvals, fl, budget, failures, obs>>

CanWrite == wstate = "idle"

\* Tenant-admin authority as the facade decides it: an operator is trusted to
\* learn that a tenant is not registered; anyone else is an admin of a
\* registered tenant, or is denied.
AuthorizeAdmin(c, t) ==
    IF IsOperator(c) THEN (IF status[t] = "active" THEN "ok" ELSE "tenantNotFound")
    ELSE IF status[t] = "active" /\ HoldsAdmin(c, t) THEN "ok"
    ELSE "denied"
\* The non-throwing probe revoke makes of the granting side: an unregistered
\* tenant is a denial for every caller, operators included.
TryAuthorizeAdmin(c, t) ==
    status[t] = "active" /\ (IsOperator(c) \/ HoldsAdmin(c, t))

\* The authority of the side a step belongs to: an offer the granting
\* tenant's, an approval or rejection the grantee's, a revocation either's.
SideOk(verb, c) ==
    CASE verb = "offer" -> GranterSide(c)
      [] verb \in {"approve", "reject"} -> GranteeSide(c)
      [] OTHER -> GranterSide(c) \/ GranteeSide(c)
\* An operator, or an admin of a registered party to the grant.
Party(c) == IsOperator(c) \/ \E t \in Tenants : HoldsAdmin(c, t) /\ status[t] = "active"

\* AdminScopedToOwnSide's verdict on a call by c: a write is made with the
\* authority of the side the step belongs to.
CallScoped(verb, c, wrote) == wrote => SideOk(verb, c)
\* NonDisclosure's verdict on a call by c whose authorization answered a: a
\* denied caller is told only that and writes nothing; only an operator
\* learns that a tenant is not registered; an authorized answer to a grant
\* that is not there is a not-found, whether it was never offered or its
\* granting tenant is not registered.
CallDiscreet(verb, c, a, present, result, wrote) ==
    /\ a = "denied" => (result = "denied" /\ ~wrote)
    /\ a = "tenantNotFound" => (IsOperator(c) /\ ~wrote)
    /\ (verb \in AnswerVerbs /\ a = "ok" /\ ~present) => result = "notFound"
\* NoUnilateralTrap's verdict: a party's revocation of an active grant is
\* never refused, and succeeds unless the grant was removed meanwhile.
CallUntrapped(verb, c, present, preActive, result) ==
    (verb = "revoke" /\ Party(c) /\ present /\ preActive) => result \in {"ok", "notFound"}
CallObs(verb, c, a, present, preActive, result, wrote) ==
    [kind |-> "call",
     scoped |-> CallScoped(verb, c, wrote),
     discreet |-> CallDiscreet(verb, c, a, present, result, wrote),
     untrapped |-> CallUntrapped(verb, c, present, preActive, result)]

\* The reader's own authorization, its right to act as the grantee, and an
\* active grant from a registered granting tenant covering tree t/A/n, with
\* the grant as slot x shows it.
Entitled(rdr, n, x) ==
    /\ Allowed(rdr, ReadOp, ReadScope)
    /\ rdr \in Members /\ status["B"] = "active"
    /\ status["A"] = "active" /\ Live(x) /\ x.st = "active" /\ n = "orders"
\* The reasons a refused read may be given: the policy decision's, the
\* unconfirmable window's, one for any active-tenant refusal and one for any
\* refused crossing. None depends on whether a tenant is registered.
ReasonClasses == {"policy", "unconfirmed", "activeTenant", "crossing"}

\* ReadRequiresActiveGrant's, RevocationTakesEffect's and NonDisclosure's
\* verdicts on a read that saw the grant as slot x and was refused with
\* reason: an admitted read is entitled, sees a closed agreement as active
\* only while a failed publication is owed, and a refused one is told only
\* the stage that refused it.
ReadObs(admitted, reason, rdr, n, x) ==
    [kind |-> "read",
     requires |-> admitted => Entitled(rdr, n, x),
     effect |-> (admitted /\ x.gen \in closed) => owed,
     discreet |-> ~admitted => reason \in ReasonClasses]

Init ==
    /\ status = [t \in Tenants |-> "active"]
    /\ incB = 0
    /\ grant = NoSlot
    /\ seen = NoSlot
    /\ trusted = FALSE
    /\ wstate = "idle"
    /\ owed = FALSE
    /\ captured = FALSE
    /\ ack = {}
    /\ closed = {}
    /\ offers = {}
    /\ approvals = {}
    /\ fl = NoCall
    /\ budget = MaxWrites
    /\ failures = 0
    /\ obs = NoObs

\* A registry write publishes a new epoch: a peer holding its snapshot
\* authoritative is captured, to be told or to have its lease waited out.
\* acks are the agreements the write closes once it returns.
Publish(acks) ==
    /\ captured' = trusted
    /\ wstate' = "pub"
    /\ ack' = acks

Refuse(verb, c, a, present, preActive, result) ==
    /\ obs' = CallObs(verb, c, a, present, preActive, result, FALSE)
    /\ UNCHANGED <<status, incB, grant, seen, trusted, wstate, owed, captured, ack, closed,
                   offers, approvals, fl, budget, failures>>

\* The registry's read-merge-write: m is the caller's copy of the granting
\* tenant's record, cp, joined with whatever is committed. The call reports the
\* merged state and refuses when it is not the state it asked for. The ghost
\* sets record the agreements each side wrote with that side's authority.
Commit(verb, c, a, present, preActive, cp, m, target) ==
    LET result == IF ~Live(m) THEN "notFound" ELSE IF m.st = target THEN "ok" ELSE "conflict"
    IN /\ CanWrite
       /\ budget > 0
       /\ grant' = m
       /\ budget' = budget - 1
       /\ Publish(IF verb = "revoke" /\ result = "ok" THEN {m.gen} ELSE {})
       /\ offers' = IF verb = "offer" /\ GranterSide(c) THEN offers \cup {cp.gen} ELSE offers
       /\ approvals' = IF verb = "approve" /\ GranteeSide(c) THEN approvals \cup {<<cp.gen, incB>>} ELSE approvals
       /\ obs' = CallObs(verb, c, a, present, preActive, result, TRUE)
       /\ UNCHANGED <<status, incB, seen, trusted, owed, closed, failures>>

\* A write either commits at once or, split, reads now and commits later
\* (CommitCall), so another call or a deletion can land between its read and
\* its write.
Write(verb, c, a, present, preActive, cp, target, split) ==
    IF split
    THEN /\ fl = NoCall
         /\ fl' = [verb |-> verb, caller |-> c, auth |-> a, present |-> present,
                   preActive |-> preActive, cp |-> cp, target |-> target]
         /\ obs' = NoObs
         /\ UNCHANGED <<status, incB, grant, seen, trusted, wstate, owed, captured, ack,
                        closed, offers, approvals, budget, failures>>
    ELSE /\ Commit(verb, c, a, present, preActive, cp, Merge(grant, cp), target)
         /\ UNCHANGED fl

\* LatticeTenantGrantAdmin.OfferGrantAsync: the granting tenant's authority,
\* then the lifecycle's offer rule; an identical unanswered offer is a no-op
\* and amended terms begin a new generation.
Offer(c, amend) ==
    LET a == AuthorizeAdmin(c, "A")
        present == status["A"] = "active" /\ Live(grant)
        preActive == Live(grant) /\ grant.st = "active"
    IN IF a # "ok" THEN Refuse("offer", c, a, present, preActive, a)
       ELSE IF preActive THEN Refuse("offer", c, a, present, preActive, "illegal")
       ELSE IF Live(grant) /\ grant.st = "pending" /\ ~amend
            THEN Refuse("offer", c, a, present, preActive, "unchanged")
       ELSE Write("offer", c, a, present, preActive, OfferSlot(grant), "pending", FALSE)

\* LatticeTenantGrantAdmin.ApproveGrantAsync and RejectGrantAsync: the
\* grantee's authority first, then the granting tenant's record, where an
\* unregistered granting tenant reads exactly as an unoffered grant.
Answer(c, target, split) ==
    LET verb == IF target = "active" THEN "approve" ELSE "reject"
        a == AuthorizeAdmin(c, "B")
        present == status["A"] = "active" /\ Live(grant)
        preActive == Live(grant) /\ grant.st = "active"
    IN IF a # "ok" THEN Refuse(verb, c, a, present, preActive, a)
       ELSE IF ~present THEN Refuse(verb, c, a, present, preActive, "notFound")
       ELSE IF grant.st = target THEN Refuse(verb, c, a, present, preActive, "unchanged")
       ELSE IF ~Legal(grant.st, target) THEN Refuse(verb, c, a, present, preActive, "illegal")
       ELSE Write(verb, c, a, present, preActive, Slot(grant.gen, target), target, split)

\* LatticeTenantGrantAdmin.RevokeGrantAsync: the granting side is probed
\* without throwing, then the grantee side is authorized, so either party
\* (or an operator) may walk away.
Revoke(c, split) ==
    LET a == IF TryAuthorizeAdmin(c, "A") THEN "ok" ELSE AuthorizeAdmin(c, "B")
        present == status["A"] = "active" /\ Live(grant)
        preActive == Live(grant) /\ grant.st = "active"
    IN IF a # "ok" THEN Refuse("revoke", c, a, present, preActive, a)
       ELSE IF ~present THEN Refuse("revoke", c, a, present, preActive, "notFound")
       ELSE IF grant.st = "revoked" THEN Refuse("revoke", c, a, present, preActive, "unchanged")
       ELSE IF ~Legal(grant.st, "revoked") THEN Refuse("revoke", c, a, present, preActive, "illegal")
       ELSE Write("revoke", c, a, present, preActive, Slot(grant.gen, "revoked"), "revoked", split)

\* The write half of a split call: its copy, read before the writes that
\* landed since, is merged by the record join. A grant record arriving by
\* replication or restore is merged by the same join. The write is an update:
\* when the granting tenant was deleted since the read it is refused, as an
\* unoffered grant, rather than re-creating the tenant from the stale copy.
CommitCall ==
    /\ fl # NoCall
    /\ IF status["A"] = "active"
       THEN Commit(fl.verb, fl.caller, fl.auth, fl.present, fl.preActive, fl.cp, Merge(grant, fl.cp), fl.target)
       ELSE /\ obs' = CallObs(fl.verb, fl.caller, fl.auth, fl.present, fl.preActive, "notFound", FALSE)
            /\ UNCHANGED <<status, grant>>
            /\ UNCHANGED <<incB, seen, trusted, wstate, owed, captured, ack, closed, offers,
                           approvals, budget, failures>>
    /\ fl' = NoCall

\* A reader acting as tenant B reads tree t/A/n on silo s. The policy
\* decision comes first and only an allow reaches the tenant enforcer, which
\* trusts an authoritative snapshot, confirms against the registry when that
\* is reachable (r), and otherwise denies. The enforcer validates the
\* reader's right to act as B, then resolves an active grant from A covering
\* the tree.
Read(s, rdr, n, r) ==
    LET auth == s = Peer /\ trusted
        x == IF auth THEN seen ELSE grant
        pol == Allowed(rdr, ReadOp, ReadScope)
        decided == auth \/ r
        actsAsB == rdr \in Members /\ status["B"] = "active"
        grantOk == status["A"] = "active" /\ Live(x) /\ x.st = "active" /\ n = "orders"
        admitted == pol /\ decided /\ actsAsB /\ grantOk
        activeTenantReason == "activeTenant"
        crossingReason == "crossing"
        reason == IF ~pol THEN "policy" ELSE IF ~decided THEN "unconfirmed"
                  ELSE IF ~actsAsB THEN activeTenantReason ELSE crossingReason
    IN /\ obs' = ReadObs(admitted, reason, rdr, n, x)
       /\ UNCHANGED <<status, incB, grant, seen, trusted, wstate, owed, captured, ack,
                      closed, offers, approvals, fl, budget, failures>>

\* The gate's existence probe (PolicyAccessGate.HasAnyGrantAsync): a policy
\* grant reveals the tree only when the tenant enforcer, confirming against
\* the registry, would admit the reader too.
Probe(rdr, n) ==
    LET pol == Allowed(rdr, ReadOp, ReadScope)
        actsAsB == status["B"] = "active" /\ rdr \in Members
        grantOk == status["A"] = "active" /\ Live(grant) /\ grant.st = "active" /\ n = "orders"
        visible == pol /\ actsAsB /\ grantOk
    IN /\ obs' = [kind |-> "probe", discreet |-> visible => Entitled(rdr, n, grant)]
       /\ UNCHANGED <<status, incB, grant, seen, trusted, wstate, owed, captured, ack,
                      closed, offers, approvals, fl, budget, failures>>

\* The epoch push reaches the captured peer, which stops trusting its
\* snapshot.
Deliver ==
    /\ captured
    /\ captured' = FALSE
    /\ trusted' = FALSE
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, seen, wstate, owed, ack, closed, offers, approvals,
                   fl, budget, failures>>

\* The peer's lease lapses, margin included: it no longer trusts its
\* snapshot, and a publication that captured it no longer waits for it.
Lapse ==
    /\ trusted
    /\ trusted' = FALSE
    /\ captured' = FALSE
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, seen, wstate, owed, ack, closed, offers, approvals,
                   fl, budget, failures>>

\* The captured peer was told or lapsed: the write returns covered, and any
\* owed re-publish is settled.
Complete ==
    /\ wstate = "pub"
    /\ ~captured
    /\ wstate' = "idle"
    /\ owed' = FALSE
    /\ closed' = closed \cup ack
    /\ ack' = {}
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, seen, trusted, captured, offers, approvals, fl,
                   budget, failures>>

\* The publication fails: the durable write returns anyway, ahead of its
\* coverage, and owes a background re-publish (TenantIsolation's
\* PublicationWindowTracked window).
Fail ==
    /\ wstate = "pub"
    /\ failures < MaxFailures
    /\ failures' = failures + 1
    /\ wstate' = "idle"
    /\ owed' = TRUE
    /\ captured' = FALSE
    /\ closed' = closed \cup ack
    /\ ack' = {}
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, seen, trusted, offers, approvals, fl, budget>>

\* The background loop re-publishes the owed advance.
Retry ==
    /\ wstate = "idle"
    /\ owed
    /\ wstate' = "pub"
    /\ captured' = trusted
    /\ ack' = {}
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, seen, trusted, owed, closed, offers, approvals, fl,
                   budget, failures>>

\* The peer renews its lease and rebuilds from the registry as it stands.
Rebuild ==
    /\ ~trusted
    /\ seen' = grant
    /\ trusted' = TRUE
    /\ obs' = NoObs
    /\ UNCHANGED <<status, incB, grant, wstate, owed, captured, ack, closed, offers,
                   approvals, fl, budget, failures>>

\* LatticeTenantAdmin.DeleteTenantAsync over the registry delete. Deleting
\* the grantee removes the grant issued to it from the granting tenant's
\* record; deleting the granting tenant removes its record, and the grant on
\* it.
DeleteTenant(t) ==
    /\ status[t] = "active"
    /\ CanWrite
    /\ status' = [status EXCEPT ![t] = "absent"]
    /\ grant' = IF t = "A" THEN NoSlot
                ELSE IF Live(grant) THEN Slot(grant.gen, "removed") ELSE grant
    /\ Publish({})
    /\ obs' = NoObs
    /\ UNCHANGED <<incB, seen, trusted, owed, closed, offers, approvals, fl, budget, failures>>

\* LatticeTenantAdmin.CreateTenantAsync re-creating the grantee: only an
\* unregistered id is created, and as a fresh record, a new incarnation.
Recreate ==
    /\ status["B"] = "absent"
    /\ incB < MaxInc
    /\ CanWrite
    /\ status' = [status EXCEPT !["B"] = "active"]
    /\ incB' = incB + 1
    /\ Publish({})
    /\ obs' = NoObs
    /\ UNCHANGED <<grant, seen, trusted, owed, closed, offers, approvals, fl, budget, failures>>

\* Only the operator's approvals and revocations are split: the operator
\* holds both sides' authority, so its read-to-write window meets every
\* interleaving a tenant admin's would, without multiplying the callers that
\* can hold one open.
Splits(c) == IF c = "op" THEN BOOLEAN ELSE {FALSE}

Next ==
    \/ \E c \in Admins, amend \in BOOLEAN : Offer(c, amend)
    \/ \E c \in Admins, target \in {"active", "rejected"} : \E split \in Splits(c) : Answer(c, target, split)
    \/ \E c \in Admins : \E split \in Splits(c) : Revoke(c, split)
    \/ CommitCall
    \/ \E s \in Silos, rdr \in Readers, n \in Names, r \in BOOLEAN : Read(s, rdr, n, r)
    \/ \E rdr \in Readers, n \in Names : Probe(rdr, n)
    \/ Deliver
    \/ Lapse
    \/ Complete
    \/ Fail
    \/ Retry
    \/ Rebuild
    \/ \E t \in Tenants : DeleteTenant(t)
    \/ Recreate

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ status \in [Tenants -> Status]
    /\ incB \in 0..MaxInc
    /\ grant \in SlotSet
    /\ seen \in SlotSet
    /\ trusted \in BOOLEAN
    /\ wstate \in {"idle", "pub"}
    /\ owed \in BOOLEAN
    /\ captured \in BOOLEAN
    /\ ack \subseteq 0..MaxGen
    /\ closed \subseteq 0..MaxGen
    /\ offers \subseteq 0..MaxGen
    /\ approvals \subseteq (0..MaxGen) \X (0..MaxInc)
    /\ fl \in Calls
    /\ budget \in 0..MaxWrites
    /\ failures \in 0..MaxFailures
    /\ obs \in Obs

\* A grant is active only once the granting side offered, and the grantee
\* side approved, that very agreement, the approval in the grantee's current
\* incarnation: no single tenant admin activates a grant exposing another
\* tenant's data, and a re-created grantee inherits nothing.
TwoPartyActivation ==
    (Live(grant) /\ grant.st = "active") =>
        /\ grant.gen \in offers
        /\ <<grant.gen, incB>> \in approvals

\* Every grant write is made with the authority of the side the step belongs
\* to (CallScoped).
AdminScopedToOwnSide ==
    obs.kind = "call" => obs.scoped

\* A crossing read is admitted only under the reader's own authorization,
\* its right to act as the grantee, and an active grant from a registered
\* granting tenant whose scope covers the tree (Entitled).
ReadRequiresActiveGrant ==
    obs.kind = "read" => obs.requires

\* Once a revocation has returned, a read admitted later observes the
\* revoked agreement as active only while a failed publication is still owed
\* a re-publish: the staleness bound is TenantIsolation's
\* PublicationWindowTracked window, and no other.
RevocationTakesEffect ==
    obs.kind = "read" => obs.effect

\* An unauthorized caller is told only that it was denied, and an authorized
\* one cannot tell an unoffered grant from an unregistered granting tenant
\* (CallDiscreet); a refused read is told only the stage that refused it,
\* never whether a tenant is registered (ReadObs); the existence probe never
\* reveals a tree to a reader a read would refuse.
NonDisclosure ==
    obs.kind \in {"call", "probe", "read"} => obs.discreet

\* Either party can always walk away (CallUntrapped).
NoUnilateralTrap ==
    obs.kind = "call" => obs.untrapped

\* A deleted tenant comes back only as a new incarnation created on purpose,
\* never re-created by a write that read its record before the deletion.
DeletionIsFinal ==
    [][\A t \in Tenants :
          (status[t] = "absent" /\ status'[t] = "active") => (t = "B" /\ incB' = incB + 1)]_vars
=============================================================================
