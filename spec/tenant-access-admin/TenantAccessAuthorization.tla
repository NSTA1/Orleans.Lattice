---------------------- MODULE TenantAccessAuthorization ----------------------
(***************************************************************************)
(* The authorization half of delegated tenant-access administration       *)
(* (epic #4154, issue #6049): who may write a tenant-tier rule, where a    *)
(* tenant group may appear, which token claims survive the claim filter,   *)
(* what the delegated-access flag switches off, and the order in which    *)
(* every facade verb authorizes, gates, validates and writes.              *)
(*                                                                         *)
(* Authorization is the abstract operator Allowed(caller, op, scope) of    *)
(* the F1 contract (spec/auth-decision, contract version 1). The cfg binds *)
(* it to ContractAllowed, the contract's minimal grant-set definition.     *)
(* The concurrency half (last-admin CAS, removal stamps, replicated        *)
(* cross-tenant entries) is a sibling module in this directory.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANT Allowed(_, _, _)

\* ---- F1 contract vocabulary (contract version 1), the subset used here ----
NoKey == "none"
NoTenant == "none"
Tenants == {"A", "B"}
Default == "default"
Named == Tenants \cup {Default}
Callers == {"op", "adminA", "outsider"}
BootstrapAdministrators == {}
AdminOp == "Admin"
Tree(kind, t) == [kind |-> kind, tenant |-> t]
ClusterWideTreeId == Tree("clusterWide", NoTenant)
PolicyTree == Tree("reserved", NoTenant)
TenantCapability(t) == Tree("tenantCapability", t)
Scope(tree, key) == [tree |-> tree, key |-> key]

\* The platform operator holds Admin on the policy tree; tenant A's delegated
\* admin holds Admin on A's capability scope (the C4 admin tiers). Tenant B
\* has no delegated admin, so adminA acting on B is the cross-tenant case. No
\* bootstrap administrator, so every allow comes from a grant.
Grants ==
    { [caller |-> "op", op |-> AdminOp, scope |-> Scope(PolicyTree, NoKey)],
      [caller |-> "adminA", op |-> AdminOp, scope |-> Scope(TenantCapability("A"), NoKey)] }
ContractAllowed(c, op, s) ==
    c \in BootstrapAdministrators \/ [caller |-> c, op |-> op, scope |-> s] \in Grants

IsOperator(c) == Allowed(c, AdminOp, Scope(PolicyTree, NoKey))
TenantAdminAuth(c, t) ==
    IsOperator(c) \/ (t \in Tenants /\ Allowed(c, AdminOp, Scope(TenantCapability(t), NoKey)))

\* ---- Identifiers: users, cluster groups and the reserved t/ namespace ----
TierIds == {"t/A/g", "t/B/g", "t/default/x", "t/bad"}
WellFormed == {"t/A/g", "t/B/g"}
IsTier(id) == id \in TierIds
GroupTenant(g) == IF g = "t/A/g" THEN "A" ELSE "B"
TGroup(t) == CASE t = "A" -> "t/A/g" [] t = "B" -> "t/B/g" [] OTHER -> "t/default/x"
Resolvable == {"u1", "cg"}
ResolvableGroups == {"cg"}
EntryIds == {"u1", "ghost", "cg"} \cup TierIds
TenancyRegistered == TRUE
Silos == {"s1", "s2"}
MaxWrites == 1
\* The delegated facades are called on one silo; the second silo is the
\* lagging one whose flag differs when it resolves claims or decides.
FacadeSilos == {"s1"}

\* ---- Rules ----
TreeScope(tree) == [tree |-> tree, kind |-> "tree"]
WideScope(t) == [tree |-> Tree("tenant", t), kind |-> "tenantWide"]
RuleScopes ==
    { TreeScope(Tree("tenant", "A")), TreeScope(Tree("tenant", "B")),
      TreeScope(Tree("appOwned", "A")), TreeScope(Tree("system", "A")),
      TreeScope(Tree("default", NoTenant)), TreeScope(ClusterWideTreeId),
      WideScope("A") }
RuleSubjects == {"u1", "cg", "t/A/g", "t/B/g", "t/bad"}
FacadeSubjects == {"u1", "t/A/g", "t/B/g"}
FacadeScopes ==
    { TreeScope(Tree("tenant", "A")), TreeScope(Tree("tenant", "B")),
      TreeScope(Tree("appOwned", "A")), WideScope("A") }
OperatorSubjects == {"u1", "t/A/g", "t/B/g", "t/bad"}
OperatorScopes ==
    { TreeScope(Tree("tenant", "A")), TreeScope(Tree("tenant", "B")),
      TreeScope(Tree("default", NoTenant)), TreeScope(ClusterWideTreeId), WideScope("A") }
RuleOps == {"data", "none", "telemetry"}
Rule(layer, owner, subj, sc, ops, via, by) ==
    [layer |-> layer, owner |-> owner, subject |-> subj, scope |-> sc,
     ops |-> ops, via |-> via, by |-> by]
RuleRecords ==
    [layer : {"operator", "tenant"}, owner : Named \cup {NoTenant},
     subject : RuleSubjects, scope : RuleScopes, ops : RuleOps,
     via : {"facade", "operator", "arrival"}, by : Callers \cup {"peer"}]

\* Records a replication apply or a restore writes straight to the policy
\* tree under system origin, bypassing the store's guards.
ArrivalRules ==
    { Rule("tenant", "A", "u1", TreeScope(Tree("tenant", "A")), "data", "arrival", "peer"),
      Rule("tenant", "A", "u1", TreeScope(Tree("tenant", "B")), "data", "arrival", "peer"),
      Rule("tenant", "A", "u1", TreeScope(Tree("tenant", "A")), "telemetry", "arrival", "peer"),
      Rule("tenant", "A", "t/B/g", TreeScope(Tree("tenant", "A")), "data", "arrival", "peer"),
      Rule("operator", NoTenant, "u1", TreeScope(Tree("default", NoTenant)), "data", "arrival", "peer") }

OwnLayerScope(sc, t) == sc.tree.kind = "tenant" /\ sc.tree.tenant = t
TenantRuleShapeOk(r) ==
    /\ r.owner \in Tenants
    /\ OwnLayerScope(r.scope, r.owner)
    /\ r.ops = "data"
    /\ IsTier(r.subject) => (r.subject \in WellFormed /\ GroupTenant(r.subject) = r.owner)
GroupSubjectOk(r) ==
    IsTier(r.subject) => (r.subject \in WellFormed /\ OwnLayerScope(r.scope, GroupTenant(r.subject)))
WideOk(r) == r.scope.kind = "tenantWide" => r.layer = "tenant"
StoreAdmits(r) == WideOk(r) /\ (r.layer = "tenant" => TenantRuleShapeOk(r)) /\ GroupSubjectOk(r)
CompileAdmits(r) == r.layer = "tenant" => TenantRuleShapeOk(r)

NestingOk(g, m) ==
    /\ IsTier(g) => g \in WellFormed
    /\ IsTier(m) => (m \in WellFormed /\ g \in WellFormed /\ GroupTenant(g) = GroupTenant(m))

\* ---- Observations of the last step ----
Verbs == {"putRule", "opPutRule", "opUpsertGroup", "facadeUpsertGroup",
          "clusterAddEdge", "facadeAddEdge", "addAdmin", "addMember", "posture"}
FlaggedVerbs == {"putRule", "facadeUpsertGroup", "facadeAddEdge", "addMember"}
ValidatingVerbs == {"opUpsertGroup", "clusterAddEdge", "facadeAddEdge", "addAdmin", "addMember"}
Results == {"denied", "disabled", "reserved", "refused", "unresolved", "ok"}
ClaimIds == {"cg", "t/A/g", "t/default/x"}
EdgeGroups == {"cg", "t/A/g", "t/B/g"}
EdgeMembers == {"u1", "ghost", "cg", "t/A/g", "t/B/g", "t/bad"}
NoObs == [kind |-> "none"]
Call(verb, allowed, fl, result, wrote, validated) ==
    [kind |-> "call", verb |-> verb, allowed |-> allowed, flagOn |-> fl,
     result |-> result, wrote |-> wrote, validated |-> validated]
Obs ==
    {NoObs}
    \cup [kind : {"call"}, verb : Verbs, allowed : BOOLEAN, flagOn : BOOLEAN,
          result : Results, wrote : BOOLEAN, validated : BOOLEAN]
    \cup [kind : {"eval"}, flagOn : BOOLEAN, usedTenant : BOOLEAN]
    \cup [kind : {"claim"}, kept : SUBSET ClaimIds, dir : SUBSET EdgeGroups]

VARIABLES flag, vreq, rules, clusterGroups, tenantGroups, edges,
          adminEntries, memberEntries, budget, obs
Stores == <<rules, clusterGroups, tenantGroups, edges, adminEntries, memberEntries, budget>>
vars == <<flag, vreq, rules, clusterGroups, tenantGroups, edges,
          adminEntries, memberEntries, budget, obs>>

Effective == {r \in rules : CompileAdmits(r)}

Init ==
    /\ flag \in [Silos -> BOOLEAN]
    /\ vreq \in BOOLEAN
    /\ rules = {}
    /\ clusterGroups = {}
    /\ tenantGroups = {"t/A/g"}
    /\ edges = {}
    /\ adminEntries = [t \in Named |-> {}]
    /\ memberEntries = [t \in Named |-> {}]
    /\ budget = MaxWrites
    /\ obs = NoObs

Refuse(verb, allowed, fl, result, validated) ==
    /\ obs' = Call(verb, allowed, fl, result, FALSE, validated)
    /\ UNCHANGED <<flag, vreq>>
    /\ UNCHANGED Stores

\* The tenant policy facade (LatticeTenantPolicyAdmin.PutRuleAsync): authorize,
\* flag, reserved tenant, confinement, then a system-origin store write.
FacadePutRule(c, t, s, subj, sc, ops) ==
    LET auth == TenantAdminAuth(c, t)
        fl == flag[s]
        draft == Rule("tenant", t, subj, sc, ops, "facade", c)
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("putRule", auth, fl, "denied", FALSE)
          ELSE IF ~fl THEN Refuse("putRule", auth, fl, "disabled", FALSE)
          ELSE IF t = Default THEN Refuse("putRule", auth, fl, "reserved", FALSE)
          ELSE IF ~StoreAdmits(draft) THEN Refuse("putRule", auth, fl, "refused", FALSE)
          ELSE /\ rules' = rules \cup {draft}
               /\ budget' = budget - 1
               /\ obs' = Call("putRule", auth, fl, "ok", TRUE, FALSE)
               /\ UNCHANGED <<flag, vreq, clusterGroups, tenantGroups, edges, adminEntries, memberEntries>>

\* The cluster authorization facade (LatticeAuthAdmin.PutRuleAsync) and the
\* store beneath it: tenant-tier ids and tenant-wide scopes are refused to
\* operators, then the store's confinement guard runs for every caller.
OperatorPutRule(c, layer, subj, sc) ==
    LET auth == IsOperator(c)
        draft == Rule(layer, IF layer = "tenant" THEN "A" ELSE NoTenant, subj, sc, "data", "operator", c)
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("opPutRule", auth, FALSE, "denied", FALSE)
          ELSE IF layer = "tenant" \/ sc.kind = "tenantWide" THEN Refuse("opPutRule", auth, FALSE, "refused", FALSE)
          ELSE IF ~StoreAdmits(draft) THEN Refuse("opPutRule", auth, FALSE, "refused", FALSE)
          ELSE /\ rules' = rules \cup {draft}
               /\ budget' = budget - 1
               /\ obs' = Call("opPutRule", auth, FALSE, "ok", TRUE, FALSE)
               /\ UNCHANGED <<flag, vreq, clusterGroups, tenantGroups, edges, adminEntries, memberEntries>>

\* A policy record arriving by replication or restore, written under system
\* origin without the store's guards.
ArriveRule(r) ==
    /\ budget > 0
    /\ rules' = rules \cup {r}
    /\ budget' = budget - 1
    /\ obs' = NoObs
    /\ UNCHANGED <<flag, vreq, clusterGroups, tenantGroups, edges, adminEntries, memberEntries>>

\* LatticeAuthAdmin.UpsertGroupAsync: authorize, reserve t/, validate, write.
OperatorUpsertGroup(c, g) ==
    LET auth == IsOperator(c)
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("opUpsertGroup", auth, FALSE, "denied", FALSE)
          ELSE IF IsTier(g) THEN Refuse("opUpsertGroup", auth, FALSE, "refused", FALSE)
          ELSE IF vreq /\ g \notin ResolvableGroups THEN Refuse("opUpsertGroup", auth, FALSE, "unresolved", TRUE)
          ELSE /\ clusterGroups' = clusterGroups \cup {g}
               /\ budget' = budget - 1
               /\ obs' = Call("opUpsertGroup", auth, FALSE, "ok", TRUE, TRUE)
               /\ UNCHANGED <<flag, vreq, rules, tenantGroups, edges, adminEntries, memberEntries>>

\* LatticeTenantDirectoryAdmin.UpsertGroupAsync: the id is composed from the
\* tenant the call names, so a caller can only create its own tenant's group.
FacadeUpsertGroup(c, t, s) ==
    LET auth == TenantAdminAuth(c, t)
        fl == flag[s]
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("facadeUpsertGroup", auth, fl, "denied", FALSE)
          ELSE IF ~fl THEN Refuse("facadeUpsertGroup", auth, fl, "disabled", FALSE)
          ELSE IF t = Default THEN Refuse("facadeUpsertGroup", auth, fl, "reserved", FALSE)
          ELSE /\ tenantGroups' = tenantGroups \cup {TGroup(t)}
               /\ budget' = budget - 1
               /\ obs' = Call("facadeUpsertGroup", auth, fl, "ok", TRUE, FALSE)
               /\ UNCHANGED <<flag, vreq, rules, clusterGroups, edges, adminEntries, memberEntries>>

\* LatticeAuthAdmin.AddMemberAsync over LatticeMembershipDirectory.AddMemberAsync:
\* authorize, validate the non-t/ ids, then the directory's nesting invariant.
ClusterAddEdge(c, g, m) ==
    LET auth == IsOperator(c)
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("clusterAddEdge", auth, FALSE, "denied", FALSE)
          ELSE IF vreq /\ ((~IsTier(m) /\ m \notin Resolvable) \/ (~IsTier(g) /\ g \notin ResolvableGroups))
               THEN Refuse("clusterAddEdge", auth, FALSE, "unresolved", TRUE)
          ELSE IF ~NestingOk(g, m) THEN Refuse("clusterAddEdge", auth, FALSE, "refused", TRUE)
          ELSE /\ edges' = edges \cup {<<m, g>>}
               /\ budget' = budget - 1
               /\ obs' = Call("clusterAddEdge", auth, FALSE, "ok", TRUE, TRUE)
               /\ UNCHANGED <<flag, vreq, rules, clusterGroups, tenantGroups, adminEntries, memberEntries>>

\* LatticeTenantDirectoryAdmin.AddGroupMemberAsync with a non-tenant-group
\* member kind: authorize, flag, reserved, ToStoredId, group exists, validate,
\* then the directory's nesting invariant.
FacadeAddEdge(c, t, s, m) ==
    LET auth == TenantAdminAuth(c, t)
        fl == flag[s]
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("facadeAddEdge", auth, fl, "denied", FALSE)
          ELSE IF ~fl THEN Refuse("facadeAddEdge", auth, fl, "disabled", FALSE)
          ELSE IF t = Default THEN Refuse("facadeAddEdge", auth, fl, "reserved", FALSE)
          ELSE IF IsTier(m) THEN Refuse("facadeAddEdge", auth, fl, "refused", FALSE)
          ELSE IF TGroup(t) \notin tenantGroups THEN Refuse("facadeAddEdge", auth, fl, "refused", FALSE)
          ELSE IF vreq /\ m \notin Resolvable THEN Refuse("facadeAddEdge", auth, fl, "unresolved", TRUE)
          ELSE IF ~NestingOk(TGroup(t), m) THEN Refuse("facadeAddEdge", auth, fl, "refused", TRUE)
          ELSE /\ edges' = edges \cup {<<m, TGroup(t)>>}
               /\ budget' = budget - 1
               /\ obs' = Call("facadeAddEdge", auth, fl, "ok", TRUE, TRUE)
               /\ UNCHANGED <<flag, vreq, rules, clusterGroups, tenantGroups, adminEntries, memberEntries>>

\* LatticeTenantAccessAdmin.AddAdminSubjectAsync (not behind the flag):
\* authorize, reserved, admin-entry confinement, then verify the own group
\* exists or validate the id against the identity directory, then write.
AddAdminEntry(c, t, e) ==
    LET auth == TenantAdminAuth(c, t)
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("addAdmin", auth, FALSE, "denied", FALSE)
          ELSE IF t = Default THEN Refuse("addAdmin", auth, FALSE, "reserved", FALSE)
          ELSE IF IsTier(e) /\ e # TGroup(t) THEN Refuse("addAdmin", auth, FALSE, "refused", FALSE)
          ELSE IF IsTier(e) /\ e \notin tenantGroups THEN Refuse("addAdmin", auth, FALSE, "refused", TRUE)
          ELSE IF vreq /\ ~IsTier(e) /\ e \notin Resolvable THEN Refuse("addAdmin", auth, FALSE, "unresolved", TRUE)
          ELSE /\ adminEntries' = [adminEntries EXCEPT ![t] = @ \cup {e}]
               /\ budget' = budget - 1
               /\ obs' = Call("addAdmin", auth, FALSE, "ok", TRUE, TRUE)
               /\ UNCHANGED <<flag, vreq, rules, clusterGroups, tenantGroups, edges, memberEntries>>

\* LatticeTenantDirectoryAdmin.AddMemberAsync: "own" stands for the
\* TenantGroup kind, whose id is composed from the tenant the call names.
AddMemberEntry(c, t, s, e) ==
    LET auth == TenantAdminAuth(c, t)
        fl == flag[s]
        stored == IF e = "own" THEN TGroup(t) ELSE e
    IN /\ budget > 0
       /\ IF ~auth THEN Refuse("addMember", auth, fl, "denied", FALSE)
          ELSE IF ~fl THEN Refuse("addMember", auth, fl, "disabled", FALSE)
          ELSE IF t = Default THEN Refuse("addMember", auth, fl, "reserved", FALSE)
          ELSE IF e # "own" /\ IsTier(e) THEN Refuse("addMember", auth, fl, "refused", FALSE)
          ELSE IF e = "own" /\ stored \notin tenantGroups THEN Refuse("addMember", auth, fl, "refused", TRUE)
          ELSE IF vreq /\ e # "own" /\ e \notin Resolvable THEN Refuse("addMember", auth, fl, "unresolved", TRUE)
          ELSE /\ memberEntries' = [memberEntries EXCEPT ![t] = @ \cup {stored}]
               /\ budget' = budget - 1
               /\ obs' = Call("addMember", auth, fl, "ok", TRUE, TRUE)
               /\ UNCHANGED <<flag, vreq, rules, clusterGroups, tenantGroups, edges, adminEntries>>

\* MembershipContext resolution on silo s: claim-asserted groups pass the
\* tenant group claim filter, which is active whenever tenancy is
\* registered, before expansion; directory-recorded groups are kept.
ResolveClaims(s, asserted) ==
    LET dir == {x[2] : x \in {y \in edges : y[1] = "u1"}}
        kept == {g \in asserted : ~(TenancyRegistered /\ IsTier(g)) \/ g \in dir}
    IN /\ obs' = [kind |-> "claim", kept |-> kept, dir |-> dir]
       /\ UNCHANGED <<flag, vreq>>
       /\ UNCHANGED Stores

\* One point decision on silo s: the operator layer first, then the tenant
\* layer only while this silo's flag is on and only for a tenant-layer tree.
Evaluate(s, subj, tree) ==
    LET opMatch == \E r \in Effective : r.layer = "operator" /\ r.subject = subj /\ r.scope.tree = tree
        consulted == ~opMatch /\ flag[s] /\ tree.kind = "tenant"
        used == consulted /\ \E r \in Effective :
                    r.layer = "tenant" /\ r.owner = tree.tenant /\ r.subject = subj /\ r.scope.tree = tree
    IN /\ obs' = [kind |-> "eval", flagOn |-> flag[s], usedTenant |-> used]
       /\ UNCHANGED <<flag, vreq>>
       /\ UNCHANGED Stores

\* An options reload on one silo; every other silo keeps its own value.
FlipFlag(s) ==
    /\ flag' = [flag EXCEPT ![s] = ~@]
    /\ obs' = NoObs
    /\ UNCHANGED vreq
    /\ UNCHANGED Stores

\* LatticeTenantPolicyAdmin.GetPostureAsync: authorized like every verb, and
\* the one verb that answers while the flag is off.
Posture(c, t, s) ==
    LET auth == TenantAdminAuth(c, t)
        fl == flag[s]
    IN IF ~auth THEN Refuse("posture", auth, fl, "denied", FALSE)
       ELSE IF t = Default THEN Refuse("posture", auth, fl, "reserved", FALSE)
       ELSE Refuse("posture", auth, fl, "ok", FALSE)

Next ==
    \/ \E c \in Callers, t \in Named, s \in FacadeSilos, subj \in FacadeSubjects, sc \in FacadeScopes, ops \in {"data", "telemetry"} : FacadePutRule(c, t, s, subj, sc, ops)
    \/ \E c \in Callers, layer \in {"operator", "tenant"}, subj \in OperatorSubjects, sc \in OperatorScopes : OperatorPutRule(c, layer, subj, sc)
    \/ \E r \in ArrivalRules : ArriveRule(r)
    \/ \E c \in Callers, g \in {"cg", "ghost", "t/default/x", "t/bad", "t/A/g"} : OperatorUpsertGroup(c, g)
    \/ \E c \in Callers, t \in Named, s \in FacadeSilos : FacadeUpsertGroup(c, t, s)
    \/ \E c \in Callers, g \in EdgeGroups, m \in EdgeMembers : ClusterAddEdge(c, g, m)
    \/ \E c \in Callers, t \in Named, s \in FacadeSilos, m \in {"u1", "ghost", "cg", "t/B/g", "t/bad"} : FacadeAddEdge(c, t, s, m)
    \/ \E c \in Callers, t \in Named, e \in {"u1", "ghost", "cg", "t/A/g", "t/B/g", "t/bad"} : AddAdminEntry(c, t, e)
    \/ \E c \in Callers, t \in Named, s \in FacadeSilos, e \in {"u1", "ghost", "cg", "t/B/g", "t/bad", "own"} : AddMemberEntry(c, t, s, e)
    \/ \E s \in Silos, asserted \in SUBSET ClaimIds : ResolveClaims(s, asserted)
    \/ \E s \in Silos, subj \in {"u1", "t/A/g"}, tree \in {Tree("tenant", "A"), Tree("tenant", "B")} : Evaluate(s, subj, tree)
    \/ \E s \in Silos : FlipFlag(s)
    \/ \E c \in Callers, t \in Named, s \in FacadeSilos : Posture(c, t, s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ flag \in [Silos -> BOOLEAN]
    /\ vreq \in BOOLEAN
    /\ rules \subseteq RuleRecords
    /\ clusterGroups \subseteq {"cg", "ghost", "t/default/x", "t/bad", "t/A/g"}
    /\ tenantGroups \subseteq TierIds
    /\ edges \subseteq EdgeMembers \X EdgeGroups
    /\ adminEntries \in [Named -> SUBSET EntryIds]
    /\ memberEntries \in [Named -> SUBSET EntryIds]
    /\ budget \in 0..MaxWrites
    /\ obs \in Obs

\* A tenant-tier rule is written only by the tenant policy facade after its
\* admin check, is confined to its tenant's tenant-layer trees and
\* tenant-wide scope with data-plane operations only, and no rule in effect
\* names a tenant group outside that group's tenant. A non-conforming
\* tenant-tier record arriving by replication or restore never takes effect.
TenantRuleConfinement ==
    /\ \A r \in rules : r.via # "arrival" => StoreAdmits(r)
    /\ \A r \in rules : r.layer = "tenant" => r.via \in {"facade", "arrival"}
    /\ \A r \in rules : r.via = "facade" => TenantAdminAuth(r.by, r.owner)
    /\ \A r \in Effective : r.layer = "tenant" => TenantRuleShapeOk(r)
    /\ \A r \in Effective : GroupSubjectOk(r)

\* No cluster group in the t/ namespace, no tenant group for the reserved
\* default tenant, and no edge nesting a tenant group in a cluster group or
\* in another tenant's group.
ReservedGroupNamespace ==
    /\ clusterGroups \cap TierIds = {}
    /\ tenantGroups \subseteq WellFormed
    /\ \A x \in edges : NestingOk(x[2], x[1])

\* No identity-provider-asserted t/ group survives claim filtering, whatever
\* the resolving silo's flag says; only a directory-recorded membership does.
ClaimsNeverConferTenantGroups ==
    obs.kind = "claim" => \A g \in obs.kept : IsTier(g) => g \in obs.dir

\* While a silo's flag is off its delegated facades refuse every authorized
\* call (the posture excepted) and its evaluator never uses a tenant rule.
FeatureOffInert ==
    /\ (obs.kind = "call" /\ obs.verb \in FlaggedVerbs /\ obs.allowed /\ ~obs.flagOn)
           => (obs.result = "disabled" /\ ~obs.wrote)
    /\ (obs.kind = "eval" /\ obs.usedTenant) => obs.flagOn

\* A caller who fails authorization learns only that it was denied: not the
\* flag, not the reserved tenant, not a validation outcome, and nothing is
\* written.
AuthorizeBeforeDisclose ==
    (obs.kind = "call" /\ ~obs.allowed) => (obs.result = "denied" /\ ~obs.wrote)

\* Authorize, then validate, then write: validation runs only for an
\* authorized caller, a refused validation writes nothing, and with
\* validation required no stored reference names an unresolvable principal
\* or a tenant group that does not exist.
DirectoryValidationOrder ==
    /\ obs.kind = "call" =>
           /\ obs.validated => obs.allowed
           /\ (obs.wrote /\ obs.verb \in ValidatingVerbs) => obs.validated
           /\ obs.result = "unresolved" => ~obs.wrote
    /\ vreq =>
           /\ clusterGroups \subseteq ResolvableGroups
           /\ \A x \in edges : ~IsTier(x[1]) => x[1] \in Resolvable
           /\ \A t \in Named : \A e \in adminEntries[t] \cup memberEntries[t] :
                  e \in Resolvable \/ (e = TGroup(t) /\ e \in tenantGroups)

\* A flag flip, either way, takes effect at once and deletes nothing.
FlipDeletesNothing ==
    [][flag' # flag => UNCHANGED <<rules, clusterGroups, tenantGroups, edges, adminEntries, memberEntries>>]_vars
=============================================================================
