---------------------------- MODULE AuthDecision ----------------------------
(***************************************************************************)
(* The authorization decision as one function over a rule store, a caller, *)
(* an operation and a scope: the operator layer (final when it matches),   *)
(* the tenant layer confined to a tenant data tree of its owner, the       *)
(* per-key composition of range filters on operator coverage, scopeless    *)
(* capabilities conferred only at the cluster-wide id (and, for Telemetry, *)
(* by a platform grant over one tenant's capability scope), fail-closed    *)
(* control-plane isolation, the bootstrap root of trust, and batch         *)
(* authorization decided before any intent is placed. The rule store is    *)
(* written by the operator surface, the tenant policy facade and           *)
(* system-origin writes (restore, replication) that bypass the store's     *)
(* confinement, so the compiled snapshot's own filter is exercised.        *)
(*                                                                         *)
(* This module publishes the Allowed(caller, op, scope) contract the auth  *)
(* modules of epic #6046 model against; see README.md.                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

\* ---------------------------------------------------------------------------
\* Bounded instance.
\* ---------------------------------------------------------------------------
Tenants == {"A", "B"}
NoTenant == "none"
NoKey == "none"
Keys == {"a1", "b1"}
\* The single prefix "a" covers the key "a1" only.
Covers(p, k) == p = "a" /\ k = "a1"

Anonymous == "anon"
\* Callers that reach the policy path; "root" is a bootstrap administrator.
Callers == {"u", "anon"}
AllCallers == Callers \cup {"root"}
\* The configured bootstrap set deliberately names the anonymous subject: the
\* gate must never honour it for an anonymous caller.
BootstrapConfigured == {"root", "anon"}
\* The caller's active tenant differs from the owner of the tree it reads.
ActiveTenant(c) == "B"
\* The permissive data-plane default, the one fail-closed must never inherit.
DefaultEffect == "Allow"
\* No MCP authorizer is registered in this instance.
McpAuthorizerRegistered == FALSE
MaxRules == 3

\* ---------------------------------------------------------------------------
\* Trees, operations, scopes and rules (the contract vocabulary).
\* ---------------------------------------------------------------------------
Tree(kind, t) == [kind |-> kind, tenant |-> t]
TD == Tree("default", NoTenant)
TA == Tree("tenant", "A")
TB == Tree("tenant", "B")
TAapp == Tree("appOwned", "A")
PolicyTree == Tree("reserved", NoTenant)
ClusterWideTreeId == Tree("clusterWide", NoTenant)
TenantCapability(T) == Tree("tenantCapability", T)
TenantWide(T) == Tree("tenantWide", T)
DataTrees == {TD, TA, TB, TAapp}
DataKinds == {"default", "tenant", "appOwned", "system"}
ControlKinds == {"reserved", "clusterWide", "tenantCapability"}

DataOps == {"Read", "Write"}
TenantOps == DataOps \cup {"Admin"}
ScopelessOps == {"Telemetry", "AppInstall"}
Ops == TenantOps \cup ScopelessOps

Scope(tree, key) == [tree |-> tree, key |-> key]

R(layer, owner, ops, tree, kind, at, effect) ==
    [layer |-> layer, owner |-> owner, subject |-> "u", ops |-> ops,
     tree |-> tree, kind |-> kind, at |-> at, effect |-> effect]
NoRule == R("none", NoTenant, {}, TD, "none", NoKey, "none")

RuleUniverse == {
    R("operator", NoTenant, {"Read", "Write"}, TA, "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"Read", "Write"}, TA, "key", "a1", "Deny"),
    R("tenant", "A", {"Read"}, TA, "tree", NoKey, "Deny"),
    R("tenant", "A", {"Read", "Write"}, TA, "prefix", "a", "Allow"),
    \* Escaped tenant rules, reachable only through a system-origin write.
    R("tenant", "B", {"Read"}, TA, "tree", NoKey, "Allow"),
    R("tenant", "A", {"Read"}, TAapp, "tree", NoKey, "Allow"),
    R("tenant", "A", {"Telemetry"}, TenantCapability("A"), "tree", NoKey, "Allow"),
    R("tenant", "A", {"Read"}, TenantWide("A"), "tree", NoKey, "Allow"),
    R("tenant", "B", {"Read"}, TenantWide("B"), "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"Telemetry"}, ClusterWideTreeId, "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"AppInstall"}, ClusterWideTreeId, "prefix", "a", "Allow"),
    R("operator", NoTenant, {"Telemetry"}, TenantCapability("A"), "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"Read", "Admin"}, ClusterWideTreeId, "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"Admin"}, TenantCapability("A"), "tree", NoKey, "Allow"),
    R("operator", NoTenant, {"Admin"}, PolicyTree, "prefix", "a", "Allow"),
    R("operator", NoTenant, {"Admin"}, PolicyTree, "tree", NoKey, "Allow")
}

Actors == {"operator", "system"} \cup Tenants

\* The requests the gate is asked: data-plane points, whole-scope admin and
\* registry requests, and the capability checks the telemetry and app
\* authorizers issue (cluster-wide, and Telemetry over a tenant capability).
DataScopes == {Scope(t, k) : t \in DataTrees, k \in Keys}
CapabilityScopes == {Scope(ClusterWideTreeId, NoKey),
                     Scope(TenantCapability("A"), NoKey),
                     Scope(TenantCapability("B"), NoKey)}
ControlScopes == CapabilityScopes \cup {Scope(PolicyTree, NoKey)}
Requests ==
    {[op |-> o, scope |-> s] : o \in {"Read", "Admin"}, s \in DataScopes \cup ControlScopes}
    \cup {[op |-> "Telemetry", scope |-> s] : s \in CapabilityScopes}
    \cup {[op |-> "AppInstall", scope |-> Scope(ClusterWideTreeId, NoKey)]}

BatchSets == {{Scope(TA, "a1"), Scope(TA, "b1")},
              {Scope(TA, "b1"), Scope(TD, "a1")},
              {Scope(TD, "b1"), Scope(TB, "a1")}}

VARIABLES rules, last
vars == <<rules, last>>

Blank == [kind |-> "none", caller |-> "none", op |-> "none", scope |-> Scope(TD, NoKey),
          verdict |-> "none", layer |-> "none", rule |-> NoRule, admitted |-> {},
          pairs |-> {}, placed |-> {}, actor |-> "none"]

\* ---------------------------------------------------------------------------
\* Rule resolution (CompiledTree.ResolvePoint): most specific scope wins,
\* key over prefix over tree, deny overrides allow at an equal tier.
\* ---------------------------------------------------------------------------
MatchesAt(r, c, op, tree, key) ==
    /\ r.subject = c
    /\ op \in r.ops
    /\ r.tree = tree
    /\ \/ r.kind = "tree"
       \/ key # NoKey /\ r.kind = "key" /\ r.at = key
       \/ key # NoKey /\ r.kind = "prefix" /\ Covers(r.at, key)

Tier(r) == IF r.kind = "key" THEN 3 ELSE IF r.kind = "prefix" THEN 2 ELSE 1

NoMatch == [matched |-> FALSE, effect |-> "none", rule |-> NoRule, layer |-> "default"]
Filtered == [matched |-> FALSE, effect |-> "none", rule |-> NoRule, layer |-> "filtered"]

Resolve(rs, c, op, tree, key, layer) ==
    LET ms == {r \in rs : MatchesAt(r, c, op, tree, key)}
        top == {r \in ms : \A x \in ms : Tier(x) <= Tier(r)}
        denies == {r \in top : r.effect = "Deny"}
        win == IF denies # {} THEN CHOOSE r \in denies : TRUE ELSE CHOOSE r \in top : TRUE
    IN IF ms = {} THEN NoMatch
       ELSE [matched |-> TRUE, effect |-> win.effect, rule |-> win, layer |-> layer]

\* PolicyEvaluator.ResolveTiered: an all-trees deny, then the specific verdict,
\* then an all-trees allow.
Tiered(specific, wide) ==
    IF wide.matched /\ wide.effect = "Deny" THEN wide
    ELSE IF specific.matched THEN specific ELSE wide

\* ---------------------------------------------------------------------------
\* The compiled snapshot (CompiledPolicy.Compile, CompiledTenantPartition).
\* ---------------------------------------------------------------------------
OperatorRules == {r \in rules : r.layer = "operator"}

\* TenantRuleConfinement.CheckTenantRule.
Conforms(r) ==
    /\ r.layer = "tenant"
    /\ r.owner \in Tenants
    /\ r.ops \subseteq TenantOps
    /\ \/ r.tree = TenantWide(r.owner) /\ r.kind = "tree"
       \/ r.tree.kind = "tenant" /\ r.tree.tenant = r.owner

TenantPartition == {r \in rules : r.layer = "tenant" /\ Conforms(r)}

\* TenantRuleConfinement.TryGetTenantLayerTree: t/{T}/{name} only.
TenantLayerTree(tree) == tree.kind = "tenant"

\* The tenant whose layer governs the tree: its owner.
LayerTenant(c, tree) == tree.tenant

\* CompiledTenantPartition.TryGetBuckets.
TenantGoverns(c, tree) ==
    /\ TenantLayerTree(tree)
    /\ \E r \in TenantPartition : r.tree \in {tree, TenantWide(LayerTenant(c, tree))}

\* PolicyEvaluator.ShouldConsultAllTrees: never a control-plane id.
ConsultAllTrees(tree) == tree.kind \in DataKinds

AllTrees(c, op, tree) ==
    IF ConsultAllTrees(tree)
    THEN Resolve(OperatorRules, c, op, ClusterWideTreeId, NoKey, "operator")
    ELSE NoMatch

HasPerKeyRules(rs, tree) == \E r \in rs : r.tree = tree /\ r.kind \in {"key", "prefix"}

OperatorMatch(c, op, tree, key) ==
    Tiered(Resolve(OperatorRules, c, op, tree, key, "operator"), AllTrees(c, op, tree))

TenantMatch(c, op, tree, key) ==
    Tiered(Resolve(TenantPartition, c, op, tree, key, "tenant"),
           Resolve(TenantPartition, c, op, TenantWide(LayerTenant(c, tree)), NoKey, "tenant"))

\* PolicyEvaluator.Evaluate and EvaluateLayered for a point (key) or a
\* whole-scope (NoKey) request. A request on the cluster-wide id is a scopeless
\* capability check and resolves uniformly; any other whole-scope request over
\* per-key rules is a key-filtered decision.
EvalPoint(c, op, s) ==
    LET opm == OperatorMatch(c, op, s.tree, s.key)
        tenm == TenantMatch(c, op, s.tree, s.key)
    IN IF s.key = NoKey /\ s.tree # ClusterWideTreeId /\ HasPerKeyRules(OperatorRules, s.tree)
       THEN Filtered
       ELSE IF TenantGoverns(c, s.tree)
       THEN IF opm.matched THEN opm ELSE tenm
       ELSE opm

\* A data-plane point decision as the gate returns it.
PointAllowed(c, op, s) ==
    LET m == EvalPoint(c, op, s)
    IN IF m.matched THEN m.effect = "Allow" ELSE DefaultEffect = "Allow"

\* The per-key composition's lower layers: the tenant layer when it governs
\* the tree, otherwise (or when it does not match) the default effect.
LowerKey(c, op, tree, k) ==
    IF ~TenantGoverns(c, tree) THEN DefaultEffect
    ELSE LET t == TenantMatch(c, op, tree, k)
         IN IF t.matched THEN t.effect ELSE DefaultEffect

IsBootstrapCaller(c) == c # Anonymous /\ c \in BootstrapConfigured

\* ---------------------------------------------------------------------------
\* Actions.
\* ---------------------------------------------------------------------------
Init ==
    /\ rules = {}
    /\ last = Blank

\* LatticeAuthorizationPolicyStore.PutRuleAsync off system origin: a tenant:
\* rule id is refused.
OperatorUpsert(r) ==
    /\ last = Blank
    /\ r \notin rules
    /\ Cardinality(rules) < MaxRules
    /\ r.layer = "operator"
    /\ rules' = rules \cup {r}
    /\ last' = [Blank EXCEPT !.kind = "write", !.actor = "operator", !.rule = r]

\* The tenant policy facade: validates the rule against its tenant, then writes
\* under system origin.
TenantFacadeUpsert(T, r) ==
    /\ last = Blank
    /\ r \notin rules
    /\ Cardinality(rules) < MaxRules
    /\ r.layer = "tenant"
    /\ r.owner = T
    /\ Conforms(r)
    /\ rules' = rules \cup {r}
    /\ last' = [Blank EXCEPT !.kind = "write", !.actor = T, !.rule = r]

\* A system-origin write (restore, replication apply) that bypasses the
\* store's confinement: any rule, escaped tenant rules included.
SystemUpsert(r) ==
    /\ last = Blank
    /\ r \notin rules
    /\ Cardinality(rules) < MaxRules
    /\ rules' = rules \cup {r}
    /\ last' = [Blank EXCEPT !.kind = "write", !.actor = "system", !.rule = r]

\* RemoveRuleAsync: the operator surface may not remove a tenant: rule; the
\* tenant facade removes only its own tenant's rules.
RuleDelete(a, r) ==
    /\ last = Blank
    /\ r \in rules
    /\ \/ a = "system"
       \/ a = "operator" /\ r.layer = "operator"
       \/ a \in Tenants /\ r.layer = "tenant" /\ r.owner = a
    /\ rules' = rules \ {r}
    /\ last' = [Blank EXCEPT !.kind = "delete", !.actor = a, !.rule = r]

\* PolicyAccessGate.AuthorizeAsync for a non-bootstrap caller: control-plane
\* isolation on the reserved, cluster-wide and tenant-capability ids admits
\* only an explicit matched allow; the data plane falls to the default effect.
Evaluate(c, op, s) ==
    /\ last = Blank
    /\ ~IsBootstrapCaller(c)
    /\ [op |-> op, scope |-> s] \in Requests
    /\ LET m == EvalPoint(c, op, s)
           control == s.tree.kind \in ControlKinds
           verdict == IF control
                      THEN IF m.matched /\ m.effect = "Allow" THEN "Allow" ELSE "Deny"
                      ELSE IF m.layer = "filtered" THEN "Filtered"
                      ELSE IF m.matched THEN m.effect ELSE DefaultEffect
           layer == IF control /\ ~m.matched THEN "failclosed" ELSE m.layer
       IN last' = [Blank EXCEPT !.kind = "point", !.caller = c, !.op = op, !.scope = s,
                                !.verdict = verdict, !.layer = layer, !.rule = m.rule]
    /\ UNCHANGED rules

\* A range or scan: the key filter composes the layers per key on operator
\* coverage (PolicyEvaluator.LayeredEffect, OperatorFiltered).
EvaluateRange(c, op, tree) ==
    /\ last = Blank
    /\ ~IsBootstrapCaller(c)
    /\ LET admitted == {k \in Keys :
               LET o == OperatorMatch(c, op, tree, k)
               IN IF o.matched THEN o.effect = "Allow"
                  ELSE LowerKey(c, op, tree, k) = "Allow"}
       IN last' = [Blank EXCEPT !.kind = "range", !.caller = c, !.op = op,
                                !.scope = Scope(tree, NoKey), !.admitted = admitted]
    /\ UNCHANGED rules

\* The gate's root of trust, checked before the engine.
BootstrapEvaluate(c, op) ==
    /\ last = Blank
    /\ c # Anonymous /\ c \in BootstrapConfigured
    /\ last' = [Blank EXCEPT !.kind = "bootstrap", !.caller = c, !.op = op,
                             !.scope = Scope(PolicyTree, NoKey), !.verdict = "Allow",
                             !.layer = "bootstrap"]
    /\ UNCHANGED rules

\* A cross-tree (multi-tree, multi-key) atomic batch: every leg is authorized
\* before any intent or prepare is placed.
BatchEvaluate(c, pairs) ==
    /\ last = Blank
    /\ ~IsBootstrapCaller(c)
    /\ LET ok == \A p \in pairs : PointAllowed(c, "Write", p)
       IN last' = [Blank EXCEPT !.kind = "batch", !.caller = c, !.op = "Write",
                                !.pairs = pairs, !.verdict = IF ok THEN "Allow" ELSE "Deny",
                                !.placed = IF ok THEN pairs ELSE {}]
    /\ UNCHANGED rules

\* McpToolAuthorizationGate.IsAuthorizedAsync: no registered authorizer denies.
ToolCheck(c) ==
    /\ last = Blank
    /\ last' = [Blank EXCEPT !.kind = "tool", !.caller = c,
                             !.verdict = IF McpAuthorizerRegistered THEN "Delegated" ELSE "Deny"]
    /\ UNCHANGED rules

\* Returns the recorder to idle between steps, so a decision is never taken
\* from another decision's state. Models no protocol step.
Settle ==
    /\ last # Blank
    /\ last' = Blank
    /\ UNCHANGED rules

Next ==
    \/ \E r \in RuleUniverse : OperatorUpsert(r)
    \/ \E T \in Tenants, r \in RuleUniverse : TenantFacadeUpsert(T, r)
    \/ \E r \in RuleUniverse : SystemUpsert(r)
    \/ \E a \in Actors, r \in RuleUniverse : RuleDelete(a, r)
    \/ \E c \in Callers, q \in Requests : Evaluate(c, q.op, q.scope)
    \/ \E c \in Callers, t \in DataTrees : EvaluateRange(c, "Read", t)
    \/ \E c \in AllCallers, o \in Ops : BootstrapEvaluate(c, o)
    \/ \E c \in Callers, b \in BatchSets : BatchEvaluate(c, b)
    \/ \E c \in Callers : ToolCheck(c)
    \/ Settle

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------
\* Properties.
\* ---------------------------------------------------------------------------
TypeOK ==
    /\ rules \subseteq RuleUniverse
    /\ Cardinality(rules) <= MaxRules
    /\ last.kind \in {"none", "point", "range", "batch", "bootstrap", "tool", "write", "delete"}
    /\ last.caller \in AllCallers \cup {"none"}
    /\ last.op \in Ops \cup {"none"}
    /\ last.verdict \in {"Allow", "Deny", "Filtered", "Delegated", "none"}
    /\ last.layer \in {"operator", "tenant", "default", "filtered", "failclosed", "bootstrap", "none"}
    /\ last.rule \in RuleUniverse \cup {NoRule}
    /\ last.admitted \subseteq Keys
    /\ last.placed \subseteq DataScopes
    /\ last.actor \in Actors \cup {"none"}

\* Some operator rule (or, on a data tree, the all-trees tier) covers the key.
OpCovered(c, op, tree, key) ==
    \E r \in rules :
        /\ r.layer = "operator"
        /\ \/ MatchesAt(r, c, op, tree, key)
           \/ ConsultAllTrees(tree) /\ MatchesAt(r, c, op, ClusterWideTreeId, NoKey)

\* The operator layer decides the request outright: it covers the key, or, for
\* a whole-scope request, it matches and cannot vary by key.
OpDecides(c, op, s) ==
    IF s.key # NoKey THEN OpCovered(c, op, s.tree, s.key)
    ELSE /\ s.tree = ClusterWideTreeId \/ ~HasPerKeyRules(OperatorRules, s.tree)
         /\ OpCovered(c, op, s.tree, NoKey)

OperatorFinal ==
    last.kind = "point" /\ OpDecides(last.caller, last.op, last.scope) =>
        /\ last.layer = "operator"
        /\ (last.verdict = "Allow")
               = (OperatorMatch(last.caller, last.op, last.scope.tree, last.scope.key).effect = "Allow")

TenantLayerConfined ==
    last.kind = "point" /\ last.layer = "tenant" =>
        /\ ~OpDecides(last.caller, last.op, last.scope)
        /\ last.scope.tree.kind = "tenant"
        /\ last.rule.layer = "tenant"
        /\ last.rule.owner = last.scope.tree.tenant
        /\ last.rule.tree \in {last.scope.tree, TenantWide(last.scope.tree.tenant)}
        /\ last.op \in TenantOps

\* As sets: the admitted keys are those an operator rule allows, plus those
\* the lower layers allow that no operator rule covers.
PerKeyCoverage ==
    last.kind = "range" =>
        \A k \in Keys :
            (k \in last.admitted)
                = IF OpCovered(last.caller, last.op, last.scope.tree, k)
                  THEN OperatorMatch(last.caller, last.op, last.scope.tree, k).effect = "Allow"
                  ELSE LowerKey(last.caller, last.op, last.scope.tree, k) = "Allow"

ScopelessFromClusterWideOnly ==
    last.kind = "point" /\ last.op \in ScopelessOps /\ last.verdict = "Allow" =>
        /\ last.rule.layer = "operator"
        /\ last.rule.kind = "tree"
        /\ last.rule.effect = "Allow"
        /\ last.op \in last.rule.ops
        /\ \/ /\ last.scope = Scope(ClusterWideTreeId, NoKey)
              /\ last.rule.tree = ClusterWideTreeId
           \/ /\ last.op = "Telemetry"
              /\ \E T \in Tenants :
                    /\ last.scope = Scope(TenantCapability(T), NoKey)
                    /\ last.rule.tree = TenantCapability(T)

FailClosed ==
    /\ last.kind = "point" /\ last.scope.tree.kind \in ControlKinds /\ last.verdict = "Allow" =>
           /\ last.rule.layer = "operator"
           /\ last.rule.kind = "tree"
           /\ last.rule.effect = "Allow"
           /\ last.rule.tree = last.scope.tree
           /\ last.op \in last.rule.ops
    /\ last.kind = "tool" /\ ~McpAuthorizerRegistered => last.verdict = "Deny"

BootstrapRootOfTrust ==
    last.layer = "bootstrap" =>
        /\ last.caller \in BootstrapConfigured
        /\ last.caller # Anonymous

BatchAllOrNothingAuthorization ==
    last.kind = "batch" =>
        /\ last.placed \in {{}, last.pairs}
        /\ last.placed # {} => \A p \in last.pairs : PointAllowed(last.caller, last.op, p)

RuleAuthoringConfined ==
    last.kind \in {"write", "delete"} =>
        \/ last.actor = "system"
        \/ last.actor = "operator" /\ last.rule.layer = "operator"
        \/ /\ last.actor \in Tenants
           /\ last.rule.layer = "tenant"
           /\ last.rule.owner = last.actor
           /\ last.kind = "write" => Conforms(last.rule)
=============================================================================
