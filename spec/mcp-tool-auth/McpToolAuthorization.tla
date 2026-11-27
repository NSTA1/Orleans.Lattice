------------------------ MODULE McpToolAuthorization ------------------------
(***************************************************************************)
(* MCP tool authorization in lock-step (issue #6052, epic #6046): what a   *)
(* session's tools/list advertises, and what its tools/call serves, when   *)
(* both pass through the shared McpToolAuthorizationGate and the policy,   *)
(* the app registry or the caller's request context change between the     *)
(* two calls.                                                              *)
(*                                                                         *)
(* A session's tool collection is built once, from the snapshot current    *)
(* when the session opens, and it serves both tools/list and tools/call.   *)
(* Every invocation then re-runs the gate and the tool's own check against *)
(* the CURRENT snapshot, so advertisement is never a capability.           *)
(*                                                                         *)
(* Authorization is the abstract operator Allowed(caller, op, scope) of    *)
(* the F1 contract (spec/auth-decision, contract version 1). Because the   *)
(* policy changes here, the module declares it per policy snapshot:        *)
(* AllowedUnder(p, caller, op, scope) is a contract Allowed for each p.    *)
(* The cfg binds it to ContractAllowedUnder, the contract's minimal        *)
(* grant-set definition over one grant set per snapshot.                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANT AllowedUnder(_, _, _, _)

\* ---- F1 contract vocabulary (contract version 1), the subset used here ----
NoKey == "none"
NoTenant == "none"
Tenants == {"A", "B"}
Tree(kind, t) == [kind |-> kind, tenant |-> t]
ClusterWideTreeId == Tree("clusterWide", NoTenant)
TenantCapability(t) == Tree("tenantCapability", t)
Scope(tree, key) == [tree |-> tree, key |-> key]
DataOps == {"Read", "Write"}
BootstrapAdministrators == {}

\* A default-tenant data tree, and tenant A's app-owned tree, which also
\* carries the installed app's role binding.
DataTree == Tree("default", NoTenant)
AppTree == Tree("appOwned", "A")
DataScopes == {Scope(DataTree, NoKey), Scope(AppTree, NoKey)}
ClusterTelemetry == Scope(ClusterWideTreeId, NoKey)

\* ---- Callers ----
\* "op" is a cluster operator; "reader" a data reader; "tenantOp" holds
\* Telemetry only over tenant A's capability scope (#5582); "anon" presents
\* no credential. Member(c) is what ITenantContextResolver validates an
\* asserted active tenant against.
Callers == {"op", "reader", "tenantOp", "anon"}
Anonymous == "anon"
Authenticated == Callers \ {Anonymous}
Member(c) == CASE c = "op" -> {"A"} [] c = "reader" -> {"B"} [] c = "tenantOp" -> {"A"} [] OTHER -> {}
Headers == {NoTenant} \cup Tenants

\* ---- Policy snapshots ----
\* P2 revokes the reader, the operator's Write, the operator's app role and
\* the operator's cluster-wide Telemetry, which it replaces with Telemetry
\* over tenant A's capability scope.
Policies == {"P1", "P2"}
G(c, op, s) == [caller |-> c, op |-> op, scope |-> s]
Grants(p) ==
    IF p = "P1"
    THEN { G("op", "Read", Scope(DataTree, NoKey)), G("op", "Write", Scope(DataTree, NoKey)),
           G("op", "Telemetry", ClusterTelemetry), G("op", "Read", Scope(AppTree, NoKey)),
           G("reader", "Read", Scope(DataTree, NoKey)), G("reader", "Read", Scope(AppTree, NoKey)),
           G("tenantOp", "Telemetry", Scope(TenantCapability("A"), NoKey)) }
    ELSE { G("op", "Read", Scope(DataTree, NoKey)),
           G("op", "Telemetry", Scope(TenantCapability("A"), NoKey)),
           G("tenantOp", "Telemetry", Scope(TenantCapability("A"), NoKey)) }
ContractAllowedUnder(p, c, op, s) ==
    c \in BootstrapAdministrators \/ G(c, op, s) \in Grants(p)

\* ---- Tools ----
\* "caps" is lattice_capabilities, the one ungated meta-tool; "regions" is
\* lattice_list_regions; the group tools stand for the data group's read and
\* mutating tools and the telemetry group's tools; "appTool" is a tool of an
\* app installed in tenant A, gated on its compiled role grant.
DataTools == {"dataRead", "dataWrite"}
GroupTools == DataTools \cup {"telemetry"}
AllTools == {"caps", "regions", "appTool"} \cup GroupTools
ToolGroup(t) == IF t = "telemetry" THEN "Telemetry" ELSE "Data"
Required(t) == CASE t = "dataRead" -> {"Read"} [] t = "dataWrite" -> {"Write"} [] OTHER -> {"Telemetry"}
CallScopes(t) == IF t = "telemetry" THEN {ClusterTelemetry} ELSE DataScopes
NoScope == Scope(Tree("none", NoTenant), NoKey)

\* ---- The registered ILatticeApiMcpAuthorizer ----
\* "denyAll" is the DenyAllMcpAuthorizer AddLatticeMcp registers by default,
\* "absent" a host that removed it, and "byPolicy" a permissive host
\* authorizer whose verdict follows the policy snapshot (P2 withholds the
\* read tool by name).
AuthzKinds == {"denyAll", "absent", "byPolicy"}
Withheld(p) == IF p = "P2" THEN {"dataRead"} ELSE {}

\* ---- Regions ----
Regions == {"current", "peerA", "peerB"}
Actionable(t) == IF t = "A" THEN {"current", "peerA"} ELSE {"current", "peerB"}

Results == {"notFound", "denied", "clientError", "ok"}
FacadeVerdicts == {"skipped", "allow", "deny"}

VARIABLES policy, appEnabled, authz, session, obs
vars == <<policy, appEnabled, authz, session, obs>>

NoSession == [caller |-> "none"]
SessionRecs ==
    [caller : Callers, hdr : Headers, detail : BOOLEAN, ops : SUBSET (DataOps \cup {"Telemetry"}),
     pol : Policies, app : BOOLEAN, tools : SUBSET AllTools]
NoObs == [kind |-> "none"]
CallObsRecs ==
    [kind : {"call"}, caller : Callers, tool : AllTools, hdr : Headers, ctx : BOOLEAN,
     bad : BOOLEAN, scope : DataScopes \cup {ClusterTelemetry, NoScope}, adv : BOOLEAN,
     gate : BOOLEAN, facade : FacadeVerdicts, result : Results, annot : Headers,
     regions : SUBSET Regions, pol : Policies, app : BOOLEAN]

-----------------------------------------------------------------------------
\* ---- Shared seams ----

\* McpToolAuthorizationGate.IsAuthorizedAsync: no request context, or no
\* registered authorizer, denies; otherwise the registered authorizer decides.
AuthorizerAdmits(p, t) ==
    CASE authz = "absent" -> FALSE
      [] authz = "denyAll" -> FALSE
      [] OTHER -> t \notin Withheld(p)
Gate(ctx, p, t) == ctx /\ AuthorizerAdmits(p, t)

\* ITenantContextResolver re-resolving an asserted active tenant against the
\* caller's membership: the assertion survives only when it re-resolves to
\* itself.
Resolve(c, hdr) == IF hdr \in Member(c) THEN hdr ELSE NoTenant

\* AuthAdminMcpPermissionResolver: a group is admitted on grant presence on
\* any tree (scope-blind), except that the scopeless Telemetry is carried only
\* from a rule on the cluster-wide id.
TelemetryCarriedFrom == {ClusterWideTreeId}
TelemetryCarried(p, c) == \E tr \in TelemetryCarriedFrom : AllowedUnder(p, c, "Telemetry", Scope(tr, NoKey))
DataOpsHeld(p, c) == {op \in DataOps : \E sc \in DataScopes : AllowedUnder(p, c, op, sc)}
AccessGroups(p, c) ==
    (IF DataOpsHeld(p, c) # {} THEN {"Data"} ELSE {}) \cup (IF TelemetryCarried(p, c) THEN {"Telemetry"} ELSE {})
CarriedOps(p, c) == DataOpsHeld(p, c) \cup (IF TelemetryCarried(p, c) THEN {"Telemetry"} ELSE {})

\* The installed app's role is held when the shared access gate allows the
\* role's binding on the app's tree.
RoleHeld(p, c) == AllowedUnder(p, c, "Read", Scope(AppTree, NoKey))

-----------------------------------------------------------------------------
\* ---- The design's entitlement, stated independently of the discovery code ----

\* Whether t is invocable by c under snapshot (p, app) with a request context
\* present, for SOME choice of arguments.
HoldsFacadeGrant(p, c) ==
    (\E op \in DataOps, sc \in DataScopes : AllowedUnder(p, c, op, sc)) \/ AllowedUnder(p, c, "Telemetry", ClusterTelemetry)
Entitled(c, t, hdr, p, app) ==
    /\ c \in Authenticated
    /\ (t = "caps" \/ AuthorizerAdmits(p, t))
    /\ CASE t = "caps" -> TRUE
         [] t = "regions" -> HoldsFacadeGrant(p, c)
         [] t \in DataTools -> \E op \in Required(t), sc \in DataScopes : AllowedUnder(p, c, op, sc)
         [] t = "telemetry" -> AllowedUnder(p, c, "Telemetry", ClusterTelemetry)
         [] OTHER -> app /\ Resolve(c, hdr) = "A" /\ RoleHeld(p, c)

\* The same decision for one concrete call, under the snapshot current when it
\* is made: a data tool must be allowed on the scope the call names. The
\* region-discovery tool's facade-grant condition is decided at advertisement
\* only (see the refinement note's gap register).
EntitledCall(o) ==
    /\ IF o.tool = "regions"
       THEN o.caller \in Authenticated /\ AuthorizerAdmits(o.pol, "regions")
       ELSE Entitled(o.caller, o.tool, o.hdr, o.pol, o.app)
    /\ o.tool \in DataTools => \E op \in Required(o.tool) : AllowedUnder(o.pol, o.caller, op, o.scope)

-----------------------------------------------------------------------------
Init ==
    /\ policy \in Policies
    /\ appEnabled = TRUE
    /\ authz \in AuthzKinds
    /\ session = NoSession
    /\ obs = NoObs

\* LatticeApiMcpSessionConfigurator.BuildSessionPlanAsync for caller c, with
\* asserted active tenant hdr. detail is whether the registered permission
\* resolver carried operation detail with the groups it admitted.
OpenSession(c, hdr, detail) ==
    LET groups == AccessGroups(policy, c)
        ops == IF detail THEN CarriedOps(policy, c) ELSE {}
        meta == IF c \in Authenticated THEN {"caps"} ELSE {}
        regions == IF c \in Authenticated /\ groups # {} /\ Gate(TRUE, policy, "regions") THEN {"regions"} ELSE {}
        group == {t \in GroupTools : c \in Authenticated /\ ToolGroup(t) \in groups /\ ops \cap Required(t) # {} /\ Gate(TRUE, policy, t)}
        app == IF c \in Authenticated /\ appEnabled /\ Resolve(c, hdr) = "A" /\ RoleHeld(policy, c) /\ Gate(TRUE, policy, "appTool") THEN {"appTool"} ELSE {}
    IN /\ session' = [caller |-> c, hdr |-> hdr, detail |-> detail, ops |-> ops,
                      pol |-> policy, app |-> appEnabled, tools |-> meta \cup regions \cup group \cup app]
       /\ obs' = NoObs
       /\ UNCHANGED <<policy, appEnabled, authz>>

CallObs(t, ctx, bad, sc, adv, g, fac, res, annot, regs) ==
    [kind |-> "call", caller |-> session.caller, tool |-> t, hdr |-> session.hdr, ctx |-> ctx,
     bad |-> bad, scope |-> sc, adv |-> adv, gate |-> g, facade |-> fac, result |-> res,
     annot |-> annot, regions |-> regs, pol |-> policy, app |-> appEnabled]

\* Invoking lattice_capabilities: served from the session plan, ungated.
CallMeta ==
    LET adv == "caps" \in session.tools
        res == IF adv THEN "ok" ELSE "notFound"
    IN /\ session # NoSession
       /\ obs' = CallObs("caps", TRUE, FALSE, NoScope, adv, TRUE, "skipped", res, NoTenant, {})
       /\ UNCHANGED <<policy, appEnabled, authz, session>>

\* Invoking lattice_list_regions: AuthorizedMetaTool re-runs the gate, then
\* LatticeApiMcpRegionCatalog scopes the answer to the validated tenant.
CallRegions(ctx) ==
    LET c == session.caller
        hdr == session.hdr
        adv == "regions" \in session.tools
        g == Gate(ctx, policy, "regions")
        honoured == hdr # NoTenant /\ Resolve(c, hdr) = hdr
        annot == IF honoured THEN hdr ELSE NoTenant
        regs == IF hdr = NoTenant THEN Regions ELSE IF honoured THEN Actionable(hdr) ELSE {"current"}
        res == IF ~adv THEN "notFound" ELSE IF ~g THEN "denied" ELSE "ok"
    IN /\ session # NoSession
       /\ obs' = CallObs("regions", ctx, FALSE, NoScope, adv, g, "skipped", res,
                         IF res = "ok" THEN annot ELSE NoTenant, IF res = "ok" THEN regs ELSE {})
       /\ UNCHANGED <<policy, appEnabled, authz, session>>

\* Invoking a group tool: CredentialStampingTool re-runs the gate, answers an
\* argument error as a client error, then the facade (data tools) or
\* TelemetryToolHandlers' call-time capability check (telemetry tools)
\* decides against the current policy.
CallGroupTool(t, sc, ctx, bad) ==
    LET c == session.caller
        adv == t \in session.tools
        g == Gate(ctx, policy, t)
        fac == IF t = "telemetry" THEN AllowedUnder(policy, c, "Telemetry", ClusterTelemetry) ELSE \E op \in Required(t) : AllowedUnder(policy, c, op, sc)
        res == IF ~adv THEN "notFound" ELSE IF ~g THEN "denied" ELSE IF bad THEN "clientError" ELSE IF fac THEN "ok" ELSE "denied"
        facObs == IF ~adv \/ ~g \/ bad THEN "skipped" ELSE IF fac THEN "allow" ELSE "deny"
    IN /\ session # NoSession
       /\ obs' = CallObs(t, ctx, bad, sc, adv, g, facObs, res, NoTenant, {})
       /\ UNCHANGED <<policy, appEnabled, authz, session>>

\* Invoking the app tool: CredentialStampingTool re-runs the gate and checks
\* the arguments, then AppMcpToolSource.IsInvocationPermittedAsync re-runs
\* the advertisement decision against the current registry and policy.
CallAppTool(ctx, bad) ==
    LET c == session.caller
        adv == "appTool" \in session.tools
        g == Gate(ctx, policy, "appTool")
        permitted == appEnabled /\ Resolve(c, session.hdr) = "A" /\ RoleHeld(policy, c)
        res == IF ~adv THEN "notFound" ELSE IF ~g THEN "denied" ELSE IF bad THEN "clientError" ELSE IF permitted THEN "ok" ELSE "denied"
        facObs == IF ~adv \/ ~g \/ bad THEN "skipped" ELSE IF permitted THEN "allow" ELSE "deny"
    IN /\ session # NoSession
       /\ obs' = CallObs("appTool", ctx, bad, NoScope, adv, g, facObs, res, NoTenant, {})
       /\ UNCHANGED <<policy, appEnabled, authz, session>>

\* The environment: an operator edits the policy, or disables or re-enables
\* the app's install, between a session's advertisement and its calls.
ChangePolicy ==
    /\ policy' \in Policies \ {policy}
    /\ obs' = NoObs
    /\ UNCHANGED <<appEnabled, authz, session>>

ToggleApp ==
    /\ appEnabled' = ~appEnabled
    /\ obs' = NoObs
    /\ UNCHANGED <<policy, authz, session>>

Next ==
    \/ \E c \in Callers, hdr \in Headers, detail \in BOOLEAN : OpenSession(c, hdr, detail)
    \/ CallMeta
    \/ \E ctx \in BOOLEAN : CallRegions(ctx)
    \/ \E t \in GroupTools, ctx \in BOOLEAN, bad \in BOOLEAN : \E sc \in CallScopes(t) : CallGroupTool(t, sc, ctx, bad)
    \/ \E ctx \in BOOLEAN, bad \in BOOLEAN : CallAppTool(ctx, bad)
    \/ ChangePolicy
    \/ ToggleApp

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ policy \in Policies
    /\ appEnabled \in BOOLEAN
    /\ authz \in AuthzKinds
    /\ session \in {NoSession} \cup SessionRecs
    /\ obs \in {NoObs} \cup CallObsRecs

IsCall == obs.kind = "call"
Open == session # NoSession

\* LockStep: under the snapshot a session was built from, it advertises only
\* tools the design entitles its caller to, and every one of them when the
\* resolver carried operation detail; and a call is served only when the tool
\* was advertised AND the design entitles the call under the snapshot current
\* when it is made. Advertisement is never a capability.
LockStep ==
    /\ Open => \A t \in session.tools : Entitled(session.caller, t, session.hdr, session.pol, session.app)
    /\ (Open /\ session.detail) => \A t \in AllTools : Entitled(session.caller, t, session.hdr, session.pol, session.app) => t \in session.tools
    /\ (IsCall /\ obs.result = "ok") => obs.adv /\ obs.ctx /\ EntitledCall(obs)

\* OnlyMetaToolUngated: every advertised tool but lattice_capabilities was
\* admitted by the authorizer, and every served call but it passed the gate.
OnlyMetaToolUngated ==
    /\ Open => \A t \in session.tools \ {"caps"} : AuthorizerAdmits(session.pol, t)
    /\ (IsCall /\ obs.result = "ok" /\ obs.tool # "caps") => obs.gate

\* PerToolMinimum: a group tool is advertised only when the resolver's
\* carried operations meet its minimum, so an access set carrying no
\* operation reaches no group tool; and a withheld tool is unreachable.
PerToolMinimum ==
    /\ Open => \A t \in session.tools \cap GroupTools : session.ops \cap Required(t) # {}
    /\ (Open /\ session.ops = {}) => session.tools \cap GroupTools = {}
    /\ (IsCall /\ obs.result = "ok" /\ obs.tool \in GroupTools) => obs.adv

\* ScopelessToolsClusterWide: the MCP telemetry tools are advertised,
\* carried and served only on cluster-wide Telemetry. A Telemetry grant over
\* a tenant's capability scope (#5582) never reaches one.
ScopelessToolsClusterWide ==
    /\ Open => ("telemetry" \in session.tools => AllowedUnder(session.pol, session.caller, "Telemetry", ClusterTelemetry))
    /\ Open => ("Telemetry" \in session.ops => AllowedUnder(session.pol, session.caller, "Telemetry", ClusterTelemetry))
    /\ (IsCall /\ obs.tool = "telemetry" /\ obs.result = "ok") => AllowedUnder(obs.pol, obs.caller, "Telemetry", ClusterTelemetry)

\* FailClosed: an anonymous session is offered nothing, the default
\* DenyAllMcpAuthorizer and an absent authorizer leave only the meta-tool,
\* and no call without a request context, or under either, is served past
\* the meta-tool.
FailClosed ==
    /\ (Open /\ session.caller = Anonymous) => session.tools = {}
    /\ (Open /\ authz \in {"denyAll", "absent"}) => session.tools \subseteq {"caps"}
    /\ (IsCall /\ obs.result = "ok") => obs.caller \in Authenticated
    /\ (IsCall /\ obs.result = "ok" /\ obs.tool # "caps") => obs.ctx /\ authz = "byPolicy"

\* ActiveTenantValidated: a tenant-scoped decision uses the asserted tenant
\* only when it re-resolves to itself. A refused assertion degrades the
\* region list to the current region and is never echoed back.
ActiveTenantValidated ==
    /\ Open => ("appTool" \in session.tools => session.hdr \in Member(session.caller))
    /\ (IsCall /\ obs.tool = "appTool" /\ obs.result = "ok") => obs.hdr \in Member(obs.caller)
    /\ (IsCall /\ obs.tool = "regions" /\ obs.result = "ok" /\ obs.annot # NoTenant) => obs.annot = obs.hdr /\ obs.hdr \in Member(obs.caller)
    /\ (IsCall /\ obs.tool = "regions" /\ obs.result = "ok" /\ obs.hdr # NoTenant /\ obs.hdr \notin Member(obs.caller)) => obs.regions = {"current"} /\ obs.annot = NoTenant

\* DenialNotClientError: a call the gate refuses, or the tool's own check
\* refuses, is answered as a denial, never as a client error.
DenialNotClientError ==
    /\ (IsCall /\ obs.adv /\ ~obs.gate) => obs.result = "denied"
    /\ (IsCall /\ obs.facade = "deny") => obs.result = "denied"
    /\ (IsCall /\ obs.result = "clientError") => obs.gate /\ obs.facade = "skipped"
=============================================================================
