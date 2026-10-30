---- MODULE SchemaTenantScope ----
EXTENDS Naturals

\* The schema facade under tenancy: a caller's tree name is resolved once to
\* its effective, tenant-scoped tree, that tree is authorized through the
\* access gate, and the operation acts on exactly that tree. Every per-tree
\* schema setting (policy, version config, runtime catalog, dead letters) is
\* keyed by the effective tree, and tracked schema operations are recorded
\* under the caller's tenant. The tenancy layer can lose the ability to
\* confirm its policy, and a caller can fail tenant attribution altogether.
\*
\* Rely (restated, no INSTANCE): the tenancy gate denies a caller it cannot
\* confirm and admits a crossing into another tenant's namespace only on that
\* tenant's grant (TenantIsolation); the access gate admits a platform-owned
\* tree on capability alone, which is why resolution must refuse a confined
\* caller's escape into one.

CONSTANTS MaxRequests, MaxFaults

\* "D" is the reserved default tenant, "T1" a confined tenant, and "None" a
\* caller whose tenant assertion failed validation or could not be resolved;
\* "None" holds every capability, so its refusal never rests on a grant.
Callers == {"D", "T1", "None"}
\* An unqualified name, another tenant's qualified name, a reserved add-on
\* name, and a malformed tenant-namespace name.
Names == {"x", "t/T2/x", "sys-x", "t/bad"}
Trees == {"x", "t/T1/x", "t/T2/x", "sys-x", "t/bad"}
Ops == {"SetPolicy", "Register", "GetRegistry", "Start", "Status"}
Caps == {"Admin", "Read"}
Phases == {"Idle", "Received", "Resolved", "Authorized"}

Owner(t) ==
    CASE t = "x" -> "D"
      [] t = "t/T1/x" -> "T1"
      [] t = "t/T2/x" -> "T2"
      [] OTHER -> "Platform"

\* The single tenant-resolution seam: a denying tenant fails closed, the default
\* tenant keeps the bare name, a confined tenant composes an unqualified name
\* into its namespace, passes a well-formed qualified one through to the gate,
\* and is refused a reserved or malformed one.
Compose(c, n) ==
    IF c = "None" THEN "Denied"
    ELSE IF c = "D" THEN n
    ELSE CASE n = "x" -> "t/T1/x"
           [] n = "t/T2/x" -> n
           [] OTHER -> "Denied"

CapOf(op) == IF op \in {"GetRegistry", "Status"} THEN "Read" ELSE "Admin"

VARIABLES grant, cross, confirm, faults, count, req,
          policy, catalog, ops, disclosed, seen
vars == <<grant, cross, confirm, faults, count, req,
          policy, catalog, ops, disclosed, seen>>

\* The tenancy half of the access gate: admitted only while the tenancy layer can
\* confirm its policy, into the caller's own namespace, a platform-owned tree, or
\* a tenant namespace whose owner granted the crossing.
TenancyAllows(c, t) ==
    /\ confirm
    /\ \/ Owner(t) = c
       \/ Owner(t) = "Platform"
       \/ (Owner(t) = "T2" /\ c = "T1" /\ cross)

Allowed(c, t, cap) == cap \in grant[c] /\ TenancyAllows(c, t)

\* Whether a caller's tenant may hold an effect on, or see, a tree: its own
\* namespace, a granted crossing, or, for the default tenant, a platform tree.
Within(c, t) ==
    \/ Owner(t) = c
    \/ (Owner(t) = "T2" /\ c = "T1" /\ cross)
    \/ (c = "D" /\ Owner(t) = "Platform")

Req == [c : Callers, n : Names, op : Ops, eff : Trees \cup {"None"}, phase : Phases]
NoReq == [c |-> "D", n |-> "x", op |-> "GetRegistry", eff |-> "None", phase |-> "Idle"]

TypeOK ==
    /\ grant \in [Callers -> SUBSET Caps]
    /\ cross \in BOOLEAN /\ confirm \in BOOLEAN
    /\ faults \in 0..MaxFaults /\ count \in 0..MaxRequests
    /\ req \in Req
    /\ policy \in [Trees -> SUBSET Callers]
    /\ catalog \in [Trees -> SUBSET Callers]
    /\ ops \subseteq (Callers \X Trees)
    /\ disclosed \subseteq (Callers \X Trees)
    /\ seen \subseteq (Callers \X Callers \X Trees)

\* Every schema setting a caller changed, and every operation it started, is on
\* a tree its tenant owns, a crossing its owner granted, or, for the default
\* tenant, a platform tree; an unattributed caller changed nothing.
EffectsStayInTenant ==
    /\ \A t \in Trees : \A c \in policy[t] \cup catalog[t] : c # "None" /\ Within(c, t)
    /\ \A p \in ops : p[1] # "None" /\ Within(p[1], p[2])

\* The same holds for every schema setting a caller read.
DisclosureStaysInTenant ==
    \A p \in disclosed : p[1] # "None" /\ Within(p[1], p[2])

\* A status read sees only operations recorded under the caller's own tenant.
StatusSeesOwnTenantOnly == \A s \in seen : s[1] = s[2]

Mutated == policy' # policy \/ catalog' # catalog \/ ops' # ops
Changed(f) == {t \in Trees : f'[t] # f[t]}

\* A schema mutation needs the SchemaAdmin capability; Read is not enough.
ManageNeedsSchemaAdmin ==
    [][Mutated => "Admin" \in grant[req.c]]_vars

\* A schema read needs Read authority.
ReadNeedsRead ==
    [][(disclosed' # disclosed \/ seen' # seen) => "Read" \in grant[req.c]]_vars

\* An operation acts on exactly the effective tree it authorized: the name is
\* resolved once, and the gate and the operation see the same id.
ActsOnAuthorizedTree ==
    [][/\ Changed(policy) \cup Changed(catalog) \subseteq {req.eff}
       /\ \A p \in (ops' \ ops) \cup (disclosed' \ disclosed) : p[2] = req.eff]_vars

\* A caller with no attributable tenant is refused at resolution.
UnattributableDenied ==
    [][(req.phase = "Received" /\ req'.phase = "Resolved") => req.c # "None"]_vars

\* A confined tenant never resolves to a reserved or malformed tree, which the
\* gate would admit on capability alone.
EscapeRefusedAtResolution ==
    [][(req.phase = "Received" /\ req'.phase = "Resolved" /\ req.c = "T1" /\ req.op # "Status")
       => Owner(req'.eff) \in {"T1", "T2"}]_vars

\* Authorization succeeds only while the tenancy layer can confirm its policy.
AuthorizedOnlyWhenConfirmed ==
    [][(req.phase = "Resolved" /\ req'.phase = "Authorized" /\ req.op # "Status") => confirm]_vars

Init ==
    /\ grant \in {g \in [Callers -> SUBSET Caps] : g["None"] = Caps}
    /\ cross \in BOOLEAN
    /\ confirm = TRUE /\ faults = 0 /\ count = 0
    /\ req = NoReq
    /\ policy = [t \in Trees |-> {}]
    /\ catalog = [t \in Trees |-> {}]
    /\ ops = {} /\ disclosed = {} /\ seen = {}

Data == <<policy, catalog, ops, disclosed, seen>>

\* A caller sends a schema facade request naming a tree.
Receive(c, n, op) ==
    /\ req.phase = "Idle" /\ count < MaxRequests
    /\ req' = [c |-> c, n |-> n, op |-> op, eff |-> "None", phase |-> "Received"]
    /\ count' = count + 1
    /\ UNCHANGED <<grant, cross, confirm, faults, Data>>

\* The facade resolves the caller's tenant and the effective tree once. A status
\* read is addressed by operation id under the caller's tenant, not by tree.
Resolve ==
    /\ req.phase = "Received"
    /\ LET eff == IF req.op = "Status" /\ req.c # "None" THEN "None" ELSE Compose(req.c, req.n)
       IN IF eff = "Denied"
          THEN req' = NoReq
          ELSE req' = [req EXCEPT !.eff = eff, !.phase = "Resolved"]
    /\ UNCHANGED <<grant, cross, confirm, faults, count, Data>>

\* The schema authorizer consults the access gate over the whole effective
\* tree for the operation's capability, and fails closed. A status read is
\* authorized per record when it is served.
Authorize ==
    /\ req.phase = "Resolved"
    /\ IF req.op = "Status" \/ Allowed(req.c, req.eff, CapOf(req.op))
       THEN req' = [req EXCEPT !.phase = "Authorized"]
       ELSE req' = NoReq
    /\ UNCHANGED <<grant, cross, confirm, faults, count, Data>>

\* The operation runs against the effective tree it authorized.
Act ==
    /\ req.phase = "Authorized"
    /\ LET c == req.c
           t == req.eff
       IN CASE req.op = "SetPolicy" ->
                 /\ policy' = [policy EXCEPT ![t] = @ \cup {c}]
                 /\ UNCHANGED <<catalog, ops, disclosed, seen>>
            [] req.op = "Register" ->
                 /\ catalog' = [catalog EXCEPT ![t] = @ \cup {c}]
                 /\ UNCHANGED <<policy, ops, disclosed, seen>>
            [] req.op = "Start" ->
                 /\ ops' = ops \cup {<<c, t>>}
                 /\ UNCHANGED <<policy, catalog, disclosed, seen>>
            [] req.op = "GetRegistry" ->
                 /\ disclosed' = disclosed \cup {<<c, t>>}
                 /\ UNCHANGED <<policy, catalog, ops, seen>>
            [] OTHER ->
                 /\ seen' = seen \cup {<<c, p[1], p[2]>> : p \in {q \in ops : q[1] = c /\ Allowed(c, q[2], "Read")}}
                 /\ UNCHANGED <<policy, catalog, ops, disclosed>>
    /\ req' = NoReq
    /\ UNCHANGED <<grant, cross, confirm, faults, count>>

\* Environment: the tenancy layer loses the ability to confirm its policy.
Unconfirm ==
    /\ confirm /\ faults < MaxFaults
    /\ confirm' = FALSE /\ faults' = faults + 1
    /\ UNCHANGED <<grant, cross, count, req, Data>>

\* Environment: the tenancy layer can confirm its policy again.
Reconfirm ==
    /\ ~confirm
    /\ confirm' = TRUE
    /\ UNCHANGED <<grant, cross, faults, count, req, Data>>

Stutter == UNCHANGED vars

Next ==
    \/ \E c \in Callers, n \in Names, op \in Ops : Receive(c, n, op)
    \/ Resolve
    \/ Authorize
    \/ Act
    \/ Unconfirm
    \/ Reconfirm
    \/ Stutter

Spec == Init /\ [][Next]_vars
====
