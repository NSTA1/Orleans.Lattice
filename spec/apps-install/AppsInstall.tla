----------------------------- MODULE AppsInstall -----------------------------
(***************************************************************************)
(* Installable apps (issue #6051): the cluster-wide AppInstall capability *)
(* that gates an install or upgrade, the manifest digest pin that ties an *)
(* install to the manifest an operator reviewed, the role compiler that   *)
(* never emits a scopeless capability, the app: rule namespace only       *)
(* system origin may write, the alias tree-ownership guard, and the app   *)
(* UI bridge that answers a workspace verb.                               *)
(*                                                                         *)
(* Every caller's own authority is the abstract operator                   *)
(* Allowed(caller, op, scope) of the F1 contract (spec/auth-decision,      *)
(* contract version 1). The cfg binds it to ContractAllowed, the           *)
(* contract's minimal grant-set definition.                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANT Allowed(_, _, _)

\* ---- F1 contract vocabulary (contract version 1), the subset used here ----
NoKey == "none"
NoTenant == "none"
\* op holds AppInstall cluster-wide; scoped holds AppInstall on one data tree
\* only, which is not the scope the install gate asks for; mallory holds
\* nothing.
Callers == {"op", "scoped", "mallory"}
BootstrapAdministrators == {}
AppInstallOp == "AppInstall"
Tree(kind, t) == [kind |-> kind, tenant |-> t]
ClusterWideTreeId == Tree("clusterWide", NoTenant)
DataTree == Tree("data", NoTenant)
Scope(tree, key) == [tree |-> tree, key |-> key]
Grants ==
    { [caller |-> "op", op |-> AppInstallOp, scope |-> Scope(ClusterWideTreeId, NoKey)],
      [caller |-> "scoped", op |-> AppInstallOp, scope |-> Scope(DataTree, NoKey)] }
ContractAllowed(c, op, s) ==
    c \in BootstrapAdministrators \/ [caller |-> c, op |-> op, scope |-> s] \in Grants

HoldsClusterWideAppInstall(c) == Allowed(c, AppInstallOp, Scope(ClusterWideTreeId, NoKey))

\* ---- The instance ----
\* Two manifest contents with distinct digests. The source can change
\* between a review and the install.
Contents == {"M1", "M2"}
Digests == {"D1", "D2"}
Digest(m) == IF m = "M1" THEN "D1" ELSE "D2"
\* Capabilities a role ceiling can name. Telemetry and AppInstall are
\* scopeless: they are granted cluster-wide or not at all.
Ops == {"Read", "Telemetry", "AppInstall"}
ScopelessOps == {"Telemetry", "AppInstall"}
\* data1 and data2 are ordinary data trees the app declares; reserved is a
\* system tree no app role may reach.
Trees == {"data1", "data2", "reserved"}
GrantableTrees == {"data1", "data2"}
\* The one app-owned rule id, in the reserved app: namespace.
RuleIds == {"app:x:r1"}
Origins == {"absent", "system", "user"}
\* Alias names and alias targets, with their ledger owners. La is owned by
\* the app, Ln by nobody. owned is the app's tree; copy is a derived copy of
\* it (a restore or snapshot), whose own ledger row names no owner.
Aliases == {"La", "Ln"}
AliasTargets == {"data1", "owned", "copy"}
Owner(x) == IF x \in {"La", "owned"} THEN "app" ELSE "none"
DerivedFrom == [p \in {"copy"} |-> "owned"]
Root(p) == IF p \in DOMAIN DerivedFrom THEN DerivedFrom[p] ELSE p
\* The ownership guard bound in the silo: the ledger-backed guard, the
\* default guard registered when no apps package is present, or a guard
\* whose ledger read faults.
Guards == {"ledger", "null", "faulty"}
\* The bridge: alice is a member of the app's role, mallory of none. A
\* request names an app, the revision the page was served at, and a tree.
RoleMembers == {"alice"}
BridgeCallers == {"alice"}
Apps == {"x", "missing"}
DeclaredTrees == GrantableTrees
RequestTrees == {"data1", "data2", "other"}
\* What a caller is told about an app that does not exist.
AbsentAnswer == "denied"

\* ---- Observations of the last step ----
\* An observation carries each property's verdict on the step that made it,
\* judged from what the step did and from who the caller was, never from
\* the decision under test.
NoObs == [kind |-> "none"]
Obs ==
    {NoObs}
    \cup [kind : {"install"}, gated : BOOLEAN, pinned : BOOLEAN]
    \cup [kind : {"rule"}, gated : BOOLEAN, discreet : BOOLEAN]
    \cup [kind : {"alias"}, inert : BOOLEAN]
    \cup [kind : {"bridge"}, discreet : BOOLEAN, granted : BOOLEAN]

VARIABLES
    source,      \* the manifest content the source currently serves
    seen,        \* the content the operator reviewed, or "none"
    pin,         \* the digest the review recorded, or "none"
    installed,   \* the installed content, or "none"
    roleGrants,  \* the compiled <<op, tree>> grants of the app's role
    rules,       \* the origin of the last write to each app-owned rule id
    alias,       \* the target each alias name resolves to, or "none"
    obs          \* the verdicts of the last step

vars == <<source, seen, pin, installed, roleGrants, rules, alias, obs>>

Init ==
    /\ source = "M1"
    /\ seen = "none"
    /\ pin = "none"
    /\ installed = "none"
    /\ roleGrants = {}
    /\ rules = [r \in RuleIds |-> "absent"]
    /\ alias = [l \in Aliases |-> "none"]
    /\ obs = NoObs

\* The operator reviews the manifest the source serves and records its digest.
Review ==
    /\ seen' = source
    /\ pin' = Digest(source)
    /\ obs' = NoObs
    /\ UNCHANGED <<source, installed, roleGrants, rules, alias>>

\* The source changes the manifest after (or before) a review. The recorded
\* review stands.
ChangeManifest(m) ==
    /\ m # source
    /\ source' = m
    /\ seen' = seen
    /\ obs' = NoObs
    /\ UNCHANGED <<pin, installed, roleGrants, rules, alias>>

\* Install or upgrade, optionally pinned to the reviewed digest. A non-system
\* caller needs AppInstall over the cluster-wide tree; a pinned install whose
\* resolved manifest no longer has the pinned digest is refused before any
\* write.
Install(c, sys, pinned) ==
    LET auth == sys \/ HoldsClusterWideAppInstall(c)
        matches == ~pinned \/ (pin # "none" /\ Digest(source) = pin)
        ok == auth /\ matches
    IN /\ installed' = IF ok THEN source ELSE installed
       /\ obs' = [kind |-> "install",
                  gated |-> (ok => (sys \/ HoldsClusterWideAppInstall(c))),
                  pinned |-> ((ok /\ pinned) => source = seen)]
       /\ UNCHANGED <<source, seen, pin, roleGrants, rules, alias>>

\* The role compiler admits one entry of a role ceiling: never a scopeless
\* capability, and only on an ordinary data tree.
CompileRoles(o, t) ==
    LET admit == o \notin ScopelessOps /\ t \in GrantableTrees
    IN /\ roleGrants' = IF admit THEN roleGrants \cup {<<o, t>>} ELSE roleGrants
       /\ obs' = NoObs
       /\ UNCHANGED <<source, seen, pin, installed, rules, alias>>

\* Activation persists the app's rules inside a system-origin scope.
ActivateRules ==
    LET origin == "system"
    IN /\ installed # "none"
       /\ rules' = [r \in RuleIds |-> origin]
       /\ obs' = NoObs
       /\ UNCHANGED <<source, seen, pin, installed, roleGrants, alias>>

\* A put of an app-owned rule id. The store refuses it, before any read or
\* write, unless the caller is system origin.
WriteAppRule(sys) ==
    LET admitted == sys
        r == CHOOSE x \in RuleIds : TRUE
    IN /\ rules' = IF admitted
                   THEN [rules EXCEPT ![r] = IF sys THEN "system" ELSE "user"]
                   ELSE rules
       /\ obs' = [kind |-> "rule", gated |-> (admitted => sys), discreet |-> TRUE]
       /\ UNCHANGED <<source, seen, pin, installed, roleGrants, alias>>

\* A delete of an app-owned rule id. The refusal comes before the store reads
\* the rule, so a refused caller learns nothing about whether it exists.
DeleteAppRule(sys) ==
    LET r == CHOOSE x \in RuleIds : TRUE
        present == rules[r] # "absent"
        Answer(s, pr) == IF ~s THEN "appOwned" ELSE IF pr THEN "removed" ELSE "notFound"
        ans == Answer(sys, present)
    IN /\ rules' = IF ans = "removed" THEN [rules EXCEPT ![r] = "absent"] ELSE rules
       /\ obs' = [kind |-> "rule",
                  gated |-> (ans = "removed" => sys),
                  discreet |-> (sys \/ Answer(FALSE, TRUE) = Answer(FALSE, FALSE))]
       /\ UNCHANGED <<source, seen, pin, installed, roleGrants, alias>>

\* An alias assignment, by any caller including system origin. The guard
\* allows it only when the alias name and the target's derivation root share
\* an owner; the default guard and a faulting guard write nothing. System
\* origin is not consulted: the guard binds it too.
SetAlias(sys, l, p, g) ==
    LET root == Root(p)
        decision == CASE g = "ledger" -> (IF Owner(l) = Owner(root) THEN "allow" ELSE "deny")
                      [] g = "null" -> "default"
                      [] g = "faulty" -> "fault"
        write == decision = "allow"
    IN /\ alias' = IF write THEN [alias EXCEPT ![l] = p] ELSE alias
       /\ obs' = [kind |-> "alias", inert |-> (g # "ledger" => ~write)]
       /\ UNCHANGED <<source, seen, pin, installed, roleGrants, rules>>

\* The bridge authorizes a workspace verb: the app must be installed at the
\* requested revision, the tree must be one the app declares, and the caller
\* must hold a role grant on it.
BridgeAuthorize(c, a, rev, t) ==
    LET live == a = "x" /\ installed # "none" /\ Digest(installed) = rev
        holds == c \in RoleMembers /\ <<"Read", t>> \in roleGrants
        ans == IF ~live THEN "denied"
               ELSE IF t \notin DeclaredTrees THEN "notFound"
               ELSE IF ~holds THEN "denied"
               ELSE "allowed"
    IN /\ obs' = [kind |-> "bridge",
                  discreet |-> (~live => ans = AbsentAnswer),
                  granted |-> (ans = "allowed" => (c \in RoleMembers /\ <<"Read", t>> \in roleGrants))]
       /\ UNCHANGED <<source, seen, pin, installed, roleGrants, rules, alias>>

Next ==
    \/ Review
    \/ \E m \in Contents : ChangeManifest(m)
    \/ \E c \in Callers, s \in BOOLEAN, p \in BOOLEAN : Install(c, s, p)
    \/ \E o \in Ops, t \in Trees : CompileRoles(o, t)
    \/ ActivateRules
    \/ \E s \in BOOLEAN : WriteAppRule(s)
    \/ \E s \in BOOLEAN : DeleteAppRule(s)
    \/ \E s \in BOOLEAN, l \in Aliases, p \in AliasTargets, g \in Guards : SetAlias(s, l, p, g)
    \/ \E c \in BridgeCallers, a \in Apps, r \in Digests, t \in RequestTrees : BridgeAuthorize(c, a, r, t)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ source \in Contents
    /\ seen \in Contents \cup {"none"}
    /\ pin \in Digests \cup {"none"}
    /\ installed \in Contents \cup {"none"}
    /\ roleGrants \subseteq Ops \X Trees
    /\ rules \in [RuleIds -> Origins]
    /\ alias \in [Aliases -> AliasTargets \cup {"none"}]
    /\ obs \in Obs

\* An install or upgrade that writes was made by system origin or by a caller
\* holding AppInstall over the cluster-wide tree, never a narrower scope.
InstallOnlyWithClusterWideAppInstall ==
    obs.kind = "install" => obs.gated

\* No compiled role grant carries a scopeless capability or reaches a tree
\* that is not an ordinary data tree.
RoleCompilerNeverEmitsScopeless ==
    \A g \in roleGrants : g[1] \notin ScopelessOps /\ g[2] \in GrantableTrees

\* Every app-owned rule was last written by system origin, a non-system write
\* or delete never lands, and a refused delete does not disclose whether the
\* rule exists.
AppRulesSystemOriginOnly ==
    /\ \A r \in RuleIds : rules[r] # "user"
    /\ obs.kind = "rule" => (obs.gated /\ obs.discreet)

\* Every alias resolves to a target whose derivation root has the alias
\* name's owner, and a default decision or a guard fault writes nothing.
AliasOwnershipPreserved ==
    /\ \A l \in Aliases : alias[l] # "none" => Owner(l) = Owner(Root(alias[l]))
    /\ obs.kind = "alias" => obs.inert

\* A pinned install that writes installs exactly the content the operator
\* reviewed.
InstalledEqualsReviewed ==
    obs.kind = "install" => obs.pinned

\* For a caller who is a member of the app's role, an app that is not
\* installed at the requested revision is answered exactly as an app that
\* does not exist, and a verb is allowed only with the role grant on the tree.
WorkspaceNonDisclosure ==
    obs.kind = "bridge" => (obs.discreet /\ obs.granted)

=============================================================================
