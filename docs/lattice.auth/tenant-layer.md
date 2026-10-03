# The tenant rule layer

With [delegated tenant access administration](../lattice.tenancy/README.md#delegated-tenant-access-administration)
on, a tenant's own administrators write **tenant-tier rules** on their tenant's
trees. This page describes how the authorization layer evaluates those rules beneath
the operator's, and which guards keep them inside the tenant. The rules themselves are
ordinary `LatticeAuthorizationRule` records in the same policy store; what makes one
tenant-tier is its reserved rule id.

## The seam

`ITenantRuleLayer` (`bool IsActive`) is the on/off switch. `AddLatticeAuth` registers
an inactive default with `TryAdd`, and `Orleans.Lattice.Tenancy` replaces it with one
whose `IsActive` reads `LatticeTenancyOptions.DelegatedAccessAdministrationEnabled`.
The auth package never references tenancy. With tenancy absent or the flag off, the
policy snapshot builds no tenant partition, the decision engine reads one `bool` per
decision and never enters the tenant layer, and every tenant-tier rule is left out of
evaluation entirely.

When the layer becomes active over a snapshot built without it, the engine schedules
a rebuild. Until that rebuild lands the tenant layer holds no rules, so a request it
would have decided falls through to `DefaultEffect`; under the recommended `Deny`
default that fails closed. Turning the layer off takes effect on the next decision.

## Two layers

- **The operator layer** is every rule whose id does not start with `tenant:`,
  including the cluster-wide `Tree:*` tier and the `app:` rules installed apps
  compile. It is evaluated exactly as without the tenant layer (see
  [Precedence](README.md#precedence)). **A matched operator verdict, allow or deny,
  is final.**
- **The tenant layer** runs only when no operator rule matches, only while
  `ITenantRuleLayer.IsActive`, and only for a **tenant-layer tree**: a tree id
  `t/{tenant}/{name}` whose tenant is valid and not `default`, and whose `{name}` is
  not `*` and does not start with `a/` (an app-owned tree), `sys-` or `_lattice_`. It
  is the layer of the tree's **owner**, whoever the caller is. Its order is:
  1. A matching **tenant-wide deny** denies.
  2. Otherwise the tree's own **most-specific tenant verdict** applies: key, then
     longest prefix, then the whole tree, with deny overriding allow at an equal tier
     and `UserRuleBeatsGroupRuleAtEqualScope` honoured.
  3. Otherwise a matching **tenant-wide allow** grants.
  4. Otherwise `DefaultEffect` applies.

The control-plane trees (`sys-auth-*`, `sys-tenant-*`, `sys-app-*`, the
tenant-administration capability ids and the `*` sentinel) are never tenant-layer
trees, so their fail-closed rule is unchanged.

So a tenant key-scoped allow can never carve a hole in an operator deny, and a tenant
deny can never revoke an operator allow:

| Operator rules on `t/acme/orders` | Tenant rules | Request | Decision |
|---|---|---|---|
| Deny `Read` on the tree | Allow `Read` on key `k1` | `Read` `k1` | Deny (operator, final) |
| Allow `Read` on the tree | Deny `Read` on key `k1` | `Read` `k1` | Allow (operator, final) |
| none matching | Allow `Read` tenant-wide, deny `Read` on prefix `eu/` | `Read` `eu/k2` | Deny (tenant tree verdict) |
| none matching | Allow `Read` tenant-wide | `Read` `k3` | Allow (tenant-wide allow) |
| none matching | none | `Read` `k3` | `DefaultEffect` |

### Range and scan filters

A collection request (a range read, a scan, a range delete) composes the two layers
key by key, so its filter admits exactly the keys a point request would allow. As
sets, the admitted keys are the keys an operator rule allows, plus the keys the
tenant layer (or the default effect) allows that **no operator rule covers**, minus
the keys an operator rule denies. A key is covered by the operator layer when any
operator rule for the subject and operation matches it, at key, prefix, tree or
all-trees scope; coverage, not the rule's effect, is what hands the key to the
operator layer.

The decision is uniform, with no per-key filter, only when neither layer can vary by
key: the operator layer has no key or prefix rule for the subject and operation (and
so decides every key alike, ending the evaluation if it matched), and the tenant
tree has none either. A request the existence checks make - whether a subject holds
any grant on a tree - follows the same two layers.

## Tenant-tier rule ids

`LatticeTenantRuleIds` owns the reserved namespace:

- `Prefix` is `tenant:`. A rule id that starts with it is reserved to the tenant tier
  however the remainder is spelled.
- `For(TenantId, string)` composes `tenant:{tenant}:{localId}`, refusing the
  uninitialised and the `default` tenant.
- `IsTenantOwned(string)` tests the prefix; `TryGetTenant(string, out TenantId)`
  parses the owning tenant from a well-formed id.

```csharp verify
using Orleans.Lattice;
using Orleans.Lattice.Auth;

var acme = TenantId.Parse("acme");

// What the tenant facade writes for a tenant-wide read grant to a tenant group.
var rule = new LatticeAuthorizationRule(
    ruleId: LatticeTenantRuleIds.For(acme, "readers-read"),
    subject: LatticeSubjectSelector.Group(LatticeTenantGroupId.Compose(acme, "readers").Value),
    scope: LatticeScope.TenantWide(acme),
    operations: LatticeOperation.Read | LatticeOperation.RangeRead,
    effect: LatticeEffect.Allow);

bool tenantOwned = LatticeTenantRuleIds.IsTenantOwned(rule.RuleId); // true
bool tenantWide = rule.Scope.IsTenantWide(); // true
```

The policy store refuses a `PutRuleAsync` or `RemoveRuleAsync` of a `tenant:` id from
any caller not already running under system origin, a bootstrap administrator
included, with `LatticeTenantOwnedRuleException` (an `ArgumentException` carrying
`RuleId`), before anything is read or written. Only the tenant facade
(`ILatticeTenantPolicyAdmin`) writes these rules, after its own tenant-admin check.
Reads are not guarded, so operators can list tenant-tier rules, and the cluster
facade's `RemoveRuleAsync` removes one under system origin as a break-glass action
(see [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md#tenant-tier-alignment)).

## The tenant-wide scope

`LatticeScope.TenantWide(TenantId)` is a whole-tree scope over the sentinel tree id
`t/{tenant}/*`, standing for every tree the tenant owns. It is the tenant-bounded
analogue of `LatticeScope.ClusterWide()`, but it never reaches an app-owned, reserved
or system tree, nor another tenant's trees. `IsTenantWide()` and
`TryGetTenantWideTenant(out TenantId)` recognise it; they are methods rather than
properties so the record's printed and serialized members are unchanged. No real tree
can carry the sentinel id: the data plane refuses a user-origin write to a tenant tree
id ending in `/*`.

The tenant-wide scope is authorable only in the tenant layer. The policy store
refuses a rule over a tree id of the shape `t/{x}/*` unless its id is `tenant:` and
its scope is exactly `LatticeScope.TenantWide` of the rule's own tenant, so no
operator or app rule can use it. The cluster `Tree:*` tier and
`AllTreesGrantsEnabled` are unchanged, and `Tree:*` stays operator-only.

## Confinement guards

The policy store checks every write against these rules for **every caller, system
origin included**, and refuses a violation with an `ArgumentException` naming the
`rule` parameter:

- **A tenant-tier rule** (`tenant:` id) must parse with
  `LatticeTenantRuleIds.TryGetTenant`, must be scoped to `TenantWide` of that tenant or
  to one of its tenant-layer trees, must carry a non-empty subset of
  `LatticeAuthOperations.All` (never `Telemetry`, `Replication`, `TreeLifecycle` or
  `AppInstall`), and may name a tenant group only of its own tenant. Users and
  cluster groups are allowed.
- **Any rule whose subject is a group id starting with `t/`** must name a well-formed
  `LatticeTenantGroupId` (so `t/default/...` and malformed ids are refused) and may
  be scoped only to one of that group's tenant's tenant-layer trees, to that tenant's
  `TenantWide` scope (which only a tenant-tier rule may use), or - for an `app:` rule
  only - to one of that tenant's app-owned trees (`t/{tenant}/a/...`). A rule on
  `Tree:*`, a legacy or `default` tree, a `sys-` or `_lattice_` tree, or another
  tenant's tree may never name a tenant group. User subjects are not affected.

An operator rule or an app role rule that names a tenant group is evaluated in the
operator layer, so it is honoured whatever the delegated-access flag says. That is why
the membership package strips identity-provider-asserted `t/` groups whenever tenancy
is registered rather than only while the flag is on: a token asserting
`t/{tenant}/{name}` can never match such a rule (see
[Tenant groups](../lattice.membership/README.md#tenant-groups)).

When the snapshot compiles, it also drops any tenant-tier rule that breaks the first
rule above, so one that reached the store by restore or replication without passing
the guard is never evaluated.

## Explaining a decision

A deny the tenant layer decides reads `Denied by tenant rule '{ruleId}' ({scope}
scope) ...`, where the scope is `tenant-wide`, a key, a prefix, or `tree`. The
cluster facade's `ExplainAsync` reports the deciding layer as
`AuthExplanation.DecidingLayer` (`Platform` or `Tenant`) with `DecidingRuleId`, and
`EffectivePermissionsAsync` labels each rule's layer in `RuleLayers`; see
[`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md#tenant-tier-alignment). A
tenant administrator reads the same through `ILatticeTenantPolicyAdmin.ExplainAsync`,
which withholds the subject and scope of a deciding `Tree:*` or app role rule.

## When a tenant is deleted

Deleting a tenant purges every rule whose id starts with `tenant:{tenant}:`, wherever
it is scoped, under system origin and before the tenant record is removed. The purge
is idempotent; see
[Deleting a tenant purges its access data](../lattice.tenancy/README.md#deleting-a-tenant-purges-its-access-data).
