# Orleans.Lattice.Auth

Authorization and enforcement add-on for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Auth` is the authorization layer of a lattice cluster. It builds on the subjects that [`Orleans.Lattice.Membership`](../lattice.membership/README.md) resolves and adds three things:

- **A policy store** (`ILatticeAuthorizationPolicyStore`) that persists authorization rules durably in a dogfooded `ILattice` tree, so the policy is itself introspectable through the standard read / scan / change-feed surface.
- **A decision engine** that compiles the rule set into an in-memory snapshot and evaluates a request (subject, operation, tree, key/range) into an allow or deny.
- **An enforcing access gate** that the core data path consults on every user-originated operation, so a denied write throws and a denied read reports absent - fail-closed.

Registering the package installs the enforcing gate. A cluster that does not register it runs with the core allow-all gate and pays no authorization cost on the data path (see [Zero cost when disabled](#zero-cost-when-disabled)).

## Setup

Register the three layers in order on the silo: the core lattice, then membership, then authorization.

```csharp verify
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeMembership()
    .AddLatticeAuth(options =>
    {
        // Fail-closed: anything not explicitly allowed is denied.
        options.DefaultEffect = LatticeEffect.Deny;

        // Root-of-trust subjects that bypass policy entirely.
        options.BootstrapAdministrators.Add("root-admin");
    });
```

`AddLatticeAuth(...)` must be called **after** `AddLattice(...)` and `AddLatticeMembership()`. Calling it out of order fails fast at registration with an actionable message rather than failing obscurely at silo start.

## Concepts

### Subject

Every gated operation is evaluated against the caller's resolved **subject**: a stable subject id plus the transitively-expanded closure of groups it belongs to (produced by [`Orleans.Lattice.Membership`](../lattice.membership/README.md)). A rule can target a subject by user id or by group; a group rule applies to every member without naming them.

### Rule

A `LatticeAuthorizationRule` grants or denies a set of operations to a subject selector at a scope:

```csharp verify
var rule = new LatticeAuthorizationRule(
    ruleId: "editors-write-orders",
    subject: LatticeSubjectSelector.Group("editors"),
    scope: LatticeScope.Tree("orders"),
    operations: LatticeOperation.Read | LatticeOperation.Write,
    effect: LatticeEffect.Allow);
```

- **Subject selector** (`LatticeSubjectSelector`) targets a `User(id)` or a `Group(id)`.
- **Operations** (`LatticeOperation`, a `[Flags]` set) name the capability classes covered: `Read`, `Write`, `Delete`, `RangeRead`, `RangeDelete`, `CrdtApply`, `AtomicWrite`, `BulkLoad`, `Admin`, `Backup`, `Restore`, `SchemaAdmin`, `Telemetry`, `Replication`, and `TreeLifecycle`. Grants do not imply each other unless the enum member explicitly says so: for example, `Write` does not confer `Delete`, `Admin` does not confer `Telemetry`, and `Restore` authorizes populating the target scope from a backup without a separate `Write` or `BulkLoad` grant.
- **Effect** (`LatticeEffect`) is `Allow` or `Deny`.
- **Condition** is an optional, opaque string reserved for a future claim / attribute predicate language. Nothing evaluates it in this version: a rule carrying a condition matches exactly as an unconditional rule would, so never rely on one to narrow a grant.

`LatticeAuthOperations.All` is a convenience mask of every tree-scoped data-plane operation, `Read` through `SchemaAdmin`. It deliberately excludes `Telemetry`, `Replication`, and `TreeLifecycle`, so a whole-data-plane grant never confers them; each must be granted explicitly.

Rules are authored through the policy store, resolved from the silo's service provider:

```csharp verify
public sealed class PolicySeeder(ILatticeAuthorizationPolicyStore store)
{
    public async Task GrantAsync(CancellationToken cancellationToken)
    {
        await store.PutRuleAsync(
            new LatticeAuthorizationRule(
                "analysts-read-eu",
                LatticeSubjectSelector.Group("analysts"),
                LatticeScope.Prefix("orders", "eu/"),
                LatticeOperation.Read | LatticeOperation.RangeRead,
                LatticeEffect.Allow),
            cancellationToken);
    }
}
```

### Scope

A `LatticeScope` names how broadly a rule applies within a tree, from broadest to narrowest:

| Scope | Factory | Matches |
|---|---|---|
| Tree | `LatticeScope.Tree(treeId)` | Every key in the tree. |
| Prefix | `LatticeScope.Prefix(treeId, prefix)` | Every key that starts with the prefix. |
| Key | `LatticeScope.Key(treeId, key)` | Exactly one key. |
| Cluster-wide | `LatticeScope.ClusterWide()` | A scopeless, all-trees grant over the `*` sentinel tree id, used for a capability not attached to any single tree (notably `LatticeOperation.Telemetry`). |

### Precedence

When more than one rule matches, the decision engine resolves them deterministically:

1. **Most-specific scope wins.** A hit at a more specific tier (exact key, then longest matching prefix, then tree-wide) is never overridden by a less specific one. A key-scoped allow carves an exception out of a tree-scoped deny, and a key-scoped deny carves a hole out of a tree-scoped allow.
2. **Within a single scope tier, deny overrides allow.** Two sibling rules at the same specificity that disagree resolve to deny (deny-override).
3. **A user rule outranks a group rule at equal scope** (configurable through `UserRuleBeatsGroupRuleAtEqualScope`, default on), so a user-specific allow can lift an individual out of a group-level deny.
4. **Default-deny.** With no matching rule, the configured `DefaultEffect` applies. The recommended and default posture is `Deny`, so anything not explicitly granted is refused.

### Bootstrap administrators

`BootstrapAdministrators` is the root-of-trust: subjects in this set bypass policy entirely and can always read, write, and administer every tree. Use them to seed the very first rules (there is otherwise no one authorized to author policy under default-deny) and for break-glass operations. Keep the set small.

### System-tree replication

The policy store and the membership directory are ordinary `ILattice` trees. In a multi-cluster deployment they are replicated through the [`Orleans.Lattice.Replication`](../lattice.replication/README.md) package's system-tree enrolment (`ReplicateLatticeSystemTrees()`), so a rule authored (or revoked) in one cluster converges to the others. The receiver applies shipped policy writes under a system-origin scope, so a replicated write lands on the destination's default-deny policy tree even though the applier carries no user identity.

### Consistency modes

Policy propagation is **eventually consistent** by default: a rule change is visible once the destination's compiled snapshot rebuilds off the updated policy tree, which happens continuously in the background. A tree that needs a caller to observe a policy change before its next operation can opt into a **strict epoch fence** by naming the tree in `StrictConsistencyTrees`; while a fenced tree's local compiled policy epoch is below the required floor, opted-in user writes to it are rejected (denied) rather than made to wait. Reads, system-origin writes, and replication-applied writes are never fenced.

```csharp verify
siloBuilder.AddLatticeAuth(options =>
{
    options.DefaultEffect = LatticeEffect.Deny;
    options.BootstrapAdministrators.Add("root-admin");

    // "billing" observes policy changes under a strict epoch fence;
    // every other tree stays eventually consistent.
    options.StrictConsistencyTrees = new HashSet<string> { "billing" };
});
```

## Zero cost when disabled

The authorization layer is opt-in. The core `AddLattice(...)` registration installs only the allow-all null gate; `AddLatticeAuth(...)` replaces it with the enforcing policy gate. A cluster that never registers authorization keeps the null gate, whose decision is a synchronously-completed, allocation-free allow that never resolves a subject - so the data path is byte-for-byte what it was before the authorization layer existed.

## Observability

Every authorization decision, the decision latency, and the compiled-snapshot epoch / age are published on a single meter, and an optional audit sink records a durable decision trail. See [Observability](observability.md) for the full instrument catalogue, the audit-sink seam, and the reserved subject-resolution-cache counters.

## Public API

Every type below is in the `Orleans.Lattice.Auth` namespace. The operation vocabulary (`LatticeOperation`) and the access-gate seam the package plugs into are core types.

| Type | Purpose |
|---|---|
| `LatticeAuthServiceCollectionExtensions` | `AddLatticeAuth(configure)` installs the enforcing gate; `ConfigureLatticeAuth(configure)` layers a further `LatticeAuthOptions` delegate after registration. |
| `LatticeAuthOptions` | Every knob; see [Configuration](configuration.md). |
| `LatticeAuthorizationRule`, `LatticeSubjectSelector`, `LatticeScope` | A rule, its subject selector, and its scope, as described above. |
| `LatticeSubjectSelectorKind`, `LatticeScopeKind`, `LatticeEffect` | The discriminators: `User` or `Group`; `Tree`, `Key`, or `Prefix`; `Allow` or `Deny`. |
| `LatticeAuthOperations` | `All`, the whole-data-plane operation mask. |
| `ILatticeAuthorizationPolicyStore` | Durable rule storage: `PutRuleAsync`, `GetRuleAsync`, `RemoveRuleAsync`, `ListRulesForTreeAsync`, and `ListRulesAsync`. |
| `ILatticeDecisionEngine` | Evaluates a request against the compiled policy snapshot, synchronously and in memory (`Evaluate`), and reports the snapshot's `CurrentEpoch`. It is the policy decision only: the enforcing gate layers the bootstrap-administrator bypass, the control-plane fail-closed rule, tenant isolation, and the strict epoch fence around it. |
| `LatticeAuthReservedTrees` | The reserved `sys-auth-*` namespace guard: `Prefix`, `PolicyTreeId`, `IsReserved(treeId)`, and `ThrowIfReserved(treeId)`, which throws the same error the policy store enforces. |
| `LatticePolicyEpochFenceContext` | The caller half of the strict epoch fence: `RequireAtLeast(epoch)` scopes a required policy-epoch floor onto the ambient request context (nesting never lowers an outer floor), and `RequiredEpoch` reads it. |
| `ITenantGateEnforcer` | The tenant-isolation seam the gate consults only for a request the policy already allowed; a deny from either side denies. The default is an inactive allow-everything implementation, and `Orleans.Lattice.Tenancy` supplies the active one. |
| `ILatticeAuthAuditSink`, `LatticeAuthDecisionEvent`, `LatticeAuthAuditVerbosity` | The audit seam, its value-free decision event, and its verbosity; see [Observability](observability.md#the-audit-sink). |
| `LatticeAuthMetrics` | The `orleans.lattice.auth` meter and its instrument and tag names; see [Observability](observability.md). |

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Security posture](security-posture.md) - threat model, attack surface, fail-closed guarantees, the internal-grain trust boundary, the bootstrap-administrator root of trust, and TLS expectations.
- [Observability](observability.md) - the `orleans.lattice.auth` meter and the audit sink.

## See also

- [`Orleans.Lattice.Membership`](../lattice.membership/README.md) - the identity directory and subject-resolution pipeline this layer authorizes against.
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the transport-agnostic control facade for administering membership and policy and explaining decisions.
- [`Orleans.Lattice.Replication`](../lattice.replication/README.md) - the system-tree replication that converges policy and membership across clusters.
