# System-Tree Replication (membership and auth)

`Orleans.Lattice.Membership` and `Orleans.Lattice.Auth` dogfood ordinary `ILattice`
trees for their identity directory and authorization policy store. In a
single-cluster deployment those trees stay local. In a multi-cluster deployment you
usually want a **single converged identity and authorization surface** across every
site: a group or membership edge created on site A should exist on site B, and a policy revoke authored
on site A should eventually be enforced on site B.

`ReplicateLatticeSystemTrees` is the first-class, explicit, gated way to opt those
reserved trees into `Orleans.Lattice.Replication`. This page covers what gets
enrolled, the per-tree merge mode, the security-critical apply bypass, the
divergence-window semantics of the default eventual mode, and the optional strict
policy-epoch fence that closes the window for chosen trees.

## What gets enrolled

The reserved tree ids and their merge modes are the public contract on
`LatticeSystemTreeNames`:

| Tree id | Constant | Merge mode | Enrolled |
|---|---|---|---|
| `sys-membership-groups` | `LatticeSystemTreeNames.MembershipGroups` | LWW-Register | always |
| `sys-membership-edges` | `LatticeSystemTreeNames.MembershipEdges` | LWW-Register | always |
| `sys-auth-policy` | `LatticeSystemTreeNames.AuthPolicy` | LWW-Register | always |
| `sys-auth-audit` | `LatticeSystemTreeNames.AuthAudit` | OR-Set | opt-in only |

Membership and policy trees replicate **last-writer-wins**: each group, edge,
and rule is an independent key whose latest HLC-stamped write wins on convergence,
which is the correct model for a mutable identity/authorization record.

These trees are the deliberate exception to the "system state stays cluster-local"
rule that keeps the core registry tree from replicating. They are addressed by
ordinary tree ids (they do **not** use the core reserved `_lattice_` tree-id prefix),
so the receiver apply seam never rejects them as system trees.

### Why the audit tree is opt-in (off by default)

The auth package derives its policy-change history as a **materialised view** over
the replicated policy tree, not as an independently written tree. Once the policy
tree converges on a site, that site rebuilds exactly the same history locally, so
shipping the audit rows as well is redundant. The audit tree is therefore off by
default. A deployment that dogfoods a genuinely distinct cross-site audit tree can
opt it in explicitly with `includeAudit: true`; it replicates as an OR-Set so
concurrently appended rows on different sites all survive the merge instead of
overwriting one another.

## Enrolling the trees

Register `AddLatticeReplication` first, then call `ReplicateLatticeSystemTrees`. The
call merges the reserved ids into `LatticeReplicationOptions.ReplicatedTrees` via a
`PostConfigure`, so it wins regardless of the order in which the host configures its
own replicated-trees map; any host-declared tree is preserved.

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeReplication(opts =>
        {
            opts.ClusterId = "site-a";
            opts.ReplicationPeers = new[] { "site-b" };
        })
        // Enrol the reserved membership + auth policy trees. Pass
        // includeAudit: true to also replicate the append-only audit tree.
        .ReplicateLatticeSystemTrees();
});
```

### Guardrail

Enrolling these trees requires `Orleans.Lattice.Replication` to be registered first:
the receiver apply seam and the merge-mode resolver that make replication actually
run come from `AddLatticeReplication`. Calling `ReplicateLatticeSystemTrees` before
that add-on throws `InvalidOperationException` with an actionable message rather than
silently declaring trees that never ship.

## System-origin apply bypass (security-critical)

Writes to `sys-auth-policy` and the membership trees are normally gated: a user with
no policy grant cannot write a rule. Replicated writes arriving from a peer cluster
have **no user identity** - the "caller" is the replication apply pipeline, not a
subject. If those applies were authorized as user writes they would be denied, and a
replicated revoke would never land.

The receiver apply path therefore runs under the **system-origin scope**. Both the
per-entry (`ApplyAsync`) and batch (`ApplyBatchAsync`) apply paths wrap their
whole body in `LatticeAccessGateContext.EnterSystemOrigin()`. That flag rides the
`RequestContext` to every outgoing grain call the applier makes, and the core access
gate short-circuits to allow before it ever consults the auth engine when the ambient
scope is system-origin or gate-bypassed. The result: a replicated policy write lands
even though the caller has no user identity, while a genuine user write to the same
tree is still fully gated.

This bypass covers every apply sub-path uniformly. The last-writer-wins apply seam
writes below the gate already; the wrap additionally covers the CRDT-delta and
prepared-write sub-paths, which route through gated public grain methods, so the
system-origin invariant holds no matter which merge mode a tree uses.

The bypass applies **only** to replication-applied writes. It never widens the gate
for ordinary traffic: user writes on the receiving cluster continue through the full
authorization path.

## Divergence window (eventual mode, the default)

Enrolment gives **eventual** cross-cluster consistency. A policy or membership edit
made on one site becomes visible on another only after:

1. the mutation is captured and shipped to the peer, and
2. the peer applies it, and
3. the peer's compiled-policy snapshot maintainer rebuilds off the change feed so the
   new rule participates in decisions.

During that window a revoke authored on site A is not yet enforced on site B: a user
may still perform on B an operation that A's newer policy forbids. This is the
last-writer-wins convergence contract. It is the right default for most deployments -
it adds **zero** cost to the write path and converges without cross-cluster
coordination - but it does mean the revoke is not globally instantaneous.

## Strict policy-epoch fence (optional, off by default)

A deployment that must close the revoke window for specific trees can layer the auth
package's **strict policy-epoch fence** on top. It is off by default and adds no cost
when off.

The fence works off the monotonic policy epoch that the compiled-policy maintainer
bumps on every policy change. A caller that has just observed epoch N on one site can
require that the site serving a write has compiled at least epoch N before the write
is allowed:

- Opt a tree into strict mode with `LatticeAuthOptions.StrictConsistencyTrees`.
- On the write path, establish an epoch floor with
  `LatticePolicyEpochFenceContext.RequireAtLeast(epoch)` (it flows on the ambient
  request scope).
- A **user** write to a strict-configured tree is rejected when the local compiled
  epoch is older than the required floor - the revoke has not yet been observed
  locally, so the write is fenced rather than allowed under stale policy.

```csharp
// Auth-side configuration (Orleans.Lattice.Auth): opt a tree into strict mode.
authOptions.StrictConsistencyTrees = new HashSet<string>(StringComparer.Ordinal)
{
    "billing-ledger",
};

// On a user write that must not run under stale policy, require an epoch floor:
using (LatticePolicyEpochFenceContext.RequireAtLeast(observedPolicyEpoch))
{
    await lattice.SetAsync(treeId, key, value);
}
```

The fence covers **only user writes** to a strict-configured tree. It never fences:

- reads (only the write mask is checked),
- system-origin or replication-applied writes (they short-circuit the gate before the
  fence is ever consulted, so a replicated policy write always lands),
- the break-glass bootstrap admin (checked before the fence), or
- writes to any tree not listed in `StrictConsistencyTrees`.

When `StrictConsistencyTrees` is null or empty the fence early-outs with a single
null/count check, so the eventual path is byte-for-byte unchanged and free.

## See also

- [Replication Modes](replication-modes.md) - the per-tree merge-mode model.
- [Replication Apply](replication-apply.md) - the receiver apply pipeline the
  system-origin bypass wraps.
- [Configuration](configuration.md) - `LatticeReplicationOptions` surface.
