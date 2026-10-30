---
agent_spec: "docs/agents/governance/authorization.json"
---

# Architecture

`Orleans.Lattice.Auth` turns the core library's access-gate seam into an enforcing control point. It stores rules in a dogfooded lattice tree, compiles them into an in-memory snapshot on every silo, and answers each gated request from that snapshot without storage I/O. This page describes the pipeline by behaviour; the public types are listed in the [Public API](api.md).

## Registration

`AddLatticeAuth` refuses to run unless `AddLattice(...)` and `AddLatticeMembership(...)` have already been called on the same builder: the policy store needs the core tree registry, and enforcement resolves callers through the membership pipeline. A repeated call layers its options delegate and skips the structural wiring.

The registration installs, in one pass:

- the options and their validator, and the view infrastructure so the policy tree gets a durable per-key history view;
- the `ILatticeAuthorizationPolicyStore` and the once-per-silo history bootstrap;
- the compiled-snapshot maintainer and the cross-silo policy-epoch synchronizer, both subscribed to the core `IMutationObserver` change feed;
- the `ILatticeDecisionEngine`;
- the decision observer, a logger audit sink and the (off by default) durable audit-trail sink as `ILatticeAuthAuditSink` implementations;
- inactive defaults for `ITenantGateEnforcer` and `ITenantRuleLayer`, registered with `TryAdd` so `Orleans.Lattice.Tenancy` can replace them;
- the enforcing gate, which replaces the core allow-all `ILatticeAccessGate`;
- a silo-wide incoming grain-call filter that strips forged internal capability markers from external calls (see [Trust boundary for internal grain calls](security-posture.md#trust-boundary-for-internal-grain-calls));
- a hosted service that logs the authorization posture once at start-up.

Without this registration the core keeps its allow-all gate and none of the above runs ([Zero cost when disabled](README.md#zero-cost-when-disabled)).

## Policy storage

The policy store keeps every rule in the reserved `sys-auth-policy` tree, one JSON value per rule under a key composed of the governed tree id and the rule id, so a tree's rules form one contiguous range and the whole policy is a full-tree scan. The store is infrastructure: it reads and writes under system origin, so the very first rule can be written under default-deny and the snapshot rebuild never re-enters the gate. Deciding who may edit policy belongs to the layer above it - a bootstrap administrator or the [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/architecture.md) facade.

Before anything is read or written, a mutation is checked in this order of concern:

1. The rule is well formed (ids present, enum values defined, key or prefix present exactly when the scope kind needs one).
2. An `app:` or `tenant:` rule id is accepted only from a caller already under system origin.
3. Tenant-tier confinement holds for every caller: a tenant rule stays within its tenant's trees, a tenant-wide scope appears only on a tenant-tier rule, and a rule naming a tenant group stays on that tenant's trees ([Confinement guards](tenant-layer.md#confinement-guards)).
4. A rule on the reserved `sys-auth-*` namespace is accepted only in the whole-tree `Admin` delegation shape on `sys-auth-policy` and only with `AccessAdministrationDelegationEnabled`; a data-plane `Tree:*` rule only with `AllTreesGrantsEnabled` ([Configuration](configuration.md)).

A removal runs only the rule-id guard of step 2. Before its first write or delete, the store runs the once-per-silo history bootstrap: it applies `HistoryRetentionMode` and `HistoryRetentionWindow` to the policy tree and, when `EnableDurableHistoryView` is set, creates the history view over it.

## The compiled snapshot

Each silo holds one immutable compiled snapshot of the whole rule set and a monotonic epoch. The snapshot maintainer builds it on first use and rebuilds it when it observes a `sys-auth-policy` mutation. The change-feed callback only schedules the rebuild; the rescan runs in the background, rebuilds are coalesced (at most one in flight and one queued) and serialized, and the new snapshot is swapped in atomically. The snapshot drops any malformed rule that reached the policy tree by restore or replication, so it never grants or denies.

The change feed fires only on the silo hosting the leaf that committed a write, so the epoch synchronizer carries the change to every other silo. On a write it observed it advances a cluster-wide policy-epoch grain, retrying until the advance lands. Every silo reads that epoch and the cluster membership version about once a second and rebuilds when the epoch advanced, the epoch grain restarted, or membership changed; the values are recorded only after a successful rebuild, so a failed rebuild is retried on the next read. See [Consistency modes](README.md#consistency-modes) for the resulting propagation bound.

## The enforcing gate

The core resolves the caller's subject through the membership context and hands it to the gate on the `LatticeAccessRequest`, so the gate performs no subject resolution and no storage I/O. System-origin and replication-applied writes are short-circuited by the core before the gate is consulted. For every other request the gate decides in this order:

1. **Bootstrap administrators.** A non-anonymous subject in `BootstrapAdministrators` is allowed outright, before the snapshot is consulted, so a policy mistake cannot lock operators out.
2. **Control plane.** A request on the reserved `sys-auth-*` namespace, the `sys-tenant-*`, `sys-app-*`, `sys-membership-*`, `sys-schema-*` and `sys-backup-*` system-data trees, the tenant-administration capability namespace, or the `*` cluster-wide sentinel is decided by explicit matched allow only; `DefaultEffect` never applies there.
3. **Strict epoch fence.** When `StrictConsistencyTrees` lists the target tree, a write whose caller stamped a floor with `LatticePolicyEpochFenceContext.RequireAtLeast` is denied while the local epoch is below it. Reads, unlisted trees and writes with no floor skip the fence.
4. **Warm-up.** On a silo whose snapshot has not been built, or whose empty snapshot has just received its first rule, the request waits for the rebuild instead of being answered from an empty snapshot.
5. **Policy evaluation.** The decision engine evaluates the compiled snapshot synchronously: most-specific scope, deny-overrides within a tier, the user-over-group tie-break, the opt-in all-trees tier, and, when `ITenantRuleLayer.IsActive`, the tenant layer beneath the operator layer ([Precedence](README.md#precedence), [The tenant rule layer](tenant-layer.md)).
6. **Tenant isolation.** Only an allowed request is passed to `ITenantGateEnforcer`, and a deny from either side denies. With the inactive default this is one `bool` read.

On the warm steady state every step completes synchronously, so the hot path allocates no asynchronous state machine.

## Observation and audit

After every decision the gate hands the request, decision and (when auditing is on) the deciding rule to the decision observer, which cannot change the outcome. It records the `orleans.lattice.auth` instruments when a listener is attached and, when `EnableAuditSink` is set, builds a `LatticeAuthDecisionEvent` and dispatches it to every registered `ILatticeAuthAuditSink` subject to `AuditVerbosity` and `AuditSamplingRatio`. Sink failures never reach the gate. The logger sink writes denials at warning level and allows at debug level; the durable trail appends each event to the reserved `sys-auth-audit` tree under system origin when `EnableDurableAuditTrail` is set. An administrator's explain evaluation of another subject is neither counted nor audited in that subject's name. See [Observability](observability.md).

## Seams

| Seam | Owner | Role |
|---|---|---|
| `ILatticeAccessGate` | Core | Replaced by the enforcing gate. |
| `ILatticeMembershipContext` | Core, implemented by [`Orleans.Lattice.Membership`](../lattice.membership/architecture.md) | Supplies the subject on each request. |
| `IMutationObserver` | Core | Drives snapshot rebuilds and epoch publication. |
| `ITenantGateEnforcer`, `ITenantRuleLayer` | This package, implemented by `Orleans.Lattice.Tenancy` | Tenant isolation and the tenant rule layer; inactive by default. |
| `ILatticeAuthAuditSink` | This package | Additional audit destinations. |

## Source map

- [Registration](../../src/lattice.auth/LatticeAuthServiceCollectionExtensions.cs)
- [Policy store](../../src/lattice.auth/LatticeAuthorizationPolicyStore.cs)
- [Snapshot maintainer](../../src/lattice.auth/CompiledPolicySnapshotMaintainer.cs)
- [Epoch synchronizer](../../src/lattice.auth/AuthPolicyEpochSynchronizer.cs)
- [Enforcing gate](../../src/lattice.auth/PolicyAccessGate.cs)
- [Policy evaluation](../../src/lattice.auth/PolicyEvaluator.cs)
- [Decision observer](../../src/lattice.auth/LatticeAuthDecisionObserver.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Security posture](security-posture.md)
- [`Orleans.Lattice.Api.Auth` architecture](../lattice.api.auth/architecture.md)
