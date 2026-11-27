# Authorization decision layering

`AuthDecision` checks the authorization decision as one function over a rule
store, a caller, an operation and a scope. It is the base the other
authorization modules of epic #6046 refine or assume: they model authorization
as the abstract `Allowed(caller, op, scope)` this module publishes (see
[The `Allowed` contract](#the-allowed-contract)). It is a design check over a
bounded instance, not a machine-checked refinement proof of the C# code; the
[refinement note](Refinement.md) states which production seam each action and
property stands for and which tests detect a departure from it.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `AuthDecision` | 9 | 0 | 10 | 18 | 17 | 61,747 |

## What it models

- **The rule store and its writers.** The operator surface writes and removes
  only operator-layer rules (every id outside `tenant:`). The tenant policy
  facade writes only rules confined to its own tenant's data trees or
  tenant-wide scope and the data-plane operation mask, and removes only its own
  tenant's rules. A system-origin write (a restore or a replication apply)
  bypasses that confinement, so the rule set can hold an escaped tenant rule:
  one on another tenant's tree, on an app-owned tree, or carrying `Telemetry`.
- **The compiled snapshot.** Operator rules, and a tenant partition that drops
  every tenant rule which does not conform, as `CompiledTenantPartition.Build`
  does.
- **Evaluation.** Most specific scope wins (key over prefix over tree), with
  deny overriding allow at an equal tier. The all-trees (`Tree:*`) tier applies
  to data trees only. A matched operator verdict is final. Only when no
  operator rule matches does the tenant layer run, and only on a tenant data
  tree `t/{tenant}/{name}` and with that tree owner's rules (a tenant-wide deny,
  then the tree's own verdict, then a tenant-wide allow). Otherwise the
  permissive default effect applies.
- **The gate.** A request on a control-plane id (the reserved policy tree,
  `LatticeScope.ClusterWideTreeId`, or a tenant capability scope) is allowed
  only on an explicit matched allow, whatever the default effect; a whole-scope
  request over per-key rules is a key-filtered decision, which a control-plane
  request refuses. Bootstrap administrators are allowed before the engine runs,
  and an anonymous caller is never one, even when the configured set names it.
- **Ranges.** A range filter composes the layers per key on operator
  **coverage**, not on the operator rule's effect.
- **Batches.** A cross-tree batch is authorized only when every (tree, key)
  pair is, before any intent is placed.
- **MCP.** The tool gate with no registered authorizer denies.

The capability requests are the ones the authorizers issue: `Telemetry` and
`AppInstall` over the cluster-wide id, and `Telemetry` over a tenant's
capability scope (#5582).

## Bounds

Two tenants; one subject (`u`), the anonymous caller and one bootstrap
administrator; two keys and one prefix covering one of them; a universe of
sixteen candidate rules, of which at most three are held at once; four data
trees (a default tree, two tenant trees and one app-owned tree) and four
control-plane scopes. The default effect is `Allow`, the one fail-closed must
never inherit. Every decision is taken from an idle recorder (`Settle` returns
it there), so the state space is the rule sets times the decisions over each.

## The `Allowed` contract

F2a (#6049), F3 (#6050), F4 (#6051) and F5 (#6052) model authorization as
`Allowed(caller, op, scope)` and give it a small concrete definition in their own
cfg. Its vocabulary is this module's: tree records `[kind, tenant]` with data
kinds `default`, `tenant`, `appOwned` and `system` and control kinds
`reserved`, `clusterWide` (`ClusterWideTreeId`) and `tenantCapability`
(`TenantCapability(T)`); the operations `Read`, `Write`, `Admin`, `Telemetry` and
`AppInstall`; and `Scope(tree, key)` with `NoKey` for a whole-scope request,
where `TRUE` means an unfiltered allow. `Allowed` is the gate decision before
tenant isolation composes with it, which can only turn an allow into a deny.

A consumer's definition must satisfy, for a caller that is not a bootstrap
administrator:

| Contract property | Checked here as | Statement |
| --- | --- | --- |
| C1 OperatorFinal | `OperatorFinal` | A matched operator verdict decides. |
| C2 TenantLayerConfined | `TenantLayerConfined` | A tenant rule affects a decision only when no operator rule matches, on a `tenant` tree, of the tree owner, for a data-plane operation or `Admin`. |
| C3 ScopelessFromClusterWideOnly | `ScopelessFromClusterWideOnly` | `AppInstall` only from a tree-scope allow on `ClusterWideTreeId`; `Telemetry` from that or from a platform tree-scope allow on `TenantCapability(T)`, which never covers another tenant, the platform series or the cluster-wide id. Never from the tenant layer. |
| C4 FailClosed | `FailClosed` | An unmatched control-plane request is denied whatever the default effect; `Tree:*` never reaches a control kind; a filtered allow where a whole-scope allow is required is denied; a missing authorizer denies. |
| C5 Bootstrap root of trust | `BootstrapRootOfTrust` | Bootstrap administrators are allowed everything, and an anonymous caller is never one. |
| C6 BatchAllOrNothingAuthorization | `BatchAllOrNothingAuthorization` | A batch is allowed only when every pair is, decided before any intent. |

The full statement, with the TLA+ vocabulary and a minimal grant-set definition,
is on [#6046](https://github.com/NSTA1/Orleans.Lattice/issues/6046#issuecomment-7216495902)
as contract version 1. A change to a property statement is posted there as a new
contract version.

## Refinement and mutations

- [Refinement note](Refinement.md)

The adjacent `mutations/` catalogue uses exact anchored edits. Every property
has a falsifying mutation, and every behavioural action is perturbed by one
(`Settle` is the one non-behavioural action).
