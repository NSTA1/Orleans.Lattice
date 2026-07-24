# Orleans.Lattice.Api.Auth

A configuration and control facade for the [Orleans.Lattice](../../README.md) authorization system - administer the membership directory and the authorization policy store, and introspect policy (why a subject is or is not authorized, and what rules are in effect for them), over a single transport-agnostic surface.

## What is it?

`Orleans.Lattice.Api.Auth` is the **control plane** of a lattice cluster's authorization system. The core data plane is reached through .NET grain interfaces; the [`Orleans.Lattice.Auth`](../lattice.auth/README.md) package adds the membership directory, the policy store, and the enforcing access gate. This package adds the administrative surface an operator dashboard, a language-agnostic control tool, or an internal admin service needs to **manage** membership and policy and to **explain** authorization decisions - without embedding the Orleans client.

It is the authorization sibling of the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and the read-write [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) packages, and is built the same way, in two layers:

- **A transport-agnostic facade.** `ILatticeAuthAdmin` (a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) exposes membership CRUD, policy CRUD, `ExplainAsync`, `EffectivePermissionsAsync`, and identity-directory search and validation over plain request/response records. The facade has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding (the sibling [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md) package).** That binding projects this facade onto a remotely callable service whose messages are the same Orleans-serialized records. This package intentionally ships **no** transport of its own; it is the contract every binding adapts over.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeAuthApi()` on the silo. A cluster that does not add the package has no admin surface, and once added the facade performs **no background work** until a method is called.
- **Administrator-gated, fail-closed.** This is a control plane, so **every** operation - read or write - first authorizes the caller as an administrator through the **same enforcement primitive the in-cluster data path uses**, anchored on the authorization package's bootstrap root-of-trust. A non-administrator or anonymous caller is refused, and nothing is mutated. The facade adds no bespoke, un-authorized write path to the membership or policy trees.
- **Explain parity by construction.** `ExplainAsync` produces its verdict by consulting the **same access gate** the data plane consults, so an explanation can never disagree with the decision that would actually be enforced. The reported matched rules are advisory debugging detail layered on top of that authoritative verdict.
- **Transport-agnostic.** The facade is the contract; a gRPC binding is one adapter. The same records flow to an in-process consumer and a remote one.

## Ordering

`AddLatticeAuthApi()` must be called **after** `AddLatticeAuth(...)`: the authorization registration is the source of truth for the policy store, the membership directory, and the access gate this facade administers and introspects. Calling it first fails fast at registration with an actionable message rather than failing obscurely at silo start.

## Surface (v1)

### Membership administration

| Operation | Facade method |
|---|---|
| Create / replace a group | `UpsertGroupAsync` |
| Read a group | `GetGroupAsync` |
| Remove a group | `RemoveGroupAsync` |
| List groups (paged) | `ListGroupsAsync` |
| Add a membership edge | `AddMemberAsync` |
| Remove a membership edge | `RemoveMemberAsync` |
| List a group's direct members | `ListGroupMembersAsync` |
| List a subject's transitive groups | `ListSubjectGroupsAsync` |

### Policy administration

| Operation | Facade method |
|---|---|
| Create / replace a rule | `PutRuleAsync` |
| Read a rule | `GetRuleAsync` |
| Remove a rule | `RemoveRuleAsync` |
| List all rules (paged) | `ListRulesAsync` |
| List a tree's rules (paged) | `ListRulesForTreeAsync` |

> `ListRulesForTreeAsync` (MCP tool `lattice_auth_list_rules_for_tree`) returns a tree's own rules **and** the cluster-wide wildcard rules (scope `Tree:*`, authored with `LatticeScope.ClusterWide()`) that effectively govern it. Wildcard rules are stored under the reserved `*` tree id and carry `Scope.TreeId == "*"`, so you can tell them apart from exact-tree rules; listing the reserved `*` tree itself returns only its own bucket. Use `ExplainAsync` / `EffectivePermissionsAsync` for the resolved verdict a subject's access actually receives.

### Policy introspection

| Operation | Facade method | Purpose |
|---|---|---|
| Explain a verdict | `ExplainAsync` | Returns the gate's allow/deny verdict for a subject / operation / scope, plus the authored rules that apply (including cluster-wide `Tree:*` wildcard rules that govern the target tree) - for debugging policy. Pass `subjectKind: LatticeSubjectSelectorKind.Group` to explain the decision for a *group* (evaluated as a member of that group and its ancestors); the default is `User`. |
| Resolve effective permissions | `EffectivePermissionsAsync` | Returns the rules currently in effect for a subject (matched directly or through a group) - for dashboards and UX. Pass `subjectKind: LatticeSubjectSelectorKind.Group` to resolve a group's own rules; the default is `User`. |

## Wire model

Every request / response record is Orleans-serialized with a stable, compact alias (the `oli.` prefix). Group records are the package's own serializable DTOs (`AuthGroup`); rules are surfaced as the durable `LatticeAuthorizationRule` policy model directly, so a binding sees the same rule shape the store persists. List endpoints page with an exclusive continuation-token cursor (`AuthPageRequest` / `Auth*Page`), mirroring the `Orleans.Lattice.Api.State` catalog paging convention.

## Explicitly deferred

The following are **not** in v1 and are deliberately left out so a caller cannot mistake their absence for a bug:

- **A condition / attribute predicate language.** Rules carry an opaque, reserved condition string that is not evaluated in this version, matching the policy model the store persists.
- **Bulk import / export of policy or membership.** Administration is per-record; a caller composes bulk workflows over the CRUD surface.

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.

## See also

- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the membership directory, policy store, and enforcing access gate this facade administers.
- [`Orleans.Lattice.Membership`](../lattice.membership/README.md) - the user / group directory backing subject resolution.
- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) - the read-only and read-write data-plane facades this control facade is modelled on.
