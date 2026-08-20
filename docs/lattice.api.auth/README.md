# Orleans.Lattice.Api.Auth

A configuration and control facade for the [Orleans.Lattice](../../README.md) authorization system - administer the membership directory and the authorization policy store, and introspect policy (why a subject is or is not authorized, and what rules are in effect for them), over a single transport-agnostic surface.

## What is it?

`Orleans.Lattice.Api.Auth` is the **control plane** of a lattice cluster's authorization system. The core data plane is reached through .NET grain interfaces; the [`Orleans.Lattice.Auth`](../lattice.auth/README.md) package adds the membership directory, the policy store, and the enforcing access gate. This package adds the administrative surface an operator dashboard, a language-agnostic control tool, or an internal admin service needs to **manage** membership and policy and to **explain** authorization decisions - without embedding the Orleans client.

It is the authorization sibling of the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) and the read-write [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) packages, and is built the same way, in two layers:

- **A transport-agnostic facade.** `ILatticeAuthAdmin` (a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) exposes membership CRUD, policy CRUD, `ExplainAsync`, `EffectivePermissionsAsync`, and identity-directory search and validation over plain request/response records. The facade has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding (the sibling [`Orleans.Lattice.Api.Auth.Grpc`](../lattice.api.auth.grpc/README.md) package).** That binding projects this facade onto a remotely callable service. Its RPC messages carry these facade DTOs, but the gRPC contract wraps most of them in that package's own code-first request/response envelopes (for example a group read travels as an `AuthGroupRef`, a rule write as an `AuthPutRule`), all marshalled with the Orleans binary serializer; the wire types are the contract records, not the facade DTOs verbatim. This package intentionally ships **no** transport of its own; it is the contract every binding adapts over.

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

> `ListRulesForTreeAsync` (MCP tool `lattice_auth_list_rules_for_tree`) returns a tree's own rules **and** the cluster-wide all-trees rules (scope `Tree:*`, authored with `LatticeScope.ClusterWide()`) that govern it. All-trees rules are stored under the reserved `*` tree id and carry `Scope.TreeId == "*"`, so you can tell them apart from exact-tree rules; listing the reserved `*` tree itself returns only its own bucket. When `AllTreesGrantsEnabled` is on (see [All-trees grants](#all-trees-grants) below) these rules actively grant or deny across every application tree. While the flag is off, a *new* data-plane `Tree:*` rule cannot be authored at all - the policy store rejects `PutRuleAsync` with an `ArgumentException` - so any data-plane `Tree:*` rule you see was authored while the tier was enabled and is now inert (listed for visibility only, granting or denying nothing until the tier is re-enabled or the rule is removed). Use `ExplainAsync` / `EffectivePermissionsAsync` for the resolved verdict a subject's access actually receives.

### Policy introspection

| Operation | Facade method | Purpose |
|---|---|---|
| Explain a verdict | `ExplainAsync` | Returns the gate's allow/deny verdict for a subject / operation / scope, plus the authored rules that apply (including cluster-wide `Tree:*` wildcard rules that govern the target tree) - for debugging policy. Pass `subjectKind: LatticeSubjectSelectorKind.Group` to explain the decision for a *group* (evaluated as a member of that group and its ancestors); the default is `User`. |
| Resolve effective permissions | `EffectivePermissionsAsync` | Returns the rules currently in effect for a subject (matched directly or through a group) - for dashboards and UX. Pass `subjectKind: LatticeSubjectSelectorKind.Group` to resolve the rules for a group evaluated as a member of itself and its ancestor groups (its transitive group closure), not the named group alone; the default is `User`. |

### Identity directory and access model

| Operation | Facade method | Purpose |
|---|---|---|
| Search or browse principals | `SearchDirectoryAsync` | Searches the configured identity directory for matching users and groups. |
| Resolve one principal | `ResolveDirectoryPrincipalAsync` | Resolves a single directory principal by id, returning no principal when none exists. |
| Read access-model posture | `GetAccessModelAsync` | Returns the cluster's best-effort access model descriptor, including the live all-trees and access-administration-delegation posture flags. |

### Access administration delegation

By default the only subjects who can administer access (manage groups, membership, and policy rules) are the cluster's **bootstrap administrators** - the statically-configured `LatticeAuthOptions.BootstrapAdministrators` root of trust. "Access administration" is the `Admin` capability on the reserved policy tree `sys-auth-policy` (its id is the public constant `LatticeAuthReservedTrees.PolicyTreeId`); the enforcement gate requires whole-tree `Admin` on that tree to authorize every control-plane call.

You can **delegate** access administration to another user or group by authoring one narrow rule: a whole-tree `Admin` rule on the `sys-auth-policy` tree for that subject. The effect is unconstrained: an `Allow` delegates access administration to the subject, and the store equally permits a `Deny` of the same shape (a whole-tree `Admin` `Deny` on `sys-auth-policy`) to revoke a delegated subject through policy. Bootstrap administrators are the root of trust and are never affected either way. This is off by default and must be enabled per deployment:

- Set `AccessAdministrationDelegationEnabled = true` in the `AddLatticeAuth(options => ...)` configuration. Existing clusters are unchanged until they opt in.
- With the flag on, an existing access administrator (a bootstrap administrator, or a subject who already holds the delegated grant) may author **exactly** the whole-tree `Admin` delegation shape on `sys-auth-policy` - of either effect (an `Allow` to delegate, or a `Deny` to revoke through policy). No other rule shape on the reserved `sys-auth-*` namespace becomes authorable: another reserved tree, a key/prefix scope, or any operation set that is not exactly `Admin` is still rejected fail-closed by the policy store. With the flag off, authoring any rule on the reserved namespace is rejected, exactly as before.
- The subject then satisfies the same administrator check every facade call makes (`PutRuleAsync`, `ListRulesAsync`, membership CRUD, and the rest), because the gate honours the matched `Allow` on the reserved namespace. Bootstrap administrators remain the root of trust and are never affected by policy.

From the Explorer Access tab, the rule form has an **Access administration (delegate)** option: pick the target user or group, tick the option, and save. The form supplies the reserved `sys-auth-policy` tree, whole-tree scope, and `Admin` operation automatically, so you never pick the reserved tree from the catalog. Delegated grants are labelled `access administration` in the ranked rule table so they are easy to tell apart from an ordinary whole-tree rule.

**Security caveat.** A delegated access-administration grant confers full authority over membership and policy, including the ability to delegate further. Grant it sparingly. Turning `AccessAdministrationDelegationEnabled` back off stops **new** delegations from being authored but does **not** revoke a grant that already exists - remove that rule (via `RemoveRuleAsync`, or the Explorer rule list) to revoke the delegation.

### All-trees grants

An **all-trees grant** is a rule whose scope is the cluster-wide sentinel `Tree:*` (authored with `LatticeScope.ClusterWide()`). When enabled, it governs **every** ordinary application tree at once, so an operator can grant (or deny) a subject a capability across the whole cluster without authoring one rule per tree. This reuses the existing `*` sentinel and is off by default:

- Set `AllTreesGrantsEnabled = true` in the `AddLatticeAuth(options => ...)` configuration. Existing clusters are byte-for-byte unchanged until they opt in. While the flag is off, authoring a *new* data-plane `Tree:*` rule is rejected fail-closed - the policy store throws an `ArgumentException` from `PutRuleAsync` rather than silently persisting a rule that does nothing - and a data-plane `Tree:*` rule authored earlier (while the tier was enabled) stays inert, listed for visibility but never granting or denying, until the tier is re-enabled. A pure `Telemetry` wildcard rule is unaffected and remains authorable while the flag is off, because telemetry resolves against the `*` bucket regardless of the tier flag.
- With the flag on, the decision engine consults the `Tree:*` bucket for every non-system tree using a **four-tier precedence** (most authoritative first):
  1. **All-trees deny** - a matched `Tree:*` deny is returned outright; a global deny is never overridden by a specific-tree allow.
  2. **Specific-tree verdict** - the target tree's own most-specific-wins verdict (a specific deny overrides a global allow, and a specific allow stands).
  3. **All-trees allow** - a matched `Tree:*` allow grants access when the target tree has no matching rule of its own.
  4. **Default effect** - otherwise the cluster's `DefaultEffect` applies.
- **System-tree exclusion (fail-closed).** The all-trees tier is **never** consulted for the reserved authorization namespace (`sys-auth-*`, `LatticeAuthReservedTrees.IsReserved`) or for a literal request targeting the sentinel id `*` itself. A `Tree:*` allow therefore can never satisfy a control-plane admin check or leak into access administration.
- **Operation-bit separation is preserved.** A widened data-plane `Tree:*` grant never confers `Telemetry`, and a telemetry `Tree:*` grant never confers a data-plane operation; the operation mask semantics are unchanged.
- A real tree literally named `*` is never creatable: the core lattice grain rejects user-origin creation or mutation of a tree whose id is exactly `*`, so the sentinel can only ever be an authorization scope.

From the Explorer Access tab, the rule form has an **All trees (cluster-wide)** option: pick the target user or group, tick the option, choose the operations and effect, and save. The form supplies the `Tree:*` scope automatically. The help text notes that a data-plane all-trees rule can only be *authored* once an operator has enabled all-trees grants on the silo - the save is rejected otherwise - and that it takes effect immediately while the tier is on; all-trees rules are labelled `all trees` in the ranked rule table so they are easy to tell apart from an ordinary whole-tree rule.

**Security caveat.** An all-trees grant is broad by design - it applies to every application tree, including trees created after the rule was authored. Grant it sparingly, and prefer a specific-tree deny to carve out an exception. Turning `AllTreesGrantsEnabled` back off stops **new** all-trees evaluation but does **not** delete existing `Tree:*` rules - remove the rule to retire it.

## Wire model

Every request / response record is Orleans-serialized with a stable, compact alias (the `oli.` prefix). Group records are the package's own serializable DTOs (`AuthGroup`); rules are surfaced as the durable `LatticeAuthorizationRule` policy model directly, so a binding sees the same rule shape the store persists. Only the **catalog** list endpoints - `ListGroupsAsync`, `ListRulesAsync`, and `ListRulesForTreeAsync` - page with an exclusive continuation-token cursor (`AuthPageRequest` / `Auth*Page`), mirroring the `Orleans.Lattice.Api.State` catalog paging convention. The membership lookups `ListGroupMembersAsync` and `ListSubjectGroupsAsync` are **not** paged: each returns the full `IReadOnlyList<string>` of member (or group) ids in a single call.

## Facade method signatures

All 18 `ILatticeAuthAdmin` methods, exactly as declared in the shared `Orleans.Lattice.Api.Abstractions` package. Every method takes a trailing `CancellationToken cancellationToken = default`.

| Method | Signature |
|---|---|
| `UpsertGroupAsync` | `Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)` |
| `GetGroupAsync` | `Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)` |
| `RemoveGroupAsync` | `Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)` |
| `ListGroupsAsync` | `Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)` |
| `AddMemberAsync` | `Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)` |
| `RemoveMemberAsync` | `Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)` |
| `ListGroupMembersAsync` | `Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)` |
| `ListSubjectGroupsAsync` | `Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)` |
| `PutRuleAsync` | `Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)` |
| `GetRuleAsync` | `Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)` |
| `RemoveRuleAsync` | `Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)` |
| `ListRulesAsync` | `Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)` |
| `ListRulesForTreeAsync` | `Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)` |
| `ExplainAsync` | `Task<AuthExplanation> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)` |
| `EffectivePermissionsAsync` | `Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)` |
| `SearchDirectoryAsync` | `Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)` |
| `ResolveDirectoryPrincipalAsync` | `Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)` |
| `GetAccessModelAsync` | `Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)` |

`ListGroupsAsync`, `ListRulesAsync`, and `ListRulesForTreeAsync` return a page record (`AuthGroupPage` / `AuthRulePage`) carrying the items plus a continuation cursor; `ListGroupMembersAsync` and `ListSubjectGroupsAsync` return the full id list unpaged.

## Result and DTO types

### `AccessModelDescriptor` (returned by `GetAccessModelAsync`)

| Field | Type | Meaning |
|---|---|---|
| `AuthenticationMode` | `AccessAuthenticationMode` | Best-effort in-silo authentication posture (see the enum below). |
| `RulesEnforced` | `bool` | Whether the access gate actually enforces authorization rules. |
| `DirectoryAvailable` | `bool` | Whether an identity directory is configured for validating candidate ids. |
| `DirectoryProviderId` | `string` | The directory provider id (empty when none is configured). |
| `DirectoryExplanation` | `string` | Operator-facing explanation of the directory availability, for create-form guidance. |
| `LocalMembershipEffective` | `bool` | Whether the local membership store is the effective directory. |
| `AllTreesGrantsEnabled` | `bool` | The live all-trees-grants tier flag. |
| `AccessAdministrationDelegationEnabled` | `bool` | The live access-administration-delegation tier flag. |

### `AuthExplanation` (returned by `ExplainAsync`)

| Field | Type | Meaning |
|---|---|---|
| `SubjectId` | `string` | The subject the verdict was resolved for. |
| `GroupIds` | `IReadOnlyList<string>` | The subject's full transitively-expanded group closure, ascending. |
| `Operation` | `LatticeOperation` | The operation the verdict was resolved for. |
| `Scope` | `LatticeScope` | The scope the verdict was resolved for. |
| `Allowed` | `bool` | The gate's verdict (possibly partial - see `Filtered`). |
| `Filtered` | `bool` | `true` when the allow is partial: a per-key filter applies to a tree- or prefix-scoped request. Always `false` for a point (key-scoped) request. |
| `Reason` | `string?` | A human-readable reason, or `null` for a plain unqualified allow. |
| `DefaultEffect` | `LatticeEffect` | The closed-world default effect applied when no rule matches. |
| `MatchedRules` | `IReadOnlyList<LatticeAuthorizationRule>` | The authored rules that apply (advisory; `Allowed` is authoritative). Empty when the verdict rests on `DefaultEffect` or a bootstrap-administrator bypass. |
| `Posture` | `AuthPolicyPosture` | The cluster's opt-in posture (both tier flags), so a caller can tell an in-force all-trees rule from an authored-but-inert one. |

### `AuthEffectivePermissions` (returned by `EffectivePermissionsAsync`)

| Field | Type | Meaning |
|---|---|---|
| `SubjectId` | `string` | The subject the permissions were resolved for. |
| `GroupIds` | `IReadOnlyList<string>` | The subject's transitive group closure, ascending. |
| `Rules` | `IReadOnlyList<LatticeAuthorizationRule>` | The rules currently in effect for the subject (matched directly or through a group), ordered by rule id. |
| `Posture` | `AuthPolicyPosture` | The cluster's opt-in posture (both tier flags). |

### `AuthPolicyPosture`

| Field | Type | Meaning |
|---|---|---|
| `AllTreesGrantsEnabled` | `bool` | The live all-trees-grants tier flag. |
| `AccessAdministrationDelegationEnabled` | `bool` | The live access-administration-delegation tier flag. |

### Identity-directory DTOs

`DirectorySearchRequest` (input to `SearchDirectoryAsync`):

| Field | Type | Meaning |
|---|---|---|
| `Term` | `string` | The typeahead / browse term (defaults to empty for a browse). |
| `Kind` | `DirectoryPrincipalKind?` | Restrict to users or groups, or `null` for both. |
| `PageSize` | `int` | Requested page size. |
| `ContinuationToken` | `string?` | Opaque cursor for the next page, or `null` for the first. |

`DirectorySearchResult` (returned by `SearchDirectoryAsync`):

| Field | Type | Meaning |
|---|---|---|
| `Principals` | `IReadOnlyList<DirectoryPrincipalDescriptor>` | The matching principals. |
| `ContinuationToken` | `string?` | Opaque cursor for the next page, or `null` when exhausted. |
| `Available` | `bool` | `false` (with an empty result) when no directory is configured, rather than erroring. The static `DirectorySearchResult.Unavailable` is this state. |

`DirectoryPrincipalDescriptor` (returned by `ResolveDirectoryPrincipalAsync` and inside a search result):

| Field | Type | Meaning |
|---|---|---|
| `Id` | `string` | The exact principal id. |
| `DisplayName` | `string` | The principal's display name. |
| `Kind` | `DirectoryPrincipalKind` | Whether the principal is a user or a group. |
| `Claims` | `IReadOnlyDictionary<string, string>?` | Optional provider claims, or `null`. |

### `AccessAuthenticationMode` (enum)

Best-effort classification of what the in-silo identity registrations reveal, surfaced on `AccessModelDescriptor.AuthenticationMode`. It is not a claim about every transport in front of the cluster (a transport-terminated scheme is refined by the capability probe layered above this facade).

| Value | Meaning |
|---|---|
| `Unknown` (0) | Posture could not be determined: no credential authenticator (not even the anonymous fallback) is registered. |
| `Anonymous` (1) | Only the anonymous fallback authenticator is registered, so every caller resolves to the anonymous subject and no caller is ever authenticated. |
| `Claims` (2) | At least one real credential authenticator (for example a JWT / claims authenticator) is registered, so the silo can authenticate a caller from its token claims. |
| `Basic` (3) | Reserved for a flat username / password (Basic) scheme. Never reported by this facade - the flat-Basic authorizer lives at the transport layer, out of this facade's view - but reserved so the transport capability probe above can surface it. |

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
