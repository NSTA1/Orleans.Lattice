# Orleans.Lattice.Membership

Identity and subject-resolution add-on for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Membership` turns the raw credential a caller presents into the **subject** the authorization layer reasons about. It owns two things:

- **A directory** of groups and their membership edges (with transitive group membership), persisted in an ordinary, dogfooded `ILattice` tree so it is fully introspectable through the standard read / scan / change-feed surface.
- **A credential-to-subject resolution pipeline** that maps an incoming credential (an opaque scheme + token, or an anonymous request) onto a stable subject id plus the flat closure of every group that subject belongs to.

It is the identity foundation the [`Orleans.Lattice.Auth`](../lattice.auth/README.md) package builds its policy and enforcement on. Registering membership alone adds identity resolution and the directory; it does **not** enforce anything on its own. Enforcement arrives only when `Orleans.Lattice.Auth` is also registered.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeMembership()` on the silo. A cluster that does not add the package has no directory and no resolution pipeline, and the core read/write path is byte-for-byte unchanged.
- **Subject = id + group closure.** A resolved subject is a stable subject id and the transitively-expanded set of groups it belongs to, so a group-scoped authorization rule applies to every member without the rule naming them.
- **Pluggable authentication.** A credential is turned into a principal by one or more `ILatticeCredentialAuthenticator`s, tried in registration order: the first whose `CanHandle` recognizes the credential - by its scheme / issuer hint, or by the issuer parsed from the token when no hint is present - resolves it, and a credential no authenticator claims resolves to the anonymous subject. The package ships an anonymous authenticator (which never claims a credential) and a JWT authenticator; a host can register its own.
- **Resolution is cached.** Subject resolution is memoised with a configurable TTL (`ResolutionCacheTtl`, default 5 minutes) so a burst of calls from the same caller does not re-expand its group closure every time. Cache hit and miss rates are exposed as counters on the `orleans.lattice.membership` meter (see [Observability](observability.md)).

## Setup

Register membership on the silo **after** the core lattice. It layers cleanly under the authorization add-on:

```csharp verify
siloBuilder
    .AddLatticeMembership(options =>
    {
        // How token-asserted groups combine with directory groups. Cluster-wide;
        // see "Group merge mode" below for the three modes and their impact.
        options.GroupMergeMode = SubjectGroupMergeMode.Union;

        // How long a resolved subject (id + group closure) is cached.
        options.ResolutionCacheTtl = TimeSpan.FromMinutes(5);
    });
```

### Group merge mode

`GroupMergeMode` decides how the two possible sources of a subject's groups - the
groups a **token asserts** (from a trusted issuer's group claim) and the groups
the **local membership directory** derives (transitively expanded) - are combined
into the closure the authorization layer sees. It is a cluster-wide setting with a
material effect on which rules apply to a caller.

| Mode | Resolved groups | Effect |
|---|---|---|
| `Union` (default) | Token-asserted **and** directory-derived groups | Both sources count. Adding a directory group or a token group each just works; nothing is silently dropped. |
| `TokenOnly` | Token-asserted groups only | The local directory is **ignored for group membership**. The IdP is the sole authority; local group edits (including in the Explorer Access area) are inert. |
| `DirectoryOnly` | Directory-derived groups only | Token-asserted groups are **ignored**. The local directory is the sole authority; the IdP's group claims are not trusted for membership. |

Unless the mode is `TokenOnly`, token-asserted and claim-projected seed groups are not taken at face value: the merged set is run back through the local directory's transitive closure, so a token that carries only a child group still picks up that group's directory-derived ancestor groups. Under `TokenOnly` the directory is bypassed entirely, so token groups are used exactly as asserted. In every mode, groups projected from the principal's claims by `LatticeMembershipOptions.ClaimToGroups` (when configured) are added on top of the mode's source - including under `DirectoryOnly`, where the projection still reads the token's claims.

**Choosing a mode.** Use `Union` when either source may legitimately contribute
groups. Use `TokenOnly` when the IdP is authoritative and the local directory is
only a display-name registry. Use `DirectoryOnly` when you curate membership
locally and do not want to trust the IdP's group claims.

**On the default.** `Union` is the least-surprising, additive choice, and its
breadth is bounded downstream: a wider group closure only elevates privilege if a
policy grants that group something, and [`Orleans.Lattice.Auth`](../lattice.auth/README.md)
is deny-by-default - `Union` widens *membership*, not *grants*. It is nonetheless
the most permissive composition (it trusts token-asserted groups on top of the
curated directory), so pick `TokenOnly` or `DirectoryOnly` when you want a single
authoritative source.

### Registering an authenticator

A JWT authenticator is registered per trusted issuer:

```csharp verify
siloBuilder.AddLatticeJwtAuthenticator(options =>
{
    options.Issuer = "https://issuer.example.com";
    options.Audiences.Add("orleans-lattice");
    options.SubjectClaimTypes.Add("sub");
    options.GroupClaimTypes.Add("groups");
});
```

The JWT authenticator reads a credential's token issuer only when the credential carries no scheme. The credential bridges in the gRPC and MCP facade bindings stamp one (`Bearer` by default), so an authenticator that must serve those calls also sets `SchemeHint` to that scheme; see [`JwtAuthenticatorOptions`](configuration.md#jwtauthenticatoroptions).

A host that authenticates its own way registers a custom `ILatticeCredentialAuthenticator`:

```csharp
siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, MyAuthenticator>();
```

## Managing the directory

Groups and their membership edges are managed through `ILatticeMembershipDirectory`, resolved from the silo's service provider. Group membership is transitive: a group can be a member of another group, and a subject's resolved closure includes every group reachable from it. A membership edge references a member by id (a user or nested group id); the directory does not maintain a separate user record - a member id is a plain subject id, asserted through credentials and, when directory validation is enabled, checked against the configured identity source (an [identity directory provider](identity-directory-providers.md)) by the `ILatticeAuthAdmin` administration facade before it writes.

```csharp verify
public sealed class DirectorySeeder(ILatticeMembershipDirectory directory)
{
    public async Task SeedAsync(CancellationToken cancellationToken)
    {
        // Create a group.
        await directory.UpsertGroupAsync(new MembershipGroup("editors", "Editors"), cancellationToken);

        // Add members by id. A member can itself be a group (nested membership).
        await directory.AddMemberAsync("editors", "alice", MembershipMemberKind.User, cancellationToken);
        await directory.AddMemberAsync("editors", "bob", MembershipMemberKind.User, cancellationToken);

        // Read a subject's transitive group closure.
        IReadOnlyCollection<string> groups = await directory.GroupsOfAsync("alice", cancellationToken);
    }
}
```

The directory is a trusted, silo-side seam: it runs its reads and writes under system origin, bypassing the access gate. Operator and remote administration goes through the `ILatticeAuthAdmin` facade that [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) registers, which authorizes the caller before it touches the directory.

| Member | Returns | Purpose |
|---|---|---|
| `UpsertGroupAsync(MembershipGroup group, CancellationToken)` | `Task` | Creates or replaces a group record. |
| `GetGroupAsync(string groupId, CancellationToken)` | `Task<MembershipGroup?>` | Reads a group record, or `null` when no such group exists. |
| `ListGroupsAsync(CancellationToken)` | `IAsyncEnumerable<MembershipGroup>` | Enumerates every group record in id order. |
| `RemoveGroupAsync(string groupId, CancellationToken)` | `Task` | Removes a group record; a no-op when it does not exist. Leaves the group's membership edges in place (see below). |
| `AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind, CancellationToken)` | `Task` | Makes `memberId` a direct member of `groupId`; `memberKind` defaults to `User`. Idempotent. |
| `RemoveMemberAsync(string groupId, string memberId, CancellationToken)` | `Task` | Removes a membership edge; a no-op when it does not exist. |
| `GroupsOfAsync(string memberId, CancellationToken)` | `Task<IReadOnlyCollection<string>>` | The member's full transitive group closure (nested groups walked with cycle detection), excluding the member itself. |
| `ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken)` | `Task<IReadOnlyCollection<string>>` | The transitive closure of a set of seed groups, including the seeds; a seed the directory does not know contributes only itself. |
| `MembersOfAsync(string groupId, CancellationToken)` | `Task<IReadOnlyCollection<string>>` | The group's direct members (users and nested groups). |

Every `CancellationToken` parameter defaults to `default`.

Removal is **non-cascading**: `RemoveGroupAsync` deletes only the group record, not the membership edges that reference the group (as a parent or as a nested member). Remove those edges explicitly with `RemoveMemberAsync` when retiring a group, or an orphaned edge can keep contributing the deleted group id to a subject's closure.

## Concepts

| Concept | Type | Notes |
|---|---|---|
| Group | `MembershipGroup` | Stable `GroupId`, optional display name. |
| Membership edge | `MembershipMemberKind` | An edge is a user-in-group or a group-in-group (nested); the member is referenced by id. |
| Membership directory | `ILatticeMembershipDirectory` | The group and edge store that subject resolution reads (see [Managing the directory](#managing-the-directory)). |
| Authenticated principal | `LatticePrincipal` | What an authenticator produces from a validated credential: the subject id, issuer, claim bag, token-asserted groups, and token expiry - before it is merged with the directory. |
| Resolved subject | `LatticeSubject` (core) | The final subject id, transitive group closure, and claim bag the authorization layer evaluates. |
| Credential authenticator | `ILatticeCredentialAuthenticator` | Recognizes a credential (`CanHandle`) and validates it into a `LatticePrincipal` (`AuthenticateAsync`). |
| Built-in authenticators | `JwtCredentialAuthenticator`, `AnonymousCredentialAuthenticator` | The per-issuer JWT authenticator - the extensible base the Entra and OIDC authenticators specialize - and the fallback that never claims a credential. |
| Subject mapper | `ILatticeSubjectMapper` | Merges a principal with its directory-derived groups into the final `LatticeSubject`; the default mapper applies `GroupMergeMode` and the optional `ClaimToGroups` projection. |
| Group merge mode | `SubjectGroupMergeMode` | How token-asserted groups combine with directory groups (`Union` by default). |
| Identity-directory provider | `ILatticeIdentityDirectory` | The read-only search / validate view onto the external identity source (see [Identity-directory providers](identity-directory-providers.md)). |

## Relationship to authorization

Membership produces subjects; [`Orleans.Lattice.Auth`](../lattice.auth/README.md) decides what a subject may do. `AddLatticeAuth(...)` must be called **after** `AddLatticeMembership()` because the authorization layer resolves the caller's subject through this package on every gated operation. Registering membership without authorization gives you identity resolution and a directory but no enforcement.

## See also

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Identity-directory providers](identity-directory-providers.md) - the provider-agnostic identity source (static, Entra Graph, or custom) that backs subject search and fail-closed validation in the Explorer Access area.
- [Observability](observability.md) - the `orleans.lattice.membership` meter and the subject-resolution cache hit / miss counters.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the policy store, decision engine, and enforcing access gate that consume the subjects this package resolves.
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the transport-agnostic control facade for administering this directory and the policy store.
