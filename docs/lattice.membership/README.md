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
- **Pluggable authentication.** A credential is turned into a principal by one or more `ILatticeCredentialAuthenticator`s, selected by the credential's scheme. The package ships an anonymous authenticator and a JWT authenticator; a host can register its own.
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

Unless the mode is `TokenOnly`, token-asserted and claim-projected seed groups are not taken at face value: the merged set is run back through the local directory's transitive closure, so a token that carries only a child group still picks up that group's directory-derived ancestor groups. Under `TokenOnly` the directory is bypassed entirely, so token groups are used exactly as asserted.

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

A host that authenticates its own way registers a custom `ILatticeCredentialAuthenticator`:

```csharp
siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, MyAuthenticator>();
```

## Managing the directory

Groups and their membership edges are managed through `ILatticeMembershipDirectory`, resolved from the silo's service provider. Group membership is transitive: a group can be a member of another group, and a subject's resolved closure includes every group reachable from it. A membership edge references a member by id (a user or nested group id); the directory does not maintain a separate user record - member ids are resolved from the configured identity source (an [identity directory provider](identity-directory-providers.md)) and asserted through credentials.

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

Removal is **non-cascading**: `RemoveGroupAsync` deletes only the group record, not the membership edges that reference the group (as a parent or as a nested member). Remove those edges explicitly with `RemoveMemberAsync` when retiring a group, or an orphaned edge can keep contributing the deleted group id to a subject's closure.

## Concepts

| Concept | Type | Notes |
|---|---|---|
| Group | `MembershipGroup` | Stable `GroupId`, optional display name. |
| Membership edge | `MembershipMemberKind` | An edge is a user-in-group or a group-in-group (nested); the member is referenced by id. |
| Resolved principal | `LatticePrincipal` | The subject id + group closure a credential resolved to. |
| Credential authenticator | `ILatticeCredentialAuthenticator` | Scheme-selected credential to principal mapper. |
| Subject mapper | `ILatticeSubjectMapper` | Maps a principal's claims onto additional groups. |
| Group merge mode | `SubjectGroupMergeMode` | How token-asserted groups combine with directory groups (`Union` by default). |

## Relationship to authorization

Membership produces subjects; [`Orleans.Lattice.Auth`](../lattice.auth/README.md) decides what a subject may do. `AddLatticeAuth(...)` must be called **after** `AddLatticeMembership()` because the authorization layer resolves the caller's subject through this package on every gated operation. Registering membership without authorization gives you identity resolution and a directory but no enforcement.

## See also

- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Identity-directory providers](identity-directory-providers.md) - the provider-agnostic identity source (static, Entra Graph, or custom) that backs subject search and fail-closed validation in the Explorer Access area.
- [Observability](observability.md) - the `orleans.lattice.membership` meter and the subject-resolution cache hit / miss counters.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the policy store, decision engine, and enforcing access gate that consume the subjects this package resolves.
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the transport-agnostic control facade for administering this directory and the policy store.
