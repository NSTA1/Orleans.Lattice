# Orleans.Lattice.Membership

Identity and subject-resolution add-on for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Membership` turns the raw credential a caller presents into the **subject** the authorization layer reasons about. It owns two things:

- **A directory** of users and groups (with transitive group membership), persisted in an ordinary, dogfooded `ILattice` tree so it is fully introspectable through the standard read / scan / change-feed surface.
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
        // How token-asserted groups combine with directory groups.
        options.GroupMergeMode = SubjectGroupMergeMode.Union;

        // How long a resolved subject (id + group closure) is cached.
        options.ResolutionCacheTtl = TimeSpan.FromMinutes(5);
    });
```

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

Users and groups are managed through `ILatticeMembershipDirectory`, resolved from the silo's service provider. Group membership is transitive: a group can be a member of another group, and a subject's resolved closure includes every group reachable from it.

```csharp verify
public sealed class DirectorySeeder(ILatticeMembershipDirectory directory)
{
    public async Task SeedAsync(CancellationToken cancellationToken)
    {
        // Create users and a group.
        await directory.UpsertUserAsync(new MembershipUser("alice", "Alice"), cancellationToken);
        await directory.UpsertUserAsync(new MembershipUser("bob", "Bob"), cancellationToken);
        await directory.UpsertGroupAsync(new MembershipGroup("editors", "Editors"), cancellationToken);

        // Add members. A member can itself be a group (nested membership).
        await directory.AddMemberAsync("editors", "alice", MembershipMemberKind.User, cancellationToken);
        await directory.AddMemberAsync("editors", "bob", MembershipMemberKind.User, cancellationToken);

        // Read a subject's transitive group closure.
        IReadOnlyCollection<string> groups = await directory.GroupsOfAsync("alice", cancellationToken);
    }
}
```

## Concepts

| Concept | Type | Notes |
|---|---|---|
| User | `MembershipUser` | Stable `UserId`, optional display name and claim bag. |
| Group | `MembershipGroup` | Stable `GroupId`, optional display name. |
| Membership edge | `MembershipMemberKind` | An edge is a user-in-group or a group-in-group (nested). |
| Resolved principal | `LatticePrincipal` | The subject id + group closure a credential resolved to. |
| Credential authenticator | `ILatticeCredentialAuthenticator` | Scheme-selected credential to principal mapper. |
| Subject mapper | `ILatticeSubjectMapper` | Maps a principal's claims onto additional groups. |
| Group merge mode | `SubjectGroupMergeMode` | How token-asserted groups combine with directory groups (`Union` by default). |

## Relationship to authorization

Membership produces subjects; [`Orleans.Lattice.Auth`](../lattice.auth/README.md) decides what a subject may do. `AddLatticeAuth(...)` must be called **after** `AddLatticeMembership()` because the authorization layer resolves the caller's subject through this package on every gated operation. Registering membership without authorization gives you identity resolution and a directory but no enforcement.

## See also

- [Observability](observability.md) - the `orleans.lattice.membership` meter and the subject-resolution cache hit / miss counters.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the policy store, decision engine, and enforcing access gate that consume the subjects this package resolves.
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) - the transport-agnostic control facade for administering this directory and the policy store.

Feature planning is managed on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files.
