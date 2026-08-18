# Identity-directory providers

`Orleans.Lattice.Membership` separates two directories that are easy to confuse:

- The **membership directory** (`ILatticeMembershipDirectory`) is the roster the
  cluster *owns* - the groups and nested-membership edges it persists in a
  dogfooded `ILattice` tree and resolves subjects against. A membership edge names
  its member by id (a user or nested group id); the cluster does not persist a
  separate per-user record, so a member id is a plain subject id that is asserted
  through a credential or validated against an identity-directory provider. See
  [the package README](README.md#managing-the-directory).
- The **identity-directory provider** (`ILatticeIdentityDirectory`) is a
  read-only view onto the *external* identity source the deployment trusts - a
  static roster, an Entra tenant, or your own system. It exists to **search** and
  **validate** principal ids before an operator references them from the membership
  directory (as a group, or as a group's member), so the Explorer Access area can
  offer a real typeahead and fail closed on unknown ids. A principal returned by
  this provider - often called a "directory user" - is a searchable identity, not a
  credential: it never grants access on its own.

This document covers the identity-directory provider seam: the interface, the
three built-in providers, and how to write your own.

## The provider seam

A provider implements a small, read-only contract - `ILatticeIdentityDirectory`:

The members are:

| Member | Purpose |
|---|---|
| `ProviderId` | A stable, short id for the active provider (`"null"`, `"static"`, `"entra"`, or your own). Surfaced to the Explorer so it can label the source. |
| `DescribeEntry` | One human-readable sentence describing what a *valid* id looks like for this source, scoped to the `DirectoryPrincipalKind?` a create form is entering (`User`, `Group`, or `null` for a combined form). The Explorer shows it under the create form so an operator knows what to type. |
| `SearchAsync` | Returns a `DirectorySearchPage` of `DirectoryPrincipal`s matching a term, optionally filtered by `DirectoryPrincipalKind`, with an opaque continuation token for paging. |
| `ResolveAsync` | Looks up a single principal id and returns its `DirectoryPrincipal`, or `null` when the source has no such principal. This is the fail-closed validation call. |

`DirectorySearchQuery` is a `readonly record struct` (`Term`, optional `Kind`,
`PageSize`, `ContinuationToken`); an empty `Term` requests an unfiltered browse of
the first page. `DirectorySearchPage.Empty` is a shared no-allocation page for a
source that has nothing to return.

### Global provider options

`LatticeIdentityDirectoryOptions` bounds every provider uniformly:

```csharp verify
siloBuilder.ConfigureLatticeMembership(_ => { });
siloBuilder.Services.Configure<LatticeIdentityDirectoryOptions>(options =>
{
    // Page size used when a query does not request one.
    options.DefaultPageSize = 25;

    // Hard ceiling a provider must not exceed for a single page.
    options.MaxPageSize = 100;

    // When true, creating a principal the provider cannot resolve is blocked
    // (fail closed). When the active provider is the no-op NullIdentityDirectory,
    // validation is skipped regardless.
    options.ValidationRequired = true;
});
```

### Fail-closed create validation (`ValidationRequired`)

`ValidationRequired` is opt-in and defaults to `false`. Left at the default, a supplied principal id is accepted without any directory lookup, matching the no-op provider's behaviour.

When `ValidationRequired` is `true` **and** a real provider is active (any provider other than the default `NullIdentityDirectory`), the administrative create paths on the public `ILatticeAuthAdmin` seam validate the supplied principal id against the identity directory before writing anything:

- `UpsertGroupAsync` requires the group id to resolve to a `Group` principal.
- `AddMemberAsync` requires both the member id and the target group id to resolve: the member id to the kind implied by the member kind - a `User` id for a user member, a `Group` id for a nested-group member - and the `groupId` to a `Group` principal.

The check is **fail-closed**: an id that resolves to no principal, or that resolves to a principal of the wrong `DirectoryPrincipalKind` (for example a user id supplied where a group was required), is rejected with the public `LatticeDirectoryValidationException` (which derives from `ArgumentException`) *before* any membership edge is written, so an unresolved or mis-kinded reference never leaves a partial edge behind. Over the gRPC auth binding the exception surfaces as an `InvalidArgument` status.

When the active provider is the no-op `NullIdentityDirectory`, no validation runs regardless of `ValidationRequired`, so the exception is never raised in that configuration.

## The default: no directory (`NullIdentityDirectory`)

`AddLatticeMembership()` registers `NullIdentityDirectory` as the default. Its
`ProviderId` is `"null"`, its `SearchAsync` returns `DirectorySearchPage.Empty`,
and its `ResolveAsync` returns `null` for every id. With the null provider active
the Explorer reports the directory as **unavailable**: the subject picker falls
back to a free-text box and the create form accepts the entered id verbatim
without validating it. A deployment that wants validated create must register a
real provider below.

## The static provider (`AddStaticIdentityDirectory`)

The static provider serves a fixed, in-process roster. It is the zero-dependency
choice for samples, tests, and small deployments, and it is what the
[Explorer sample](../../samples/Explorer/README.md) uses by default.

```csharp verify
siloBuilder.AddLatticeMembership();
siloBuilder.AddStaticIdentityDirectory(roster =>
{
    roster.AddUser("alice", "Alice Example");
    roster.AddUser("bob", "Bob Example");
    roster.AddGroup("editors", "Editors");

    // Also admit any Basic user ids discovered from the process environment,
    // so the roster stays in step with the deployed credential set. With no
    // argument this scans the default LATTICE_STATE_USER_ prefix - the same
    // prefix the reference environment-variable Basic authorizer provisions.
    roster.AddUsersFromEnvironment();
});
```

It registers with a last-wins `AddSingleton`, so it cleanly overrides the default
`NullIdentityDirectory`. `AddUsersFromEnvironment(prefix)` scans the process
environment for variables whose name starts with `prefix` (defaulting to
`LATTICE_STATE_USER_`) and admits the suffix as a user id - the mechanism that
keeps the identity directory aligned with the `LATTICE_STATE_USER_*` Basic
credentials provisioned for the reference environment-variable Basic authorizer.
It reads only variable names, never the credential values.

## The Entra Graph provider (`AddEntraGraphGroupResolver`)

`Orleans.Lattice.Membership.Entra.Graph` registers a Microsoft Graph-backed
`ILatticeIdentityDirectory` (`ProviderId` `"entra"`) that searches and
resolves real users and groups in an Entra tenant. It is **app-only**: it uses the
client-credentials flow with an application registration, independent of how a
console operator signs in.

Register the Entra credential authenticator **before** the Graph resolver (the
resolver's registration marker throws otherwise):

```
siloBuilder.AddLatticeMembership();
siloBuilder.AddEntraCredentialAuthenticator(options =>
{
    options.Authority = $"https://login.microsoftonline.com/{tenantId}/v2.0";
    options.TenantIds.Add(tenantId);
    options.Audiences.Add(clientId);
});
siloBuilder.AddEntraGraphGroupResolver(options =>
{
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.ClientSecret = clientSecret;
    // Scopes default to the Graph app-only ".default" scope.
});
```

The application registration needs the following **application** (not delegated)
Microsoft Graph permissions, each granted admin consent:

| Permission | What it enables |
|---|---|
| `User.Read.All` | Search and resolve users in the tenant. |
| `Group.Read.All` | Search and resolve groups, and expand group membership. |

**Graph unavailability.** On the normal path the provider returns
`DirectoryPrincipal`s carrying the id, display name, and kind (it does not attach a
claim bag). If the app-only token cannot be minted, or a Graph call is denied or
fails, the provider degrades cleanly rather than throwing: a search returns an
empty page and a resolve returns `null`. The provider stays registered, so the
directory is still reported as configured - a search simply surfaces no matches,
and an id that cannot be resolved is blocked as an unknown principal rather than
created.

For the app-registration walkthrough (ids, secret, permission GUIDs) see
[Entra setup](../lattice.membership.entra/entra-setup.md), and for a runnable
end-to-end deployment see the
[Explorer sample's Entra enablement path](../../samples/Explorer/README.md#identity-directory-static-default-and-entra-opt-in).

## Writing a custom provider

Implement `ILatticeIdentityDirectory` and register it last-wins so it overrides
the default. Honour the `PageSize`/`MaxPageSize` bounds and return `null` from
`ResolveAsync` for anything you cannot vouch for - that `null` is what makes the
Explorer create form fail closed.

```csharp verify
public sealed class LdapIdentityDirectory : ILatticeIdentityDirectory
{
    public string ProviderId => "ldap";

    public string DescribeEntry(DirectoryPrincipalKind? kind) => kind switch
    {
        DirectoryPrincipalKind.Group => "Enter an LDAP group cn, for example 'engineering'.",
        _ => "Enter an LDAP sAMAccountName, for example 'jsmith'.",
    };

    public Task<DirectorySearchPage> SearchAsync(
        DirectorySearchQuery query,
        CancellationToken cancellationToken = default)
    {
        // Query your source for query.Term (optionally filtered by query.Kind),
        // cap the result at query.PageSize, and set a continuation token if more
        // pages remain. Returning DirectorySearchPage.Empty is always valid.
        var principals = new List<DirectoryPrincipal>
        {
            new("jsmith", "Jane Smith", DirectoryPrincipalKind.User),
        };
        return Task.FromResult(new DirectorySearchPage(principals));
    }

    public Task<DirectoryPrincipal?> ResolveAsync(
        string principalId,
        CancellationToken cancellationToken = default)
    {
        // Return the principal only if it genuinely exists; null blocks creation.
        DirectoryPrincipal? found = principalId == "jsmith"
            ? new("jsmith", "Jane Smith", DirectoryPrincipalKind.User)
            : null;
        return Task.FromResult(found);
    }
}
```

Register it last-wins after membership so it overrides the default
`NullIdentityDirectory`:

```
siloBuilder.AddLatticeMembership();
siloBuilder.Services.AddSingleton<ILatticeIdentityDirectory, LdapIdentityDirectory>();
```

## Observability

Directory searches record latency and hit/miss outcomes on the
`orleans.lattice.membership` meter. See
[Observability](observability.md#what-the-directory-search-instruments-measure) for the instrument
names and the bundled Grafana panels.

## See also

- [Managing access control from the Explorer](../lattice.explorer/managing-access.md) - the Access area that consumes a provider for its typeahead and validated create.
- [Entra setup](../lattice.membership.entra/entra-setup.md) - the Entra app registration shared by the authenticator and the Graph provider.
- [Observability](observability.md) - the `orleans.lattice.membership` meter, including the identity-directory-search instruments.
