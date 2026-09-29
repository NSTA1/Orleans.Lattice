# Orleans.Lattice.Explorer.Entra.Web

`Orleans.Lattice.Explorer.Entra.Web` adds hosted-web Microsoft Entra ID sign-in
to the Explorer's Blazor Server web head.

## What it does

The package wires ASP.NET Core OpenID Connect through Microsoft.Identity.Web and
then exchanges the signed-in browser session for a downstream State API token.
The Explorer still sees an `IExplorerAuthMethod` for the `entra` scheme; the web
provider is the implementation behind that scheme.

Use [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md) for
interactive non-web hosts. A remote Blazor Server circuit has no ambient
`HttpContext`, so this package explicitly passes the circuit's `ClaimsPrincipal`
to Microsoft.Identity.Web when it acquires tokens.

## Behaviour

- The provider registers a scoped `IExplorerAuthMethod` for `entra` and a scoped
  token-acquirer seam. Each browser circuit uses its own user and token source.
- By default it installs a fallback authorization policy, so unauthenticated HTTP
  requests are challenged into the OIDC redirect.
- It registers cascading authentication state so the Blazor Server circuit sees
  the authenticated browser principal.
- Silent renewal failures are translated into `ExplorerWebReauthRequiredException`.
  During renewal, the auth method treats that as a revoked credential and lets the
  session chrome show the re-authentication interstitial.
- `AutoSignIn` is on by default. A best-effort circuit handler signs in to the
  State API automatically when the browser user is already authenticated and the
  endpoint advertises `entra`.
- `SignOutPath` publishes a federated sign-out path so the identity menu posts to
  the Entra sign-out endpoint instead of only clearing the local State API
  credential.
- `ReauthChallengePath` publishes the forced-interactive challenge path used by
  the `Your session expired` interstitial.

## Setup

Register the provider on the web host and map the re-authentication and sign-out
endpoints. Tenant id and the Explorer console's own application id are required.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra.Web;

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = "00000000-0000-0000-0000-000000000000";
    options.ClientId = "11111111-1111-1111-1111-111111111111";
    options.Scopes.Add("api://00000000-0000-0000-0000-000000000000/lattice-silo/.default");
});

var app = builder.Build();
app.MapLatticeExplorerEntraWebReauth();
app.MapLatticeExplorerEntraWebSignOut();
```

## Reference

- [API reference](api.md) - registration, endpoint extensions, token seam and
  exception.
- [Configuration](configuration.md) - every public option and enum member.
- [Architecture](architecture.md) - how OIDC middleware, the scoped auth method,
  token acquisition and auto-sign-in fit together.

## See also

- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md)
- [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md)
- [Multi-replica and failover hosting](../lattice.explorer/multi-replica-hosting.md)
- [`Orleans.Lattice.Caching.AzureBlob`](../lattice.caching.azureblob/README.md)
