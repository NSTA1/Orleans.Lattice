# Orleans.Lattice.Explorer.Entra.Web

Hosted-web [Microsoft Entra ID](https://learn.microsoft.com/entra/identity/) (OpenID Connect) sign-in for the [Orleans.Lattice Explorer](../lattice.explorer/README.md) when it runs as a Blazor Server web app.

## What is it?

`Orleans.Lattice.Explorer.Entra.Web` adds an interactive, browser-based Entra sign-in to a hosted Explorer. It wires the standard ASP.NET Core OpenID Connect flow (authorization code + PKCE, cookie session) through [Microsoft.Identity.Web](https://learn.microsoft.com/entra/msal/dotnet/microsoft-identity-web/), then exchanges the signed-in user's session for a downstream **State API** token so the Explorer connects to an auth-enabled cluster as that user.

It is the web counterpart to [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md), which uses an interactive desktop/device-code MSAL flow. A remote Blazor Server circuit has no ambient `HttpContext`, so this package acquires tokens by passing the circuit's `ClaimsPrincipal` to Microsoft.Identity.Web explicitly.

## Core properties

- **No public API change to the released Explorer.** The package plugs into the core `IExplorerAuthMethod` seam for the `entra` scheme; the existing sign-in dialog already renders a generic "Sign in with Entra ID" button when the State API advertises that scheme. The OIDC redirect happens at the ASP.NET middleware layer, not inside the SignalR circuit.
- **Per-circuit credential isolation.** The auth method and token acquirer are registered **scoped**, so each user's circuit acquires and holds only its own token.
- **Fail-closed authorization.** By default the registration installs a fallback authorization policy that challenges any unauthenticated request into the OIDC redirect. A re-auth-required signal from Microsoft.Identity.Web is translated into a typed `ExplorerWebReauthRequiredException` and latches the credential as revoked rather than serving a stale token.
- **Multi-replica ready.** Select the distributed token cache and register a shared `IDistributedCache` (for example [`Orleans.Lattice.Caching.AzureBlob`](../lattice.caching.azureblob/README.md)) so a user routed to a cold replica does not silently lose their session. See [multi-replica and failover hosting](../lattice.explorer/multi-replica-hosting.md).
- **Graceful re-authentication.** When the credential latches as revoked, the core Explorer shows a "sign in again" interstitial that navigates to the mapped `MapLatticeExplorerEntraWebReauth` endpoint, which forces a fresh interactive sign-in (`prompt=login`) so a new authorization code is redeemed and the replica's token cache is repopulated.
- **Optional auto-sign-in.** A best-effort Blazor Server circuit handler completes the State API sign-in automatically for an already browser-authenticated user, so the console connects without a manual click; any failure degrades silently to the interactive dialog.

## Setup

Register the provider on the web host and map the re-authentication and sign-out endpoints. Supplying the tenant and the Explorer console's own application (client) id is required.

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

- [API reference](api.md) - the public options, extensions, token-acquirer seam, and exception.
- [Configuration](configuration.md) - every public options property, its type, and its default.
- [Architecture](architecture.md) - how the OIDC middleware, the scoped auth method, the token acquirer, and the auto-sign-in circuit handler fit together.

## See also

- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md) - the core Explorer and its `IExplorerAuthMethod` auth seam.
- [`Orleans.Lattice.Caching.AzureBlob`](../lattice.caching.azureblob/README.md) - a durable `IDistributedCache` for the distributed token cache on a multi-replica host.
