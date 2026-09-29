# Orleans.Lattice.Explorer.Entra

`Orleans.Lattice.Explorer.Entra` adds an interactive Microsoft Entra ID sign-in
method to the Explorer for hosts that do not use the hosted-web OpenID Connect
cookie flow.

## What it does

When the configured State API advertises the `entra` auth scheme, this provider
runs an MSAL sign-in, acquires a bearer token for the State API audience, and
returns it through the Explorer's `IExplorerAuthMethod` seam. The token is
refreshed silently before it expires while refresh remains possible.

Use [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md)
for the Blazor Server web head. That package uses ASP.NET Core OpenID Connect
and Microsoft.Identity.Web instead of opening the interactive MSAL flow from the
host process.

## Behaviour

- The provider registers an `IExplorerAuthMethod` for the `entra` scheme. The
  session chrome offers it when the endpoint advertises that scheme.
- MSAL is isolated to this package. Hosts that only need Basic auth do not take
  the dependency.
- All configured values are public OIDC parameters. No client secret is
  configured by this package.
- Static configuration wins over endpoint advertisements. Advertised values fill
  in only the fields left unset locally.
- An advertised authority is accepted only when it is an absolute `https` URL and
  its host is admitted. With no custom allow-list, the provider accepts the known
  Entra login hosts. A non-empty `AllowedAuthorityHosts` list replaces that set.
- `UseDeviceCode` switches from an interactive browser flow to device-code flow.

## Setup

Register the core auth services, then the Entra provider. Supplying an authority
(or tenant id), client id and scope is required unless the endpoint advertises
what you omit.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Entra;

var services = new ServiceCollection();

services.AddExplorerAuth();
services.AddExplorerEntraAuth(options =>
{
    options.Authority = "https://login.microsoftonline.com/<tenant>";
    options.ClientId = "<public-client-id>";
    options.Scopes.Add("api://<state-api-app-id>/.default");
});
```

## Configuration

`ExplorerEntraOptions` configures the provider.

| Property | Type | Default | Purpose |
|---|---|---|---|
| `Authority` | `string?` | `null` | OIDC authority, for example `https://login.microsoftonline.com/<tenant>`. When set, it takes precedence over `TenantId`. |
| `TenantId` | `string?` | `null` | Directory tenant id used to compose the authority when `Authority` is unset. |
| `ClientId` | `string?` | `null` | Public client application id registered in Entra. |
| `Scopes` | `IList<string>` | Empty | Scopes requested for the access token. At least one scope is required after configuration and advertisement are combined. |
| `AllowedAuthorityHosts` | `IList<string>` | Empty | Hosts an advertised authority may name. Empty means the built-in Entra host list; non-empty replaces it. |
| `UseDeviceCode` | `bool` | `false` | Uses device-code flow instead of an interactive browser redirect. |
| `DeviceCodeCallback` | `Func<string, CancellationToken, Task>?` | `null` | Receives the device-code prompt when device-code flow is enabled. The default acquirer writes the prompt to the console. |

## API

| Type or member | Kind | Purpose |
|---|---|---|
| `AddExplorerEntraAuth(this IServiceCollection services, Action<ExplorerEntraOptions>? configure = null)` | Registration extension | Registers options, the MSAL-backed token acquirer, and the scoped Entra auth method. Throws `ArgumentNullException` when `services` is null. |
| `EntraExplorerAuthMethod` | `IExplorerAuthMethod` | Handles the `entra` scheme. It resolves authority, client id and scopes from configuration first, then advertisement, and returns a bearer sign-in with silent renewal bound to the signed-in account. |
| `IEntraInteractiveTokenAcquirer` | Seam | Acquires the first token interactively and renews silently. |
| `MsalEntraInteractiveTokenAcquirer` | Default acquirer | MSAL public-client implementation. MSAL owns the in-memory token cache; the Explorer configuration store is not used for tokens. |
| `EntraTokenRequest` | `sealed record` | Authority, client id, scopes, device-code flag, and optional username for silent renewal. |
| `EntraTokenResult` | `readonly record struct` | Access token, expiry and optional username. In memory only. |

## See also

- [Connecting to an auth-enabled State API](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md)
- [Adding a custom auth method](../lattice.explorer/adding-a-custom-auth-method.md)
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md)
