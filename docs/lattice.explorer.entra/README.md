# Orleans.Lattice.Explorer.Entra

Interactive [Microsoft Entra ID](https://learn.microsoft.com/entra/identity/) (Azure AD) sign-in for the [Orleans.Lattice Explorer](../lattice.explorer/README.md) when it runs as a desktop or CLI host.

## What is it?

`Orleans.Lattice.Explorer.Entra` adds an interactive Entra sign-in method to the Explorer. When the console connects to a **State API** that advertises the `entra` auth scheme, this provider runs an OpenID Connect sign-in (authorization code + PKCE, or the device-code flow for headless hosts), acquires a bearer token for the configured audience, and attaches it to every State API call. The token is refreshed silently before it expires, so a signed-in session is not interrupted while a refresh is still possible.

It is the interactive counterpart to [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md), which drives the hosted-web (Blazor Server) OpenID Connect cookie flow. This package carries the [MSAL](https://learn.microsoft.com/entra/msal/dotnet/) dependency so that hosts using only Basic auth never pay for it.

## Core properties

- **No public API change to the released Explorer.** The package plugs into the core `IExplorerAuthMethod` seam for the `entra` scheme; the existing sign-in dialog already renders a "Sign in with Entra ID" button when the State API advertises that scheme.
- **MSAL isolated to this package.** `AddExplorerEntraAuth` registers the Entra `IExplorerAuthMethod` alongside the built-in Basic provider without the core Explorer taking any dependency on MSAL.
- **Client-only OIDC parameters.** Every configured value (authority, tenant, client id, scopes) is a public OIDC parameter; no client secret is ever configured on the Explorer.
- **Advertised parameters take precedence.** When the State API advertises its Entra authority, tenant, client id, and audience, those advertised values take precedence over the static options, so the static configuration can be omitted.
- **Interactive or headless.** The default is an interactive browser redirect; set `UseDeviceCode` to switch to the device-code flow for headless or CLI hosts, with a `DeviceCodeCallback` to surface the prompt text.

## Setup

Register the Explorer's auth methods, then add the Entra provider. Supplying the authority (or tenant), the public client (application) id, and at least one scope is required unless the State API advertises them:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra;

var services = new ServiceCollection();

services.AddExplorerEntraAuth(options =>
{
    options.Authority = "https://login.microsoftonline.com/<tenant>";
    options.ClientId = "<public-client-id>";
    options.Scopes.Add("api://<state-api-app-id>/.default");
});
```

## Configuration

`ExplorerEntraOptions` configures the interactive login provider:

| Property | Type | Default | Purpose |
|---|---|---|---|
| `Authority` | `string?` | `null` | The OIDC authority (for example `https://login.microsoftonline.com/<tenant>`). When set it takes precedence over `TenantId`. |
| `TenantId` | `string?` | `null` | The directory tenant id, used to compose the authority when `Authority` is unset. |
| `ClientId` | `string?` | `null` | The public client (application) id registered in Entra. |
| `Scopes` | `IList<string>` | empty | The scopes requested for the access token, identifying the State API audience (for example `api://<app-id>/.default`). At least one scope is required to acquire a token. |
| `UseDeviceCode` | `bool` | `false` | When `true`, sign-in uses the device-code flow (for headless/CLI hosts) instead of an interactive browser redirect. |
| `DeviceCodeCallback` | `Func<string, CancellationToken, Task>?` | `null` | Invoked with the device-code prompt text when `UseDeviceCode` is enabled, so a host can surface it however it likes. Defaults to writing to the console. |

Statically supplied values may be discovered instead at connect time from the State API's auth-scheme advertisement; the advertised parameters take precedence.

## Reference

- [Connecting to an auth-enabled State API](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md) - how the Explorer selects and drives an advertised auth scheme, including this Entra provider.
- [Adding a custom auth method](../lattice.explorer/adding-a-custom-auth-method.md) - the `IExplorerAuthMethod` seam this provider implements.

## See also

- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md) - the core Explorer and its `IExplorerAuthMethod` auth seam.
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md) - the hosted-web (Blazor Server) OpenID Connect counterpart.
