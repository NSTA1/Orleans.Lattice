# Orleans.Lattice.Explorer.Entra

Interactive [Microsoft Entra ID](https://learn.microsoft.com/entra/identity/) (Azure AD) sign-in for the [Orleans.Lattice Explorer](../lattice.explorer/README.md) when it runs as a desktop or CLI host.

## What is it?

`Orleans.Lattice.Explorer.Entra` adds an interactive Entra sign-in method to the Explorer. When the console connects to a **State API** that advertises the `entra` auth scheme, this provider runs an OpenID Connect sign-in (authorization code + PKCE, or the device-code flow for headless hosts), acquires a bearer token for the configured audience, and attaches it to every State API call. The token is refreshed silently before it expires, so a signed-in session is not interrupted while a refresh is still possible.

It is the interactive counterpart to [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md), which drives the hosted-web (Blazor Server) OpenID Connect cookie flow. This package carries the [MSAL](https://learn.microsoft.com/entra/msal/dotnet/) dependency so that hosts using only Basic auth never pay for it.

## Core properties

- **No public API change to the released Explorer.** The package plugs into the core `IExplorerAuthMethod` seam for the `entra` scheme; the existing sign-in dialog already renders a generic "Sign in with ..." button, labelled with the display name the State API advertises for that scheme (falling back to the scheme id), when the endpoint advertises it.
- **MSAL isolated to this package.** `AddExplorerEntraAuth` registers the Entra `IExplorerAuthMethod` alongside the built-in Basic provider without the core Explorer taking any dependency on MSAL.
- **Client-only OIDC parameters.** Every configured value (authority, tenant, client id, scopes) is a public OIDC parameter; no client secret is ever configured on the Explorer.
- **Configuration takes precedence over the advertisement.** When the State API advertises its Entra authority, tenant, client id, and audience, each advertised value is used only for what is *not* configured locally, so static configuration can be omitted without it ever being silently overridden. The advertisement is fetched over an unauthenticated RPC from the very endpoint the minted token is handed to, so it is not a trustworthy source for the identity provider you sign in against: an advertised authority is admitted only when it is `https` and its host is allow-listed: by default the recognised Entra login hosts (`login.microsoftonline.com` and the sovereign-cloud hosts), or, when you set `AllowedAuthorityHosts`, exactly the hosts you list there. Anything else is refused at sign-in with the remedy named.
- **Interactive or headless.** The default is an interactive browser redirect; set `UseDeviceCode` to switch to the device-code flow for headless or CLI hosts, with a `DeviceCodeCallback` to surface the prompt text.

## Setup

Register the Explorer's auth methods, then add the Entra provider. Supplying the authority (or tenant), the public client (application) id, and at least one scope is required unless the State API advertises them:

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

`ExplorerEntraOptions` configures the interactive login provider:

| Property | Type | Default | Purpose |
|---|---|---|---|
| `Authority` | `string?` | `null` | The OIDC authority (for example `https://login.microsoftonline.com/<tenant>`). When set it takes precedence over `TenantId`. |
| `TenantId` | `string?` | `null` | The directory tenant id, used to compose the authority when `Authority` is unset. |
| `ClientId` | `string?` | `null` | The public client (application) id registered in Entra. |
| `Scopes` | `IList<string>` | empty | The scopes requested for the access token, identifying the State API audience (for example `api://<app-id>/.default`). At least one scope is required to acquire a token. |
| `AllowedAuthorityHosts` | `IList<string>` | empty | The hosts an *advertised* authority may name. Consulted only when neither `Authority` nor `TenantId` is configured. When empty, the well-known Entra login hosts are accepted; when non-empty, it replaces that set, so only the listed hosts are accepted. |
| `UseDeviceCode` | `bool` | `false` | When `true`, sign-in uses the device-code flow (for headless/CLI hosts) instead of an interactive browser redirect. |
| `DeviceCodeCallback` | `Func<string, CancellationToken, Task>?` | `null` | Invoked with the device-code prompt text when `UseDeviceCode` is enabled, so a host can surface it however it likes. Defaults to writing to the console. |

`Authority`/`TenantId`, `ClientId` and the audience may be discovered at connect time from the State API's auth-scheme advertisement instead of being supplied statically, but only for what is left unset: a configured value always wins. An advertised authority is additionally admitted only when it is `https` and its host is allow-listed - a recognised Entra login host by default, or one of the hosts in a non-empty `AllowedAuthorityHosts`, which replaces the default set - so a hostile endpoint cannot choose the directory you authenticate against.

## API

| Type or member | Kind | Purpose |
|---|---|---|
| `AddExplorerEntraAuth(this IServiceCollection services, Action<ExplorerEntraOptions>? configure = null)` | Registration (`ExplorerEntraServiceCollectionExtensions`) | Registers the options, the MSAL-backed token acquirer, and the Entra auth method. The acquirer and the method are registered **scoped** (per Blazor circuit) with `TryAdd`, so each circuit holds only its own operator's tokens and a host may substitute its own acquirer. Throws `ArgumentNullException` when `services` is null. |
| `EntraExplorerAuthMethod` | `IExplorerAuthMethod` | The `entra` scheme. `CanHandle` matches the scheme id case-insensitively. `ChallengeAsync` resolves the authority, client id, and scopes (configuration first, then the advertisement), runs the interactive or device-code acquisition, and returns a bearer sign-in whose silent renewal is bound to the account that signed in. Throws `InvalidOperationException` when no authority, client id, or scope can be resolved, or when an advertised authority is refused. |
| `IEntraInteractiveTokenAcquirer` | Seam | `AcquireInteractiveAsync(EntraTokenRequest, CancellationToken)` runs the interactive or device-code flow; `AcquireSilentAsync(EntraTokenRequest, CancellationToken)` renews from cached refresh material and returns `null` when the user must be re-challenged. |
| `MsalEntraInteractiveTokenAcquirer` | Default acquirer | The MSAL public-client implementation. MSAL owns its in-memory token cache, so nothing is written to the Explorer's configuration store. With no `DeviceCodeCallback` the device-code prompt is written to the console. Silent renewal selects the cached account matching the request's `Username` (or the first cached account when the request names none) and returns `null` rather than renew with a different account. |
| `EntraTokenRequest` | `sealed record` | `Authority`, `ClientId`, and `Scopes` (required), `UseDeviceCode`, and `Username` - the account silent renewal must bind to. |
| `EntraTokenResult` | `readonly record struct` | `AccessToken` and `ExpiresOn` (required) and `Username`. In memory only, never persisted. |

## Reference

- [Connecting to an auth-enabled State API](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md) - how the Explorer selects and drives an advertised auth scheme, including this Entra provider.
- [Adding a custom auth method](../lattice.explorer/adding-a-custom-auth-method.md) - the `IExplorerAuthMethod` seam this provider implements.

## See also

- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md) - the core Explorer and its `IExplorerAuthMethod` auth seam.
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md) - the hosted-web (Blazor Server) OpenID Connect counterpart.
