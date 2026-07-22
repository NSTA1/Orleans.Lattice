# Orleans.Lattice.Explorer.Entra.Web configuration

The package has a single public options type, `ExplorerEntraWebOptions`, bound through `AddLatticeExplorerEntraWebAuth(configure)`, plus the `ExplorerWebTokenCacheKind` enum.

## `ExplorerEntraWebOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Instance` | `string` | `DefaultInstance` (`https://login.microsoftonline.com/`) | The Entra authority instance. |
| `TenantId` | `string?` | `null` | The directory (tenant) id the console signs users in against. **Required.** |
| `ClientId` | `string?` | `null` | The application (client) id of the Explorer console's own confidential web app registration - the one holding the OIDC redirect URIs, not the State API resource app. **Required.** |
| `ClientSecret` | `string?` | `null` | Optional secret for the confidential client. Leave unset to use a secret-less credential (federated managed-identity assertion or certificate) supplied through `ConfigureMicrosoftIdentityOptions` - the recommended production configuration. |
| `CallbackPath` | `string` | `DefaultCallbackPath` (`/signin-oidc`) | The OIDC authorization-code callback path. |
| `SignedOutCallbackPath` | `string` | `DefaultSignedOutCallbackPath` (`/signout-callback-oidc`) | The OIDC signed-out callback path. |
| `Scopes` | `IList<string>` | empty | The scopes requested for the downstream State API (for example `api://{tenantId}/{app}-silo/.default`). When empty, the provider resolves the scope at sign-in time from the audience the State API advertises, appending `/.default` when the advertised value is a bare resource id. |
| `TokenCache` | `ExplorerWebTokenCacheKind` | `InMemory` | Which token cache backs Microsoft.Identity.Web. Select `Distributed` and register a shared `IDistributedCache` on a multi-replica host. |
| `RequireAuthenticatedUser` | `bool` | `true` | When true, installs a fallback authorization policy so an unauthenticated request to any endpoint is challenged into the OIDC redirect. Set false to manage authorization yourself. |
| `AutoSignIn` | `bool` | `true` | When true, a Blazor Server circuit handler completes the State API sign-in automatically for an already browser-authenticated user. Set false to always require the manual dialog click. |
| `ConfigureMicrosoftIdentityOptions` | `Action<MicrosoftIdentityOptions>?` | `null` | Escape hatch to configure the underlying `MicrosoftIdentityOptions` directly - for example to attach federated managed-identity client credentials for secret-less auth, or adjust the OIDC events. Invoked after the values above are applied. |
| `ConfigureCookieOptions` | `Action<CookieAuthenticationOptions>?` | `null` | Optional callback to configure the cookie authentication options (session lifetime, cookie name). Invoked after Microsoft.Identity.Web applies its defaults. |

### Constants

- `const string DefaultInstance = "https://login.microsoftonline.com/"`
- `const string DefaultCallbackPath = "/signin-oidc"`
- `const string DefaultSignedOutCallbackPath = "/signout-callback-oidc"`

### Validation

The options are validated at registration time (`AddLatticeExplorerEntraWebAuth` calls the configure delegate then validates). A missing `Instance`, `TenantId`, `ClientId`, or `CallbackPath` throws `InvalidOperationException` with an actionable message.

## `ExplorerWebTokenCacheKind`

| Member | Meaning |
|---|---|
| `InMemory` | A per-process in-memory token cache. Correct for a single-replica host. On a multi-replica host a user's cached token is not shared across replicas, so a request routed to a cold replica re-acquires silently. |
| `Distributed` | A Microsoft.Identity.Web distributed token cache over the registered `IDistributedCache`. Register a shared cache (for example `Orleans.Lattice.Caching.AzureBlob`) so a multi-replica host shares one token cache and tokens survive a replica restart. |

## Secret-less production configuration

Prefer a federated managed-identity credential over a client secret. Leave `ClientSecret` unset and attach the credential through the escape hatch.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra.Web;

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = "00000000-0000-0000-0000-000000000000";
    options.ClientId = "11111111-1111-1111-1111-111111111111";
    options.TokenCache = ExplorerWebTokenCacheKind.Distributed;
    options.ConfigureMicrosoftIdentityOptions = identity =>
    {
        // Attach a federated managed-identity or certificate credential here.
    };
});
```
