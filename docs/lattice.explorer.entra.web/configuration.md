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
| `ReauthChallengePath` | `string?` | `DefaultReauthPattern` (`/explorer-entra/reauth`) | The path the core Explorer's re-authentication interstitial navigates to when the credential latches into its revoked state. Registered as the core `ExplorerReauthOptions.ChallengePath` so the core UI can drive a forced-interactive sign-in without taking a dependency on this package. Set to `null` to leave the core default (a plain reload) in place. Point `MapLatticeExplorerEntraWebReauth` at the same path. |
| `SignOutPath` | `string?` | `DefaultSignOutPattern` (`/explorer-entra/signout`) | The path the Explorer's "Sign out" button posts to for a full federated sign-out (drop the API credential, clear the OIDC cookie, and end the Entra session). Registered as the core `ExplorerSignOutOptions.FederatedSignOutPath` so the core UI posts here without taking a dependency on this package. Set to `null` to leave the core default (a local-only sign-out that only drops the API credential) in place. Point `MapLatticeExplorerEntraWebSignOut` at the same path. |

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

## Estate-global token cache

The `Distributed` token cache backs Microsoft.Identity.Web with the registered
`IDistributedCache`. On a multi-replica or geo-distributed estate, register one
shared cache and point **every** region at a single estate-global container.
Because the on-behalf-of token an operator acquired on one replica is then
visible to every other replica, a request routed to a cold replica - or to a
different region after a failover - finds the cached token and acquisition
succeeds instead of latching the credential as revoked.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra.Web;

var builder = WebApplication.CreateBuilder();

// Register one IDistributedCache pointed at a single estate-global container,
// shared by every replica in every region (for example
// Orleans.Lattice.Caching.AzureBlob over one estate-global blob container).
builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = "00000000-0000-0000-0000-000000000000";
    options.ClientId = "11111111-1111-1111-1111-111111111111";
    options.TokenCache = ExplorerWebTokenCacheKind.Distributed;
});
```

Pair the estate-global token cache with a
[shared Data Protection key ring](../lattice.explorer/multi-replica-hosting.md#durable-auth-state-a-shared-data-protection-key-ring)
so the session cookie is decryptable on every replica too.

## Forced-interactive re-authentication

A token can expire or be revoked while an operator is signed in (a
conditional-access change, a password reset), or a request can land on a replica
whose cache cannot satisfy it. Microsoft.Identity.Web then raises a re-auth
signal, which this package translates into a typed
`ExplorerWebReauthRequiredException` and latches the credential as revoked. The
core Explorer traps that revoked state and shows a "Your session expired - sign
in again" interstitial whose button navigates to a forced-interactive sign-in.

Map the re-authentication endpoint so that navigation redeems a **new**
authorization code even when a valid session cookie already exists - which is
what repopulates the receiving replica's token cache. A plain page refresh sees
the still-valid cookie and never redeems a fresh code; a `prompt=login`
challenge forces the redemption.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra.Web;

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = "00000000-0000-0000-0000-000000000000";
    options.ClientId = "11111111-1111-1111-1111-111111111111";
});

var app = builder.Build();

// The interstitial's path is wired automatically from ReauthChallengePath; map
// the endpoint at that same path (the default is /explorer-entra/reauth).
app.MapLatticeExplorerEntraWebReauth();
app.MapLatticeExplorerEntraWebSignOut();
```

The endpoint honours a caller-supplied `returnUrl` query parameter, but only
when it is a **local** path: an absolute or protocol-relative URL is rejected and
the browser returns to `/`, so the endpoint cannot be abused as an open
redirect. Pass `select_account` for the `prompt` argument to let the operator
pick a different account, or a custom pattern to relocate the endpoint (keep
`ReauthChallengePath` in sync so the interstitial navigates to the same path).

