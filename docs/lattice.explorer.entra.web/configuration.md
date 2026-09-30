# Orleans.Lattice.Explorer.Entra.Web configuration

`Orleans.Lattice.Explorer.Entra.Web` exposes one options type,
`ExplorerEntraWebOptions`, and one public enum, `ExplorerWebTokenCacheKind`.
The options are configured through `AddLatticeExplorerEntraWebAuth(configure)`
and validated during registration.

## `ExplorerEntraWebOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Instance` | `string` | `DefaultInstance` (`https://login.microsoftonline.com/`) | Entra authority instance. |
| `TenantId` | `string?` | `null` | Directory tenant id the console signs users in against. Required. |
| `ClientId` | `string?` | `null` | Application id of the Explorer console's confidential web app registration. Required. |
| `ClientSecret` | `string?` | `null` | Optional confidential-client secret. Leave unset when credentials are supplied through `ConfigureMicrosoftIdentityOptions`. |
| `CallbackPath` | `string` | `DefaultCallbackPath` (`/signin-oidc`) | OIDC authorization-code callback path. Required to be non-blank. |
| `SignedOutCallbackPath` | `string` | `DefaultSignedOutCallbackPath` (`/signout-callback-oidc`) | OIDC signed-out callback path. |
| `Scopes` | `IList<string>` | Empty | Downstream State API scopes. When empty, the auth method resolves the scope from the State API advertised audience and appends `/.default` when needed. |
| `TokenCache` | `ExplorerWebTokenCacheKind` | `InMemory` | Microsoft.Identity.Web token-cache backing. Use `Distributed` with a shared `IDistributedCache` for multi-replica hosting. |
| `RequireAuthenticatedUser` | `bool` | `true` | Installs a fallback authorization policy that challenges unauthenticated requests into OIDC. Set `false` to manage HTTP authorization yourself. |
| `AutoSignIn` | `bool` | `true` | Enables the best-effort circuit handler that signs in to the State API automatically for an already browser-authenticated user. |
| `ReauthChallengePath` | `string?` | `DefaultReauthPattern` (`/explorer-entra/reauth`) | Path published to `ExplorerReauthOptions.ChallengePath` for the session-expired interstitial. Set `null` to leave the core default reload behaviour in place. Map `MapLatticeExplorerEntraWebReauth` at the same path. |
| `SignOutPath` | `string?` | `DefaultSignOutPattern` (`/explorer-entra/signout`) | Path published to `ExplorerSignOutOptions.FederatedSignOutPath` so the identity menu performs full federated sign-out. Set `null` to leave local-only sign-out in place. Map `MapLatticeExplorerEntraWebSignOut` at the same path. |
| `ConfigureMicrosoftIdentityOptions` | `Action<MicrosoftIdentityOptions>?` | `null` | Callback invoked after the option values above are copied to Microsoft.Identity.Web options. Use it for advanced OIDC events or secret-less credentials. |
| `ConfigureCookieOptions` | `Action<CookieAuthenticationOptions>?` | `null` | Callback invoked after Microsoft.Identity.Web applies cookie defaults. Use it for session lifetime or cookie-name changes. |

### Constants

| Constant | Value |
|---|---|
| `DefaultInstance` | `https://login.microsoftonline.com/` |
| `DefaultCallbackPath` | `/signin-oidc` |
| `DefaultSignedOutCallbackPath` | `/signout-callback-oidc` |

### Validation

Registration throws `InvalidOperationException` when `Instance`, `TenantId`,
`ClientId`, or `CallbackPath` is blank. It throws `ArgumentNullException` when
the service collection or configure callback is null.

## `ExplorerWebTokenCacheKind`

| Member | Meaning |
|---|---|
| `InMemory` | Per-process Microsoft.Identity.Web token cache. Correct for a single replica. A cold replica in a multi-replica deployment cannot read tokens acquired elsewhere. |
| `Distributed` | Microsoft.Identity.Web distributed token cache over the registered `IDistributedCache`. Use one shared cache for every replica and region that must survive failover without re-authentication. |

## Secret-less production configuration

Prefer a secret-less credential over a client secret when your hosting platform
supports it. Leave `ClientSecret` unset and attach the credential through
`ConfigureMicrosoftIdentityOptions`.

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

For multi-replica or geo-distributed hosting, select `Distributed` and register
one shared `IDistributedCache`. Point every region at the same estate-global
cache so a token acquired by one replica can be used by another after failover.

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Entra.Web;

var builder = WebApplication.CreateBuilder();

// Register one IDistributedCache shared by every Explorer replica before this.
builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = "00000000-0000-0000-0000-000000000000";
    options.ClientId = "11111111-1111-1111-1111-111111111111";
    options.TokenCache = ExplorerWebTokenCacheKind.Distributed;
});
```

Pair the distributed token cache with a
[shared Data Protection key ring](../lattice.explorer/multi-replica-hosting.md#durable-auth-state-a-shared-data-protection-key-ring)
so every replica can decrypt the browser session cookie too.

## Forced-interactive re-authentication

When Microsoft.Identity.Web cannot acquire a token silently, the package throws
`ExplorerWebReauthRequiredException`. During renewal of an existing sign-in, the
auth method latches the credential as revoked. The session chrome then shows the
`Your session expired` interstitial, whose **Sign in again** button navigates to
`ReauthChallengePath`.

Map the endpoint so that navigation redeems a new authorization code even when a
valid browser cookie already exists.

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

app.MapLatticeExplorerEntraWebReauth();
app.MapLatticeExplorerEntraWebSignOut();
```

The re-authentication endpoint honours `returnUrl` only when it is a local path;
absolute and protocol-relative URLs return to `/`. Pass `select_account` as the
`prompt` argument when operators should choose a different account. If you change
the pattern, keep `ReauthChallengePath` in sync.

## Federated sign-out and CSP

`SignOutPath` defaults to `/explorer-entra/signout`. When present, registration
publishes it to the Explorer core so the identity menu posts there. The endpoint
validates antiforgery, clears the local State API credential, clears the cookie,
and signs out of Entra.

The registration also adds the configured `Instance` origin to the Explorer web
head's `form-action` CSP sources when it parses as an HTTP or HTTPS URI. A
malformed value contributes nothing, so the policy fails closed.

## See also

- [API reference](api.md)
- [Architecture](architecture.md)
- [Multi-replica and failover hosting](../lattice.explorer/multi-replica-hosting.md)
