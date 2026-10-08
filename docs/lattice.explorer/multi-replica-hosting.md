# Multi-replica and failover hosting

When the Explorer runs as a Blazor Server web app behind more than one replica,
a signed-in operator can move between replicas during a session. That can happen
on restart, scale-in, rolling deployment or load-balancer rebalance. Two pieces
of auth state must survive that move:

1. The browser session cookie must be decryptable on the replica that receives
   the request. ASP.NET Data Protection uses a key ring; the framework default is
   per-instance, so replica B cannot read a cookie issued by replica A. The
   Basic sign-in's credential cookie is encrypted with the same key ring, so a
   replica that cannot decrypt it treats the operator as signed out.
2. A downstream State API token must be obtainable on the new replica. With the
   hosted-web Entra provider, Microsoft.Identity.Web reads a token cache. A cold
   replica has no token unless that cache is shared or the user is forced through
   a fresh interactive sign-in.

Both fixes are opt-in. Single-instance hosting keeps the default framework
behaviour.

## Live circuits still need session affinity

Shared auth storage does not transfer a live Blazor Server circuit between
replicas. The web head uses interactive server components, and each circuit
owns its scoped cluster connection and in-memory UI state. Route a live
circuit's requests to the same replica with session affinity. If that replica
is lost, the browser must establish a fresh circuit; a shared key ring and
token cache let that new circuit recover sign-in, not resume the old circuit.

Keep the Explorer in a separate deployment where possible, or scope affinity
to its mount path. See [Deployment: prefer an isolated head](running-the-explorer.md#deployment-prefer-an-isolated-head).

## Durable auth state: a shared Data Protection key ring

Point every replica at one shared key ring so each can decrypt cookies issued by
any other replica. The web head exposes this through `LatticeExplorerWebOptions`.

```csharp verify
using Azure.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

TokenCredential keyRingCredential = ResolveCredential();

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.DataProtectionKeyRingBlobUri =
        new Uri("https://estate.blob.core.windows.net/keys/explorer-keyring.xml");
    options.DataProtectionKeyRingCredential = keyRingCredential;
    options.DataProtectionApplicationName = "lattice-explorer";
});

static TokenCredential ResolveCredential() => null!;
```

Setting `DataProtectionKeyRingBlobUri` without
`DataProtectionKeyRingCredential` throws during registration. The head fails
closed rather than silently using an ephemeral key ring.

For advanced Data Protection configuration, use `ConfigureDataProtection`. It
runs after the built-in blob and application-name configuration.

```csharp verify
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

var services = new ServiceCollection();

services.AddLatticeExplorerWeb(options =>
{
    options.ConfigureDataProtection = dataProtection =>
        dataProtection.SetDefaultKeyLifetime(TimeSpan.FromDays(30));
});
```

## Durable auth state: a shared token cache

The hosted-web Entra provider uses Microsoft.Identity.Web to acquire downstream
State API tokens. Select the distributed token cache and register one shared
`IDistributedCache` for every replica. In a geo-distributed estate, point every
region at the same estate-global cache so a failover replica can find the token
acquired in another region.

See [the Entra web configuration](../lattice.explorer.entra.web/configuration.md#estate-global-token-cache)
for the `TokenCache` option and cache guidance.

## Graceful re-authentication

Even with a shared key ring and token cache, a token can expire or be revoked.
When the credential provider reports that silent renewal is no longer possible,
the session chrome shows a `Your session expired` interstitial. Its **Sign in
again** button performs a full-page navigation to the configured
re-authentication challenge.

The hosted-web Entra provider publishes a default challenge path and the host
maps it with `MapLatticeExplorerEntraWebReauth()`. That endpoint issues an OpenID
Connect challenge with `prompt=login`, redeeming a new authorization code even
when the browser still holds a valid cookie.

A custom provider can point the interstitial at its own endpoint by registering
`ExplorerReauthOptions`:

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

var services = new ServiceCollection();

services.AddSingleton(new ExplorerReauthOptions
{
    ChallengePath = "/my-auth/reauth",
    AppendReturnUrl = true,
    ReturnUrlParameter = "returnUrl",
});
```

When `ChallengePath` is unset, the interstitial falls back to a plain full-page
reload. That is useful only for methods that can recover on reload.

## Checklist

- Keep live Blazor Server circuits on one replica with session affinity;
  shared auth storage is not a replacement for sticky routing.

- Persist the Data Protection key ring to shared storage with
  `DataProtectionKeyRingBlobUri` and `DataProtectionKeyRingCredential`.
- Set one stable `DataProtectionApplicationName` across all replicas that share
  cookies.
- For hosted-web Entra, use `ExplorerWebTokenCacheKind.Distributed` and register
  a shared `IDistributedCache`.
- Map the forced-interactive re-authentication endpoint. With the hosted-web
  Entra provider this is `app.MapLatticeExplorerEntraWebReauth()`.

## See also

- [Configuration](configuration.md)
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [`Orleans.Lattice.Explorer.Entra.Web` configuration](../lattice.explorer.entra.web/configuration.md)
