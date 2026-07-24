# Multi-replica and failover hosting

When the Explorer runs as a Blazor Server web app behind more than one replica -
for high availability, a rolling deployment, or a geo-distributed estate - a
signed-in operator can be moved between replicas mid-session (a replica restart,
a scale-in, or a load-balancer rebalance). Two things must survive that move, or
the operator is wedged behind an error they cannot clear without manually
clearing cookies:

1. **The session cookie must be decryptable on the replica that receives the
   request.** ASP.NET Data Protection encrypts the OpenID Connect session cookie
   with a key ring that, by default, is per-instance and ephemeral - so a cookie
   issued by replica A is undecryptable garbage to replica B.
2. **A downstream State API token must be obtainable on the new replica.** With
   the [Entra hosted-web provider](connecting-to-an-auth-enabled-state-api.md),
   the on-behalf-of token is acquired from a token cache. A cold replica has an
   empty cache and, holding only a still-valid cookie, never redeems a fresh
   authorization code - so it cannot acquire a token and the circuit latches
   into a revoked state.

Both fixes are **opt-in and additive**: the default single-instance behaviour is
unchanged. Configure them together for any deployment that runs more than one
replica.

## Durable auth state: a shared Data Protection key ring

Point every replica at one shared, persisted key ring so each can decrypt the
session cookie any other replica issued. `LatticeExplorerWebOptions` exposes the
key-ring persistence as opt-in options; when they are left unset the framework
default (a per-instance ephemeral ring) is used exactly as before.

```csharp verify
using Azure.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Web;

// Supply a DefaultAzureCredential (from Azure.Identity) or a managed-identity
// credential. It is shown here as a seam so the snippet compiles without taking
// a dependency on the Azure.Identity package.
TokenCredential keyRingCredential = ResolveCredential();

var builder = WebApplication.CreateBuilder();

builder.Services.AddLatticeExplorerWeb(options =>
{
    // Persist the key ring to a single blob every replica can read and write.
    options.DataProtectionKeyRingBlobUri =
        new Uri("https://estate.blob.core.windows.net/keys/explorer-keyring.xml");
    options.DataProtectionKeyRingCredential = keyRingCredential;

    // Every replica that must decrypt one another's cookies has to share the
    // same application-discriminator name. Set a stable, deployment-wide value.
    options.DataProtectionApplicationName = "lattice-explorer";
});

static TokenCredential ResolveCredential() => null!;
```

Fail-closed: setting `DataProtectionKeyRingBlobUri` without
`DataProtectionKeyRingCredential` throws at registration time rather than
silently falling back to the ephemeral ring, so a half-configured deployment
fails loudly instead of wedging operators at the first failover.

For advanced needs (key encryption at rest, a different key store, a custom key
lifetime), the `ConfigureDataProtection` escape hatch runs after the built-in
persistence and application-name configuration, whether or not the blob options
above are set:

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

The Entra hosted-web provider caches on-behalf-of tokens. Select the distributed
token cache and register one shared `IDistributedCache` so every replica reads
and writes the same cache. Pointing every region at a single estate-global
container makes on-behalf-of acquisition succeed on any replica an operator is
routed to. See
[the Entra provider's configuration](../lattice.explorer.entra.web/configuration.md#estate-global-token-cache)
for the token-cache option and the estate-global container guidance.

## Graceful re-authentication

Even with a shared key ring and a shared token cache, a token can expire or be
revoked while an operator is signed in (a conditional-access policy change, a
password reset). When the credential provider latches into its revoked state,
the Explorer no longer renders the raw gRPC error inside the circuit: it shows a
small "Your session expired - sign in again" interstitial with an explicit
button that does a full-page navigation to a forced-interactive sign-in, so a
**new** authorization code is redeemed and the replica's token cache is
repopulated.

The interstitial is always present; what it navigates to is configurable. The
[Entra hosted-web provider](../lattice.explorer.entra.web/configuration.md#forced-interactive-re-authentication)
maps a re-authentication endpoint and wires its path automatically, so no extra
configuration is needed with that provider. A custom auth method (see
[Adding a custom auth method](adding-a-custom-auth-method.md)) can point the
interstitial at its own forced-interactive challenge endpoint by registering
`ExplorerReauthOptions`:

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

var services = new ServiceCollection();

// The core UI reads this to build the challenge URL the interstitial navigates
// to. Registered after AddLatticeExplorerWeb so it overrides the core default
// (which leaves the interstitial to a plain reload when no path is set).
services.AddSingleton(new ExplorerReauthOptions
{
    ChallengePath = "/my-auth/reauth",
    AppendReturnUrl = true,
    ReturnUrlParameter = "returnUrl",
});
```

When `ChallengePath` is left unset (the core default), the interstitial button
reloads the current page instead of navigating to a challenge endpoint - correct
for auth methods that recover on a plain reload, such as Basic.

## Checklist

- [x] Persist the Data Protection key ring to shared storage
      (`DataProtectionKeyRingBlobUri` + `DataProtectionKeyRingCredential`) and set
      a stable `DataProtectionApplicationName`.
- [x] Select the distributed token cache and register one estate-global shared
      `IDistributedCache`.
- [x] Map the forced-interactive re-authentication endpoint (automatic with the
      Entra hosted-web provider).

## See also

- [Configuration](configuration.md) - every `LatticeExplorerWebOptions` property.
- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md) - selecting a login method.
- [`Orleans.Lattice.Explorer.Entra.Web` configuration](../lattice.explorer.entra.web/configuration.md) - the reauth endpoint and the estate-global token cache.
