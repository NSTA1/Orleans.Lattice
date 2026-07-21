# Connecting to an auth-enabled State API

The Orleans.Lattice Explorer connects to a Lattice State API endpoint to browse
cluster state. When the endpoint requires authentication, the Explorer runs an
extensible login challenge: it discovers which authentication scheme the
endpoint accepts, presents the matching sign-in surface, and attaches the
resulting credential to every State-API call.

## How discovery works

Before sign-in the Explorer calls the endpoint's unauthenticated
`GetAuthScheme` RPC. This probe carries no credential and the server answers it
without enforcing authorization, so the Explorer can learn how to sign in before
it holds anything. The response advertises the accepted schemes, in the server's
preference order, and the public parameters each one needs (for example an OIDC
authority, tenant, client id, and audience).

The advertisement carries only public configuration. It never contains a secret,
a signing key, or any user-specific data, so it is safe to serve without a
credential. An endpoint that advertises nothing (the default) leaves the
Explorer to fall back to manual scheme selection or the built-in Basic form,
which keeps older or anonymous endpoints working unchanged.

## Selecting a login method

Each login method is an `IExplorerAuthMethod` with a stable `SchemeId`. The
Explorer matches the advertised scheme to a registered method and runs its
challenge:

- `basic` presents a username and password form and attaches an
  `authorization: Basic ...` header. This is always available.
- `entra` (from the optional `Orleans.Lattice.Explorer.Entra` package) runs an
  interactive Microsoft Entra ID sign-in and attaches a bearer token.
- Any custom scheme a host registers presents its own sign-in surface.

When the endpoint advertises a scheme the Explorer has no method for, the sign-in
surface shows a clear, actionable message rather than guessing.

## Signing in with Basic

The Basic form behaves exactly as it always has. Nothing changes for an endpoint
that requires a username and password; Basic is simply one login method among
many now.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

var services = new ServiceCollection();
services.AddExplorerAuth();
```

## Signing in with Entra

Add the optional Entra package and register the Entra login method. The MSAL and
Entra dependencies stay out of the core Explorer, so hosts that do not need Entra
never pay for it.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Entra;

var services = new ServiceCollection();
services.AddExplorerAuth();
services.AddExplorerEntraAuth(options =>
{
    // These are public OIDC parameters. When the endpoint advertises them,
    // the advertised values take precedence and no static configuration is
    // needed here.
    options.Authority = "https://login.microsoftonline.com/<tenant>";
    options.ClientId = "<public-client-id>";
    options.Scopes.Add("api://<state-api-app-id>/.default");
});
```

When the endpoint advertises the Entra authority, client id, and audience, the
static options above are optional: the Explorer resolves the parameters from the
advertisement at connect time.

## Token freshness

A bearer-token method never attaches an expired token. The token source refreshes
proactively and silently: a token is treated as expiring once the clock reaches
its expiry minus a clock-skew margin (two minutes by default), so a fresh token
is acquired before the old one is rejected. A signed-in user sees no interruption
while a refresh is still possible.

Concurrency is single-flight. When many calls observe an expiring token at once,
one refresh runs and the rest reuse its result, so a burst of calls never causes
a thundering herd on the token endpoint. If a mid-session call is rejected with
an authentication failure, the Explorer performs a single silent refresh and
retries once before surfacing anything to the user.

Only when a refresh is genuinely impossible - the refresh material has expired or
been revoked, or consent was withdrawn - does the Explorer re-run the interactive
challenge. A hard authentication failure re-challenges rather than dropping the
user into a broken session.

## Where tokens live

Tokens are session state. They live only in memory and are never written to the
Explorer's configuration store. Signing out clears the token and drops the
connection back to anonymous. This differs from the Basic credential, which the
Explorer may persist to its injected credential store; a token is never
persisted by the core Explorer. Persistence of refresh material is a
provider-owned, opt-in concern.

## Reaching an endpoint behind an origin-locked proxy

Some deployments front the State API with a proxy that only accepts requests
carrying a specific routing header, and reject anything else at the origin. Azure
Front Door with an origin lock is the common case: the silo origin refuses any
request that does not carry the `X-Azure-FDID` header identifying the expected
Front Door instance.

The Explorer dials the State API over native gRPC (HTTP/2), which such a proxy
usually cannot forward, so the Explorer connects to the origin directly and must
present the routing header itself. That header is **not** a credential, so it
does not belong on the authentication seam: an interactive sign-in replaces the
whole authentication object, which would drop any header carried there. Instead
it rides on `LatticeConnectionSettings.TransportHeaders`, applied to every call
independently of the sign-in state.

Set it through `ExplorerConfiguration.TransportHeaders`, which maps straight onto
the connection settings:

```csharp
using System.Collections.Generic;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

var configuration = new ExplorerConfiguration
{
    Endpoint = "https://silo-origin.example:443",
    TransportHeaders = new Dictionary<string, string>
    {
        ["X-Azure-FDID"] = "<front-door-id>",
    },
};

LatticeConnectionSettings settings = configuration.ToConnectionSettings();
```

A deployment that uses the environment bootstrap can seed the same header without
code through the `LATTICE_EXPLORER_TRANSPORT_HEADERS` variable, a semicolon-
separated list of `Name=Value` pairs (a value may itself contain `=` or be
empty):

```
LATTICE_EXPLORER_TRANSPORT_HEADERS=X-Azure-FDID=<front-door-id>
```

These headers are non-secret routing metadata, so they are safe to persist in the
Explorer's configuration store and to pass through the environment. The live
authentication credential never flows through this seam.

## Reference

- `IExplorerAuthMethod` - the login-method seam (`SchemeId`, `CanHandle`,
  `ChallengeAsync`).
- `IExplorerAuthSession` - the session that discovers schemes and drives sign-in
  (`DiscoverAsync`, `LoginAsync`, `LoginWithMethodAsync`, `CurrentScheme`).
- `ExplorerAccessTokenSource` - the proactive, single-flight token-refresh engine.
- `LatticeConnectionSettings.TransportHeaders` - non-secret headers attached to
  every call regardless of the sign-in state (for example an origin-lock routing
  header), seedable via `LATTICE_EXPLORER_TRANSPORT_HEADERS`.
- [Adding a custom auth method](adding-a-custom-auth-method.md)
