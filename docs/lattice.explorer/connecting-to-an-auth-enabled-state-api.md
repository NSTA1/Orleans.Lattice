---
agent_spec: "docs/agents/governance/identity-propagation.json"
---

# Connecting to an auth-enabled State API

The Explorer connects to one Lattice API endpoint. When that endpoint requires
authentication, the session chrome discovers the advertised auth schemes, offers
a matching sign-in action, and attaches the resulting credential to every State
API call made by that browser circuit.

## Discovery

Before sign-in, the Explorer calls the endpoint's unauthenticated `GetAuthScheme`
gRPC method. The probe carries no credential. If the endpoint advertises schemes,
the Explorer keeps the ordered list and the public parameters attached to each
scheme. If the probe fails or the endpoint advertises nothing, the Explorer falls
back to the built-in Basic form so older or anonymous endpoints keep working.

The advertisement is public configuration only. It can include values such as an
OIDC authority, tenant id, client id and audience. It must not contain secrets.

## Selecting a sign-in method

Each sign-in method implements `IExplorerAuthMethod`:

- `SchemeId` is the stable scheme id.
- `CanHandle(advertisedScheme)` decides whether the method handles an advertised
  scheme.
- `ChallengeAsync(context, cancellationToken)` runs the sign-in and returns an
  `ExplorerAuthSignIn` with the credential attached to the connection.

The shipped methods are:

- `basic` - always available. It accepts the Basic scheme and also handles an
  empty advertisement. The session chrome renders a username/password form.
- `entra` from `Orleans.Lattice.Explorer.Entra` - an interactive MSAL Entra
  sign-in, run from the host process (browser auth-code with PKCE, or device
  code), for hosts that do not use the hosted-web OpenID Connect cookie flow.
- `entra` from `Orleans.Lattice.Explorer.Entra.Web` - hosted-web Entra sign-in
  for the Blazor Server web head. It exchanges the browser OpenID Connect
  session for a downstream State API bearer token.

If the endpoint advertises only schemes for which no method is registered, the
sign-in dialog shows an actionable unsupported-method message rather than
choosing a different scheme.

## Session chrome and server form posts

The web head configures the session chrome to submit Basic credentials through
native server form posts:

- `POST auth/login` validates an antiforgery token, reads `username` and
  `password`, signs in through the auth session, and redirects back to the
  Explorer base href.
- `POST auth/logout` validates an antiforgery token, clears the local State API
  credential, and redirects back to the Explorer base href.

The identity menu also contains **Reset view** and **Sign out**. A federated
provider can publish `ExplorerSignOutOptions.FederatedSignOutPath`; when it does,
the sign-out button posts to that provider endpoint instead of the local logout
endpoint.

The header connection indicator shows connection state, exposes **Sign in** when
the endpoint requires authentication, can reconnect a disconnected endpoint, and
opens the connection settings dialog when the web head allows endpoint editing
(`AllowInteractiveEndpointConfiguration`). Otherwise it has no **Connection
settings** entry; see [The connection dialog](running-the-explorer.md#the-connection-dialog).

## Signing in with Basic

Registering the core auth services makes the Basic provider available. The web
head calls this for you.

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

var services = new ServiceCollection();
services.AddExplorerAuth();
```

The Basic method attaches an `authorization: Basic ...` header through the
connection authentication seam. A static credential header is sent only to
`https` endpoints, or to an endpoint whose settings explicitly allow unencrypted
HTTP/2 for local development.

## Signing in with Entra

Add the optional `Orleans.Lattice.Explorer.Entra` package for an interactive
host, call `AddExplorerAuth()`, then call `AddExplorerEntraAuth(...)` to set the
public OIDC authority (or tenant), client id and State API scope. The verified
setup snippet lives in the
[`Orleans.Lattice.Explorer.Entra` package docs](../lattice.explorer.entra/README.md#setup),
where the Entra package is part of the compiling reference set. Configured values
win over the endpoint advertisement; advertised values only fill in unset options.

An advertised authority is admitted only when it is an absolute `https` URL and
its host is allowed. With no custom allow-list, the provider accepts the known
Entra login hosts. When `AllowedAuthorityHosts` is non-empty, it replaces that
set. When scopes are not configured, an advertised audience is admitted only when
it is an `api://` resource, an `https` resource on the endpoint host, or an exact
match in `AllowedAudiences`.

For the Blazor Server web head, use
[`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md)
instead. That provider integrates with ASP.NET Core OpenID Connect middleware and
Microsoft.Identity.Web.

## Token freshness and re-authentication

Bearer-token methods return `LatticeCallAuthentication.Bearer` over an
`ExplorerAccessTokenSource`. The source refreshes before expiry, coalesces
concurrent refreshes, and latches into a revoked state when silent renewal can no
longer produce a token.

When a credential latches as revoked, the session chrome shows the `Your session
expired` interstitial. If an `ExplorerReauthOptions.ChallengePath` is configured,
**Sign in again** navigates there with the current local URL as the return URL.
Otherwise it performs a full-page reload.

## Where credentials live

Tokens are session state. The core auth session never writes token material to
the Explorer configuration store. Token providers own any optional persistence of
their refresh material.

The Basic credential may be stored by the injected credential store. In the web
head that store is `CookieCredentialStore`: a Data Protection-protected,
`HttpOnly`, `Secure`, `SameSite=Strict` browser cookie, written and cleared by
the `auth/login` and `auth/logout` endpoints. Signing out clears the store and
reconfigures the connection without the credential. Inside a running circuit the
response has already started, so the cookie cannot be deleted there; a clear
therefore also revokes the presented cookie value, and a revoked value reads as
no credential from then on. That revocation is held in the web head's process
only, so a restart loses it and another replica does not see it.

A sign-in is bound to the endpoint it was minted for. If the endpoint changes,
the auth session signs out instead of carrying the credential to a different
host. The cookie carries the same binding: it records the endpoint the
credential was minted for, and the store refuses the credential unless that is
recognisably the endpoint now configured, including when no endpoint can be
resolved. That check does not rely on the in-process revocation, so it holds
after a restart and on every replica.

## Reaching an endpoint behind an origin-locked proxy

Some deployments front the State API with a proxy that requires a routing header,
such as `X-Azure-FDID` for an Azure Front Door origin lock. That header is not a
credential and must survive sign-in. Put it in `TransportHeaders`, not in the
authentication seam.

```csharp verify
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

The environment bootstrap can seed the same headers:

```text
LATTICE_EXPLORER_TRANSPORT_HEADERS=X-Azure-FDID=<front-door-id>
```

## Reference

- `IExplorerAuthMethod` - sign-in provider contract.
- `IExplorerAuthSession` - discovers schemes, drives sign-in and sign-out, and
  applies credentials to the connection. A client that builds its own channel
  attaches `GetAuthenticationFor(endpoint)`, never `CurrentAuthentication`: it
  returns the sign-in only for the endpoint the sign-in was minted for (compared
  ignoring case and a trailing `/`), and `null` for any other. See
  [Adding a custom auth method](adding-a-custom-auth-method.md#security-notes).
- `ExplorerAuthChallengeContext` - selected scheme, advertised parameters,
  interactive inputs, endpoint and `TimeProvider`.
- `ExplorerAccessTokenSource` - proactive, single-flight token refresh.
- `LatticeConnectionSettings.TransportHeaders` - non-secret headers attached to
  every call regardless of sign-in state.

## See also

- [Adding a custom auth method](adding-a-custom-auth-method.md)
- [Configuration](configuration.md)
- [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md)
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md)
