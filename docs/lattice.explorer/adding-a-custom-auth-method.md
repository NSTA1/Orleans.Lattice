# Adding a custom auth method

The Explorer sign-in challenge is provider based. A sign-in mechanism is an
`IExplorerAuthMethod`: Basic, Entra, and custom schemes all plug into the same
contract and are selected from the auth scheme advertised by the State API.

## The contract

`IExplorerAuthMethod` has three members:

- `SchemeId` - the stable scheme id the method implements.
- `CanHandle(advertisedScheme)` - returns whether the method can service a scheme
  advertised by the endpoint.
- `ChallengeAsync(context, cancellationToken)` - runs the sign-in flow and
  returns an `ExplorerAuthSignIn` carrying the credential to apply to the
  connection.

`ExplorerAuthChallengeContext` supplies the selected scheme id, advertised public
parameters, user inputs, endpoint address and `TimeProvider`. Use that clock for
expiry decisions so token flows stay testable.

## A static-header method

The simplest method validates an input and returns a static header through
`LatticeCallAuthentication`.

```csharp verify
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

public sealed class ApiKeyAuthMethod : IExplorerAuthMethod
{
    public string SchemeId => "apikey";

    public bool CanHandle(string advertisedScheme)
        => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    public Task<ExplorerAuthSignIn> ChallengeAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var apiKey = context.Inputs.GetValueOrDefault("apiKey");
        ArgumentException.ThrowIfNullOrWhiteSpace(apiKey);

        var authentication = new LatticeCallAuthentication
        {
            Headers = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["authorization"] = $"ApiKey {apiKey}",
            },
        };

        return Task.FromResult(new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = "API key",
            Authentication = authentication,
        });
    }
}
```

The connection sends static credential headers such as `authorization` only to
`https` endpoints, or to an endpoint whose connection settings explicitly allow
unencrypted HTTP/2 for local development.

## A token method with transparent refresh

For a short-lived token, return `LatticeCallAuthentication.Bearer` over an
`ExplorerAccessTokenSource`. The source refreshes before expiry, collapses
concurrent refreshes into one, and returns to the UI for re-authentication when
silent renewal can no longer produce a token.

```csharp verify
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

public sealed class CustomTokenAuthMethod : IExplorerAuthMethod
{
    public string SchemeId => "custom-oidc";

    public bool CanHandle(string advertisedScheme)
        => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    public async Task<ExplorerAuthSignIn> ChallengeAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var initial = await AcquireInteractiveAsync(context, cancellationToken);

        var source = new ExplorerAccessTokenSource(
            initial,
            ct => new ValueTask<ExplorerAccessToken?>(AcquireSilentAsync(context, ct)),
            context.TimeProvider);

        return new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = "Custom identity",
            Authentication = LatticeCallAuthentication.Bearer(source),
        };
    }

    private static Task<ExplorerAccessToken> AcquireInteractiveAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken ct) => throw new NotImplementedException();

    private static Task<ExplorerAccessToken?> AcquireSilentAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken ct) => throw new NotImplementedException();
}
```

Return `null` from the silent-renewal delegate when the user must complete an
interactive sign-in again. The session chrome then shows its re-authentication
interstitial.

## Registration

Register the method in DI as an `IExplorerAuthMethod`. Stateless methods can be
singletons. Methods that hold per-circuit user state or depend on scoped services
should be scoped, as the shipped Entra methods are.

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

public static class CustomAuthRegistration
{
    public static IServiceCollection AddApiKeyAuth(IServiceCollection services)
    {
        services.AddExplorerAuth();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IExplorerAuthMethod, ApiKeyAuthMethod>());
        return services;
    }

    private sealed class ApiKeyAuthMethod : IExplorerAuthMethod
    {
        public string SchemeId => "apikey";

        public bool CanHandle(string advertisedScheme)
            => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

        public Task<ExplorerAuthSignIn> ChallengeAsync(
            ExplorerAuthChallengeContext context,
            CancellationToken cancellationToken = default)
            => Task.FromResult(new ExplorerAuthSignIn
            {
                SchemeId = SchemeId,
                DisplayName = "API key",
                Authentication = new LatticeCallAuthentication
                {
                    Headers = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        [LatticeCallAuthentication.AuthorizationHeaderName] = "ApiKey example",
                    },
                },
            });
    }
}
```

The Explorer resolves `IEnumerable<IExplorerAuthMethod>` and chooses a method
whose `CanHandle` accepts the advertised scheme. No Explorer core code changes
are required.

## Re-authentication, federated sign-out and CSP

A provider can configure the session chrome around its sign-in without adding a
compile-time dependency from the core Explorer to the provider package:

- `ExplorerReauthOptions` points the `Your session expired` interstitial at a
  forced-interactive challenge endpoint.
- `ExplorerSignOutOptions` points the identity menu's **Sign out** button at a
  federated sign-out endpoint.
- `ExplorerContentSecurityPolicyOptions` contributes extra `form-action` sources
  when that sign-out endpoint redirects to another origin.

`AddExplorerAuth` registers default options with no paths. Register provider
instances after it, or use a provider registration method that does so for the
host.

## Security notes

- Never log access tokens, passwords or API keys.
- Keep token material in memory unless your provider deliberately owns secure
  persistence of refresh material.
- Treat endpoint advertisements as public hints. Do not let a server-provided
  authority or audience override values that the host configured explicitly.
- If your provider accepts advertised parameters, validate them before opening a
  browser or sending a credential.
- A sign-in is bound to the endpoint it was minted for.
  `IExplorerAuthSession.GetAuthenticationFor(endpoint)` returns the credential only
  for that endpoint, compared ignoring case and a trailing `/`, and `null` for any
  other. The Explorer's own transport attaches a credential only through it, so a
  credential never reaches a new endpoint while the console is being repointed,
  before the old sign-in is dropped. A client that builds its own channel should
  do the same rather than read `CurrentAuthentication`. It is a default interface
  member whose default returns `null`, so an implementation that does not record
  the endpoint fails closed and sends no credential.
- A sign-in whose endpoint changes while its challenge runs is refused ("The
  endpoint changed while signing in, so the sign-in was not applied. Sign in to
  the new endpoint.") and is neither applied nor persisted; a stored credential
  replayed across such a change leaves the session anonymous.

## See also

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- [Configuration](configuration.md#explorerreauthoptions)
- [`Orleans.Lattice.Explorer.Entra`](../lattice.explorer.entra/README.md)
- [`Orleans.Lattice.Explorer.Entra.Web`](../lattice.explorer.entra.web/README.md)
