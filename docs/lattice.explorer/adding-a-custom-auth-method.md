# Adding a custom auth method

The Explorer's login challenge is a provider model. Every sign-in mechanism -
Basic, Entra, or one you write - is an `IExplorerAuthMethod`. A custom method
plugs in through dependency injection and drives its own challenge end to end
without any change to the Explorer core.

## The seam

`IExplorerAuthMethod` has three members:

- `SchemeId` - the stable scheme id your method implements. The Explorer matches
  this against the scheme an endpoint advertises.
- `CanHandle(advertisedScheme)` - decides whether your method services a given
  advertised scheme. The built-in default is an ordinal, case-insensitive match
  against `SchemeId`; override it to accept aliases or a family of names.
- `ChallengeAsync(context, cancellationToken)` - runs the (possibly interactive)
  sign-in and returns an `ExplorerAuthSignIn` carrying the credential the
  connection attaches to every call.

The challenge receives an `ExplorerAuthChallengeContext`: the selected scheme,
the public parameters the server advertised for it, any interactive inputs the
user supplied, the endpoint address, and a `TimeProvider` to use for all
token-expiry maths so the flow stays testable.

## A static-header method

The simplest custom method returns a fixed header. Validate inputs, then build a
`LatticeCallAuthentication`:

```csharp
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

## A token method with transparent refresh

For a short-lived token, wrap acquisition in an `ExplorerAccessTokenSource`. You
supply the silent-renewal delegate; the source decides when to call it, refreshes
proactively before expiry, collapses concurrent refreshes into one, and latches
into a re-challenge state when renewal is no longer possible. Return the token
source through `LatticeCallAuthentication.Bearer` so the connection always
attaches a currently-valid token.

```csharp
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

        // Run your interactive flow here to obtain the first token.
        var initial = await AcquireInteractiveAsync(context, cancellationToken);

        var source = new ExplorerAccessTokenSource(
            initial,
            // Silent renewal: return a fresh token, or null when a renewal is no
            // longer possible so the Explorer re-runs the interactive challenge.
            async ct => await AcquireSilentAsync(context, ct),
            context.TimeProvider);

        return new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = "Custom identity",
            Authentication = LatticeCallAuthentication.Bearer(source),
        };
    }

    private static Task<ExplorerAccessToken> AcquireInteractiveAsync(
        ExplorerAuthChallengeContext context, CancellationToken ct) => throw new NotImplementedException();

    private static Task<ExplorerAccessToken?> AcquireSilentAsync(
        ExplorerAuthChallengeContext context, CancellationToken ct) => throw new NotImplementedException();
}
```

Use `context.TimeProvider` for every expiry decision rather than reading the
system clock directly. That keeps the refresh timing deterministic under test:
inject a controllable `TimeProvider`, advance it past the refresh margin, and
assert that exactly one silent renewal ran.

## Registration

Register your method alongside the built-ins with `TryAddEnumerable`. The
Explorer discovers it through `IEnumerable<IExplorerAuthMethod>` and selects it
whenever an endpoint advertises its scheme - no Explorer core code changes.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;

var services = new ServiceCollection();
services.AddExplorerAuth();
services.TryAddEnumerable(ServiceDescriptor.Singleton<IExplorerAuthMethod, ApiKeyAuthMethod>());
```

## Security notes

- Never log the token or credential. The Explorer is a security-sensitive
  client.
- Keep token material in memory. The core Explorer never writes a token to its
  configuration store; persistence of refresh material is your method's opt-in
  decision.
- If your scheme is advertised by the server, advertise only public parameters -
  never a secret or signing key.

## Reference

- [Connecting to an auth-enabled State API](connecting-to-an-auth-enabled-state-api.md)
- `IExplorerAuthMethod`, `ExplorerAuthChallengeContext`, `ExplorerAuthSignIn`
- `ExplorerAccessTokenSource`, `LatticeCallAuthentication`
