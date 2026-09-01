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

## The advertised-parameter vocabulary

`context.Parameters` carries what the endpoint advertised for the selected
scheme. That advertisement is public configuration only - never a secret - and
`ExplorerAuthSchemes` names the keys, so a host and a client agree on a spelling
without either side hard-coding a private string.

| Key | Constant | Scheme | Value |
|------|----------|--------|-------|
| `authority` | `AuthorityParameter` | `entra`, `oidc` | The OIDC authority base URL, for example `https://login.microsoftonline.com/<tenant>` or `https://id.example.com/`. |
| `tenantId` | `TenantIdParameter` | `entra` | A directory tenant id, used to build the authority when none was advertised. |
| `clientId` | `ClientIdParameter` | `entra`, `oidc` | The public client (application) id the sign-in runs as. |
| `audience` | `AudienceParameter` | `entra` | The resource the token targets. |
| `scope` | `ScopeParameter` | `oidc` | The scopes to request, space-delimited exactly as the OAuth 2.0 `scope` request parameter encodes them: `openid profile lattice.api`. |
| `metadataAddress` | `MetadataAddressParameter` | `oidc` | An explicit discovery document URL, for a provider whose document does not sit at `{authority}/.well-known/openid-configuration`. |

`basic` advertises no parameters at all: its challenge is the username and
password the user types, which arrive on `context.Inputs` under `UsernameInput`
and `PasswordInput`.

`entra` is the only token scheme with a method in the box today. `oidc` is the
reserved scheme id for a conformant OpenID Connect provider, and the rows above
are the vocabulary a method on that scheme reads - so a host advertising for a
generic provider, and any method written against it, agree on the keys up front.

The two differ in where the scope comes from, and the difference is deliberate.
`entra` derives one from `audience` by appending `/.default`, which is an
Entra/MSAL convention with no generic equivalent. A conformant OpenID Connect
provider states its scopes explicitly, so the `oidc` vocabulary carries `scope`
and nothing infers one from `audience` - an audience is not a scope.

Reading the generic `oidc` set:

```csharp verify
using Orleans.Lattice.Explorer.Core.Authentication;

// What a generic OIDC endpoint advertises, and how a method reads it.
var context = new ExplorerAuthChallengeContext
{
    SchemeId = ExplorerAuthSchemes.Oidc,
    Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [ExplorerAuthSchemes.AuthorityParameter] = "https://id.example.com/",
        [ExplorerAuthSchemes.ClientIdParameter] = "lattice-explorer",
        [ExplorerAuthSchemes.ScopeParameter] = "openid profile lattice.api",
    },
};

var authority = context.Parameters.GetValueOrDefault(ExplorerAuthSchemes.AuthorityParameter);
var clientId = context.Parameters.GetValueOrDefault(ExplorerAuthSchemes.ClientIdParameter);

// An advertised discovery address wins; otherwise fall back to the conventional
// well-known path under the authority.
var metadataAddress =
    context.Parameters.GetValueOrDefault(ExplorerAuthSchemes.MetadataAddressParameter)
    ?? $"{authority?.TrimEnd('/')}/.well-known/openid-configuration";

// The scope list is advertised, never derived.
string[] scopes = (context.Parameters.GetValueOrDefault(ExplorerAuthSchemes.ScopeParameter) ?? string.Empty)
    .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

Console.WriteLine($"{clientId} signs in at {metadataAddress} for {scopes.Length} scope(s)");
```

A method of your own may define its own keys - `CanHandle` is what binds a method
to a scheme, not the parameter names. Reuse the constants above wherever your
scheme means the same thing, so a host does not have to learn a second spelling
of `clientId`.

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
- `ExplorerAuthSchemes` - the scheme ids, challenge input keys, and
  advertised-parameter keys the built-in schemes read.
- `ExplorerAccessTokenSource`, `LatticeCallAuthentication`
