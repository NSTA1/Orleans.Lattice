# Orleans.Lattice.Explorer.Entra.Web API reference

The public surface is the options type and cache enum, one service-registration
extension, two endpoint-mapping extensions, the token-acquirer seam and token
result, the auth method, and the re-authentication exception. The
Microsoft.Identity.Web-backed acquirer and the auto-sign-in circuit handler are
internal.

## `ExplorerEntraWebServiceCollectionExtensions`

```text
IServiceCollection AddLatticeExplorerEntraWebAuth(
    this IServiceCollection services,
    Action<ExplorerEntraWebOptions> configure)
```

Registers the Microsoft.Identity.Web OpenID Connect app, the scoped
`EntraWebExplorerAuthMethod`, the scoped `IExplorerWebTokenAcquirer`, the
selected token cache, and, by default, a fallback authorization policy plus the
auto-sign-in circuit handler.

- Throws `ArgumentNullException` when `services` or `configure` is null.
- Throws `InvalidOperationException` when a required option is missing.
- Registers the auth method and token acquirer as scoped for per-circuit
  credential isolation.
- Calls `AddCascadingAuthenticationState()` so the Blazor Server circuit sees the
  OpenID Connect user.
- Installs a fallback authenticated-user policy only when
  `RequireAuthenticatedUser` is `true`.
- Registers the auto-sign-in circuit handler only when `AutoSignIn` is `true`.
- When `ReauthChallengePath` is set, publishes `ExplorerReauthOptions` so the
  session chrome navigates to that path for re-authentication.
- When `SignOutPath` is set, publishes `ExplorerSignOutOptions` so the identity
  menu posts to that path for federated sign-out.
- When `SignOutPath` is set and `Instance` is an absolute HTTP or HTTPS URI,
  contributes its origin to `ExplorerContentSecurityPolicyOptions` as an extra
  `form-action` source.

## `ExplorerEntraWebEndpointRouteBuilderExtensions`

```text
const string DefaultSignOutPattern = "/explorer-entra/signout";

IEndpointConventionBuilder MapLatticeExplorerEntraWebSignOut(
    this IEndpointRouteBuilder endpoints,
    string pattern = DefaultSignOutPattern,
    string redirectUri = "/")
```

Maps a federated sign-out `POST` endpoint. The endpoint validates antiforgery,
clears the local State API credential when an `IExplorerAuthSession` is available,
clears the OpenID Connect cookie, and signs the user out of Entra. It redirects
back to `redirectUri` after sign-out. Throws `ArgumentNullException` for a null
endpoint builder and `ArgumentException` for a blank pattern.

```text
const string DefaultReauthPattern = "/explorer-entra/reauth";
const string DefaultReauthPrompt = "login";
const string DefaultReturnUrlParameter = "returnUrl";

IEndpointConventionBuilder MapLatticeExplorerEntraWebReauth(
    this IEndpointRouteBuilder endpoints,
    string pattern = DefaultReauthPattern,
    string prompt = DefaultReauthPrompt,
    string returnUrlParameter = DefaultReturnUrlParameter)
```

Maps a forced-interactive re-authentication `GET` endpoint. It issues an OpenID
Connect challenge with the supplied `prompt` value, defaulting to `login`, so a
new authorization code is redeemed even when the browser already has a valid
cookie. The return URL query value is honoured only when it is a local path; an
absolute or protocol-relative URL returns the browser to `/`. Throws
`ArgumentNullException` for a null endpoint builder and `ArgumentException` for a
blank pattern, prompt or return-url parameter.

## `IExplorerWebTokenAcquirer`

```text
Task<ExplorerWebToken> AcquireTokenAsync(
    IReadOnlyList<string> scopes,
    CancellationToken cancellationToken = default)
```

Acquires a downstream State API token for the signed-in browser user. The default
implementation passes the circuit's `ClaimsPrincipal` to Microsoft.Identity.Web
explicitly. It throws `ExplorerWebReauthRequiredException` when the browser
session is not authenticated or Microsoft.Identity.Web requires interaction.

## `ExplorerWebToken`

`ExplorerWebToken` is a `readonly record struct` with these properties:

| Property | Type | Meaning |
|---|---|---|
| `AccessToken` | `string` | Required raw access token. |
| `ExpiresOn` | `DateTimeOffset` | Required absolute expiry instant. |
| `Username` | `string?` | Resolved account name when known. |

## `EntraWebExplorerAuthMethod`

`EntraWebExplorerAuthMethod` is the public sealed `IExplorerAuthMethod` for the
`entra` scheme. It is registered scoped by `AddLatticeExplorerEntraWebAuth`.

```text
EntraWebExplorerAuthMethod(
    IExplorerWebTokenAcquirer acquirer,
    IOptionsMonitor<ExplorerEntraWebOptions> options)

string SchemeId { get; }
bool CanHandle(string advertisedScheme)
Task<ExplorerAuthSignIn> ChallengeAsync(
    ExplorerAuthChallengeContext context,
    CancellationToken cancellationToken = default)
```

`SchemeId` returns `entra`. `CanHandle` matches that scheme case-insensitively.
`ChallengeAsync` resolves scopes from `ExplorerEntraWebOptions.Scopes` or the
advertised audience, acquires the initial downstream token, and returns a bearer
sign-in. Silent renewal latches the credential as revoked when token acquisition
throws `ExplorerWebReauthRequiredException`.

## `ExplorerWebReauthRequiredException`

Thrown when the browser must complete or repeat the interactive OIDC sign-in
before a State API token can be acquired.

```text
ExplorerWebReauthRequiredException()
ExplorerWebReauthRequiredException(string message)
ExplorerWebReauthRequiredException(string message, Exception innerException)
```

The exception is sealed and derives directly from `System.Exception`.

## See also

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Hosted-web Entra overview](README.md)
