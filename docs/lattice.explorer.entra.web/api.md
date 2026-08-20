# Orleans.Lattice.Explorer.Entra.Web API reference

The public surface is the options (covered in [configuration](configuration.md)), two registration/endpoint extensions, the token-acquirer seam and its result type, the auth method, and a typed re-auth exception. The Microsoft.Identity.Web-backed acquirer and the auto-sign-in circuit handler are internal.

## `ExplorerEntraWebServiceCollectionExtensions`

```csharp
public static IServiceCollection AddLatticeExplorerEntraWebAuth(
    this IServiceCollection services,
    Action<ExplorerEntraWebOptions> configure)
```

Registers the Microsoft.Identity.Web OpenID Connect app (auth-code + PKCE, cookie session), the scoped `EntraWebExplorerAuthMethod` for the `entra` scheme, the scoped `IExplorerWebTokenAcquirer`, the selected token cache, and (by default) a fallback authorization policy plus the auto-sign-in circuit handler.

- **Throws** `ArgumentNullException` when `services` or `configure` is null, and `InvalidOperationException` when a required option is missing (validation runs during this call).
- The auth method and token acquirer are registered **scoped** for per-circuit credential isolation.
- The fallback authorization policy is installed only when `RequireAuthenticatedUser` is true; the circuit handler only when `AutoSignIn` is true.
- **CSP side effect.** When `SignOutPath` is set, the call also publishes it as the core `ExplorerSignOutOptions.FederatedSignOutPath` and, when `Instance` parses as an absolute http(s) authority, adds that Entra authority origin to the Explorer web head's Content-Security-Policy `form-action` sources (via `ExplorerContentSecurityPolicyOptions.AdditionalFormActionSources`) so the federated sign-out form's redirect to Entra's end-session URL is not blocked by the default `form-action 'self'`. A malformed `Instance` contributes nothing (fail closed).

## `ExplorerEntraWebEndpointRouteBuilderExtensions`

```csharp
public const string DefaultSignOutPattern = "/explorer-entra/signout";

public static IEndpointConventionBuilder MapLatticeExplorerEntraWebSignOut(
    this IEndpointRouteBuilder endpoints,
    string pattern = DefaultSignOutPattern,
    string redirectUri = "/")
```

Maps a federated sign-out **`POST`** endpoint that drops the local State API credential (via `IExplorerAuthSession.LogoutAsync`, when the session is registered), clears the OpenID Connect cookie, and signs the user out of Entra, redirecting to `redirectUri` afterwards. Because signing out mutates session state it is a `POST` guarded by antiforgery validation - a cross-site `GET` (a logout-CSRF) cannot trigger it - so the Explorer's "Sign out" button renders an HTML form carrying a `RequestVerificationToken`. `AddLatticeExplorerEntraWebAuth` publishes `SignOutPath` as the core `ExplorerSignOutOptions.FederatedSignOutPath` so the button posts here automatically. This is distinct from the Explorer's own in-process State API sign-out, which only drops the API credential and leaves the browser session in place (letting the fallback authorization policy silently re-authenticate the circuit). Throws `ArgumentNullException` when `endpoints` is null and `ArgumentException` when `pattern` is blank.

```csharp
public const string DefaultReauthPattern = "/explorer-entra/reauth";
public const string DefaultReauthPrompt = "login";
public const string DefaultReturnUrlParameter = "returnUrl";

public static IEndpointConventionBuilder MapLatticeExplorerEntraWebReauth(
    this IEndpointRouteBuilder endpoints,
    string pattern = DefaultReauthPattern,
    string prompt = DefaultReauthPrompt,
    string returnUrlParameter = DefaultReturnUrlParameter)
```

Maps a forced-interactive re-authentication endpoint that issues an OpenID Connect challenge with `prompt=login`, so a **new** authorization code is redeemed even when a valid session cookie already exists - repopulating a failover replica's token cache. The core Explorer's re-authentication interstitial navigates here when the credential latches into its revoked state. The endpoint honours the `returnUrlParameter` query value only when it is a **local** path (an absolute or protocol-relative URL is rejected and the browser returns to `/`), so it cannot be abused as an open redirect. Pass `select_account` for `prompt` to let the operator pick a different account. Throws `ArgumentNullException` when `endpoints` is null and `ArgumentException` when `pattern`, `prompt`, or `returnUrlParameter` is blank.

## `IExplorerWebTokenAcquirer`

```csharp
public interface IExplorerWebTokenAcquirer
{
    Task<ExplorerWebToken> AcquireTokenAsync(
        IReadOnlyList<string> scopes,
        CancellationToken cancellationToken = default);
}
```

Acquires a downstream State API token for the signed-in browser user. The default implementation passes the circuit's `ClaimsPrincipal` to Microsoft.Identity.Web explicitly (a remote Blazor Server circuit has no ambient `HttpContext`). It throws `ExplorerWebReauthRequiredException` when the browser session is not authenticated or when Microsoft.Identity.Web signals that interactive sign-in is required.

## `ExplorerWebToken`

A `readonly record struct` holding the acquired access token, its `ExpiresOn` instant, and the resolved `Username`.

## `EntraWebExplorerAuthMethod`

The public sealed `IExplorerAuthMethod` for the `entra` scheme. Registered scoped by `AddLatticeExplorerEntraWebAuth`; resolves the State API scope from `Scopes` or, when empty, from the advertised audience (appending `/.default` to a bare resource id), and wires token renewal so a re-auth-required signal latches the credential as revoked.

```csharp
public EntraWebExplorerAuthMethod(
    IExplorerWebTokenAcquirer acquirer,
    IOptionsMonitor<ExplorerEntraWebOptions> options)

public string SchemeId { get; }
public bool CanHandle(string advertisedScheme)
public Task<ExplorerAuthSignIn> ChallengeAsync(
    ExplorerAuthChallengeContext context,
    CancellationToken cancellationToken = default)
```

`SchemeId` returns the `entra` scheme id; `CanHandle` matches that scheme case-insensitively; `ChallengeAsync` acquires the initial downstream token and returns a bearer sign-in whose renewal delegate latches the credential as revoked on a re-auth-required signal.

## `ExplorerWebReauthRequiredException`

Thrown when the browser must complete (or repeat) the interactive OIDC sign-in before a State API token can be acquired.

```csharp
public ExplorerWebReauthRequiredException()
public ExplorerWebReauthRequiredException(string message)
public ExplorerWebReauthRequiredException(string message, Exception innerException)
```

A `sealed` exception deriving directly from `System.Exception` with the three standard constructors (default message, custom message, and message-plus-inner-exception).
