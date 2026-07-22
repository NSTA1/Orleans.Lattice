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

## `ExplorerEntraWebEndpointRouteBuilderExtensions`

```csharp
public const string DefaultSignOutPattern = "/explorer-entra/signout";

public static IEndpointConventionBuilder MapLatticeExplorerEntraWebSignOut(
    this IEndpointRouteBuilder endpoints,
    string pattern = DefaultSignOutPattern,
    string redirectUri = "/")
```

Maps a sign-out endpoint that clears the OpenID Connect cookie and signs the user out of Entra, redirecting to `redirectUri` afterwards. This is distinct from the Explorer's own State API sign-out, which only drops the API credential. Throws `ArgumentNullException` when `endpoints` is null and `ArgumentException` when `pattern` is blank.

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

## `ExplorerWebReauthRequiredException`

Thrown when the browser must complete (or repeat) the interactive OIDC sign-in before a State API token can be acquired.
