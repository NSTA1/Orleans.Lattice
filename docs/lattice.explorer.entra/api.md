# Orleans.Lattice.Explorer.Entra API reference

The package adds an interactive MSAL-backed `entra` authentication method to
Explorer Core. Its public surface consists of a registration extension, the
provider options, the auth method, a token-acquisition seam and the request and
result records. The public MSAL acquirer is the default production implementation;
its in-memory cache is local to the provider's DI scope.

## Registration

`ExplorerEntraServiceCollectionExtensions.AddExplorerEntraAuth` has this
signature:

```text
IServiceCollection AddExplorerEntraAuth(
    IServiceCollection services,
    Action<ExplorerEntraOptions>? configure = null)
```

It adds the options, scoped `IEntraInteractiveTokenAcquirer` and scoped
`IExplorerAuthMethod` registrations. It does not register Core's auth session;
call `AddExplorerAuth()` as well. A null service collection throws
`ArgumentNullException`.

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Entra;

var services = new ServiceCollection();
services.AddExplorerAuth();
services.AddExplorerEntraAuth(options =>
{
    options.Authority = "https://login.microsoftonline.com/tenant-id";
    options.ClientId = "public-client-id";
    options.Scopes.Add("api://state-api/.default");
});
```

## `IEntraInteractiveTokenAcquirer`

| Member | Signature | Behaviour |
|---|---|---|
| `AcquireInteractiveAsync` | `Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)` | Starts browser auth-code with PKCE, or device-code when requested. |
| `AcquireSilentAsync` | `Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)` | Renews from cached account state; returns `null` when another interactive sign-in is required. |

## `EntraExplorerAuthMethod`

The public constructor is
`EntraExplorerAuthMethod(IEntraInteractiveTokenAcquirer acquirer, IOptionsMonitor<ExplorerEntraOptions> options)`.
It implements `IExplorerAuthMethod`:

| Member | Behaviour |
|---|---|
| `SchemeId` | Returns the `entra` scheme id. |
| `CanHandle(string advertisedScheme)` | Matches `entra` case-insensitively. |
| `ChallengeAsync(ExplorerAuthChallengeContext context, CancellationToken cancellationToken = default)` | Resolves authority, client id and scopes from configured options first and the endpoint advertisement only for unset values; acquires the first token and returns a bearer sign-in with renewal bound to that account. |

Advertised authorities must be absolute HTTPS URLs on an admitted authority
host. Advertised audiences must be `api://` resources, HTTPS resources on the
State API endpoint host, or exact matches in `AllowedAudiences`. Static scopes
and authority/client settings take precedence over advertised values.

## Request and result records

`EntraTokenRequest` is a public sealed record. It carries the resolved request:
`Authority` (`required string`), `ClientId` (`required string`), `Scopes`
(`required IReadOnlyList<string>`), `UseDeviceCode` (`bool`, default `false`)
and `Username` (`string?`, default `null`; used to bind silent renewal to the
account that signed in).

`EntraTokenResult` is a public readonly record struct. Its public init-only
properties are `AccessToken` (`required string`), `ExpiresOn` (`required
DateTimeOffset`) and `Username` (`string`, empty when the account name is
unknown). Token and refresh material remain in memory; the Explorer configuration
store is not a token cache.

## `MsalEntraInteractiveTokenAcquirer`

The public constructor is
`MsalEntraInteractiveTokenAcquirer(IOptions<ExplorerEntraOptions> options)`.
It implements both `IEntraInteractiveTokenAcquirer` methods using MSAL's public
client: interactive or device-code acquisition followed by silent renewal. When
a request names a username, renewal selects that account and returns `null`
instead of switching to another cached identity if it is missing.

The complete `ExplorerEntraOptions` property set and exact defaults are in
[Configuration](configuration.md).

## Complete exported symbol index

This index lists every effectively public type and each member declared directly on that type in the current package builds. Overloaded members are listed once with their overload count. The preceding sections describe the supported integration contracts; feature-specific UI behavior is documented in the linked area guides.

### Orleans.Lattice.Explorer.Entra

#### `Orleans.Lattice.Explorer.Entra` (7 exported types)
- `Orleans.Lattice.Explorer.Entra.EntraExplorerAuthMethod`
  - `constructors (1)`; `CanHandle`; `ChallengeAsync`; `SchemeId`
- `Orleans.Lattice.Explorer.Entra.EntraTokenRequest`
  - `constructors (1)`; `<Clone>$`; `Authority`; `ClientId`; `Equals (2 overloads)`; `GetHashCode`; `op_Equality`; `op_Inequality`; `Scopes`
    `ToString`; `UseDeviceCode`; `Username`
- `Orleans.Lattice.Explorer.Entra.EntraTokenResult`
  - `AccessToken`; `Equals (2 overloads)`; `ExpiresOn`; `GetHashCode`; `op_Equality`; `op_Inequality`; `ToString`; `Username`
- `Orleans.Lattice.Explorer.Entra.ExplorerEntraOptions`
  - `constructors (1)`; `AllowedAudiences`; `AllowedAuthorityHosts`; `Authority`; `ClientId`; `DeviceCodeCallback`; `Scopes`; `TenantId`
    `UseDeviceCode`
- `Orleans.Lattice.Explorer.Entra.ExplorerEntraServiceCollectionExtensions`
  - `AddExplorerEntraAuth`
- `Orleans.Lattice.Explorer.Entra.IEntraInteractiveTokenAcquirer`
  - `AcquireInteractiveAsync`; `AcquireSilentAsync`
- `Orleans.Lattice.Explorer.Entra.MsalEntraInteractiveTokenAcquirer`
  - `constructors (1)`; `AcquireInteractiveAsync`; `AcquireSilentAsync`


## See also

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Explorer auth integration](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md)
- [Adding a custom auth method](../lattice.explorer/adding-a-custom-auth-method.md)
