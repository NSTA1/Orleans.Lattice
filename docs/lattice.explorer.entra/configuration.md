# Orleans.Lattice.Explorer.Entra configuration

Configure `ExplorerEntraOptions` with
`AddExplorerEntraAuth(configure)`. These values are public OIDC parameters;
the package has no client-secret option. Configured values take precedence over
a State API auth-scheme advertisement; the advertisement fills only unset
values.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `Authority` | `string?` | `null` | OIDC authority. Takes precedence over `TenantId`; when neither is set, an admitted advertised authority may supply it. |
| `TenantId` | `string?` | `null` | Directory tenant id used to compose `https://login.microsoftonline.com/{tenant}` when `Authority` is unset. |
| `ClientId` | `string?` | `null` | Public client application id. An advertised client id is used only when this is unset. |
| `Scopes` | `IList<string>` | Empty | Requested State API scopes. If empty, the method resolves an admitted advertised audience and adds `/.default` unless already present. |
| `AllowedAuthorityHosts` | `IList<string>` | Empty | Replaces the built-in Entra login-host set for advertised authorities. Configured `Authority` or `TenantId` does not use this list. |
| `AllowedAudiences` | `IList<string>` | Empty | Replaces the default admission rule for an advertised audience when `Scopes` is empty. The default accepts `api://` resources and HTTPS resources on the State API endpoint host. |
| `UseDeviceCode` | `bool` | `false` | Uses device-code flow for initial token acquisition instead of the interactive browser flow. |
| `DeviceCodeCallback` | `Func<string, CancellationToken, Task>?` | `null` | Receives the device-code prompt. The default acquirer writes it to standard output when no callback is supplied. |

`AddExplorerEntraAuth` registers the acquirer and auth method as scoped services.
Options are not required at registration time: when authority, client id or
scope is absent, the method can use endpoint-advertised values. A challenge
fails with `InvalidOperationException` if it still cannot resolve an authority,
client id or at least one scope.

## Authority and audience admission

An endpoint advertisement is a hint, not trusted configuration. When it supplies
the authority, that value must be an absolute HTTPS URL whose host is admitted.
The default authority-host set is `login.microsoftonline.com`,
`login.microsoftonline.us`, `login.partner.microsoftonline.cn` and
`login.microsoftonline.de`. A non-empty `AllowedAuthorityHosts` list replaces
that set.

When scopes are empty, the advertised audience is accepted by default only when
it is an `api://` resource or an HTTPS resource sharing the configured State API
endpoint's host. A non-empty `AllowedAudiences` list replaces that rule with
case-insensitive exact matches. If the audience already ends in `/.default`, it
is used as the scope; otherwise that suffix is appended.

## Example

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

## See also

- [API reference](api.md)
- [Architecture](architecture.md)
- [Connecting to an auth-enabled State API](../lattice.explorer/connecting-to-an-auth-enabled-state-api.md)
