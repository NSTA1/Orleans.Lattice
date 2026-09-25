# Orleans.Lattice.Explorer.Entra

Microsoft Entra ID (Azure AD) interactive login provider for the Orleans.Lattice Explorer.

This optional package adds an Entra sign-in method to the explorer. When you connect to a State API that advertises the `entra` auth scheme, the explorer runs an interactive OIDC sign-in (auth-code + PKCE, or device-code for headless hosts), acquires a bearer token for the configured audience, and attaches it to every State API call. The token is refreshed silently and transparently before it expires, so a signed-in session is not interrupted while a refresh is still possible.

The MSAL dependency lives here, not in the core explorer, so hosts that only use Basic auth do not pay for it.

## Usage

```csharp
services.AddExplorerAuth();
services.AddExplorerEntraAuth(options =>
{
    options.Authority = "https://login.microsoftonline.com/<tenant>";
    options.ClientId = "<public-client-id>";
    options.Scopes.Add("api://<state-api-app-id>/.default");
});
```

When the State API advertises its Entra authority, tenant, client id, and audience, the static options can be omitted: an advertised value is used only for what is left unset, so a configured value always takes precedence. Because the advertisement arrives over an unauthenticated call from the endpoint that will receive the token, an advertised authority is admitted only when it is `https` and its host is allow-listed: the well-known Entra login hosts by default, or exactly the hosts in `AllowedAuthorityHosts` when that list is non-empty (a non-empty list replaces the default set).

See the Orleans.Lattice documentation for connecting to an auth-enabled State API.
