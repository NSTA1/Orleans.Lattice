# Orleans.Lattice.Explorer.Entra.Web

Microsoft Entra ID hosted-web (OpenID Connect) sign-in for the Orleans.Lattice
Explorer web console.

The interactive desktop provider (`Orleans.Lattice.Explorer.Entra`) runs a
browser flow on the machine that hosts the UI. That cannot work for a remotely
hosted Blazor Server console: the circuit runs on the server, which has no local
browser. This package serves that topology instead.

It wires:

- ASP.NET Core OpenID Connect (auth-code + PKCE) and a cookie session through
  Microsoft.Identity.Web, with an optional fallback authorization policy that
  challenges unauthenticated requests into the sign-in redirect.
- A scoped `IExplorerAuthMethod` for the `entra` scheme that exchanges the
  signed-in browser session for a downstream State API bearer token (acquired for
  the circuit's authenticated user) and refreshes it silently.
- An optional best-effort circuit handler that completes the State API sign-in
  automatically, so an already browser-authenticated user connects without a
  manual click.

## Quick start

```csharp
builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = builder.Configuration["Explorer:Entra:TenantId"];
    options.ClientId = builder.Configuration["Explorer:Entra:ClientId"];
    // Leave ClientSecret unset and attach a federated managed-identity
    // credential via ConfigureMicrosoftIdentityOptions for a secret-less host.
});
```

On a multi-replica host select the distributed token cache and register a shared
`IDistributedCache` (for example `Orleans.Lattice.Caching.AzureBlob`):

```csharp
builder.Services.AddLatticeExplorerEntraWebAuth(options =>
{
    options.TenantId = tenantId;
    options.ClientId = clientId;
    options.TokenCache = ExplorerWebTokenCacheKind.Distributed;
});
```

See the [package documentation](https://github.com/NSTA1/Orleans.Lattice/tree/main/docs/lattice.explorer.entra.web)
for configuration and the full sign-in flow.
