using Azure.Identity;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Identity.Abstractions;
using Orleans.Lattice.Caching.AzureBlob;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Entra.Web;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.ReferenceArchitecture.Explorer;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

// ---------------------------------------------------------------------------
// Reference-architecture Explorer head.
//
// A standalone operator web console (Blazor Server) that is NOT co-hosted with a
// silo: it connects, as a gRPC / gRPC-web client, to a remote State + Auth gRPC
// endpoint published by the region's silo. It is the same embeddable code path
// the Orleans.Lattice.Explorer.Web hosting library exposes, driven here purely by
// configuration so the standalone head and any co-hosted explorer cannot drift.
//
// The console's first-run connection is seeded by the explorer's own environment
// bootstrap (LATTICE_EXPLORER_ENDPOINT / LATTICE_EXPLORER_INSECURE_DEV /
// LATTICE_EXPLORER_USERNAME / LATTICE_EXPLORER_PASSWORD / LATTICE_EXPLORER_CONFIG),
// so the remote endpoint and any auto-sign-in are configuration, never code.
//
// Auth: the console offers a hosted-web Microsoft Entra sign-in (OpenID Connect,
// auth-code + PKCE) when Entra is enabled (AddLatticeExplorerEntraWebAuth). The
// browser signs in against the console's OWN confidential web-app registration; a
// downstream State API token is acquired on the user's behalf and re-validated by
// the silo's Entra authenticator. Because this is a multi-replica Blazor Server
// head, the token cache is a distributed cache over the region storage account so
// tokens are shared across warm replicas and survive restart, and the confidential
// client authenticates secret-lessly via a federated managed-identity assertion.
// The local compose harness disables Entra (a documented dev bypass) and uses the
// built-in Basic sign-in against the dev cluster.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Drop the informational framework request-log spam produced by the platform
// /health probe (Front Door + Container Apps probe it several times a second per
// replica). Real requests keep full logging; warnings/errors on the health path
// still surface.
builder.Logging.SuppressHealthProbeRequestLogs();

// Serve the Explorer UI's packaged static web assets in every environment (the
// framework only auto-maps these in Development).
builder.WebHost.UseStaticWebAssets();

var entraEnabled = config.GetValue("Entra:Enabled", false);

// Global-ingress origin lock: when set, every request other than /health must
// carry an X-Azure-FDID header matching this id. Empty (dev/compose, and the
// first deploy pass before Front Door exists) leaves the head unlocked. The
// browser reaches the console through Front Door, so its asset and SignalR
// requests carry the header; only the platform /health probe is exempt.
var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];

// Persist the explorer's JSON config to a writable location. The chiseled,
// non-root container has no writable per-user app-data directory, so default the
// backing store to a writable path (overridable via Explorer:ConfigFilePath or
// the LATTICE_EXPLORER_CONFIG environment variable).
var configFilePath = config["Explorer:ConfigFilePath"]
    ?? Path.Combine(Path.GetTempPath(), "lattice-explorer", "config.json");
Directory.CreateDirectory(Path.GetDirectoryName(configFilePath)!);

builder.Services.AddLatticeExplorerWeb(options =>
{
    options.ConfigFilePath = configFilePath;
});

// The Schema management area is an opt-in plugin: registering it is the whole of
// the opt-in, and a head that does not register it renders no Schema tab at all.
// Kept behind the same configuration switch this head has always exposed, and
// still off by default because the versioning UI cannot yet express what differs
// between schema versions.
if (config.GetValue("Explorer:EnableSchemaArea", false))
{
    builder.Services.AddExplorerSchemaPlugin();
}

// Hosted-web Entra (OpenID Connect) sign-in provider, offered alongside the
// built-in Basic provider when Entra is enabled.
if (entraEnabled)
{
    var tenantId = config["Entra:TenantId"]
        ?? throw new InvalidOperationException("Entra:TenantId is required when Entra:Enabled is true.");
    // The Explorer console's OWN confidential web-app registration (the app that
    // holds the OIDC redirect URIs), NOT the silo State API resource app whose
    // audience the facades validate.
    var webClientId = config["Entra:WebClientId"]
        ?? throw new InvalidOperationException(
            "Entra:WebClientId is required when Entra:Enabled is true (the Explorer console's own Entra application id).");

    // Optional explicit downstream scope(s) for the silo State API (for example
    // api://{tenantId}/{base}-silo/user_impersonation). When empty the provider
    // resolves the scope at sign-in from the audience the State API advertises.
    var scopes = (config["Entra:Scopes"] ?? string.Empty)
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    // Secret-less confidential-client credential: in Azure the container runs as a
    // user-assigned managed identity federated (workload-identity federation) to
    // the Explorer app registration, so Microsoft.Identity.Web authenticates the
    // client with a managed-identity signed assertion - no client secret. A secret
    // is only a fallback for non-Azure hosts that set Entra:ClientSecret.
    var managedIdentityClientId = config["AZURE_CLIENT_ID"];
    var clientSecret = config["Entra:ClientSecret"];

    // Distributed token cache over the per-region storage account so a signed-in
    // operator's token is shared across warm replicas and survives restart. Empty
    // (dev/compose) falls back to an in-memory cache.
    var tokenCacheBlobUri = config["Entra:TokenCache:BlobServiceUri"];
    var useDistributedTokenCache = !string.IsNullOrWhiteSpace(tokenCacheBlobUri);
    if (useDistributedTokenCache)
    {
        var containerName = config["Entra:TokenCache:ContainerName"] ?? "explorer-token-cache";
        builder.Services.AddAzureBlobDistributedCache(cacheOptions =>
        {
            cacheOptions.ServiceUri = new Uri(tokenCacheBlobUri!);
            // DefaultAzureCredential resolves the region's user-assigned managed
            // identity (selected by AZURE_CLIENT_ID) - keyless, container-scoped
            // Storage Blob Data Contributor RBAC on the token-cache container.
            cacheOptions.TokenCredential = new DefaultAzureCredential();
            cacheOptions.ContainerName = containerName;
        });
    }

    builder.Services.AddLatticeExplorerEntraWebAuth(options =>
    {
        options.TenantId = tenantId;
        options.ClientId = webClientId;
        options.ClientSecret = clientSecret;
        foreach (var scope in scopes)
        {
            options.Scopes.Add(scope);
        }
        options.TokenCache = useDistributedTokenCache
            ? ExplorerWebTokenCacheKind.Distributed
            : ExplorerWebTokenCacheKind.InMemory;

        if (string.IsNullOrWhiteSpace(clientSecret) && !string.IsNullOrWhiteSpace(managedIdentityClientId))
        {
            options.ConfigureMicrosoftIdentityOptions = identity =>
            {
                identity.ClientCredentials = new[]
                {
                    new CredentialDescription
                    {
                        SourceType = CredentialSource.SignedAssertionFromManagedIdentity,
                        ManagedIdentityClientId = managedIdentityClientId,
                    },
                };
            };
        }
    });
}
else
{
    // Local dev bypass (Entra disabled): the console has no identity provider, so a
    // stock anonymous state-API connection is fail-closed by the silo's
    // state-visibility filter - the tree catalog comes back empty and the Access
    // area is denied. Replace the built-in Basic auth method with one that signs
    // the console in as the configured bootstrap administrator by forwarding
    // `Bearer <subject>`, exactly the credential the silo's
    // DevBypassCredentialAuthenticator trusts (and the MCP head already forwards).
    // Driven by the LATTICE_EXPLORER_USERNAME sign-in seed so it auto-applies on
    // first load with no dialog. Registered ONLY when Entra is disabled, so it can
    // never coexist with, or weaken, a real deployment's Entra sign-in.
    builder.Services.RemoveAll<IExplorerAuthMethod>();
    builder.Services.AddSingleton<IExplorerAuthMethod, DevBypassExplorerAuthMethod>();
}

var app = builder.Build();

// Behind Front Door + Container Apps the platform terminates TLS at the edge and
// forwards plain HTTP to the container under the origin's OWN host name, so the
// framework would otherwise build sign-in redirect URIs as
// http://<internal-origin>/signin-oidc - a scheme Entra rejects and a host that
// bypasses the Front-Door-locked origin (the browser's auth callback would hit
// the origin lock directly and 403). Pin the externally visible scheme and host
// to the known public origin so OpenID Connect emits, and the code-for-token
// exchange replays, the correct https://<front-door-host>/signin-oidc redirect
// URI. Empty (dev/compose) leaves the request untouched.
var publicOrigin = config["Explorer:PublicOrigin"];
if (!string.IsNullOrWhiteSpace(publicOrigin))
{
    var origin = new Uri(publicOrigin, UriKind.Absolute);
    var forwardedScheme = origin.Scheme;
    // Omit the default port: HostString.FromUriComponent would emit an explicit
    // :443, but the registered OIDC redirect URI has no port, and Entra matches
    // redirect URIs exactly (a :443 mismatch fails with AADSTS50011).
    var forwardedHost = origin.IsDefaultPort
        ? new HostString(origin.Host)
        : new HostString(origin.Host, origin.Port);
    app.Use((context, next) =>
    {
        context.Request.Scheme = forwardedScheme;
        context.Request.Host = forwardedHost;
        return next();
    });
}

// Enforce the Front Door origin lock before any request processing; /health
// (the platform liveness probe, reached directly) is exempt.
app.UseFrontDoorOriginLock(frontDoorId);

// OpenID Connect authentication + the fallback "require authenticated user"
// policy that challenges anonymous requests into the sign-in redirect. Only wired
// when Entra is enabled (compose/dev runs anonymous with Basic sign-in).
if (entraEnabled)
{
    app.UseAuthentication();
    app.UseAuthorization();
}

// TLS is terminated at the platform ingress (ACA / the compose front); the
// container itself serves plain HTTP so health probes and the SignalR circuit do
// not hit an in-container HTTPS redirect. No UseHttpsRedirection here by design.
app.UseAntiforgery();

app.MapLatticeExplorer();

// Browser sign-out endpoint (clears the OIDC cookie and signs out of Entra),
// distinct from the explorer's own State API sign-out.
if (entraEnabled)
{
    app.MapLatticeExplorerEntraWebSignOut();
}

// The platform liveness probe must bypass the fallback authenticated-user policy.
// Front Door health probes issue HEAD (Container Apps uses GET), so both verbs are
// mapped: a HEAD that only matched a GET endpoint would fall through to the
// authenticated-user fallback and be redirected (302) to sign-in, which both fails
// the "expect 200" probe contract and floods the log with auth-challenge noise.
app.MapMethods("/health", ["GET", "HEAD"], () => Results.Ok("healthy")).AllowAnonymous();

app.Run();
