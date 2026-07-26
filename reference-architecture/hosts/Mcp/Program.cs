using Microsoft.AspNetCore.Authentication.JwtBearer;
using Azure.Identity;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Api.Mcp.Telemetry;
using Orleans.Lattice.Api.Mcp.Telemetry.Azure;
using Orleans.Lattice.ReferenceArchitecture.Hosting;

// ---------------------------------------------------------------------------
// Reference-architecture MCP head.
//
// A stateless Model Context Protocol server that fronts the region's silo
// cluster it is NOT co-hosted with, delegating each facade group to the silo's
// gRPC endpoint (AddLatticeMcpRemote). It exposes the read-only state facade and
// the auth-admin control plane as MCP tools, permission-scoped per caller, and
// - when a PromQL backend is configured - the cluster telemetry tools.
//
// Auth: the head is Entra-authenticated. When Entra is enabled the MCP HTTP
// endpoint validates the inbound Entra JWT (JwtBearer) and the same bearer token
// is forwarded to the silo's gRPC facades, which re-validate it against their
// own Entra authenticator - defense in depth across the front door and the data
// plane. The local compose harness disables Entra (a documented dev bypass) and
// leaves the endpoint open, relying on the silo's fail-closed discovery
// underneath.
//
// OAuth discovery: when Entra is on and the head's public URL (Mcp:PublicUrl) is
// configured, the endpoint serves an OAuth 2.0 Protected Resource Metadata
// document (RFC 9728) at /.well-known/oauth-protected-resource and hints it on
// 401 challenges, pointing a standard MCP client at the Entra authorization
// server and the silo scope so it can sign in itself (no pre-pasted token). The
// silo API pre-authorizes the well-known first-party MCP clients (VS Code, Visual
// Studio, Copilot) for that scope, so the sign-in needs no per-user consent.
//
// Every endpoint and credential comes from environment variables /
// IConfiguration; no secret is hardcoded.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Azure Front Door probes this head's /health endpoint (HEAD) continuously per
// edge PoP. Real MCP requests keep full logging; the framework "Request
// starting"/"Request finished" chatter on the probe path is dropped so it does
// not dominate log storage. Warnings/errors on /health still surface.
builder.Logging.SuppressHealthProbeRequestLogs();

var stateEndpoint = config["Mcp:StateEndpoint"]
    ?? throw new InvalidOperationException("Mcp:StateEndpoint (the silo gRPC endpoint) is required.");
var authEndpoint = config["Mcp:AuthEndpoint"] ?? stateEndpoint;
var dataEndpoint = config["Mcp:DataEndpoint"];
var backupEndpoint = config["Mcp:BackupEndpoint"];
var replicationEndpoint = config["Mcp:ReplicationEndpoint"];

// Multi-region targeting. The top-level endpoints above define the DEFAULT
// (current) region a tool call targets when no optional `region` selector is
// supplied; these name the current region for lattice_list_regions. Mcp:Regions
// (below) enumerates the peer regions a caller may additionally target. All three
// are unset in a single-region deployment, leaving the head unchanged.
var regionId = config["Mcp:RegionId"];
var clusterId = config["Mcp:ClusterId"];

// Assert a peer region actually reaches its advertised cluster before routing to
// it (probe its state facade once, compare the reported cluster id). This is the
// real guarantee behind a global load balancer: a peer accidentally pointed at an
// anycast/Front Door endpoint that latency-routes to the nearest region fails the
// assertion and is omitted / rejected fail-closed rather than silently answered by
// the wrong cluster. On by default here because the reference estate fronts every
// region with one global Front Door; peers are always dialed at their DIRECT
// region-pinned silo FQDN, so the assertion passes.
var verifyRegionIdentity = config.GetValue("Mcp:VerifyRegionIdentity", false);

var entraEnabled = config.GetValue("Entra:Enabled", false);
var requireAuthorization = config.GetValue("Mcp:RequireAuthorization", entraEnabled);
var enableAuthAdministration = config.GetValue("Mcp:EnableAuthAdministration", false);
var enableDataWrites = config.GetValue("Mcp:EnableDataWrites", false);
var enableBackupControl = config.GetValue("Mcp:EnableBackupControl", false);

// The replication control tool group (lattice_replication_*). The inspect tool
// (get_config) is contributed whenever the replication endpoint is wired;
// EnableReplicationControl additionally advertises the mutating enable / disable
// tools. Both default off so an unset replication endpoint leaves the group
// absent rather than surfacing an unreachable control plane.
var enableReplicationControl = config.GetValue("Mcp:EnableReplicationControl", false);

// Streamable-HTTP session mode. This head is documented and deployed as a
// STATELESS MCP server (see the file header): it fronts an active-active Front
// Door origin group with no session affinity, so a follow-up request can land on
// any region or replica. A stateful, in-memory session would then break with
// "Session not found" the moment routing moved. Stateless mode makes every HTTP
// request self-contained; the per-request ConfigureSessionOptions hook still
// runs (see HttpServerTransportOptions), so the permission-scoped tool discovery
// is applied on every call from the caller's own token rather than being lost.
// Defaults on for this multi-region host; the single-instance compose harness
// may set Mcp:Stateless=false to exercise the stateful path.
var stateless = config.GetValue("Mcp:Stateless", true);

// Global-ingress origin lock: when set, every request other than /health must
// carry an X-Azure-FDID header matching this id. Empty (dev/compose, and the
// first deploy pass before Front Door exists) leaves the head unlocked.
var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];

// The service credential the discovery core uses for the trusted, read-only
// permission introspection it performs on each non-administrator caller's
// behalf. Supplied via config (injected from Key Vault at deploy); a bare token
// with an optional scheme. Absent means only an administrator caller can
// enumerate tools remotely.
var administratorToken = config["Mcp:AdministratorToken"];
var administratorScheme = config["Mcp:AdministratorScheme"] ?? "Bearer";

// Allow HTTP/2 without TLS (h2c) only when a plaintext http:// gRPC endpoint is
// configured - the local compose harness dials the silo over h2c. Production
// endpoints are https:// and never trip this switch.
if (new[] { stateEndpoint, authEndpoint, dataEndpoint, backupEndpoint, replicationEndpoint }
        .Any(e => e is not null && e.StartsWith("http://", StringComparison.OrdinalIgnoreCase)))
{
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
}

// The silo enforces a Front Door origin lock on its external gRPC port: every
// request other than /health must carry an X-Azure-FDID header matching the
// front-door id, or it is rejected 403 before authentication. The MCP head dials
// the silo over its internal ACA FQDN (not through Front Door), so it must stamp
// that header itself on every outbound gRPC call - the introspection the
// discovery core performs AND the forwarded tool calls - or all silo calls 403
// and every facade group reports unavailable. Build one origin-lock call invoker
// per distinct silo address (state/auth/data share one FQDN) and hand it to the
// remote binding, which layers the caller-credential-forwarding interceptor on
// top. When no front-door id is configured (the local compose harness, whose
// silo is unlocked) the endpoints fall back to the binding's address-derived
// channel with no header.
var siloInvokerCache = new Dictionary<string, CallInvoker>(StringComparer.OrdinalIgnoreCase);
CallInvoker? OriginLockInvoker(string? endpoint)
{
    if (string.IsNullOrWhiteSpace(endpoint) || string.IsNullOrEmpty(frontDoorId))
    {
        return null;
    }

    if (!siloInvokerCache.TryGetValue(endpoint, out var invoker))
    {
        invoker = GrpcChannel.ForAddress(endpoint).CreateCallInvoker()
            .Intercept(metadata =>
            {
                metadata.Add("X-Azure-FDID", frontDoorId);
                return metadata;
            });
        siloInvokerCache[endpoint] = invoker;
    }

    return invoker;
}

// Build the peer (additional) regions a caller may target via the optional
// per-call `region` selector, from Mcp:Regions[]. Each peer facade group is dialed
// at the peer's DIRECT region-pinned silo gRPC FQDN (NOT the anycast Front Door
// endpoint - that would defeat region targeting and, with VerifyRegionIdentity on,
// be rejected) and wrapped with the SAME origin-lock invoker as the current region
// (the estate shares one Front Door id, and the peer silo enforces the same
// X-Azure-FDID lock). The peer's single silo endpoint serves every facade group,
// mirroring the current-region wiring above. The forwarded caller credential
// authorizes the call independently at the peer, fail-closed.
LatticeApiMcpRemoteEndpoint? RegionEndpoint(string? endpoint)
    => string.IsNullOrWhiteSpace(endpoint)
        ? null
        : new LatticeApiMcpRemoteEndpoint { Endpoint = endpoint, CallInvoker = OriginLockInvoker(endpoint) };

var peerRegions = new List<LatticeApiMcpRemoteRegionOptions>();
foreach (var regionSection in config.GetSection("Mcp:Regions").GetChildren())
{
    var peerRegionId = regionSection["RegionId"];
    if (string.IsNullOrWhiteSpace(peerRegionId))
    {
        continue;
    }

    var peerState = regionSection["StateEndpoint"];
    var peerAuth = regionSection["AuthEndpoint"] ?? peerState;
    peerRegions.Add(new LatticeApiMcpRemoteRegionOptions
    {
        RegionId = peerRegionId,
        ClusterId = regionSection["ClusterId"],
        State = RegionEndpoint(peerState),
        Auth = RegionEndpoint(peerAuth),
        Data = RegionEndpoint(regionSection["DataEndpoint"]),
        Backup = RegionEndpoint(regionSection["BackupEndpoint"]),
        Replication = RegionEndpoint(regionSection["ReplicationEndpoint"]),
    });
}

builder.Services.AddLatticeMcpRemote(options =>
{
    if (!string.IsNullOrWhiteSpace(regionId))
    {
        options.RegionId = regionId;
    }

    options.ClusterId = clusterId;
    options.VerifyRegionIdentity = verifyRegionIdentity;
    foreach (var peer in peerRegions)
    {
        options.Regions.Add(peer);
    }

    options.State = new LatticeApiMcpRemoteEndpoint { Endpoint = stateEndpoint, CallInvoker = OriginLockInvoker(stateEndpoint) };
    options.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = authEndpoint, CallInvoker = OriginLockInvoker(authEndpoint) };
    if (!string.IsNullOrWhiteSpace(dataEndpoint))
    {
        options.Data = new LatticeApiMcpRemoteEndpoint { Endpoint = dataEndpoint, CallInvoker = OriginLockInvoker(dataEndpoint) };
    }

    if (!string.IsNullOrWhiteSpace(backupEndpoint))
    {
        options.Backup = new LatticeApiMcpRemoteEndpoint { Endpoint = backupEndpoint, CallInvoker = OriginLockInvoker(backupEndpoint) };
    }

    if (!string.IsNullOrWhiteSpace(replicationEndpoint))
    {
        options.Replication = new LatticeApiMcpRemoteEndpoint { Endpoint = replicationEndpoint, CallInvoker = OriginLockInvoker(replicationEndpoint) };
    }

    options.EnableDataWrites = enableDataWrites;
    options.EnableBackupControl = enableBackupControl;
    options.EnableReplicationControl = enableReplicationControl;
    options.EnableAuthAdministration = enableAuthAdministration;
    if (!string.IsNullOrWhiteSpace(administratorToken))
    {
        options.AdministratorCredential = new LatticeCredential(administratorToken, administratorScheme);
    }
});

// The base MCP endpoint's fail-closed toggle. Mounted at the root of the HTTP
// transport (the SDK default); the liveness probe lives at /health.
builder.Services.AddLatticeMcp(options =>
{
    options.RequireAuthorization = requireAuthorization;
    options.Stateless = stateless;
});

// Coarse transport authorizer. AddLatticeMcp installs a fail-closed
// DenyAllMcpAuthorizer that rejects every tool until a host opts in, so without
// this registration only the lattice_capabilities meta-tool is ever advertised.
// Opt into the permissive coarse gate: the real, subject-scoped enforcement is
// layered underneath - the endpoint still requires an authenticated Entra
// principal (RequireAuthorization), discovery only advertises the groups the
// caller holds authored grants for, and every forwarded call is re-authorized
// against the silo facade's own default-deny per-subject rules. The coarse gate
// is therefore the transport allow; the silo remains the source of truth.
builder.Services.AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>();

// Optional cluster telemetry tools: proxy a read-only PromQL backend (the local
// compose Prometheus, or Azure Monitor managed Prometheus) as MCP tools. Only
// wired when a backend address is configured, so an unset backend leaves the
// group off rather than failing options validation.
var telemetryBackend = config["Mcp:Telemetry:BackendAddress"];
if (!string.IsNullOrWhiteSpace(telemetryBackend))
{
    // Backend auth mode. The local compose Prometheus is unauthenticated (None,
    // the default); Azure Monitor managed Prometheus requires a rotating Entra
    // access token (DynamicBearer), minted from the workload's managed identity
    // by the Azure companion provider - no static secret is stored or forwarded.
    var telemetryAuthMode = config.GetValue("Mcp:Telemetry:AuthMode", LatticeTelemetryBackendAuthMode.None);

    builder.Services.AddTelemetryTools(options =>
    {
        options.BackendAddress = new Uri(telemetryBackend);
        options.AuthMode = telemetryAuthMode;
    });

    if (telemetryAuthMode == LatticeTelemetryBackendAuthMode.DynamicBearer)
    {
        // DefaultAzureCredential resolves the region's user-assigned managed
        // identity in ACA via AZURE_CLIENT_ID. The credential never leaves this
        // provider; the core telemetry package only ever sees the bearer token it
        // mints for the managed-Prometheus scope. An override scope is honoured
        // for a non-default Azure Monitor audience.
        builder.Services.AddAzureTelemetryBackendToken(options =>
        {
            options.Credential = new DefaultAzureCredential();
            var scope = config["Mcp:Telemetry:Scope"];
            if (!string.IsNullOrWhiteSpace(scope))
            {
                options.Scope = scope;
            }
        });
    }
}

// Entra JWT validation on the front door (defense in depth; the forwarded token
// is re-validated at the silo). Disabled for the local dev bypass.
if (entraEnabled)
{
    var tenantId = config["Entra:TenantId"]
        ?? throw new InvalidOperationException("Entra:TenantId is required when Entra:Enabled is true.");
    var authority = config["Entra:Authority"] ?? $"https://login.microsoftonline.com/{tenantId}/v2.0";
    var audience = config["Entra:Audience"] ?? config["Entra:ClientId"];

    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            options.Authority = authority;
            options.Audience = audience;

            // Log the identity behind every accepted (and rejected) token so an
            // operator can wire up a new MCP client without guesswork. The client
            // id (azp on v2.0 tokens, appid on v1.0) is exactly what goes into the
            // Bicep preAuthorizedMcpClientIds list to pre-consent a new client
            // (for example the GitHub Copilot app, whose first-party id is not
            // otherwise easy to obtain); the subject oid is what an admin grants
            // MCP access to via the Explorer Access tab.
            options.Events = new JwtBearerEvents
            {
                OnTokenValidated = context =>
                {
                    var principal = context.Principal;
                    var clientId = principal?.FindFirst("azp")?.Value
                        ?? principal?.FindFirst("appid")?.Value
                        ?? "(none)";
                    var appName = principal?.FindFirst("app_displayname")?.Value ?? "(none)";
                    var subjectOid = principal?.FindFirst("oid")?.Value
                        ?? principal?.FindFirst("http://schemas.microsoft.com/identity/claims/objectidentifier")?.Value
                        ?? "(none)";
                    context.HttpContext.RequestServices
                        .GetRequiredService<ILoggerFactory>()
                        .CreateLogger("Mcp.Auth")
                        .LogInformation(
                            "MCP token accepted: clientId={ClientId} appName={AppName} subjectOid={SubjectOid}",
                            clientId, appName, subjectOid);
                    return Task.CompletedTask;
                },
                OnAuthenticationFailed = context =>
                {
                    context.HttpContext.RequestServices
                        .GetRequiredService<ILoggerFactory>()
                        .CreateLogger("Mcp.Auth")
                        .LogWarning(context.Exception, "MCP token rejected: {Reason}", context.Exception.Message);
                    return Task.CompletedTask;
                },
            };
        });
    builder.Services.AddAuthorization();

    // OAuth 2.0 Protected Resource Metadata (RFC 9728) discovery. When this head's
    // own public URL is configured, advertise an anonymous metadata document at
    // /.well-known/oauth-protected-resource and append a resource_metadata hint to
    // the endpoint's 401 bearer challenge, so a spec-compliant MCP client can
    // discover the Entra authorization server and run the sign-in flow itself
    // instead of needing a pre-acquired token pasted into the client. This is only
    // meaningful when Entra is on (there is an authorization server to point at);
    // the open local compose harness advertises nothing. Layered as a follow-up
    // AddLatticeMcp so it composes with the base registration above; MapLatticeMcp
    // reads the options at map time and serves the well-known document.
    var mcpPublicUrl = config["Mcp:PublicUrl"];
    if (!string.IsNullOrWhiteSpace(mcpPublicUrl))
    {
        var metadata = new LatticeApiMcpProtectedResourceMetadata
        {
            Resource = new Uri(mcpPublicUrl, UriKind.Absolute),
        };
        metadata.AuthorizationServers.Add(new Uri(authority, UriKind.Absolute));

        // The scopes a client should request so the resulting access token's
        // audience matches what this head validates and forwards to the silo. In
        // the reference architecture this is the silo facade's delegated scope
        // (api://{tenant}/{base}-silo/user_impersonation). Space- or
        // comma-separated; omitted from the document when empty.
        foreach (var scope in (config["Mcp:Oauth:Scopes"] ?? string.Empty)
                     .Split(new[] { ' ', ',' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            metadata.ScopesSupported.Add(scope);
        }

        builder.Services.AddLatticeMcp(mcp => mcp.ProtectedResourceMetadata = metadata);
    }
}

var app = builder.Build();

// Enforce the Front Door origin lock before authentication and endpoint routing;
// /health (the platform liveness probe, reached directly) is exempt.
app.UseFrontDoorOriginLock(frontDoorId);

if (entraEnabled)
{
    app.UseAuthentication();
    app.UseAuthorization();
}

app.MapLatticeMcp();

// Front Door health-probes this path with HEAD (see frontdoor.bicep mcpProbePath);
// map both verbs so the probe gets 200 rather than falling through to the MCP
// transport at `/` and being answered 405. Anonymous: the MCP host installs no
// fallback authorization policy, and the origin lock exempts /health.
app.MapMethods("/health", ["GET", "HEAD"], () => Results.Ok("healthy"));

app.Run();
