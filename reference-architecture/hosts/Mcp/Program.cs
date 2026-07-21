using Microsoft.AspNetCore.Authentication.JwtBearer;
using Orleans.Lattice;
using Orleans.Lattice.Api.Mcp;
using Orleans.Lattice.Api.Mcp.Telemetry;
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
// Every endpoint and credential comes from environment variables /
// IConfiguration; no secret is hardcoded.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

var stateEndpoint = config["Mcp:StateEndpoint"]
    ?? throw new InvalidOperationException("Mcp:StateEndpoint (the silo gRPC endpoint) is required.");
var authEndpoint = config["Mcp:AuthEndpoint"] ?? stateEndpoint;
var dataEndpoint = config["Mcp:DataEndpoint"];
var backupEndpoint = config["Mcp:BackupEndpoint"];

var entraEnabled = config.GetValue("Entra:Enabled", false);
var requireAuthorization = config.GetValue("Mcp:RequireAuthorization", entraEnabled);
var enableAuthAdministration = config.GetValue("Mcp:EnableAuthAdministration", false);
var enableDataWrites = config.GetValue("Mcp:EnableDataWrites", false);
var enableBackupControl = config.GetValue("Mcp:EnableBackupControl", false);

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
if (new[] { stateEndpoint, authEndpoint, dataEndpoint, backupEndpoint }
        .Any(e => e is not null && e.StartsWith("http://", StringComparison.OrdinalIgnoreCase)))
{
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
}

builder.Services.AddLatticeMcpRemote(options =>
{
    options.State = new LatticeApiMcpRemoteEndpoint { Endpoint = stateEndpoint };
    options.Auth = new LatticeApiMcpRemoteEndpoint { Endpoint = authEndpoint };
    if (!string.IsNullOrWhiteSpace(dataEndpoint))
    {
        options.Data = new LatticeApiMcpRemoteEndpoint { Endpoint = dataEndpoint };
    }

    if (!string.IsNullOrWhiteSpace(backupEndpoint))
    {
        options.Backup = new LatticeApiMcpRemoteEndpoint { Endpoint = backupEndpoint };
    }

    options.EnableDataWrites = enableDataWrites;
    options.EnableBackupControl = enableBackupControl;
    options.EnableAuthAdministration = enableAuthAdministration;
    if (!string.IsNullOrWhiteSpace(administratorToken))
    {
        options.AdministratorCredential = new LatticeCredential(administratorToken, administratorScheme);
    }
});

// The base MCP endpoint's fail-closed toggle. Mounted at the root of the HTTP
// transport (the SDK default); the liveness probe lives at /health.
builder.Services.AddLatticeMcp(options => options.RequireAuthorization = requireAuthorization);

// Optional cluster telemetry tools: proxy a read-only PromQL backend (the local
// compose Prometheus, or Azure Managed Prometheus) as MCP tools. Only wired when
// a backend address is configured, so an unset backend leaves the group off
// rather than failing options validation.
var telemetryBackend = config["Mcp:Telemetry:BackendAddress"];
if (!string.IsNullOrWhiteSpace(telemetryBackend))
{
    builder.Services.AddTelemetryTools(options => options.BackendAddress = new Uri(telemetryBackend));
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
        });
    builder.Services.AddAuthorization();
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
app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
