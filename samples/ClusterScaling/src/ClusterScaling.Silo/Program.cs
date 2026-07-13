using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Samples.ClusterScaling.Silo;
using Orleans.Lattice.Scaling;
using Orleans.Lattice.Storage.AzureTable;

// ---------------------------------------------------------------------------
// ClusterScaling - a deployable Azure Container Apps (ACA) silo host that proves
// the Orleans.Lattice.Scaling autoscaling signal drives KEDA replica scale-out on
// the COMPUTE axis.
//
// One container image, run as many ACA replicas (a genuine multi-silo Orleans
// cluster). Each replica:
//   1. Joins the cluster over REAL Azure Storage clustering (managed identity).
//   2. Persists grain state + the Lattice WAL to Azure Table storage (managed
//      identity) so scale-out means something - no in-memory / localhost storage.
//   3. Co-hosts the write-capable data-API gRPC surface (guarded by a Basic
//      admin credential whose salted PBKDF2 hash arrives as an ACA secret) and
//      the /lattice/scale HTTP signal endpoint the ACA KEDA metrics-api scale
//      rule scrapes.
//
// Nothing here redefines the scaling endpoint or the scale-rule contract (that
// is Orleans.Lattice.Scaling / issue #1188); this host only wires a concrete
// deployment around them.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);

// --- Configuration (all injected by deploy.ps1 / the ACA container app) ------
var tableUri = RequireEnv("CLUSTERSCALING_TABLE_URI");            // https://<acct>.table.core.windows.net
var clusterId = EnvOrDefault("CLUSTERSCALING_CLUSTER_ID", "clusterscaling");
var serviceId = EnvOrDefault("CLUSTERSCALING_SERVICE_ID", "clusterscaling");
var clusteringTable = EnvOrDefault("CLUSTERSCALING_CLUSTERING_TABLE", "OrleansClustering");
var grainTable = EnvOrDefault("CLUSTERSCALING_GRAIN_TABLE", "OrleansGrainState");
var reminderTable = EnvOrDefault("CLUSTERSCALING_REMINDER_TABLE", "OrleansReminders");
var walTable = EnvOrDefault("CLUSTERSCALING_WAL_TABLE", AzureTableWalStorageOptions.DefaultTableName);
var httpPort = EnvIntOrDefault("CLUSTERSCALING_HTTP_PORT", 8080);  // ACA ingress target port
var siloPort = EnvIntOrDefault("CLUSTERSCALING_SILO_PORT", 11111); // Orleans silo-to-silo
var gatewayPort = EnvIntOrDefault("CLUSTERSCALING_GATEWAY_PORT", 30000);

// A single TableServiceClient authenticated with the container's managed
// identity (DefaultAzureCredential resolves the ACA user-assigned identity).
// Every Azure Storage dependency - clustering, grain state, reminders, WAL -
// shares it, so no key or connection string is ever read.
var credential = new DefaultAzureCredential();
var tableServiceClient = new TableServiceClient(new Uri(tableUri), credential);

builder.Logging.AddSimpleConsole(o => o.SingleLine = true);

// ACA terminates external TLS at its managed ingress and forwards to the
// container as CLEARTEXT HTTP/2 - the ingress 'transport: http2' the bicep sets
// talks h2c (HTTP/2 prior knowledge) to the backend. On a plaintext port Kestrel
// cannot ALPN-negotiate the protocol (ALPN needs TLS), so Http1AndHttp2 would
// silently downgrade to HTTP/1.1 only and the ingress's h2c connection preface
// would be rejected (the caller sees 'upstream connect error / refused stream
// reset', and /lattice/scale returns 503). Listen with Http2 (prior-knowledge
// h2c) so the same port serves both the gRPC data API and the plain GET
// /lattice/scale scrape - both arrive from the ingress as HTTP/2. No server
// certificate is needed: the TLS boundary is the ingress, not the container.
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(httpPort, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(o =>
    {
        o.ClusterId = clusterId;
        o.ServiceId = serviceId;
    });

    // Real multi-silo clustering over an Azure Storage table. Each replica
    // advertises its endpoints here and discovers its peers, so ACA scaling the
    // replica count out (or in) grows / shrinks a genuine Orleans cluster.
    silo.UseAzureStorageClustering(o =>
    {
        o.TableName = clusteringTable;
        o.TableServiceClient = tableServiceClient;
    });

    // Advertise the container's primary NIC address on fixed ports so peers in
    // the ACA environment's internal network can reach this silo. ACA gives
    // each replica a routable internal IP; ConfigureEndpoints picks it up.
    silo.ConfigureEndpoints(siloPort, gatewayPort);

    // Durable Azure Table grain state (managed identity). LatticeGrain and the
    // B+ tree grains persist here; a replica restart or a fresh replica reads
    // the committed state back.
    silo.AddAzureTableGrainStorageAsDefault(o =>
    {
        o.TableName = grainTable;
        o.TableServiceClient = tableServiceClient;
    });

    // Reminders: the compaction reminder LatticeGrain registers on first write
    // needs a durable, cluster-shared reminder table on a multi-silo cluster.
    silo.UseAzureTableReminderService(o =>
    {
        o.TableName = reminderTable;
        o.TableServiceClient = tableServiceClient;
    });

    // The core lattice, its grain-state storage factory, and the durable Azure
    // Table WAL - all on managed identity.
    silo.AddLattice((services, name) => services.AddAzureTableGrainStorage(name, o =>
    {
        o.TableName = grainTable;
        o.TableServiceClient = tableServiceClient;
    }));
    silo.AddAzureTableWalStorage(o =>
    {
        o.TableName = walTable;
        o.ServiceUri = new Uri(tableUri);
        o.TokenCredential = credential;
    });

    // The write-capable data-plane facade (ILatticeDataApi) the gRPC surface
    // binds. Must be added after AddLattice.
    silo.AddLatticeDataApi();

    // The opt-in autoscaling signal: samples cluster-aggregate compute pressure
    // on a timer and caches the ScalingSignal the /lattice/scale endpoint serves.
    silo.AddLatticeScalingSignal();
});

// The Basic-hash authorizer replaces the data-API binding's default-deny
// authorizer; register it BEFORE AddLatticeDataApiGrpc so the binding's TryAdd
// preserves ours. The surface stays fail-closed for anonymous / wrong-password
// callers.
builder.Services.AddSingleton<ILatticeDataApiAuthorizer, BasicAdminDataApiAuthorizer>();
builder.Services.AddLatticeDataApiGrpc(o => o.RequireAuthorization = true);

// A readiness probe backed by the scaling signal (Degraded / Unhealthy derived
// from the signal's own thresholds). ACA can point its health probes at it.
builder.Services.AddHealthChecks().AddLatticeScalingHealthCheck(tags: new[] { "ready" });

var app = builder.Build();

// The KEDA metrics-api scale rule scrapes this route (default /lattice/scale)
// and reads the top-level scaleValue. Unauthenticated by design - it is a
// scrape target that discloses only aggregate pressure, never data.
app.MapLatticeScalingSignal();

// The write-capable data-API gRPC surface (Basic-credential gated).
app.MapLatticeDataApiGrpc();

// Liveness / readiness endpoints for the ACA health probes.
app.MapHealthChecks("/healthz");
app.MapHealthChecks("/readyz", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = registration => registration.Tags.Contains("ready"),
});

app.Logger.LogInformation(
    "ClusterScaling silo starting: clusterId={ClusterId} httpPort={HttpPort} siloPort={SiloPort} gatewayPort={GatewayPort} tableUri={TableUri}",
    clusterId,
    httpPort,
    siloPort,
    gatewayPort,
    tableUri);

await app.RunAsync();

// --- helpers ---------------------------------------------------------------

static string RequireEnv(string name)
{
    var value = Environment.GetEnvironmentVariable(name);
    if (string.IsNullOrWhiteSpace(value))
    {
        throw new InvalidOperationException(
            $"Required environment variable '{name}' is not set. deploy.ps1 injects it into the container app.");
    }

    return value;
}

static string EnvOrDefault(string name, string fallback)
{
    var value = Environment.GetEnvironmentVariable(name);
    return string.IsNullOrWhiteSpace(value) ? fallback : value;
}

static int EnvIntOrDefault(string name, int fallback)
{
    var value = Environment.GetEnvironmentVariable(name);
    return int.TryParse(value, out var parsed) && parsed > 0 ? parsed : fallback;
}
