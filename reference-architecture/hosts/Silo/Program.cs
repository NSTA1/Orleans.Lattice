using System.Net;
using Azure.Identity;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Backup.AzureBlob;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Entra;
using Orleans.Lattice.Membership.Entra.Graph;
using Orleans.Lattice.ReferenceArchitecture.Hosting;
using Orleans.Lattice.ReferenceArchitecture.Silo;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Scaling;
using Orleans.Lattice.Storage.AzureTable;

// ---------------------------------------------------------------------------
// Reference-architecture Silo host.
//
// A production-shaped Orleans silo hosting Lattice for the active-active,
// cross-region estate described in reference-architecture.md. It is the only
// always-on head in each region and hosts:
//
//   * Azure Table Orleans clustering + durable Azure Table WAL.
//   * The cross-region replication shipper + receiver over gRPC, with
//     receiver-enrollment gating (Replication:Peers) and an explicit per-tree
//     wire merge mode (Replication:Trees), set symmetrically per region.
//   * The Azure Blob backup sink, with a Backup:Primary flag selecting whether
//     this region runs the scheduler (primary) or is DR standby (scheduler off).
//   * The read-only State API and the auth-admin control plane over gRPC.
//   * The lattice.scaling compute-axis signal endpoint for KEDA.
//   * Entra-backed authentication for its exposed facades.
//
// Every external input (connection targets, tenant / client ids, the
// replication key, the peer list, merge modes, the backup-primary flag) comes
// from environment variables / IConfiguration. No secret is ever hardcoded: the
// only secret, the per-cluster replication key, is read from the environment
// (LATTICE_REPLICATION_SECRET, injected from Key Vault at deploy time).
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

var storage = AzureStorageIdentity.FromConfiguration(config);

var clusterId = config["Cluster:Id"] ?? "lattice";
var serviceId = config["Cluster:ServiceId"] ?? clusterId;
var replicationClusterId = config["Replication:ClusterId"] ?? clusterId;

var httpPort = config.GetValue("Silo:HttpPort", 8080);
var grpcPort = config.GetValue("Silo:GrpcPort", 8081);
var siloPort = config.GetValue("Silo:SiloPort", 11111);
var gatewayPort = config.GetValue("Silo:GatewayPort", 30000);
var advertisedIp = config["Silo:AdvertisedIp"];

var backupPrimary = config.GetValue("Backup:Primary", false);
var entraEnabled = config.GetValue("Entra:Enabled", false);
var requireApiAuthorization = config.GetValue("StateApi:RequireAuthorization", false);

// Global-ingress origin lock: when set, every client-facing request on the
// external gRPC port must carry an X-Azure-FDID header matching this id. Empty
// (dev/compose, and the first deploy pass before Front Door exists) leaves the
// head unlocked. Threaded from the compute Bicep as LATTICE_FRONT_DOOR_ID.
var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];

var replicationPeers = ReplicationTopology.ParsePeers(config);
var replicatedTrees = ReplicationTopology.ParseTrees(config);
var allowPlaintextReplication = config.GetValue("Replication:AllowPlaintext", false);

// Kestrel exposes two ports: an HTTP/1 port for health probes and the scaling
// signal (both plain REST, so ACA can TCP/HTTP-probe without a shell), and an
// HTTP/2 port for the gRPC surfaces (state, auth, replication).
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(httpPort, listen => listen.Protocols = HttpProtocols.Http1);
    kestrel.ListenAnyIP(grpcPort, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(options =>
    {
        options.ClusterId = clusterId;
        options.ServiceId = serviceId;
    });

    // ACA runs the silo as a single container app whose replicas form the
    // Orleans cluster. Same-revision replica-to-replica connectivity carries
    // the silo-to-silo and gateway traffic; the advertised IP is the replica's
    // own address (supplied by the platform via Silo:AdvertisedIp when the
    // default NIC probe is not appropriate).
    if (!string.IsNullOrWhiteSpace(advertisedIp))
    {
        silo.ConfigureEndpoints(IPAddress.Parse(advertisedIp), siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }
    else
    {
        silo.ConfigureEndpoints(siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }

    // Azure Table clustering (Orleans membership).
    silo.UseAzureStorageClustering(options =>
        storage.ConfigureTable(options, config["Clustering:TableName"] ?? "OrleansLatticeClustering"));

    // Azure Table reminders (used by replication maintenance and backup sweeps).
    silo.UseAzureTableReminderService(options =>
        storage.ConfigureTable(options, config["Reminders:TableName"] ?? "OrleansLatticeReminders"));

    // Durable grain storage on Azure Table for Lattice grain state. The core
    // takes a per-named-store factory so every Lattice grain-state store is
    // durable across a replica restart, with the WAL as the mutation-durability
    // boundary underneath.
    var grainTableName = config["GrainStorage:TableName"] ?? "OrleansLatticeGrains";
    silo.AddAzureTableGrainStorageAsDefault(options => storage.ConfigureTable(options, grainTableName));
    silo.AddLattice((services, storeName) =>
        services.AddAzureTableGrainStorage(storeName, options => storage.ConfigureTable(options, grainTableName)));

    // Durable Azure Table WAL: the region's mutation-durability boundary.
    silo.AddAzureTableWalStorage(options =>
        storage.ConfigureWal(options, config["Wal:TableName"] ?? "OrleansLatticeWal"));

    // -- Cross-region replication (shipper + receiver) --------------------
    // ReplicatedTrees is the per-tree wire merge mode; ReplicationPeers is the
    // receiver-enrollment gate. Both must be set symmetrically across regions.
    silo.AddLatticeReplication(options =>
    {
        options.ClusterId = replicationClusterId;
        if (replicatedTrees.Count > 0)
        {
            options.ReplicatedTrees = replicatedTrees;
        }

        if (replicationPeers.Count > 0)
        {
            options.ReplicationPeers = replicationPeers.Keys.ToArray();
        }
    });

    // The gRPC replication transport dials the enrolled peer endpoints. The
    // per-cluster replication key is read from the environment by the default
    // EnvironmentVariableSecretSource (LATTICE_REPLICATION_SECRET), never from
    // source or the image. AllowPlaintextEndpoints is a local-only escape hatch
    // for the http:// compose harness; Azure uses server TLS via the ACA FQDN.
    silo.Services.AddLatticeReplicationGrpc(grpc =>
    {
        grpc.LocalClusterId = replicationClusterId;
        grpc.AllowPlaintextEndpoints = allowPlaintextReplication;
        foreach (var (peerClusterId, endpoint) in replicationPeers)
        {
            grpc.Peers[peerClusterId] = endpoint;
        }
    });

    // -- Backup sink + primary/standby scheduler --------------------------
    silo.AddLatticeBackup();
    silo.AddLatticeBackupAzureBlob(options =>
        storage.ConfigureBackupSink(options, config["Backup:ContainerName"] ?? LatticeBackupAzureBlobOptions.DefaultContainerName));

    // Exactly one region is the designated backup-primary and owns the schedule;
    // every other region is DR standby with the scheduler off, so there are no
    // competing backup chains writing the shared sink.
    if (backupPrimary)
    {
        silo.ConfigureLatticeBackupSchedule(schedule =>
        {
            schedule.FullBackupScheduleEnabled = true;
            schedule.FullBackupInterval = TimeSpan.FromHours(config.GetValue("Backup:FullIntervalHours", 24));
            schedule.IncrementalBackupScheduleEnabled = true;
            schedule.IncrementalBackupInterval = TimeSpan.FromMinutes(config.GetValue("Backup:IncrementalIntervalMinutes", 60));
            schedule.RetentionEnabled = true;
            schedule.RetentionKeepLast = config.GetValue("Backup:RetentionKeepLast", 7);
        });
    }

    // -- Scaling signal (compute axis, for the KEDA bridge) ---------------
    silo.AddLatticeScalingSignal(options =>
        options.MinReplicas = config.GetValue("Scaling:MinReplicas", 1));

    // -- State API + membership + authorization + auth-admin API ----------
    silo.AddLatticeStateApi();
    silo.AddLatticeMembership();
    silo.AddLatticeAuth(options =>
    {
        // Deny-by-default: a subject with no matching rule is refused, and the
        // read-visibility filter only surfaces trees a caller may read. The
        // effect is configurable purely so the local compose harness can run a
        // fully-open dev cluster (Auth:DefaultEffect=Allow); every deployed
        // region leaves it at the secure Deny default.
        options.DefaultEffect = string.Equals(config["Auth:DefaultEffect"], "Allow", StringComparison.OrdinalIgnoreCase)
            ? LatticeEffect.Allow
            : LatticeEffect.Deny;
        foreach (var administrator in ParseCsv(config["Auth:BootstrapAdministrators"]))
        {
            options.BootstrapAdministrators.Add(administrator);
        }
    });
    silo.AddLatticeAuthApi();

    // -- Entra-backed authentication for the exposed facades --------------
    if (entraEnabled)
    {
        var tenantId = Require(config, "Entra:TenantId");
        var clientId = Require(config, "Entra:ClientId");
        var authority = config["Entra:Authority"] ?? $"https://login.microsoftonline.com/{tenantId}/v2.0";

        silo.AddEntraCredentialAuthenticator(options =>
        {
            options.Authority = authority;
            options.TenantIds.Add(tenantId);
            foreach (var audience in ParseCsv(config["Entra:Audiences"]))
            {
                options.Audiences.Add(audience);
            }

            if (options.Audiences.Count == 0)
            {
                options.Audiences.Add(clientId);
                options.Audiences.Add($"api://{clientId}");
            }
        });

        // App-only Microsoft Graph directory backing (subject / group resolution)
        // is opt-in. Preferred path in Azure: a secret-less managed identity - the
        // region's user-assigned MI (resolved by DefaultAzureCredential via
        // AZURE_CLIENT_ID) authenticates app-only through a federated credential on
        // the silo app registration, so no client secret is stored or rotated.
        // A client secret (injected from Key Vault) is still accepted as a
        // dev / back-compat override and takes precedence when supplied.
        var graphSecret = config["Entra:Graph:ClientSecret"];
        var graphUseManagedIdentity = config.GetValue("Entra:Graph:UseManagedIdentity", false);
        if (!string.IsNullOrWhiteSpace(graphSecret))
        {
            silo.AddEntraGraphGroupResolver(options =>
            {
                options.TenantId = tenantId;
                options.ClientId = clientId;
                options.ClientSecret = graphSecret;
            });
        }
        else if (graphUseManagedIdentity)
        {
            silo.AddEntraGraphGroupResolver(options =>
            {
                options.Credential = new DefaultAzureCredential();
            });
        }
    }
});

// The gRPC bindings over the facades. RequireAuthorization is off for the local
// compose harness (a clearly-labelled dev bypass); a deployment sets
// StateApi:RequireAuthorization=true, behind the Entra-authenticated front door.
// When enforcement is on, the turnkey env-var credential authorizer secures the
// state surface; the auth-admin surface stays fail-closed until an operator
// wires its authorizer.
builder.Services.AddLatticeStateApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
if (requireApiAuthorization)
{
    builder.Services.AddEnvVarCredentialAuthorizer();
}

builder.Services.AddLatticeAuthApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);

// Export the orleans.lattice meter over Prometheus at /metrics so a scraper
// (the local compose Prometheus, or Azure Managed Prometheus) can collect the
// cluster telemetry that backs the bundled Grafana dashboards and the MCP
// telemetry tools.
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(LatticeMetrics.MeterName)
        .AddPrometheusExporter());

var app = builder.Build();

// Enforce the Front Door origin lock before any endpoint runs. The internal
// HTTP/1 port serves /health (platform liveness probe), /metrics (Prometheus
// scrape), and the /lattice/scale signal (KEDA) - all reached directly on the
// internal network without transiting Front Door, so they are exempt. Every
// client-facing gRPC request on the external port is locked.
app.UseFrontDoorOriginLock(frontDoorId, "/metrics", "/lattice/scale");

app.MapLatticeStateApiGrpc();
app.MapLatticeAuthApiGrpc();
app.MapLatticeReplicationGrpc();

// Compute-axis scaling signal for the KEDA Prometheus scaler (default route
// /lattice/scale) and a liveness probe. Both are plain HTTP so ACA can probe
// them without a shell in the final image.
app.MapLatticeScalingSignal();
app.MapPrometheusScrapingEndpoint();
app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();

static IEnumerable<string> ParseCsv(string? value) =>
    string.IsNullOrWhiteSpace(value)
        ? []
        : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

static string Require(IConfiguration configuration, string key) =>
    configuration[key] is { Length: > 0 } value
        ? value
        : throw new InvalidOperationException($"Required configuration '{key}' is not set.");
