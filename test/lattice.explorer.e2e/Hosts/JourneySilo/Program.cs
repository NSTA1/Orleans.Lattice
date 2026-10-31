using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.Telemetry.Grpc;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Backup.AzureBlob;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;
using Orleans.Lattice.Membership;
using Orleans.Lattice.ReferenceArchitecture.Hosting;
using Orleans.Lattice.ReferenceArchitecture.Silo;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Scaling;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Lattice.Tenancy;

// ---------------------------------------------------------------------------
// The journey silo (issue #5359).
//
// reference-architecture/hosts/Silo/Program.cs, built from source and run as a
// real process by the journey harness. It makes the same registration calls in
// the same order (JourneySiloParityTests fails if the reference silo gains an
// AddLattice* or MapLattice* call this host does not make), configured by the
// same environment variables, with these differences, each one a deployment
// choice rather than a test convenience:
//
//   * Identity. Entra is the one provider CI cannot reach, so the facades run
//     in the reference silo's "authorization required" shape with the Entra
//     authenticator replaced by DirectoryCredentialAuthenticator: a Basic
//     credential checked against the harness's directory file. The same file
//     is the cluster's identity directory, so the Explorer's pickers search it.
//   * TLS. The gRPC port serves HTTPS with a certificate from the run-scoped CA,
//     as the region's load balancer reaches it.
//   * Add-ons the reference silo does not register: Apps (with the named app
//     sources samples and samples-next), tenancy (off unless Tenancy:Enabled),
//     replication status, and the telemetry facade over the estate's Prometheus
//     (only when Telemetry:BackendAddress is set).
//   * The replication transport trusts the run-scoped CA, which a peer region's
//     gateway certificate chains to.
//   * Fault seams, enabled only by LATTICE_E2E_FAULTS=1.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

builder.Logging.SuppressProbeRequestLogs(
    FrontDoorOriginLockApplicationBuilderExtensions.HealthPath,
    "/orleans.lattice.replication.LatticeReplication",
    "/orleans.lattice.replication.LatticeRemoteSnapshot",
    "/orleans.lattice.replication.LatticeSaga");

var storage = AzureStorageIdentity.FromConfiguration(config);
var backupStorage = AzureStorageIdentity.ForBackupSink(config, storage);

var clusterId = config["Cluster:Id"] ?? "lattice";
var serviceId = config["Cluster:ServiceId"] ?? clusterId;
var replicationClusterId = config["Replication:ClusterId"] ?? clusterId;

var httpPort = config.GetValue("Silo:HttpPort", 8080);
var grpcPort = config.GetValue("Silo:GrpcPort", 8081);
var siloPort = config.GetValue("Silo:SiloPort", 11111);
var gatewayPort = config.GetValue("Silo:GatewayPort", 30000);
var advertisedIp = config["Silo:AdvertisedIp"];

var backupPrimary = config.GetValue("Backup:Primary", false);
var dataApiEnabled = config.GetValue("DataApi:Enabled", true);
var tenancyEnabled = config.GetValue("Tenancy:Enabled", false);
var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];

var replicationPeers = ReplicationTopology.ParsePeers(config);
var replicatedTrees = ReplicationTopology.ParseTrees(config);
var allowPlaintextReplication = config.GetValue("Replication:AllowPlaintext", false);
var enableRuntimeReplicationConfig = config.GetValue("Replication:EnableRuntimeConfig", false);
var enableDigestAntiEntropy = config.GetValue("Replication:EnableDigestAntiEntropy", false);
var digestProbeIntervalSeconds = config.GetValue("Replication:DigestProbeIntervalSeconds", 0);

var directory = JourneyDirectoryFile.FromEnvironment();
var serverCertificate = JourneyHostEnvironment.LoadServerCertificate();
var certificateAuthority = JourneyHostEnvironment.LoadCertificateAuthority();
var telemetryBackend = config["Telemetry:BackendAddress"];

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(httpPort, listen => listen.Protocols = HttpProtocols.Http1);
    kestrel.ListenAnyIP(grpcPort, listen =>
    {
        if (serverCertificate is null)
        {
            listen.Protocols = HttpProtocols.Http2;
            return;
        }

        // Behind TLS, ALPN negotiates HTTP/2 for gRPC and HTTP/1.1 for the
        // gateway's health probe, so both are offered.
        listen.Protocols = HttpProtocols.Http1AndHttp2;
        listen.UseHttps(serverCertificate);
    });
});

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(options =>
    {
        options.ClusterId = clusterId;
        options.ServiceId = serviceId;
    });

    if (!string.IsNullOrWhiteSpace(advertisedIp))
    {
        silo.ConfigureEndpoints(IPAddress.Parse(advertisedIp), siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }
    else
    {
        silo.ConfigureEndpoints(siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }

    silo.UseAzureStorageClustering(options =>
        storage.ConfigureTable(options, config["Clustering:TableName"] ?? "OrleansLatticeClustering"));
    silo.UseAzureTableReminderService(options =>
        storage.ConfigureTable(options, config["Reminders:TableName"] ?? "OrleansLatticeReminders"));

    var grainTableName = config["GrainStorage:TableName"] ?? "OrleansLatticeGrains";
    silo.AddAzureTableGrainStorageAsDefault(options => storage.ConfigureTable(options, grainTableName));
    silo.AddLattice((services, storeName) =>
        services.AddAzureTableGrainStorage(storeName, options => storage.ConfigureTable(options, grainTableName)));

    // Time compression an operator would apply through configuration: the idle
    // lifetime of a scan cursor, short enough to expire inside a journey.
    if (config.GetValue<int?>("Lattice:CursorIdleTtlSeconds") is { } cursorIdleSeconds)
    {
        silo.ConfigureLattice(options => options.CursorIdleTtl = TimeSpan.FromSeconds(cursorIdleSeconds));
    }

    silo.AddAzureTableWalStorage(options =>
        storage.ConfigureWal(options, config["Wal:TableName"] ?? "OrleansLatticeWal"));

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

        if (enableDigestAntiEntropy)
        {
            options.DigestProbeEnabled = true;
            options.MerkleWalkEnabled = true;
            options.AutoRemediateOnDigestMismatch = true;
            options.LeafReReplayEnabled = true;
            options.BootstrapFallbackEnabled = true;
            if (digestProbeIntervalSeconds > 0)
            {
                options.DigestProbeInterval = TimeSpan.FromSeconds(digestProbeIntervalSeconds);
            }
        }
    }, enableRuntimeConfig: enableRuntimeReplicationConfig);

    silo.ReplicateLatticeSystemTrees();

    silo.Services.AddLatticeReplicationGrpc(grpc =>
    {
        grpc.LocalClusterId = replicationClusterId;
        grpc.AllowPlaintextEndpoints = allowPlaintextReplication;
        foreach (var (peerClusterId, endpoint) in replicationPeers)
        {
            grpc.Peers[peerClusterId] = endpoint;
        }

        // A peer region's gateway presents a certificate from the run-scoped CA, which
        // the silo trusts as a service configured for a private PKI does.
        if (certificateAuthority is not null)
        {
            grpc.ConfigureChannel = (_, channel) => channel.HttpHandler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
                SslOptions = { RemoteCertificateValidationCallback = JourneyHostEnvironment.TrustOnly(certificateAuthority) },
            };
        }
    });

    if (enableRuntimeReplicationConfig)
    {
        silo.AddLatticeReplicationApi();
    }

    // Not in the reference silo: the read-only replication status the Explorer's
    // Replication area renders.
    silo.AddLatticeReplicationStatusApi();

    silo.AddLatticeBackup();
    silo.AddLatticeBackupAzureBlob(options =>
        backupStorage.ConfigureBackupSink(options, config["Backup:ContainerName"] ?? LatticeBackupAzureBlobOptions.DefaultContainerName));
    silo.AddLatticeBackupApi();

    if (backupPrimary)
    {
        // The reference silo reads the cadence in hours; the journey estate
        // compresses it to minutes through the same kind of setting.
        silo.ConfigureLatticeBackupSchedule(schedule =>
        {
            schedule.FullBackupScheduleEnabled = true;
            schedule.FullBackupInterval = config.GetValue<int?>("Backup:FullIntervalMinutes") is { } fullMinutes
                ? TimeSpan.FromMinutes(fullMinutes)
                : TimeSpan.FromHours(config.GetValue("Backup:FullIntervalHours", 24));
            schedule.IncrementalBackupScheduleEnabled = true;
            schedule.IncrementalBackupInterval = TimeSpan.FromMinutes(config.GetValue("Backup:IncrementalIntervalMinutes", 60));
            schedule.RetentionEnabled = true;
            schedule.RetentionKeepLast = config.GetValue("Backup:RetentionKeepLast", 7);
        });
    }

    silo.AddLatticeScalingSignal(options =>
        options.MinReplicas = config.GetValue("Scaling:MinReplicas", 1));

    silo.AddLatticeStateApi();
    if (dataApiEnabled)
    {
        silo.AddLatticeDataApi();
    }

    silo.AddLatticeMembership();

    // The harness's directory is the cluster's identity directory (the search and
    // resolve seam Entra fills in a deployment), so the subject pickers run in
    // their pick-existing mode. Directory validation is left at its default (off):
    // the story creates stored groups the directory does not hold.
    silo.AddStaticIdentityDirectory(roster =>
    {
        foreach (var user in directory.Current.Users)
        {
            roster.AddUser(user.Id, user.DisplayName);
        }

        foreach (var group in directory.Current.Groups)
        {
            roster.AddGroup(group.Id, group.DisplayName);
        }
    });

    silo.AddLatticeAuth(options =>
    {
        options.DefaultEffect = string.Equals(config["Auth:DefaultEffect"], "Allow", StringComparison.OrdinalIgnoreCase)
            ? LatticeEffect.Allow
            : LatticeEffect.Deny;
        options.AllTreesGrantsEnabled = true;
        foreach (var administrator in ParseCsv(config["Auth:BootstrapAdministrators"]))
        {
            options.BootstrapAdministrators.Add(administrator);
        }
    });
    silo.AddLatticeAuthApi();

    if (tenancyEnabled)
    {
        silo.AddLatticeTenancy(options =>
            options.DelegatedAccessAdministrationEnabled = config.GetValue("Tenancy:DelegatedAccess", false));
        silo.AddLatticeTenantAdminApi();
    }

    silo.AddLatticeSchemaEnforcement();
    if (config.GetValue("Schema:EnableVersioning", false))
    {
        silo.AddLatticeSchemaVersioning();
    }

    silo.AddLatticeSchemaApi();
    silo.AddLatticeTreeAdminApi();

    // Not in the reference silo: installable apps, from two named sources.
    silo.AddLatticeApps();
    silo.AddLatticeAppSource(JourneyAppSource.CreateSamples());
    silo.AddLatticeAppSource(JourneyAppSource.CreateSamplesNext());
    silo.AddLatticeAppsApi();
    silo.AddLatticeAppBridgeApi();

    // In place of the Entra authenticator.
    silo.Services.AddSingleton(directory);
    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DirectoryCredentialAuthenticator>();
});

// Every facade's coarse transport gate is opened, as the reference silo opens
// them in its Entra deployment: the real enforcement is the deny-by-default
// per-subject gate applied afterwards on the subject the authenticator resolved.
// The bindings read the Basic scheme, and the State API advertises it, so the
// Explorer offers its built-in sign-in form.
const string credentialScheme = DirectoryCredentialAuthenticator.Scheme;

builder.Services.AddLatticeStateApiGrpc(options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
    options.AdvertisedAuthSchemes.Add(new Orleans.Lattice.Api.State.Grpc.AuthSchemeDescriptor
    {
        SchemeId = "basic",
        DisplayName = "Username / password",
    });
});
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();

builder.Services.AddLatticeAuthApiGrpc(options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
});
builder.Services.AddSingleton<ILatticeAuthApiAuthorizer, AllowAllAuthApiAuthorizer>();

builder.Services.AddHostedService<AdministratorAccessSeeder>();

// The line feed's in-cluster component, which owns the order-status tag index.
if (config.GetValue("LineFeed:Indexer", false))
{
    builder.Services.AddHostedService<LineFeedOrderIndexer>();
}

if (dataApiEnabled)
{
    builder.Services.AddLatticeDataApiGrpc(options =>
    {
        options.RequireAuthorization = true;
        options.CredentialScheme = credentialScheme;
    });
    builder.Services.AddSingleton<ILatticeDataApiAuthorizer, AllowAllDataApiAuthorizer>();
}

builder.Services.AddLatticeBackupApiGrpc(options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
});
builder.Services.AddSingleton<ILatticeBackupApiAuthorizer, AllowAllBackupApiAuthorizer>();

if (enableRuntimeReplicationConfig)
{
    builder.Services.AddLatticeReplicationApiGrpc(options =>
    {
        options.RequireAuthorization = true;
        options.CredentialScheme = credentialScheme;
    });
    builder.Services.AddSingleton<ILatticeReplicationApiAuthorizer, AllowAllReplicationApiAuthorizer>();
}

builder.Services.AddLatticeReplicationStatusApiGrpc();

builder.Services.AddLatticeSchemaApiGrpc(options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
});
builder.Services.AddSingleton<ILatticeSchemaApiAuthorizer, AllowAllSchemaApiAuthorizer>();

builder.Services.AddLatticeTreeAdminApiGrpc(options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
});
builder.Services.AddSingleton<ILatticeTreeAdminApiAuthorizer, AllowAllTreeAdminApiAuthorizer>();

Action<LatticeAppsApiGrpcOptions> appsGrpc = options =>
{
    options.RequireAuthorization = true;
    options.CredentialScheme = credentialScheme;
};
builder.Services.AddLatticeAppsApiGrpc(appsGrpc);
builder.Services.AddLatticeAppCatalogApiGrpc(appsGrpc);
builder.Services.AddLatticeAppWorkspaceApiGrpc(appsGrpc);
builder.Services.AddLatticeAppBridgeApiGrpc(appsGrpc);
builder.Services.AddSingleton<ILatticeAppsApiAuthorizer, OpenAppsApiAuthorizer>();

if (tenancyEnabled)
{
    builder.Services.AddLatticeTenantAdminApiGrpc(options =>
    {
        options.RequireAuthorization = true;
        options.CredentialScheme = credentialScheme;
    });
    builder.Services.AddSingleton<ILatticeTenantAdminApiAuthorizer, AllowAllTenantAdminApiAuthorizer>();
}

// Not in the reference silo, whose telemetry is read by the MCP head: the read-only
// telemetry facade and its gRPC binding over the estate's Prometheus (#5362), as
// reference-architecture/local-dev/hosts/Silo registers them, so the Explorer's
// Telemetry area has boards. Wired only when Telemetry:BackendAddress names it.
if (!string.IsNullOrWhiteSpace(telemetryBackend))
{
    builder.Services.Configure<LatticeTelemetryOptions>(options =>
        options.BackendAddress = new Uri(telemetryBackend, UriKind.Absolute));
    builder.Services.AddSingleton<IValidateOptions<LatticeTelemetryOptions>, LatticeTelemetryOptionsValidator>();
    builder.Services.AddLatticeTelemetryApi();
    builder.Services.AddLatticeTelemetryApiGrpc(options =>
    {
        options.RequireAuthorization = true;
        options.CredentialScheme = credentialScheme;
    });
    builder.Services.AddSingleton<ILatticeTelemetryApiAuthorizer, AllowAllTelemetryApiAuthorizer>();
}

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter($"{LatticeMetrics.MeterName}*")
        .AddMeter("Microsoft.Orleans")
        .AddMeter("System.Runtime")
        .AddPrometheusExporter());

var app = builder.Build();

app.UseFrontDoorOriginLock(
    frontDoorId,
    "/metrics",
    "/lattice/scale",
    "/orleans.lattice.replication.LatticeReplication",
    "/orleans.lattice.replication.LatticeRemoteSnapshot",
    "/orleans.lattice.replication.LatticeSaga");

app.UseInternalPortEndpointGuard(httpPort, "/metrics", "/lattice/scale", JourneyFaultEndpoints.Prefix);

app.MapLatticeStateApiGrpc();
app.MapLatticeAuthApiGrpc();
if (dataApiEnabled)
{
    app.MapLatticeDataApiGrpc();
}

app.MapLatticeBackupApiGrpc();
app.MapLatticeReplicationGrpc();
if (enableRuntimeReplicationConfig)
{
    app.MapLatticeReplicationApiGrpc();
}

app.MapLatticeReplicationStatusApiGrpc();
app.MapLatticeSchemaApiGrpc();
app.MapLatticeTreeAdminApiGrpc();
app.MapLatticeAppsApiGrpc();
app.MapLatticeAppCatalogApiGrpc();
app.MapLatticeAppWorkspaceApiGrpc();
app.MapLatticeAppBridgeApiGrpc();
if (tenancyEnabled)
{
    app.MapLatticeTenantAdminApiGrpc();
}

if (!string.IsNullOrWhiteSpace(telemetryBackend))
{
    app.MapLatticeTelemetryApiGrpc();
}

app.MapLatticeScalingSignal();
app.MapPrometheusScrapingEndpoint();
app.MapMethods("/health", ["GET", "HEAD"], () => Results.Ok("healthy"));

if (JourneyHostEnvironment.FaultsEnabled)
{
    app.MapJourneyFaultEndpoints();
}

app.Run();

static IEnumerable<string> ParseCsv(string? value) =>
    string.IsNullOrWhiteSpace(value)
        ? []
        : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
