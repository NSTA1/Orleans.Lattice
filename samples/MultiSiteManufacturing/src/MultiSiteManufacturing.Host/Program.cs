using Azure.Data.Tables;
using Azure.Storage.Queues;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Storage.AzureTable;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MultiSiteManufacturing.Host;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Components;
using MultiSiteManufacturing.Host.Dashboard;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Grpc;
using MultiSiteManufacturing.Host.Inventory;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Host.Operator;
using MultiSiteManufacturing.Host.Replication;

// HTTP/2 cleartext (h2c) for the gRPC push transport. The dev
// docker-compose topology and the local `dotnet run` topology both
// expose plaintext HTTP/2 on the silo HTTP ports; the grpc-dotnet
// channel needs this AppContext switch enabled before constructing a
// GrpcChannel against an http:// URI. Production deployments swap to
// https:// + mTLS (configured via LatticeReplicationGrpcOptions.ConfigureChannel)
// and do not need this switch.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = WebApplication.CreateBuilder(args);

// Cluster-aware bootstrap. The sample launches as one of two
// independent Orleans clusters - "us" and "eu" - selected via
// the --cluster command-line argument. Each cluster has its own ClusterId,
// its own Azurite instance, and its own HTTP port range. A per-cluster
// overlay file (appsettings.cluster.{name}.json) supplies connection
// strings, ports, and the replication topology; it's merged on top of
// appsettings.json so shared defaults still apply.
var clusterName = ResolveArg(args, "--cluster", builder.Configuration["CLUSTER_NAME"]) ?? "us";
builder.Configuration.AddJsonFile($"appsettings.cluster.{clusterName}.json", optional: false, reloadOnChange: false);

// The default WebApplicationBuilder config chain is:
//   appsettings.json -> appsettings.{Env}.json -> env vars -> cmd line
// Later sources win. By appending the cluster overlay above we put JSON
// *after* env vars, which silently invalidates Docker Compose overrides
// like ConnectionStrings__AzureTableStorage. Re-adding env vars + args
// here restores the documented precedence (env/args beat the overlay).
builder.Configuration.AddEnvironmentVariables();
builder.Configuration.AddCommandLine(args);

var useInMemoryStorage = builder.Environment.IsEnvironment("Testing")
    || builder.Configuration.GetValue<bool>("Orleans:UseInMemoryStorage");

var tableStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureTableStorage")
    ?? "UseDevelopmentStorage=true";

// Queue storage for the dashboard broadcast stream. Falls back to the
// same account as table storage (Azurite exposes both services on one
// connection string); override via the ConnectionStrings:AzureQueueStorage
// setting if the deployment puts queues on a different account.
var queueStorageConnectionString =
    builder.Configuration.GetConnectionString("AzureQueueStorage")
    ?? tableStorageConnectionString;

// Cluster section - per-cluster ports, cluster id, etc.
var clusterSection = builder.Configuration.GetSection("Cluster");
var orleansClusterId = clusterSection["OrleansClusterId"] ?? $"msmfg-{clusterName}";

var siloId = ResolveArg(args, "--silo-id", builder.Configuration["SILO_ID"]) ?? "a";
var isPrimarySilo = string.Equals(siloId, "a", StringComparison.OrdinalIgnoreCase);

var siloPort = isPrimarySilo
    ? clusterSection.GetValue("SiloPortA", 11111)
    : clusterSection.GetValue("SiloPortB", 11112);
var gatewayPort = isPrimarySilo
    ? clusterSection.GetValue("GatewayPortA", 30000)
    : clusterSection.GetValue("GatewayPortB", 30001);
var httpPort = isPrimarySilo
    ? clusterSection.GetValue("HttpPortA", 5001)
    : clusterSection.GetValue("HttpPortB", 5002);

// When running under Docker Compose, ASPNETCORE_URLS is set by the
// container env (typically "http://+:8080") and must be honoured as-is -
// binding to "localhost" inside a container only binds the loopback
// interface and the host-side port publish (5001..5004) never reaches
// the app. For the host-process path (plain `dotnet run`),
// ASPNETCORE_URLS is unset and we fall back to the per-A/B httpPort
// from the cluster overlay.
if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ASPNETCORE_URLS")))
{
    builder.WebHost.UseUrls($"http://localhost:{httpPort}");
}

// When the host is launched via `dotnet <dll>` (as run.ps1 does, to avoid
// the shared-bin/obj race of two concurrent `dotnet run` invocations), the
// default environment is Production and ASP.NET Core does not auto-wire the
// Static Web Assets manifest. Without this call, MapStaticAssets serves
// _framework/blazor.web.js as a 200 with 0 bytes - the Blazor bootstrap
// script loads but does nothing, so the interactive SignalR circuit never
// forms and all @onclick handlers (Chaos flyout, Race, Fix) are dead.
builder.WebHost.UseStaticWebAssets();

// Cross-cluster replication uses gRPC, which requires HTTP/2 end-to-end.
// On plaintext Kestrel endpoints, ALPN is unavailable, so Kestrel cannot
// multiplex HTTP/1.1 and HTTP/2 on the same port: setting Protocols to
// Http1AndHttp2 silently downgrades to HTTP/1 only on http://. Browsers
// also refuse HTTP/2 cleartext, so we cannot flip port 8080 to Http2.
//
// Solution: two cleartext ports, both declared explicitly here. The
// presence of any Listen* call in ConfigureKestrel makes Kestrel ignore
// ASPNETCORE_URLS entirely (it logs `Overriding address(es)`...), so we
// must re-declare port 8080 alongside the new 8081. Port 8080 keeps
// HTTP/1.1 for Blazor SignalR + the in-cluster Fact / Site / Compliance
// / Inventory gRPC services. Port 8081 is dedicated h2c (HTTP/2 prior
// knowledge) and serves the package's LatticeReplicationGrpcService -
// the peer Traefik forwards `/orleans.lattice.replication.*` requests
// onto silo-{cluster}-{a|b}:8081.
builder.WebHost.ConfigureKestrel(k =>
{
    k.ListenAnyIP(8080, o => o.Protocols = HttpProtocols.Http1);
    k.ListenAnyIP(8081, o => o.Protocols = HttpProtocols.Http2);
});

builder.Services.AddSingleton(new SiloIdentity(siloId, isPrimarySilo, clusterName));

// Cross-cluster replication is delegated end-to-end to
// Orleans.Lattice.Replication: WAL, shipper, applier, dead-letter
// handling, plus the gRPC push transport. Every replicated tree in
// the sample ships through that package - `mfg-facts`,
// `mfg-site-activity`, and its `tag-mfg-site` membership tree as
// LwwRegister, `mfg-part-labels` as OrSet (typed CRDT delta
// shipping). The `mfg-part-operator` tree
// stays cluster-local because LWW across clusters with disjoint HLCs
// is meaningless. See `docs/lattice.replication/` for the package's
// wire format and bootstrap protocol.
//
// The two configuration keys below switch the package wire-up on:
// `PackageReplication:PeerClusterId` and `PackageReplication:PeerGrpcEndpoint`.
// When either is missing (e.g. the in-memory test path or a stand-alone
// `dotnet run` against a single cluster), package replication is
// elided cleanly so the host still boots without a peer.
var packagePeerClusterId = builder.Configuration["PackageReplication:PeerClusterId"];
var packagePeerGrpcEndpoint = builder.Configuration["PackageReplication:PeerGrpcEndpoint"];
var packageReplicationConfigured = !useInMemoryStorage
    && !string.IsNullOrWhiteSpace(packagePeerClusterId)
    && !string.IsNullOrWhiteSpace(packagePeerGrpcEndpoint)
    && Uri.TryCreate(packagePeerGrpcEndpoint, UriKind.Absolute, out _);

builder.Host.UseOrleans(silo =>
{
    if (useInMemoryStorage)
    {
        // Single-silo in-memory mode (tests + quick-start without Azurite).
        silo.UseLocalhostClustering(siloPort, gatewayPort);
        silo.UseInMemoryReminderService();
        silo.AddMemoryGrainStorageAsDefault();
        silo.AddMemoryGrainStorage("msmfgGrainState");
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // Read-only cluster state API (issue #886). Co-hosted on the
        // lattice-backed silo so the Orleans.Lattice.Explorer can browse this
        // single-process quick-start over the gRPC state surface. Must follow
        // AddLattice (it reads the core tree registry / digests).
        silo.AddLatticeStateApi();

        // Drive the deep storage gauges (snapshot / leaf-state / total bytes)
        // on a slow cadence so the Grafana storage panels populate without an
        // operator calling the storage-usage API. The deep read is O(1) per
        // shard root (it never walks the leaf chain), so it does not pin idle
        // trees resident. WAL-bytes still refresh on the faster default poll.
        silo.ConfigureLattice(o => o.StorageUsageDeepPollInterval = TimeSpan.FromSeconds(60));

        // Dashboard broadcast stream - in-memory variant. This path is
        // only used by the single-silo quick-start so cluster-wide
        // fan-out is a no-op; the memory provider keeps the broadcaster
        // wiring identical to the production code path.
        silo.AddMemoryStreams(DashboardBroadcaster.StreamProviderName);
        silo.AddMemoryGrainStorage("PubSubStore");
    }
    else
    {
        silo.Configure<ClusterOptions>(o =>
        {
            o.ClusterId = orleansClusterId;
            o.ServiceId = "msmfg-service";
        });

        silo.ConfigureEndpoints(siloPort, gatewayPort, listenOnAnyHostAddress: true);

        silo.UseAzureStorageClustering(o =>
        {
            o.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.UseAzureTableReminderService(o =>
        {
            o.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddAzureTableGrainStorageAsDefault(options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddAzureTableGrainStorage("msmfgGrainState", options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        silo.AddLattice((services, name) =>
        {
            services.AddAzureTableGrainStorage(name, options =>
            {
                options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
            });
        });

        // Persist the lattice write-ahead log to Azure Table Storage
        // (Azurite locally) rather than the in-memory baseline AddLattice
        // installs. The WAL now survives silo restarts alongside grain
        // state, clustering, and reminders, all backed by the same
        // per-cluster Azurite instance. Must follow AddLattice - the last
        // AddWalStorage-with-factory call wins.
        silo.AddAzureTableWalStorage(options =>
        {
            options.ServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        // Drive the deep storage gauges (snapshot / leaf-state / total bytes)
        // on a slow cadence so the Grafana storage panels populate across
        // every silo without an operator calling the storage-usage API. The
        // deep read is O(1) per shard root (it never walks the leaf chain),
        // so it does not pin idle trees resident. WAL-bytes still refresh on
        // the faster default poll.
        silo.ConfigureLattice(o => o.StorageUsageDeepPollInterval = TimeSpan.FromSeconds(60));

        // Read-only cluster state API (issue #886). Co-hosted on the
        // lattice-backed silo so the Orleans.Lattice.Explorer can browse the
        // running cluster over the gRPC state surface, exposed through Traefik
        // on the published cluster endpoint. Must follow AddLattice.
        silo.AddLatticeStateApi();

        // Cluster-wide dashboard broadcast stream backed by Azure
        // Storage Queues. Every silo's DashboardBroadcaster publishes
        // each inbound fact onto this stream and subscribes to it, so
        // every Blazor Server circuit - regardless of which silo hosts
        // it - receives every fact. Durable transport survives silo
        // restarts and transient outages: queued messages are picked
        // up when subscribers reconnect. PubSubStore (Azure Table)
        // backs subscription metadata so subscriptions survive restart
        // too.
        silo.AddAzureQueueStreams(
            DashboardBroadcaster.StreamProviderName,
            configurator =>
            {
                configurator.ConfigureAzureQueue(ob => ob.Configure(options =>
                {
                    options.QueueServiceClient = new QueueServiceClient(queueStorageConnectionString);
                    // A small queue count keeps ordering predictable
                    // for a modest-volume dashboard feed. Orleans hashes
                    // the StreamId across these queues; single-queue
                    // gives strict FIFO which is what the dashboard
                    // wants (no reorder relative to publish).
                    options.QueueNames = new List<string> { "msmfgdashboard-0" };
                }));
            });
        silo.AddAzureTableGrainStorage("PubSubStore", options =>
        {
            options.TableServiceClient = new TableServiceClient(tableStorageConnectionString);
        });

        // Cross-cluster replication. The package's ReplicatedTrees map
        // covers every tree the sample wants replicated:
        // `mfg-facts` (LWW), `mfg-site-activity` + its `tag-mfg-site`
        // membership tree (LWW), and `mfg-part-labels` (OrSet - typed
        // CRDT delta shipping). The
        // `mfg-part-operator` tree stays cluster-local because LWW
        // across clusters with disjoint HLCs is meaningless. Wired
        // only on the persistent-storage path because the package's
        // WAL, shipper, and maintenance grain require Azure Table
        // reminders + grain storage; the in-memory path runs without
        // package replication and is unaffected.
        if (packageReplicationConfigured)
        {
            silo.AddLatticeReplication(opts =>
            {
                opts.ClusterId = clusterName;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    // The primary HLC-ordered fact log. Every domain
                    // event lands here; the Compliance fold reads
                    // from it; cross-cluster replication keeps both
                    // regions converged on the same fold result.
                    [LatticeFactBackend.FactTreeId] = LatticeMergeMode.LwwRegister,
                    // Tag-index "parts at site" view. The part-major
                    // subject tree (keyed {serial}/{site}) carries the
                    // activity value as an LWW register; the sibling
                    // tag-membership tree (tag-{IndexName}) carries the
                    // site posting rows. Both replicate so the per-site
                    // view stays answerable in either cluster. The
                    // membership tree is OrFlag (enable-wins): under
                    // active-active replication both clusters tag keys, so
                    // the index authors flag-CRDT membership dots that
                    // converge without a single-writer assumption - an LWW
                    // membership tree would silently lose concurrently
                    // authored postings.
                    [SiteActivityIndex.TreeId] = LatticeMergeMode.LwwRegister,
                    [SiteActivityIndex.IndexTreeId] = LatticeMergeMode.OrFlag,
                    // Per-serial OR-Set of process labels. Typed
                    // CRDT delta shipping: the package transmits
                    // add/remove/merge operations rather than raw
                    // byte values, which is the reason this tree is
                    // a separate replicated surface from the
                    // cluster-local operator-LWW tree.
                    [PartCrdtStore.LabelsTreeId] = LatticeMergeMode.OrSet,
                };
                opts.ReplicationPeers = new[] { packagePeerClusterId! };
            });
        }
    }
});

builder.Services.AddGrpc();

// Read-only state API gRPC binding (issue #886). Shares the host's single
// AddGrpc pipeline and is served on the existing :8081 h2c listener, mapped at
// the bottom of this file. Reached through each cluster's Traefik via the
// `/orleans.lattice.api.state/` router (priority 200, round-robin h2c, no
// sticky cookie) on the published cluster endpoint (5001 US / 5002 EU).
//
// Authorization is OFF by default so the sample is one-command runnable: the
// explorer connects anonymously over loopback h2c. When run.ps1 is given
// credentials it sets EXPLORER_STATE_AUTH=true and delivers each operator's
// salted PBKDF2 hash to the silo containers as LATTICE_STATE_USER_<user>, at
// which point the reference EnvVarCredentialAuthorizer enforces Basic auth and
// an anonymous explorer is rejected. The state-API auth interceptor scopes
// itself to the state service by name prefix, so the in-cluster Fact / Site /
// Compliance / Inventory and replication gRPC services are unaffected.
var stateApiAuthEnabled = builder.Configuration.GetValue<bool>("EXPLORER_STATE_AUTH");
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = stateApiAuthEnabled);
if (stateApiAuthEnabled)
{
    builder.Services.AddEnvVarCredentialAuthorizer();
}

// Cross-cluster replication transport. The package's gRPC push
// transport ships outbound batches via grpc-dotnet over the same
// Kestrel/AddGrpc pipeline that hosts the in-cluster Fact / Site /
// Compliance / Inventory services; the sender's GrpcChannel is
// constructed lazily on the first ship attempt and reused thereafter
// (HTTP/2 multiplexing). The receiver-side gRPC service is mapped at
// the bottom of this file alongside the other gRPC routes. Wired only
// when a peer endpoint has been resolved from configuration so the
// in-memory test path and partial local runs do not trip the
// transport's strict "every peer must be in Peers" check.
if (packageReplicationConfigured)
{
    var peerUri = new Uri(packagePeerGrpcEndpoint!, UriKind.Absolute);

    builder.Services.AddLatticeReplicationGrpc(opts =>
    {
        opts.Peers[packagePeerClusterId!] = peerUri;
        // The sample's docker-compose + run.ps1 topologies expose the
        // replication endpoint as plaintext h2c (HTTP/2 prior knowledge)
        // on an internal Docker network or localhost. The package
        // hardens the sender by default to refuse non-https endpoints;
        // explicitly opt in here because the sample is a dev / demo
        // deployment, not a production cluster. Production deployments
        // should instead supply an https://... endpoint and leave this
        // flag at its secure default.
        opts.AllowPlaintextEndpoints = true;
        // Stamp the origin-cluster header on every outbound batch so
        // the peer's auth interceptor can resolve a per-peer secret
        // when the operator chooses to partition secrets by origin.
        opts.LocalClusterId = clusterName;
    });

    // Sample security defaults. Cross-cluster replication is now
    // authenticated by default (the package's
    // LatticeReplicationGrpcAuthInterceptor rejects unmatched batches
    // with PermissionDenied). The sample uses two paths:
    //
    //   1. If the operator sets LATTICE_REPLICATION_SECRET on every
    //      silo (the docker-compose path does), the default
    //      EnvironmentVariableSecretSource picks it up and the
    //      receiver-side authenticator accepts batches whose
    //      x-lattice-replication-secret header matches.
    //
    //   2. If no secret is configured (the legacy `run-legacy.ps1` /
    //      single-host quick-start path), authentication is disabled
    //      so the sample still works out-of-the-box. This is safe for
    //      a dev sample bound to loopback; production deployments
    //      must supply a secret and leave RequireAuthentication at
    //      its secure default.
    var configuredSecret = Environment.GetEnvironmentVariable(
        LatticeReplicationEnvironmentVariables.Secret);
    var requireAuthentication = !string.IsNullOrWhiteSpace(configuredSecret);
    builder.Services.Configure<LatticeReplicationSecurityOptions>(o =>
    {
        o.RequireAuthentication = requireAuthentication;
        // The sample ships every appsettings.cluster.{us,eu}.json
        // without any secret material; the hostile-config scan would
        // be a no-op anyway but leaving it on covers operators who
        // copy the sample into their own deployment and later add a
        // LatticeReplication:Secret key by mistake.
    });

    // Operator-driven Tier 4b chaos: pause cross-cluster replication
    // from the dashboard fly-out. The decorator wraps the package's
    // gRPC push transport and consults IReplicationDisconnectGrain on
    // every ship; when the flag is set, outbound ship returns
    // Accepted=false so the package shipper does not advance its
    // per-peer cursor and the local WAL grows until the flag is
    // cleared, at which point replication resumes from the stationary
    // cursor. Tier 5 (`docker network disconnect`) is
    // transport-agnostic and remains untouched.
    builder.Services.AddChaosReplicationTransportDecorator();

    // Receiver-side mirror to the baseline backend. The dashboard's
    // "Inventory By Activity" tab needs to see facts that originated
    // on the peer cluster as soon as they apply locally; the
    // BaselineReplicationApplier decorator wraps the package's
    // IReplicationApplier, observes every successful receiver-side
    // apply (single and batched), decodes the mfg-facts payload,
    // mirrors the fact into BaselineFactBackend, and raises
    // FederationRouter.FactReplicated so the dashboard refreshes live.
    // (Why a decorator and not an IChangeFeed consumer? See the
    // BaselineReplicationApplier class-level remarks - briefly: the
    // package's merge path bypasses the per-key mutation observer that
    // populates the replog, so a change-feed subscriber never sees
    // foreign-origin entries.
    builder.Services.AddBaselineReplicationApplierDecorator();

    // Inbound counterpart to AddChaosReplicationTransportDecorator
    // above: when the operator-driven IReplicationDisconnectGrain
    // flag is set, the package's gRPC Push handler will see the
    // applier throw, rethrow as StatusCode.Internal to the peer, and
    // the peer's transport will not advance its per-peer cursor.
    // Without this, the local "Disconnect" button only halved the
    // cut: outbound from this cluster paused but inbound from the
    // peer kept arriving and applying. Must be the OUTERMOST applier
    // decorator so the gate fires before BaselineReplicationApplier
    // fans an entry out to the dashboard.
    builder.Services.AddChaosReplicationApplierDecorator();
}

// OpenTelemetry: export the orleans.lattice and orleans.lattice.replication
// meters via Prometheus. Each silo serves /metrics on its ASP.NET Core port
// (8080 inside the container); the docker-compose `prometheus` service
// scrapes all four silos and the `grafana` service renders the dashboards
// shipped by Orleans.Lattice.Dashboards.
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddPrometheusExporter());

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Federation: one concrete backend per name, each wrapped in a
// ChaosFactBackend decorator so IBackendChaosGrain can inject jitter,
// transient failures, and write amplification.
builder.Services.AddSingleton<BaselineFactBackend>();
builder.Services.AddSingleton<LatticeFactBackend>();
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<BaselineFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<IFactBackend>(sp => new ChaosFactBackend(
    sp.GetRequiredService<LatticeFactBackend>(),
    sp.GetRequiredService<IGrainFactory>()));
builder.Services.AddSingleton<FederationRouter>();

// Operator action surface.
builder.Services.AddSingleton<OperatorClock>();
builder.Services.AddScoped<OperatorActions>();

// CRDT-typed grain state backed by Orleans.Lattice. Singleton
// because it wraps a single ILattice grain reference and holds no
// per-call state.
builder.Services.AddSingleton<PartCrdtStore>();

// Drains this silo's CRDT shadow prefix back into the shared
// prefix when the simulated inter-silo partition heals.
builder.Services.AddHostedService<PartitionHealHostedService>();

// Keeps a lightweight B+ tree index of {site}/{stage}/{serial}
// entries so the "Parts by site" page can render a live per-site
// inventory via a range scan on ILattice. The index subscribes to
// FederationRouter.FactRouted and writes one entry per Fact - every
// fact type carries a Site, so inspection-only sites (Stuttgart CMM
// Lab) appear alongside process-step sites.
builder.Services.AddSingleton<SiteActivityIndex>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SiteActivityIndex>());

builder.Services.AddSingleton<DashboardBroadcaster>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<DashboardBroadcaster>());

// Per-silo bridge from the canonical orleans.lattice.replication meter
// into the cluster-singleton aggregator that backs the topbar's
// ship/recv strip. Registered as both a singleton (so MainLayout can
// hold a stable reference if needed) and a hosted service (so its
// MeterListener starts at silo boot and its 500ms push loop keeps
// the cluster grain warm).
builder.Services.AddSingleton<MultiSiteManufacturing.Host.Replication.ReplicationActivityTracker>();
builder.Services.AddHostedService(sp =>
    sp.GetRequiredService<MultiSiteManufacturing.Host.Replication.ReplicationActivityTracker>());

// Seeder runs on exactly one silo of exactly one cluster so the
// two clusters don't race and produce duplicate keys. Suppressed in
// the Testing environment so contract tests start against empty state.
//
// Gating precedence:
//   1. Configuration key "Seeder:Enabled" (env var `Seeder__Enabled`)
//      wins outright when present - `true` forces seeding on this
//      silo, `false` forces it off. Useful in docker-compose / CI
//      where the topology is declarative.
//   2. Otherwise, the default heuristic runs the seeder only on the
//      primary silo of the "us" cluster - the convention the
//      sample's docker-compose and run.ps1 topologies rely on.
var isSeeder = false;
if (!builder.Environment.IsEnvironment("Testing"))
{
    var seederOverride = builder.Configuration["Seeder:Enabled"];
    if (bool.TryParse(seederOverride, out var explicitFlag))
    {
        isSeeder = explicitFlag;
    }
    else
    {
        isSeeder = isPrimarySilo
            && string.Equals(clusterName, "us", StringComparison.OrdinalIgnoreCase);
    }
}
if (isSeeder)
{
    builder.Services.AddHostedService<InventorySeeder>();
}

var app = builder.Build();

app.MapStaticAssets();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapGrpcService<FactIngressServiceImpl>();
app.MapGrpcService<SiteControlServiceImpl>();
app.MapGrpcService<ComplianceServiceImpl>();
app.MapGrpcService<InventoryServiceImpl>();

// Read-only state API gRPC routes (issue #886). Served on the existing :8081
// h2c listener via the shared AddGrpc pipeline; reachable through Traefik's
// `/orleans.lattice.api.state/` router on the published cluster endpoint so the
// Orleans.Lattice.Explorer can browse this cluster's trees, views, metrics,
// topology, and data with no new published host ports.
app.MapLatticeStateApiGrpc();

// Receiver-side gRPC route for cross-cluster replication push. Mapped
// only when package replication was wired in above so the in-memory
// test path does not require the gRPC method singleton to resolve at
// startup.
if (packageReplicationConfigured)
{
    app.MapLatticeReplicationGrpc();
}

// Prometheus scrape endpoint - served at /metrics on the ASP.NET Core
// HTTP port. Anonymous access is fine because the endpoint is only
// reachable on the internal cluster network (not published to the host).
app.MapPrometheusScrapingEndpoint();

await app.RunAsync();

static string? ResolveArg(string[] args, string name, string? fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
        {
            return args[i + 1];
        }
    }
    return fallback;
}

/// <summary>
/// Program entry-point marker, exposed so the test project can reference the
/// host assembly for in-process gRPC contract tests.
/// </summary>
public partial class Program;
