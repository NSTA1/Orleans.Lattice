using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Orleans.Configuration;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Apps.Grpc;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Entra;
using Orleans.Lattice.Membership.Entra.Graph;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// One region of the sample: an ASP.NET host running a single-silo Orleans
/// cluster, every control plane the Explorer has an area for on one h2c gRPC
/// endpoint, and - in the two-region estate - the replication receiver its peer
/// ships to. The primary region also serves the Explorer console.
/// </summary>
/// <remarks>
/// <para>
/// Both regions are built by the same code, so pointing the console at the
/// west region shows the same areas as the east one. They differ only in their
/// ports, their region id, the peer they replicate with, and which of them
/// serves the console.
/// </para>
/// <para>
/// Transport authorization is off on every gRPC binding, because the console
/// carries no client certificate. That is sample-only: every call is still
/// authorized by the cluster against the resolved caller, which the bindings
/// read from the console's <c>Basic</c> sign-in header (see
/// <see cref="DemoBasicAuthenticator"/>). Cross-region replication runs over
/// plaintext loopback h2c with no shared secret, which is equally sample-only.
/// </para>
/// </remarks>
internal sealed class SampleRegion : IAsyncDisposable
{
    /// <summary>The Orleans service id both regions share.</summary>
    public const string ServiceId = "explorer-sample";

    private SampleRegion(SampleRegionPlan plan, WebApplication app)
    {
        Plan = plan;
        App = app;
    }

    /// <summary>What the region runs.</summary>
    public SampleRegionPlan Plan { get; }

    /// <summary>The region id.</summary>
    public string Id => Plan.Id;

    /// <summary>The region's host.</summary>
    public WebApplication App { get; }

    /// <summary>The region's services, which are also its silo's.</summary>
    public IServiceProvider Services => App.Services;

    /// <summary>Builds a region; nothing starts until <see cref="StartAsync"/>.</summary>
    /// <param name="plan">What the region runs.</param>
    /// <param name="options">The sample's options: directory mode, merge mode and replication health thresholds.</param>
    /// <param name="sink">The backup sink both regions share; required for a region of the estate.</param>
    /// <param name="peerLink">The switch that pauses the link to the peer region.</param>
    /// <returns>The region.</returns>
    public static SampleRegion Build(
        SampleRegionPlan plan,
        ExplorerSampleOptions options,
        ILatticeBackupSink? sink,
        PeerLink peerLink)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(peerLink);
        if (plan.IsEstate && sink is null)
        {
            throw new ArgumentException("A region of the two-region estate needs the shared backup sink.", nameof(sink));
        }

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ApplicationName = typeof(SampleRegion).Assembly.GetName().Name,
        });
        builder.Logging.ClearProviders();
        builder.WebHost.UseSetting(WebHostDefaults.ServerUrlsKey, string.Empty);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            // gRPC needs HTTP/2 without TLS (h2c) so the sample needs no dev
            // certificate; the console's Blazor circuit runs over HTTP/1.1, so
            // it gets its own port.
            kestrel.ListenLocalhost(plan.GrpcPort, listen => listen.Protocols = HttpProtocols.Http2);
            if (plan.Console is { } console)
            {
                kestrel.ListenLocalhost(console.WebPort, listen => listen.Protocols = HttpProtocols.Http1);
            }
        });

        if (plan.Console is not null)
        {
            // The console's stylesheets, fonts, scripts and app frame kit ship
            // as packaged static web assets, which WebApplication only maps in
            // Development; map them in every environment.
            builder.WebHost.UseStaticWebAssets();
        }

        builder.Host.UseOrleans(silo => ConfigureSilo(silo, plan, options, sink));
        ConfigureGrpc(builder.Services, plan);
        if (plan.Console is { } consolePlan)
        {
            ConfigureConsole(builder.Services, consolePlan);
        }

        var app = builder.Build();

        // Refuses cross-region replication while the link is paused. It runs
        // before routing, so a refused call never reaches the receiver.
        app.Use(peerLink.InvokeAsync);
        if (plan.Console is not null)
        {
            app.UseAntiforgery();
        }

        MapGrpc(app, plan);
        if (plan.Console is not null)
        {
            app.MapLatticeExplorer();
        }

        return new SampleRegion(plan, app);
    }

    /// <summary>Starts the region's silo and endpoints.</summary>
    /// <param name="cancellationToken">Cancels the start.</param>
    public Task StartAsync(CancellationToken cancellationToken = default) => App.StartAsync(cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await App.StopAsync().ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is OperationCanceledException or ObjectDisposedException)
        {
            // Already stopping or stopped.
        }

        await App.DisposeAsync().ConfigureAwait(false);
    }

    private static void ConfigureSilo(ISiloBuilder silo, SampleRegionPlan plan, ExplorerSampleOptions options, ILatticeBackupSink? sink)
    {
        silo.UseLocalhostClustering(plan.SiloPort, plan.GatewayPort, serviceId: ServiceId, clusterId: plan.Id);
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // The read-only state API behind the Data area.
        silo.AddLatticeStateApi();

        // Membership and authorization behind the Access area. The data plane is
        // deny-by-default; the bootstrap administrator bypasses the decision
        // engine, which is what lets the console's administrator see everything.
        silo.AddLatticeMembership(membership => membership.GroupMergeMode = options.GroupMergeMode);
        ConfigureIdentityDirectory(silo, options);
        silo.AddLatticeAuth(auth =>
        {
            auth.DefaultEffect = LatticeEffect.Deny;
            auth.BootstrapAdministrators.Add(SampleIdentities.Administrator);
        });
        silo.AddLatticeAuthApi();

        // Tenancy behind the Tenancy area, on the estate only: the tenant
        // registry and isolation seams, and the tenant-administration facades
        // (lifecycle, admins, grants, regions, quota usage and self-service).
        if (plan.IsEstate)
        {
            silo.AddLatticeTenancy();
            silo.AddLatticeTenantAdminApi();
        }

        // Schema enforcement and versioning behind the Schema area: one demo
        // schema with two versions and a v1 -> v2 upcaster. Enforcement comes
        // first because versioning composes its write interceptor.
        silo.AddLatticeSchemaEnforcement();
        silo.AddLatticeSchemaVersioning(registry =>
        {
            registry.AddSchema(schemaId: 1, version: 1, name: "machine-status");
            registry.AddSchema(schemaId: 1, version: 2, name: "machine-status");
            registry.AddUpcaster(
                schemaId: 1,
                fromVersion: 1,
                toVersion: 2,
                transform: LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember(
                        "state", LatticeValueTransform.Const(LatticeConstant.Text("unknown")))));
        });
        silo.AddLatticeSchemaApi();

        // Lattice Apps behind the Apps area, with the task-board sample app in
        // the in-image source. Registering it only makes it available.
        silo.AddLatticeApps();
        silo.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);
        silo.AddLatticeAppsApi();
        silo.AddLatticeAppBridgeApi();

        // Tree administration behind the Cluster area.
        silo.AddLatticeTreeAdminApi();

        // Backups behind the Backups area. A replicated tree must be backed by a
        // sink every region reads, so the estate registers the shared sink ahead
        // of AddLatticeBackup, whose in-cluster default is then skipped.
        if (sink is not null)
        {
            silo.Services.AddSingleton(sink);
        }

        silo.AddLatticeBackup();
        silo.AddLatticeBackupApi();

        ConfigureReplication(silo, plan);

        // Trusts the console's Basic sign-in: the username is the caller subject.
        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoBasicAuthenticator>();
    }

    private static void ConfigureIdentityDirectory(ISiloBuilder silo, ExplorerSampleOptions options)
    {
        if (options.Entra is { } entra)
        {
            // AddEntraGraphGroupResolver requires an Entra authenticator, but the
            // console still signs in over Basic; this one only governs bearer
            // callers, of which the console is not one.
            silo.AddEntraCredentialAuthenticator(entraAuth =>
            {
                entraAuth.Authority = $"https://login.microsoftonline.com/{entra.TenantId}/v2.0";
                entraAuth.TenantIds.Add(entra.TenantId);
                entraAuth.Audiences.Add(entra.ClientId);
                entraAuth.Audiences.Add($"api://{entra.ClientId}");
            });
            silo.AddEntraGraphGroupResolver(graph =>
            {
                graph.TenantId = entra.TenantId;
                graph.ClientId = entra.ClientId;
                graph.ClientSecret = entra.ClientSecret;
            });
            return;
        }

        // The static roster the Access area's picker searches and its create form
        // validates against: an id that is not listed fails closed.
        silo.AddStaticIdentityDirectory(roster => roster
            .AddUser(SampleIdentities.Administrator, "Explorer Administrator")
            .AddUser(SampleIdentities.AcmeAdmin, "Acme tenant administrator")
            .AddUser(SampleIdentities.GlobexAdmin, "Globex tenant administrator")
            .AddUser(SampleIdentities.Alice, "Alice Ng")
            .AddUser(SampleIdentities.Bob, "Bob Ito")
            .AddUser(SampleIdentities.Carol, "Carol Diaz")
            .AddGroup(SampleIdentities.OperatorsGroup, "Floor Operators")
            .AddGroup(SampleIdentities.TaskEditorsGroup, "Task board editors")
            .AddGroup(SampleIdentities.TaskViewersGroup, "Task board viewers")
            .AddGroup(SampleIdentities.VisitorsGroup, "Visitors")
            .AddGroup(SampleIdentities.AcmeEditorsGroup, "Acme task board editors"));
    }

    private static void ConfigureReplication(ISiloBuilder silo, SampleRegionPlan plan)
    {
        // The single-region run registers replication so the Replication area
        // reads one region with nothing behind it, and hosts no runtime control:
        // that replicates its own configuration tree, which needs a shared sink.
        if (plan.Peer is not { } peer)
        {
            silo.AddLatticeReplication(replication => replication.ClusterId = plan.Id);
            silo.AddLatticeReplicationStatusApi();
            return;
        }

        // The estate enables runtime replication control, so trees - including
        // an app's declared trees on install - are enrolled while it runs. The
        // liveness probe runs often enough that an idle link keeps fresh contact.
        silo.AddLatticeReplication(
            replication =>
            {
                replication.ClusterId = plan.Id;
                replication.ReplicationPeers = [peer.Id];
                replication.LivenessProbeInterval = TimeSpan.FromSeconds(5);
            },
            enableRuntimeConfig: true);
        silo.AddLatticeReplicationApi();

        // Health thresholds low enough that pausing the peer link turns a link
        // Lagging within seconds and Stalled within about a minute.
        silo.AddLatticeReplicationStatusApi(status =>
        {
            status.LaggingEntriesBehind = 10;
            status.StalledEntriesBehind = 60;
            status.LaggingAfterNoContact = TimeSpan.FromSeconds(20);
            status.StalledAfterNoContact = TimeSpan.FromSeconds(60);
        });

        // The cross-region transport: gRPC push over loopback h2c to the peer's
        // receiver, mapped on this region's gRPC endpoint.
        silo.Services.AddLatticeReplicationGrpc(grpc =>
        {
            grpc.Peers[peer.Id] = peer.Endpoint;
            grpc.AllowPlaintextEndpoints = true;
            grpc.LocalClusterId = plan.Id;
        });
        silo.Services.Configure<LatticeReplicationSecurityOptions>(security => security.RequireAuthentication = false);
    }

    private static void ConfigureGrpc(IServiceCollection services, SampleRegionPlan plan)
    {
        services.AddLatticeStateApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });
        services.AddLatticeAuthApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });
        services.AddLatticeSchemaApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });

        Action<LatticeAppsApiGrpcOptions> apps = o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        };
        services.AddLatticeAppsApiGrpc(apps);
        services.AddLatticeAppCatalogApiGrpc(apps);
        services.AddLatticeAppWorkspaceApiGrpc(apps);
        services.AddLatticeAppBridgeApiGrpc(apps);

        services.AddLatticeTreeAdminApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });
        services.AddLatticeBackupApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });

        // The replication binding's options carry the status read and, on the
        // estate, runtime enrolment.
        services.AddLatticeReplicationApiGrpc(o =>
        {
            o.RequireAuthorization = false;
            o.CredentialScheme = DemoBasicAuthenticator.Scheme;
        });
        services.AddLatticeReplicationStatusApiGrpc();

        if (plan.IsEstate)
        {
            services.AddLatticeTenantAdminApiGrpc(o =>
            {
                o.RequireAuthorization = false;
                o.CredentialScheme = DemoBasicAuthenticator.Scheme;
            });
        }
    }

    private static void ConfigureConsole(IServiceCollection services, SampleConsolePlan console)
    {
        // The console's first-run seed reads its endpoint and automatic sign-in
        // from this, ahead of the web head's TryAdd of the process environment.
        services.AddSingleton<IExplorerEnvironment>(new SampleExplorerEnvironment(console.Endpoint, console.SignInAs));

        // The one call a consumer makes to host the Explorer. The console's
        // configuration is pinned to a sample-owned file the sample clears on
        // start, so a previous run's endpoint can never hijack this one.
        services.AddLatticeExplorerWeb(web =>
        {
            web.ConfigFilePath = console.ConfigPath;

            // The environment credential seed signs every anonymous browser in
            // as the seeded identity. This is a single-operator loopback demo
            // whose point is the zero-login walkthrough, so it opts in here and
            // nowhere else.
            web.AllowEnvironmentCredentialSeed = true;
        });
    }

    private static void MapGrpc(WebApplication app, SampleRegionPlan plan)
    {
        app.MapLatticeStateApiGrpc();
        app.MapLatticeAuthApiGrpc();
        app.MapLatticeSchemaApiGrpc();
        app.MapLatticeAppsApiGrpc();
        app.MapLatticeAppCatalogApiGrpc();
        app.MapLatticeAppWorkspaceApiGrpc();
        app.MapLatticeAppBridgeApiGrpc();
        app.MapLatticeTreeAdminApiGrpc();
        app.MapLatticeBackupApiGrpc();
        app.MapLatticeReplicationStatusApiGrpc();

        if (plan.IsEstate)
        {
            app.MapLatticeReplicationApiGrpc();
            app.MapLatticeTenantAdminApiGrpc();
            app.MapLatticeReplicationGrpc();
        }
    }
}
