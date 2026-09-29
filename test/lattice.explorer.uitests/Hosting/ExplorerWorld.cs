using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
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
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The suite's test world: a single-silo cluster, every control plane the Explorer
/// calls over gRPC, and the Explorer web head, co-hosted in one process the way the
/// Explorer sample runs, so every area renders over real data and the task-board
/// pilot app runs through the real registry, consent, broker and bridge.
/// </summary>
/// <remarks>
/// <para>
/// Identity is deliberately simple and deliberately real. The cluster trusts the
/// user name in a Basic credential (<see cref="TrustedUserAuthenticator"/>), so a test
/// signs in as <see cref="WorldIdentities.Admin"/>, <see cref="WorldIdentities.Alice"/>
/// and the others through the Explorer's own sign-in form. What each of them may do is
/// then decided by the real authorization engine over the rules and groups seeded here.
/// </para>
/// <para>
/// The seeded groups follow the task-board walkthrough: <c>alice</c> edits, <c>bob</c>
/// and <c>dave</c> view, <c>carol</c> is bound to no role. <c>bob</c> also holds a broad allow on the
/// whole cluster, which is what the isolation fixture needs to prove a viewer's write is
/// refused by the app's roles rather than by the viewer's own rights.
/// </para>
/// </remarks>
internal sealed class ExplorerWorld : IAsyncDisposable
{
    /// <summary>The demo tree the world seeds, readable by the <c>operators</c> group.</summary>
    public const string DemoTree = "factory-floor";

    /// <summary>How many entries <see cref="DemoTree"/> holds.</summary>
    public const int DemoEntryCount = 12;

    /// <summary>
    /// A tree of JSON orders for the schema rule builder's journey (issue #3963),
    /// one of which has a negative total, so a range rule has a failing sample.
    /// </summary>
    public const string OrdersTree = "schema-orders";

    private readonly GrpcChannel _adminChannel;

    private ExplorerWorld(ExplorerHead head, string grpcEndpoint)
    {
        Head = head;
        GrpcEndpoint = grpcEndpoint;
        _adminChannel = GrpcChannel.ForAddress(grpcEndpoint);
    }

    /// <summary>The world's own Explorer head.</summary>
    public ExplorerHead Head { get; }

    /// <summary>The <c>http://</c> (h2c) address of the world's gRPC surface.</summary>
    public string GrpcEndpoint { get; }

    /// <summary>Starts the world and seeds its data, groups and rules.</summary>
    public static async Task<ExplorerWorld> StartAsync()
    {
        var grpcPort = LoopbackEndpoints.ReservePort();
        var siloPort = LoopbackEndpoints.ReservePort();
        var gatewayPort = LoopbackEndpoints.ReservePort();
        var grpcEndpoint = $"http://127.0.0.1:{grpcPort}";

        var head = await ExplorerHead.StartAsync(new ExplorerHeadOptions
        {
            Endpoint = grpcEndpoint,
            ConfigureKestrel = kestrel => kestrel.Listen(System.Net.IPAddress.Loopback, grpcPort, listen => listen.Protocols = HttpProtocols.Http2),
            ConfigureBuilder = builder => ConfigureCluster(builder, siloPort, gatewayPort),
            ConfigureApp = MapClusterSurface,
        });

        var world = new ExplorerWorld(head, grpcEndpoint);
        await world.SeedAsync();
        return world;
    }

    /// <summary>
    /// The app-control facade as <paramref name="user"/> sees it, over the world's
    /// gRPC surface: what the Explorer would do on that user's behalf, used to arrange
    /// state a test is not itself exercising.
    /// </summary>
    /// <param name="user">The caller, <see cref="WorldIdentities.Admin"/> by default.</param>
    public ILatticeAppsControl AppsControl(string user = WorldIdentities.Admin) =>
        LatticeAppsApiGrpcClient.Create(Invoker(user), Head.Services);

    /// <summary>
    /// Installs the task-board app from the in-image source with the walkthrough's
    /// bindings (<c>editor</c> to <c>task-editors</c>, <c>viewer</c> to
    /// <c>task-viewers</c>) and enables it, replacing any earlier install.
    /// </summary>
    public async Task InstallTaskBoardAsync()
    {
        var control = AppsControl();
        await RemoveTaskBoardAsync();

        await control.InstallAsync(new AppInstallRequest
        {
            Slug = TaskBoardApp.Slug,
            Version = "1.0.0",
            SourceKey = "in-image",
            RoleBindings =
            [
                new AppRoleBindingDescriptor { RoleName = "editor", GroupId = WorldIdentities.EditorsGroup },
                new AppRoleBindingDescriptor { RoleName = "viewer", GroupId = WorldIdentities.ViewersGroup },
            ],
            Ceiling = new AppCapabilityCeilingDescriptor
            {
                AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete,
            },
        });

        var enabled = await control.EnableAsync(TaskBoardApp.Slug);
        if (enabled.State != AppLifecycleState.Enabled)
        {
            throw new InvalidOperationException(
                $"The task-board app did not enable in the test world; it reports {enabled.State}.");
        }
    }

    /// <summary>Uninstalls the task-board app if it is installed, so a test starts from nothing.</summary>
    public async Task RemoveTaskBoardAsync()
    {
        var control = AppsControl();
        var installed = await control.ListAsync();
        if (installed.Apps.Any(app => app.Slug == TaskBoardApp.Slug))
        {
            await control.UninstallAsync(TaskBoardApp.Slug);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _adminChannel.Dispose();
        await Head.DisposeAsync();
    }

    private CallInvoker Invoker(string user)
    {
        var header = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + WorldIdentities.Password));
        return _adminChannel.Intercept(metadata =>
        {
            metadata.Add("authorization", header);
            return metadata;
        });
    }

    private static void ConfigureCluster(WebApplicationBuilder builder, int siloPort, int gatewayPort)
    {
        builder.Host.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering(siloPort, gatewayPort, serviceId: "explorer-uitests", clusterId: "explorer-uitests-" + siloPort);
            silo.AddMemoryGrainStorageAsDefault();
            silo.UseInMemoryReminderService();
            silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));
            silo.AddLatticeStateApi();

            silo.AddLatticeMembership();
            silo.AddStaticIdentityDirectory(roster => roster
                .AddUser(WorldIdentities.Admin, "Explorer Administrator")
                .AddUser(WorldIdentities.Alice, "Alice Ng")
                .AddUser(WorldIdentities.Bob, "Bob Ito")
                .AddUser(WorldIdentities.Carol, "Carol Diaz")
                .AddUser(WorldIdentities.Dave, "Dave Okafor")
                .AddGroup(WorldIdentities.OperatorsGroup, "Floor Operators")
                .AddGroup(WorldIdentities.EditorsGroup, "Task board editors")
                .AddGroup(WorldIdentities.ViewersGroup, "Task board viewers")
                .AddGroup(WorldIdentities.VisitorsGroup, "Visitors"));
            silo.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.AllTreesGrantsEnabled = true;
                options.BootstrapAdministrators.Add(WorldIdentities.Admin);
            });
            silo.AddLatticeAuthApi();

            silo.AddLatticeSchemaEnforcement();
            silo.AddLatticeSchemaApi();

            silo.AddLatticeApps();
            silo.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);
            silo.AddLatticeAppsApi();
            silo.AddLatticeAppBridgeApi();

            silo.AddLatticeTreeAdminApi();
            silo.AddLatticeBackup();
            silo.AddLatticeBackupApi();
            silo.AddLatticeReplication(options => options.ClusterId = "explorer-uitests");
            silo.AddLatticeReplicationStatusApi();

            silo.Services.AddSingleton<ILatticeCredentialAuthenticator, TrustedUserAuthenticator>();
        });

        var services = builder.Services;
        services.AddLatticeStateApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        services.AddLatticeAuthApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        services.AddLatticeSchemaApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });

        Action<LatticeAppsApiGrpcOptions> apps = o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; };
        services.AddLatticeAppsApiGrpc(apps);
        services.AddLatticeAppCatalogApiGrpc(apps);
        services.AddLatticeAppWorkspaceApiGrpc(apps);
        services.AddLatticeAppBridgeApiGrpc(apps);

        services.AddLatticeTreeAdminApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        services.AddLatticeBackupApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        services.AddLatticeReplicationApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        services.AddLatticeReplicationStatusApiGrpc();
    }

    private static void MapClusterSurface(WebApplication app)
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
    }

    private async Task SeedAsync()
    {
        var services = Head.Services;
        var grains = services.GetRequiredService<IGrainFactory>();
        using (LatticeSystemOrigin.Enter())
        {
            var tree = grains.GetGrain<ILattice>(DemoTree);
            for (var i = 0; i < DemoEntryCount; i++)
            {
                await tree.SetAsync($"machine-{i:D3}", Encoding.UTF8.GetBytes($"status-{i:D3}"));
            }

            var orders = grains.GetGrain<ILattice>(OrdersTree);
            string[] totals = ["129.9", "18", "-5", "250", "9.99"];
            for (var i = 0; i < totals.Length; i++)
            {
                await orders.SetAsync(
                    $"order/{i + 1:D4}",
                    Encoding.UTF8.GetBytes($"{{\"total\":{totals[i]},\"status\":\"{(i % 2 == 0 ? "open" : "shipped")}\",\"email\":\"buyer{i}@example.com\"}}"));
            }

            var membership = services.GetRequiredService<ILatticeMembershipDirectory>();
            await membership.AddMemberAsync(WorldIdentities.OperatorsGroup, WorldIdentities.Alice);
            await membership.AddMemberAsync(WorldIdentities.EditorsGroup, WorldIdentities.Alice);
            await membership.AddMemberAsync(WorldIdentities.ViewersGroup, WorldIdentities.Bob);
            await membership.AddMemberAsync(WorldIdentities.ViewersGroup, WorldIdentities.Dave);
            await membership.AddMemberAsync(WorldIdentities.VisitorsGroup, WorldIdentities.Carol);

            var policy = services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
            await policy.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: "operators-read-factory-floor",
                subject: LatticeSubjectSelector.Group(WorldIdentities.OperatorsGroup),
                scope: LatticeScope.Tree(DemoTree),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                effect: LatticeEffect.Allow));

            // A viewer with broad rights of their own: every data operation, on the
            // whole cluster. The app's viewer role must still refuse a write.
            await policy.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: "bob-broad-data-rights",
                subject: LatticeSubjectSelector.User(WorldIdentities.Bob),
                scope: LatticeScope.ClusterWide(),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write
                    | LatticeOperation.Delete | LatticeOperation.RangeDelete,
                effect: LatticeEffect.Allow));
        }
    }
}
