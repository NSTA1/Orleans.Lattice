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
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Tenancy;

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
internal sealed partial class ExplorerWorld : IAsyncDisposable
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
    public static Task<ExplorerWorld> StartAsync() => StartCoreAsync(tenancy: false, delegatedAccess: false);

    /// <summary>
    /// Starts a world that also serves tenancy - the tenant registry, the tenant
    /// administration facades and the tenants in <see cref="Tenants"/>, each
    /// administered by <see cref="WorldIdentities.Admin"/> - so an operator can
    /// reach several tenants. It is a separate world, so the shared one keeps
    /// tenancy's single-tenant shape for every other fixture.
    /// </summary>
    public static Task<ExplorerWorld> StartWithTenancyAsync() => StartCoreAsync(tenancy: true, delegatedAccess: false);

    private static async Task<ExplorerWorld> StartCoreAsync(bool tenancy, bool delegatedAccess)
    {
        var grpcPort = LoopbackEndpoints.ReservePort();
        var siloPort = LoopbackEndpoints.ReservePort();
        var gatewayPort = LoopbackEndpoints.ReservePort();
        var grpcEndpoint = $"http://127.0.0.1:{grpcPort}";

        var head = await ExplorerHead.StartAsync(new ExplorerHeadOptions
        {
            Endpoint = grpcEndpoint,
            ConfigureKestrel = kestrel => kestrel.Listen(System.Net.IPAddress.Loopback, grpcPort, listen => listen.Protocols = HttpProtocols.Http2),
            ConfigureBuilder = builder => ConfigureCluster(builder, siloPort, gatewayPort, tenancy, delegatedAccess),
            ConfigureApp = app => MapClusterSurface(app, tenancy),
        });

        var world = new ExplorerWorld(head, grpcEndpoint);
        await world.SeedAsync();
        if (delegatedAccess)
        {
            await world.SeedDelegatedAccessAsync();
        }
        else if (tenancy)
        {
            await world.SeedTenantsAsync();
        }

        return world;
    }

    /// <summary>The tenants a world started with <see cref="StartWithTenancyAsync"/> serves, beside the reserved default.</summary>
    public static IReadOnlyList<string> Tenants { get; } = ["acme", "globex"];

    /// <summary>
    /// The app-control facade as <paramref name="user"/> sees it, over the world's
    /// gRPC surface: what the Explorer would do on that user's behalf, used to arrange
    /// state a test is not itself exercising.
    /// </summary>
    /// <param name="user">The caller, <see cref="WorldIdentities.Admin"/> by default.</param>
    /// <param name="tenant">
    /// The tenant the calls assert, as the Explorer asserts a circuit's tenant, or
    /// <see langword="null"/> (the default) to assert none - the reserved default tenant.
    /// </param>
    public ILatticeAppsControl AppsControl(string user = WorldIdentities.Admin, string? tenant = null) =>
        LatticeAppsApiGrpcClient.Create(Invoker(user, tenant), Head.Services);

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

    /// <summary>
    /// Uninstalls the task-board app if it is installed, so a test starts from nothing.
    /// An install is per tenant, and uninstalling it withdraws the role rules it wrote there.
    /// </summary>
    /// <param name="tenant">The tenant to uninstall it from, or <see langword="null"/> (the default) for the reserved default tenant.</param>
    public async Task RemoveTaskBoardAsync(string? tenant = null)
    {
        var control = AppsControl(tenant: tenant);
        var installed = await control.ListAsync();
        if (installed.Apps.Any(app => app.Slug == TaskBoardApp.Slug))
        {
            await control.UninstallAsync(TaskBoardApp.Slug);
        }
    }

    /// <summary>
    /// Creates <paramref name="treeId"/> holding <paramref name="entries"/> keys, for a
    /// test that needs a tree of its own to change - to delete or purge, say - without
    /// touching the trees other fixtures read.
    /// </summary>
    /// <param name="treeId">The tree to create; it must not exist yet.</param>
    /// <param name="entries">How many keys it holds.</param>
    /// <param name="shardCount">The physical shard count to create it with, or <see langword="null"/> for the default.</param>
    public async Task SeedTreeAsync(string treeId, int entries, int? shardCount = null)
    {
        var grains = Head.Services.GetRequiredService<IGrainFactory>();
        using (LatticeSystemOrigin.Enter())
        {
            if (shardCount is { } shards)
            {
                await Head.Services.GetRequiredService<ILatticeTreeAdmin>().CreateTreeAsync(treeId, shardCount: shards);
            }

            var tree = grains.GetGrain<ILattice>(treeId);
            for (var i = 0; i < entries; i++)
            {
                await tree.SetAsync($"key-{i:D3}", Encoding.UTF8.GetBytes($"value-{i:D3}"));
            }
        }
    }

    /// <summary>
    /// Removes the group record <paramref name="groupId"/> if there is one, so a journey
    /// that creates it starts from the same state however often the world has run it.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    public async Task RemoveGroupAsync(string groupId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Head.Services.GetRequiredService<ILatticeMembershipDirectory>().RemoveGroupAsync(groupId);
        }
    }

    /// <summary>
    /// Removes <paramref name="memberId"/> from <paramref name="groupId"/> if it is a member,
    /// so a journey that joins it starts from the same state however often the world has run it.
    /// </summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="memberId">The member id.</param>
    public async Task RemoveMemberAsync(string groupId, string memberId)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Head.Services.GetRequiredService<ILatticeMembershipDirectory>().RemoveMemberAsync(groupId, memberId);
        }
    }

    /// <summary>
    /// The region this world serves: its cluster id. A tenant with residency is
    /// served here only while its status in this region is Online.
    /// </summary>
    public string ServingRegion =>
        Head.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<Orleans.Configuration.ClusterOptions>>().Value.ClusterId;

    /// <summary>
    /// Makes <paramref name="tenant"/> resident and Online in <paramref name="regions"/>,
    /// the way the Explorer sample seeds acme in its two regions: as the operator it
    /// allows them and sets them as the residency, which starts each Provisioning,
    /// then promotes each to Online one step at a time, as an operator of the
    /// hosting deployment does. Repeatable: a region drained by an earlier run is
    /// added again and promoted.
    /// </summary>
    /// <param name="tenant">One of <see cref="Tenants"/>.</param>
    /// <param name="regions">The regions; include <see cref="ServingRegion"/> to keep the tenant served in this world.</param>
    public async Task MakeResidentAndOnlineAsync(string tenant, params string[] regions)
    {
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(WorldIdentities.Admin + ":" + WorldIdentities.Password));
        using (LatticeCredentialContext.Use(token, scheme: TrustedUserAuthenticator.Scheme))
        {
            var admin = Head.Services.GetRequiredService<ILatticeTenantRegionAdmin>();
            await admin.AuthorizeAllowedRegionsAsync(tenant, regions);
            await admin.SetResidencyAsync(tenant, regions);
        }

        var registry = Head.Services.GetRequiredService<ITenantRegistry>();
        var id = TenantId.Parse(tenant);
        using (LatticeSystemOrigin.Enter())
        {
            foreach (var region in regions)
            {
                while (await registry.GetAsync(id) is { } record
                    && record.GetRegionStatus(region) is TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling
                    && record.TryPromoteRegionStatus(region, ServingRegion, out _))
                {
                    await registry.PutAsync(record);
                }
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _adminChannel.Dispose();
        await Head.DisposeAsync();
    }

    private CallInvoker Invoker(string user, string? tenant = null)
    {
        var header = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + WorldIdentities.Password));
        return _adminChannel.Intercept(metadata =>
        {
            metadata.Add("authorization", header);
            if (!string.IsNullOrEmpty(tenant))
            {
                metadata.Add(LatticeActiveTenantAssertion.DefaultHeaderName, tenant);
            }

            return metadata;
        });
    }

    private static void ConfigureCluster(WebApplicationBuilder builder, int siloPort, int gatewayPort, bool tenancy, bool delegatedAccess)
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
                .AddUser(WorldIdentities.GlobexAdmin, "Globex tenant administrator")
                .AddGroup(WorldIdentities.OperatorsGroup, "Floor Operators")
                .AddGroup(WorldIdentities.EditorsGroup, "Task board editors")
                .AddGroup(WorldIdentities.ViewersGroup, "Task board viewers")
                .AddGroup(WorldIdentities.VisitorsGroup, "Visitors")
                .AddGroup(WorldIdentities.AuditorsGroup, "Auditors"));
            silo.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.AllTreesGrantsEnabled = true;
                options.BootstrapAdministrators.Add(WorldIdentities.Admin);
            });
            silo.AddLatticeAuthApi();

            if (tenancy)
            {
                silo.AddLatticeTenancy(options => options.DelegatedAccessAdministrationEnabled = delegatedAccess);
                silo.AddLatticeTenantAdminApi();
            }

            silo.AddLatticeSchemaEnforcement();
            silo.AddLatticeSchemaApi();

            silo.AddLatticeApps();
            silo.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);
            silo.AddLatticeAppsApi();
            silo.AddLatticeAppBridgeApi();

            silo.AddLatticeTreeAdminApi();
            silo.Services.AddSingleton<ILatticeBackupSink>(new Orleans.Lattice.Samples.Explorer.SampleSharedBackupSink());
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
        if (tenancy)
        {
            services.AddLatticeTenantAdminApiGrpc(o => { o.RequireAuthorization = false; o.CredentialScheme = TrustedUserAuthenticator.Scheme; });
        }
    }

    private static void MapClusterSurface(WebApplication app, bool tenancy)
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
        if (tenancy)
        {
            app.MapLatticeTenantAdminApiGrpc();
        }
    }

    private async Task SeedTenantsAsync()
    {
        // As the operator, through the same facade the Tenancy area calls: the
        // tenant directory lists the tenants its caller administers.
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(WorldIdentities.Admin + ":" + WorldIdentities.Password));
        using (LatticeCredentialContext.Use(token, scheme: TrustedUserAuthenticator.Scheme))
        {
            var admin = Head.Services.GetRequiredService<ILatticeTenantAdmin>();
            foreach (var tenant in Tenants)
            {
                await admin.CreateTenantAsync(tenant, [WorldIdentities.Admin]);
            }
        }

        // One rule per tenant, on that tenant's own tree, so a tenant-rooted Access
        // listing has something of its own to show and another tenant's to leave out.
        var policy = Head.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        using (LatticeSystemOrigin.Enter())
        {
            foreach (var tenant in Tenants)
            {
                await policy.PutRuleAsync(new LatticeAuthorizationRule(
                    ruleId: TenantRuleId(tenant),
                    subject: LatticeSubjectSelector.Group(WorldIdentities.OperatorsGroup),
                    scope: LatticeScope.Tree($"t/{tenant}/{OrdersTree}"),
                    operations: LatticeOperation.Read,
                    effect: LatticeEffect.Allow));
            }
        }
    }

    /// <summary>The id of the rule a tenancy world seeds on <paramref name="tenant"/>'s own orders tree.</summary>
    /// <param name="tenant">One of <see cref="Tenants"/>.</param>
    public static string TenantRuleId(string tenant) => tenant + "-operators-read-orders";

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

            // A membership edge alone makes no group: each seeded group gets its record,
            // so Access lists it and refuses its id as a new group's.
            await membership.UpsertGroupAsync(new MembershipGroup(WorldIdentities.OperatorsGroup, "Floor Operators"));
            await membership.UpsertGroupAsync(new MembershipGroup(WorldIdentities.EditorsGroup, "Task board editors"));
            await membership.UpsertGroupAsync(new MembershipGroup(WorldIdentities.ViewersGroup, "Task board viewers"));
            await membership.UpsertGroupAsync(new MembershipGroup(WorldIdentities.VisitorsGroup, "Visitors"));
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
