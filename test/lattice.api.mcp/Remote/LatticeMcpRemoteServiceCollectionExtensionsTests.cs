using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeMcpRemoteServiceCollectionExtensions.AddLatticeMcpRemote"/>
/// and the acceptance criteria of the remote-host topology. Proves the composition
/// surface registers a gRPC-backed facade adapter and its tool module only for a
/// configured group, wires the per-group endpoint source, and - via the discovery
/// core - that <c>lattice_capabilities</c> reports the configured per-group
/// endpoints while two callers with different grants still observe different tool
/// sets over the remote adapters. Deterministic - prebuilt fake call invokers,
/// no network.
/// </summary>
[TestFixture]
public sealed class LatticeMcpRemoteServiceCollectionExtensionsTests
{
    private static readonly FakeCallInvoker Idle = new(_ => throw new InvalidOperationException());

    private static LatticeApiMcpRemoteEndpoint Endpoint(string address)
        => new() { Endpoint = address, CallInvoker = Idle };

    [Test]
    public void AddLatticeMcpRemote_null_services_throws()
        => Assert.That(() => ((IServiceCollection)null!).AddLatticeMcpRemote(_ => { }), Throws.ArgumentNullException);

    [Test]
    public void AddLatticeMcpRemote_null_configure_throws()
        => Assert.That(() => new ServiceCollection().AddLatticeMcpRemote(null!), Throws.ArgumentNullException);

    [Test]
    public void All_facade_groups_are_registered_when_every_group_is_configured()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.State = Endpoint("https://state:5001");
                o.Data = Endpoint("https://data:5002");
                o.Auth = Endpoint("https://auth:5003");
                o.Backup = Endpoint("https://backup:5004");
                o.Replication = Endpoint("https://replication:5005");
            })
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<ILatticeStateQuery>(), Is.TypeOf<GrpcLatticeStateQuery>());
            Assert.That(provider.GetService<ILatticeDataApi>(), Is.TypeOf<GrpcLatticeDataApi>());
            Assert.That(provider.GetService<ILatticeAuthAdmin>(), Is.TypeOf<GrpcLatticeAuthAdmin>());
            Assert.That(provider.GetService<ILatticeBackupControl>(), Is.TypeOf<GrpcLatticeBackupControl>());
            Assert.That(provider.GetService<ILatticeReplicationControl>(), Is.TypeOf<GrpcLatticeReplicationControl>());
        });
    }

    [Test]
    public void Unconfigured_groups_register_no_facade()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o => o.State = Endpoint("https://state:5001"))
            .BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(provider.GetService<ILatticeStateQuery>(), Is.TypeOf<GrpcLatticeStateQuery>());
            Assert.That(provider.GetService<ILatticeDataApi>(), Is.Null);
            Assert.That(provider.GetService<ILatticeAuthAdmin>(), Is.Null);
            Assert.That(provider.GetService<ILatticeBackupControl>(), Is.Null);
            Assert.That(provider.GetService<ILatticeReplicationControl>(), Is.Null);
        });
    }

    [Test]
    public void Replication_control_flag_off_advertises_inspect_only()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o => o.Replication = Endpoint("https://replication:5005"))
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.EnableReplicationTools, Is.True);
            Assert.That(options.EnableReplicationControlTools, Is.False);
        });
    }

    [Test]
    public void Replication_control_flag_on_advertises_mutating_tools()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.Replication = Endpoint("https://replication:5005");
                o.EnableReplicationControl = true;
            })
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;
        Assert.Multiple(() =>
        {
            Assert.That(options.EnableReplicationTools, Is.True);
            Assert.That(options.EnableReplicationControlTools, Is.True);
        });
    }

    [Test]
    public void Endpoint_source_reports_configured_endpoints()
    {
        using var provider = new ServiceCollection()
            .AddLatticeMcpRemote(o =>
            {
                o.State = Endpoint("https://state:5001");
                o.Auth = Endpoint("https://auth:5003");
            })
            .BuildServiceProvider();

        var source = provider.GetRequiredService<ILatticeApiMcpGroupEndpointSource>();

        Assert.Multiple(() =>
        {
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.State), Is.EqualTo("https://state:5001"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Auth), Is.EqualTo("https://auth:5003"));
            Assert.That(source.EndpointFor(LatticeApiMcpGroup.Data), Is.Null);
        });
    }

    [Test]
    public async Task Capabilities_report_the_configured_per_group_endpoints()
    {
        var endpointSource = new LatticeApiMcpRemoteGroupEndpointSource(RemoteTestSupport.Options(o =>
        {
            o.State = Endpoint("https://state:5001");
            o.Data = Endpoint("https://data:5002");
        }));

        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State),
            endpointSource,
            new FakeToolGroup(LatticeApiMcpGroup.State, "state_read"));

        var plan = await configurator.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(EndpointOf(plan.Capabilities, LatticeApiMcpGroup.State), Is.EqualTo("https://state:5001"));
            Assert.That(EndpointOf(plan.Capabilities, LatticeApiMcpGroup.Data), Is.EqualTo("https://data:5002"));
            Assert.That(EndpointOf(plan.Capabilities, LatticeApiMcpGroup.Auth), Is.Null);
            Assert.That(EndpointOf(plan.Capabilities, LatticeApiMcpGroup.Backup), Is.Null);
        });
    }

    [Test]
    public async Task Two_callers_with_different_grants_see_different_tools_over_remote_adapters()
    {
        var endpointSource = new LatticeApiMcpRemoteGroupEndpointSource(RemoteTestSupport.Options(o =>
        {
            o.State = Endpoint("https://state:5001");
            o.Auth = Endpoint("https://auth:5003");
        }));

        var stateGroup = new FakeToolGroup(LatticeApiMcpGroup.State, "state_read");
        var authGroup = new FakeToolGroup(LatticeApiMcpGroup.Auth, "auth_admin");

        var stateCaller = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State),
            endpointSource, stateGroup, authGroup);
        var authCaller = CreateConfigurator(
            new LatticeCredential("bob"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Auth),
            endpointSource, stateGroup, authGroup);

        var statePlan = await stateCaller.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);
        var authPlan = await authCaller.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ToolNames(statePlan), Is.EquivalentTo(new[] { "lattice_capabilities", "state_read" }));
            Assert.That(ToolNames(authPlan), Is.EquivalentTo(new[] { "lattice_capabilities", "auth_admin" }));
        });
    }

    [Test]
    public async Task Unauthenticated_caller_gets_nothing_over_remote_adapters()
    {
        var endpointSource = new LatticeApiMcpRemoteGroupEndpointSource(RemoteTestSupport.Options(o =>
            o.State = Endpoint("https://state:5001")));

        var configurator = CreateConfigurator(
            credential: null,
            LatticeApiMcpAccessSet.None,
            endpointSource,
            new FakeToolGroup(LatticeApiMcpGroup.State, "state_read"));

        var plan = await configurator.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Tools, Is.Empty);
            Assert.That(plan.Capabilities.Authenticated, Is.False);
        });
    }

    [Test]
    public async Task Remote_topology_defers_unbindable_tools_from_a_fully_granted_caller()
    {
        var stateGroup = new FakeToolGroup(LatticeApiMcpGroup.State, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary,
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries,
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount,
            "lattice_state_list_trees",
        });
        var backupGroup = new FakeToolGroup(LatticeApiMcpGroup.Backup, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory,
            "lattice_backup_describe",
        });

        var configurator = CreateConfigurator(
            new LatticeCredential("root"),
            FullAccess(),
            endpointSource: null,
            new LatticeApiMcpRemoteUnsupportedToolSource(),
            stateGroup,
            backupGroup);

        var plan = await configurator.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);
        var names = ToolNames(plan);

        Assert.Multiple(() =>
        {
            // The four unbindable tools are omitted entirely - never listed-then-erroring.
            Assert.That(names, Does.Not.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary));
            Assert.That(names, Does.Not.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries));
            Assert.That(names, Does.Not.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount));
            Assert.That(names, Does.Not.Contain(LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory));
            // Every other tool in those groups is retained.
            Assert.That(names, Does.Contain("lattice_state_list_trees"));
            Assert.That(names, Does.Contain("lattice_backup_describe"));
            Assert.That(names, Does.Contain("lattice_capabilities"));
        });
    }

    [Test]
    public async Task In_silo_configurator_without_a_source_lists_every_tool()
    {
        var stateGroup = new FakeToolGroup(LatticeApiMcpGroup.State, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary,
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries,
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount,
            "lattice_state_list_trees",
        });
        var backupGroup = new FakeToolGroup(LatticeApiMcpGroup.Backup, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory,
            "lattice_backup_describe",
        });

        var configurator = CreateConfigurator(
            new LatticeCredential("root"),
            FullAccess(),
            endpointSource: null,
            unsupportedToolSource: null,
            stateGroup,
            backupGroup);

        var plan = await configurator.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);
        var names = ToolNames(plan);

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary));
            Assert.That(names, Does.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetShardSummaries));
            Assert.That(names, Does.Contain(LatticeApiMcpRemoteUnsupportedToolSource.StateGetPhysicalShardCount));
            Assert.That(names, Does.Contain(LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory));
            Assert.That(names, Does.Contain("lattice_state_list_trees"));
            Assert.That(names, Does.Contain("lattice_backup_describe"));
        });
    }

    [Test]
    public async Task Deferred_tools_groups_remain_available_in_capabilities()
    {
        var stateGroup = new FakeToolGroup(LatticeApiMcpGroup.State, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.StateGetTreeSummary,
            "lattice_state_list_trees",
        });
        var backupGroup = new FakeToolGroup(LatticeApiMcpGroup.Backup, new[]
        {
            LatticeApiMcpRemoteUnsupportedToolSource.BackupInventory,
            "lattice_backup_describe",
        });

        var configurator = CreateConfigurator(
            new LatticeCredential("root"),
            FullAccess(),
            endpointSource: null,
            new LatticeApiMcpRemoteUnsupportedToolSource(),
            stateGroup,
            backupGroup);

        var plan = await configurator.BuildSessionPlanAsync(HttpContext(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(AvailableOf(plan.Capabilities, LatticeApiMcpGroup.State), Is.True);
            Assert.That(AvailableOf(plan.Capabilities, LatticeApiMcpGroup.Backup), Is.True);
        });
    }

    private static LatticeApiMcpAccessSet FullAccess()
        => LatticeApiMcpAccessSet.None
            .With(LatticeApiMcpGroup.State)
            .With(LatticeApiMcpGroup.Data)
            .With(LatticeApiMcpGroup.Auth)
            .With(LatticeApiMcpGroup.Backup);

    private static LatticeApiMcpSessionConfigurator CreateConfigurator(
        LatticeCredential? credential,
        LatticeApiMcpAccessSet access,
        ILatticeApiMcpGroupEndpointSource endpointSource,
        params ILatticeApiMcpToolGroup[] toolGroups)
        => new(
            new StubBridge(credential),
            new StubResolver(access),
            toolGroups,
            AuthorizedServices(),
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance,
            endpointSource);

    private static LatticeApiMcpSessionConfigurator CreateConfigurator(
        LatticeCredential? credential,
        LatticeApiMcpAccessSet access,
        ILatticeApiMcpGroupEndpointSource? endpointSource,
        ILatticeApiMcpUnsupportedToolSource? unsupportedToolSource,
        params ILatticeApiMcpToolGroup[] toolGroups)
        => new(
            new StubBridge(credential),
            new StubResolver(access),
            toolGroups,
            AuthorizedServices(),
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance,
            endpointSource,
            unsupportedToolSource);

    private static IServiceProvider AuthorizedServices()
        => new ServiceCollection()
            .AddSingleton<ILatticeApiMcpAuthorizer, AllowAllMcpAuthorizer>()
            .BuildServiceProvider();

    private static DefaultHttpContext HttpContext()
        => new() { RequestServices = new ServiceCollection().BuildServiceProvider() };

    private static string? EndpointOf(LatticeApiMcpCapabilities capabilities, LatticeApiMcpGroup group)
        => capabilities.Groups.Single(g => g.Group == group).Endpoint;

    private static bool AvailableOf(LatticeApiMcpCapabilities capabilities, LatticeApiMcpGroup group)
        => capabilities.Groups.Single(g => g.Group == group).Available;

    private static HashSet<string> ToolNames(LatticeApiMcpSessionPlan plan)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tool in plan.Tools)
        {
            names.Add(tool.ProtocolTool.Name);
        }

        return names;
    }

    private sealed class StubBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }

    private sealed class StubResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(LatticeCredential credential, CancellationToken cancellationToken)
            => new(access);
    }

    private sealed class FakeToolGroup : ILatticeApiMcpToolGroup
    {
        public FakeToolGroup(LatticeApiMcpGroup group, string toolName)
            : this(group, new[] { toolName })
        {
        }

        public FakeToolGroup(LatticeApiMcpGroup group, IReadOnlyList<string> toolNames)
        {
            Group = group;
            var tools = new ModelContextProtocol.Server.McpServerTool[toolNames.Count];
            for (var i = 0; i < toolNames.Count; i++)
            {
                tools[i] = ModelContextProtocol.Server.McpServerTool.Create(
                    () => "ok",
                    new ModelContextProtocol.Server.McpServerToolCreateOptions { Name = toolNames[i] });
            }

            Tools = tools;
        }

        public LatticeApiMcpGroup Group { get; }

        public IReadOnlyList<ModelContextProtocol.Server.McpServerTool> Tools { get; }
    }
}
