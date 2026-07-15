using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpSessionConfigurator"/>, the
/// permission-aware discovery core. Proves per-session tool filtering (two
/// callers with different grants observe different tool sets and different
/// capabilities), fail-closed behaviour for an unauthenticated session, that a
/// group is offered only when registered <b>and</b> granted, the cluster-identity
/// projection, the instructions text, and the <c>ConfigureAsync</c> application
/// onto <see cref="McpServerOptions"/>. All deterministic - fakes, no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpSessionConfiguratorTests
{
    private static LatticeApiMcpSessionConfigurator CreateConfigurator(
        LatticeCredential? credential,
        LatticeApiMcpAccessSet access,
        params ILatticeApiMcpToolGroup[] toolGroups)
        => new(
            new FakeBridge(credential),
            new FakeResolver(_ => access),
            toolGroups,
            new ServiceCollection().BuildServiceProvider(),
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance);

    private static DefaultHttpContext ContextWith(ILatticeStateQuery? stateQuery = null)
    {
        var services = new ServiceCollection();
        if (stateQuery is not null)
        {
            services.AddSingleton(stateQuery);
        }

        return new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
    }

    private static ILatticeStateQuery StateQueryReturning(string clusterId, string serviceId)
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = clusterId, ServiceId = serviceId });
        return query;
    }

    private static HashSet<string> ToolNames(McpServerPrimitiveCollection<McpServerTool> tools)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tool in tools)
        {
            names.Add(tool.ProtocolTool.Name);
        }

        return names;
    }

    private static bool GroupAvailable(LatticeApiMcpCapabilities capabilities, LatticeApiMcpGroup group)
        => capabilities.Groups.Single(g => g.Group == group).Available;

    [Test]
    public async Task Two_callers_with_different_grants_see_different_tool_sets()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");
        var authGroup = new FakeToolGroup(LatticeApiMcpGroup.Auth, "auth_admin");

        var dataCaller = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            dataGroup, authGroup);
        var authCaller = CreateConfigurator(
            new LatticeCredential("bob"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Auth),
            dataGroup, authGroup);

        var dataPlan = await dataCaller.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);
        var authPlan = await authCaller.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ToolNames(dataPlan.Tools), Is.EquivalentTo(new[] { "lattice_capabilities", "data_read" }));
            Assert.That(ToolNames(authPlan.Tools), Is.EquivalentTo(new[] { "lattice_capabilities", "auth_admin" }));
            Assert.That(GroupAvailable(dataPlan.Capabilities, LatticeApiMcpGroup.Data), Is.True);
            Assert.That(GroupAvailable(dataPlan.Capabilities, LatticeApiMcpGroup.Auth), Is.False);
            Assert.That(GroupAvailable(authPlan.Capabilities, LatticeApiMcpGroup.Auth), Is.True);
            Assert.That(GroupAvailable(authPlan.Capabilities, LatticeApiMcpGroup.Data), Is.False);
        });
    }

    [Test]
    public async Task Ungranted_group_tools_are_absent_not_listed_then_denied()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");
        var authGroup = new FakeToolGroup(LatticeApiMcpGroup.Auth, "auth_admin");

        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            dataGroup, authGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(ToolNames(plan.Tools), Does.Not.Contain("auth_admin"),
            "An ungranted group must contribute no tools at all.");
    }

    [Test]
    public async Task Unauthenticated_session_gets_no_tools_and_no_groups()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");

        var configurator = CreateConfigurator(
            credential: null,
            LatticeApiMcpAccessSet.None,
            dataGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Tools, Is.Empty, "A fail-closed anonymous session is offered nothing, not even the meta-tool.");
            Assert.That(plan.Capabilities.Authenticated, Is.False);
            Assert.That(plan.Capabilities.SubjectId, Is.Null);
            foreach (var group in plan.Capabilities.Groups)
            {
                Assert.That(group.Available, Is.False);
            }

            Assert.That(plan.Instructions, Does.Contain("not authenticated"));
        });
    }

    [Test]
    public async Task Authenticated_caller_with_no_grants_gets_only_the_meta_tool()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");

        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None,
            dataGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ToolNames(plan.Tools), Is.EquivalentTo(new[] { "lattice_capabilities" }));
            Assert.That(plan.Capabilities.Authenticated, Is.True);
            Assert.That(plan.Capabilities.SubjectId, Is.EqualTo("alice"));
            Assert.That(plan.Instructions, Does.Contain("No facade groups are available"));
        });
    }

    [Test]
    public async Task Group_available_requires_both_a_grant_and_a_registered_module()
    {
        // Caller is granted State and Data, but only the Data module is registered.
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");

        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.State).With(LatticeApiMcpGroup.Data),
            dataGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(GroupAvailable(plan.Capabilities, LatticeApiMcpGroup.Data), Is.True);
            Assert.That(GroupAvailable(plan.Capabilities, LatticeApiMcpGroup.State), Is.False,
                "A granted-but-unregistered group is not usable and must report unavailable.");
            Assert.That(ToolNames(plan.Tools), Is.EquivalentTo(new[] { "lattice_capabilities", "data_read" }));
        });
    }

    [Test]
    public async Task Capabilities_carry_the_cluster_identity()
    {
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None);

        var plan = await configurator.BuildSessionPlanAsync(
            ContextWith(StateQueryReturning("cluster-7", "svc-3")),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Capabilities.ClusterId, Is.EqualTo("cluster-7"));
            Assert.That(plan.Capabilities.ServiceId, Is.EqualTo("svc-3"));
            Assert.That(plan.Instructions, Does.Contain("cluster-7"));
        });
    }

    [Test]
    public async Task Missing_state_facade_leaves_cluster_identity_empty()
    {
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Capabilities.ClusterId, Is.Empty);
            Assert.That(plan.Capabilities.ServiceId, Is.Empty);
        });
    }

    [Test]
    public async Task Telemetry_group_is_unavailable_in_core_without_a_registered_module_even_when_granted()
    {
        // B1 lands no telemetry tools in core: a granted-but-unregistered
        // telemetry group is discoverable but not usable until the companion
        // package registers its tool module.
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Telemetry));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(GroupAvailable(plan.Capabilities, LatticeApiMcpGroup.Telemetry), Is.False);
    }

    [Test]
    public async Task Telemetry_group_is_available_when_granted_and_a_module_is_registered()
    {
        var telemetryGroup = new FakeToolGroup(LatticeApiMcpGroup.Telemetry, "telemetry_read");
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Telemetry),
            telemetryGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(GroupAvailable(plan.Capabilities, LatticeApiMcpGroup.Telemetry), Is.True);
            Assert.That(ToolNames(plan.Tools), Is.EquivalentTo(new[] { "lattice_capabilities", "telemetry_read" }));
        });
    }

    [Test]
    public async Task Every_group_capability_slot_is_present_in_declaration_order()
    {
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(
            plan.Capabilities.Groups.Select(g => g.Group),
            Is.EqualTo(new[]
            {
                LatticeApiMcpGroup.State,
                LatticeApiMcpGroup.Data,
                LatticeApiMcpGroup.Backup,
                LatticeApiMcpGroup.Auth,
                LatticeApiMcpGroup.Telemetry,
            }));
    }

    [Test]
    public async Task Group_endpoint_is_null_for_the_in_silo_topology()
    {
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(plan.Capabilities.Groups.All(g => g.Endpoint is null), Is.True);
    }

    [Test]
    public async Task Subject_id_falls_back_to_the_token_when_no_principal_id()
    {
        var configurator = CreateConfigurator(
            new LatticeCredential("opaque-token"),
            LatticeApiMcpAccessSet.None);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(plan.Capabilities.SubjectId, Is.EqualTo("opaque-token"));
    }

    [Test]
    public async Task ConfigureAsync_applies_the_plan_and_advertises_list_changed()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            dataGroup);
        var options = new McpServerOptions();

        await configurator.ConfigureAsync(ContextWith(), options, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(options.ToolCollection, Is.Not.Null);
            Assert.That(ToolNames(options.ToolCollection!), Is.EquivalentTo(new[] { "lattice_capabilities", "data_read" }));
            Assert.That(options.Capabilities?.Tools?.ListChanged, Is.True);
            Assert.That(options.ServerInstructions, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task ConfigureAsync_leaves_an_anonymous_session_with_an_empty_tool_collection()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");
        var configurator = CreateConfigurator(credential: null, LatticeApiMcpAccessSet.None, dataGroup);
        var options = new McpServerOptions();

        await configurator.ConfigureAsync(ContextWith(), options, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(options.ToolCollection, Is.Not.Null);
            Assert.That(options.ToolCollection!, Is.Empty);
            Assert.That(options.Capabilities?.Tools?.ListChanged, Is.True);
        });
    }

    [Test]
    public void ConfigureAsync_rejects_null_arguments()
    {
        var configurator = CreateConfigurator(new LatticeCredential("alice"), LatticeApiMcpAccessSet.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                () => configurator.ConfigureAsync(null!, new McpServerOptions(), CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                () => configurator.ConfigureAsync(ContextWith(), null!, CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Granted_group_tools_are_wrapped_for_credential_stamping()
    {
        var dataGroup = new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read");
        var configurator = CreateConfigurator(
            new LatticeCredential("alice"),
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            dataGroup);

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        var groupTool = plan.Tools.Single(t => t.ProtocolTool.Name == "data_read");
        var metaTool = plan.Tools.Single(t => t.ProtocolTool.Name == "lattice_capabilities");

        Assert.Multiple(() =>
        {
            Assert.That(groupTool, Is.InstanceOf<CredentialStampingTool>(),
                "A facade-backed group tool must be wrapped so the caller's credential is stamped for its invocation.");
            Assert.That(metaTool, Is.Not.InstanceOf<CredentialStampingTool>(),
                "The capabilities meta-tool performs no facade call and is not wrapped.");
        });
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var bridge = new FakeBridge(null);
        var resolver = new FakeResolver(_ => LatticeApiMcpAccessSet.None);
        var groups = Array.Empty<ILatticeApiMcpToolGroup>();
        var provider = new ServiceCollection().BuildServiceProvider();
        var logger = NullLogger<LatticeApiMcpSessionConfigurator>.Instance;

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new LatticeApiMcpSessionConfigurator(null!, resolver, groups, provider, logger));
            Assert.Throws<ArgumentNullException>(() => new LatticeApiMcpSessionConfigurator(bridge, null!, groups, provider, logger));
            Assert.Throws<ArgumentNullException>(() => new LatticeApiMcpSessionConfigurator(bridge, resolver, null!, provider, logger));
            Assert.Throws<ArgumentNullException>(() => new LatticeApiMcpSessionConfigurator(bridge, resolver, groups, null!, logger));
            Assert.Throws<ArgumentNullException>(() => new LatticeApiMcpSessionConfigurator(bridge, resolver, groups, provider, null!));
        });
    }

    private sealed class FakeBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }

    private sealed class FakeResolver(Func<LatticeCredential, LatticeApiMcpAccessSet> map)
        : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken)
            => new(map(credential));
    }

    private sealed class FakeToolGroup : ILatticeApiMcpToolGroup
    {
        public FakeToolGroup(LatticeApiMcpGroup group, params string[] toolNames)
        {
            Group = group;
            var tools = new McpServerTool[toolNames.Length];
            for (var i = 0; i < toolNames.Length; i++)
            {
                tools[i] = McpServerTool.Create(
                    () => "ok",
                    new McpServerToolCreateOptions { Name = toolNames[i] });
            }

            Tools = tools;
        }

        public LatticeApiMcpGroup Group { get; }

        public IReadOnlyList<McpServerTool> Tools { get; }
    }
}
