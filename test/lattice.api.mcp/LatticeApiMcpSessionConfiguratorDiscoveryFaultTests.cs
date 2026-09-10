using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit coverage for how <c>LatticeApiMcpSessionConfigurator</c> degrades when
/// building a session plan goes wrong: the cluster-identity resolve failing
/// transiently versus authoritatively, a tool-name collision between two granted
/// groups, and the capabilities meta-tool's own invocation.
/// <para>
/// The transient/authoritative split is the load-bearing one and it is a
/// security property, not a robustness nicety. The advertised tool list <i>is</i>
/// the caller's permission surface, so a session plan assembled from answers
/// that never arrived would present a permission-scoped advertisement backed by
/// nothing. A transient fault therefore fails the whole discovery with a
/// retryable error - strictly narrower than answering - while a fault the
/// backend really did produce leaves only the decorative cluster identity
/// unresolved, because an absent cluster id cannot be mistaken for a narrower
/// grant set.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpSessionConfiguratorDiscoveryFaultTests
{
    private static LatticeApiMcpSessionConfigurator CreateConfigurator(
        LatticeApiMcpAccessSet access,
        params ILatticeApiMcpToolGroup[] toolGroups)
    {
        var services = new ServiceCollection()
            .AddSingleton<ILatticeApiMcpAuthorizer>(new AllowAllMcpAuthorizer())
            .BuildServiceProvider();
        return new(
            new FakeBridge(new LatticeCredential("alice")),
            new FakeResolver(access),
            toolGroups,
            services,
            NullLogger<LatticeApiMcpSessionConfigurator>.Instance);
    }

    private static DefaultHttpContext ContextWith(ILatticeStateQuery? stateQuery = null)
    {
        var services = new ServiceCollection();
        if (stateQuery is not null)
        {
            services.AddSingleton(stateQuery);
        }

        return new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
    }

    private static ILatticeStateQuery StateQueryThrowing(Exception fault)
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>()).ThrowsAsync(fault);
        return query;
    }

    private static List<string> ToolNames(McpServerPrimitiveCollection<McpServerTool> tools)
    {
        var names = new List<string>();
        foreach (var tool in tools)
        {
            names.Add(tool.ProtocolTool.Name);
        }

        return names;
    }

    // --- the transient arm: the answer never arrived, so advertise nothing ---

    [TestCaseSource(nameof(TransientFaults))]
    public void A_transient_backend_fault_fails_the_whole_discovery_rather_than_advertising(Exception fault)
    {
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"));

        var thrown = Assert.ThrowsAsync<LatticeApiMcpDiscoveryUnavailableException>(
            () => configurator.BuildSessionPlanAsync(ContextWith(StateQueryThrowing(fault)), CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.InnerException, Is.SameAs(fault),
                "The original fault must be preserved, or an operator cannot tell a "
                + "deadline from silo churn.");
            Assert.That(thrown.Message, Does.Contain("Retry"),
                "The caller has to be told this is retryable; a plan built from a "
                + "missing answer would otherwise be indistinguishable from a real "
                + "permission set.");
        });
    }

    private static IEnumerable<TestCaseData> TransientFaults()
    {
        yield return new TestCaseData(new TimeoutException("orleans response deadline"))
            .SetName("A_transient_backend_fault_fails_discovery_TimeoutException");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.Unavailable, "no route")))
            .SetName("A_transient_backend_fault_fails_discovery_RpcUnavailable");
        yield return new TestCaseData(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow")))
            .SetName("A_transient_backend_fault_fails_discovery_RpcDeadlineExceeded");
    }

    // --- the authoritative arm: the backend answered, so only the id is lost ---

    [Test]
    public async Task An_authoritative_fault_leaves_the_cluster_identity_blank_and_still_serves_the_plan()
    {
        // The backend replied, so the caller's grants were resolved from real
        // answers and the advertisement is trustworthy. Only the cluster
        // identity - a decorative field - is missing, and blank cannot be
        // misread as a narrower permission set.
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"));
        var fault = new InvalidOperationException("cluster info projection is malformed");

        var plan = await configurator.BuildSessionPlanAsync(
            ContextWith(StateQueryThrowing(fault)), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Capabilities.ClusterId, Is.Empty);
            Assert.That(plan.Capabilities.ServiceId, Is.Empty);
            Assert.That(ToolNames(plan.Tools), Does.Contain("data_read"),
                "A decorative field failing must not narrow the tool list - that would "
                + "silently revoke a caller's tools over a cosmetic fault.");
            Assert.That(plan.Capabilities.Authenticated, Is.True);
        });
    }

    [Test]
    public void A_cancellation_is_neither_transient_nor_authoritative_and_propagates_unchanged()
    {
        // Both catch filters exclude OperationCanceledException. Translating it
        // into a discovery error would report the caller's own cancellation as
        // a backend outage, and swallowing it would hide a cancelled request
        // behind a plan nobody asked for.
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"));

        Assert.ThrowsAsync<OperationCanceledException>(
            () => configurator.BuildSessionPlanAsync(
                ContextWith(StateQueryThrowing(new OperationCanceledException())),
                CancellationToken.None));
    }

    [Test]
    public async Task A_resolvable_cluster_identity_is_stamped_into_the_capabilities()
    {
        // Positive control for the two fault arms above: without it, a
        // configurator that never stamped a cluster identity at all would pass
        // the "blank on fault" assertion too.
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = "cluster-a", ServiceId = "svc-a" });
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(query), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(plan.Capabilities.ClusterId, Is.EqualTo("cluster-a"));
            Assert.That(plan.Capabilities.ServiceId, Is.EqualTo("svc-a"));
        });
    }

    // --- tool-name collision between two granted groups ---

    [Test]
    public async Task A_tool_name_claimed_by_two_granted_groups_is_added_once_and_the_loser_is_skipped()
    {
        // The session's tool collection is keyed by name and serves both
        // tools/list and tools/call, so a duplicate cannot simply overwrite:
        // whichever group won would silently decide which facade every
        // invocation of that name reaches. Skipping keeps the first
        // registration authoritative and leaves a warning behind, which is the
        // only trace an operator gets of a packaging mistake.
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data).With(LatticeApiMcpGroup.Auth),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "shared_name"),
            new FakeToolGroup(LatticeApiMcpGroup.Auth, "shared_name", "auth_only"));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        var names = ToolNames(plan.Tools);
        Assert.Multiple(() =>
        {
            Assert.That(names.Count(n => n == "shared_name"), Is.EqualTo(1),
                "A colliding name must appear exactly once, or the collection is ambiguous.");
            Assert.That(names, Does.Contain("auth_only"),
                "The collision must skip only the duplicate tool, not abandon the rest "
                + "of the group that raised it.");
        });
    }

    [Test]
    public async Task Distinct_tool_names_across_two_granted_groups_are_all_added()
    {
        // Positive control for the collision test: a configurator that dropped
        // every tool from the second group would also report exactly one
        // "shared_name".
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data).With(LatticeApiMcpGroup.Auth),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"),
            new FakeToolGroup(LatticeApiMcpGroup.Auth, "auth_admin"));

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(), CancellationToken.None);

        Assert.That(ToolNames(plan.Tools), Is.EquivalentTo(
            new[] { "lattice_capabilities", "lattice_list_regions", "data_read", "auth_admin" }));
    }

    // --- the capabilities meta-tool's own body ---

    [Test]
    public async Task Invoking_the_capabilities_tool_returns_the_session_plans_capabilities()
    {
        // The meta-tool is built as an SDK delegate closed over the plan's
        // capabilities, so its body only runs on a real tools/call - asserting
        // on the tool's protocol metadata never reaches it. Driving the
        // delegate is what proves the advertised report and the invoked report
        // are the same object rather than two independently-built ones that
        // could drift.
        var services = new ServiceCollection().BuildServiceProvider();
        var configurator = CreateConfigurator(
            LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.Data),
            new FakeToolGroup(LatticeApiMcpGroup.Data, "data_read"));
        var query = Substitute.For<ILatticeStateQuery>();
        query.GetClusterInfoAsync(Arg.Any<CancellationToken>())
            .Returns(new ClusterInfo { ClusterId = "cluster-a", ServiceId = "svc-a" });

        var plan = await configurator.BuildSessionPlanAsync(ContextWith(query), CancellationToken.None);
        var capabilitiesTool = plan.Tools.Single(t => t.ProtocolTool.Name == "lattice_capabilities");

        var result = await McpToolInvocation.CallAsync(capabilitiesTool, services);

        Assert.That(result.IsError ?? false, Is.False, "The meta-tool must not fault on invoke.");
        var payload = System.Text.Json.JsonSerializer.Serialize(result.StructuredContent);
        Assert.Multiple(() =>
        {
            Assert.That(payload, Does.Contain("cluster-a"),
                "The invoked report must carry the same cluster identity the plan advertised.");
            Assert.That(payload, Does.Contain("svc-a"));
        });
    }

    // --- fakes ---

    private sealed class FakeBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }

    private sealed class FakeResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
    {
        public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
            LatticeCredential credential,
            CancellationToken cancellationToken)
            => new(access);
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
