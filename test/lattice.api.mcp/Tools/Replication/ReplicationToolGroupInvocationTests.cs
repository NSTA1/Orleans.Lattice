using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests that drive every <see cref="ReplicationToolGroup"/> tool's own
/// invocation delegate through <see cref="McpToolInvocation"/>: the body that
/// stamps the caller credential, resolves
/// <see cref="ILatticeReplicationControl"/> from the request service provider,
/// and forwards the bound arguments to <c>ReplicationToolInvocations</c>. The
/// sibling <see cref="ReplicationToolGroupTests"/> covers only the advertised
/// metadata, which never reaches these bodies.
/// </summary>
/// <remarks>
/// All deterministic against a substituted facade - no cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class ReplicationToolGroupInvocationTests
{
    private ILatticeReplicationControl _control = null!;

    [SetUp]
    public void SetUp() => _control = Substitute.For<ILatticeReplicationControl>();

    private ServiceProvider Services()
        => new ServiceCollection().AddSingleton(_control).BuildServiceProvider();

    private static McpServerTool Tool(string name)
        => new ReplicationToolGroup(
                Options.Create(new LatticeApiMcpOptions { EnableReplicationControlTools = true }))
            .Tools.Single(t => t.ProtocolTool.Name == name);

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(
            Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    [Test]
    public async Task Get_config_tool_delegate_projects_the_permission_scoped_report()
    {
        _control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns(new ReplicationConfigReport(new[]
            {
                new ReplicationTreeConfigEntry("orders", enabled: true, LatticeMergeMode.OrSet, ambiguous: false),
                new ReplicationTreeConfigEntry("inventory", enabled: false, mode: null, ambiguous: true),
            }));

        var config = await CallAsync<McpReplicationConfig>("lattice_replication_get_config");

        Assert.Multiple(() =>
        {
            Assert.That(config.Trees.Select(t => t.TreeId), Is.EqualTo(new[] { "orders", "inventory" }));
            Assert.That(config.Trees[0].Mode, Is.EqualTo(nameof(LatticeMergeMode.OrSet)));
            Assert.That(config.Trees[1].Mode, Is.Null, "An ambiguous-mode tree reports a null mode.");
            Assert.That(config.Trees[1].Ambiguous, Is.True);
        });
    }

    [Test]
    public async Task Enable_tool_delegate_forwards_the_mode_and_bootstrap_source()
    {
        _control.EnableReplicationAsync("orders", LatticeMergeMode.OrSet, "cluster-b", Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult(
                "orders", LatticeMergeMode.OrSet, alreadyEnabled: false, bootstrapRequested: true));

        var result = await CallAsync<McpReplicationEnableResult>(
            "lattice_replication_enable",
            ("treeId", "orders"),
            ("mode", "OrSet"),
            ("bootstrapSourceClusterId", "cluster-b"));

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Mode, Is.EqualTo(nameof(LatticeMergeMode.OrSet)));
            Assert.That(result.BootstrapRequested, Is.True,
                "The delegate must forward the bootstrap source cluster so the tree is seeded.");
        });
    }

    [Test]
    public async Task Enable_tool_delegate_omits_the_bootstrap_source_when_it_is_not_supplied()
    {
        _control.EnableReplicationAsync("orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>())
            .Returns(new ReplicationEnableResult(
                "orders", LatticeMergeMode.LwwRegister, alreadyEnabled: false, bootstrapRequested: false));

        var result = await CallAsync<McpReplicationEnableResult>(
            "lattice_replication_enable",
            ("treeId", "orders"),
            ("mode", "LwwRegister"));

        Assert.That(result.BootstrapRequested, Is.False);
        await _control.Received(1).EnableReplicationAsync(
            "orders", LatticeMergeMode.LwwRegister, null, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Enable_tool_delegate_rejects_an_unknown_merge_mode_before_touching_the_facade()
    {
        Assert.That(
            async () => await CallAsync<McpReplicationEnableResult>(
                "lattice_replication_enable",
                ("treeId", "orders"),
                ("mode", "not-a-mode")),
            Throws.Exception,
            "An unparsable merge mode must be rejected rather than reaching the facade.");

        Assert.That(
            _control.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILatticeReplicationControl.EnableReplicationAsync)),
            Is.False,
            "The mode is validated before the facade is touched.");
    }

    [Test]
    public async Task Disable_tool_delegate_forwards_the_tree_id()
    {
        _control.DisableReplicationAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new ReplicationDisableResult("orders", alreadyDisabled: true));

        var result = await CallAsync<McpReplicationDisableResult>(
            "lattice_replication_disable", ("treeId", "orders"));

        Assert.Multiple(() =>
        {
            Assert.That(result.TreeId, Is.EqualTo("orders"));
            Assert.That(result.AlreadyDisabled, Is.True, "Disable is idempotent.");
        });
    }

    [Test]
    public void Delegate_surfaces_the_facades_fail_closed_denial()
    {
        _control.GetReplicationConfigAsync(Arg.Any<CancellationToken>())
            .Returns<Task<ReplicationConfigReport>>(_ => throw new LatticeAuthorizationDeniedException("denied"));

        Assert.That(
            async () => await CallAsync<McpReplicationConfig>("lattice_replication_get_config"),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path: the facade's denial must surface unchanged.");
    }
}
