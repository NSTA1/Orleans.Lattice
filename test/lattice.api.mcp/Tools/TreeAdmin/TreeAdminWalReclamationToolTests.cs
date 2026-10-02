using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Drives <c>lattice_treeadmin_wal_reclamation</c> (#4237) through the tree-administration
/// group's own invocation delegate: an authorized call projects the facade's report
/// (holder, wedge verdict, int64 offsets as strings), a facade denial surfaces unchanged,
/// and a host that registers no <see cref="ILatticeWalReclamation"/> fails the call
/// instead of succeeding. The tool is looked up by name on the group, so before the tool
/// existed each test failed on the lookup rather than not compiling.
/// </summary>
[TestFixture]
public sealed class TreeAdminWalReclamationToolTests
{
    private const string ToolName = "lattice_treeadmin_wal_reclamation";

    private ILatticeWalReclamation _reclamation = null!;

    [SetUp]
    public void SetUp() => _reclamation = Substitute.For<ILatticeWalReclamation>();

    private static McpServerTool Tool()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        using var provider = services.BuildServiceProvider();
        var group = new TreeAdminToolGroup(provider, Options.Create(new LatticeApiMcpOptions()));
        var tool = group.Tools.SingleOrDefault(t => t.ProtocolTool.Name == ToolName);
        Assert.That(tool, Is.Not.Null, $"The tree-administration group must contribute {ToolName} by default.");
        return tool!;
    }

    private ServiceProvider Services(bool register = true)
    {
        var services = new ServiceCollection();
        if (register)
        {
            services.AddSingleton(_reclamation);
        }

        return services.BuildServiceProvider();
    }

    private async Task<JsonElement> CallAsync(string treeId)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(Tool(), services, McpToolInvocation.Args(("treeId", treeId)));
        Assert.That(result.IsError, Is.Not.True);
        Assert.That(result.StructuredContent, Is.Not.Null);
        return result.StructuredContent!.Value;
    }

    [Test]
    public async Task An_authorized_call_projects_the_wedged_floor_holder()
    {
        _reclamation.GetWalReclamationAsync("orders", Arg.Any<CancellationToken>()).Returns(new TreeWalReclamationReport
        {
            TreeId = "orders",
            PinStoreReadable = true,
            PinCount = 3,
            PinsWithoutOffset = 1,
            FloorHolder = new TreeWalFloorHolder
            {
                ConsumerId = "leaf/orders/7",
                LeafId = "orders/7",
                Partition = 2,
                PinOffset = 9_007_199_254_740_993,
                PersistedCheckpoint = -1,
                State = TreeWalFloorHolderState.NeverCheckpointed,
            },
        });

        var report = await CallAsync("orders");
        var holder = report.GetProperty("floorHolder");

        Assert.Multiple(() =>
        {
            Assert.That(report.GetProperty("treeId").GetString(), Is.EqualTo("orders"));
            Assert.That(report.GetProperty("pinStoreReadable").GetBoolean(), Is.True);
            Assert.That(report.GetProperty("pinCount").GetInt32(), Is.EqualTo(3));
            Assert.That(report.GetProperty("pinsWithoutOffset").GetInt32(), Is.EqualTo(1));
            Assert.That(report.GetProperty("isWedged").GetBoolean(), Is.True);
            Assert.That(holder.GetProperty("consumerId").GetString(), Is.EqualTo("leaf/orders/7"));
            Assert.That(holder.GetProperty("leafId").GetString(), Is.EqualTo("orders/7"));
            Assert.That(holder.GetProperty("partition").GetInt32(), Is.EqualTo(2));
            Assert.That(holder.GetProperty("pinOffset").GetString(), Is.EqualTo("9007199254740993"), "int64 travels as a string.");
            Assert.That(holder.GetProperty("persistedCheckpoint").GetString(), Is.EqualTo("-1"));
            Assert.That(holder.GetProperty("state").GetString(), Is.EqualTo("NeverCheckpointed"));
            Assert.That(holder.GetProperty("holdsOffsetFloor").GetBoolean(), Is.True);
        });
        await _reclamation.Received(1).GetWalReclamationAsync("orders", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_tree_with_no_pin_reports_no_holder_and_no_wedge()
    {
        _reclamation.GetWalReclamationAsync("idle", Arg.Any<CancellationToken>())
            .Returns(new TreeWalReclamationReport { TreeId = "idle", PinStoreReadable = true });

        var report = await CallAsync("idle");

        Assert.Multiple(() =>
        {
            Assert.That(report.GetProperty("isWedged").GetBoolean(), Is.False);
            Assert.That(
                !report.TryGetProperty("floorHolder", out var holder) || holder.ValueKind == JsonValueKind.Null,
                Is.True,
                "A tree that holds no pin has no floor holder.");
        });
    }

    [Test]
    public void A_denied_call_surfaces_the_facades_fail_closed_denial()
    {
        _reclamation.GetWalReclamationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<TreeWalReclamationReport>>(_ => throw new LatticeAuthorizationDeniedException("denied"));

        Assert.That(
            async () => await CallAsync("orders"),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path: the facade's denial must surface unchanged.");
    }

    [Test]
    public async Task A_host_without_the_facade_fails_the_call_rather_than_succeeding()
    {
        var tool = Tool();
        await using var services = Services(register: false);
        CallToolResult? result = null;
        InvalidOperationException? thrown = null;
        try
        {
            result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("treeId", "orders")));
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        Assert.That(
            thrown is not null ? thrown.Message.Contains(nameof(ILatticeWalReclamation), StringComparison.Ordinal) : result!.IsError == true,
            Is.True,
            "A host that serves no WAL reclamation read must fail the call, never succeed.");
    }

    [Test]
    public void The_tool_is_a_read_that_takes_only_a_tree_id()
    {
        var tool = Tool();
        var schema = tool.ProtocolTool.InputSchema.GetRawText();

        Assert.Multiple(() =>
        {
            Assert.That(tool.ProtocolTool.Annotations?.ReadOnlyHint, Is.True);
            Assert.That(tool.ProtocolTool.Annotations?.DestructiveHint, Is.False);
            Assert.That(schema, Does.Contain("\"treeId\""));
            Assert.That(schema, Does.Not.Contain("\"context\""));
            Assert.That(schema, Does.Not.Contain("cancellationToken"));
        });
    }
}
