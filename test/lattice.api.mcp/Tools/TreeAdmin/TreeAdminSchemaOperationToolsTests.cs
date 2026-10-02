using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Drives each accept-then-poll schema remediation and migration tool of
/// <see cref="TreeAdminSchemaOperationTools"/> (#4209) through its own invocation
/// delegate: the start tools and the deprecated aliases forward their arguments and
/// idempotency id to <see cref="ILatticeSchemaOperations"/> and answer a handle that
/// names the status tool, never calling a blocking verb on
/// <see cref="ILatticeSchemaControl"/>; the status, list and cancel tools project the
/// shared operation contract; and a server without the surface fails the call.
/// </summary>
[TestFixture]
public sealed class TreeAdminSchemaOperationToolsTests
{
    private ILatticeSchemaOperations _operations = null!;
    private ILatticeSchemaControl _control = null!;

    [SetUp]
    public void SetUp()
    {
        _operations = Substitute.For<ILatticeSchemaOperations>();
        _control = Substitute.For<ILatticeSchemaControl>();
    }

    private static TreeAdminToolGroup Group(bool enableSchemaControl = true)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ILatticeSchemaControl>());
        return new TreeAdminToolGroup(
            services.BuildServiceProvider(),
            Options.Create(new LatticeApiMcpOptions { EnableTreeAdminSchemaControlTools = enableSchemaControl }));
    }

    private static McpServerTool Tool(string name) => Group().Tools.Single(t => t.ProtocolTool.Name == name);

    private ServiceProvider Services(bool register = true)
    {
        var services = new ServiceCollection();
        services.AddSingleton(_control);
        if (register)
        {
            services.AddSingleton(_operations);
        }

        return services.BuildServiceProvider();
    }

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    private static LatticeOperationHandle Handle(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        Created = true,
    };

    private static LatticeOperationStatus Status(LatticeOperationState state = LatticeOperationState.Running) => new()
    {
        OperationId = "op-1",
        Kind = SchemaOperationKinds.Remediation,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = state,
        Phase = SchemaOperationPhases.Build,
        PhaseIndex = 1,
        PhaseCount = 3,
        CompletedUnits = 40,
        TotalUnits = 100,
        UnitName = SchemaOperationPhases.ValuesUnit,
    };

    private static readonly LatticeSchemaPolicy Policy = new(new[] { LatticeSchemaRule.Json() });

    [TestCase(TreeAdminSchemaOperationTools.RemediationStartToolName)]
    [TestCase(TreeAdminSchemaOperationTools.RemediateAliasToolName)]
    public async Task The_remediation_tools_start_an_operation_and_name_the_status_tool(string toolName)
    {
        _operations.StartRemediationAsync("orders", Arg.Any<LatticeValueTransform>(), Arg.Any<LatticeSchemaPolicy>(), "rem-1", Arg.Any<CancellationToken>())
            .Returns(Handle(SchemaOperationKinds.Remediation));

        var handle = await CallAsync<McpLatticeOperationHandle>(
            toolName,
            ("treeId", "orders"),
            ("transform", LatticeValueTransform.Passthrough()),
            ("targetPolicy", Policy),
            ("operationId", "rem-1"));

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Remediation));
            Assert.That(handle.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.StatusTool, Is.EqualTo(TreeAdminSchemaOperationTools.StatusToolName));
        });
        await _operations.Received(1).StartRemediationAsync(
            "orders", Arg.Any<LatticeValueTransform>(), Arg.Is<LatticeSchemaPolicy>(p => p.Rules.Count == 1), "rem-1", Arg.Any<CancellationToken>());
        Assert.That(_control.ReceivedCalls(), Is.Empty, "A schema tool must never reach a blocking ILatticeSchemaControl verb.");
    }

    [TestCase(TreeAdminSchemaOperationTools.MigrationStartToolName)]
    [TestCase(TreeAdminSchemaOperationTools.MigrateAliasToolName)]
    public async Task The_migration_tools_start_an_operation_without_an_id_when_none_is_given(string toolName)
    {
        _operations.StartMigrationAsync("orders", null, Arg.Any<CancellationToken>()).Returns(Handle(SchemaOperationKinds.Migration));

        var handle = await CallAsync<McpLatticeOperationHandle>(toolName, ("treeId", "orders"));

        Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.Migration));
        await _operations.Received(1).StartMigrationAsync("orders", null, Arg.Any<CancellationToken>());
        Assert.That(_control.ReceivedCalls(), Is.Empty, "A schema tool must never reach a blocking ILatticeSchemaControl verb.");
    }

    [TestCase(TreeAdminSchemaOperationTools.AdvanceAndMigrateStartToolName)]
    [TestCase(TreeAdminSchemaOperationTools.AdvanceAndMigrateAliasToolName)]
    public async Task The_advance_and_migrate_tools_forward_the_new_target(string toolName)
    {
        _operations.StartAdvanceAndMigrateAsync("orders", 4u, "adv-1", Arg.Any<CancellationToken>())
            .Returns(Handle(SchemaOperationKinds.AdvanceAndMigrate));

        var handle = await CallAsync<McpLatticeOperationHandle>(
            toolName, ("treeId", "orders"), ("newTargetVersion", 4), ("operationId", "adv-1"));

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(SchemaOperationKinds.AdvanceAndMigrate));
            Assert.That(handle.StatusTool, Is.EqualTo(TreeAdminSchemaOperationTools.StatusToolName));
        });
        await _operations.Received(1).StartAdvanceAndMigrateAsync("orders", 4u, "adv-1", Arg.Any<CancellationToken>());
        Assert.That(_control.ReceivedCalls(), Is.Empty, "A schema tool must never reach a blocking ILatticeSchemaControl verb.");
    }

    [Test]
    public async Task The_status_tool_projects_the_operation_and_reports_not_found()
    {
        _operations.GetOperationStatusAsync("op-1", Arg.Any<CancellationToken>()).Returns(Status());
        _operations.GetOperationStatusAsync("missing", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);

        var found = await CallAsync<McpLatticeOperationResult>(TreeAdminSchemaOperationTools.StatusToolName, ("operationId", "op-1"));
        var missing = await CallAsync<McpLatticeOperationResult>(TreeAdminSchemaOperationTools.StatusToolName, ("operationId", "missing"));

        Assert.Multiple(() =>
        {
            Assert.That(found.Found, Is.True);
            Assert.That(found.Operation!.State, Is.EqualTo("Running"));
            Assert.That(found.Operation.Phase, Is.EqualTo(SchemaOperationPhases.Build));
            Assert.That(found.Operation.CompletedUnits, Is.EqualTo(40));
            Assert.That(found.Operation.TotalUnits, Is.EqualTo(100));
            Assert.That(found.Operation.UnitName, Is.EqualTo(SchemaOperationPhases.ValuesUnit));
            Assert.That(missing.Found, Is.False);
            Assert.That(missing.Operation, Is.Null);
        });
    }

    [Test]
    public async Task The_list_and_cancel_tools_reach_the_schema_operations_facade()
    {
        _operations.ListOperationsAsync(Arg.Is<LatticeOperationListRequest>(r => r.PageSize == 5 && r.PageToken == "next"), Arg.Any<CancellationToken>())
            .Returns(new LatticeOperationPage { Operations = [Status()], NextPageToken = "after" });
        _operations.CancelOperationAsync("op-1", Arg.Any<CancellationToken>())
            .Returns(Status() with { CancelRequested = true });

        var page = await CallAsync<McpLatticeOperationPage>(TreeAdminSchemaOperationTools.ListToolName, ("pageSize", 5), ("pageToken", "next"));
        var cancelled = await CallAsync<McpLatticeOperationResult>(TreeAdminSchemaOperationTools.CancelToolName, ("operationId", "op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(page.Operations, Has.Count.EqualTo(1));
            Assert.That(page.NextPageToken, Is.EqualTo("after"));
            Assert.That(cancelled.Found, Is.True);
            Assert.That(cancelled.Operation!.CancelRequested, Is.True);
        });
    }

    [Test]
    public async Task A_server_without_the_surface_fails_the_call_rather_than_succeeding()
    {
        await using var services = Services(register: false);
        ModelContextProtocol.Protocol.CallToolResult? result = null;
        InvalidOperationException? thrown = null;
        try
        {
            result = await McpToolInvocation.CallAsync(
                Tool(TreeAdminSchemaOperationTools.MigrationStartToolName), services, McpToolInvocation.Args(("treeId", "orders")));
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        Assert.That(
            thrown is not null ? thrown.Message.Contains("ILatticeSchemaOperations", StringComparison.Ordinal) : result!.IsError == true,
            Is.True,
            "A missing surface must fail the call, never succeed.");
        Assert.That(_control.ReceivedCalls(), Is.Empty, "A missing surface must not fall back to a blocking verb.");
    }

    [Test]
    public void The_tools_advertise_their_arguments_and_never_the_facade()
    {
        var tools = TreeAdminSchemaOperationTools.CreateReadTools().Concat(TreeAdminSchemaOperationTools.CreateControlTools()).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(TreeAdminSchemaOperationTools.CreateReadTools(), Has.Count.EqualTo(2));
            Assert.That(TreeAdminSchemaOperationTools.CreateControlTools(), Has.Count.EqualTo(7));
            Assert.That(tools.Select(t => t.ProtocolTool.Name), Is.Unique);
            foreach (var tool in tools)
            {
                var schema = tool.ProtocolTool.InputSchema.GetRawText();
                Assert.That(schema, Does.Not.Contain("\"operations\""), tool.ProtocolTool.Name);
                Assert.That(schema, Does.Not.Contain("\"context\""), tool.ProtocolTool.Name);
                Assert.That(schema, Does.Not.Contain("cancellationToken"), tool.ProtocolTool.Name);
            }

            var remediate = Tool(TreeAdminSchemaOperationTools.RemediateAliasToolName).ProtocolTool;
            Assert.That(remediate.InputSchema.GetRawText(), Does.Contain("\"transform\"").And.Contain("\"targetPolicy\"").And.Contain("\"operationId\""));
            Assert.That(remediate.Description, Does.Contain(TreeAdminSchemaOperationTools.RemediationStartToolName).And.Contain("Deprecated"));
            Assert.That(Tool(TreeAdminSchemaOperationTools.StatusToolName).ProtocolTool.Annotations?.ReadOnlyHint, Is.True);
            Assert.That(Tool(TreeAdminSchemaOperationTools.MigrationStartToolName).ProtocolTool.Annotations?.DestructiveHint, Is.True);
            Assert.That(Tool(TreeAdminSchemaOperationTools.CancelToolName).ProtocolTool.Annotations?.DestructiveHint, Is.False);
        });
    }

    [Test]
    public void The_status_and_list_tools_are_contributed_without_schema_control()
    {
        var names = Group(enableSchemaControl: false).Tools.Select(t => t.ProtocolTool.Name).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(names, Does.Contain(TreeAdminSchemaOperationTools.StatusToolName));
            Assert.That(names, Does.Contain(TreeAdminSchemaOperationTools.ListToolName));
            Assert.That(names, Does.Not.Contain(TreeAdminSchemaOperationTools.RemediationStartToolName));
            Assert.That(names, Does.Not.Contain(TreeAdminSchemaOperationTools.RemediateAliasToolName));
            Assert.That(names, Does.Not.Contain(TreeAdminSchemaOperationTools.CancelToolName));
        });
    }
}
