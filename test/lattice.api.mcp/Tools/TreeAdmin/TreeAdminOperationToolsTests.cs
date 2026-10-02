using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Drives each accept-then-poll tool of <see cref="TreeAdminOperationTools"/> (#4126)
/// through its own invocation delegate: the start tools forward the tree and
/// idempotency id to the operations facade and answer a handle that names the
/// status tool, the status / list / cancel tools project the shared operation
/// contract, the facade is resolved from the request services rather than exposed as
/// an argument, and a server without the surface fails the call with a clear message.
/// </summary>
[TestFixture]
public sealed class TreeAdminOperationToolsTests
{
    private ILatticeSchemaComplianceOperations _compliance = null!;
    private ILatticeStorageUsageOperations _storage = null!;

    [SetUp]
    public void SetUp()
    {
        _compliance = Substitute.For<ILatticeSchemaComplianceOperations>();
        _storage = Substitute.For<ILatticeStorageUsageOperations>();
    }

    private static McpServerTool Tool(string name) => TreeAdminOperationTools.Create().Single(t => t.ProtocolTool.Name == name);

    private ServiceProvider Services(bool register = true)
    {
        var services = new ServiceCollection();
        if (register)
        {
            services.AddSingleton(_compliance);
            services.AddSingleton(_storage);
        }

        return services.BuildServiceProvider();
    }

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    private static LatticeOperationHandle Handle(string kind, params string[] trees) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = trees },
        Created = true,
    };

    private static LatticeOperationStatus Status(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        State = LatticeOperationState.Succeeded,
        Phase = SchemaComplianceScanOperation.ScanningPhase,
        PhaseIndex = 1,
        PhaseCount = 2,
        CompletedUnits = 7,
        TotalUnits = 7,
        UnitName = SchemaComplianceScanOperation.EntriesUnit,
        ResultReference = "orders",
        Result = new Dictionary<string, string> { [SchemaComplianceScanResults.ScannedCountKey] = "7" },
    };

    [Test]
    public async Task The_compliance_start_tool_forwards_the_tree_and_id_and_names_its_status_tool()
    {
        _compliance.StartComplianceScanAsync("orders", "scan-1", Arg.Any<CancellationToken>())
            .Returns(Handle(SchemaComplianceScanOperation.Kind, "orders"));

        var handle = await CallAsync<McpLatticeOperationHandle>(
            TreeAdminOperationTools.ComplianceScanStartToolName, ("treeId", "orders"), ("operationId", "scan-1"));

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Kind, Is.EqualTo(SchemaComplianceScanOperation.Kind));
            Assert.That(handle.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.StatusTool, Is.EqualTo(TreeAdminOperationTools.ComplianceScanStatusToolName));
        });
    }

    [Test]
    public async Task A_start_without_an_id_lets_the_server_generate_one()
    {
        _compliance.StartComplianceScanAsync("orders", null, Arg.Any<CancellationToken>())
            .Returns(Handle(SchemaComplianceScanOperation.Kind, "orders"));
        _storage.StartStorageUsageRefreshAsync(null, Arg.Any<CancellationToken>())
            .Returns(Handle(StorageUsageRefreshOperation.Kind));

        await CallAsync<McpLatticeOperationHandle>(TreeAdminOperationTools.ComplianceScanStartToolName, ("treeId", "orders"));
        var refresh = await CallAsync<McpLatticeOperationHandle>(TreeAdminOperationTools.StorageRefreshStartToolName);

        Assert.That(refresh.StatusTool, Is.EqualTo(TreeAdminOperationTools.StorageRefreshStatusToolName));
        await _compliance.Received(1).StartComplianceScanAsync("orders", null, Arg.Any<CancellationToken>());
        await _storage.Received(1).StartStorageUsageRefreshAsync(null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_status_tools_project_the_operation_and_report_not_found()
    {
        _compliance.GetOperationStatusAsync("op-1", Arg.Any<CancellationToken>()).Returns(Status(SchemaComplianceScanOperation.Kind));
        _storage.GetOperationStatusAsync("missing", Arg.Any<CancellationToken>()).Returns((LatticeOperationStatus?)null);

        var found = await CallAsync<McpLatticeOperationResult>(TreeAdminOperationTools.ComplianceScanStatusToolName, ("operationId", "op-1"));
        var missing = await CallAsync<McpLatticeOperationResult>(TreeAdminOperationTools.StorageRefreshStatusToolName, ("operationId", "missing"));

        Assert.Multiple(() =>
        {
            Assert.That(found.Found, Is.True);
            Assert.That(found.Operation!.State, Is.EqualTo("Succeeded"));
            Assert.That(found.Operation.CompletedUnits, Is.EqualTo(7));
            Assert.That(found.Operation.TotalUnits, Is.EqualTo(7));
            Assert.That(found.Operation.Result[SchemaComplianceScanResults.ScannedCountKey], Is.EqualTo("7"));
            Assert.That(missing.Found, Is.False);
            Assert.That(missing.Operation, Is.Null);
        });
    }

    [Test]
    public async Task The_list_and_cancel_tools_reach_their_own_facades()
    {
        _storage.ListOperationsAsync(Arg.Is<LatticeOperationListRequest>(r => r.PageSize == 5 && r.PageToken == "next"), Arg.Any<CancellationToken>())
            .Returns(new LatticeOperationPage { Operations = [Status(StorageUsageRefreshOperation.Kind)], NextPageToken = "after" });
        _compliance.CancelOperationAsync("op-1", Arg.Any<CancellationToken>())
            .Returns(Status(SchemaComplianceScanOperation.Kind) with { State = LatticeOperationState.Running, CancelRequested = true });

        var page = await CallAsync<McpLatticeOperationPage>(TreeAdminOperationTools.StorageRefreshListToolName, ("pageSize", 5), ("pageToken", "next"));
        var cancelled = await CallAsync<McpLatticeOperationResult>(TreeAdminOperationTools.ComplianceScanCancelToolName, ("operationId", "op-1"));

        Assert.Multiple(() =>
        {
            Assert.That(page.Operations, Has.Count.EqualTo(1));
            Assert.That(page.NextPageToken, Is.EqualTo("after"));
            Assert.That(cancelled.Operation!.CancelRequested, Is.True);
        });
        await _storage.DidNotReceive().CancelOperationAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
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
                Tool(TreeAdminOperationTools.StorageRefreshStartToolName), services, McpToolInvocation.Args());
        }
        catch (InvalidOperationException ex)
        {
            thrown = ex;
        }

        Assert.That(
            thrown is not null ? thrown.Message.Contains("ILatticeStorageUsageOperations", StringComparison.Ordinal) : result!.IsError == true,
            Is.True,
            "A missing surface must fail the call, never succeed.");
    }

    [Test]
    public void The_tools_advertise_their_arguments_and_never_the_facade()
    {
        var tools = TreeAdminOperationTools.Create();

        Assert.Multiple(() =>
        {
            Assert.That(tools.Select(t => t.ProtocolTool.Name), Is.Unique);
            Assert.That(tools, Has.Count.EqualTo(8));
            foreach (var tool in tools)
            {
                var schema = tool.ProtocolTool.InputSchema.GetRawText();
                Assert.That(schema, Does.Not.Contain("\"operations\""), tool.ProtocolTool.Name);
                Assert.That(schema, Does.Not.Contain("cancellationToken"), tool.ProtocolTool.Name);
                Assert.That(tool.ProtocolTool.Annotations?.DestructiveHint, Is.False, tool.ProtocolTool.Name);
            }

            Assert.That(Tool(TreeAdminOperationTools.ComplianceScanStartToolName).ProtocolTool.InputSchema.GetRawText(), Does.Contain("\"treeId\""));
            Assert.That(Tool(TreeAdminOperationTools.ComplianceScanStatusToolName).ProtocolTool.Annotations?.ReadOnlyHint, Is.True);
            Assert.That(Tool(TreeAdminOperationTools.StorageRefreshCancelToolName).ProtocolTool.Annotations?.ReadOnlyHint, Is.False);
        });
    }
}
