using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the remote-host accept-then-poll adapters of #4126,
/// <see cref="GrpcLatticeSchemaComplianceOperations"/> and
/// <see cref="GrpcLatticeStorageUsageOperations"/>: each member forwards its
/// request to the matching gRPC call and unwraps the client result verbatim.
/// Deterministic over a <see cref="FakeCallInvoker"/> - no channel, no cluster.
/// </summary>
[TestFixture]
public sealed class GrpcOperationAdaptersTests
{
    private static LatticeOperationHandle Handle(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [] },
        Created = true,
    };

    private static LatticeOperationStatus Status(string kind) => new()
    {
        OperationId = "op-1",
        Kind = kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = [] },
        State = LatticeOperationState.Running,
        Phase = "Scanning",
    };

    [Test]
    public void Constructors_refuse_a_null_client()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new GrpcLatticeSchemaComplianceOperations(null!), Throws.ArgumentNullException);
            Assert.That(() => new GrpcLatticeStorageUsageOperations(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_compliance_adapter_forwards_each_verb()
    {
        var handle = Handle(SchemaComplianceScanOperation.Kind);
        var status = Status(SchemaComplianceScanOperation.Kind);
        var page = new LatticeOperationPage();
        var invoker = new FakeCallInvoker(request => request switch
        {
            SchemaComplianceScanStartRequest => handle,
            SchemaComplianceOperationRequest => new SchemaComplianceOperationStatusResponse { Status = status },
            LatticeOperationListRequest => page,
            _ => throw new InvalidOperationException(request.GetType().Name),
        });
        var adapter = new GrpcLatticeSchemaComplianceOperations(RemoteTestSupport.SchemaClient(invoker));

        var started = await adapter.StartComplianceScanAsync("orders", "scan-1");
        var sentStart = (SchemaComplianceScanStartRequest)invoker.LastRequest!;
        var read = await adapter.GetOperationStatusAsync("op-1");
        var listed = await adapter.ListOperationsAsync(new LatticeOperationListRequest());
        var cancelled = await adapter.CancelOperationAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.SameAs(handle));
            Assert.That(sentStart, Is.EqualTo(new SchemaComplianceScanStartRequest { TreeId = "orders", OperationId = "scan-1" }));
            Assert.That(read, Is.SameAs(status));
            Assert.That(listed, Is.SameAs(page));
            Assert.That(cancelled, Is.SameAs(status));
            Assert.That(((SchemaComplianceOperationRequest)invoker.LastRequest!).OperationId, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public async Task The_storage_usage_adapter_forwards_each_verb()
    {
        var handle = Handle(StorageUsageRefreshOperation.Kind);
        var status = Status(StorageUsageRefreshOperation.Kind);
        var page = new LatticeOperationPage();
        var invoker = new FakeCallInvoker(request => request switch
        {
            TreeAdminStorageUsageRefreshRequest => handle,
            TreeAdminStorageUsageOperationRequest => new TreeAdminStorageUsageOperationStatusResponse { Status = status },
            LatticeOperationListRequest => page,
            _ => throw new InvalidOperationException(request.GetType().Name),
        });
        var adapter = new GrpcLatticeStorageUsageOperations(RemoteTestSupport.TreeAdminClient(invoker));

        var started = await adapter.StartStorageUsageRefreshAsync("refresh-1");
        var sentStart = (TreeAdminStorageUsageRefreshRequest)invoker.LastRequest!;
        var read = await adapter.GetOperationStatusAsync("op-1");
        var listed = await adapter.ListOperationsAsync(new LatticeOperationListRequest());
        var cancelled = await adapter.CancelOperationAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(started, Is.SameAs(handle));
            Assert.That(sentStart.OperationId, Is.EqualTo("refresh-1"));
            Assert.That(read, Is.SameAs(status));
            Assert.That(listed, Is.SameAs(page));
            Assert.That(cancelled, Is.SameAs(status));
        });
    }
}
