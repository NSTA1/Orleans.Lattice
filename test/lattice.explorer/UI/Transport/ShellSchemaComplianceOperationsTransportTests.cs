using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeSchemaComplianceOperations"/> transport adapter
/// (#4126): every accept-then-poll verb reaches its RPC, maps a denial, carries the
/// circuit credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellSchemaComplianceOperationsTransportTests : ShellTransportAdapterContractTests<ILatticeSchemaComplianceOperations>
{
    private const string Service = "/orleans.lattice.api.schema/";

    private static readonly LatticeOperationHandle Handle = new()
    {
        OperationId = "op-1",
        Kind = SchemaComplianceScanOperation.Kind,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        Created = true,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeSchemaComplianceOperations>> Calls() =>
    [
        new("StartComplianceScanAsync", Service + "StartComplianceScan", (f, ct) => f.StartComplianceScanAsync("orders", "op-1", ct)),
        new("GetOperationStatusAsync", Service + "GetComplianceScanStatus", (f, ct) => f.GetOperationStatusAsync("op-1", ct)),
        new("ListOperationsAsync", Service + "ListComplianceScans", (f, ct) => f.ListOperationsAsync(new LatticeOperationListRequest(), ct)),
        new("CancelOperationAsync", Service + "CancelComplianceScan", (f, ct) => f.CancelOperationAsync("op-1", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
        peer.Respond(Service + "StartComplianceScan", Handle);
        peer.Respond(Service + "GetComplianceScanStatus", new SchemaComplianceOperationStatusResponse());
        peer.Respond(Service + "CancelComplianceScan", new SchemaComplianceOperationStatusResponse());
        peer.Respond(Service + "ListComplianceScans", new LatticeOperationPage());
    }

    [Test]
    public void The_verbs_validate_their_arguments_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeSchemaComplianceOperations>();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.StartComplianceScanAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => facade.GetOperationStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => facade.ListOperationsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.CancelOperationAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task A_start_returns_the_clusters_handle_and_a_missing_status_reads_as_null()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeSchemaComplianceOperations>();
        ScriptSuccess(circuit.Peer);

        var handle = await facade.StartComplianceScanAsync("orders", "op-1");
        var status = await facade.GetOperationStatusAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(status, Is.Null);
        });
    }
}
