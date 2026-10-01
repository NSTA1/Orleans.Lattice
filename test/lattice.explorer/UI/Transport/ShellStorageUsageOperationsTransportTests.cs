using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeStorageUsageOperations"/> transport adapter
/// (#4126): every accept-then-poll verb reaches its RPC, maps a denial, carries the
/// circuit credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellStorageUsageOperationsTransportTests : ShellTransportAdapterContractTests<ILatticeStorageUsageOperations>
{
    private const string Service = "/orleans.lattice.api.treeadmin/";

    private static readonly LatticeOperationHandle Handle = new()
    {
        OperationId = "op-1",
        Kind = StorageUsageRefreshOperation.Kind,
        Scope = new LatticeOperationScope { TenantId = "default" },
        Created = true,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeStorageUsageOperations>> Calls() =>
    [
        new("StartStorageUsageRefreshAsync", Service + "StartStorageUsageRefresh", (f, ct) => f.StartStorageUsageRefreshAsync("op-1", ct)),
        new("GetOperationStatusAsync", Service + "GetStorageUsageRefreshStatus", (f, ct) => f.GetOperationStatusAsync("op-1", ct)),
        new("ListOperationsAsync", Service + "ListStorageUsageRefreshes", (f, ct) => f.ListOperationsAsync(new LatticeOperationListRequest(), ct)),
        new("CancelOperationAsync", Service + "CancelStorageUsageRefresh", (f, ct) => f.CancelOperationAsync("op-1", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
        peer.Respond(Service + "StartStorageUsageRefresh", Handle);
        peer.Respond(Service + "GetStorageUsageRefreshStatus", new TreeAdminStorageUsageOperationStatusResponse());
        peer.Respond(Service + "CancelStorageUsageRefresh", new TreeAdminStorageUsageOperationStatusResponse());
        peer.Respond(Service + "ListStorageUsageRefreshes", new LatticeOperationPage());
    }

    [Test]
    public void The_verbs_validate_their_arguments_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeStorageUsageOperations>();

        Assert.Multiple(() =>
        {
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
        var facade = circuit.Resolve<ILatticeStorageUsageOperations>();
        ScriptSuccess(circuit.Peer);

        var handle = await facade.StartStorageUsageRefreshAsync("op-1");
        var status = await facade.GetOperationStatusAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Kind, Is.EqualTo(StorageUsageRefreshOperation.Kind));
            Assert.That(status, Is.Null);
        });
    }
}
