using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeBackupOperations"/> transport adapter (#4122):
/// every accept-then-poll verb reaches its RPC, maps a denial, carries the
/// circuit credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellBackupOperationsTransportTests : ShellTransportAdapterContractTests<ILatticeBackupOperations>
{
    private const string Service = "/orleans.lattice.api.backup/";

    private static readonly BackupScopeSelector Scope = BackupScopeSelector.WholeTree("orders");

    private static readonly LatticeOperationHandle Handle = new()
    {
        OperationId = "op-1",
        Kind = BackupOperationKinds.Capture,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        Created = true,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeBackupOperations>> Calls() =>
    [
        new("StartBackupAsync", Service + "StartBackup", (f, ct) => f.StartBackupAsync(new LatticeBackupCaptureRequest("nightly", Scope), "op-1", ct)),
        new("StartIncrementalBackupAsync", Service + "StartIncrementalBackup", (f, ct) => f.StartIncrementalBackupAsync(new LatticeBackupIncrementalCaptureRequest("hourly", Scope, "b1"), null, ct)),
        new("StartBackupSetAsync", Service + "StartBackupSet", (f, ct) => f.StartBackupSetAsync(new LatticeBackupSetCaptureRequest("set", [Scope]), null, ct)),
        new("StartRestoreAsync", Service + "StartRestore", (f, ct) => f.StartRestoreAsync(new LatticeRestoreRequest("b1"), null, ct)),
        new("StartColdRestoreAsync", Service + "StartColdRestore", (f, ct) => f.StartColdRestoreAsync(new LatticeRestoreRequest("b1"), null, ct)),
        new("GetOperationStatusAsync", Service + "GetBackupOperationStatus", (f, ct) => f.GetOperationStatusAsync("op-1", ct)),
        new("ListOperationsAsync", Service + "ListBackupOperations", (f, ct) => f.ListOperationsAsync(new LatticeOperationListRequest(), ct)),
        new("CancelOperationAsync", Service + "CancelBackupOperation", (f, ct) => f.CancelOperationAsync("op-1", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
        foreach (var start in new[] { "StartBackup", "StartIncrementalBackup", "StartBackupSet", "StartRestore", "StartColdRestore" })
        {
            peer.Respond(Service + start, Handle);
        }

        peer.Respond(Service + "GetBackupOperationStatus", new BackupOperationStatusResponse());
        peer.Respond(Service + "CancelBackupOperation", new BackupOperationStatusResponse());
        peer.Respond(Service + "ListBackupOperations", new LatticeOperationPage());
    }

    [Test]
    public void The_verbs_validate_their_arguments_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeBackupOperations>();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.StartBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.StartIncrementalBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.StartBackupSetAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.StartRestoreAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.StartColdRestoreAsync(null!), Throws.ArgumentNullException);
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
        var facade = circuit.Resolve<ILatticeBackupOperations>();
        ScriptSuccess(circuit.Peer);

        var handle = await facade.StartBackupAsync(new LatticeBackupCaptureRequest("nightly", Scope), "op-1");
        var status = await facade.GetOperationStatusAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(status, Is.Null);
        });
    }
}
