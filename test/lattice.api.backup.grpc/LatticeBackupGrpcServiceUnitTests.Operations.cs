using Grpc.Core;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// The accept-then-poll RPCs of <see cref="LatticeBackupGrpcService"/> (#4122):
/// each forwards to <see cref="ILatticeBackupOperations"/> with its tracking id,
/// a not-found status travels as a null status, and a host that registers no
/// operations facade answers <see cref="StatusCode.Unimplemented"/> rather than
/// failing the whole service.
/// </summary>
public sealed partial class LatticeBackupGrpcServiceUnitTests
{
    private static readonly LatticeOperationHandle Handle = new()
    {
        OperationId = "op-1",
        Kind = BackupOperationKinds.Capture,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["t"] },
        Created = true,
    };

    [Test]
    public async Task StartBackup_forwards_the_request_and_tracking_id()
    {
        var operations = Substitute.For<ILatticeBackupOperations>();
        operations.StartBackupAsync(Arg.Any<LatticeBackupCaptureRequest>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Handle);
        var service = CreateService(Substitute.For<ILatticeBackupControl>(), operations: operations);

        var handle = await service.StartBackup(
            new BackupCaptureRequestMessage { Name = "n", Scope = BackupScopeSelector.WholeTree("t"), TrackingOperationId = "op-1" },
            Context());

        Assert.That(handle, Is.SameAs(Handle));
        await operations.Received(1).StartBackupAsync(
            Arg.Is<LatticeBackupCaptureRequest>(r => r.Name == "n" && r.Scope.TreeId == "t"), "op-1", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StartRestore_and_StartColdRestore_map_the_restore_message()
    {
        var operations = Substitute.For<ILatticeBackupOperations>();
        var service = CreateService(Substitute.For<ILatticeBackupControl>(), operations: operations);
        var message = new RestoreRequestMessage
        {
            BackupId = "bk",
            TargetTreeId = "target",
            Mode = LatticeRestoreMode.ShadowCutover,
            OperationId = "restore-key",
            TrackingOperationId = "tracked",
        };

        await service.StartRestore(message, Context());
        await service.StartColdRestore(message, Context());

        await operations.Received(1).StartRestoreAsync(
            Arg.Is<LatticeRestoreRequest>(r => r.BackupId == "bk" && r.TargetTreeId == "target"
                && r.Mode == LatticeRestoreMode.ShadowCutover && r.OperationId == "restore-key"),
            "tracked",
            Arg.Any<CancellationToken>());
        await operations.Received(1).StartColdRestoreAsync(Arg.Any<LatticeRestoreRequest>(), "tracked", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Incremental_and_set_starts_forward_their_requests()
    {
        var operations = Substitute.For<ILatticeBackupOperations>();
        var service = CreateService(Substitute.For<ILatticeBackupControl>(), operations: operations);

        await service.StartIncrementalBackup(
            new BackupIncrementalCaptureRequestMessage { Name = "d", Scope = BackupScopeSelector.WholeTree("t"), BaseBackupId = "b" },
            Context());
        await service.StartBackupSet(
            new BackupSetCaptureRequestMessage { Name = "s", Scopes = [BackupScopeSelector.WholeTree("t")], CrossTreeConsistent = true },
            Context());

        await operations.Received(1).StartIncrementalBackupAsync(
            Arg.Is<LatticeBackupIncrementalCaptureRequest>(r => r.BaseBackupId == "b"), null, Arg.Any<CancellationToken>());
        await operations.Received(1).StartBackupSetAsync(
            Arg.Is<LatticeBackupSetCaptureRequest>(r => r.CrossTreeConsistent), null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Status_list_and_cancel_forward_and_carry_not_found_as_a_null_status()
    {
        var operations = Substitute.For<ILatticeBackupOperations>();
        operations.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>())
            .Returns(new LatticeOperationPage());
        var service = CreateService(Substitute.For<ILatticeBackupControl>(), operations: operations);

        var status = await service.GetBackupOperationStatus(new BackupOperationRequestMessage { OperationId = "x" }, Context());
        var cancelled = await service.CancelBackupOperation(new BackupOperationRequestMessage { OperationId = "x" }, Context());
        var page = await service.ListBackupOperations(new LatticeOperationListRequest(), Context());

        Assert.Multiple(() =>
        {
            Assert.That(status.Status, Is.Null);
            Assert.That(cancelled.Status, Is.Null);
            Assert.That(page.Operations, Is.Empty);
        });
        await operations.Received(1).CancelOperationAsync("x", Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_host_without_an_operations_facade_answers_unimplemented()
    {
        var service = CreateService(Substitute.For<ILatticeBackupControl>());

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StartBackup(
            new BackupCaptureRequestMessage { Name = "n", Scope = BackupScopeSelector.WholeTree("t") }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void An_operations_denial_maps_to_permission_denied()
    {
        var operations = Substitute.For<ILatticeBackupOperations>();
        operations.StartBackupAsync(Arg.Any<LatticeBackupCaptureRequest>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns<LatticeOperationHandle>(_ => throw new LatticeAuthorizationDeniedException("no"));
        var service = CreateService(Substitute.For<ILatticeBackupControl>(), operations: operations);

        var ex = Assert.ThrowsAsync<RpcException>(async () => await service.StartBackup(
            new BackupCaptureRequestMessage { Name = "n", Scope = BackupScopeSelector.WholeTree("t") }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
