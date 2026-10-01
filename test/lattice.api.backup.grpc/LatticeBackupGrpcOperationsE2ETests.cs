using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll backup operation RPCs (#4122)
/// through the public <see cref="LatticeBackupApiGrpcClient"/> over a real gRPC
/// channel: start a capture and a restore, poll their status to the outcome, list
/// them, and cancel. Proves the new wire contract round-trips the shared
/// <see cref="LatticeOperationHandle"/> and <see cref="LatticeOperationStatus"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupGrpcOperationsE2ETests
{
    private const string Source = "grpc-ops";

    private GrpcBackupClusterFixture _fixture = null!;
    private GrpcBackupHost _host = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcBackupClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllBackupApiAuthorizer());
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await source.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _host.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    private async Task<LatticeOperationStatus> UntilTerminalAsync(string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await _host.Client.GetBackupOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task Start_capture_then_restore_round_trips_handles_and_statuses_over_the_wire()
    {
        var capture = await _host.Client.StartBackupAsync(
            new LatticeBackupCaptureRequest("wire", BackupScopeSelector.WholeTree(Source)), "wire-capture");
        var captured = await UntilTerminalAsync(capture.OperationId);

        var restore = await _host.Client.StartRestoreAsync(
            new LatticeRestoreRequest(captured.ResultReference!, "grpc-ops-restored"), "wire-restore");
        var restored = await UntilTerminalAsync(restore.OperationId);
        var value = await _fixture.GrainFactory.GetGrain<ILattice>("grpc-ops-restored").GetAsync("k2");

        Assert.Multiple(() =>
        {
            Assert.That(capture.OperationId, Is.EqualTo("wire-capture"));
            Assert.That(capture.Kind, Is.EqualTo(BackupOperationKinds.Capture));
            Assert.That(capture.Created, Is.True);
            Assert.That(captured.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(captured.Scope.TreeIds, Is.EqualTo(new[] { Source }));
            Assert.That(restored.State, Is.EqualTo(LatticeOperationState.Succeeded), restored.FailureReason);
            Assert.That(BackupOperationResults.TryReadRestoreResult(restored.Result, out var result), Is.True);
            Assert.That(result!.EntriesApplied, Is.EqualTo(2));
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v2"));
        });
    }

    [Test]
    public async Task Incremental_and_set_starts_round_trip_over_the_wire()
    {
        var full = await UntilTerminalAsync((await _host.Client.StartBackupAsync(
            new LatticeBackupCaptureRequest("base", BackupScopeSelector.WholeTree(Source)))).OperationId);

        var incremental = await _host.Client.StartIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest("delta", BackupScopeSelector.WholeTree(Source), full.ResultReference!));
        var set = await _host.Client.StartBackupSetAsync(
            new LatticeBackupSetCaptureRequest("set", [BackupScopeSelector.WholeTree(Source)]));

        Assert.Multiple(async () =>
        {
            Assert.That(incremental.Kind, Is.EqualTo(BackupOperationKinds.IncrementalCapture));
            Assert.That((await UntilTerminalAsync(incremental.OperationId)).State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(set.Kind, Is.EqualTo(BackupOperationKinds.SetCapture));
            Assert.That((await UntilTerminalAsync(set.OperationId)).State, Is.EqualTo(LatticeOperationState.Succeeded));
        });
    }

    [Test]
    public async Task List_cancel_and_not_found_round_trip_over_the_wire()
    {
        var handle = await _host.Client.StartBackupAsync(
            new LatticeBackupCaptureRequest("listed", BackupScopeSelector.WholeTree(Source)), "wire-listed");
        await UntilTerminalAsync(handle.OperationId);

        var page = await _host.Client.ListBackupOperationsAsync(new LatticeOperationListRequest { PageSize = 500 });
        var cancelled = await _host.Client.CancelBackupOperationAsync(handle.OperationId);
        var unknown = await _host.Client.GetBackupOperationStatusAsync("never-started");

        Assert.Multiple(() =>
        {
            Assert.That(page.Operations.Select(o => o.OperationId), Does.Contain("wire-listed"));
            Assert.That(cancelled!.State, Is.EqualTo(LatticeOperationState.Succeeded), "A finished operation is unchanged.");
            Assert.That(unknown, Is.Null);
        });
    }

    [Test]
    public async Task A_cold_restore_start_round_trips_over_the_wire()
    {
        var captured = await UntilTerminalAsync((await _host.Client.StartBackupAsync(
            new LatticeBackupCaptureRequest("cold", BackupScopeSelector.WholeTree(Source)))).OperationId);

        var cold = await _host.Client.StartColdRestoreAsync(
            new LatticeRestoreRequest(captured.ResultReference!, "grpc-ops-cold"));

        Assert.Multiple(async () =>
        {
            Assert.That(cold.Kind, Is.EqualTo(BackupOperationKinds.ColdRestore));
            Assert.That((await UntilTerminalAsync(cold.OperationId)).State, Is.EqualTo(LatticeOperationState.Succeeded));
        });
    }

    [Test]
    public void Client_operation_verbs_validate_their_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _host.Client.StartBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.StartIncrementalBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.StartBackupSetAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.StartRestoreAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.StartColdRestoreAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.GetBackupOperationStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(async () => await _host.Client.ListBackupOperationsAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _host.Client.CancelBackupOperationAsync(string.Empty), Throws.ArgumentException);
        });
    }
}
