using System.Text;
using Grpc.Core;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll backup maintenance RPCs (#4125)
/// through the public <see cref="LatticeBackupApiGrpcClient"/> over a real gRPC
/// channel: a health check, a catalog rebuild and a catalog scrub each start, poll
/// to their outcome and carry their result over the wire, and the per-operation
/// authorizer sees each new RPC as its own operation.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupGrpcMaintenanceOperationsE2ETests
{
    private const string Source = "grpc-maint";

    private GrpcBackupClusterFixture _fixture = null!;
    private GrpcBackupHost _host = null!;
    private string _backupId = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrpcBackupClusterFixture();
        await _fixture.InitializeAsync();
        _host = await _fixture.CreateGrpcHostAsync(new AllowAllBackupApiAuthorizer());
        await _fixture.GrainFactory.GetGrain<ILattice>(Source).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var captured = await UntilTerminalAsync((await _host.Client.StartBackupAsync(
            new LatticeBackupCaptureRequest("maint", BackupScopeSelector.WholeTree(Source)))).OperationId);
        _backupId = captured.ResultReference!;
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
    public async Task A_health_check_start_round_trips_its_verdict_over_the_wire()
    {
        var handle = await _host.Client.StartBackupHealthCheckAsync(_backupId, "wire-health");
        var status = await UntilTerminalAsync(handle.OperationId);
        var report = await _host.Client.GetBackupHealthAsync(_backupId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("wire-health"));
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.HealthCheck));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.Result[BackupOperationResultKeys.HealthStatus], Is.EqualTo(nameof(BackupHealthStatus.Healthy)));
            Assert.That(status.UnitName, Is.EqualTo(BackupOperationUnits.Artifacts));
            Assert.That(report!.Status, Is.EqualTo(BackupHealthStatus.Healthy), "The report was persisted server-side.");
        });
    }

    [Test]
    public async Task Catalog_rebuild_and_scrub_starts_round_trip_their_reports_over_the_wire()
    {
        var rebuild = await _host.Client.StartCatalogRebuildAsync("wire-rebuild");
        var scrub = await _host.Client.StartCatalogScrubAsync(pruneOrphans: false, operationId: "wire-scrub");
        var rebuilt = await UntilTerminalAsync(rebuild.OperationId);
        var scrubbed = await UntilTerminalAsync(scrub.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(rebuild.Kind, Is.EqualTo(BackupOperationKinds.CatalogRebuild));
            Assert.That(scrub.Kind, Is.EqualTo(BackupOperationKinds.CatalogScrub));
            Assert.That(BackupOperationResults.TryReadCatalogRebuildReport(rebuilt.Result, out var rebuildReport), Is.True);
            Assert.That(rebuildReport!.ScannedCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(BackupOperationResults.TryReadCatalogScrubReport(scrubbed.Result, out var scrubReport), Is.True);
            Assert.That(scrubReport!.Pruned, Is.False);
        });
    }

    [Test]
    public void An_uncatalogued_health_check_maps_to_not_found()
    {
        var ex = Assert.ThrowsAsync<RpcException>(async () => await _host.Client.StartBackupHealthCheckAsync("no-such-backup"));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void Client_maintenance_verbs_validate_their_arguments()
    {
        Assert.That(async () => await _host.Client.StartBackupHealthCheckAsync(string.Empty), Throws.ArgumentException);
    }

    [TestCase(LatticeBackupGrpcMethods.StartBackupHealthCheckMethodName, LatticeBackupApiOperation.StartBackupHealthCheck)]
    [TestCase(LatticeBackupGrpcMethods.StartCatalogRebuildMethodName, LatticeBackupApiOperation.StartCatalogRebuild)]
    [TestCase(LatticeBackupGrpcMethods.StartCatalogScrubMethodName, LatticeBackupApiOperation.StartCatalogScrub)]
    public void Each_maintenance_rpc_is_presented_to_the_authorizer_as_its_own_operation(string method, LatticeBackupApiOperation expected)
    {
        var (operation, _) = LatticeBackupApiGrpcAuthInterceptor.DescribeCall("/svc/" + method, new BackupCatalogScrubRequestMessage());

        Assert.That(operation, Is.EqualTo(expected));
    }

    [Test]
    public void A_health_check_start_names_its_backup_and_a_catalog_operation_names_no_target()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeBackupApiGrpcAuthInterceptor.DescribeCall(
                    "/svc/" + LatticeBackupGrpcMethods.StartBackupHealthCheckMethodName,
                    new BackupHealthCheckRequestMessage { BackupId = "b1" }).TargetId,
                Is.EqualTo("b1"));
            Assert.That(
                LatticeBackupApiGrpcAuthInterceptor.DescribeCall(
                    "/svc/" + LatticeBackupGrpcMethods.StartCatalogRebuildMethodName,
                    new BackupCatalogRebuildRequestMessage()).TargetId,
                Is.Null);
        });
    }

    [Test]
    public async Task A_denying_authorizer_refuses_a_catalog_rebuild_start()
    {
        await using var denied = await _fixture.CreateGrpcHostAsync(new DenyAllBackupApiAuthorizer());

        var ex = Assert.ThrowsAsync<RpcException>(async () => await denied.Client.StartCatalogRebuildAsync());

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }
}
