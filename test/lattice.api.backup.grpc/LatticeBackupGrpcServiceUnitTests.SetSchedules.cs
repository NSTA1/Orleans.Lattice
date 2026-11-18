using Grpc.Core;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// The backup-set schedule RPCs of <see cref="LatticeBackupGrpcService"/> (#6067):
/// each forwards to <see cref="ILatticeBackupControl"/> and wraps its answer, a
/// missing schedule travels as <c>Found=false</c>, an unknown schedule on run-now
/// maps to <see cref="StatusCode.NotFound"/>, and a facade that does not support
/// backup-set schedules answers <see cref="StatusCode.Unimplemented"/>.
/// </summary>
public sealed partial class LatticeBackupGrpcServiceUnitTests
{
    [Test]
    public async Task ScheduleBackupSet_forwards_the_request_and_wraps_the_status()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        var status = BackupSetScheduleTestData.Status();
        var request = BackupSetScheduleTestData.Request();
        control.ScheduleBackupSetAsync(request, Arg.Any<CancellationToken>()).Returns(status);

        var response = await CreateService(control).ScheduleBackupSet(new BackupSetScheduleRequestMessage { Request = request }, Context());

        Assert.Multiple(() =>
        {
            Assert.That(response.Found, Is.True);
            Assert.That(response.Status, Is.SameAs(status));
        });
    }

    [Test]
    public async Task Cancel_remove_and_run_now_forward_the_name_and_unwrap_the_result()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.CancelBackupSetScheduleAsync("a", Arg.Any<CancellationToken>()).Returns(true);
        control.RemoveBackupSetScheduleAsync("b", true, Arg.Any<CancellationToken>()).Returns(true);
        control.RunBackupSetScheduleNowAsync("c", Arg.Any<CancellationToken>()).Returns(false);
        var service = CreateService(control);

        var cancelled = await service.CancelBackupSetSchedule(new BackupSetScheduleNameRequestMessage { Name = "a" }, Context());
        var removed = await service.RemoveBackupSetSchedule(
            new BackupSetScheduleRemoveRequestMessage { Name = "b", DeleteRetainedSets = true }, Context());
        var ran = await service.RunBackupSetScheduleNow(new BackupSetScheduleNameRequestMessage { Name = "c" }, Context());

        Assert.Multiple(() =>
        {
            Assert.That(cancelled.Result, Is.True);
            Assert.That(removed.Result, Is.True);
            Assert.That(ran.Result, Is.False, "A run already in flight answers false.");
        });
    }

    [Test]
    public async Task GetBackupSetScheduleStatus_reports_found_and_not_found()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        var status = BackupSetScheduleTestData.Status();
        control.GetBackupSetScheduleStatusAsync("nightly", Arg.Any<CancellationToken>()).Returns(status);
        control.GetBackupSetScheduleStatusAsync("absent", Arg.Any<CancellationToken>()).Returns((BackupSetScheduleStatus?)null);
        var service = CreateService(control);

        var found = await service.GetBackupSetScheduleStatus(new BackupSetScheduleNameRequestMessage { Name = "nightly" }, Context());
        var missing = await service.GetBackupSetScheduleStatus(new BackupSetScheduleNameRequestMessage { Name = "absent" }, Context());

        Assert.Multiple(() =>
        {
            Assert.That((found.Found, found.Status), Is.EqualTo((true, status)));
            Assert.That((missing.Found, missing.Status), Is.EqualTo((false, (BackupSetScheduleStatus?)null)));
        });
    }

    [Test]
    public async Task GetBackupSetScheduleProgress_reports_progress_idle_and_not_found()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        var progress = BackupSetScheduleTestData.Progress();
        control.GetBackupSetScheduleProgressAsync("running", Arg.Any<CancellationToken>()).Returns(progress);
        control.GetBackupSetScheduleProgressAsync("idle", Arg.Any<CancellationToken>()).Returns((BackupSetScheduleRunProgress?)null);
        control.GetBackupSetScheduleProgressAsync("absent", Arg.Any<CancellationToken>())
            .ThrowsAsync(new KeyNotFoundException("No backup-set schedule is named 'absent'."));
        var service = CreateService(control);

        var running = await service.GetBackupSetScheduleProgress(new BackupSetScheduleNameRequestMessage { Name = "running" }, Context());
        var idle = await service.GetBackupSetScheduleProgress(new BackupSetScheduleNameRequestMessage { Name = "idle" }, Context());
        var missing = await service.GetBackupSetScheduleProgress(new BackupSetScheduleNameRequestMessage { Name = "absent" }, Context());

        Assert.Multiple(() =>
        {
            Assert.That((running.Found, running.Progress), Is.EqualTo((true, progress)));
            Assert.That((idle.Found, idle.Progress), Is.EqualTo((true, (BackupSetScheduleRunProgress?)null)));
            Assert.That((missing.Found, missing.Progress), Is.EqualTo((false, (BackupSetScheduleRunProgress?)null)));
        });
    }

    [Test]
    public void GetBackupSetScheduleProgress_on_a_facade_without_it_answers_unimplemented()
    {
        var service = CreateService(new NoSetScheduleControl());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.GetBackupSetScheduleProgress(new BackupSetScheduleNameRequestMessage { Name = "nightly" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void GetBackupSetScheduleProgress_maps_a_denial_to_permission_denied()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.GetBackupSetScheduleProgressAsync("guarded", Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CreateService(control)
            .GetBackupSetScheduleProgress(new BackupSetScheduleNameRequestMessage { Name = "guarded" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    [Test]
    public async Task ListBackupSetSchedules_wraps_every_status()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        var statuses = new[] { BackupSetScheduleTestData.Status("a"), BackupSetScheduleTestData.Status("b") };
        control.ListBackupSetSchedulesAsync(Arg.Any<CancellationToken>()).Returns(statuses);

        var response = await CreateService(control).ListBackupSetSchedules(new BackupSetScheduleListRequestMessage(), Context());

        Assert.That(response.Schedules, Is.EqualTo(statuses));
    }

    [Test]
    public void RunBackupSetScheduleNow_maps_an_unknown_schedule_to_not_found()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.RunBackupSetScheduleNowAsync("absent", Arg.Any<CancellationToken>())
            .ThrowsAsync(new KeyNotFoundException("No backup-set schedule is named 'absent'."));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CreateService(control)
            .RunBackupSetScheduleNow(new BackupSetScheduleNameRequestMessage { Name = "absent" }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.NotFound));
    }

    [Test]
    public void A_facade_without_backup_set_schedules_answers_unimplemented()
    {
        var service = CreateService(new NoSetScheduleControl());

        var ex = Assert.ThrowsAsync<RpcException>(async () =>
            await service.ListBackupSetSchedules(new BackupSetScheduleListRequestMessage(), Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.Unimplemented));
    }

    [Test]
    public void A_backup_set_schedule_denial_maps_to_permission_denied()
    {
        var control = Substitute.For<ILatticeBackupControl>();
        control.ScheduleBackupSetAsync(Arg.Any<LatticeBackupSetScheduleRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new LatticeAuthorizationDeniedException("no"));

        var ex = Assert.ThrowsAsync<RpcException>(async () => await CreateService(control).ScheduleBackupSet(
            new BackupSetScheduleRequestMessage { Request = BackupSetScheduleTestData.Request() }, Context()));

        Assert.That(ex!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
    }

    /// <summary>
    /// A facade that implements none of the backup-set schedule members, so they
    /// fall through to the interface defaults, which throw
    /// <see cref="NotSupportedException"/>.
    /// </summary>
    private sealed class NoSetScheduleControl : ILatticeBackupControl
    {
        public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(string backupId, string artifactId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task ScheduleBackupAsync(LatticeBackupScheduleRequest request, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default) => throw new InvalidOperationException();

        public Task<BackupHealthConfig?> GetBackupHealthConfigAsync(string backupId, CancellationToken cancellationToken = default) => throw new InvalidOperationException();
    }
}
