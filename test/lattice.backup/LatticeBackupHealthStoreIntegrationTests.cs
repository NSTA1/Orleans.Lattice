namespace Orleans.Lattice.Backup.Tests;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Integration coverage for <see cref="ILatticeBackupHealthStore"/>: report and
/// per-backup configuration round-trips over the dogfooded reserved
/// <c>sys-backup-health</c> tree, list, and removal of both report and config.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupHealthStoreIntegrationTests
{
    private BackupClusterFixture _fixture = null!;

    private ILatticeBackupHealthStore Store =>
        _fixture.SiloServices.GetRequiredService<ILatticeBackupHealthStore>();

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new BackupClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static BackupHealthReport Report(string backupId, BackupHealthStatus status = BackupHealthStatus.Healthy) =>
        new(
            backupId,
            status,
            manifestPresent: true,
            missingArtifactIds: Array.Empty<string>(),
            hashMismatchArtifactIds: Array.Empty<string>(),
            checkedAtUtc: DateTimeOffset.UnixEpoch,
            explanation: "ok");

    [Test]
    public async Task SetReportAsync_then_GetReportAsync_round_trips()
    {
        await Store.SetReportAsync(Report("h-1", BackupHealthStatus.Warning));

        var readBack = await Store.GetReportAsync("h-1");

        Assert.That(readBack, Is.Not.Null);
        Assert.That(readBack!.Status, Is.EqualTo(BackupHealthStatus.Warning));
    }

    [Test]
    public async Task GetReportAsync_unknown_backup_returns_null() =>
        Assert.That(await Store.GetReportAsync("never-stored"), Is.Null);

    [Test]
    public async Task SetConfigAsync_then_GetConfigAsync_round_trips()
    {
        await Store.SetConfigAsync("h-cfg", new BackupHealthConfig(monitoringEnabled: false, TimeSpan.FromHours(3)));

        var config = await Store.GetConfigAsync("h-cfg");

        Assert.That(config, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(config!.MonitoringEnabled, Is.False);
            Assert.That(config.Interval, Is.EqualTo(TimeSpan.FromHours(3)));
        });
    }

    [Test]
    public async Task ListReportsAsync_returns_every_stored_report_in_id_order()
    {
        await Store.SetReportAsync(Report("h-charlie"));
        await Store.SetReportAsync(Report("h-alpha"));
        await Store.SetReportAsync(Report("h-bravo"));

        var ids = new List<string>();
        await foreach (var report in Store.ListReportsAsync())
        {
            ids.Add(report.BackupId);
        }

        Assert.That(ids, Is.EqualTo(new[] { "h-alpha", "h-bravo", "h-charlie" }));
    }

    [Test]
    public async Task RemoveAsync_removes_both_report_and_config()
    {
        await Store.SetReportAsync(Report("h-del"));
        await Store.SetConfigAsync("h-del", new BackupHealthConfig(true, TimeSpan.FromHours(1)));

        var removed = await Store.RemoveAsync("h-del");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(removed, Is.True);
            Assert.That(await Store.GetReportAsync("h-del"), Is.Null);
            Assert.That(await Store.GetConfigAsync("h-del"), Is.Null);
        });
    }
}
