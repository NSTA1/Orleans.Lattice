namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the backup-health value types: <see cref="BackupHealthReport"/>,
/// <see cref="BackupHealthConfig"/>, <see cref="BackupHealthStatus"/>, and
/// <see cref="LatticeBackupHealthOptions"/>. Exercises construction, parameter
/// guards, the computed <see cref="BackupHealthReport.IsHealthy"/> helper, and the
/// documented option defaults.
/// </summary>
[TestFixture]
public sealed class BackupHealthModelTests
{
    private static BackupHealthReport Report(
        BackupHealthStatus status = BackupHealthStatus.Healthy,
        bool manifestPresent = true) =>
        new(
            "backup-1",
            status,
            manifestPresent,
            missingArtifactIds: Array.Empty<string>(),
            hashMismatchArtifactIds: Array.Empty<string>(),
            checkedAtUtc: DateTimeOffset.UnixEpoch,
            explanation: "ok");

    [Test]
    public void Report_constructor_populates_every_field()
    {
        var report = new BackupHealthReport(
            "b",
            BackupHealthStatus.Warning,
            manifestPresent: true,
            missingArtifactIds: new[] { "m1" },
            hashMismatchArtifactIds: new[] { "h1", "h2" },
            checkedAtUtc: DateTimeOffset.UnixEpoch,
            explanation: "torn");

        Assert.Multiple(() =>
        {
            Assert.That(report.BackupId, Is.EqualTo("b"));
            Assert.That(report.Status, Is.EqualTo(BackupHealthStatus.Warning));
            Assert.That(report.ManifestPresent, Is.True);
            Assert.That(report.MissingArtifactIds, Is.EqualTo(new[] { "m1" }));
            Assert.That(report.HashMismatchArtifactIds, Is.EqualTo(new[] { "h1", "h2" }));
            Assert.That(report.CheckedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(report.Explanation, Is.EqualTo("torn"));
        });
    }

    [Test]
    public void Report_constructor_empty_backup_id_throws() =>
        Assert.That(
            () => new BackupHealthReport(
                string.Empty,
                BackupHealthStatus.Healthy,
                true,
                Array.Empty<string>(),
                Array.Empty<string>(),
                DateTimeOffset.UnixEpoch,
                "x"),
            Throws.ArgumentException);

    [Test]
    public void Report_constructor_null_missing_list_throws() =>
        Assert.That(
            () => new BackupHealthReport(
                "b",
                BackupHealthStatus.Healthy,
                true,
                null!,
                Array.Empty<string>(),
                DateTimeOffset.UnixEpoch,
                "x"),
            Throws.ArgumentNullException);

    [Test]
    public void Report_constructor_null_mismatch_list_throws() =>
        Assert.That(
            () => new BackupHealthReport(
                "b",
                BackupHealthStatus.Healthy,
                true,
                Array.Empty<string>(),
                null!,
                DateTimeOffset.UnixEpoch,
                "x"),
            Throws.ArgumentNullException);

    [Test]
    public void Report_constructor_null_explanation_throws() =>
        Assert.That(
            () => new BackupHealthReport(
                "b",
                BackupHealthStatus.Healthy,
                true,
                Array.Empty<string>(),
                Array.Empty<string>(),
                DateTimeOffset.UnixEpoch,
                null!),
            Throws.ArgumentNullException);

    [Test]
    public void Report_IsHealthy_true_only_for_healthy_status()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Report(BackupHealthStatus.Healthy).IsHealthy, Is.True);
            Assert.That(Report(BackupHealthStatus.Warning).IsHealthy, Is.False);
            Assert.That(Report(BackupHealthStatus.Missing, manifestPresent: false).IsHealthy, Is.False);
            Assert.That(Report(BackupHealthStatus.Unknown).IsHealthy, Is.False);
        });
    }

    [Test]
    public void Config_constructor_populates_fields()
    {
        var config = new BackupHealthConfig(monitoringEnabled: true, interval: TimeSpan.FromHours(2));
        Assert.Multiple(() =>
        {
            Assert.That(config.MonitoringEnabled, Is.True);
            Assert.That(config.Interval, Is.EqualTo(TimeSpan.FromHours(2)));
        });
    }

    [Test]
    public void Config_constructor_non_positive_interval_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new BackupHealthConfig(true, TimeSpan.Zero), Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new BackupHealthConfig(true, TimeSpan.FromSeconds(-1)), Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Options_defaults_are_enabled_and_six_hours()
    {
        var options = new LatticeBackupHealthOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.Enabled, Is.True);
            Assert.That(options.DefaultInterval, Is.EqualTo(TimeSpan.FromHours(6)));
            Assert.That(LatticeBackupHealthOptions.DefaultSweepInterval, Is.EqualTo(TimeSpan.FromHours(6)));
            Assert.That(LatticeBackupHealthOptions.MinimumInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
        });
    }

    [Test]
    public void Status_enum_has_the_expected_ordinal_ordering()
    {
        // The monitor and UI rely on Missing > Warning > Healthy > Unknown to pick
        // the worst status across a set's members.
        Assert.Multiple(() =>
        {
            Assert.That((int)BackupHealthStatus.Unknown, Is.EqualTo(0));
            Assert.That((int)BackupHealthStatus.Healthy, Is.EqualTo(1));
            Assert.That((int)BackupHealthStatus.Warning, Is.EqualTo(2));
            Assert.That((int)BackupHealthStatus.Missing, Is.EqualTo(3));
        });
    }
}
