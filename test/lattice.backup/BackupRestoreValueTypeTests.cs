namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for the two plain in-process value types on the restore /
/// scheduling surface: <see cref="RestoreAdmissionReport"/>, the self-describing
/// size and topology a coordinated restore uses to hard-refuse an infeasible
/// target before it fences the fleet, and
/// <see cref="LatticeBackupScheduleRequest"/>, the recurring-schedule
/// registration. Both validate eagerly in their constructors, so each guard is
/// asserted individually.
/// </summary>
[TestFixture]
public sealed class BackupRestoreValueTypeTests
{
    private static RestoreAdmissionReport Report(
        string backupId = "backup-1",
        string targetTreeId = "orders",
        long totalByteLength = 4096,
        long totalChunkCount = 8,
        int shardCount = 4,
        IReadOnlyList<string>? chain = null) =>
        new(backupId, targetTreeId, totalByteLength, totalChunkCount, shardCount, chain ?? ["base", "backup-1"]);

    [Test]
    public void RestoreAdmissionReport_exposes_every_probed_field()
    {
        var report = Report();

        Assert.Multiple(() =>
        {
            Assert.That(report.BackupId, Is.EqualTo("backup-1"));
            Assert.That(report.TargetTreeId, Is.EqualTo("orders"));
            Assert.That(report.TotalByteLength, Is.EqualTo(4096));
            Assert.That(report.TotalChunkCount, Is.EqualTo(8));
            Assert.That(report.ShardCount, Is.EqualTo(4));
            Assert.That(report.ManifestChain, Is.EqualTo(new[] { "base", "backup-1" }));
        });
    }

    [Test]
    public void RestoreAdmissionReport_accepts_a_zero_sized_chain()
    {
        var report = Report(totalByteLength: 0, totalChunkCount: 0, chain: []);

        Assert.Multiple(() =>
        {
            Assert.That(report.TotalByteLength, Is.Zero);
            Assert.That(report.TotalChunkCount, Is.Zero);
            Assert.That(report.ManifestChain, Is.Empty);
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void RestoreAdmissionReport_rejects_a_missing_backup_id(string? backupId)
    {
        Assert.That(() => Report(backupId: backupId!), Throws.InstanceOf<ArgumentException>());
    }

    [TestCase(null)]
    [TestCase("")]
    public void RestoreAdmissionReport_rejects_a_missing_target_tree_id(string? treeId)
    {
        Assert.That(() => Report(targetTreeId: treeId!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RestoreAdmissionReport_rejects_a_negative_byte_length()
    {
        Assert.That(() => Report(totalByteLength: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void RestoreAdmissionReport_rejects_a_negative_chunk_count()
    {
        Assert.That(() => Report(totalChunkCount: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-3)]
    public void RestoreAdmissionReport_rejects_a_non_positive_shard_count(int shardCount)
    {
        Assert.That(() => Report(shardCount: shardCount), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void RestoreAdmissionReport_rejects_a_null_manifest_chain()
    {
        Assert.That(
            () => new RestoreAdmissionReport("backup-1", "orders", 4096, 8, 4, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ScheduleRequest_exposes_its_scope_kind_and_cadence()
    {
        var scope = BackupScopeSelector.WholeTree("orders");

        var request = new LatticeBackupScheduleRequest(scope, incremental: true, TimeSpan.FromHours(4));

        Assert.Multiple(() =>
        {
            Assert.That(request.Scope, Is.SameAs(scope));
            Assert.That(request.Incremental, Is.True);
            Assert.That(request.Interval, Is.EqualTo(TimeSpan.FromHours(4)));
        });
    }

    [Test]
    public void ScheduleRequest_rejects_a_null_scope()
    {
        Assert.That(
            () => new LatticeBackupScheduleRequest(null!, false, TimeSpan.FromMinutes(5)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ScheduleRequest_rejects_a_non_positive_interval()
    {
        var scope = BackupScopeSelector.WholeTree("orders");

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new LatticeBackupScheduleRequest(scope, false, TimeSpan.Zero),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => new LatticeBackupScheduleRequest(scope, false, TimeSpan.FromMinutes(-1)),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void ScheduleRequest_is_a_value_record()
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        var a = new LatticeBackupScheduleRequest(scope, true, TimeSpan.FromHours(1));
        var b = new LatticeBackupScheduleRequest(scope, true, TimeSpan.FromHours(1));

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void ScheduleRequest_supports_non_destructive_mutation()
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        var original = new LatticeBackupScheduleRequest(scope, true, TimeSpan.FromHours(1));

        var slower = original with { Interval = TimeSpan.FromHours(12) };

        Assert.Multiple(() =>
        {
            Assert.That(slower.Interval, Is.EqualTo(TimeSpan.FromHours(12)));
            Assert.That(slower.Incremental, Is.True);
            Assert.That(original.Interval, Is.EqualTo(TimeSpan.FromHours(1)));
        });
    }
}
