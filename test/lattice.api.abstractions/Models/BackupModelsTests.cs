using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the hand-written constructors and <see cref="ArgumentNullException"/>
/// guards of the backup control result records. The serialization fixture only
/// round-trips uninitialised instances, so this construction logic is otherwise
/// uncovered.
/// </summary>
[TestFixture]
public class BackupModelsTests
{
    // The BackupChainDescription / BackupScopeStatus constructors only null-check
    // and store the manifest / scope references without inspecting them, so an
    // uninitialised BackupManifest is a sufficient, deterministic reference.
    private static BackupManifest UninitializedManifest()
        => (BackupManifest)RuntimeHelpers.GetUninitializedObject(typeof(BackupManifest));

    [Test]
    public void BackupChainDescription_ctor_captures_manifest_and_chain()
    {
        var manifest = UninitializedManifest();
        var chain = new[] { "base", "inc-1" };

        var description = new BackupChainDescription(manifest, chain);

        Assert.That(description.Manifest, Is.SameAs(manifest));
        Assert.That(description.ChainBackupIds, Is.SameAs(chain));
    }

    [Test]
    public void BackupChainDescription_ctor_throws_for_null_manifest()
        => Assert.That(() => new BackupChainDescription(null!, Array.Empty<string>()),
            Throws.ArgumentNullException);

    [Test]
    public void BackupChainDescription_ctor_throws_for_null_chain()
        => Assert.That(() => new BackupChainDescription(UninitializedManifest(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void BackupInventoryReport_ctor_captures_all_fields()
    {
        var oldest = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var newest = new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero);

        var report = new BackupInventoryReport(
            totalBackupCount: 10,
            totalCatalogBytes: 2048,
            fullBackupCount: 3,
            incrementalBackupCount: 7,
            oldestBackupUtc: oldest,
            newestBackupUtc: newest,
            captureFailureCount: 1,
            restoreFailureCount: 2,
            bytesReclaimed: 512);

        Assert.That(report.TotalBackupCount, Is.EqualTo(10));
        Assert.That(report.TotalCatalogBytes, Is.EqualTo(2048));
        Assert.That(report.FullBackupCount, Is.EqualTo(3));
        Assert.That(report.IncrementalBackupCount, Is.EqualTo(7));
        Assert.That(report.OldestBackupUtc, Is.EqualTo(oldest));
        Assert.That(report.NewestBackupUtc, Is.EqualTo(newest));
        Assert.That(report.CaptureFailureCount, Is.EqualTo(1));
        Assert.That(report.RestoreFailureCount, Is.EqualTo(2));
        Assert.That(report.BytesReclaimed, Is.EqualTo(512));
    }

    [Test]
    public void BackupInventoryReport_ctor_accepts_null_timestamps_for_empty_catalog()
    {
        var report = new BackupInventoryReport(0, 0, 0, 0, null, null, 0, 0, 0);

        Assert.That(report.OldestBackupUtc, Is.Null);
        Assert.That(report.NewestBackupUtc, Is.Null);
        Assert.That(report.TotalBackupCount, Is.Zero);
    }

    [Test]
    public void BackupScopeStatus_ctor_captures_all_fields()
    {
        var scope = BackupScopeSelector.WholeTree("orders");
        var fullRun = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);

        var status = new BackupScopeStatus(
            scope,
            fullScheduleRegistered: true,
            incrementalScheduleRegistered: false,
            lastFullRunUtc: fullRun,
            lastFullSuccessUtc: fullRun,
            lastIncrementalRunUtc: null,
            lastIncrementalSuccessUtc: null,
            lastRunOutcome: BackupScopeRunOutcome.Success,
            chainDepth: 4,
            runtimeFullBackupInterval: TimeSpan.FromHours(1),
            runtimeIncrementalBackupInterval: TimeSpan.FromMinutes(5));

        Assert.That(status.Scope, Is.SameAs(scope));
        Assert.That(status.FullScheduleRegistered, Is.True);
        Assert.That(status.IncrementalScheduleRegistered, Is.False);
        Assert.That(status.LastFullRunUtc, Is.EqualTo(fullRun));
        Assert.That(status.LastFullSuccessUtc, Is.EqualTo(fullRun));
        Assert.That(status.LastIncrementalRunUtc, Is.Null);
        Assert.That(status.LastIncrementalSuccessUtc, Is.Null);
        Assert.That(status.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Success));
        Assert.That(status.ChainDepth, Is.EqualTo(4));
        Assert.That(status.RuntimeFullBackupInterval, Is.EqualTo(TimeSpan.FromHours(1)));
        Assert.That(status.RuntimeIncrementalBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void BackupScopeStatus_ctor_defaults_optional_intervals_to_null()
    {
        var status = new BackupScopeStatus(
            BackupScopeSelector.WholeTree("orders"),
            fullScheduleRegistered: false,
            incrementalScheduleRegistered: false,
            lastFullRunUtc: null,
            lastFullSuccessUtc: null,
            lastIncrementalRunUtc: null,
            lastIncrementalSuccessUtc: null,
            lastRunOutcome: BackupScopeRunOutcome.None,
            chainDepth: 0);

        Assert.That(status.RuntimeFullBackupInterval, Is.Null);
        Assert.That(status.RuntimeIncrementalBackupInterval, Is.Null);
    }

    [Test]
    public void BackupScopeStatus_ctor_throws_for_null_scope()
        => Assert.That(() => new BackupScopeStatus(
            null!, false, false, null, null, null, null, BackupScopeRunOutcome.None, 0),
            Throws.ArgumentNullException);
}
