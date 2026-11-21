using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests.Models;

/// <summary>
/// The storage-usage refresh operation contract (#4126): its kind, phase and unit
/// constants, and the round trip of the cluster totals through the result map the
/// tracked refresh records.
/// </summary>
[TestFixture]
public sealed class StorageUsageRefreshResultsTests
{
    [Test]
    public void The_operation_constants_name_the_kind_phase_and_unit()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StorageUsageRefreshOperation.Kind, Is.EqualTo("treeadmin.storage-usage-refresh"));
            Assert.That(StorageUsageRefreshOperation.MeasuringPhase, Is.EqualTo("Measuring"));
            Assert.That(StorageUsageRefreshOperation.TreesUnit, Is.EqualTo("trees"));
        });
    }

    [Test]
    public void The_cluster_totals_round_trip_as_a_deep_summary_without_per_tree_rows()
    {
        var summary = new ClusterStorageUsageSummary
        {
            TreeCount = 3,
            WalRetainedBytes = 10,
            SnapshotBytes = 20,
            LeafStateBytes = 30,
            TotalBytes = 60,
            Partial = false,
            SampledAt = new DateTimeOffset(2026, 10, 1, 9, 30, 0, TimeSpan.Zero),
        };

        var map = StorageUsageRefreshResults.ToResultMap(summary);

        Assert.Multiple(() =>
        {
            Assert.That(map[StorageUsageRefreshResults.TotalBytesKey], Is.EqualTo("60"));
            Assert.That(map[StorageUsageRefreshResults.PartialKey], Is.EqualTo("false"));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out var read), Is.True);
            Assert.That(read, Is.EqualTo(summary with { Deep = true }));
            Assert.That(read!.Trees, Is.Empty);
        });
    }

    [Test]
    public void A_stale_refresh_round_trips_its_stale_flag()
    {
        var summary = new ClusterStorageUsageSummary { TreeCount = 1, Stale = true, SampledAt = DateTimeOffset.UnixEpoch };

        var map = StorageUsageRefreshResults.ToResultMap(summary);

        Assert.Multiple(() =>
        {
            Assert.That(map[StorageUsageRefreshResults.StaleKey], Is.EqualTo("true"));
            Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out var read), Is.True);
            Assert.That(read!.Stale, Is.True);
        });
    }

    [Test]
    public void A_result_recorded_before_the_stale_key_reads_as_not_stale()
    {
        var map = new Dictionary<string, string>(StorageUsageRefreshResults.ToResultMap(new ClusterStorageUsageSummary { Stale = true }));
        map.Remove(StorageUsageRefreshResults.StaleKey);

        Assert.Multiple(() =>
        {
            Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out var read), Is.True);
            Assert.That(read!.Stale, Is.False);
        });
    }

    [Test]
    public void A_malformed_stale_flag_is_not_read()
    {
        var map = new Dictionary<string, string>(StorageUsageRefreshResults.ToResultMap(new ClusterStorageUsageSummary()))
        {
            [StorageUsageRefreshResults.StaleKey] = "maybe",
        };

        Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out _), Is.False);
    }

    [Test]
    public void A_map_missing_a_total_is_not_read()
    {
        var map = new Dictionary<string, string>(StorageUsageRefreshResults.ToResultMap(new ClusterStorageUsageSummary()));
        map.Remove(StorageUsageRefreshResults.SampledAtKey);

        Assert.Multiple(() =>
        {
            Assert.That(StorageUsageRefreshResults.TryReadSummary(map, out var read), Is.False);
            Assert.That(read, Is.Null);
        });
    }
}
