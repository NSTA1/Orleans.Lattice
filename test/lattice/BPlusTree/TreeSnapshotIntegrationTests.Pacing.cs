using System.Diagnostics;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for issue #5455: a snapshot left to its own coordinator
/// advanced one step per two-second phase-timer tick, and an offline snapshot is
/// a lock, a copy and an unlock per shard. On the journey estate a 96-shard
/// source was still locked, in UnlockSource with 82 of 96 shards copied, when the
/// caller gave up after six minutes. These tests trigger the snapshot through the
/// public surface and never drive it with <c>RunSnapshotPassAsync</c>, so the
/// coordinator's own pacing is what is measured, across the fixture's two silos.
/// </summary>
public partial class TreeSnapshotIntegrationTests
{
    /// <summary>
    /// A default-sized tree has 64 shards, so the one-step-per-tick pace took
    /// about four and a half minutes; the bound is far below that and far above
    /// what the work needs.
    /// </summary>
    private static readonly TimeSpan CoordinatorPacedSnapshotBound = TimeSpan.FromSeconds(60);

    [TestCase(SnapshotMode.Offline)]
    [TestCase(SnapshotMode.Online)]
    public async Task A_snapshot_driven_by_its_own_coordinator_completes_promptly_and_releases_the_source(
        SnapshotMode mode)
    {
        var sourceTree = $"snap-paced-{mode}-{Guid.NewGuid():N}".ToLowerInvariant();
        var destTree = $"snap-paced-dest-{mode}-{Guid.NewGuid():N}".ToLowerInvariant();
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(sourceTree);

        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 12; i++)
        {
            var key = $"line-{i:D2}";
            var value = $"value-{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            expected[key] = value;
        }

        await tree.SnapshotAsync(destTree, mode);

        var snapshot = _cluster.GrainFactory.GetGrain<ITreeSnapshotGrain>(sourceTree);
        var clock = Stopwatch.StartNew();
        var last = await snapshot.GetProgressAsync();
        while (!await tree.IsSnapshotCompleteAsync())
        {
            last = await snapshot.GetProgressAsync();
            if (clock.Elapsed > CoordinatorPacedSnapshotBound)
            {
                Assert.Fail(
                    $"The {mode} snapshot was still running after {clock.Elapsed.TotalSeconds:F0}s: "
                    + $"phase {last.Phase}, {last.CopiedShardCount} of {last.ShardCount} shards copied. "
                    + "Its coordinator is advancing one step per phase-timer tick (issue #5455).");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        Assert.That(clock.Elapsed, Is.LessThan(CoordinatorPacedSnapshotBound));

        var dest = _cluster.GrainFactory.GetGrain<ILattice>(destTree);
        foreach (var (key, value) in expected)
        {
            var copied = await dest.GetAsync(key);
            Assert.That(copied, Is.Not.Null, $"Key '{key}' missing from the snapshot");
            Assert.That(Encoding.UTF8.GetString(copied!), Is.EqualTo(value));
        }

        // The source is back in service: every shard reads and accepts writes.
        foreach (var key in expected.Keys)
        {
            Assert.That(await tree.GetAsync(key), Is.Not.Null, $"Key '{key}' unreadable on the source after the snapshot");
        }

        await tree.SetAsync("after-snapshot", Encoding.UTF8.GetBytes("written"));
        Assert.That(await tree.GetAsync("after-snapshot"), Is.Not.Null);
    }
}
