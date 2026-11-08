using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6017 on the deep usage aggregator: a WAL partition whose retained
/// scan was refused (or failed) reports its last measured figures instead of
/// contributing nothing, so the report the admission and metering consumers
/// read stays an estimate rather than a silently lowered lower bound.
/// </summary>
public sealed partial class LatticeStorageUsageGrainTests
{
    [Test]
    public async Task GetReportAsync_refused_wal_partition_keeps_its_previous_bytes_and_is_not_partial()
    {
        var time = new ManualTimeProvider();
        long retainedNow = 400;
        var grain = CreateGrain(
            shardCount: 1,
            walPartitions: 2,
            shardBehaviour: _ => Task.FromResult(new ShardStorageUsage { LeafStateBytes = 10, LiveKeys = 3 }),
            walBehaviour: partition => Task.FromResult(partition == 0 ? Interlocked.Read(ref retainedNow) : 100L),
            timeProvider: time);

        var first = await grain.GetReportAsync(forceRefresh: true, CancellationToken.None);
        Interlocked.Exchange(ref retainedNow, -1);
        time.Advance(TimeSpan.FromMinutes(2));
        var second = await grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.WalRetainedBytes, Is.EqualTo(500L));
            Assert.That(second.WalRetainedBytes, Is.EqualTo(500L), "the refused partition reports its last figure");
            Assert.That(second.TotalBytes, Is.EqualTo(first.TotalBytes));
            Assert.That(second.Partial, Is.False, "a carried figure does not make the report partial");
            Assert.That(second.Stale, Is.True);
            Assert.That(second.OldestMeasuredAtUtc, Is.EqualTo(first.SampledAt));
        });
    }

    [Test]
    public async Task GetReportAsync_refused_wal_partition_past_the_carry_cap_is_partial()
    {
        var time = new ManualTimeProvider();
        long retainedNow = 400;
        var grain = CreateGrain(
            shardCount: 1,
            walPartitions: 1,
            shardBehaviour: _ => Task.FromResult(new ShardStorageUsage()),
            walBehaviour: _ => Task.FromResult(Interlocked.Read(ref retainedNow)),
            timeProvider: time);

        await grain.GetReportAsync(forceRefresh: true, CancellationToken.None);
        Interlocked.Exchange(ref retainedNow, -1);
        time.Advance(WalPartitionUsageCarryMaxAge + TimeSpan.FromSeconds(1));
        var report = await grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.Stale, Is.False);
            Assert.That(report.WalRetainedBytes, Is.Zero);
        });
    }

    private static TimeSpan WalPartitionUsageCarryMaxAge
        => Orleans.Lattice.BPlusTree.Grains.WalPartitionUsageCarry.MaxCarryAge;
}
