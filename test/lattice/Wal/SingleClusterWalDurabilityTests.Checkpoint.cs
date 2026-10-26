namespace Orleans.Lattice.Tests.Wal;

public sealed partial class SingleClusterWalDurabilityTests
{
    [TestCase(-1L, false)]
    [TestCase(null, false)]
    [TestCase(0L, true)]
    public async Task Checkpoint_readiness_with_override_hold_requires_a_durable_offset(
        long? offset, bool expectedReady)
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_0";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(treeId, 0,
            [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
            CancellationToken.None);
        var pins = new Dictionary<string, HybridLogicalClock>
        {
            [consumerId] = new() { WallClockTicks = 1 },
        };
        var offsets = new Dictionary<string, long>();
        if (offset is { } checkpoint)
        {
            offsets[consumerId] = checkpoint;
        }

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 1, pins, offsets, new HashSet<string> { consumerId });

        Assert.That(ready, Is.EqualTo(expectedReady),
            "An empty-release HLC is not checkpoint coverage for a subsequent held write.");
    }

    [Test]
    public async Task Checkpoint_readiness_does_not_count_an_empty_partition_with_an_assigned_offset()
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_1";
        var provider = new InMemoryWalStorageProvider();
        for (var partition = 0; partition < 2; partition++)
        {
            await provider.AppendBatchAsync(treeId, partition,
                [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
                CancellationToken.None);
        }
        await provider.TrimAsync(treeId, 0, 0, CancellationToken.None);
        var highest = await provider.GetHighestOffsetAsync(treeId, 0, CancellationToken.None);
        var lowest = await provider.GetLowestOffsetAsync(treeId, 0, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(highest,
                Is.EqualTo(0), "The allocation watermark survives a full trim.");
            Assert.That(lowest,
                Is.EqualTo(-1), "The unheld partition has no live entry left to trim.");
        });

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 2,
            new Dictionary<string, HybridLogicalClock> { [consumerId] = HybridLogicalClock.Zero },
            new Dictionary<string, long> { [consumerId] = -1 },
            new HashSet<string>());

        Assert.That(ready, Is.False,
            "Only the held partition retains WAL; a historical allocation is not an available trim target.");
    }

    [Test]
    public async Task Checkpoint_readiness_allows_an_unheld_live_partition_despite_another_partition_hold()
    {
        const string treeId = "checkpoint-readiness";
        const string consumerId = "bplusleaf/00000000000000000000000000000001_1";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(treeId, 0,
            [new WalEntry { Offset = 0, Mutation = new LatticeMutation { TreeId = treeId, Key = "key" } }],
            CancellationToken.None);

        var ready = await HasCheckpointedPartitionAsync(
            treeId, provider, 2,
            new Dictionary<string, HybridLogicalClock> { [consumerId] = new() { WallClockTicks = 1 } },
            new Dictionary<string, long> { [consumerId] = -1 },
            new HashSet<string> { consumerId });

        Assert.That(ready, Is.True, "A hold on another partition must not stall a usable trim target.");
    }

    private static async Task<bool> HasCheckpointedPartitionAsync(
        string treeId,
        IWalStorageProvider provider,
        int partitions,
        IReadOnlyDictionary<string, HybridLogicalClock> pins,
        IReadOnlyDictionary<string, long> offsets,
        IReadOnlySet<string> overrideHolds)
    {
        var held = new HashSet<int>(pins
            .Where(static pin => pin.Value <= HybridLogicalClock.Zero)
            .Select(static pin => pin.Key)
            .Concat(overrideHolds.Where(consumerId =>
                !offsets.TryGetValue(consumerId, out var offset) || offset < 0))
            .Select(static consumerId => int.Parse(consumerId[(consumerId.LastIndexOf('_') + 1)..],
                System.Globalization.CultureInfo.InvariantCulture)));
        var free = false;
        for (var partition = 0; partition < partitions && !free; partition++)
        {
            free = !held.Contains(partition)
                && await provider.GetLowestOffsetAsync(treeId, partition, CancellationToken.None) >= 0;
        }

        return pins.Count > 0 && free;
    }
}
