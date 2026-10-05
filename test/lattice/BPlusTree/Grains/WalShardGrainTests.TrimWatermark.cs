using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    // Issue #4621: offsets are not dense. A flush abandoned at its deadline that
    // never lands leaves a permanent hole, and a hole directly above a trim point
    // is indistinguishable from a trim by the lowest stored offset. The trim
    // watermark tells them apart for every reader of the tail: the leaf's
    // IsPrefixLost check, the WAL log subscriber, and the fall-off detector.

    [Test]
    public async Task A_hole_directly_above_the_trim_watermark_is_not_a_fall_off_for_the_leaf_or_the_subscriber()
    {
        var (grain, reader) = await HoleAboveTrimPointAsync(trustWatermark: true);

        var tail = await reader.GetTailOffsetAsync(TreeId, 0);
        var drained = await DrainFromAsync(reader, checkpoint: 1);

        Assert.Multiple(() =>
        {
            Assert.That(tail, Is.EqualTo(2L), "the tail is one past the trim watermark, not the lowest stored offset");
            Assert.That(WalFallOffCore.IsPrefixLost(1, tail), Is.False,
                "a leaf checkpointed through the trim point has lost nothing to the hole above it");
            Assert.That(drained.Result.FellOffLog, Is.False, "the subscriber reads past the hole");
            Assert.That(drained.Offsets, Is.EqualTo(new long[] { 3 }));
        });
        Assert.That(await grain.GetTrimWatermarkAsync(CancellationToken.None), Is.EqualTo(1L));
    }

    [Test]
    public async Task A_trim_past_a_readers_position_is_still_a_fall_off_with_the_trim_watermark()
    {
        var (_, reader) = await HoleAboveTrimPointAsync(trustWatermark: true);

        // Offset 1 was trimmed before a reader at checkpoint 0 read it.
        var tail = await reader.GetTailOffsetAsync(TreeId, 0);
        var drained = await DrainFromAsync(reader, checkpoint: 0);

        Assert.Multiple(() =>
        {
            Assert.That(WalFallOffCore.IsPrefixLost(0, tail), Is.True, "the leaf lost offset 1 to the trim");
            Assert.That(drained.Result.FellOffLog, Is.True, "the subscriber lost offset 1 to the trim");
        });
    }

    [Test]
    public async Task Without_a_trusted_trim_watermark_a_reader_treats_the_hole_above_a_trim_point_as_trimmed()
    {
        // A silo in the cluster predates the watermark: it may have trimmed without
        // moving it, so the reader falls back to the conservative rule.
        var (grain, reader) = await HoleAboveTrimPointAsync(trustWatermark: false);

        var tail = await reader.GetTailOffsetAsync(TreeId, 0);

        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetTrimWatermarkAsync(CancellationToken.None), Is.Null);
            Assert.That(tail, Is.EqualTo(3L), "the tail is the lowest stored offset");
            Assert.That(WalFallOffCore.IsPrefixLost(1, tail), Is.True);
        });
    }

    /// <summary>
    /// A shard holding offsets 0, 1 and 3 - offset 2 was allocated but never
    /// written - trimmed through 1, with a real <see cref="WalCommitLogReader"/>
    /// over it.
    /// </summary>
    private static async Task<(WalShardGrain Grain, WalCommitLogReader Reader)> HoleAboveTrimPointAsync(bool trustWatermark)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(TreeId, 0, [WalEntryAt(0), WalEntryAt(1)], CancellationToken.None);
        await provider.AppendBatchAsync(TreeId, 0, [WalEntryAt(3)], CancellationToken.None);
        var grain = await CreateGrainAsync(provider);
        grain.TrimWatermarkSupportForTesting = () => trustWatermark;
        await provider.TrimAsync(TreeId, 0, 1, CancellationToken.None);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(grain);
        return (grain, new WalCommitLogReader(factory));
    }

    private static async Task<(WalDrainResult Result, List<long> Offsets)> DrainFromAsync(WalCommitLogReader reader, long checkpoint)
    {
        var handler = new OffsetCollectingHandler();
        var result = await new WalLogSubscriber(reader, new InMemoryWalCursorRegistry()).DrainAsync(
            new WalSubscriptionContext(TreeId, "trim-watermark-reader", 1, new Dictionary<int, long> { [0] = checkpoint })
            {
                PinWal = false,
            },
            handler,
            CancellationToken.None);
        return (result, handler.Offsets);
    }

    private static WalEntry WalEntryAt(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = [1],
            Timestamp = new HybridLogicalClock { WallClockTicks = 10 + offset, Counter = 0 },
            OriginClusterId = "site-a",
        },
    };

    private sealed class OffsetCollectingHandler : IWalSubscriptionHandler
    {
        public List<long> Offsets { get; } = new();

        public HybridLogicalClock? BlockedAtHlc { get; set; }

        public void OnEntry(in WalSubscriptionEntry entry) => Offsets.Add(entry.Offset);
    }
}
