using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Wal;

public sealed partial class WalLogSubscriberTests
{
    [Test]
    public async Task DrainAsync_idle_partitions_issue_only_one_head_grain_call_each()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetReadableHeadAsync(Arg.Any<CancellationToken>()).Returns(ValueTask.FromResult(5L));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(wal);
        var subscriber = new WalLogSubscriber(new WalCommitLogReader(factory), new InMemoryWalCursorRegistry());
        var checkpoints = Enumerable.Range(0, 4).ToDictionary(p => p, _ => 4L);

        var result = await subscriber.DrainAsync(Context(4, checkpoints, pinWal: false), new CollectingHandler());

        await wal.Received(4).GetReadableHeadAsync(Arg.Any<CancellationToken>());
        await wal.DidNotReceive().ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        await wal.DidNotReceive().GetTailOffsetAsync(Arg.Any<CancellationToken>());
        Assert.That(result.AdvancedOffsets, Is.Empty);
        Assert.That(result.EntriesRead, Is.Zero);
    }

    [Test]
    public async Task DrainAsync_skips_only_idle_partitions_and_rechecks_them_on_the_next_drain()
    {
        var (subscriber, reader, _) = Create();
        reader.Append(Tree, 0, Set(10));
        // Partition 1 is idle at its probe; the write arrives while partition 0
        // drains. Its cursor must stay put until the next head probe sees it.
        reader.BeforeRead = () =>
        {
            reader.BeforeRead = null;
            reader.Append(Tree, 1, Set(20));
        };
        var handler = new CollectingHandler();
        var first = await subscriber.DrainAsync(Context(2, pinWal: false), handler);

        Assert.That(reader.Reads.Select(r => r.Shard), Is.EqualTo(new[] { 0 }));
        Assert.That(first.AdvancedOffsets.Keys, Is.EqualTo(new[] { 0 }));
        Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new[] { 0L }));

        var second = await subscriber.DrainAsync(
            Context(2, first.AdvancedOffsets, pinWal: false), handler);

        Assert.That(reader.Reads.Select(r => r.Shard), Is.EqualTo(new[] { 0, 1 }));
        Assert.That(second.AdvancedOffsets[1], Is.Zero);
        Assert.That(handler.Entries.Select(e => e.Timestamp), Is.EqualTo(new[] { Hlc(10), Hlc(20) }));
    }

    [Test]
    public async Task DrainAsync_idle_partition_keeps_existing_checkpoint_and_blocked_pin()
    {
        var (subscriber, reader, registry) = Create();
        reader.Append(Tree, 0, Set(10));
        var handler = new CollectingHandler { BlockedAtHlc = Hlc(5) };
        var context = new WalSubscriptionContext(Tree, Consumer, 1, new Dictionary<int, long> { [0] = 0 })
        {
            HighestApplied = Hlc(10),
        };

        var result = await subscriber.DrainAsync(context, handler);

        Assert.That(reader.Reads, Is.Empty);
        Assert.That(result.AdvancedOffsets, Is.Empty);
        Assert.That(result.HighestTimestamp, Is.EqualTo(Hlc(10)));
        Assert.That(await registry.GetBlockedFloorAsync(Tree, CancellationToken.None), Is.EqualTo(Hlc(5)));
        var offsets = (await registry.SnapshotAsync(Tree)).Single().CheckpointOffsets;
        Assert.That(offsets, Is.Not.Null);
        Assert.That(offsets![0], Is.Zero);
    }
}
