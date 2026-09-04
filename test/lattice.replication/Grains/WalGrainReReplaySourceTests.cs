using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Tests for <see cref="WalGrainReReplaySource"/>, the read-only production
/// source that pages retained write-ahead-log entries out of the local shard's
/// WAL partition grains for the targeted leaf re-replay repair.
/// <para>
/// The two paths a single short page never reaches are the multi-page cursor
/// advance and the trimmed-partition report - the latter being what tells the
/// repair engine the local WAL was garbage collected past the divergence point.
/// </para>
/// </summary>
[TestFixture]
public sealed class WalGrainReReplaySourceTests
{
    private const string Tree = "orders";

    [Test]
    public async Task ReadAsync_reports_no_trim_and_collects_a_single_short_page()
    {
        var factory = Substitute.For<IGrainFactory>();
        Partition(factory, 0, Page([Seq(0, 10), Seq(1, 20)], next: 2));

        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 1, pageSize: 8);
        var result = await source.ReadAsync(CancellationToken.None);

        Assert.That(result.Entries, Has.Count.EqualTo(2));
        Assert.That(result.WasTrimmed, Is.False);
        Assert.That(result.OldestRetainedHlc, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task ReadAsync_advances_the_cursor_across_full_pages_until_the_log_ends()
    {
        // A full page means there may be more, so the source must re-read from
        // the reported NextSequence rather than stopping at the page boundary.
        // The read budget is partitionCount * pageSize, so two partitions of
        // page size two leave room for a second page on partition zero.
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(0, 2, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(Page([Seq(0, 10), Seq(1, 20)], next: 2)));
        grain.ReadAsync(2, 2, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(Page([Seq(2, 30)], next: 3)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(grain);
        Partition(factory, 1, WalShardPage.Empty(0));

        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 2, pageSize: 2);
        var result = await source.ReadAsync(CancellationToken.None);

        Assert.That(result.Entries.Select(e => e.Timestamp.WallClockTicks).ToArray(), Is.EqualTo(new long[] { 10, 20, 30 }));
        await grain.Received(1).ReadAsync(2, 2, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadAsync_reports_a_trimmed_partition_and_its_oldest_retained_clock()
    {
        // A first page whose oldest entry sits above sequence zero means the
        // provider returned the oldest *retained* offset: the tail was trimmed.
        var factory = Substitute.For<IGrainFactory>();
        Partition(factory, 0, Page([Seq(7, 500), Seq(8, 600)], next: 9));

        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 1, pageSize: 8);
        var result = await source.ReadAsync(CancellationToken.None);

        Assert.That(result.WasTrimmed, Is.True);
        Assert.That(result.OldestRetainedHlc.WallClockTicks, Is.EqualTo(500));
    }

    [Test]
    public async Task ReadAsync_reports_the_lowest_oldest_retained_clock_across_trimmed_partitions()
    {
        // Every trimmed partition contributes a candidate; the engine needs the
        // globally oldest retained clock to compare against the peer's cursor,
        // so a later partition with an older clock must win.
        var factory = Substitute.For<IGrainFactory>();
        Partition(factory, 0, Page([Seq(4, 900)], next: 5));
        Partition(factory, 1, Page([Seq(2, 300)], next: 3));
        Partition(factory, 2, Page([Seq(6, 700)], next: 7));

        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 3, pageSize: 8);
        var result = await source.ReadAsync(CancellationToken.None);

        Assert.That(result.WasTrimmed, Is.True);
        Assert.That(result.OldestRetainedHlc.WallClockTicks, Is.EqualTo(300));
        Assert.That(result.Entries, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task ReadAsync_treats_an_empty_partition_as_untrimmed()
    {
        var factory = Substitute.For<IGrainFactory>();
        Partition(factory, 0, WalShardPage.Empty(0));

        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 1, pageSize: 8);
        var result = await source.ReadAsync(CancellationToken.None);

        Assert.That(result.Entries, Is.Empty);
        Assert.That(result.WasTrimmed, Is.False);
    }

    [Test]
    public void ReadAsync_honours_a_cancelled_token()
    {
        var factory = Substitute.For<IGrainFactory>();
        Partition(factory, 0, WalShardPage.Empty(0));
        var source = new WalGrainReReplaySource(factory, Tree, partitionCount: 1, pageSize: 8);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await source.ReadAsync(cts.Token));
    }

    private static void Partition(IGrainFactory factory, int partition, WalShardPage page)
    {
        var grain = Substitute.For<IWalShardGrain>();
        grain.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(page));
        factory.GetGrain<IWalShardGrain>($"{Tree}/{partition}").Returns(grain);
    }

    private static WalShardPage Page(WalShardSequencedEntry[] entries, long next) => new()
    {
        Entries = entries,
        NextSequence = next,
    };

    private static WalShardSequencedEntry Seq(long sequence, long ticks) => new()
    {
        Sequence = sequence,
        Entry = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k" + sequence,
            Value = [1],
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
            OriginClusterId = "cluster-a",
        },
    };
}
