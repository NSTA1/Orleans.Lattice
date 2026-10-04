using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4511: a WAL shard's next sequence counts appends whose flush is
/// still in flight, while a read stops below the oldest one, so a durable
/// prepare can sit above a transient hole. The change feed must read up to
/// the tail it captured before it yields a terminal; a real shard cannot
/// hold a hole on demand, so these drive the real feed over shard stubs.
/// </summary>
public partial class ChangeFeedTests
{
    [Test]
    public async Task Subscribe_waits_out_a_transient_hole_before_yielding_a_terminal()
    {
        var txid = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var holed = HoledShard(PrepareEntry(txid, "a", 200), holeReads: 3, nextSequences: [1]);
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(holed.Shard);
        var other = Grain(TerminalEntry(txid, 100));
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(other);
        var feed = HoleFeed(factory, limit: TimeSpan.FromSeconds(30));

        var entries = await CollectAsync(feed.Subscribe(Tree, ChangeFeedCursor.Initial));

        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.Op), Is.EqualTo(new[] { MutationKind.Set, MutationKind.TxCommit }));
            Assert.That(holed.Reads, Is.GreaterThan(3), "the feed stopped at the hole instead of waiting it out");
        });
    }

    [Test]
    public void Subscribe_fails_rather_than_yield_a_terminal_over_a_hole_that_never_fills()
    {
        var txid = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var holed = HoledShard(PrepareEntry(txid, "a", 200), holeReads: int.MaxValue, nextSequences: [1]);
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(holed.Shard);
        var other = Grain(TerminalEntry(txid, 100));
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(other);
        var feed = HoleFeed(factory, limit: TimeSpan.FromMilliseconds(100));

        Assert.That(
            async () => await CollectAsync(feed.Subscribe(Tree, ChangeFeedCursor.Initial)),
            Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task Subscribe_stops_at_a_next_sequence_a_failed_flush_rewound()
    {
        // The tail capture saw an in-flight append at sequence 0 whose flush
        // then failed and rewound the shard, so nothing at 0 ever became
        // durable and there is nothing to wait for.
        var txid = Guid.NewGuid();
        var factory = Substitute.For<IGrainFactory>();
        var holed = HoledShard(PrepareEntry(txid, "a", 200), holeReads: int.MaxValue, nextSequences: [1, 0]);
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(holed.Shard);
        var other = Grain(Entry("b", Hlc(100)));
        factory.GetGrain<IWalShardGrain>($"{Tree}/1").Returns(other);
        var feed = HoleFeed(factory, limit: TimeSpan.FromSeconds(30));

        var entries = await CollectAsync(feed.Subscribe(Tree, ChangeFeedCursor.Initial));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "b" }));
    }

    private static ChangeFeed HoleFeed(IGrainFactory factory, TimeSpan limit)
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        return new ChangeFeed(factory, Monitor(partitions: 2), resolver)
        {
            TailCatchUpLimit = limit,
            TailCatchUpPollInterval = TimeSpan.FromMilliseconds(5),
        };
    }

    private static WalRecord PrepareEntry(Guid txid, string key, long ticks) =>
        Entry(key, Hlc(ticks)) with { TransactionId = txid, IsPrepared = true };

    private static WalRecord TerminalEntry(Guid txid, long ticks) =>
        Entry(string.Empty, Hlc(ticks)) with { Op = MutationKind.TxCommit, Value = null!, TransactionId = txid };

    // A one-entry shard whose entry sits above a hole for the first
    // `holeReads` reads. GetNextSequenceAsync answers `nextSequences` in
    // order, repeating the last.
    private static HoledShardStub HoledShard(WalRecord entry, int holeReads, long[] nextSequences)
    {
        var stub = new HoledShardStub(Substitute.For<IWalShardGrain>());
        var nextCalls = 0;
        stub.Shard.GetNextSequenceAsync(Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromResult(nextSequences[Math.Min(nextCalls++, nextSequences.Length - 1)]));
        stub.Shard.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = (long)call[0];
                stub.Reads++;
                if (from > 0 || stub.Reads <= holeReads)
                {
                    return ValueTask.FromResult(WalShardPage.Empty(from));
                }

                return ValueTask.FromResult(new WalShardPage
                {
                    Entries = [new WalShardSequencedEntry { Sequence = 0, Entry = entry }],
                    NextSequence = 1,
                });
            });
        return stub;
    }

    private sealed class HoledShardStub(IWalShardGrain shard)
    {
        public IWalShardGrain Shard { get; } = shard;

        public int Reads { get; set; }
    }
}
