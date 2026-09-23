using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers which grain overload a bulk WAL append carrying exactly one entry
/// dispatches to, as selected by
/// <see cref="LatticeOptions.WalBatchedSingleEntryAppends"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="WalCommitLogWriter.AppendManyAsync"/> historically collapsed a
/// single-entry bulk append onto <see cref="IWalShardGrain.AppendAsync"/> to
/// match that overload's per-call allocation cost. That overload takes an
/// <b>exclusive</b> grain turn, and an Orleans activation stays busy across
/// awaits, so the partition is held for a whole provider round trip. Under a
/// wide fan-out whose per-leaf slices are one entry each - the dominant shape
/// for uniformly distributed keys - every append serialises against every
/// other append on the same partition, pinning batch occupancy at one entry
/// and making <see cref="LatticeOptions.WalAppendCoalescingInFlightThreshold"/>
/// unreachable, because reaching it requires the very concurrency the
/// exclusive turn removed.
/// </para>
/// <para>
/// These tests assert the dispatch target directly rather than any downstream
/// throughput effect, because the dispatch target is the whole mechanism: a
/// silent revert to the exclusive overload would restore the serialisation
/// while leaving every offset and ordering assertion in the suite green.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public class WalCommitLogWriterSingleEntryDispatchTests
{
    private const string TreeId = "tree-single-entry-dispatch";

    private static (WalCommitLogWriter Writer, IWalShardGrain Shard) CreateWriter(
        bool batchedSingleEntryAppends)
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard
            .AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(7L));
        shard
            .AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult<IReadOnlyList<long>>(
                Enumerable
                    .Range(0, callInfo.Arg<IReadOnlyList<WalRecord>>().Count)
                    .Select(i => (long)(100 + i))
                    .ToArray()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var options = new LatticeOptions
        {
            WalBatchedSingleEntryAppends = batchedSingleEntryAppends,
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: grainFactory);
        var writer = new WalCommitLogWriter(
            grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);

        return (writer, shard);
    }

    private static WalRecord MakeMutation(string key) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
    };

    [Test]
    public async Task AppendManyAsync_single_entry_takes_exclusive_overload_when_option_disabled()
    {
        // The historical behaviour, preserved by default. Asserted explicitly
        // so that enabling the option cannot be mistaken for a no-op and so a
        // future default flip is a deliberate, visible edit to this test.
        var (writer, shard) = CreateWriter(batchedSingleEntryAppends: false);

        var offsets = await writer.AppendManyAsync(new[] { MakeMutation("k0") });

        await shard.Received(1).AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
        await shard.DidNotReceive().AppendBatchAsync(
            Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        Assert.That(offsets, Is.EqualTo(new[] { 7L }));
    }

    [Test]
    public async Task AppendManyAsync_single_entry_takes_interleaving_overload_when_option_enabled()
    {
        // The fix: a bulk append of one entry becomes semantically identical
        // to a bulk append of two, so the partition is no longer held
        // exclusively for the provider round trip.
        var (writer, shard) = CreateWriter(batchedSingleEntryAppends: true);

        var offsets = await writer.AppendManyAsync(new[] { MakeMutation("k0") });

        await shard.Received(1).AppendBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(e => e.Count == 1), Arg.Any<CancellationToken>());
        await shard.DidNotReceive().AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());

        // The offset must come from the batched dispatch, not from a
        // silently-retained singular call.
        Assert.That(offsets, Is.EqualTo(new[] { 100L }));
    }

    [Test]
    public async Task AppendManyAsync_multi_entry_takes_interleaving_overload_under_both_settings()
    {
        // The option must not reach the multi-entry path: that path was
        // already batched and its dispatch target is not in question.
        foreach (var enabled in new[] { false, true })
        {
            var (writer, shard) = CreateWriter(batchedSingleEntryAppends: enabled);

            var offsets = await writer.AppendManyAsync(
                new[] { MakeMutation("k0"), MakeMutation("k1") });

            // Two distinct keys may route to two distinct partitions, so the
            // call count is a routing detail. What is being asserted is the
            // dispatch target: batched, never singular.
            await shard.Received().AppendBatchAsync(
                Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
            await shard.DidNotReceive().AppendAsync(
                Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
            Assert.That(offsets, Has.Count.EqualTo(2), $"option={enabled}");
        }
    }

    [Test]
    public async Task AppendManyAsync_empty_batch_dispatches_nothing_under_both_settings()
    {
        // The empty-batch short circuit sits above the option check, so
        // reading the option must not be reachable for a zero-entry call
        // (entries[0] would throw).
        foreach (var enabled in new[] { false, true })
        {
            var (writer, shard) = CreateWriter(batchedSingleEntryAppends: enabled);

            var offsets = await writer.AppendManyAsync(Array.Empty<WalRecord>());

            Assert.That(offsets, Is.Empty, $"option={enabled}");
            await shard.DidNotReceive().AppendAsync(
                Arg.Any<WalRecord>(), Arg.Any<CancellationToken>());
            await shard.DidNotReceive().AppendBatchAsync(
                Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public void DefaultWalBatchedSingleEntryAppends_is_false_and_is_the_option_default()
    {
        // Guards the "off by default" decision: enabling it broadens an
        // existing race between an append and the exclusive reader methods
        // that report a shard's next sequence and live entry count, so the
        // default is deliberate and a flip must be argued separately.
        Assert.That(LatticeOptions.DefaultWalBatchedSingleEntryAppends, Is.False);
        Assert.That(new LatticeOptions().WalBatchedSingleEntryAppends, Is.False);
    }
}
