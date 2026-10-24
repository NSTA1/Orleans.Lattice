using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalCommitLogWriterTests
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    [TestCase(false, false, true)]
    [TestCase(false, true, true)]
    public async Task Append_records_every_returned_offset_age_for_old_stamps(
        bool batch, bool infiniteDeadline, bool batchedSingle = false)
    {
        var tree = $"append-age-{Guid.NewGuid():N}";
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(10L);
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult<IReadOnlyList<long>>(
                Enumerable.Range(10, call.Arg<IReadOnlyList<WalRecord>>().Count).Select(i => (long)i).ToArray()));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalBatchedSingleEntryAppends = batchedSingle,
            WalAppendDispatchTimeout = infiniteDeadline ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(30),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var writer = new WalCommitLogWriter(factory, monitor,
            TestOptionsResolver.Create(baseOptions: options, factory: factory),
            Substitute.For<ILatticeMergeModeResolver>(), TestOriginClusterIdResolver.Default());
        var oldStamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.AddHours(-6).Ticks };
        var entry = new WalRecord { TreeId = tree, Key = "k", Op = MutationKind.Tombstone, Timestamp = oldStamp };
        var started = DateTime.UtcNow.Ticks;

        var offsets = batch
            ? await writer.AppendManyAsync(new[] { entry, entry })
            : new[] { await writer.AppendAsync(entry) };
        var now = DateTime.UtcNow.Ticks;

        var timeline = WalCommitLogWriter._walAppendTimes[tree][0];
        foreach (var offset in offsets)
        {
            var lag = timeline.GetLagTicks(offset - 1, now, out var incomplete);
            Assert.That(incomplete, Is.False);
            Assert.That(lag, Is.InRange(0, now - started));
        }
        Assert.That(entry.Timestamp, Is.EqualTo(oldStamp));
        WalCommitLogWriter._walAppendTimes.TryRemove(tree, out _);
    }
}
