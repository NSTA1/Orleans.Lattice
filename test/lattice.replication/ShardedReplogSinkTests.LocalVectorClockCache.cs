using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the producer-side local vector clock cache hook on
/// <see cref="ShardedReplogSink"/>. The sink advances the local
/// cluster's diagonal entry post-WAL-append for local-origin entries
/// only; foreign-origin entries (replays of remote writes) are
/// advanced post-apply via <see cref="ReplicationApplier"/>'s
/// <see cref="LocalVectorClockCache.AdvanceForeign"/> call.
/// </summary>
[TestFixture]
public class ShardedReplogSinkLocalVectorClockCacheTests
{
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ReplogPartitions = 1,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (
        ShardedReplogSink Sink,
        IGrainFactory Factory,
        IWalShardGrain WalGrain,
        LocalVectorClockCache Cache)
        CreateSink(bool walAppendThrows = false)
    {
        var factory = Substitute.For<IGrainFactory>();
        var walGrain = Substitute.For<IWalShardGrain>();
        if (walAppendThrows)
        {
            walGrain.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
                .Throws(new InvalidOperationException("simulated WAL failure"));
        }
        else
        {
            walGrain.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        }
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(walGrain);
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        var cache = new LocalVectorClockCache(factory);
        var sink = new ShardedReplogSink(factory, Monitor(), cache, NullLogger<ShardedReplogSink>.Instance);
        return (sink, factory, walGrain, cache);
    }

    [Test]
    public async Task WriteAsync_advances_local_diagonal_after_successful_append()
    {
        var (sink, _, _, cache) = CreateSink();
        var ts = Hlc(100);

        await sink.WriteAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = LocalCluster,
        }, CancellationToken.None);

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(ts));
    }

    [Test]
    public async Task WriteAsync_does_not_advance_diagonal_for_foreign_origin_entry()
    {
        // Foreign-origin entries (replays of a remote write that the
        // receiver chose to forward back into its WAL) must not
        // advance the local diagonal - only AdvanceForeign on apply
        // touches foreign entries. This test asserts the routing
        // discipline: an inbound replay flowing through the sink
        // does not silently bump the producer cache for the wrong
        // origin.
        var (sink, _, _, cache) = CreateSink();

        await sink.WriteAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = Hlc(100),
            OriginClusterId = RemoteCluster,
        }, CancellationToken.None);

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(HybridLogicalClock.Zero),
                "Foreign-origin append must not advance the local diagonal.");
            Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(HybridLogicalClock.Zero),
                "Sink must not stamp foreign entries - that is AdvanceForeign's job on the apply path.");
        });
    }

    [Test]
    public async Task WriteAsync_throwing_append_does_not_advance_diagonal()
    {
        // A failed WAL append must not advance the producer cache -
        // the cache mirrors what is actually persisted, so a thrown
        // AppendAsync must surface to the caller without a phantom
        // diagonal bump.
        var (sink, _, _, cache) = CreateSink(walAppendThrows: true);

        Assert.That(
            async () => await sink.WriteAsync(new WalRecord
            {
                TreeId = Tree,
                Op = MutationKind.Set,
                Key = "k",
                Value = new byte[] { 1 },
                Timestamp = Hlc(100),
                OriginClusterId = LocalCluster,
            }, CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(HybridLogicalClock.Zero),
            "A thrown WAL append must not advance the producer cache.");
    }

    [Test]
    public async Task WriteAsync_advances_diagonal_pointwise_max_across_consecutive_appends()
    {
        var (sink, _, _, cache) = CreateSink();

        await sink.WriteAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = Hlc(50),
            OriginClusterId = LocalCluster,
        }, CancellationToken.None);
        await sink.WriteAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 2 },
            Timestamp = Hlc(80),
            OriginClusterId = LocalCluster,
        }, CancellationToken.None);

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(Hlc(80)));
    }
}
