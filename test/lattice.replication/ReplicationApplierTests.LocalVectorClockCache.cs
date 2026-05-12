using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the producer-side local vector clock cache hook on
/// <see cref="ReplicationApplier"/>. After a successful inbound apply
/// that advances the per-origin high-water-mark grain, the applier
/// mirrors the advance into the producer cache so a subsequent local
/// emit's <see cref="WalRecord.VectorClock"/> reflects the foreign
/// progress. Dedup, range-delete, and apply-throw paths must not
/// advance the cache.
/// </summary>
[TestFixture]
public class ReplicationApplierLocalVectorClockCacheTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor()
    {
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (
        ReplicationApplier Applier,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm,
        LocalVectorClockCache Cache)
        CreateApplier(bool tryAdvanceReturns = true)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(tryAdvanceReturns);
        var cache = new LocalVectorClockCache(factory);
        var applier = new ReplicationApplier(factory, Monitor(), cache);
        return (applier, apply, hwm, cache);
    }

    private static WalRecord SetEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    [Test]
    public async Task ApplyAsync_advances_foreign_entry_after_successful_apply()
    {
        var (applier, _, _, cache) = CreateApplier();
        var ts = Hlc(50);

        await applier.ApplyAsync(SetEntry("k", ts));

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(ts),
            "Successful apply that advanced the HWM grain must mirror the advance into the producer cache.");
    }

    [Test]
    public async Task ApplyAsync_does_not_advance_cache_when_TryAdvance_reports_no_advance()
    {
        // TryAdvanceAsync returning false means the entry was a benign
        // re-delivery already covered by the HWM. The cache must mirror
        // exactly what the grain reports - no shadow advance.
        var (applier, _, _, cache) = CreateApplier(tryAdvanceReturns: false);

        await applier.ApplyAsync(SetEntry("k", Hlc(50)));

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task ApplyAsync_does_not_advance_cache_for_local_origin_entry()
    {
        // Local-origin entries short-circuit before HWM and never
        // call TryAdvanceAsync - they must not bleed into the cache
        // via this code path. The local diagonal is advanced by
        // ShardedReplogSink instead.
        var (applier, _, _, cache) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(50), origin: LocalCluster));

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(LocalCluster), Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task ApplyAsync_does_not_advance_cache_when_HWM_already_dedupes()
    {
        // entry.Timestamp <= existing HWM short-circuits the apply
        // entirely (Applied=false, dedup outcome) - no TryAdvanceAsync
        // call, therefore no cache advance.
        var (applier, _, hwm, cache) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        await applier.ApplyAsync(SetEntry("k", Hlc(50)));

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task ApplyAsync_advances_cache_pointwise_max_across_multiple_origins()
    {
        var (applier, _, _, cache) = CreateApplier();

        await applier.ApplyAsync(SetEntry("k", Hlc(20), origin: RemoteCluster));
        await applier.ApplyAsync(SetEntry("k", Hlc(35), origin: "site-c"));
        await applier.ApplyAsync(SetEntry("k", Hlc(40), origin: RemoteCluster));

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(Hlc(40)));
            Assert.That(snapshot.GetClock("site-c"), Is.EqualTo(Hlc(35)));
        });
    }

    [Test]
    public async Task ApplyBatchAsync_advances_cache_once_per_origin_run()
    {
        var (applier, _, _, cache) = CreateApplier();
        var entries = new List<WalRecord>
        {
            SetEntry("k1", Hlc(10), origin: RemoteCluster),
            SetEntry("k2", Hlc(20), origin: RemoteCluster),
            SetEntry("k3", Hlc(30), origin: RemoteCluster),
            SetEntry("k4", Hlc(15), origin: "site-c"),
            SetEntry("k5", Hlc(25), origin: "site-c"),
        };

        await applier.ApplyBatchAsync(entries);

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(Hlc(30)),
                "Batch path must advance the cache to the run's highest applied HLC.");
            Assert.That(snapshot.GetClock("site-c"), Is.EqualTo(Hlc(25)));
        });
    }

    [Test]
    public async Task ApplyAsync_does_not_advance_cache_for_range_delete()
    {
        // Range deletes carry HybridLogicalClock.Zero by design and
        // bypass the per-origin HWM check entirely. They must not
        // touch the producer cache: a Zero "advance" would be a no-op
        // via pointwise-max anyway, but the applier short-circuits
        // before the AdvanceForeign call site so the cache should
        // remain at HLC.Zero for the origin.
        var (applier, _, _, cache) = CreateApplier();

        await applier.ApplyAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "b",
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = RemoteCluster,
        });

        var snapshot = await cache.GetSnapshotAsync(Tree);
        Assert.That(snapshot.GetClock(RemoteCluster), Is.EqualTo(HybridLogicalClock.Zero),
            "Range delete must not advance the producer cache.");
    }
}
