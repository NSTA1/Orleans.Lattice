using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationApplierTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId = LocalCluster)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = clusterId };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (
        ReplicationApplier Applier,
        IGrainFactory Factory,
        IReplicationApplyGrain Apply,
        IReplicationHighWaterMarkGrain Hwm)
        CreateApplier(string treeId = Tree, string originClusterId = RemoteCluster)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(treeId).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>($"{treeId}/{originClusterId}").Returns(hwm);
        hwm.GetAsync(Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        var applier = new ReplicationApplier(factory, Monitor());
        return (applier, factory, apply, hwm);
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static ReplogEntry SetEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    private static ReplogEntry DeleteEntry(string key, HybridLogicalClock ts, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Delete,
        Key = key,
        Timestamp = ts,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    private static ReplogEntry RangeDeleteEntry(string startInclusive, string endExclusive, string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.DeleteRange,
        Key = startInclusive,
        EndExclusiveKey = endExclusive,
        Timestamp = HybridLogicalClock.Zero,
        IsTombstone = true,
        OriginClusterId = origin,
    };

    [Test]
    public async Task ApplyAsync_routes_set_through_apply_grain_with_source_hlc_and_origin()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10, 1);

        var result = await applier.ApplyAsync(SetEntry("k", ts) with { ExpiresAtTicks = 99 });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, 99);
        await hwm.Received(1).TryAdvanceAsync(ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_delete_through_apply_grain_with_source_hlc_and_origin()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(20);

        var result = await applier.ApplyAsync(DeleteEntry("k", ts));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyDeleteAsync("k", ts, RemoteCluster);
        await hwm.Received(1).TryAdvanceAsync(ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_routes_range_delete_with_origin_only()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyDeleteRangeAsync("a", "z", RemoteCluster);

        // Range deletes carry HLC.Zero; the HWM is not advanced for them
        // because dedupe does not apply (range applies are naturally
        // idempotent at the leaf layer).
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default, default);
    }

    [Test]
    public async Task ApplyAsync_dedupes_when_entry_timestamp_equals_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<CancellationToken>()).Returns(Hlc(10, 1));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10, 1)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(10, 1)));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default, default);
    }

    [Test]
    public async Task ApplyAsync_dedupes_when_entry_timestamp_below_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<CancellationToken>()).Returns(Hlc(50));

        var result = await applier.ApplyAsync(DeleteEntry("k", Hlc(20)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplyDeleteAsync(default!, default, default!);
    }

    [Test]
    public async Task ApplyAsync_advances_hwm_after_successful_apply()
    {
        var (applier, _, _, hwm) = CreateApplier();
        var current = HybridLogicalClock.Zero;
        hwm.GetAsync(Arg.Any<CancellationToken>()).Returns(_ => current);
        hwm.TryAdvanceAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var candidate = (HybridLogicalClock)call[0];
                if (candidate > current)
                {
                    current = candidate;
                    return Task.FromResult(true);
                }
                return Task.FromResult(false);
            });

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(7)));

        Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task ApplyAsync_skips_local_origin_entries_as_no_op()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(5), origin: LocalCluster));

        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default, default);
    }

    [Test]
    public void ApplyAsync_throws_when_tree_id_is_empty()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { TreeId = "" };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_origin_cluster_id_is_null()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { OriginClusterId = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_origin_cluster_id_is_empty()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { OriginClusterId = string.Empty };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_set_entry_has_null_value()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(1)) with { Value = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_throws_when_range_delete_has_null_end()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = RangeDeleteEntry("a", "z") with { EndExclusiveKey = null };

        Assert.That(async () => await applier.ApplyAsync(entry), Throws.ArgumentException);
    }

    [Test]
    public void ApplyAsync_observes_cancellation()
    {
        var (applier, _, _, _) = CreateApplier();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await applier.ApplyAsync(SetEntry("k", Hlc(1)), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task ApplyAsync_resolves_options_with_entry_tree_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Arg.Any<string>()).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        hwm.GetAsync(Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>()).Returns(true);

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>())
            .Returns(new LatticeReplicationOptions { ClusterId = LocalCluster });
        var applier = new ReplicationApplier(factory, monitor);

        await applier.ApplyAsync(new ReplogEntry
        {
            TreeId = "alpha",
            Op = ReplogOp.Set,
            Key = "k",
            Value = new byte[] { 1 },
            Timestamp = Hlc(1),
            OriginClusterId = RemoteCluster,
        });

        monitor.Received().Get("alpha");
    }

    [Test]
    public async Task ApplyAsync_returns_zero_high_water_mark_for_local_origin_no_op()
    {
        // Local-origin entries skip the HWM grain entirely (the row would
        // never carry state) — verify the seam reports Zero rather than
        // making a needless grain call.
        var (applier, _, _, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(5), origin: LocalCluster));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default);
    }

    [Test]
    public async Task ApplyAsync_returns_zero_high_water_mark_for_range_delete()
    {
        // Range deletes bypass per-origin HWM dedupe by design; the HWM
        // grain is not consulted, so the result reports Zero.
        var (applier, _, _, hwm) = CreateApplier();

        var result = await applier.ApplyAsync(RangeDeleteEntry("a", "z"));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default);
    }

    [Test]
    public async Task ApplyAsync_skips_redundant_get_after_successful_advance()
    {
        // Steady-state apply path: after TryAdvanceAsync returns true the
        // new HWM equals entry.Timestamp under single-threaded grain
        // semantics, so the applier must not issue a second GetAsync.
        var (applier, _, _, hwm) = CreateApplier();
        var ts = Hlc(42);

        await applier.ApplyAsync(SetEntry("k", ts));

        // Exactly one GetAsync (the pre-apply HWM check) and exactly one
        // TryAdvanceAsync (the post-apply advance). No redundant read.
        await hwm.Received(1).GetAsync(Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_falls_back_to_get_when_advance_loses_race()
    {
        // Rare reentrant-race path: another applier raced ahead and
        // pushed the HWM past entry.Timestamp between our pre-apply
        // GetAsync and our TryAdvanceAsync. TryAdvanceAsync returns
        // false; the applier must fall back to a fresh GetAsync so the
        // returned HighWaterMark reflects the actual post-call state.
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero, Hlc(99));
        hwm.TryAdvanceAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(42)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(99)));
        });
        await hwm.Received(2).GetAsync(Arg.Any<CancellationToken>());
    }
}
