using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the optimised <see cref="ReplicationApplier.ApplyBatchAsync"/>
/// path that collapses per-entry per-origin high-water-mark grain RPCs
/// to a single <see cref="IReplicationHighWaterMarkGrain.GetAsync"/> +
/// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> per
/// distinct origin per batch.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyBatchAsync_returns_zero_for_empty_batch_without_touching_grains()
    {
        var (applier, _, apply, hwm) = CreateApplier();

        var result = await applier.ApplyBatchAsync(Array.Empty<ReplogEntry>());

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_throws_when_entries_null()
    {
        var (applier, _, _, _) = CreateApplier();

        Assert.That(
            async () => await applier.ApplyBatchAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task ApplyBatchAsync_single_entry_defers_to_per_entry_path()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var ts = Hlc(10);

        var result = await applier.ApplyBatchAsync(new[] { SetEntry("k", ts) });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), ts, RemoteCluster, null, 0);
        // Single-entry path uses one GetAsync + one TryAdvanceAsync, identical to legacy ApplyAsync.
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, ts, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_multi_entry_single_origin_collapses_to_one_get_and_one_advance()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var entries = new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
            SetEntry("c", Hlc(30)),
            SetEntry("d", Hlc(40)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(40)));
        });

        // Every entry routed through the apply grain for its leaf write.
        await apply.Received(4).ApplySetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>());

        // The batch collapse claim: exactly one GetAsync and one
        // TryAdvanceAsync per distinct origin in the batch.
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(40), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_multi_origin_runs_collapse_per_run()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var entries = new[]
        {
            // Run 1: origin "site-b" (RemoteCluster) - 2 entries
            SetEntry("a", Hlc(10), RemoteCluster),
            SetEntry("b", Hlc(20), RemoteCluster),
            // Run 2: origin "site-c" - 2 entries
            SetEntry("c", Hlc(30), "site-c"),
            SetEntry("d", Hlc(40), "site-c"),
            // Run 3: back to RemoteCluster - 1 entry (separate run because not contiguous)
            SetEntry("e", Hlc(50), RemoteCluster),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)));
        });

        // 5 entries dispatched to the leaf.
        await apply.Received(5).ApplySetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>());

        // Per-run HWM round-trip: 2 calls for RemoteCluster (runs 1 + 3),
        // 1 call for site-c (run 2). NOT 5.
        await hwm.Received(2).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).GetAsync("site-c", Arg.Any<CancellationToken>());

        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(20), Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync("site-c", Hlc(40), Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(50), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_in_batch_dedups_entries_below_running_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        // Persisted HWM is at 25; the batch contains some entries below
        // that and some above. The first below-HWM entry is deduped via
        // the persisted HWM; subsequent below-runningHwm entries are
        // deduped without a fresh GetAsync.
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(25));

        var entries = new[]
        {
            SetEntry("a", Hlc(10)), // dedup (<= HWM)
            SetEntry("b", Hlc(20)), // dedup (<= HWM)
            SetEntry("c", Hlc(30)), // apply
            SetEntry("d", Hlc(40)), // apply
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(40)));
        });

        // Only the two apply-eligible entries hit the leaf.
        await apply.Received(2).ApplySetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>());

        // The collapse claim survives in-batch dedup: still one
        // GetAsync + one TryAdvanceAsync for the whole run.
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(40), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_fully_deduped_run_does_not_advance_hwm()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        var entries = new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
            SetEntry("c", Hlc(30)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        // No advance because no entry was applied.
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_range_delete_in_run_bypasses_hwm_dedup()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        var entries = new[]
        {
            // Below HWM - would normally dedup, but range-delete bypasses.
            RangeDeleteEntry("a", "m"),
            SetEntry("a", Hlc(10)), // dedup
            SetEntry("z", Hlc(200)), // apply
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(200)));
        });

        // Range-delete always applied unconditionally.
        await apply.Received(1).ApplyDeleteRangeAsync("a", "m", RemoteCluster, null);
        // One leaf set (the apply-eligible "z").
        await apply.Received(1).ApplySetAsync(
            "z", Arg.Any<byte[]>(), Hlc(200), RemoteCluster, Arg.Any<VersionVector?>(), Arg.Any<long>());
        // Single TryAdvance to highest applied point timestamp (not Zero from range-delete).
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(200), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_local_origin_run_classifies_all_as_dedup_without_grain_calls()
    {
        // A run authored by the local cluster id is rejected by the
        // local-origin defence: every entry classifies as Dedup with
        // HighWaterMark=Zero, no grain calls fire for the run.
        var (applier, _, apply, hwm) = CreateApplier();

        var entries = new[]
        {
            SetEntry("a", Hlc(10), LocalCluster),
            SetEntry("b", Hlc(20), LocalCluster),
            SetEntry("c", Hlc(30), LocalCluster),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_propagates_cancellation_before_first_run()
    {
        var (applier, _, _, _) = CreateApplier();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await applier.ApplyBatchAsync(new[] { SetEntry("a", Hlc(10)) }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
