using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Reliability gaps on the post-pin causal handoff path. These tests
/// pin behaviours that - independently of the four spec bullets - must
/// hold for the receiver to be safe to ship: drained-apply failure
/// classification, defence-in-depth against local-origin loops,
/// range-delete bypass of HWM and dep-check, cancellation propagation,
/// empty-frontier pin baselines, DLQ-throw containment, frontier-with-local
/// guard interaction, and own-origin exclusion in the dep-check.
/// </summary>
public partial class BootstrapCausalHandoffTests
{
    /// <summary>
    /// Reliability 1: when a parked entry's drained apply fails on the
    /// apply grain, the entry is routed through the DLQ with a
    /// classified reason tag (schema-shaped exceptions ->
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>; everything
    /// else -> <see cref="LatticeReplicationMetrics.ReasonUnknown"/>).
    /// The transport-level retry path is unavailable for drained
    /// entries because the original delivery was already ack'd.
    /// </summary>
    [Test]
    public async Task After_pin_drained_apply_failure_routes_blocked_entry_to_dlq_with_schema_reason()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Park an origin-A entry blocked on origin-B(500).
        var blocked = SetEntry("k-fail", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));
        await h.Applier.ApplyAsync(blocked);

        // Configure the apply grain so the drained re-apply throws an
        // ArgumentException - the schema-shaped fault classification
        // path. The predecessor (origin-B@500, no deps) applies
        // successfully so the drain pass actually fires for "k-fail".
        h.Apply.ApplySetAsync("k-fail", Arg.Any<byte[]>(), Hlc(150), OriginA, Arg.Any<VersionVector?>(), Arg.Any<long>())
            .Throws(new ArgumentException("synthetic schema fault"));

        var satisfier = SetEntry("k-fail-dep", Hlc(500), OriginB, Vector((OriginB, Hlc(500))));
        var satResult = await h.Applier.ApplyAsync(satisfier);

        Assert.That(satResult.Applied, Is.True, "Predecessor entry must apply.");
        Assert.Multiple(() =>
        {
            Assert.That(h.Parked, Has.Count.EqualTo(1),
                "Drained-apply failure must enqueue exactly one DLQ entry.");
            Assert.That(h.Parked[0].Entry.Key, Is.EqualTo("k-fail"),
                "DLQ entry must be the failed drained entry, not the predecessor.");
            Assert.That(h.Parked[0].ReasonTag, Is.EqualTo(LatticeReplicationMetrics.ReasonSchema),
                "ArgumentException must classify as ReasonSchema.");
        });
    }

    /// <summary>
    /// Reliability 1b: when a parked entry's drained apply fails and is
    /// dead-lettered, the shadow-forward dedupe cache reservation that
    /// was made when the entry was originally parked must be rolled back.
    /// Without rollback, an operator-driven retry from the DLQ would
    /// observe TryAdd=false (cache hit), classify as Applied=false
    /// (shadow-forward-dedup), and the dead-letter decorator's
    /// counter-clearing contract would silently drop the entry until
    /// FIFO eviction. The HWM was never advanced for the failing entry,
    /// so cache rollback is the only step required to admit the retry.
    /// </summary>
    [Test]
    public async Task After_pin_drained_apply_failure_rolls_back_cache_so_retry_is_admitted()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)), (OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        var blocked = SetEntry("k-fail", Hlc(150), OriginA, Vector((OriginA, Hlc(150)), (OriginB, Hlc(500))));
        await h.Applier.ApplyAsync(blocked);

        // First drain: apply throws, entry is dead-lettered, cache
        // reservation must be rolled back.
        var failCallCount = 0;
        h.Apply.WhenForAnyArgs(x => x.ApplySetAsync(default!, default!, default, default!, default, default))
            .Do(callInfo =>
            {
                if ((string)callInfo[0] == "k-fail" && Interlocked.Increment(ref failCallCount) == 1)
                {
                    throw new ArgumentException("synthetic schema fault");
                }
            });

        var satisfier = SetEntry("k-fail-dep", Hlc(500), OriginB, Vector((OriginB, Hlc(500))));
        await h.Applier.ApplyAsync(satisfier);

        Assert.That(h.Parked, Has.Count.EqualTo(1),
            "Failed drained entry must be enqueued on the DLQ.");

        // Operator-driven retry from the DLQ: re-deliver the same entry.
        // With cache rollback, the retry observes TryAdd=true and
        // proceeds through the apply pipeline. Since k-fail's deps are
        // now satisfied (origin-B@500 just applied), the retry applies
        // directly without parking.
        var retry = await h.Applier.ApplyAsync(blocked);

        Assert.Multiple(() =>
        {
            Assert.That(retry.Applied, Is.True,
                "DLQ retry must be admitted by cache rollback (the original drain failure cleared the reservation).");
            Assert.That(retry.HighWaterMark, Is.EqualTo(Hlc(150)),
                "Successful retry must advance the per-origin HWM.");
            Assert.That(failCallCount, Is.EqualTo(2),
                "Apply grain should have been called twice: once for the failing drain, once for the successful retry.");
        });
    }

    /// <summary>
    /// Reliability 2: an entry whose origin matches the local cluster
    /// id is rejected by the defence-in-depth guard before the HWM
    /// lookup ever fires. This holds even when the entry's HLC is
    /// above the pinned diagonal - the guard is unconditional on
    /// origin equality, not on causal ordering.
    /// </summary>
    [Test]
    public async Task After_pin_local_origin_entry_short_circuits_with_hwm_zero_regardless_of_frontier()
    {
        var h = CreateHarness();
        // Pin a frontier that includes the local cluster diagonal so we
        // can prove the guard fires before the HWM lookup (which would
        // otherwise dedup at HLC 100).
        var frontier = Vector((LocalCluster, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        var entry = SetEntry("k-local", Hlc(500), LocalCluster);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False, "Local-origin entry must never apply back onto its authoring cluster.");
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "Local-origin guard returns HWM=Zero (it never consults the HWM table).");
            Assert.That(h.Parked, Is.Empty);
        });
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    /// <summary>
    /// Reliability 3: range deletes carry <see cref="HybridLogicalClock.Zero"/>
    /// by design and bypass both per-origin HWM dedupe and the causal
    /// dep-check (range applies are naturally idempotent at the leaf).
    /// Pinning a frontier must not block a subsequent range-delete
    /// from reaching the apply grain.
    /// </summary>
    [Test]
    public async Task After_pin_range_delete_bypasses_hwm_and_dep_check_and_applies_directly()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        var entry = RangeDeleteEntry("k-from", "k-to", OriginA);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "Range delete must always apply (idempotent at the leaf).");
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "Range delete does not consult the HWM table; result.HighWaterMark is Zero.");
            Assert.That(h.Parked, Is.Empty);
        });
        await h.Apply.Received(1).ApplyDeleteRangeAsync("k-from", "k-to", OriginA, null);
    }

    /// <summary>
    /// Reliability 4: cancellation requested before the call propagates
    /// as <see cref="OperationCanceledException"/> without touching the
    /// apply grain or the DLQ.
    /// </summary>
    [Test]
    public void After_pin_cancellation_requested_before_call_propagates_without_side_effects()
    {
        var h = CreateHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var entry = SetEntry("k-cancel", Hlc(500), OriginA);

        Assert.That(
            async () => await h.Applier.ApplyAsync(entry, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(h.Parked, Is.Empty);
    }

    /// <summary>
    /// Reliability 5: pinning an empty frontier (no origins) is the
    /// degenerate case at the start of a fresh peer's lifecycle. Every
    /// subsequent incremental entry must apply via the normal path -
    /// no entry is dominated by the empty frontier on any origin.
    /// </summary>
    [Test]
    public async Task After_pin_empty_frontier_admits_every_above_zero_entry_on_normal_path()
    {
        var h = CreateHarness();
        await h.Hwm.PinSnapshotAsync(HybridLogicalClock.Zero, new VersionVector(), CancellationToken.None);

        var entry = SetEntry("k-empty", Hlc(50), OriginA);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "Above-zero entry must apply when the frontier is empty.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)), "HWM advances to the entry's HLC.");
            Assert.That(h.Parked, Is.Empty);
        });
    }

    /// <summary>
    /// Reliability 14: a DLQ failure during overflow eviction surfaces
    /// to the caller as the original exception. Buffer state is
    /// best-effort under this fault - the contract pinned here is that
    /// subsequent normal applies on the same applier still succeed
    /// (the applier is not poisoned by a single transient DLQ outage).
    /// </summary>
    [Test]
    public async Task After_pin_dlq_failure_during_overflow_propagates_and_does_not_poison_the_applier()
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            CausalBufferMaxEntries = 1,
        };
        var h = CreateHarness(options);
        var frontier = Vector((OriginA, Hlc(100)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Configure the DLQ to throw on the next enqueue (the overflow
        // eviction). Subsequent enqueues are restored to the success
        // handler explicitly to model a transient outage.
        h.Dlq.EnqueueAsync(
                Arg.Any<WalRecord>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("synthetic DLQ outage"));

        var blocking1 = SetEntry("k-dlq-1", Hlc(10), OriginB, Vector((OriginA, Hlc(999, 1)), (OriginB, Hlc(10))));
        await h.Applier.ApplyAsync(blocking1);

        var blocking2 = SetEntry("k-dlq-2", Hlc(20), OriginB, Vector((OriginA, Hlc(999, 2)), (OriginB, Hlc(20))));

        Assert.That(
            async () => await h.Applier.ApplyAsync(blocking2),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("synthetic DLQ outage"));

        // After the outage clears, a non-overflowing apply path on the
        // same applier still works.
        h.Dlq.EnqueueAsync(
                Arg.Any<WalRecord>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult((long)1));

        var clean = SetEntry("k-dlq-clean", Hlc(150), OriginA);
        var cleanResult = await h.Applier.ApplyAsync(clean);
        Assert.That(cleanResult.Applied, Is.True, "Subsequent normal apply after DLQ outage must still succeed.");
    }

    /// <summary>
    /// Reliability 16: when the pinned frontier itself contains the
    /// local cluster id (a peer that authored prior writes before
    /// being snapshotted), the local-origin guard still fires for any
    /// incremental entry stamped with that local origin. The frontier
    /// pin does not weaken the loop-prevention contract.
    /// </summary>
    [Test]
    public async Task After_pin_frontier_with_local_cluster_does_not_weaken_local_origin_guard()
    {
        var h = CreateHarness();
        var frontier = Vector((LocalCluster, Hlc(100)), (OriginA, Hlc(50)));
        await h.Hwm.PinSnapshotAsync(Hlc(100), frontier, CancellationToken.None);

        // Above the local-cluster diagonal - would dedup-pass on HWM if
        // the guard were missing. The guard returns Applied=false /
        // HWM=Zero, distinguishable from HWM dedup which would return
        // HWM=100.
        var entry = SetEntry("k-loop", Hlc(500), LocalCluster);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "Local-origin guard returns HWM=Zero even when the pinned frontier carries a local-cluster diagonal.");
        });
        await h.Apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
    }

    /// <summary>
    /// Reliability 17: the dep-check excludes the entry's own origin
    /// from the dependency frontier (the per-origin HWM table is the
    /// authoritative dedup key for the diagonal; including the
    /// diagonal here would deadlock it). An entry whose VC carries a
    /// dep on the local frontier - but whose own origin is not in
    /// the local VC at all - applies cleanly when the cross-origin
    /// dep is satisfied.
    /// </summary>
    [Test]
    public async Task After_pin_entry_with_only_cross_origin_deps_applies_when_satisfied()
    {
        var h = CreateHarness();
        var frontier = Vector((OriginB, Hlc(200)));
        await h.Hwm.PinSnapshotAsync(Hlc(200), frontier, CancellationToken.None);

        // Origin-A entry whose VC carries only a dep on origin-B(200)
        // - origin-A itself is not in the local VC. Dep is satisfied
        // by the pinned frontier; the entry must apply.
        var entry = SetEntry("k-cross", Hlc(50), OriginA, Vector((OriginB, Hlc(200))));

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "Cross-origin-only dep satisfied by frontier must apply.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(50)));
            Assert.That(h.Parked, Is.Empty);
        });
        await h.Apply.Received(1).ApplySetAsync("k-cross", Arg.Any<byte[]>(), Hlc(50), OriginA, null, 0);
    }
}
