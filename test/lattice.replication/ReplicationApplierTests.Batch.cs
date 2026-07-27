using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
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

        var result = await applier.ApplyBatchAsync(Array.Empty<WalRecord>());

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await hwm.DidNotReceiveWithAnyArgs().GetAsync(default!, default);
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(default!, default!, default, default!, default, default);
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
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

        // The 4 LWW entries collapse to a single batched apply call -
        // one ApplyMergeManyAsync per (treeId, origin) run instead of
        // four ApplySetAsync per-entry RPCs.
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 4
                && items[0].Key == "a" && !items[0].IsTombstone
                && items[1].Key == "b"
                && items[2].Key == "c"
                && items[3].Key == "d"));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);

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

        // 3 (treeId, origin) runs → 3 ApplyMergeManyAsync calls,
        // one per run. Run 1 + Run 2 carry 2 items each; Run 3
        // carries 1 item (still flushed via ApplyMergeManyAsync
        // because the run is processed by the batched path even
        // when the run length is 1; the single-batch-entry early
        // return at the top of ApplyBatchAsync only fires for an
        // inbound batch of length 1).
        await apply.Received(3).ApplyMergeManyAsync(
            Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        await apply.Received(2).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2));
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 1 && items[0].Key == "e"));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);

        // Per-run HWM round-trip: 2 calls for RemoteCluster (runs 1 + 3),
        // 1 call for site-c (run 2). NOT 5.
        await hwm.Received(2).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        await hwm.Received(1).GetAsync("site-c", Arg.Any<CancellationToken>());

        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(20), Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync("site-c", Hlc(40), Arg.Any<CancellationToken>());
        await hwm.Received(1).TryAdvanceAsync(RemoteCluster, Hlc(50), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_dedups_entries_below_pinned_floor()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        // A snapshot floor is pinned at 25; the batch contains some
        // entries at or below that (contained in the pinned snapshot ->
        // dedup) and some above (genuinely new -> apply). The whole run
        // reads the floor once.
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(25));
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(25));

        var entries = new[]
        {
            SetEntry("a", Hlc(10)), // dedup (<= floor)
            SetEntry("b", Hlc(20)), // dedup (<= floor)
            SetEntry("c", Hlc(30)), // apply
            SetEntry("d", Hlc(40)), // apply
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(40)));
        });

        // Only the two apply-eligible entries hit the batched apply call;
        // the deduped entries never reach pendingItems.
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 2
                && items[0].Key == "c"
                && items[1].Key == "d"));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);

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
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

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
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        await hwm.Received(1).GetAsync(RemoteCluster, Arg.Any<CancellationToken>());
        // No advance because no entry was applied.
        await hwm.DidNotReceiveWithAnyArgs().TryAdvanceAsync(default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_range_delete_in_run_bypasses_hwm_dedup()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));
        hwm.GetPinnedFloorAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Hlc(100));

        var entries = new[]
        {
            // Below floor - would normally dedup, but range-delete bypasses.
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
        await apply.Received(1).ApplyDeleteRangeAsync("a", "m", HybridLogicalClock.Zero, RemoteCluster, null);
        // The single apply-eligible LWW entry "z" is flushed via the
        // batched path with a 1-item list.
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 1
                && items[0].Key == "z"
                && items[0].SourceHlc.Equals(Hlc(200))));
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);
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

    [Test]
    public void ApplyBatchAsync_records_failure_outcome_for_deferred_entries_when_loop_throws()
    {
        // A Set with null Value violates the per-entry contract and the
        // batched path raises ArgumentException for it. Any prior entries
        // in the same run that had been deferred to pendingItems must
        // also surface as OutcomeFailure samples in the apply duration
        // histogram so the metric does not show phantom started-never-
        // completed samples.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(HybridLogicalClock.Zero);

        var entries = new[]
        {
            SetEntry("a", Hlc(10)),                  // deferred to pending
            SetEntry("b", Hlc(20)),                  // deferred to pending
            SetEntry("c", Hlc(30)) with { Value = null }, // throws inside loop
            SetEntry("d", Hlc(40)),                  // never reached
        };

        Assert.That(
            async () => await applier.ApplyBatchAsync(entries),
            Throws.InstanceOf<ArgumentException>());

        // Three samples: the two previously-deferred entries (recorded by
        // the new catch block), plus the throwing entry itself (recorded
        // by the finally because deferred is still false on it). The
        // never-reached entry contributes nothing.
        Assert.That(collector.Measurements, Has.Count.EqualTo(3));
        Assert.That(
            collector.Measurements.All(m => HasOutcome(m.Tags, LatticeReplicationMetrics.OutcomeFailure)),
            Is.True,
            "every recorded sample should be tagged outcome=failure");
        Assert.That(
            collector.Measurements.All(m => HasTree(m.Tags, Tree)),
            Is.True,
            "every recorded sample should carry the tree tag");
    }

    [Test]
    public async Task ApplyBatchAsync_routes_prepared_set_through_prepared_apply_seam_not_batched_merge()
    {
        // Cross-cluster atomic-saga visibility regression. Prepared
        // entries (IsPrepared=true) carry a TransactionId and must
        // park in the receiver leaf's per-tx pending bucket via
        // ApplyPreparedSetAsync. The batched LWW path collapses
        // ordinary Set/Delete entries to ApplyMergeManyAsync, which
        // routes through the shard-root's generic LWW merge primitive
        // WITHOUT honouring IsPrepared / TransactionId. Routing a
        // prepared record through that primitive applies it directly
        // into the visible projection, bypassing the per-tx pending
        // bucket - the cross-cluster atomic-visibility contract for
        // SetManyAtomicAsync collapses to ad-hoc per-key arrival
        // order and a continuous reader on the receiving site
        // observes a strict subset of the saga's keys mid-flight.
        // The classifier in ApplyOriginRunAsync must therefore
        // exclude IsPrepared=true entries from the batched path so
        // they fall through to ApplyPointAsync's IsPrepared branch.
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var entries = new[]
        {
            new WalRecord
            {
                TreeId = Tree,
                Op = MutationKind.Set,
                Key = "k0",
                Value = new byte[] { 1 },
                Timestamp = Hlc(10),
                OriginClusterId = RemoteCluster,
                IsPrepared = true,
                TransactionId = txid,
                AtomicBatchSize = 2,
                AtomicBatchIndex = 0,
            },
            new WalRecord
            {
                TreeId = Tree,
                Op = MutationKind.Set,
                Key = "k1",
                Value = new byte[] { 2 },
                Timestamp = Hlc(20),
                OriginClusterId = RemoteCluster,
                IsPrepared = true,
                TransactionId = txid,
                AtomicBatchSize = 2,
                AtomicBatchIndex = 1,
            },
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);

        // Prepared entries MUST flow through the prepared-apply seam,
        // one call per entry.
        await apply.Received(1).ApplyPreparedSetAsync(
            "k0", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, 0, txid, 2, 0);
        await apply.Received(1).ApplyPreparedSetAsync(
            "k1", Arg.Any<byte[]>(), Hlc(20), RemoteCluster, null, 0, txid, 2, 1);

        // The batched LWW merge seam MUST NOT be invoked: that would
        // route the prepared record into the visible projection,
        // breaking cross-cluster atomic visibility.
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        // And the non-prepared per-entry seam must also stay silent.
        await apply.DidNotReceiveWithAnyArgs().ApplySetAsync(
            default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_routes_prepared_delete_through_prepared_apply_seam_not_batched_merge()
    {
        // Mirror of the prepared-Set regression for prepared-Delete
        // tombstones. Same routing contract: the prepared-apply seam,
        // not the batched LWW merge primitive.
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var entries = new[]
        {
            new WalRecord
            {
                TreeId = Tree,
                Op = MutationKind.Delete,
                Key = "k0",
                Timestamp = Hlc(10),
                IsTombstone = true,
                OriginClusterId = RemoteCluster,
                IsPrepared = true,
                TransactionId = txid,
                AtomicBatchSize = 1,
                AtomicBatchIndex = 0,
            },
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyPreparedDeleteAsync(
            "k0", Hlc(10), RemoteCluster, null, txid, 1, 0);
        await apply.DidNotReceiveWithAnyArgs().ApplyMergeManyAsync(default!);
        await apply.DidNotReceiveWithAnyArgs().ApplyDeleteAsync(
            default!, default, default!, default);
    }

    [Test]
    public async Task ApplyBatchAsync_mixed_prepared_and_unprepared_entries_route_to_correct_seams()
    {
        // A run that interleaves an unprepared Set and a prepared Set
        // from the same origin must still split correctly: the
        // unprepared entry batches through ApplyMergeManyAsync, the
        // prepared entry falls out to ApplyPreparedSetAsync. The
        // classifier must respect the per-entry IsPrepared bit, not
        // the run-level mode.
        var (applier, _, apply, _) = CreateApplier();
        var txid = Guid.NewGuid();
        var entries = new[]
        {
            SetEntry("plain-a", Hlc(10)),
            new WalRecord
            {
                TreeId = Tree,
                Op = MutationKind.Set,
                Key = "prep-b",
                Value = new byte[] { 1 },
                Timestamp = Hlc(20),
                OriginClusterId = RemoteCluster,
                IsPrepared = true,
                TransactionId = txid,
                AtomicBatchSize = 1,
                AtomicBatchIndex = 0,
            },
            SetEntry("plain-c", Hlc(30)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);

        // The two unprepared entries land on the batched merge seam.
        await apply.Received().ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.All(i => i.Key == "plain-a" || i.Key == "plain-c")));

        // The prepared entry lands on the prepared-apply seam.
        await apply.Received(1).ApplyPreparedSetAsync(
            "prep-b", Arg.Any<byte[]>(), Hlc(20), RemoteCluster, null, 0, txid, 1, 0);

        // The prepared entry must NOT have been swept into the
        // batched merge - the items collection must not contain it.
        await apply.DidNotReceive().ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Any(i => i.Key == "prep-b")));
    }

    // ------------------------------------------------------------------
    // Parallel receiver apply across independent (tree) runs
    // ------------------------------------------------------------------

    private const string TreeX = "tree-x";
    private const string TreeY = "tree-y";

    private static WalRecord SetEntryFor(
        string treeId,
        string key,
        HybridLogicalClock ts,
        string origin = RemoteCluster) => new()
    {
        TreeId = treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    private static (
        ReplicationApplier Applier,
        IGrainFactory Factory,
        Dictionary<string, IReplicationApplyGrain> Applies,
        Dictionary<string, IReplicationHighWaterMarkGrain> Hwms)
        CreateMultiTreeApplier(IReadOnlyList<string> treeIds, int applyMaxParallelRuns = 1)
    {
        var factory = Substitute.For<IGrainFactory>();
        var applies = new Dictionary<string, IReplicationApplyGrain>(StringComparer.Ordinal);
        var hwms = new Dictionary<string, IReplicationHighWaterMarkGrain>(StringComparer.Ordinal);
        foreach (var treeId in treeIds)
        {
            var apply = Substitute.For<IReplicationApplyGrain>();
            var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
            factory.GetGrain<IReplicationApplyGrain>(treeId).Returns(apply);
            factory.GetGrain<IReplicationHighWaterMarkGrain>(treeId).Returns(hwm);
            hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
            hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
                .Returns(true);
            hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
            applies[treeId] = apply;
            hwms[treeId] = hwm;
        }

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ApplyMaxParallelRuns = applyMaxParallelRuns,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var applier = new ReplicationApplier(factory, monitor, replicationContext: new AnyTreeLwwContext());
        return (applier, factory, applies, hwms);
    }

    [Test]
    public async Task ApplyBatchAsync_multi_tree_with_dop_one_applies_sequentially_and_records_one_run()
    {
        // Default posture: ApplyMaxParallelRuns=1 keeps apply fully
        // sequential even across distinct trees. The effective DOP gauge
        // records 1 and every tree's run still applies correctly.
        using var collector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyParallelRunsName);
        var (applier, _, applies, hwms) = CreateMultiTreeApplier(
            new[] { TreeX, TreeY }, applyMaxParallelRuns: 1);

        var entries = new[]
        {
            SetEntryFor(TreeX, "a", Hlc(10)),
            SetEntryFor(TreeX, "b", Hlc(20)),
            SetEntryFor(TreeY, "c", Hlc(30)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(30)));
        });

        await applies[TreeX].Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2));
        await applies[TreeY].Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 1));
        await hwms[TreeX].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(20), Arg.Any<CancellationToken>());
        await hwms[TreeY].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(30), Arg.Any<CancellationToken>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(1));
    }

    [Test]
    public async Task ApplyBatchAsync_single_tree_with_dop_greater_than_one_stays_sequential_and_records_one_run()
    {
        // A single-tree batch can never engage cross-tree parallelism,
        // regardless of the configured DOP: independence is at the tree
        // granularity, so the effective DOP is clamped to 1 and the
        // sequential walk is taken.
        using var collector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyParallelRunsName);
        var (applier, _, applies, hwms) = CreateMultiTreeApplier(
            new[] { Tree }, applyMaxParallelRuns: 4);

        var entries = new[]
        {
            // Two origins, one tree - both runs share the per-tree causal
            // buffer / dedupe cache and so stay in one ordered group.
            SetEntry("a", Hlc(10), RemoteCluster),
            SetEntry("b", Hlc(20), "site-c"),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        // Both origins applied within the single tree group.
        await hwms[Tree].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(10), Arg.Any<CancellationToken>());
        await hwms[Tree].Received(1).TryAdvanceAsync("site-c", Hlc(20), Arg.Any<CancellationToken>());
        await applies[Tree].Received(2).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(1));
    }

    [Test]
    public async Task ApplyBatchAsync_independent_trees_apply_in_parallel_when_dop_greater_than_one()
    {
        // Proves the runs overlap: TreeX's flush blocks until TreeY's
        // flush has started. Under the sequential path TreeX would be
        // awaited to completion before TreeY ever began, so the gate
        // would never open and the test would time out. Under parallel
        // apply TreeY runs concurrently, opens the gate, and both
        // complete.
        using var collector = new MeterCollector<int>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyParallelRunsName);
        var (applier, _, applies, _) = CreateMultiTreeApplier(
            new[] { TreeX, TreeY }, applyMaxParallelRuns: 2);

        var treeYStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        applies[TreeY].ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>())
            .Returns(_ =>
            {
                treeYStarted.TrySetResult();
                return Task.CompletedTask;
            });
        applies[TreeX].ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>())
            .Returns(_ => treeYStarted.Task.WaitAsync(TimeSpan.FromSeconds(10)));

        var entries = new[]
        {
            SetEntryFor(TreeX, "a", Hlc(10)),
            SetEntryFor(TreeY, "b", Hlc(20)),
        };

        var result = await applier.ApplyBatchAsync(entries).WaitAsync(TimeSpan.FromSeconds(15));

        Assert.That(result.Applied, Is.True);
        Assert.That(treeYStarted.Task.IsCompletedSuccessfully, Is.True,
            "TreeY's run must have started while TreeX's run was still in flight");

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Value, Is.EqualTo(2),
            "two distinct trees with DOP>=2 yield an effective parallelism of 2");
    }

    [Test]
    public async Task ApplyBatchAsync_parallel_apply_advances_each_tree_hwm_to_its_own_max_independently()
    {
        // Per-origin high-water-mark monotonicity is preserved across the
        // parallel groups: each tree advances its own origin frontier to
        // that tree's highest applied HLC, independent of the other tree.
        var (applier, _, applies, hwms) = CreateMultiTreeApplier(
            new[] { TreeX, TreeY }, applyMaxParallelRuns: 2);

        var entries = new[]
        {
            SetEntryFor(TreeX, "a", Hlc(10)),
            SetEntryFor(TreeX, "b", Hlc(40)),
            SetEntryFor(TreeY, "c", Hlc(20)),
            SetEntryFor(TreeY, "d", Hlc(30)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(40)));
        });

        await hwms[TreeX].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(40), Arg.Any<CancellationToken>());
        await hwms[TreeY].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(30), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyBatchAsync_parallel_apply_keeps_each_run_atomic_collapsing_multi_entry_run_to_one_merge()
    {
        // Atomic-batch / run-boundary respect: parallelism is introduced
        // only across runs, never within one. A multi-entry run still
        // collapses to a single batched merge even when another tree's
        // run applies concurrently.
        var (applier, _, applies, _) = CreateMultiTreeApplier(
            new[] { TreeX, TreeY }, applyMaxParallelRuns: 2);

        var entries = new[]
        {
            SetEntryFor(TreeX, "a", Hlc(10)),
            SetEntryFor(TreeX, "b", Hlc(20)),
            SetEntryFor(TreeX, "c", Hlc(30)),
            SetEntryFor(TreeY, "d", Hlc(40)),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        // TreeX's three-entry run is applied as a single unit.
        await applies[TreeX].Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 3
                && items[0].Key == "a"
                && items[1].Key == "b"
                && items[2].Key == "c"));
        await applies[TreeY].Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 1));
    }

    [Test]
    public async Task ApplyBatchAsync_parallel_apply_keeps_same_tree_origins_sequential_within_the_group()
    {
        // Same-tree origins share the per-tree causal buffer / dedupe
        // cache, so they remain in one ordered group and apply
        // sequentially even under DOP>1: each origin's HWM advances to
        // its own max while the distinct second tree applies in parallel.
        var (applier, _, applies, hwms) = CreateMultiTreeApplier(
            new[] { TreeX, TreeY }, applyMaxParallelRuns: 3);

        var entries = new[]
        {
            // TreeX, origin RemoteCluster
            SetEntryFor(TreeX, "a", Hlc(10), RemoteCluster),
            // TreeX, origin site-c (distinct origin, same tree -> same group)
            SetEntryFor(TreeX, "b", Hlc(20), "site-c"),
            // TreeY, origin RemoteCluster
            SetEntryFor(TreeY, "c", Hlc(30), RemoteCluster),
        };

        var result = await applier.ApplyBatchAsync(entries);

        Assert.That(result.Applied, Is.True);
        await hwms[TreeX].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(10), Arg.Any<CancellationToken>());
        await hwms[TreeX].Received(1).TryAdvanceAsync("site-c", Hlc(20), Arg.Any<CancellationToken>());
        await hwms[TreeY].Received(1).TryAdvanceAsync(RemoteCluster, Hlc(30), Arg.Any<CancellationToken>());
        await applies[TreeX].Received(2).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        await applies[TreeY].Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
    }
}
