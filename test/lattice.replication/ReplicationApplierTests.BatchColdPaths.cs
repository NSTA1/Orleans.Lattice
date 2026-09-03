using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the inbound batch-apply path's cold arms: the run-level fallbacks,
/// the deferred-batch rollbacks, and the in-batch dedupe and causal-park gates.
/// <para>
/// These are the paths a healthy steady-state batch never takes, which is exactly why
/// they matter: each one exists to keep an at-most-once or causal-order guarantee
/// under a partial failure. A rollback that does not release its shadow-forward cache
/// reservation silently suppresses the transport's retry of that entry, so the write
/// is lost rather than retried - a correctness failure that is invisible on the happy
/// path.
/// </para>
/// </summary>
public partial class ReplicationApplierTests
{
    private const string SecondTree = "tree-2";

    /// <summary>
    /// Builds an applier over two enrolled trees with an optional receive gate and
    /// peer-stats recorder, so the multi-tree parallel plan and the inbound-contact
    /// recording can both be driven. The default single-tree factory registers grains
    /// for one tree only, which collapses every batch to a single run.
    /// </summary>
    private static (
        ReplicationApplier Applier,
        IReplicationApplyGrain Apply,
        IReplicationApplyGrain Apply2,
        IReplicationHighWaterMarkGrain Hwm)
        CreateTwoTreeApplier(
            int applyMaxParallelRuns = 1,
            IReplicationReceiveGate? receiveGate = null,
            ReplicationPeerStats? peerStats = null)
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var apply2 = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationApplyGrain>(SecondTree).Returns(apply2);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ApplyMaxParallelRuns = applyMaxParallelRuns,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var applier = new ReplicationApplier(
            factory,
            monitor,
            peerStats: peerStats,
            replicationContext: new AnyTreeLwwContext(),
            receiveGate: receiveGate);
        return (applier, apply, apply2, hwm);
    }

    private static WalRecord SecondTreeSetEntry(string key, HybridLogicalClock ts) =>
        SetEntry(key, ts) with { TreeId = SecondTree };

    // ---------------------------------------------------------------
    // Single-entry batch: failure still records inbound peer contact.
    // ---------------------------------------------------------------

    [Test]
    public void ApplyBatchAsync_single_entry_records_inbound_failure_before_rethrowing()
    {
        var stats = new ReplicationPeerStats();
        var (applier, apply, _, _) = CreateTwoTreeApplier(peerStats: stats);
        apply.ApplySetAsync(
            Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(), Arg.Any<VersionVector?>(), Arg.Any<long>())
            .Throws(new InvalidOperationException("the apply grain is unreachable"));

        Assert.That(
            async () => await applier.ApplyBatchAsync(new[] { SetEntry("k", Hlc(10)) }),
            Throws.InvalidOperationException,
            "A failed single-entry batch must surface the fault rather than swallow it.");
        Assert.That(stats.Snapshot(), Is.Not.Empty,
            "The failure is still attributed to the inbound peer, so peer health reflects it.");
    }

    [Test]
    public void ApplyBatchAsync_single_entry_failure_with_no_attributable_peer_records_nothing()
    {
        var stats = new ReplicationPeerStats();
        var (applier, _, _, _) = CreateTwoTreeApplier(peerStats: stats);

        // An empty origin fails validation inside ApplyAsync, so the failure path runs
        // with nothing to attribute the contact to - the recorder must skip it rather
        // than index a peer under the empty string.
        Assert.That(
            async () => await applier.ApplyBatchAsync(new[] { SetEntry("k", Hlc(10), origin: string.Empty) }),
            Throws.ArgumentException);
        Assert.That(stats.Snapshot(), Is.Empty);
    }

    // ---------------------------------------------------------------
    // Run-level fallback for an unusable (treeId, origin) key.
    // ---------------------------------------------------------------

    [Test]
    public void ApplyBatchAsync_multi_entry_run_with_an_empty_origin_falls_back_to_the_per_entry_path()
    {
        var (applier, _, _, hwm) = CreateTwoTreeApplier();
        var entries = new[]
        {
            SetEntry("a", Hlc(10), origin: string.Empty),
            SetEntry("b", Hlc(20), origin: string.Empty),
        };

        // The fallback exists so the validation message stays identical to the
        // per-entry path rather than degrading into a run-level failure.
        Assert.That(
            async () => await applier.ApplyBatchAsync(entries),
            Throws.ArgumentException.With.Message.Contains("OriginClusterId"));
        Assert.That(hwm.ReceivedCalls(), Is.Empty,
            "A run keyed on an unusable identity must not cost a high-water-mark round trip.");
    }

    [Test]
    public void ApplyBatchAsync_multi_entry_run_with_an_empty_tree_id_falls_back_to_the_per_entry_path()
    {
        var (applier, _, _, _) = CreateTwoTreeApplier();
        var entries = new[]
        {
            SetEntry("a", Hlc(10)) with { TreeId = string.Empty },
            SetEntry("b", Hlc(20)) with { TreeId = string.Empty },
        };

        Assert.That(
            async () => await applier.ApplyBatchAsync(entries),
            Throws.ArgumentException.With.Message.Contains("TreeId"));
    }

    // ---------------------------------------------------------------
    // Receive fence: whole runs defer, and the signal survives aggregation.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyBatchAsync_defers_every_run_while_the_receive_fence_is_engaged()
    {
        var gate = Substitute.For<IReplicationReceiveGate>();
        gate.IsReceivePausedAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(true);
        var (applier, apply, apply2, _) = CreateTwoTreeApplier(receiveGate: gate);

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SecondTreeSetEntry("b", Hlc(20)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.True,
                "A deferred run must surface a cursor-preserving ack so the sender re-ships it.");
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero),
                "Deferring must not advance the frontier past entries that were never merged.");
        });
        Assert.That(apply.ReceivedCalls(), Is.Empty);
        Assert.That(apply2.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task ApplyBatchAsync_propagates_the_deferred_signal_through_the_parallel_plan()
    {
        var gate = Substitute.For<IReplicationReceiveGate>();
        // Only the second tree is fenced, so the batch mixes an applied run with a
        // deferred one and the aggregation must keep both signals.
        gate.IsReceivePausedAsync(SecondTree, Arg.Any<CancellationToken>()).Returns(true);
        gate.IsReceivePausedAsync(Tree, Arg.Any<CancellationToken>()).Returns(false);
        var (applier, apply, apply2, _) = CreateTwoTreeApplier(applyMaxParallelRuns: 4, receiveGate: gate);

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("a2", Hlc(11)),
            SecondTreeSetEntry("b", Hlc(20)),
            SecondTreeSetEntry("b2", Hlc(21)),
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.True,
                "One fenced tree-group is enough to make the whole batch a re-ship.");
            Assert.That(result.Applied, Is.True,
                "The unfenced tree still applied, so the batch is not a whole-batch no-op.");
        });
        await apply.Received(1).ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
        Assert.That(apply2.ReceivedCalls(), Is.Empty);
    }

    // ---------------------------------------------------------------
    // In-batch shadow-forward dedupe.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyBatchAsync_suppresses_a_duplicate_emit_pair_inside_one_batch()
    {
        var (applier, apply, _, _) = CreateTwoTreeApplier();
        var duplicate = SetEntry("k", Hlc(10));

        // A structural rewrite (split / merge / saga compensate) shadow-forwards a user
        // write into another shard, so both emits ride the WAL with an identical
        // (origin, hlc, key, op) identity. Only one may reach the apply grain.
        var result = await applier.ApplyBatchAsync(new[] { duplicate, duplicate, SetEntry("other", Hlc(30)) });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 2 && items[0].Key == "k" && items[1].Key == "other"));
    }

    // ---------------------------------------------------------------
    // Causal-park gate inside the batch walk.
    // ---------------------------------------------------------------

    [Test]
    public async Task ApplyBatchAsync_parks_a_batched_entry_whose_causal_dependencies_are_unmet()
    {
        var (applier, apply, _, hwm) = CreateTwoTreeApplier();
        // The local frontier has seen nothing from site-c, so the declared dependency
        // is unmet and the entry must wait rather than apply out of causal order.
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)) with { VectorClock = Vector((OriginC, Hlc(500))) },
        });

        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 1 && items[0].Key == "a"));
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "The unparked entry in the same run still applied.");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(10)),
                "The frontier must stop at the last genuinely applied entry, not at the parked one.");
        });
    }

    [Test]
    public async Task ApplyBatchAsync_applies_a_batched_entry_whose_causal_dependencies_are_met()
    {
        var (applier, apply, _, hwm) = CreateTwoTreeApplier();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(Vector((OriginC, Hlc(500))));

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)) with { VectorClock = Vector((OriginC, Hlc(500))) },
        });

        Assert.That(result.Applied, Is.True);
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2));
    }

    [Test]
    public async Task ApplyBatchAsync_reads_the_local_vector_clock_once_per_run_until_an_apply_dirties_it()
    {
        var (applier, _, _, hwm) = CreateTwoTreeApplier();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(Vector((OriginC, Hlc(500))));

        await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)) with { VectorClock = Vector((OriginC, Hlc(100))) },
            SetEntry("b", Hlc(20)) with { VectorClock = Vector((OriginC, Hlc(200))) },
        });

        // Both entries declare dependencies but neither applies before the end-of-run
        // flush, so the cached frontier is still clean for the second check.
        await hwm.Received(1).GetVectorAsync(Arg.Any<CancellationToken>());
    }

    // ---------------------------------------------------------------
    // Deferred-batch rollback on a mid-run fault.
    // ---------------------------------------------------------------

    [Test]
    public async Task A_failed_crdt_flush_rolls_back_every_deferred_entry_so_the_transport_can_retry()
    {
        var (applier, _, apply, _) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        apply.ApplyCrdtDeltaManyAsync(Arg.Any<IReadOnlyList<ApplyCrdtDeltaItem>>())
            .Throws(new InvalidOperationException("the CRDT fold failed"));
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            OrSetEntry("b", Hlc(20), new byte[] { 2 }),
        };

        Assert.That(async () => await applier.ApplyBatchAsync(entries), Throws.InvalidOperationException);

        // The rollback is the point: without it the shadow-forward reservations survive
        // and silently suppress the retry, turning a transient fold failure into lost
        // writes. A second attempt must therefore reach the apply grain again.
        apply.ClearReceivedCalls();
        apply.ApplyCrdtDeltaManyAsync(Arg.Any<IReadOnlyList<ApplyCrdtDeltaItem>>()).Returns(Task.CompletedTask);
        var retry = await applier.ApplyBatchAsync(entries);

        Assert.That(retry.Applied, Is.True);
        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items => items.Count == 2));
    }

    [Test]
    public async Task A_fault_after_a_crdt_deferral_rolls_the_deferred_bucket_back()
    {
        var (applier, _, apply, hwm) = CreateTypedCrdtApplier(LatticeMergeMode.OrSet);
        // The vector-clock read happens before the per-entry classification, so faulting
        // it stops the run while an earlier CRDT deferral is still buffered - the exact
        // state the deferred-bucket rollback exists for.
        hwm.GetVectorAsync(Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("the high-water-mark grain is unreachable"));
        var entries = new[]
        {
            OrSetEntry("a", Hlc(10), new byte[] { 1 }),
            OrSetEntry("b", Hlc(20), new byte[] { 2 }),
            OrSetEntry("c", Hlc(30), new byte[] { 3 }) with { VectorClock = Vector((OriginC, Hlc(500))) },
        };

        Assert.That(async () => await applier.ApplyBatchAsync(entries), Throws.InvalidOperationException);

        apply.ClearReceivedCalls();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(Vector((OriginC, Hlc(500))));
        var retry = await applier.ApplyBatchAsync(entries);

        Assert.That(retry.Applied, Is.True);
        await apply.Received(1).ApplyCrdtDeltaManyAsync(
            Arg.Is<IReadOnlyList<ApplyCrdtDeltaItem>>(items => items.Count == 3));
    }
}
