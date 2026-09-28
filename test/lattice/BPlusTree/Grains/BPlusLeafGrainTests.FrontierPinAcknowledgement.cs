using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3643, clarification A, driven through the REAL
/// <see cref="LeafCursorReporter"/>: its batched flush swallows a faulted pin
/// shard write, so a leaf must bank - and the <c>frontier_pin</c> barrier may
/// elide against - only a batch the reporter reports as acknowledged.
/// </summary>
/// <remarks>
/// Pre-#3643 the leaf banked after every flush that returned normally, which a
/// swallowed fault does. The #3599 coverage-lag trigger then stood down with
/// the durable pin still frozen low, and an elision built on the same signal
/// would have skipped the barrier after a faulted tail - the dormant
/// floor-holder WAL-growth class of #3761. These tests fault the pin shard for
/// real rather than stubbing the reporter's answer.
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>A real reporter whose pin shard's <c>ReportManyAsync</c> faults while <see cref="FaultsRemaining"/> is positive.</summary>
    private sealed class FaultablePinStore
    {
        public int FaultsRemaining;

        public readonly ConcurrentQueue<(bool Faulted, long[] Offsets)> Writes = new();

        public LeafCursorReporter Reporter { get; }

        public FaultablePinStore()
        {
            var pin = Substitute.For<IWalMaterialiserPinGrain>();
            pin.ReportManyAsync(Arg.Any<IReadOnlyList<MaterialiserPinReport>>()).Returns(call =>
            {
                var offsets = call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(0)
                    .Select(r => r.CheckpointOffset).ToArray();
                var faulted = Interlocked.Decrement(ref FaultsRemaining) >= 0;
                Writes.Enqueue((faulted, offsets));
                return faulted
                    ? Task.FromException(new InvalidOperationException("pin store transient fault"))
                    : Task.CompletedTask;
            });

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);

            var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            var options = new LatticeOptions { WalPartitions = 1 };
            monitor.CurrentValue.Returns(options);
            monitor.Get(Arg.Any<string>()).Returns(options);

            Reporter = new LeafCursorReporter(Substitute.For<IWalCursorRegistry>(), factory, monitor);
        }

        public void ClearWrites()
        {
            while (Writes.TryDequeue(out _))
            {
            }

            Interlocked.Exchange(ref FaultsRemaining, 0);
        }
    }

    /// <summary>
    /// A flush whose pin shard write faults is swallowed by the real reporter,
    /// and must bank nothing: the next coverage-lag tick still finds the durable
    /// pin behind the bankable offset and republishes it (the #3599 trigger).
    /// Pre-#3643 the swallowed flush banked 3 and the tick stood down.
    /// </summary>
    [Test]
    public async Task Faulted_pin_shard_write_banks_nothing_so_the_coverage_lag_tick_republishes()
    {
        WalMaterialiserPinPressure.ResetForTests();
        var store = new FaultablePinStore();
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0, reporterOverride: store.Reporter);
        await ActivateAsync(leaf.Grain);
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        await AsProjection(leaf.Grain).FlushCheckpointAsync();
        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L), "precondition: persisted 3.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L), "precondition: coverage 3.");
        });

        store.ClearWrites();
        store.FaultsRemaining = 1;
        await leaf.Grain.FlushDurableMaterialiserFrontierAsync();

        var faultedFlush = store.Writes.ToArray();
        store.ClearWrites();
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        var tick = store.Writes.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(faultedFlush, Has.Length.EqualTo(1), "control: the flush made one shard write.");
            Assert.That(faultedFlush[0].Faulted, Is.True, "control: that write faulted.");
            Assert.That(faultedFlush[0].Offsets, Is.EqualTo(new[] { 3L }), "control: it carried pin 3.");
            Assert.That(tick.Select(w => w.Offsets), Does.Contain(new[] { 3L }),
                "THE assertion: the faulted write must not be banked, so the coverage-lag tick republishes 3.");
        });
    }

    /// <summary>
    /// The control for the test above: an acknowledged flush IS banked, so the
    /// next coverage-lag tick has nothing to republish.
    /// </summary>
    [Test]
    public async Task Acknowledged_pin_shard_write_is_banked_so_the_coverage_lag_tick_stands_down()
    {
        WalMaterialiserPinPressure.ResetForTests();
        var store = new FaultablePinStore();
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0, reporterOverride: store.Reporter);
        await ActivateAsync(leaf.Grain);
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        await AsProjection(leaf.Grain).FlushCheckpointAsync();

        store.ClearWrites();
        await leaf.Grain.FlushDurableMaterialiserFrontierAsync();
        Assert.That(store.Writes.Select(w => w.Faulted), Is.EqualTo(new[] { false }),
            "control: the flush made one shard write, and it landed.");

        store.ClearWrites();
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.That(store.Writes, Is.Empty,
            "an acknowledged pin 3 is banked, so the tick finds nothing behind and makes no write.");
    }

    /// <summary>
    /// The teardown tail's shard write faults and the real reporter swallows
    /// it, so no acknowledgement is recorded and the <c>frontier_pin</c>
    /// barrier publishes - the healing publish a dormant leaf would otherwise
    /// lose. Pre-fix the swallowed fault read as acknowledgement and the
    /// barrier elided, leaving the pin frozen at its pre-teardown value.
    /// </summary>
    [Test]
    public async Task Faulted_tail_pin_shard_write_does_not_let_the_frontier_pin_barrier_elide()
    {
        WalMaterialiserPinPressure.ResetForTests();
        var store = new FaultablePinStore();
        var leaf = await CreateCleanlyDeactivatableLeafAsync(store.Reporter);
        store.ClearWrites();
        store.FaultsRemaining = 1;

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        var writes = store.Writes.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist committed the final advance.");
            Assert.That(writes.Select(w => w.Faulted), Is.EqualTo(new[] { true, false }),
                "THE assertion: the tail's write faulted, so the barrier must write again - and land.");
            Assert.That(writes[^1].Offsets, Is.EqualTo(new[] { 3L }), "the barrier published pin 3.");
            Assert.That(elisions, Is.Empty, "a publishing barrier must never be counted as elided.");
        });
    }

    /// <summary>
    /// The control for the test above through the same real reporter: the
    /// tail's write lands, so the barrier elides and makes no second write.
    /// </summary>
    [Test]
    public async Task Acknowledged_tail_pin_shard_write_lets_the_frontier_pin_barrier_elide()
    {
        WalMaterialiserPinPressure.ResetForTests();
        var store = new FaultablePinStore();
        var leaf = await CreateCleanlyDeactivatableLeafAsync(store.Reporter);
        store.ClearWrites();

        var elisions = new List<string?>();
        using (ListenForFrontierPinElisions(elisions))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(store.Writes.Select(w => w.Faulted), Is.EqualTo(new[] { false }),
                "the tail's write landed, so the barrier makes no second pin-store write.");
            Assert.That(elisions, Has.Count.EqualTo(1), "the elision is counted once.");
        });
    }
}
