using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue #2012: the routine leaf-materialiser retention
/// flush must not use the write-through birth path on the durable pin store.
/// <para>
/// A pin write is <c>O(consumers routed to the shard)</c> because Orleans
/// rewrites the whole grain-state blob, so routing
/// <see cref="LeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/>
/// through <see cref="IWalMaterialiserPinGrain.SeedManyAsync"/> made every leaf
/// activation and deactivation rewrite all of its (thousands of) neighbours,
/// awaited and serialized through one non-reentrant grain, until the grain's
/// non-reentrancy queue outran the 30 s response timeout. The flush must
/// therefore use the coalesced <see cref="IWalMaterialiserPinGrain.ReportManyAsync"/>,
/// which merges immediately and lets the pin store debounce the durable write.
/// Only the birth block-pin seed keeps the write-through, where the durability
/// barrier is genuinely load-bearing.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterPinWriteAmplificationTests
{
    /// <summary>
    /// Clears the process-wide durable-pin pressure state (issue #2014) between
    /// tests. Without it a slow write measured by one test opens a shed window
    /// that could silently drop a later test's coalescible report.
    /// </summary>
    [SetUp]
    public void ResetPinPressure() => WalMaterialiserPinPressure.ResetForTests();
    private const string Tree = "tree-2012";
    private const string ConsumerA = "_lattice_materialiser_tree-2012_leaf-A";
    private const string ConsumerB = "_lattice_materialiser_tree-2012_leaf-B";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static (LeafCursorReporter reporter, CallKindRecordingPinGrain pin) Create()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var pin = new CallKindRecordingPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        return (new LeafCursorReporter(registry, factory), pin);
    }

    private static IReadOnlyList<MaterialiserPinReport> Reports(
        params (string Consumer, HybridLogicalClock Frontier)[] pins)
    {
        var list = new List<MaterialiserPinReport>(pins.Length);
        foreach (var (consumer, frontier) in pins)
        {
            list.Add(new MaterialiserPinReport(consumer, frontier, -1));
        }

        return list;
    }

    [Test]
    public async Task Frontier_flush_uses_the_coalesced_report_path_not_the_write_through_seed()
    {
        var (reporter, pin) = Create();

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.ReportManyCalls, Is.EqualTo(1),
                "The routine retention flush must go through the coalesced pin-store path.");
            Assert.That(pin.SeedManyCalls, Is.Zero,
                "The routine retention flush must not use the write-through birth path (issue #2012): " +
                "it rewrites every co-routed consumer's pin, awaited, on a non-reentrant grain.");
        });
    }

    [Test]
    public async Task Birth_block_seed_still_uses_the_write_through_seed_path()
    {
        var (reporter, pin) = Create();

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree, Reports((ConsumerA, HybridLogicalClock.Zero)), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.SeedManyCalls, Is.EqualTo(1),
                "A new leaf's block pin must still be durable before its data becomes reachable in the WAL.");
            Assert.That(pin.ReportManyCalls, Is.Zero);
        });
    }

    [Test]
    public async Task Frontier_flush_still_merges_every_partition_pin()
    {
        var (reporter, pin) = Create();

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree,
            Reports((ConsumerA, Hlc(100)), (ConsumerB, Hlc(200))),
            CancellationToken.None);

        // Switching entry point must not lose or reorder any pin: the merge
        // itself is still immediate and covers the whole batch.
        var merged = pin.Merged.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(merged, Does.Contain((ConsumerA, Hlc(100))));
            Assert.That(merged, Does.Contain((ConsumerB, Hlc(200))));
        });
    }

    [Test]
    public async Task Repeated_frontier_flushes_never_reach_the_write_through_path()
    {
        var (reporter, pin) = Create();

        // A leaf that cycles activation repeatedly (the observed production
        // shape: thousands of leaves under activation collection) must not
        // produce one awaited full-blob rewrite per cycle.
        for (var i = 1; i <= 25; i++)
        {
            await reporter.FlushDurableMaterialiserFrontierAsync(
                Tree, Reports((ConsumerA, Hlc(i * 10))), CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(pin.SeedManyCalls, Is.Zero);
            Assert.That(pin.ReportManyCalls, Is.EqualTo(25));
        });
    }

    /// <summary>
    /// Pin-store stub that records which entry point the reporter used, so the
    /// tests can assert the routing rather than only the resulting state.
    /// </summary>
    private sealed class CallKindRecordingPinGrain : IWalMaterialiserPinGrain
    {
        private int _reportManyCalls;
        private int _seedManyCalls;

        public int ReportManyCalls => Volatile.Read(ref _reportManyCalls);

        public int SeedManyCalls => Volatile.Read(ref _seedManyCalls);

        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier)> Merged { get; } = new();

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier)
        {
            Merged.Add((consumerId, frontier));
            return Task.CompletedTask;
        }

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Interlocked.Increment(ref _reportManyCalls);
            return Merge(reports);
        }

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Interlocked.Increment(ref _seedManyCalls);
            return Merge(reports);
        }

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId) => Task.CompletedTask;

        public Task ClearAsync() => Task.CompletedTask;

        private Task Merge(IReadOnlyList<MaterialiserPinReport> reports)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                Merged.Add((reports[i].ConsumerId, reports[i].Frontier));
            }

            return Task.CompletedTask;
        }
    }
}
