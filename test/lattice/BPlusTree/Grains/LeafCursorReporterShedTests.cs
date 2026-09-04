using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the caller-side shed gate in <see cref="LeafCursorReporter"/>
/// added for issue #2014.
/// <para>
/// A coalescible retention flush routed to a pin shard whose last durable write
/// demonstrated it is not keeping up is dropped rather than enqueued. Shedding
/// on the caller is what actually shortens the shard's non-reentrancy queue: a
/// grain-side refusal still has to reach the front of that queue before it can
/// refuse, so it relieves nothing.
/// </para>
/// <para>
/// The load-bearing safety property is that shedding is a liveness decision and
/// never a correctness one - a skipped report leaves the durable pin staler,
/// which only retains more WAL - so the one report that must never be shed is
/// the write-through birth block-pin seed, which is a correctness barrier.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterShedTests
{
    private const string Tree = "tree-2014";
    private const string Consumer = "_lattice_materialiser_tree-2014_leaf-1";

    [SetUp]
    public void ResetPressure() => WalMaterialiserPinPressure.ResetForTests();

    [TearDown]
    public void ClearPressure() => WalMaterialiserPinPressure.ResetForTests();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static string ShardKey() =>
        WalMaterialiserPinRouting.ShardKey(Tree, Consumer, WalMaterialiserPinRouting.ResolveShardCount(null));

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 2000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10);
        }
    }

    private static (LeafCursorReporter reporter, CountingPinGrain pin) Create()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));
        var pin = new CountingPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        return (new LeafCursorReporter(registry, factory), pin);
    }

    [Test]
    public async Task Shed_window_suppresses_a_coalescible_retention_flush()
    {
        var (reporter, pin) = Create();

        // Seed the Zero block pin. The next real frontier crosses zero, so the
        // reporter's own wall-clock debounce always lets it through - which
        // leaves the shed gate as the only thing that can stop it.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, HybridLogicalClock.Zero, -1);
        await WaitUntilAsync(() => pin.Reports.Count >= 1);
        var baseline = pin.Reports.Count;

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), durationMs: 5000);
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        await Task.Delay(250);

        Assert.That(pin.Reports.Count, Is.EqualTo(baseline),
            "a coalescible advance routed to a shard inside its shed window must not be enqueued behind the write that opened it");
    }

    [Test]
    public async Task Shed_window_never_suppresses_the_write_through_block_pin_seed()
    {
        var (reporter, pin) = Create();

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), durationMs: 5000);

        // The block-pin seed is the birth barrier: shedding it would let the WAL
        // GC trim past a leaf that has not yet checkpointed, which is exactly the
        // cold-restart LeafProjectionStaleException the barrier exists to stop.
        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree,
            new[] { new MaterialiserPinReport(Consumer, HybridLogicalClock.Zero, -1) },
            CancellationToken.None);

        Assert.That(pin.Seeds.Count, Is.GreaterThanOrEqualTo(1),
            "the write-through birth seed is a correctness barrier and is never subject to the shed gate");
    }

    [Test]
    public async Task Reports_resume_once_the_shed_window_lapses()
    {
        var (reporter, pin) = Create();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, HybridLogicalClock.Zero, -1);
        await WaitUntilAsync(() => pin.Reports.Count >= 1);
        var baseline = pin.Reports.Count;

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), durationMs: 1);
        await Task.Delay(50);

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        await WaitUntilAsync(() => pin.Reports.Count > baseline);

        Assert.That(pin.Reports.Count, Is.GreaterThan(baseline),
            "the shed window is a short self-tuning hold-off, not a latch: reporting must resume on its own");
    }

    [Test]
    public void A_shard_with_no_recorded_pressure_is_never_shed()
    {
        Assert.That(WalMaterialiserPinPressure.ShouldShed(ShardKey()), Is.False,
            "an unmeasured shard must behave exactly as every pre-#2014 build did");
    }

    /// <summary>
    /// Pin-grain fake that keeps the write-through seed path distinguishable
    /// from the coalescible report path, which the shared reporter fakes
    /// deliberately alias together.
    /// </summary>
    private sealed class CountingPinGrain : IWalMaterialiserPinGrain
    {
        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier)> Reports { get; } = new();

        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier)> Seeds { get; } = new();

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier)
        {
            Reports.Add((consumerId, frontier));
            return Task.CompletedTask;
        }

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                Reports.Add((reports[i].ConsumerId, reports[i].Frontier));
            }

            return Task.CompletedTask;
        }

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                Seeds.Add((reports[i].ConsumerId, reports[i].Frontier));
            }

            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId) => Task.CompletedTask;

        public Task ClearAsync() => Task.CompletedTask;
    }
}
