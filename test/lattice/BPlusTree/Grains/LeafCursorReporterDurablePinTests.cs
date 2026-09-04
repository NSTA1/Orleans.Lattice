using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the durable-pin mirror in <see cref="LeafCursorReporter"/>
/// (issue #919): the reporter forwards in-memory reports to the
/// <see cref="IWalCursorRegistry"/> and additionally mirrors each leaf's
/// durable checkpoint frontier into the cluster-wide
/// <see cref="IWalMaterialiserPinGrain"/> fire-and-forget and coalesced, so no
/// synchronous durable write is added to the leaf's checkpoint hot path.
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterDurablePinTests
{
    /// <summary>
    /// Clears the process-wide durable-pin pressure state (issue #2014) between
    /// tests. Without it a slow write measured by one test opens a shed window
    /// that could silently drop a later test's coalescible report.
    /// </summary>
    [SetUp]
    public void ResetPinPressure() => WalMaterialiserPinPressure.ResetForTests();
    private const string Tree = "tree";
    private const string Consumer = "_lattice_materialiser_tree_leaf-1";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

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

    private static (LeafCursorReporter reporter, IWalCursorRegistry registry, RecordingPinGrain pin) Create()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));
        var pin = new RecordingPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        return (new LeafCursorReporter(registry, factory), registry, pin);
    }

    [Test]
    public async Task NoteDurableMaterialiserFrontier_writes_first_frontier_through_to_pin_grain()
    {
        var (reporter, _, pin) = Create();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);

        await WaitUntilAsync(() => pin.Reports.Count >= 1);
        Assert.That(pin.Reports.ToArray(), Does.Contain((Consumer, Hlc(100))));
    }

    [Test]
    public async Task NoteDurableMaterialiserFrontier_coalesces_lower_frontier_without_extra_write()
    {
        var (reporter, _, pin) = Create();

        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        await WaitUntilAsync(() => pin.Reports.Count >= 1);

        // A lower report within the debounce window must be dropped.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(50), 50);
        await Task.Delay(150);

        Assert.That(pin.Reports.Count, Is.EqualTo(1),
            "A stale/lower durable frontier must be coalesced (no extra durable write).");
    }

    [Test]
    public async Task NoteDurableMaterialiserFrontier_publishes_first_real_frontier_after_zero_seed()
    {
        var (reporter, _, pin) = Create();

        // Activation seeds a Zero block pin...
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, HybridLogicalClock.Zero, -1);
        await WaitUntilAsync(() => pin.Reports.Count >= 1);

        // ...and the first real checkpoint frontier must write through promptly
        // even inside the debounce window, leaving the Zero block behind.
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100);
        await WaitUntilAsync(() => pin.Reports.Count >= 2);

        var reports = pin.Reports.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(reports, Does.Contain((Consumer, HybridLogicalClock.Zero)));
            Assert.That(reports, Does.Contain((Consumer, Hlc(100))));
        });
    }

    [Test]
    public void NoteDurableMaterialiserFrontier_without_grain_factory_is_noop()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var reporter = new LeafCursorReporter(registry);

        Assert.That(
            () => reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100), 100),
            Throws.Nothing);
    }

    [Test]
    public async Task ReportAsync_forwards_to_in_memory_registry()
    {
        var (reporter, registry, _) = Create();

        await reporter.ReportAsync(Tree, Consumer, Hlc(100), CancellationToken.None);

        await registry.Received(1).ReportCursorAsync(Tree, Consumer, Hlc(100), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnregisterTreeAsync_clears_durable_pins_for_the_tree()
    {
        var (reporter, registry, pin) = Create();
        registry.SnapshotAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(
                new[] { new WalCursorSnapshot(Consumer, Hlc(100), 0) }));

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pin.ClearCount, Is.EqualTo(1),
                "Tree deletion must clear the durable pin store so stale pins do not retain the WAL forever.");
        });
        await registry.Received(1).UnregisterAsync(Tree, Consumer, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UnregisterAsync_removes_durable_pin_for_materialiser_consumer()
    {
        var (reporter, _, pin) = Create();

        await reporter.UnregisterAsync(Tree, Consumer, CancellationToken.None);

        Assert.That(pin.Removed, Does.Contain(Consumer));
    }

    [Test]
    public async Task UnregisterAsync_ignores_durable_pin_for_non_materialiser_consumer()
    {
        var (reporter, _, pin) = Create();

        await reporter.UnregisterAsync(Tree, "peer-A", CancellationToken.None);

        Assert.That(pin.Removed, Is.Empty,
            "A non-materialiser consumer must not touch the durable leaf-pin store.");
    }

    private sealed class RecordingPinGrain : IWalMaterialiserPinGrain
    {
        public ConcurrentBag<(string Consumer, HybridLogicalClock Frontier)> Reports { get; } = new();
        public List<string> Removed { get; } = new();
        public int ClearCount;

        private readonly object _gate = new();
        private readonly Dictionary<string, HybridLogicalClock> _pins = new(StringComparer.Ordinal);

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier)
        {
            Reports.Add((consumerId, frontier));
            lock (_gate)
            {
                _pins[consumerId] = frontier;
            }
            return Task.CompletedTask;
        }

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                Reports.Add((reports[i].ConsumerId, reports[i].Frontier));
                lock (_gate)
                {
                    _pins[reports[i].ConsumerId] = reports[i].Frontier;
                }
            }
            return Task.CompletedTask;
        }

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => ReportManyAsync(reports);

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync()
        {
            lock (_gate)
            {
                return Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                    new Dictionary<string, HybridLogicalClock>(_pins, StringComparer.Ordinal));
            }
        }

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId)
        {
            Removed.Add(consumerId);
            return Task.CompletedTask;
        }

        public Task ClearAsync()
        {
            Interlocked.Increment(ref ClearCount);
            return Task.CompletedTask;
        }
    }
}
