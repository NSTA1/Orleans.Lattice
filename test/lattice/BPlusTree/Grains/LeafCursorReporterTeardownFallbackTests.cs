using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for issue #1464: the awaited durable retention-pin flush
/// (both barriers) routes through
/// <see cref="LeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/> ->
/// the per-shard <see cref="IWalMaterialiserPinGrain.SeedManyAsync"/>. During a
/// full-silo graceful shutdown the pin-store grain is itself deactivating and
/// the stopping silo refuses to create its activation, so that grain call is
/// rejected mid-teardown and (previously) swallowed - defeating the "fall off
/// the log" floor and reintroducing <c>LeafProjectionStaleException</c> on cold
/// restart. The reporter now falls back to writing the pins straight to the
/// identical durable grain-state slot, so the floor still advances to the final
/// frontier during teardown. A genuine (non-shutdown) transient fault must keep
/// the prior swallow-and-log behaviour (no direct-store write).
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterTeardownFallbackTests
{
    private const string Tree = "tree-1464";
    private const string ConsumerA = "_lattice_materialiser_tree-1464_leaf-A";
    private const string ConsumerB = "_lattice_materialiser_tree-1464_leaf-B";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static GrainId PinGrainId(string grainKey) =>
        GrainId.Create("wal-materialiser-pin", grainKey);

    private static (LeafCursorReporter reporter, FakePinGrainStorage storage) Create(
        IWalMaterialiserPinGrain pin)
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);
        var storage = new FakePinGrainStorage();
        var reporter = new LeafCursorReporter(
            registry,
            factory,
            options: null,
            logger: null,
            pinStorage: storage,
            pinGrainIdResolver: PinGrainId);
        return (reporter, storage);
    }

    private static IReadOnlyList<MaterialiserPinReport> Reports(params (string Consumer, HybridLogicalClock Frontier)[] pins)
    {
        var list = new List<MaterialiserPinReport>(pins.Length);
        foreach (var (consumer, frontier) in pins)
        {
            list.Add(new MaterialiserPinReport(consumer, frontier));
        }
        return list;
    }

    [Test]
    public async Task Flush_on_shutdown_rejection_falls_back_to_direct_store()
    {
        var (reporter, storage) = Create(new RejectingPinGrain());

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);

        Assert.That(storage.TryReadPin(Tree, ConsumerA, out var pinned), Is.True,
            "A pin-grain rejection during teardown must fall back to a direct durable-store write.");
        Assert.That(pinned, Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task Seed_on_shutdown_rejection_falls_back_to_direct_store()
    {
        var (reporter, storage) = Create(new RejectingPinGrain());

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree, Reports((ConsumerA, HybridLogicalClock.Zero)), CancellationToken.None);

        Assert.That(storage.TryReadPin(Tree, ConsumerA, out var pinned), Is.True,
            "A rejected birth seed during teardown must also fall back to the direct durable-store write.");
        Assert.That(pinned, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task Flush_on_non_shutdown_fault_does_not_direct_store()
    {
        var (reporter, storage) = Create(new TransientFaultPinGrain());

        // Must not throw (swallow-and-log preserved) and must NOT write to the
        // direct store: a genuine transient fault re-flushes on the next
        // checkpoint through the grain, and a spurious direct-store on every
        // transient hiccup would bypass the grain's coalescing/debounce.
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None);

        Assert.That(storage.WriteCount, Is.EqualTo(0),
            "A non-shutdown transient fault must be swallowed, not routed to the direct-store fallback.");
    }

    [Test]
    public async Task DirectStore_fallback_merges_monotonic_max()
    {
        var (reporter, storage) = Create(new RejectingPinGrain());

        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(200))), CancellationToken.None);
        // A lower later frontier must be coalesced (never rolls back).
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(50))), CancellationToken.None);
        // A higher frontier must advance.
        await reporter.FlushDurableMaterialiserFrontierAsync(
            Tree, Reports((ConsumerA, Hlc(300))), CancellationToken.None);

        Assert.That(storage.TryReadPin(Tree, ConsumerA, out var pinned), Is.True);
        Assert.That(pinned, Is.EqualTo(Hlc(300)),
            "The direct-store fallback must monotonic-max merge exactly like the pin grain.");
    }

    [Test]
    public async Task DirectStore_fallback_serializes_concurrent_writes_to_same_shard()
    {
        var (reporter, storage) = Create(new RejectingPinGrain());

        // Many deactivating leaves routing to the same shard take the fallback
        // concurrently; the per-shard lock must serialize the read-modify-write
        // so every pin survives and the max wins (no lost update).
        var flushes = new List<Task>();
        for (var i = 1; i <= 20; i++)
        {
            var frontier = Hlc(i * 10);
            flushes.Add(reporter.FlushDurableMaterialiserFrontierAsync(
                Tree, Reports((ConsumerA, frontier), (ConsumerB, frontier)), CancellationToken.None));
        }
        await Task.WhenAll(flushes);

        Assert.Multiple(() =>
        {
            Assert.That(storage.TryReadPin(Tree, ConsumerA, out var a), Is.True);
            Assert.That(a, Is.EqualTo(Hlc(200)));
            Assert.That(storage.TryReadPin(Tree, ConsumerB, out var b), Is.True);
            Assert.That(b, Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public void Flush_without_pin_storage_swallows_rejection()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(new RejectingPinGrain());
        // No pinStorage: a host without the "lattice" durable provider degrades
        // to the prior swallow-and-log behaviour (no throw).
        var reporter = new LeafCursorReporter(registry, factory);

        Assert.That(
            async () => await reporter.FlushDurableMaterialiserFrontierAsync(
                Tree, Reports((ConsumerA, Hlc(100))), CancellationToken.None),
            Throws.Nothing);
    }

    /// <summary>
    /// Pin grain that rejects <see cref="SeedManyAsync"/> with an exception
    /// carrying the canonical Orleans activation-collection rejection message,
    /// simulating a durable-pin grain call issued during full-silo shutdown.
    /// </summary>
    private sealed class RejectingPinGrain : PinGrainStub
    {
        public override Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) =>
            throw new InvalidOperationException(
                "Unable to create local activation for grain wal-materialiser-pin. Rejecting now.");
    }

    /// <summary>
    /// Pin grain that fails <see cref="SeedManyAsync"/> with a generic transient
    /// fault (not a shutdown rejection), which must be swallowed rather than
    /// routed to the direct-store fallback.
    /// </summary>
    private sealed class TransientFaultPinGrain : PinGrainStub
    {
        public override Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) =>
            throw new TimeoutException("Durable pin store timed out.");
    }

    private abstract class PinGrainStub : IWalMaterialiserPinGrain
    {
        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Task.CompletedTask;
        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Task.CompletedTask;
        public virtual Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports) => Task.CompletedTask;
        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));
        public Task RemoveAsync(string consumerId) => Task.CompletedTask;
        public Task ClearAsync() => Task.CompletedTask;
    }

    /// <summary>
    /// Minimal in-memory <see cref="IGrainStorage"/> keyed by
    /// <c>{stateName}/{grainId}</c>, standing in for the durable "lattice"
    /// provider the direct-store fallback writes through.
    /// </summary>
    private sealed class FakePinGrainStorage : IGrainStorage
    {
        private readonly ConcurrentDictionary<string, WalMaterialiserPinState> _store =
            new(StringComparer.Ordinal);

        public int WriteCount;

        public bool TryReadPin(string treeName, string consumerId, out HybridLogicalClock frontier)
        {
            var key = MakeKey(WalMaterialiserPinState.StateName, PinGrainId(treeName));
            if (_store.TryGetValue(key, out var state) &&
                state.Pins.TryGetValue(consumerId, out frontier))
            {
                return true;
            }
            frontier = HybridLogicalClock.Zero;
            return false;
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var key = MakeKey(stateName, grainId);
            if (_store.TryGetValue(key, out var state))
            {
                // Return a defensive copy so the reporter's in-place merge cannot
                // mutate the stored instance except through WriteStateAsync.
                grainState.State = (T)(object)Clone(state);
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }
            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Interlocked.Increment(ref WriteCount);
            _store[MakeKey(stateName, grainId)] = Clone((WalMaterialiserPinState)(object)grainState.State!);
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            _store.TryRemove(MakeKey(stateName, grainId), out _);
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }

        private static WalMaterialiserPinState Clone(WalMaterialiserPinState source) =>
            new() { Pins = new Dictionary<string, HybridLogicalClock>(source.Pins, StringComparer.Ordinal) };

        private static string MakeKey(string stateName, GrainId grainId) => $"{stateName}/{grainId}";
    }
}
