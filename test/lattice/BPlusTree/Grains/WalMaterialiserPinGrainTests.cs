using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalMaterialiserPinGrain"/>: the durable per-tree
/// leaf-materialiser pin store the WAL GC consults so its trim floor survives a
/// full silo/cluster restart (issue #919). Exercises the monotonic-max merge,
/// snapshot read, and removal/clear contracts directly against a
/// <see cref="FakePersistentState{T}"/> with no storage provider.
/// </summary>
[TestFixture]
public sealed class WalMaterialiserPinGrainTests
{
    private const string Consumer = "_lattice_materialiser_tree-1_leaf-7";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static (WalMaterialiserPinGrain grain, FakePersistentState<WalMaterialiserPinState> state) CreateGrain(
        FakePersistentState<WalMaterialiserPinState>? existing = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", "tree-1"));
        var state = existing ?? new FakePersistentState<WalMaterialiserPinState>();
        return (new WalMaterialiserPinGrain(context, state), state);
    }

    [Test]
    public async Task ReportAsync_new_consumer_persists_pin()
    {
        var (grain, state) = CreateGrain();

        await grain.ReportAsync(Consumer, Hlc(100));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public async Task ReportAsync_higher_frontier_advances_pin_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.ReportAsync(Consumer, Hlc(200));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(2));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task ReportAsync_lower_frontier_is_coalesced_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(200));

        await grain.ReportAsync(Consumer, Hlc(100));

        Assert.Multiple(() =>
        {
            // The lower report is dropped: no second write, pin unchanged.
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(200)));
        });
    }

    [Test]
    public async Task ReportAsync_equal_frontier_is_coalesced_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(200));

        await grain.ReportAsync(Consumer, Hlc(200));

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task ReportAsync_zero_frontier_seeds_block_pin()
    {
        var (grain, state) = CreateGrain();

        await grain.ReportAsync(Consumer, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.True);
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task ReportAsync_zero_after_real_frontier_does_not_roll_back()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.ReportAsync(Consumer, HybridLogicalClock.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(state.State.Pins[Consumer], Is.EqualTo(Hlc(100)));
        });
    }

    [Test]
    public async Task GetPinsAsync_returns_independent_snapshot_copy()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        var snapshot = await grain.GetPinsAsync();
        // Mutating the durable state after the snapshot must not be observed
        // through the already-returned copy.
        state.State.Pins["other"] = Hlc(999);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Count, Is.EqualTo(1));
            Assert.That(snapshot[Consumer], Is.EqualTo(Hlc(100)));
            Assert.That(snapshot.ContainsKey("other"), Is.False);
        });
    }

    [Test]
    public async Task RemoveAsync_removes_pin_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.RemoveAsync(Consumer);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins.ContainsKey(Consumer), Is.False);
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task RemoveAsync_unknown_consumer_is_noop_without_write()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));

        await grain.RemoveAsync("_lattice_materialiser_tree-1_leaf-absent");

        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task ClearAsync_clears_all_pins_and_persists()
    {
        var (grain, state) = CreateGrain();
        await grain.ReportAsync(Consumer, Hlc(100));
        await grain.ReportAsync("_lattice_materialiser_tree-1_leaf-8", Hlc(50));

        await grain.ClearAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Pins, Is.Empty);
            // Two reports + one clear.
            Assert.That(state.WriteCount, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task ClearAsync_empty_store_is_noop_without_write()
    {
        var (grain, state) = CreateGrain();

        await grain.ClearAsync();

        Assert.That(state.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public void ReportAsync_null_consumer_throws()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            async () => await grain.ReportAsync(null!, Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RemoveAsync_whitespace_consumer_throws()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            async () => await grain.RemoveAsync("   "),
            Throws.InstanceOf<ArgumentException>());
    }
}
