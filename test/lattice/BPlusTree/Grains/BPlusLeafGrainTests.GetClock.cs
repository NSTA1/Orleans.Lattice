using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- GetClockAsync ---

    [Test]
    public async Task GetClock_returns_zero_on_fresh_leaf()
    {
        var grain = CreateGrain();
        var clock = await grain.GetClockAsync();
        Assert.That(clock, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task GetClock_returns_advanced_clock_after_set()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var before = await grain.GetClockAsync();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var after = await grain.GetClockAsync();

        Assert.That(after, Is.GreaterThan(before));
    }

    [Test]
    public async Task GetClock_returns_state_clock_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var fromMethod = await grain.GetClockAsync();
        Assert.That(fromMethod, Is.EqualTo(state.State.Clock));
    }

    [Test]
    public async Task GetClock_does_not_advance_clock()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var before = state.State.Clock;
        var writeCountBefore = state.WriteCount;

        await grain.GetClockAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.EqualTo(before));
            Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore));
        });
    }
}
