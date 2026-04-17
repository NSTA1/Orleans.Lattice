using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.BPlusTree.State;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- GetDeltaSinceAsync ---

    [Test]
    public async Task Delta_returns_all_entries_for_empty_version()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());

        Assert.That(delta.Entries.Count, Is.EqualTo(2));
        Assert.That(delta.IsEmpty, Is.False);
    }

    [Test]
    public async Task Delta_returns_empty_when_caller_is_up_to_date()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var currentVersion = state.State.Version.Clone();
        var delta = await grain.GetDeltaSinceAsync(currentVersion);

        Assert.That(delta.IsEmpty, Is.True);
    }

    [Test]
    public async Task Delta_returns_only_entries_newer_than_caller_version()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var snapshot = state.State.Version.Clone();

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var delta = await grain.GetDeltaSinceAsync(snapshot);

        Assert.That(delta.Entries, Has.Count.EqualTo(1));
        Assert.That(delta.Entries.ContainsKey("b"), Is.True);
    }

    [Test]
    public async Task Delta_includes_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var snapshot = state.State.Version.Clone();

        await grain.DeleteAsync("a");
        var delta = await grain.GetDeltaSinceAsync(snapshot);

        Assert.That(delta.Entries, Has.Count.EqualTo(1));
        Assert.That(delta.Entries["a"].IsTombstone, Is.True);
    }

    [Test]
    public async Task Delta_version_advances_monotonically()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var v1 = state.State.Version.Clone();

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var v2 = state.State.Version.Clone();

        Assert.That(v2.DominatesOrEquals(v1), Is.True);
        Assert.That(v1.DominatesOrEquals(v2), Is.False);
    }

    // --- GetDeltaSinceAsync includes SplitKey ---

    [Test]
    public async Task Delta_includes_split_key_after_split()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        // Simulate a completed split.
        state.State.SplitKey = "m";
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitComplete;

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.That(delta.SplitKey, Is.EqualTo("m"));
    }

    [Test]
    public async Task Delta_split_key_is_null_when_no_split()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.That(delta.SplitKey, Is.Null);
    }
}
