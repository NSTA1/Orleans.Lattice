using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class BPlusLeafGrainTests
{
    private static BPlusLeafGrain CreateGrain(
        FakePersistentState<LeafNodeState>? state = null,
        string replicaId = "test-leaf")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", replicaId));
        state ??= new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        return new BPlusLeafGrain(context, state, grainFactory);
    }

    // --- GetAsync ---

    [Fact]
    public async Task Get_returns_null_for_missing_key()
    {
        var grain = CreateGrain();
        var result = await grain.GetAsync("nonexistent");
        Assert.Null(result);
    }

    [Fact]
    public async Task Get_returns_value_after_set()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var result = await grain.GetAsync("k1");
        Assert.NotNull(result);
        Assert.Equal("v1", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_returns_null_after_delete()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        var result = await grain.GetAsync("k1");
        Assert.Null(result);
    }

    // --- SetAsync ---

    [Fact]
    public async Task Set_returns_null_split_when_under_capacity()
    {
        var grain = CreateGrain();
        var result = await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.Null(result);
    }

    [Fact]
    public async Task Set_overwrites_existing_key_with_LWW()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("old"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("new"));

        var result = await grain.GetAsync("k1");
        Assert.Equal("new", Encoding.UTF8.GetString(result!));
    }

    [Fact]
    public async Task Set_persists_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.True(state.State.Entries.ContainsKey("k1"));
    }

    [Fact]
    public async Task Set_advances_HLC()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clockBefore = state.State.Clock;
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.True(state.State.Clock > clockBefore);
    }

    [Fact]
    public async Task Set_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, replicaId: "replica-1");

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var clock = state.State.Version.GetClock("leaf/replica-1");
        Assert.True(clock > default(HybridLogicalClock));
    }

    [Fact]
    public async Task Set_after_delete_resurrects_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("alive"));
        await grain.DeleteAsync("k1");

        Assert.Null(await grain.GetAsync("k1"));

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("resurrected"));
        var result = await grain.GetAsync("k1");
        Assert.Equal("resurrected", Encoding.UTF8.GetString(result!));
    }

    // --- DeleteAsync ---

    [Fact]
    public async Task Delete_returns_true_for_existing_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.True(await grain.DeleteAsync("k1"));
    }

    [Fact]
    public async Task Delete_returns_false_for_missing_key()
    {
        var grain = CreateGrain();
        Assert.False(await grain.DeleteAsync("nonexistent"));
    }

    [Fact]
    public async Task Delete_returns_false_for_already_tombstoned_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        Assert.False(await grain.DeleteAsync("k1"));
    }

    [Fact]
    public async Task Delete_advances_HLC()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var clockBefore = state.State.Clock;
        await grain.DeleteAsync("k1");

        Assert.True(state.State.Clock > clockBefore);
    }

    [Fact]
    public async Task Delete_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, replicaId: "replica-1");
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versionBefore = state.State.Version.GetClock("leaf/replica-1");
        await grain.DeleteAsync("k1");

        Assert.True(state.State.Version.GetClock("leaf/replica-1") > versionBefore);
    }

    [Fact]
    public async Task Delete_creates_tombstone_in_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        Assert.True(state.State.Entries["k1"].IsTombstone);
    }

    // --- Sibling pointers ---

    [Fact]
    public async Task NextSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.Null(await grain.GetNextSiblingAsync());
    }

    [Fact]
    public async Task SetNextSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-1");
        await grain.SetNextSiblingAsync(siblingId);

        Assert.Equal(siblingId, await grain.GetNextSiblingAsync());
    }

    // --- GetDeltaSinceAsync ---

    [Fact]
    public async Task Delta_returns_all_entries_for_empty_version()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());

        Assert.Equal(2, delta.Entries.Count);
        Assert.False(delta.IsEmpty);
    }

    [Fact]
    public async Task Delta_returns_empty_when_caller_is_up_to_date()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var currentVersion = state.State.Version.Clone();
        var delta = await grain.GetDeltaSinceAsync(currentVersion);

        Assert.True(delta.IsEmpty);
    }

    [Fact]
    public async Task Delta_returns_only_entries_newer_than_caller_version()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var snapshot = state.State.Version.Clone();

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var delta = await grain.GetDeltaSinceAsync(snapshot);

        Assert.Single(delta.Entries);
        Assert.True(delta.Entries.ContainsKey("b"));
    }

    [Fact]
    public async Task Delta_includes_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var snapshot = state.State.Version.Clone();

        await grain.DeleteAsync("a");
        var delta = await grain.GetDeltaSinceAsync(snapshot);

        Assert.Single(delta.Entries);
        Assert.True(delta.Entries["a"].IsTombstone);
    }

    [Fact]
    public async Task Delta_version_advances_monotonically()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        var v1 = state.State.Version.Clone();

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        var v2 = state.State.Version.Clone();

        Assert.True(v2.DominatesOrEquals(v1));
        Assert.False(v1.DominatesOrEquals(v2));
    }

    // --- Multiple keys ---

    [Fact]
    public async Task Multiple_keys_are_independent()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        Assert.Equal("1", Encoding.UTF8.GetString((await grain.GetAsync("a"))!));
        Assert.Equal("2", Encoding.UTF8.GetString((await grain.GetAsync("b"))!));
        Assert.Equal("3", Encoding.UTF8.GetString((await grain.GetAsync("c"))!));
    }

    [Fact]
    public async Task Deleting_one_key_does_not_affect_others()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        await grain.DeleteAsync("a");

        Assert.Null(await grain.GetAsync("a"));
        Assert.Equal("2", Encoding.UTF8.GetString((await grain.GetAsync("b"))!));
    }
}
