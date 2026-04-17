using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    private static BPlusLeafGrain CreateGrain(
        FakePersistentState<LeafNodeState>? state = null,
        string replicaId = "test-leaf")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", replicaId));
        state ??= new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);
    }

    // --- GetAsync ---

    [Test]
    public async Task Get_returns_null_for_missing_key()
    {
        var grain = CreateGrain();
        var result = await grain.GetAsync("nonexistent");
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Get_returns_value_after_set()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var result = await grain.GetAsync("k1");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Get_returns_null_after_delete()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        var result = await grain.GetAsync("k1");
        Assert.That(result, Is.Null);
    }

    // --- SetAsync ---

    [Test]
    public async Task Set_returns_null_split_when_under_capacity()
    {
        var grain = CreateGrain();
        var result = await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Set_overwrites_existing_key_with_LWW()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("old"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("new"));

        var result = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("new"));
    }

    [Test]
    public async Task Set_persists_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
    }

    [Test]
    public async Task Set_advances_HLC()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clockBefore = state.State.Clock;
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(state.State.Clock > clockBefore, Is.True);
    }

    [Test]
    public async Task Set_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, replicaId: "replica-1");

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var clock = state.State.Version.GetClock("leaf/replica-1");
        Assert.That(clock > default(HybridLogicalClock), Is.True);
    }

    [Test]
    public async Task Set_after_delete_resurrects_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("alive"));
        await grain.DeleteAsync("k1");

        Assert.That(await grain.GetAsync("k1"), Is.Null);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("resurrected"));
        var result = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("resurrected"));
    }

    // --- GetOrSetAsync ---

    [Test]
    public async Task GetOrSet_sets_value_when_key_missing()
    {
        var grain = CreateGrain();
        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(result.ExistingValue, Is.Null);

        var stored = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task GetOrSet_returns_existing_value_when_key_live()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("original"));

        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("ignored"));

        Assert.That(result.ExistingValue, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result.ExistingValue!), Is.EqualTo("original"));
        Assert.That(result.Split, Is.Null);

        // Value should not have changed.
        var stored = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("original"));
    }

    [Test]
    public async Task GetOrSet_sets_value_when_key_tombstoned()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("old"));
        await grain.DeleteAsync("k1");

        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("new"));

        Assert.That(result.ExistingValue, Is.Null);

        var stored = await grain.GetAsync("k1");
        Assert.That(Encoding.UTF8.GetString(stored!), Is.EqualTo("new"));
    }

    [Test]
    public async Task GetOrSet_does_not_persist_state_when_key_exists()
    {
        var fakeState = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(fakeState);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var writeCountBefore = fakeState.WriteCount;
        await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("ignored"));

        Assert.That(fakeState.WriteCount, Is.EqualTo(writeCountBefore));
    }

    [Test]
    public async Task GetOrSet_returns_null_split_when_under_capacity()
    {
        var grain = CreateGrain();
        var result = await grain.GetOrSetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(result.ExistingValue, Is.Null);
        Assert.That(result.Split, Is.Null);
    }

    // --- DeleteAsync ---

    [Test]
    public async Task Delete_returns_true_for_existing_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.That(await grain.DeleteAsync("k1"), Is.True);
    }

    [Test]
    public async Task Delete_returns_false_for_missing_key()
    {
        var grain = CreateGrain();
        Assert.That(await grain.DeleteAsync("nonexistent"), Is.False);
    }

    [Test]
    public async Task Delete_returns_false_for_already_tombstoned_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        Assert.That(await grain.DeleteAsync("k1"), Is.False);
    }

    [Test]
    public async Task Delete_advances_HLC()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var clockBefore = state.State.Clock;
        await grain.DeleteAsync("k1");

        Assert.That(state.State.Clock > clockBefore, Is.True);
    }

    [Test]
    public async Task Delete_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, replicaId: "replica-1");
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var versionBefore = state.State.Version.GetClock("leaf/replica-1");
        await grain.DeleteAsync("k1");

        Assert.That(state.State.Version.GetClock("leaf/replica-1") > versionBefore, Is.True);
    }

    [Test]
    public async Task Delete_creates_tombstone_in_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        Assert.That(state.State.Entries["k1"].IsTombstone, Is.True);
    }
}
