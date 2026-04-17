using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- SetTreeIdAsync ---

    [Test]
    public async Task SetTreeId_is_idempotent()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetTreeIdAsync("tree-1");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));

        await grain.SetTreeIdAsync("tree-2");
        Assert.That(state.State.TreeId, Is.EqualTo("tree-1"));
    }

    // --- SetManyAsync ---

    [Test]
    public async Task SetMany_writes_all_entries()
    {
        var grain = CreateGrain();
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("a", Encoding.UTF8.GetBytes("1")),
            new("b", Encoding.UTF8.GetBytes("2")),
            new("c", Encoding.UTF8.GetBytes("3")),
        };

        var result = await grain.SetManyAsync(entries);

        Assert.That(result, Is.Null); // no split under capacity
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("3"));
    }

    [Test]
    public async Task SetMany_returns_null_when_no_split()
    {
        var grain = CreateGrain();
        var result = await grain.SetManyAsync([new("k1", [1])]);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task SetMany_empty_list_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("existing", Encoding.UTF8.GetBytes("v"));

        var result = await grain.SetManyAsync([]);

        Assert.That(result, Is.Null);
        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
    }

    // --- DeleteRangeAsync ---

    [Test]
    public async Task DeleteRange_returns_zero_for_empty_leaf()
    {
        var grain = CreateGrain();
        var count = await grain.DeleteRangeAsync("a", "z");
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteRange_tombstones_keys_in_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var count = await grain.DeleteRangeAsync("b", "d");

        Assert.That(count, Is.EqualTo(2));
        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
        Assert.That(await grain.GetAsync("b"), Is.Null);
        Assert.That(await grain.GetAsync("c"), Is.Null);
        Assert.That(await grain.GetAsync("d"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_skips_already_tombstoned_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("b");

        var count = await grain.DeleteRangeAsync("a", "c");

        Assert.That(count, Is.EqualTo(1));
        Assert.That(await grain.GetAsync("a"), Is.Null);
    }

    [Test]
    public async Task DeleteRange_returns_zero_when_no_keys_match()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var count = await grain.DeleteRangeAsync("m", "z");

        Assert.That(count, Is.EqualTo(0));
        Assert.That(await grain.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_persists_state_exactly_once()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var writesBefore = state.WriteCount;
        await grain.DeleteRangeAsync("a", "c");

        Assert.That(state.WriteCount - writesBefore, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteRange_does_not_persist_when_nothing_deleted()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var writesBefore = state.WriteCount;
        await grain.DeleteRangeAsync("m", "z");

        Assert.That(state.WriteCount - writesBefore, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteRange_advances_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var versionBefore = state.State.Version.Clone();
        await grain.DeleteRangeAsync("a", "z");

        Assert.That(state.State.Version.DominatesOrEquals(versionBefore), Is.True);
        Assert.That(versionBefore.DominatesOrEquals(state.State.Version), Is.False);
    }

    [Test]
    public async Task DeleteRange_advances_clock()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var clockBefore = state.State.Clock;
        await grain.DeleteRangeAsync("a", "z");

        Assert.That(state.State.Clock, Is.GreaterThan(clockBefore));
    }

    [Test]
    public async Task DeleteRange_deletes_all_keys_when_range_covers_entire_leaf()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        var count = await grain.DeleteRangeAsync("a", "zz");

        Assert.That(count, Is.EqualTo(3));
        Assert.That(await grain.GetAsync("a"), Is.Null);
        Assert.That(await grain.GetAsync("m"), Is.Null);
        Assert.That(await grain.GetAsync("z"), Is.Null);
    }

    [Test]
    public async Task DeleteRange_single_key_boundary()
    {
        var grain = CreateGrain();
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("2"));

        var count = await grain.DeleteRangeAsync("b", "c");

        Assert.That(count, Is.EqualTo(1));
        Assert.That(await grain.GetAsync("b"), Is.Null);
        Assert.That(await grain.GetAsync("c"), Is.Not.Null);
    }
}
