using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- GetKeysAsync ---

    [Test]
    public async Task GetKeys_empty_leaf_returns_empty_list()
    {
        var grain = CreateGrain();
        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task GetKeys_returns_live_keys_in_sorted_order()
    {
        var grain = CreateGrain();
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task GetKeys_excludes_tombstoned_entries()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("b");

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task GetKeys_filters_by_startInclusive()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var keys = await grain.GetKeysAsync(startInclusive: "b");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeys_filters_by_endExclusive()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var keys = await grain.GetKeysAsync(endExclusive: "c");
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeys_filters_by_combined_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var keys = await grain.GetKeysAsync(startInclusive: "b", endExclusive: "d");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeys_excludes_keys_at_or_above_split_key_when_split_in_progress()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeys_does_not_filter_when_split_is_complete()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitComplete;
        state.State.SplitKey = "m";

        var keys = await grain.GetKeysAsync();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    // --- GetEntriesAsync ---

    [Test]
    public async Task GetEntries_empty_leaf_returns_empty_list()
    {
        var grain = CreateGrain();
        var entries = await grain.GetEntriesAsync();
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task GetEntries_returns_live_entries_in_sorted_order()
    {
        var grain = CreateGrain();
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(Encoding.UTF8.GetString(entries[0].Value), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString(entries[1].Value), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString(entries[2].Value), Is.EqualTo("3"));
    }

    [Test]
    public async Task GetEntries_excludes_tombstoned_entries()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("b");

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task GetEntries_filters_by_startInclusive()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var entries = await grain.GetEntriesAsync(startInclusive: "b");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetEntries_filters_by_endExclusive()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var entries = await grain.GetEntriesAsync(endExclusive: "c");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetEntries_filters_by_combined_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var entries = await grain.GetEntriesAsync(startInclusive: "b", endExclusive: "d");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetEntries_excludes_keys_at_or_above_split_key_when_split_in_progress()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetEntries_afterExclusive_skips_entries_at_or_below_token()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var entries = await grain.GetEntriesAsync(afterExclusive: "b");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "c", "d" }));
    }

    [Test]
    public async Task GetEntries_afterExclusive_combined_with_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var entries = await grain.GetEntriesAsync(startInclusive: "a", endExclusive: "e", afterExclusive: "b");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "c", "d" }));
    }

    [Test]
    public async Task GetEntries_afterExclusive_null_returns_all()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var entries = await grain.GetEntriesAsync(afterExclusive: null);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetEntries_afterExclusive_beyond_all_keys_returns_empty()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var entries = await grain.GetEntriesAsync(afterExclusive: "z");
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task GetEntries_afterExclusive_equal_to_key_skips_that_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var entries = await grain.GetEntriesAsync(afterExclusive: "a");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetEntries_values_reflect_latest_overwrite()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("old"));
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("new"));

        var entries = await grain.GetEntriesAsync();
        Assert.That(entries, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(entries[0].Value), Is.EqualTo("new"));
    }

    // --- GetManyAsync ---

    [Test]
    public async Task GetMany_returns_existing_values()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetTreeIdAsync("t");
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var result = await grain.GetManyAsync(["a", "b"]);

        Assert.That(result.Count, Is.EqualTo(2));
        Assert.That(Encoding.UTF8.GetString(result["a"]), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString(result["b"]), Is.EqualTo("2"));
    }

    [Test]
    public async Task GetMany_omits_missing_and_tombstoned_keys()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetTreeIdAsync("t");
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("b");

        var result = await grain.GetManyAsync(["a", "b", "missing"]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result.ContainsKey("a"), Is.True);
    }

    [Test]
    public async Task GetMany_returns_empty_for_empty_input()
    {
        var grain = CreateGrain();
        var result = await grain.GetManyAsync([]);
        Assert.That(result, Is.Empty);
    }

    // --- GetLiveEntriesAsync ---

    [Test]
    public async Task GetLiveEntries_returns_empty_for_empty_leaf()
    {
        var grain = CreateGrain();

        var result = await grain.GetLiveEntriesAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetLiveEntries_returns_only_live_entries()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("v2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("v3"));
        await grain.DeleteAsync("b");

        var result = await grain.GetLiveEntriesAsync();

        Assert.That(result, Has.Count.EqualTo(2));
        Assert.That(result.ContainsKey("a"), Is.True);
        Assert.That(result.ContainsKey("c"), Is.True);
        Assert.That(result.ContainsKey("b"), Is.False);
    }

    [Test]
    public async Task GetLiveEntries_returns_all_entries_when_no_tombstones()
    {
        var grain = CreateGrain();
        await grain.SetAsync("x", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("y", Encoding.UTF8.GetBytes("2"));

        var result = await grain.GetLiveEntriesAsync();

        Assert.That(result, Has.Count.EqualTo(2));
        Assert.That(Encoding.UTF8.GetString(result["x"]), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString(result["y"]), Is.EqualTo("2"));
    }

    // --- GetTreeIdAsync ---

    [Test]
    public async Task GetTreeId_returns_null_when_not_set()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetTreeIdAsync(), Is.Null);
    }

    [Test]
    public async Task GetTreeId_returns_tree_id_after_set()
    {
        var grain = CreateGrain();
        await grain.SetTreeIdAsync("my-tree");
        Assert.That(await grain.GetTreeIdAsync(), Is.EqualTo("my-tree"));
    }

    // --- ExistsAsync ---

    [Test]
    public async Task Exists_returns_false_for_missing_key()
    {
        var grain = CreateGrain();
        Assert.That(await grain.ExistsAsync("missing"), Is.False);
    }

    [Test]
    public async Task Exists_returns_true_for_live_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        Assert.That(await grain.ExistsAsync("k1"), Is.True);
    }

    [Test]
    public async Task Exists_returns_false_for_tombstoned_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");
        Assert.That(await grain.ExistsAsync("k1"), Is.False);
    }

    // --- CountAsync ---

    [Test]
    public async Task Count_returns_zero_for_empty_leaf()
    {
        var grain = CreateGrain();
        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task Count_returns_number_of_live_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public async Task Count_excludes_tombstoned_keys()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.DeleteAsync("b");

        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Count_returns_zero_when_all_keys_tombstoned()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.DeleteAsync("a");

        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(0));
    }
}
