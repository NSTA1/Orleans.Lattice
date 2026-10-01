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

    /// <summary>
    /// Issue #3918, inverted from the assertion this replaced. It used to
    /// require the clip, on the stated premise that the right half "has already
    /// been wired onto the new sibling". The source contradicts that premise:
    /// <c>CompleteSplitAsync</c> drops each batch from the donor with
    /// <c>RemoveTransferredRows</c>, which is synchronous and immediately
    /// follows the await on the sibling's <c>MergeEntriesAsync</c>, so a row the
    /// donor still holds at or above <c>SplitKey</c> is one the sibling has NOT
    /// been confirmed to hold. The state this test builds - the donor holding
    /// "m" and "z" with the division still in flight - is therefore exactly the
    /// state in which the clip hid rows that lived on no leaf at all.
    /// <see cref="Orleans.Lattice.Tests.BPlusTree.LeafSplitRangeReadGapIntegrationTests"/>
    /// shows the consequence end to end against a real tree.
    /// </summary>
    [Test]
    public async Task GetKeys_still_reports_keys_at_or_above_split_key_while_a_division_is_in_flight()
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
        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "m", "z" }),
            "the donor still holds 'm' and 'z', so the sibling has not taken them; hiding them reports "
            + "rows that exist nowhere else as absent.");
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

    // --- GetKeysAsync afterExclusive ---

    [Test]
    public async Task GetKeys_afterExclusive_skips_keys_at_or_below_token()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var keys = await grain.GetKeysAsync(afterExclusive: "b");
        Assert.That(keys, Is.EqualTo(new[] { "c", "d" }));
    }

    [Test]
    public async Task GetKeys_afterExclusive_combined_with_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var keys = await grain.GetKeysAsync(startInclusive: "a", endExclusive: "e", afterExclusive: "b");
        Assert.That(keys, Is.EqualTo(new[] { "c", "d" }));
    }

    [Test]
    public async Task GetKeys_afterExclusive_null_returns_all()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var keys = await grain.GetKeysAsync(afterExclusive: null);
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeys_afterExclusive_beyond_all_keys_returns_empty()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var keys = await grain.GetKeysAsync(afterExclusive: "z");
        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task GetKeys_afterExclusive_equal_to_key_skips_that_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var keys = await grain.GetKeysAsync(afterExclusive: "a");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    // --- GetKeysAsync beforeExclusive ---

    [Test]
    public async Task GetKeys_beforeExclusive_skips_keys_at_or_above_token()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var keys = await grain.GetKeysAsync(beforeExclusive: "c");
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeys_beforeExclusive_combined_with_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var keys = await grain.GetKeysAsync(startInclusive: "b", endExclusive: "e", beforeExclusive: "d");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeys_beforeExclusive_null_returns_all()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var keys = await grain.GetKeysAsync(beforeExclusive: null);
        Assert.That(keys, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetKeys_beforeExclusive_below_all_keys_returns_empty()
    {
        var grain = CreateGrain();
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("2"));

        var keys = await grain.GetKeysAsync(beforeExclusive: "a");
        Assert.That(keys, Is.Empty);
    }

    // --- GetEntriesAsync beforeExclusive ---

    [Test]
    public async Task GetEntries_beforeExclusive_skips_entries_at_or_above_token()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        var entries = await grain.GetEntriesAsync(beforeExclusive: "c");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetEntries_beforeExclusive_combined_with_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var entries = await grain.GetEntriesAsync(startInclusive: "b", endExclusive: "e", beforeExclusive: "d");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetEntries_beforeExclusive_null_returns_all()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var entries = await grain.GetEntriesAsync(beforeExclusive: null);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task GetEntries_beforeExclusive_below_all_keys_returns_empty()
    {
        var grain = CreateGrain();
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("2"));

        var entries = await grain.GetEntriesAsync(beforeExclusive: "a");
        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task GetEntries_afterExclusive_and_beforeExclusive_combined()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var entries = await grain.GetEntriesAsync(afterExclusive: "a", beforeExclusive: "d");
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task GetKeys_afterExclusive_and_beforeExclusive_combined()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var keys = await grain.GetKeysAsync(afterExclusive: "a", beforeExclusive: "d");
        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
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

    /// <summary>
    /// Issue #3918. See
    /// <see cref="GetKeys_still_reports_keys_at_or_above_split_key_while_a_division_is_in_flight"/>
    /// for why the clip this replaced was wrong.
    /// </summary>
    [Test]
    public async Task GetEntries_still_reports_keys_at_or_above_split_key_while_a_division_is_in_flight()
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
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "b", "m", "z" }),
            "a range read must return every row the leaf still holds, or throw; it must never complete "
            + "short.");
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

    /// <summary>
    /// Issue #3918. See
    /// <see cref="GetKeys_still_reports_keys_at_or_above_split_key_while_a_division_is_in_flight"/>
    /// for why the clip this replaced was wrong. The old assertion was the
    /// unit-level twin of the production symptom: a count of 2 over a leaf
    /// holding 4 live rows, returned without an exception.
    /// </summary>
    [Test]
    public async Task Count_still_counts_keys_at_or_above_split_key_while_a_division_is_in_flight()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        // A donor stuck mid-division (a silo restart, or a WAL replay refused
        // under permit saturation). The right half is still in THIS leaf's
        // cache, which is precisely the evidence that the sibling never took
        // it: the transfer removes each batch from the donor in the same turn
        // the sibling acknowledges it.
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";

        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(4),
            "the leaf holds four live rows, so a count that completed without throwing must report four.");
    }

    [Test]
    public async Task Count_includes_all_keys_when_no_split_in_progress()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        // SplitKey set but SplitState not SplitInProgress: the boundary
        // must not be applied, so every live key is counted.
        state.State.SplitKey = "m";

        var count = await grain.CountAsync();
        Assert.That(count, Is.EqualTo(3));
    }

    // --- CountAsync(startInclusive, endExclusive) ---

    [Test]
    public async Task Count_range_null_bounds_equals_unbounded_count()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        Assert.That(await grain.CountAsync(null, null), Is.EqualTo(await grain.CountAsync()));
    }

    [Test]
    public async Task Count_range_is_inclusive_of_start_and_exclusive_of_end()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        // [b, d) -> { b, c }
        var count = await grain.CountAsync("b", "d");
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Count_range_startInclusive_only_counts_from_floor()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var count = await grain.CountAsync("b", null);
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Count_range_empty_when_start_equals_end()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var count = await grain.CountAsync("b", "b");
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task Count_range_excludes_reserved_floor_prefix()
    {
        // Mirrors the aggregation-view usage: reserved NUL-prefixed rows are
        // excluded by counting [\u0001, null) so accumulator shards never
        // inflate the group-value count.
        var grain = CreateGrain();
        await grain.SetAsync("\0acc-red", Encoding.UTF8.GetBytes("r"));
        await grain.SetAsync("\0acc-blue", Encoding.UTF8.GetBytes("b"));
        await grain.SetAsync("red", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("blue", Encoding.UTF8.GetBytes("2"));

        var count = await grain.CountAsync("\u0001", null);
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Count_range_excludes_tombstoned_keys_in_range()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await grain.DeleteAsync("b");

        var count = await grain.CountAsync("a", "d");
        Assert.That(count, Is.EqualTo(2));
    }

    /// <summary>
    /// Issue #3918: the ranged twin of
    /// <see cref="Count_still_counts_keys_at_or_above_split_key_while_a_division_is_in_flight"/>.
    /// The caller's own bounds are still honoured; only the implicit split-key
    /// bound is gone.
    /// </summary>
    [Test]
    public async Task Count_range_still_counts_keys_at_or_above_split_key_within_bounds()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";

        var count = await grain.CountAsync("a", "z");
        Assert.That(count, Is.EqualTo(3),
            "[a, z) excludes 'z' by the caller's own bound and admits 'm'; the in-flight division adds "
            + "no bound of its own.");
    }

    /// <summary>
    /// Issue #3918. See
    /// <see cref="GetKeys_still_reports_keys_at_or_above_split_key_while_a_division_is_in_flight"/>.
    /// Stats feed shard diagnostics and healing, so under-reporting a stuck
    /// donor's live rows hid the very leaves an operator would look for.
    /// </summary>
    [Test]
    public async Task Stats_still_count_keys_at_or_above_split_key_while_a_division_is_in_flight()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";

        var stats = await grain.GetStatsAsync();
        Assert.That(stats.LiveKeys, Is.EqualTo(4),
            "the leaf holds four live rows and the sibling has taken none of them.");
    }

    [Test]
    public async Task Stats_includes_all_keys_when_no_split_in_progress()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        var stats = await grain.GetStatsAsync();
        Assert.That(stats.LiveKeys, Is.EqualTo(3));
    }

    [Test]
    public async Task Stats_state_bytes_is_zero_for_empty_leaf()
    {
        var grain = CreateGrain();

        var stats = await grain.GetStatsAsync();

        Assert.That(stats.StateBytes, Is.EqualTo(0));
    }

    [Test]
    public async Task Stats_state_bytes_sums_utf8_key_and_value_lengths()
    {
        var grain = CreateGrain();
        // "ab" (2) + value [1,2,3] (3) = 5; "c" (1) + value [9] (1) = 2. Total 7.
        await grain.SetAsync("ab", new byte[] { 1, 2, 3 });
        await grain.SetAsync("c", new byte[] { 9 });

        var stats = await grain.GetStatsAsync();

        Assert.That(stats.StateBytes, Is.EqualTo(7));
    }

    [Test]
    public async Task Stats_state_bytes_excludes_tombstone_values()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", new byte[] { 1, 2, 3, 4 });
        await grain.DeleteAsync("k");

        var stats = await grain.GetStatsAsync();

        // The tombstone retains the key bytes ("k" = 1) but carries no value.
        Assert.That(stats.StateBytes, Is.EqualTo(1));
    }
}

