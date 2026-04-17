using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    // --- MergeEntriesAsync ---

    [Test]
    public async Task MergeEntries_preserves_original_timestamps()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_preserves_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Tombstone(clock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries["k1"].IsTombstone, Is.True);
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_is_idempotent()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(default);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock)
        };

        await grain.MergeEntriesAsync(entries);
        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(clock));
    }

    [Test]
    public async Task MergeEntries_keeps_newer_local_value()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Write a value locally first (gets a fresh timestamp).
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("local"));
        var localTimestamp = state.State.Entries["k1"].Timestamp;

        // Merge an older entry — should be ignored by LWW.
        var olderClock = default(HybridLogicalClock);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("stale"), olderClock)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(Encoding.UTF8.GetString(state.State.Entries["k1"].Value!), Is.EqualTo("local"));
        Assert.That(state.State.Entries["k1"].Timestamp, Is.EqualTo(localTimestamp));
    }

    [Test]
    public async Task MergeEntries_with_mixed_live_and_tombstone_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock1 = HybridLogicalClock.Tick(default);
        var clock2 = HybridLogicalClock.Tick(clock1);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["live"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("value"), clock1),
            ["dead"] = LwwValue<byte[]>.Tombstone(clock2)
        };

        await grain.MergeEntriesAsync(entries);

        Assert.That(state.State.Entries.Count, Is.EqualTo(2));
        Assert.That(state.State.Entries["live"].IsTombstone, Is.False);
        Assert.That(Encoding.UTF8.GetString(state.State.Entries["live"].Value!), Is.EqualTo("value"));
        Assert.That(state.State.Entries["dead"].IsTombstone, Is.True);
    }

    [Test]
    public async Task MergeEntries_with_empty_dictionary_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("existing", Encoding.UTF8.GetBytes("v"));
        var countBefore = state.State.Entries.Count;

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.That(state.State.Entries.Count, Is.EqualTo(countBefore));
    }
}
