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
    // --- GetAllRawEntriesAsync (internal, called directly on grain implementation) ---

    [Test]
    public async Task GetAllRawEntries_returns_empty_for_new_leaf()
    {
        var grain = CreateGrain();

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetAllRawEntries_includes_live_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("key-1", Encoding.UTF8.GetBytes("val-1"));

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Contains.Key("key-1"));
        Assert.That(result["key-1"].IsTombstone, Is.False);
    }

    [Test]
    public async Task GetAllRawEntries_includes_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("key-1", Encoding.UTF8.GetBytes("val-1"));
        await grain.DeleteAsync("key-1");

        var result = await grain.GetAllRawEntriesAsync();

        Assert.That(result, Contains.Key("key-1"));
        Assert.That(result["key-1"].IsTombstone, Is.True);
    }

    // --- MergeManyAsync ---

    [Test]
    public async Task MergeMany_inserts_new_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key-a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("val-a"), clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Null);
        Assert.That(state.State.Entries, Contains.Key("key-a"));
    }

    [Test]
    public async Task MergeMany_lww_resolves_newer_wins()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var newClock = HybridLogicalClock.Tick(oldClock);

        // Set existing with old timestamp.
        var existing = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("old"), oldClock),
        };
        await grain.MergeManyAsync(existing);

        // Merge with newer timestamp.
        var newer = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), newClock),
        };
        await grain.MergeManyAsync(newer);

        Assert.That(state.State.Entries["key"].Timestamp, Is.EqualTo(newClock));
    }

    [Test]
    public async Task MergeMany_lww_rejects_older()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var newClock = HybridLogicalClock.Tick(oldClock);

        // Set existing with new timestamp.
        var existing = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), newClock),
        };
        await grain.MergeManyAsync(existing);

        // Merge with older timestamp — should be rejected.
        var older = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("old"), oldClock),
        };
        await grain.MergeManyAsync(older);

        Assert.That(state.State.Entries["key"].Timestamp, Is.EqualTo(newClock));
    }

    [Test]
    public async Task MergeMany_triggers_split_when_overflowing()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Pre-populate to just under MaxLeafKeys (128).
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < 127; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            state.State.Entries[$"key-{i:D4}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v-{i}"), clock);
        }

        // Simulate that a split was triggered during a previous MergeMany and
        // persisted the split intent. This tests the split-aware code path.
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "key-0064"; // midpoint
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        // MergeMany should complete the interrupted split and merge the new entry.
        clock = HybridLogicalClock.Tick(clock);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["new-key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.PromotedKey, Is.EqualTo("key-0064"));
        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
    }

    [Test]
    public async Task MergeMany_ticks_version_vector()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var versionBefore = state.State.Version.GetClock("leaf/test-leaf");

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["key"] = LwwValue<byte[]>.Create([1], clock),
        };
        await grain.MergeManyAsync(entries);

        var versionAfter = state.State.Version.GetClock("leaf/test-leaf");
        Assert.That(versionAfter, Is.GreaterThan(versionBefore));
    }

    [Test]
    public async Task MergeMany_empty_entries_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var writeCountBefore = state.WriteCount;

        var result = await grain.MergeManyAsync([]);

        Assert.That(result, Is.Null);
        Assert.That(state.WriteCount, Is.EqualTo(writeCountBefore), "Empty merge should not write state");
        Assert.That(state.State.Entries, Is.Empty);
    }

    [Test]
    public async Task MergeMany_tombstone_only_entries_are_merged()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["dead-key"] = LwwValue<byte[]>.Tombstone(clock),
        };

        var result = await grain.MergeManyAsync(entries);

        Assert.That(result, Is.Null);
        Assert.That(state.State.Entries, Contains.Key("dead-key"));
        Assert.That(state.State.Entries["dead-key"].IsTombstone, Is.True);
    }

    [Test]
    public async Task MergeMany_does_not_tick_version_for_empty()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var versionBefore = state.State.Version.GetClock("leaf/test-leaf");

        await grain.MergeManyAsync([]);

        var versionAfter = state.State.Version.GetClock("leaf/test-leaf");
        Assert.That(versionAfter, Is.EqualTo(versionBefore));
    }
}
