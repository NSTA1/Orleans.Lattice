using Microsoft.Extensions.Options;
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
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);
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

    // --- MergeEntriesAsync ---

    [Fact]
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

        Assert.True(state.State.Entries.ContainsKey("k1"));
        Assert.Equal(clock, state.State.Entries["k1"].Timestamp);
    }

    [Fact]
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

        Assert.True(state.State.Entries["k1"].IsTombstone);
        Assert.Equal(clock, state.State.Entries["k1"].Timestamp);
    }

    [Fact]
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

        Assert.Single(state.State.Entries);
        Assert.Equal(clock, state.State.Entries["k1"].Timestamp);
    }

    [Fact]
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

        Assert.Equal("local", Encoding.UTF8.GetString(state.State.Entries["k1"].Value!));
        Assert.Equal(localTimestamp, state.State.Entries["k1"].Timestamp);
    }

    [Fact]
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

        Assert.Equal(2, state.State.Entries.Count);
        Assert.False(state.State.Entries["live"].IsTombstone);
        Assert.Equal("value", Encoding.UTF8.GetString(state.State.Entries["live"].Value!));
        Assert.True(state.State.Entries["dead"].IsTombstone);
    }

    [Fact]
    public async Task MergeEntries_with_empty_dictionary_is_noop()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("existing", Encoding.UTF8.GetBytes("v"));
        var countBefore = state.State.Entries.Count;

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.Equal(countBefore, state.State.Entries.Count);
    }

    // --- Split recovery ---

    [Fact]
    public async Task SetAsync_returns_split_result_when_state_has_in_progress_split()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Simulate a crash mid-split: set SplitState, SplitKey, and SplitSiblingId
        // but leave entries un-trimmed.
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        // The next SetAsync should resume the split AND forward the write to the sibling.
        var result = await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        Assert.NotNull(result);
        Assert.Equal("m", result.PromotedKey);
        Assert.Equal(siblingId, result.NewSiblingId);
    }

    [Fact]
    public async Task Recovery_reuses_persisted_sibling_id()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        // First recovery attempt.
        var result1 = await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        // The sibling ID in the result must match the persisted one — not a new Guid.
        Assert.Equal(siblingId, result1!.NewSiblingId);
    }

    [Fact]
    public async Task Recovery_trims_entries_from_original_leaf()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("3"));
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("4"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.NextSibling = state.State.SplitSiblingId;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("x", Encoding.UTF8.GetBytes("5"));

        // After recovery, entries >= "m" should be removed from the original leaf.
        Assert.True(state.State.Entries.ContainsKey("a"));
        Assert.True(state.State.Entries.ContainsKey("b"));
        Assert.False(state.State.Entries.ContainsKey("m"));
        Assert.False(state.State.Entries.ContainsKey("z"));
    }

    [Fact]
    public async Task Recovery_advances_split_state_to_complete()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.NextSibling = state.State.SplitSiblingId;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        Assert.Equal(
            Orleans.Lattice.Primitives.SplitState.SplitComplete,
            state.State.SplitState);
    }

    [Fact]
    public async Task Recovery_preserves_tombstones_in_right_half()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        await grain.DeleteAsync("m"); // creates a tombstone for "m"

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.NextSibling = state.State.SplitSiblingId;
        state.State.TreeId = "test-tree";

        // Recovery should transfer the tombstone to the sibling (via MergeEntriesAsync).
        // The tombstone should be removed from the original leaf.
        await grain.SetAsync("z", Encoding.UTF8.GetBytes("3"));

        Assert.False(state.State.Entries.ContainsKey("m"));
    }

    // --- GetDeltaSinceAsync includes SplitKey ---

    [Fact]
    public async Task Delta_includes_split_key_after_split()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        // Simulate a completed split.
        state.State.SplitKey = "m";
        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitComplete;

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.Equal("m", delta.SplitKey);
    }

    [Fact]
    public async Task Delta_split_key_is_null_when_no_split()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var delta = await grain.GetDeltaSinceAsync(new VersionVector());
        Assert.Null(delta.SplitKey);
    }

    // --- Recovery applies write ---

    [Fact]
    public async Task Recovery_applies_write_locally_when_key_below_split_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.NextSibling = state.State.SplitSiblingId;
        state.State.TreeId = "test-tree";

        // "b" < "m" → write should be applied to THIS leaf.
        var result = await grain.SetAsync("b", Encoding.UTF8.GetBytes("local"));

        Assert.NotNull(result);
        Assert.True(state.State.Entries.ContainsKey("b"));
        Assert.Equal("local", Encoding.UTF8.GetString(state.State.Entries["b"].Value!));
    }

    [Fact]
    public async Task Recovery_forwards_write_to_sibling_when_key_at_or_above_split_key()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);

        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.TreeId = "test-tree";

        // "z" >= "m" → write should be forwarded to the sibling.
        var result = await grain.SetAsync("z", Encoding.UTF8.GetBytes("forwarded"));

        Assert.NotNull(result);
        // The write was NOT applied to this leaf.
        Assert.False(state.State.Entries.ContainsKey("z"));
        // The sibling's SetAsync was called.
        await siblingMock.Received(1).SetAsync("z", Arg.Any<byte[]>());
    }

    // --- PrevSibling pointers ---

    [Fact]
    public async Task PrevSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.Null(await grain.GetPrevSiblingAsync());
    }

    [Fact]
    public async Task SetPrevSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-left");
        await grain.SetPrevSiblingAsync(siblingId);

        Assert.Equal(siblingId, await grain.GetPrevSiblingAsync());
    }

    // --- Split doubly-linked list maintenance ---
    // These tests simulate a crash mid-split (same pattern as existing recovery tests)
    // then trigger CompleteSplitAsync via the next SetAsync call.

    [Fact]
    public async Task Split_recovery_sets_new_sibling_PrevSibling_to_this_leaf()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        var grainId = GrainId.Create("leaf", "test-leaf");
        context.GrainId.Returns(grainId);
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.OldNextSibling = null;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        await siblingMock.Received().SetPrevSiblingAsync(grainId);
    }

    [Fact]
    public async Task Split_recovery_sets_new_sibling_NextSibling_to_old_next()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();
        var oldNextMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var oldNextId = GrainId.Create("leaf", "old-next");

        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId).Returns(oldNextMock);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.OldNextSibling = oldNextId;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        await siblingMock.Received().SetNextSiblingAsync(oldNextId);
    }

    [Fact]
    public async Task Split_recovery_updates_old_next_PrevSibling_to_new_sibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();
        var oldNextMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var oldNextId = GrainId.Create("leaf", "old-next");

        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId).Returns(oldNextMock);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.OldNextSibling = oldNextId;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        await oldNextMock.Received().SetPrevSiblingAsync(siblingId);
    }

    [Fact]
    public async Task Split_recovery_clears_OldNextSibling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();
        var oldNextMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var oldNextId = GrainId.Create("leaf", "old-next");

        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId).Returns(oldNextMock);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.OldNextSibling = oldNextId;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.Null(state.State.OldNextSibling);
    }

    [Fact]
    public async Task Split_recovery_with_no_old_next_sets_new_sibling_NextSibling_to_null()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        grainFactory.GetGrain<IBPlusLeafGrain>(siblingId).Returns(siblingMock);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsMonitor);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;
        state.State.OldNextSibling = null;
        state.State.TreeId = "test-tree";

        await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        await siblingMock.Received().SetNextSiblingAsync(null);
    }
}
