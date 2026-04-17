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
        Assert.That(Encoding.UTF8.GetString(result), Is.EqualTo("v1"));
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

    // --- Sibling pointers ---

    [Test]
    public async Task NextSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetNextSiblingAsync(), Is.Null);
    }

    [Test]
    public async Task SetNextSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-1");
        await grain.SetNextSiblingAsync(siblingId);

        Assert.That(await grain.GetNextSiblingAsync(), Is.EqualTo(siblingId));
    }

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

    // --- Multiple keys ---

    [Test]
    public async Task Multiple_keys_are_independent()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("1"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("3"));
    }

    [Test]
    public async Task Deleting_one_key_does_not_affect_others()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        await grain.DeleteAsync("a");

        Assert.That(await grain.GetAsync("a"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("2"));
    }

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

    // --- Split recovery ---

    [Test]
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

        Assert.That(result, Is.Not.Null);
        Assert.That(result.PromotedKey, Is.EqualTo("m"));
        Assert.That(result.NewSiblingId, Is.EqualTo(siblingId));
    }

    [Test]
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
        Assert.That(result1!.NewSiblingId, Is.EqualTo(siblingId));
    }

    [Test]
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
        Assert.That(state.State.Entries.ContainsKey("a"), Is.True);
        Assert.That(state.State.Entries.ContainsKey("b"), Is.True);
        Assert.That(state.State.Entries.ContainsKey("m"), Is.False);
        Assert.That(state.State.Entries.ContainsKey("z"), Is.False);
    }

    [Test]
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

        Assert.That(state.State.SplitState, Is.EqualTo(
            Orleans.Lattice.Primitives.SplitState.SplitComplete));
    }

    [Test]
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

        Assert.That(state.State.Entries.ContainsKey("m"), Is.False);
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

    // --- Recovery applies write ---

    [Test]
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

        Assert.That(result, Is.Not.Null);
        Assert.That(state.State.Entries.ContainsKey("b"), Is.True);
        Assert.That(Encoding.UTF8.GetString(state.State.Entries["b"].Value!), Is.EqualTo("local"));
    }

    [Test]
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

        Assert.That(result, Is.Not.Null);
        // The write was NOT applied to this leaf.
        Assert.That(state.State.Entries.ContainsKey("z"), Is.False);
        // The sibling's SetAsync was called.
        await siblingMock.Received(1).SetAsync("z", Arg.Any<byte[]>());
    }

    // --- PrevSibling pointers ---

    [Test]
    public async Task PrevSibling_is_null_initially()
    {
        var grain = CreateGrain();
        Assert.That(await grain.GetPrevSiblingAsync(), Is.Null);
    }

    [Test]
    public async Task SetPrevSibling_persists_sibling_id()
    {
        var grain = CreateGrain();
        var siblingId = GrainId.Create("leaf", "sibling-left");
        await grain.SetPrevSiblingAsync(siblingId);

        Assert.That(await grain.GetPrevSiblingAsync(), Is.EqualTo(siblingId));
    }

    // --- Split doubly-linked list maintenance ---
    // These tests simulate a crash mid-split (same pattern as existing recovery tests)
    // then trigger CompleteSplitAsync via the next SetAsync call.

    [Test]
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

    [Test]
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

    [Test]
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

    [Test]
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

        Assert.That(state.State.OldNextSibling, Is.Null);
    }

    [Test]
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

    // --- SetTreeIdAsync

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

    // --- CompactTombstonesAsync ---

    [Test]
    public async Task CompactTombstones_removes_old_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Insert a tombstone with a very old timestamp.
        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["dead"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Version.Tick("test"); // ensure version advances past LastCompactionVersion

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(state.State.Entries.ContainsKey("dead"), Is.False);
    }

    [Test]
    public async Task CompactTombstones_keeps_recent_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        // Insert a tombstone with a very recent timestamp.
        var recentClock = new HybridLogicalClock
        {
            WallClockTicks = DateTimeOffset.UtcNow.Ticks,
            Counter = 0
        };
        state.State.Entries["recent"] = LwwValue<byte[]>.Tombstone(recentClock);

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("recent"), Is.True);
    }

    [Test]
    public async Task CompactTombstones_does_not_remove_live_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["alive"] = LwwValue<byte[]>.Create(
            Encoding.UTF8.GetBytes("value"), oldClock);

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(state.State.Entries.ContainsKey("alive"), Is.True);
    }

    [Test]
    public async Task CompactTombstones_tracks_LastCompactionVersion()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        await grain.CompactTombstonesAsync(TimeSpan.Zero);

        Assert.That(state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version), Is.True);
    }

    [Test]
    public async Task CompactTombstones_skips_scan_when_nothing_changed()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.DeleteAsync("k1");

        // First compaction removes the tombstone.
        var removed1 = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        Assert.That(removed1, Is.EqualTo(1));

        // Second compaction should be a no-op (version hasn't changed).
        var removed2 = await grain.CompactTombstonesAsync(TimeSpan.Zero);
        Assert.That(removed2, Is.EqualTo(0));
    }

    [Test]
    public async Task CompactTombstones_returns_count_of_removed_entries()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);

        var oldClock = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        state.State.Entries["a"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["b"] = LwwValue<byte[]>.Tombstone(oldClock);
        state.State.Entries["c"] = LwwValue<byte[]>.Create(
            Encoding.UTF8.GetBytes("live"), oldClock);
        state.State.Version.Tick("test"); // ensure version advances past LastCompactionVersion

        var removed = await grain.CompactTombstonesAsync(TimeSpan.FromHours(1));

        Assert.That(removed, Is.EqualTo(2));
        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
        Assert.That(state.State.Entries.ContainsKey("c"), Is.True);
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
}
