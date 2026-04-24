using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Tests in this partial historically constructed BPlusLeafGrain
    /// directly with IOptionsMonitor. With structural sizing now pinned in
    /// the registry, the grain takes a LatticeOptionsResolver instead.
    /// This helper keeps the pre-existing call-sites concise.
    /// </summary>
    private static BPlusLeafGrain BuildGrain(
        IGrainContext context,
        FakePersistentState<LeafNodeState> state,
        IGrainFactory grainFactory,
        int maxLeafKeys = 128)
    {
        var resolver = TestOptionsResolver.Create(
            maxLeafKeys: maxLeafKeys,
            shardCount: 1,
            factory: grainFactory);
        return new BPlusLeafGrain(context, state, grainFactory, resolver, TestMutationObservers.NoObservers());
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
        Assert.That(result!.PromotedKey, Is.EqualTo("m"));
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

        var grain = BuildGrain(context, state, grainFactory);

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
        await siblingMock.Received(1).SetAsync("z", Arg.Any<byte[]>(), 0L);
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
        var grain = BuildGrain(context, state, grainFactory);

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
        var grain = BuildGrain(context, state, grainFactory);

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
        var grain = BuildGrain(context, state, grainFactory);

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
        var grain = BuildGrain(context, state, grainFactory);

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
        var grain = BuildGrain(context, state, grainFactory);

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

    [Test]
    public async Task Split_flushes_source_state_before_returning_SplitResult()
    {
        // Regression: the source leaf's trimmed state must be persisted
        // before the SplitResult is returned to the caller (which in turn
        // publishes the new sibling to the parent node). If the caller
        // crashes after receiving SplitResult but before the source's
        // trimmed state is persisted, the parent would route lookups to
        // the new sibling while the unflushed source still holds the
        // (now-moved) keys — producing a visible "duplicated keys"
        // state in the tree.
        var state = new FakePersistentState<LeafNodeState>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        sibling.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        sibling.SetNextSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);
        sibling.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(Task.CompletedTask);

        var grain = CreateGrain(state, siblingStub: sibling, maxLeafKeys: 3);

        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        var result = await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));

        Assert.That(result, Is.Not.Null, "split should have occurred");

        // Invariant 1: source's post-split persisted Entries must not
        // contain any key >= the promoted split key (which now lives on
        // the sibling). If the source's trim was never flushed, those
        // keys would still appear here.
        var splitKey = result!.PromotedKey;
        foreach (var key in state.State.Entries.Keys)
        {
            Assert.That(string.Compare(key, splitKey, StringComparison.Ordinal), Is.LessThan(0),
                $"source still holds {key} which should have moved to the sibling");
        }

        // Invariant 2: source's persisted NextSibling points at the new
        // sibling, and SplitState is SplitComplete. Both prove the
        // flush happened.
        Assert.That(state.State.SplitState, Is.EqualTo(Orleans.Lattice.Primitives.SplitState.SplitComplete));
        Assert.That(state.State.NextSibling, Is.EqualTo(result.NewSiblingId));
    }
}
