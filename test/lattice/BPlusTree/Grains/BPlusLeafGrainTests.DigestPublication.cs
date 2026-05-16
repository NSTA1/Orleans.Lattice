using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for leaf-side digest publication
/// (<see cref="BPlusLeafGrain.SetParentAsync"/>,
/// <see cref="BPlusLeafGrain.GetChildDigestSnapshotAsync"/>, and the
/// per-mutation <c>PublishDigestUpwardAsync</c> hook called from
/// <c>SetAsync</c> / <c>DeleteAsync</c>). Validates the dirty-flag
/// no-op short-circuit, parent persistence, and that every
/// digest-changing mutation triggers exactly one upward publish.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId LeafTestParentId =
        GrainId.Create("internal", "leaf-pub-parent");

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusInternalGrain ParentStub)
        CreateGrainWithParent(GrainId? parentId = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var parentStub = Substitute.For<IBPlusInternalGrain>();
        grainFactory
            .GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>())
            .Returns(parentStub);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-pub-leaf"));
        var state = new FakePersistentState<LeafNodeState>
        {
            State = { ParentId = parentId }
        };
        var options = new LatticeOptions();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: options,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, state, parentStub);
    }

    // --- SetParentAsync ---

    [Test]
    public async Task SetParentAsync_persists_parent_id()
    {
        var (grain, state, _) = CreateGrainWithParent();

        await grain.SetParentAsync(LeafTestParentId);

        Assert.That(state.State.ParentId, Is.EqualTo(LeafTestParentId));
    }

    [Test]
    public async Task SetParentAsync_does_not_callback_into_new_parent()
    {
        // Pull-based seeding contract: SetParentAsync persists only; the
        // parent pulls the child's snapshot via GetChildDigestSnapshotAsync
        // immediately afterward. A reentrant publish here would deadlock
        // the non-reentrant internal grain that may still be inside an
        // AcceptSplitAsync / InitializeAsync mutation frame.
        var (grain, _, parentStub) = CreateGrainWithParent();

        await grain.SetParentAsync(LeafTestParentId);

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task SetParentAsync_idempotent_on_same_parent_does_not_repersist()
    {
        var (grain, state, _) = CreateGrainWithParent(parentId: LeafTestParentId);
        var writesBefore = state.WriteCount;

        await grain.SetParentAsync(LeafTestParentId);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task SetParentAsync_idempotent_re_call_no_publish()
    {
        // The agreed contract: a re-call with the same parent skips the
        // persist and (under the pull-based seeding model) does not
        // republish either. The parent is expected to drive the
        // refresh via GetChildDigestSnapshotAsync when it needs one.
        var (grain, _, parentStub) = CreateGrainWithParent(parentId: LeafTestParentId);

        await grain.SetParentAsync(LeafTestParentId);

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task SetParentAsync_null_clears_slot()
    {
        var (grain, state, _) = CreateGrainWithParent(parentId: LeafTestParentId);

        await grain.SetParentAsync(null);

        Assert.That(state.State.ParentId, Is.Null);
    }

    // --- GetChildDigestSnapshotAsync ---

    [Test]
    public async Task GetChildDigestSnapshotAsync_returns_current_aggregates()
    {
        var (grain, state, _) = CreateGrainWithParent();
        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        var snapshot = await grain.GetChildDigestSnapshotAsync();

        Assert.That(snapshot.EntryCount, Is.EqualTo(state.State.Entries.Count));
        Assert.That(snapshot.Hash, Is.EqualTo(state.State.ProjectionHash));
        Assert.That(snapshot.CheckpointOffset, Is.EqualTo(state.State.ProjectionCheckpointOffset));
    }

    [Test]
    public async Task GetChildDigestSnapshotAsync_returns_cloned_hash()
    {
        var (grain, state, _) = CreateGrainWithParent();
        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        var snapshot = await grain.GetChildDigestSnapshotAsync();
        var firstByteBefore = state.State.ProjectionHash![0];
        snapshot.Hash![0] = (byte)(firstByteBefore ^ 0xFF);

        Assert.That(state.State.ProjectionHash[0], Is.EqualTo(firstByteBefore),
            "the snapshot Hash must be a clone so callers cannot mutate persisted state");
    }

    // --- Per-mutation publish chain ---

    [Test]
    public async Task Set_publishes_digest_upward_when_parent_is_set()
    {
        var (grain, _, parentStub) = CreateGrainWithParent(parentId: LeafTestParentId);
        parentStub.ClearReceivedCalls();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task Delete_publishes_digest_upward_when_parent_is_set()
    {
        var (grain, _, parentStub) = CreateGrainWithParent(parentId: LeafTestParentId);
        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));
        parentStub.ClearReceivedCalls();

        await grain.DeleteAsync("k0");

        await parentStub.Received(1).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task Set_does_not_publish_when_parent_is_null()
    {
        var (grain, _, parentStub) = CreateGrainWithParent();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task Delete_on_missing_key_does_not_publish()
    {
        // RemoveEntry early-returns when the key is absent: no fold
        // delta, no dirty flag flip, no publish.
        var (grain, _, parentStub) = CreateGrainWithParent(parentId: LeafTestParentId);
        parentStub.ClearReceivedCalls();

        await grain.DeleteAsync("never-existed");

        await parentStub.DidNotReceive().OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }

    [Test]
    public async Task Multiple_sets_publish_once_per_mutation()
    {
        var (grain, _, parentStub) = CreateGrainWithParent(parentId: LeafTestParentId);
        parentStub.ClearReceivedCalls();

        await grain.SetAsync("k0", Encoding.UTF8.GetBytes("v0"));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        await grain.SetAsync("k2", Encoding.UTF8.GetBytes("v2"));

        await parentStub.Received(3).OnChildDigestPublishedAsync(
            Arg.Any<GrainId>(),
            Arg.Any<ChildDigestSnapshot>());
    }
}
