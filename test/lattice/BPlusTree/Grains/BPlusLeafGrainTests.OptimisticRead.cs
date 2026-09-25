using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase("InFlight")]
    [TestCase("Committed")]
    [TestCase("Aborted")]
    public async Task GetWithVersion_pending_visibility_never_returns_an_ownership_proof(string statusName)
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparedSetAsync(grain, txid, "k", [1]);
        using (LatticeRegistrySnapshotContext.BeginScope(
            new Dictionary<Guid, TxStatus> { [txid] = Enum.Parse<TxStatus>(statusName) }))
        {
            var reply = await grain.GetWithVersionAsync("k");
            Assert.That(reply.LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
            Assert.That(reply.LeafRoutingGeneration, Is.Zero);
        }
    }

    [Test]
    public async Task GetWithVersion_failed_range_mutation_closes_scope_but_does_not_reuse_generation()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var before = await grain.GetWithVersionAsync("k");
        state.ThrowOnWrite = new InvalidOperationException("injected topology persist failure");
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.SetKeyRangeAsync("a", "z"));
        var after = await grain.GetWithVersionAsync("k");
        Assert.That(after.LeafRoutingEpoch, Is.EqualTo(before.LeafRoutingEpoch));
        Assert.That(after.LeafRoutingGeneration, Is.GreaterThan(before.LeafRoutingGeneration));
    }

    [Test]
    public async Task GetWithVersion_ordinary_writes_preserve_stamp_and_absence_proof()
    {
        var grain = CreateGrain();
        var absent = await grain.GetWithVersionAsync("k");
        await grain.SetAsync("k", [1]);
        var present = await grain.GetWithVersionAsync("k");
        await grain.DeleteAsync("k");
        var deleted = await grain.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(absent.Value, Is.Null);
            Assert.That(absent.LeafRoutingEpoch, Is.Not.EqualTo(Guid.Empty));
            Assert.That(absent.LeafRoutingGeneration, Is.GreaterThan(0));
            Assert.That(present.Value, Is.EqualTo(new byte[] { 1 }));
            Assert.That(present.LeafRoutingEpoch, Is.EqualTo(absent.LeafRoutingEpoch));
            Assert.That(present.LeafRoutingGeneration, Is.EqualTo(absent.LeafRoutingGeneration));
            Assert.That(deleted.Value, Is.Null);
            Assert.That(deleted.LeafRoutingGeneration, Is.EqualTo(absent.LeafRoutingGeneration));
        });
    }

    [Test]
    public async Task GetWithVersion_reactivation_changes_epoch_even_with_identical_persisted_state()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var before = await CreateGrain(state).GetWithVersionAsync("k");
        var after = await CreateGrain(state).GetWithVersionAsync("k");
        Assert.That(after.LeafRoutingEpoch, Is.Not.EqualTo(before.LeafRoutingEpoch));
        Assert.That(after.LeafRoutingEpoch, Is.Not.EqualTo(Guid.Empty));
    }

    [TestCase("a", false)]
    [TestCase("m", true)]
    [TestCase("y", true)]
    [TestCase("z", false)]
    public async Task GetWithVersion_absence_is_stamped_only_in_owned_half_open_range(string key, bool owned)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "m";
        state.State.HighKeyExclusive = "z";
        var reply = await CreateGrain(state).GetWithVersionAsync(key);
        Assert.That(reply.Value, Is.Null);
        Assert.That(reply.LeafRoutingEpoch != Guid.Empty, Is.EqualTo(owned));
        Assert.That(reply.LeafRoutingGeneration > 0, Is.EqualTo(owned));
    }

    [Test]
    public async Task GetWithVersion_sealing_and_unsealing_invalidate_before_persist_and_on_completion()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var before = await grain.GetWithVersionAsync("k");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        var slot = ShardMap.GetVirtualSlot("k", 64);
        var seal = grain.MarkSlotsMovedAwayAsync([slot], 64);
        Assert.That(seal.IsCompleted, Is.False);
        Assert.That((await grain.GetWithVersionAsync("k")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        gate.SetResult();
        await seal;
        Assert.That((await grain.GetWithVersionAsync("k")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));

        gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lift = grain.UnmarkSlotsMovedAwayAsync([slot], 64);
        Assert.That(lift.IsCompleted, Is.False);
        Assert.That((await grain.GetWithVersionAsync("k")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        gate.SetResult();
        await lift;
        var after = await grain.GetWithVersionAsync("k");
        Assert.That(after.LeafRoutingEpoch, Is.EqualTo(before.LeafRoutingEpoch));
        Assert.That(after.LeafRoutingGeneration, Is.GreaterThan(before.LeafRoutingGeneration));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetWithVersion_retirement_refuses_proofs_until_abandoned(bool orphan)
    {
        var grain = CreateGrain();
        var before = await grain.GetWithVersionAsync("k");
        var retired = orphan ? await grain.TryBeginOrphanRetirementAsync() : await grain.TryBeginRetirementAsync();
        Assert.That(retired, Is.True);
        Assert.That((await grain.GetWithVersionAsync("k")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        await grain.AbandonRetirementAsync();
        var after = await grain.GetWithVersionAsync("k");
        Assert.That(after.LeafRoutingGeneration, Is.GreaterThan(before.LeafRoutingGeneration));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetWithVersion_merge_range_widen_refuses_proofs_while_in_flight(bool unlink)
    {
        var state = new FakePersistentState<LeafNodeState>();
        var successor = GrainId.Create("leaf", "victim");
        state.State.HighKeyExclusive = "m";
        state.State.NextSibling = successor;
        var grain = CreateGrain(state);
        var before = await grain.GetWithVersionAsync("a");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        Task merge = unlink
            ? grain.TryUnlinkSuccessorAsync(successor, null, null)
            : grain.AbsorbSuccessorRangeAsync(null);
        Assert.That(merge.IsCompleted, Is.False);
        Assert.That((await grain.GetWithVersionAsync("a")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        Assert.That((await grain.GetWithVersionAsync("z")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        gate.SetResult();
        await merge;
        var after = await grain.GetWithVersionAsync("z");
        Assert.That(after.LeafRoutingGeneration, Is.GreaterThan(before.LeafRoutingGeneration));
    }

    [Test]
    public async Task GetWithVersion_split_refuses_proofs_before_transfer_and_fences_moved_range()
    {
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", "ownership-split-sibling"));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "test-tree";
        var grain = CreateGrain(state, siblingStub: sibling, maxLeafKeys: 4);
        foreach (var key in new[] { "a", "b", "c", "d" })
            await grain.SetAsync(key, [1]);
        var before = await grain.GetWithVersionAsync("a");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        sibling.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(gate.Task);
        var split = grain.SetAsync("e", [1]);
        Assert.That(split.IsCompleted, Is.False);
        Assert.That((await grain.GetWithVersionAsync("a")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
        gate.SetResult();
        Assert.That(await split, Is.Not.Null);
        var after = await grain.GetWithVersionAsync("a");
        Assert.That(after.LeafRoutingGeneration, Is.GreaterThan(before.LeafRoutingGeneration));
        Assert.That((await grain.GetWithVersionAsync("e")).LeafRoutingEpoch, Is.EqualTo(Guid.Empty));
    }
}
