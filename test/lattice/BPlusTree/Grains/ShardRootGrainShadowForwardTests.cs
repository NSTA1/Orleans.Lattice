using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the online shadow-forwarding primitive on
/// <see cref="ShardRootGrain"/>: lifecycle transitions, the reject gate,
/// and per-mutation-path forwarding to the destination shard.
/// </summary>
public class ShardRootGrainShadowForwardTests
{
    private const string TreeId = "src-tree";
    private const string DestTreeId = "src-tree/resized/op-1";
    private const string OperationId = "op-1";
    private const int ShardIndex = 0;

    private sealed class GrainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required IShardRootGrain ShadowTarget { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required IGrainFactory Factory { get; init; }
    }

    private static GrainHarness CreateHarness(
        FakePersistentState<ShardRootState>? state = null,
        bool getOrSetReturnsExisting = false,
        bool setIfVersionSucceeds = true)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        state ??= new FakePersistentState<ShardRootState>();
        state.State.RootNodeId ??= GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        leaf.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>()).Returns(
            Task.FromResult(new RangeDeleteResult { Deleted = 0, PastRange = true }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        leaf.GetOrSetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult(
            getOrSetReturnsExisting
                ? new GetOrSetResult { ExistingValue = [9, 9], Split = null }
                : new GetOrSetResult { ExistingValue = null, Split = null }));
        leaf.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>())
            .Returns(Task.FromResult(new CasResult { Success = setIfVersionSucceeds, Split = null }));
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        leaf.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        // The shadow target is resolved via GetGrain<IShardRootGrain>("{DestTreeId}/{ShardIndex}")
        // — return the same mock for any string key so we can verify call reception.
        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);
        shadowTarget.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<long>()).Returns(Task.CompletedTask);
        shadowTarget.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        shadowTarget.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>()).Returns(Task.FromResult(0));
        shadowTarget.GetOrSetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult<byte[]?>(null));
        shadowTarget.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.CompletedTask);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(context, state, factory, optionsResolver);
        return new GrainHarness
        {
            Grain = grain,
            Leaf = leaf,
            ShadowTarget = shadowTarget,
            State = state,
            Factory = factory,
        };
    }

    private static void SetShadowPhase(FakePersistentState<ShardRootState> state, ShadowForwardPhase phase) =>
        state.State.ShadowForward = new ShadowForwardState
        {
            DestinationPhysicalTreeId = DestTreeId,
            Phase = phase,
            OperationId = OperationId,
        };

    // ============================================================================
    // BeginShadowForwardAsync
    // ============================================================================

    [Test]
    public void BeginShadowForwardAsync_throws_when_destinationTreeId_is_null()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginShadowForwardAsync(null!, OperationId, "logical-tree"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginShadowForwardAsync_throws_when_destinationTreeId_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginShadowForwardAsync("", OperationId, "logical-tree"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginShadowForwardAsync_throws_when_operationId_is_null()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginShadowForwardAsync(DestTreeId, null!, "logical-tree"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginShadowForwardAsync_throws_when_operationId_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginShadowForwardAsync(DestTreeId, "", "logical-tree"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BeginShadowForwardAsync_throws_when_destination_equals_source()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.BeginShadowForwardAsync(TreeId, OperationId, "logical-tree"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task BeginShadowForwardAsync_persists_draining_state_on_first_call()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(h.State.State.ShadowForward, Is.Not.Null);
        Assert.That(h.State.State.ShadowForward!.DestinationPhysicalTreeId, Is.EqualTo(DestTreeId));
        Assert.That(h.State.State.ShadowForward!.OperationId, Is.EqualTo(OperationId));
        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Draining));
        Assert.That(h.State.WriteCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task BeginShadowForwardAsync_is_idempotent_for_same_destination_and_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        var writesAfterFirst = h.State.WriteCount;

        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(h.State.WriteCount, Is.EqualTo(writesAfterFirst), "no additional write on idempotent re-entry");
    }

    [Test]
    public async Task BeginShadowForwardAsync_is_idempotent_across_phase_transitions()
    {
        // Re-entry during Drained is legal — returns silently without regressing the phase.
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        await h.Grain.MarkDrainedAsync(OperationId);
        var writesBefore = h.State.WriteCount;

        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Drained));
        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore), "idempotent — no phase regression");
    }

    [Test]
    public async Task BeginShadowForwardAsync_refuses_different_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(async () => await h.Grain.BeginShadowForwardAsync(DestTreeId, "op-2", "logical-tree"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task BeginShadowForwardAsync_refuses_different_destination_under_same_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(async () => await h.Grain.BeginShadowForwardAsync("other-dest", OperationId, "logical-tree"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ============================================================================
    // MarkDrainedAsync
    // ============================================================================

    [Test]
    public void MarkDrainedAsync_throws_when_operationId_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.MarkDrainedAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MarkDrainedAsync_throws_when_no_active_operation()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.MarkDrainedAsync(OperationId),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task MarkDrainedAsync_transitions_from_draining_to_drained()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        await h.Grain.MarkDrainedAsync(OperationId);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Drained));
    }

    [Test]
    public async Task MarkDrainedAsync_is_idempotent_when_already_drained()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        await h.Grain.MarkDrainedAsync(OperationId);
        var writesBefore = h.State.WriteCount;

        await h.Grain.MarkDrainedAsync(OperationId);

        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task MarkDrainedAsync_is_idempotent_when_already_rejecting()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        await h.Grain.MarkDrainedAsync(OperationId);
        await h.Grain.EnterRejectingAsync(OperationId);
        var phaseBefore = h.State.State.ShadowForward!.Phase;

        await h.Grain.MarkDrainedAsync(OperationId);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(phaseBefore));
    }

    [Test]
    public async Task MarkDrainedAsync_refuses_different_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(async () => await h.Grain.MarkDrainedAsync("op-other"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ============================================================================
    // EnterRejectingAsync
    // ============================================================================

    [Test]
    public void EnterRejectingAsync_throws_when_operationId_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.EnterRejectingAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void EnterRejectingAsync_throws_when_no_active_operation()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.EnterRejectingAsync(OperationId),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task EnterRejectingAsync_transitions_from_drained_to_rejecting()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        await h.Grain.MarkDrainedAsync(OperationId);

        await h.Grain.EnterRejectingAsync(OperationId);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Rejecting));
    }

    [Test]
    public async Task EnterRejectingAsync_transitions_directly_from_draining_when_coordinator_skips_drained()
    {
        // Defensive: if a coordinator invokes the swap without calling MarkDrained first
        // (e.g. pathological restart path), the transition must still be allowed.
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        await h.Grain.EnterRejectingAsync(OperationId);

        Assert.That(h.State.State.ShadowForward!.Phase, Is.EqualTo(ShadowForwardPhase.Rejecting));
    }

    [Test]
    public async Task EnterRejectingAsync_is_idempotent_when_already_rejecting()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");
        await h.Grain.EnterRejectingAsync(OperationId);
        var writesBefore = h.State.WriteCount;

        await h.Grain.EnterRejectingAsync(OperationId);

        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task EnterRejectingAsync_refuses_different_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(async () => await h.Grain.EnterRejectingAsync("op-other"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ============================================================================
    // ClearShadowForwardAsync
    // ============================================================================

    [Test]
    public void ClearShadowForwardAsync_throws_when_operationId_is_empty()
    {
        var h = CreateHarness();
        Assert.That(async () => await h.Grain.ClearShadowForwardAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ClearShadowForwardAsync_is_idempotent_when_no_active_operation()
    {
        var h = CreateHarness();
        var writesBefore = h.State.WriteCount;

        await h.Grain.ClearShadowForwardAsync(OperationId);

        Assert.That(h.State.State.ShadowForward, Is.Null);
        Assert.That(h.State.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task ClearShadowForwardAsync_clears_state_on_matching_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        await h.Grain.ClearShadowForwardAsync(OperationId);

        Assert.That(h.State.State.ShadowForward, Is.Null);
    }

    [Test]
    public async Task ClearShadowForwardAsync_refuses_different_operationId()
    {
        var h = CreateHarness();
        await h.Grain.BeginShadowForwardAsync(DestTreeId, OperationId, "logical-tree");

        Assert.That(async () => await h.Grain.ClearShadowForwardAsync("op-other"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ============================================================================
    // Reject gate — every read / write throws in Rejecting phase
    // ============================================================================

    [Test]
    public void Get_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.GetAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void Set_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.SetAsync("k", [1]),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void Delete_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.DeleteAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void Count_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.CountAsync(),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void Exists_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        Assert.That(async () => await h.Grain.ExistsAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void MergeMany_throws_StaleTreeRoutingException_in_rejecting_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        };

        Assert.That(async () => await h.Grain.MergeManyAsync(entries),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public async Task Read_and_write_succeed_in_draining_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        // Reads and writes still serve locally during Draining.
        Assert.That(async () => await h.Grain.GetAsync("k"), Throws.Nothing);
        await h.Grain.SetAsync("k", [1]);

        // And the write was forwarded.
        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task Read_and_write_succeed_in_drained_phase()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);

        Assert.That(async () => await h.Grain.GetAsync("k"), Throws.Nothing);
        await h.Grain.SetAsync("k", [1]);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    // ============================================================================
    // Mutation forwarding — every write path mirrors to the destination
    // ============================================================================

    [Test]
    public async Task SetAsync_does_not_forward_when_shadow_state_is_null()
    {
        var h = CreateHarness();
        await h.Grain.SetAsync("k", [1]);

        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_forwards_during_draining()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.SetAsync("k", [1, 2, 3]);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Is<byte[]>(b => b.SequenceEqual(new byte[] { 1, 2, 3 })));
    }

    [Test]
    public async Task SetAsync_forwards_during_drained()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Drained);

        await h.Grain.SetAsync("k", [9]);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_with_expiry_forwards_the_expiry_to_destination()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);
        const long expiresAt = 1_234_567_890L;

        await h.Grain.SetAsync("k", [1], expiresAt);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>(), expiresAt);
    }

    [Test]
    public async Task DeleteAsync_forwards_during_draining()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.DeleteAsync("k");

        await h.ShadowTarget.Received().DeleteAsync("k");
    }

    [Test]
    public async Task DeleteRangeAsync_forwards_during_draining()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.DeleteRangeAsync("a", "z");

        await h.ShadowTarget.Received().DeleteRangeAsync("a", "z");
    }

    [Test]
    public async Task GetOrSetAsync_forwards_during_draining_when_write_occurs()
    {
        var h = CreateHarness(getOrSetReturnsExisting: false);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.GetOrSetAsync("k", [1]);

        await h.ShadowTarget.Received().GetOrSetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task GetOrSetAsync_forwards_during_draining_when_key_is_already_live()
    {
        // GetOrSet forwards semantic intent even when no local write happens,
        // so the destination converges on the same "existing-or-set" outcome.
        var h = CreateHarness(getOrSetReturnsExisting: true);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.GetOrSetAsync("k", [1]);

        await h.ShadowTarget.Received().GetOrSetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetIfVersionAsync_forwards_on_success()
    {
        var h = CreateHarness(setIfVersionSucceeds: true);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.SetIfVersionAsync("k", [1], HybridLogicalClock.Zero);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetIfVersionAsync_does_not_forward_on_precondition_failure()
    {
        var h = CreateHarness(setIfVersionSucceeds: false);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        var result = await h.Grain.SetIfVersionAsync("k", [1], HybridLogicalClock.Zero);

        Assert.That(result, Is.False);
        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task MergeManyAsync_forwards_full_batch_to_destination()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            ["k2"] = LwwValue<byte[]>.Create([2], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        };

        await h.Grain.MergeManyAsync(entries);

        await h.ShadowTarget.Received().MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.Count == 2 && d.ContainsKey("k1") && d.ContainsKey("k2")));
    }

    [Test]
    public async Task Writes_do_not_forward_in_rejecting_phase_because_they_throw_first()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        try { await h.Grain.SetAsync("k", [1]); } catch (StaleTreeRoutingException) { }
        try { await h.Grain.DeleteAsync("k"); } catch (StaleTreeRoutingException) { }

        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        await h.ShadowTarget.DidNotReceive().DeleteAsync(Arg.Any<string>());
    }

    [Test]
    public async Task SetManyAsync_forwards_full_batch_in_single_call_to_destination()
    {
        var h = CreateHarness();
        h.ShadowTarget.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(Task.CompletedTask);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
            new("k2", [2]),
            new("k3", [3]),
        };

        await h.Grain.SetManyAsync(entries);

        // Batched-forward contract: exactly one t.SetManyAsync call for the whole
        // batch, not one per entry. Per-entry t.SetAsync calls must not occur.
        await h.ShadowTarget.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(l => l.Count == 3));
        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public void DeleteRangeAsync_throws_in_rejecting_phase_like_other_writes()
    {
        var h = CreateHarness();
        SetShadowPhase(h.State, ShadowForwardPhase.Rejecting);

        // PrepareForOperationAsync universally rejects in the Rejecting phase.
        // DeleteRangeAsync skips per-key routing but still goes through the
        // shared preamble — confirming uniform behaviour across all writes.
        Assert.That(
            async () => await h.Grain.DeleteRangeAsync("a", "z"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }
}
