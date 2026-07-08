using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the retained-previous-tree redirect primitive on
/// <see cref="ShardRootGrain"/>: the <c>MarkRetainedRedirectAsync</c> /
/// <c>ClearRetainedRedirectAsync</c> lifecycle and the hot-path redirect gate
/// that self-heals a stale shadow-cutover routing activation while leaving
/// direct-physical access and internal maintenance untouched.
/// </summary>
public class ShardRootGrainRetainedRedirectTests
{
    private const string PhysicalTreeId = "src-tree";
    private const string LogicalTreeId = "my-logical";
    private const string DestTreeId = "my-logical-bkprestore-abc";
    private const string OperationId = "restore-op-1";
    private const int ShardIndex = 0;

    private static readonly string MarkerKey = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;

    [TearDown]
    public void ClearAmbient() => RequestContext.Clear();

    private static ShardRootGrain CreateGrain(FakePersistentState<ShardRootState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{PhysicalTreeId}/{ShardIndex}"));

        state.State.RootNodeId ??= GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        return new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
    }

    private static FakePersistentState<ShardRootState> StateWithRedirect(
        string destinationPhysicalTreeId = DestTreeId,
        string operationId = OperationId,
        string logicalTreeId = LogicalTreeId)
    {
        var state = new FakePersistentState<ShardRootState>();
        state.State.RetainedRedirect = new RetainedRedirectState
        {
            DestinationPhysicalTreeId = destinationPhysicalTreeId,
            OperationId = operationId,
            LogicalTreeId = logicalTreeId,
        };
        return state;
    }

    // ========================================================================
    // Redirect gate
    // ========================================================================

    [Test]
    public void Read_throws_StaleTreeRoutingException_when_logical_alias_routed()
    {
        var grain = CreateGrain(StateWithRedirect());
        RequestContext.Set(MarkerKey, LogicalTreeId);

        Assert.That(async () => await grain.GetAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public async Task Redirect_exception_carries_logical_stale_and_destination_ids()
    {
        var grain = CreateGrain(StateWithRedirect());
        RequestContext.Set(MarkerKey, LogicalTreeId);

        StaleTreeRoutingException? caught = null;
        try
        {
            await grain.GetAsync("k");
        }
        catch (StaleTreeRoutingException ex)
        {
            caught = ex;
        }

        Assert.That(caught, Is.Not.Null);
        Assert.That(caught!.LogicalTreeId, Is.EqualTo(LogicalTreeId));
        Assert.That(caught.StalePhysicalTreeId, Is.EqualTo(PhysicalTreeId));
        Assert.That(caught.DestinationPhysicalTreeId, Is.EqualTo(DestTreeId));
    }

    [Test]
    public void Read_does_not_throw_for_direct_physical_access()
    {
        var grain = CreateGrain(StateWithRedirect());
        // Addressed by the retained tree's own physical id (revert / diagnostics).
        RequestContext.Set(MarkerKey, PhysicalTreeId);

        Assert.That(async () => await grain.GetAsync("k"), Throws.Nothing);
    }

    [Test]
    public void Read_does_not_throw_when_marker_absent_maintenance_path()
    {
        var grain = CreateGrain(StateWithRedirect());
        // No routed-logical marker: maintenance firing directly on the shard.
        Assert.That(async () => await grain.GetAsync("k"), Throws.Nothing);
    }

    [Test]
    public void Read_does_not_throw_when_no_redirect_installed()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        RequestContext.Set(MarkerKey, LogicalTreeId);

        Assert.That(async () => await grain.GetAsync("k"), Throws.Nothing);
    }

    [Test]
    public void Read_throws_when_never_aliased_logical_equals_physical()
    {
        // Degenerate case: the tree was never aliased, so the retained physical
        // id equals the logical name. Logical-alias traffic stamps the marker
        // with that shared name and must still self-heal.
        var grain = CreateGrain(StateWithRedirect(
            destinationPhysicalTreeId: $"{PhysicalTreeId}-bkprestore-abc",
            logicalTreeId: PhysicalTreeId));
        RequestContext.Set(MarkerKey, PhysicalTreeId);

        Assert.That(async () => await grain.GetAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    [Test]
    public void Write_throws_StaleTreeRoutingException_when_logical_alias_routed()
    {
        var grain = CreateGrain(StateWithRedirect());
        RequestContext.Set(MarkerKey, LogicalTreeId);

        Assert.That(async () => await grain.SetAsync("k", [1]),
            Throws.InstanceOf<StaleTreeRoutingException>());
    }

    // ========================================================================
    // MarkRetainedRedirectAsync
    // ========================================================================

    [Test]
    public void MarkRetainedRedirectAsync_throws_when_destination_is_null()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        Assert.That(async () => await grain.MarkRetainedRedirectAsync(null!, OperationId, LogicalTreeId),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MarkRetainedRedirectAsync_throws_when_operationId_is_empty()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        Assert.That(async () => await grain.MarkRetainedRedirectAsync(DestTreeId, "", LogicalTreeId),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void MarkRetainedRedirectAsync_throws_when_logicalTreeId_is_null()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        Assert.That(async () => await grain.MarkRetainedRedirectAsync(DestTreeId, OperationId, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MarkRetainedRedirectAsync_throws_when_destination_equals_source()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        Assert.That(async () => await grain.MarkRetainedRedirectAsync(PhysicalTreeId, OperationId, LogicalTreeId),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MarkRetainedRedirectAsync_installs_redirect_state()
    {
        var state = new FakePersistentState<ShardRootState>();
        var grain = CreateGrain(state);

        await grain.MarkRetainedRedirectAsync(DestTreeId, OperationId, LogicalTreeId);

        var rr = state.State.RetainedRedirect;
        Assert.That(rr, Is.Not.Null);
        Assert.That(rr!.DestinationPhysicalTreeId, Is.EqualTo(DestTreeId));
        Assert.That(rr.OperationId, Is.EqualTo(OperationId));
        Assert.That(rr.LogicalTreeId, Is.EqualTo(LogicalTreeId));
        Assert.That(state.WriteCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task MarkRetainedRedirectAsync_is_idempotent_for_matching_operation()
    {
        var state = new FakePersistentState<ShardRootState>();
        var grain = CreateGrain(state);

        await grain.MarkRetainedRedirectAsync(DestTreeId, OperationId, LogicalTreeId);
        var writesAfterFirst = state.WriteCount;
        await grain.MarkRetainedRedirectAsync(DestTreeId, OperationId, LogicalTreeId);

        Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst),
            "A matching re-mark must not persist again.");
    }

    [Test]
    public async Task MarkRetainedRedirectAsync_overwrites_under_a_newer_operation()
    {
        var state = StateWithRedirect();
        var grain = CreateGrain(state);

        await grain.MarkRetainedRedirectAsync("newer-dest", "restore-op-2", LogicalTreeId);

        Assert.That(state.State.RetainedRedirect!.OperationId, Is.EqualTo("restore-op-2"));
        Assert.That(state.State.RetainedRedirect.DestinationPhysicalTreeId, Is.EqualTo("newer-dest"));
    }

    // ========================================================================
    // ClearRetainedRedirectAsync
    // ========================================================================

    [Test]
    public void ClearRetainedRedirectAsync_throws_when_operationId_is_empty()
    {
        var grain = CreateGrain(new FakePersistentState<ShardRootState>());
        Assert.That(async () => await grain.ClearRetainedRedirectAsync(""),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ClearRetainedRedirectAsync_clears_matching_redirect()
    {
        var state = StateWithRedirect();
        var grain = CreateGrain(state);

        await grain.ClearRetainedRedirectAsync(OperationId);

        Assert.That(state.State.RetainedRedirect, Is.Null);
    }

    [Test]
    public async Task ClearRetainedRedirectAsync_is_idempotent_when_no_redirect()
    {
        var state = new FakePersistentState<ShardRootState>();
        var grain = CreateGrain(state);

        await grain.ClearRetainedRedirectAsync(OperationId);

        Assert.That(state.State.RetainedRedirect, Is.Null);
    }

    [Test]
    public void ClearRetainedRedirectAsync_refuses_different_operationId()
    {
        var grain = CreateGrain(StateWithRedirect());
        Assert.That(async () => await grain.ClearRetainedRedirectAsync("some-other-op"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task Mark_then_Clear_round_trips_the_gate()
    {
        var state = new FakePersistentState<ShardRootState>();
        var grain = CreateGrain(state);
        RequestContext.Set(MarkerKey, LogicalTreeId);

        await grain.MarkRetainedRedirectAsync(DestTreeId, OperationId, LogicalTreeId);
        Assert.That(async () => await grain.GetAsync("k"),
            Throws.InstanceOf<StaleTreeRoutingException>());

        await grain.ClearRetainedRedirectAsync(OperationId);
        Assert.That(async () => await grain.GetAsync("k"), Throws.Nothing);
    }
}
