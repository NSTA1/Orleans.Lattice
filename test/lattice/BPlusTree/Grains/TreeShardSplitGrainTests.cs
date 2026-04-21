using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class TreeShardSplitGrainTests
{
    private const string TreeId = "split-test-tree";

    private static (TreeShardSplitGrain grain,
                    FakePersistentState<TreeShardSplitState> state,
                    IGrainFactory grainFactory,
                    ILatticeRegistry registry,
                    IShardRootGrain sourceShard,
                    IShardRootGrain targetShard) CreateGrain(
        int virtualShardCount = 16,
        int physicalShardCount = 2,
        int sourceShardIndex = 0,
        ShardMap? existingMap = null,
        FakePersistentState<TreeShardSplitState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        // Coordinator key format: {treeId}/{sourceShardIndex}.
        context.GrainId.Returns(GrainId.Create("split", $"{TreeId}/{sourceShardIndex}"));

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
        });

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(TreeId);
        registry.GetShardMapAsync(TreeId).Returns(existingMap
            ?? ShardMap.CreateDefault(virtualShardCount, physicalShardCount));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = physicalShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        // Default allocation hands back currentMaxFromMap + 1 — matches the
        // previous max-existing+1 behavior for unit-test expectations.
        registry.AllocateNextShardIndexAsync(TreeId, Arg.Any<int>())
            .Returns(ci => Task.FromResult(((int)ci[1]) + 1));

        // Stub source + target shards.
        var sourceShard = Substitute.For<IShardRootGrain>();
        var targetShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(ci =>
        {
            var key = (string)ci[0];
            var idx = int.Parse(key[(key.LastIndexOf('/') + 1)..]);
            return idx == sourceShardIndex ? sourceShard : targetShard;
        });
        sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));

        var state = existingState ?? new FakePersistentState<TreeShardSplitState>();
        var grain = new TreeShardSplitGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeShardSplitGrain>(), state);
        return (grain, state, grainFactory, registry, sourceShard, targetShard);
    }

    // --- SplitAsync validation ---

    [Test]
    public void SplitAsync_throws_on_negative_source_shard_index()
    {
        var (grain, _, _, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => grain.SplitAsync(-1));
    }

    [Test]
    public async Task SplitAsync_idempotent_when_in_progress_for_same_source()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.SourceShardIndex = 0;

        await grain.SplitAsync(0);

        Assert.That(state.State.SourceShardIndex, Is.EqualTo(0));
    }

    [Test]
    public void SplitAsync_throws_when_in_progress_for_different_source()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.SourceShardIndex = 1;

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.SplitAsync(0));
    }

    // --- InitiateSplitStateAsync ---

    [Test]
    public async Task InitiateSplit_persists_intent_and_calls_source_BeginSplit()
    {
        var (grain, state, _, _, source, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2, sourceShardIndex: 0);

        await grain.InitiateSplitStateAsync(0);

        Assert.That(state.State.InProgress, Is.True);
        Assert.That(state.State.SourceShardIndex, Is.EqualTo(0));
        Assert.That(state.State.TargetShardIndex, Is.EqualTo(2),
            "Default map (0,1) — new physical shard index should be max+1 = 2.");
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Drain),
            "After successful BeginSplit on source, phase advances to Drain.");
        Assert.That(state.State.MovedSlots.Count, Is.EqualTo(4),
            "Source owns slots 0,2,4,6,8,10,12,14 (8 slots) — upper half is 4 slots.");
        Assert.That(state.State.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(state.State.OriginalShardMap, Is.Not.Null);

        await source.Received(1).BeginSplitAsync(2, Arg.Any<int[]>(), 16);
    }

    [Test]
    public void InitiateSplit_throws_when_source_owns_fewer_than_two_slots()
    {
        // Custom map where shard 0 owns only one virtual slot.
        var slots = new int[] { 0, 1, 1, 1 };
        var map = new ShardMap { Slots = slots };
        var (grain, _, _, _, _, _) = CreateGrain(virtualShardCount: 4, physicalShardCount: 2, existingMap: map);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.InitiateSplitStateAsync(0));
    }

    // --- DrainAsync ---

    [Test]
    public async Task Drain_advances_phase_to_swap()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Drain;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        await grain.DrainAsync();

        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Swap));
    }

    // --- SwapAsync ---

    [Test]
    public async Task Swap_persists_new_shard_map_remapping_moved_slots()
    {
        var (grain, state, _, registry, _, _) = CreateGrain();
        var original = ShardMap.CreateDefault(8, 2);
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = original;

        await grain.SwapAsync();

        await registry.Received(1).SetShardMapAsync(TreeId, Arg.Is<ShardMap>(m =>
            m.Slots[2] == 2 && m.Slots[4] == 2 && m.Slots[6] == 2 &&
            m.Slots[0] == 0 && m.Slots[1] == 1));
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Reject));
    }

    // --- EnterRejectAsync ---

    [Test]
    public async Task EnterReject_calls_source_EnterRejectPhase_and_advances()
    {
        var (grain, state, _, _, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Reject;
        state.State.SourceShardIndex = 0;

        await grain.EnterRejectAsync();

        await source.Received(1).EnterRejectPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Complete));
    }

    // --- FinaliseAsync ---

    [Test]
    public async Task Finalise_calls_source_CompleteSplit_and_marks_complete()
    {
        var (grain, state, _, _, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Complete;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        await grain.FinaliseAsync();

        await source.Received(1).CompleteSplitAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None));
    }

    // --- IsCompleteAsync ---

    [Test]
    public async Task IsCompleteAsync_returns_true_when_no_split_in_progress()
    {
        var (grain, _, _, _, _, _) = CreateGrain();
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_during_in_progress_split()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        Assert.That(await grain.IsCompleteAsync(), Is.False);
    }

    // --- RunSplitPassAsync ---

    [Test]
    public async Task RunSplitPass_noop_when_not_in_progress()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        await grain.RunSplitPassAsync();
        await registry.DidNotReceive().SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
        await source.DidNotReceive().EnterRejectPhaseAsync();
    }

    [Test]
    public async Task RunSplitPass_drives_state_machine_to_completion()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Drain;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        await grain.RunSplitPassAsync();

        await registry.Received(1).SetShardMapAsync(TreeId, Arg.Any<ShardMap>());
        await source.Received(1).EnterRejectPhaseAsync();
        await source.Received(1).CompleteSplitAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task RunSplitPass_resumes_from_BeginShadowWrite_after_crash()
    {
        var (grain, state, _, _, source, _) = CreateGrain();
        // Simulate crash between persisting BeginShadowWrite intent and the call.
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        await grain.RunSplitPassAsync();

        // Idempotent re-issue of BeginSplit on source.
        await source.Received().BeginSplitAsync(2, Arg.Any<int[]>(), 16);
        Assert.That(state.State.InProgress, Is.False, "After resume, full pass completes.");
    }
}
