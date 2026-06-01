using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Ordering-invariant tests for <c>TreeShardSplitGrain.SwapAsync</c>.
/// The source shard root must enter the reject phase <em>before</em> the
/// registry's shard-map flip commits - otherwise a stale-routing reader
/// can land on the source for a moved-slot key while the source's
/// hot-path reject gate is still inactive, surfacing a pre-saga value.
/// </summary>
public partial class TreeShardSplitGrainTests
{
    [Test]
    public async Task Swap_enters_reject_phase_before_setting_shard_map()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        var original = ShardMap.CreateDefault(8, 2);
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = original;

        await grain.SwapAsync();

        // Ordering invariant: EnterRejectPhaseAsync MUST be observed BEFORE
        // SetShardMapAsync. NSubstitute's Received.InOrder verifies the call
        // sequence across distinct substitutes.
        Received.InOrder(() =>
        {
            source.EnterRejectPhaseAsync();
            registry.SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
        });
    }

    [Test]
    public async Task Swap_calls_source_enter_reject_phase_exactly_once()
    {
        var (grain, state, _, _, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = ShardMap.CreateDefault(8, 2);

        await grain.SwapAsync();

        // The reorder fix adds an EnterRejectPhaseAsync call inside SwapAsync;
        // the downstream EnterRejectAsync coordinator phase calls it again,
        // and the source-side method is documented as idempotent. From
        // SwapAsync's own perspective the call count is exactly 1.
        await source.Received(1).EnterRejectPhaseAsync();
    }

    [Test]
    public async Task Swap_runs_final_drain_after_reject_and_before_shard_map_flip()
    {
        var (grain, state, grainFactory, registry, source, _) = CreateGrain();
        var original = ShardMap.CreateDefault(8, 2);
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = original;

        // The final drain scans the source leaf chain via GetLeftmostLeafIdAsync.
        // A non-null leaf id with an empty next chain makes the drain do a single
        // bounded pass so the ordering can be observed without a full leaf graph.
        var leafId = GrainId.Create("bplusleaf", $"{TreeId}/0/leaf-0");
        source.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));

        var leaf = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
        leaf.GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(Task.FromResult(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>(),
                Version = new VersionVector(),
            }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        await grain.SwapAsync();

        // Final-drain invariant: the source must freeze its writes
        // (EnterRejectPhaseAsync) and the destination must be re-synchronised
        // by the final drain (GetLeftmostLeafIdAsync kicks the leaf-chain scan)
        // BEFORE the registry map flips. If the drain ran after the flip, a
        // reader could route to a destination that has not yet received the
        // source's final committed values.
        Received.InOrder(() =>
        {
            source.EnterRejectPhaseAsync();
            source.GetLeftmostLeafIdAsync();
            registry.SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
        });
    }
}
