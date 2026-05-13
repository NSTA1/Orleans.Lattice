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

// Class B regression tests for TreeShardSplitGrain. Every phase-transitioning
// method assigns to state.State BEFORE awaiting WriteStateAsync, and the grain
// has TWO idempotency-guarded short-circuits that observe the dirtied
// in-memory state:
//   * `if (state.State.InProgress)` in SplitAsync (L115) - HIGH for the
//     InitiateSplit first-write site.
//   * `if (!state.State.InProgress) return;` in RunSplitPassAsync (L186) and
//     ProcessNextPhaseAsync (L226) - HIGH for the Finalise terminal write.
// The intermediate phase-transition writes are MEDIUM but bundled per the
// same-grain rule so the whole grain's invariant ("in-memory state never
// reflects mutations that disk hasn't accepted") is restored in one cycle.
public partial class TreeShardSplitGrainTests
{
    [Test]
    public void InitiateSplit_reverts_in_memory_state_when_first_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2, sourceShardIndex: 0);
        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.InitiateSplitStateAsync(0),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // Without revert, every one of these 8 fields stays dirty while disk
        // is empty - and `if (state.State.InProgress)` in SplitAsync then
        // short-circuits any retry from the same activation.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.OperationId, Is.Null);
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None));
            Assert.That(state.State.SourceShardIndex, Is.EqualTo(0));
            Assert.That(state.State.TargetShardIndex, Is.EqualTo(0));
            Assert.That(state.State.MovedSlots, Is.Empty);
            Assert.That(state.State.OriginalShardMap, Is.Null);
        });
    }

    [Test]
    public async Task RunSplitPass_reverts_phase_when_BeginShadowWrite_resume_WriteStateAsync_throws()
    {
        // Same shape as the second WriteStateAsync inside InitiateSplit
        // (L180 pre-fix): persist intent → call source.BeginSplit → set
        // Phase=Drain → persist. If the persist throws, in-memory has Drain
        // while disk has BeginShadowWrite. RunSplitPass's resume branch is
        // the cleanest place to exercise the L201 site without relying on
        // multi-write ThrowOnWrite semantics.
        var (grain, state, _, _, source, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RunSplitPassAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.BeginShadowWrite),
            "in-memory Phase must not advance to Drain when the persist failed");
        await source.Received(1).BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public void Drain_reverts_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Drain;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [1, 3];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.DrainAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Drain),
            "in-memory Phase must not advance to Swap when the persist failed");
    }

    [Test]
    public void Swap_reverts_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = ShardMap.CreateDefault(8, 2);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.SwapAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // The registry SetShardMapAsync side effect deliberately not
        // reverted (cross-grain, idempotent on re-apply). Only the
        // in-memory Phase mutation needs to roll back.
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Swap),
            "in-memory Phase must not advance to Reject when the persist failed");
    }

    [Test]
    public void EnterReject_reverts_phase_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Reject;
        state.State.SourceShardIndex = 0;

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.EnterRejectAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Reject),
            "in-memory Phase must not advance to Complete when the persist failed");
    }

    [Test]
    public void Finalise_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ShardSplitPhase.Complete;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [];
        state.State.OriginalShardMap = ShardMap.CreateDefault(16, 2);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.FinaliseAsync(),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // Without the revert, in-memory has InProgress=false (and the
        // `if (!state.State.InProgress) return;` guard in RunSplitPass
        // short-circuits any retry from the same activation), Complete=true,
        // and Phase=None - while disk still has InProgress=true,
        // Phase=Complete. The activation thinks the split is done; on
        // reactivation disk says it isn't.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True,
                "in-memory InProgress must stay true when the terminal persist failed");
            Assert.That(state.State.Complete, Is.False,
                "in-memory Complete must stay false when the terminal persist failed");
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Complete),
                "in-memory Phase must stay at Complete when the terminal persist failed");
        });
    }
}
