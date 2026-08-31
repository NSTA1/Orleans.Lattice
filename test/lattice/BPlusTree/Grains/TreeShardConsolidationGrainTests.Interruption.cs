using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Interruption and resumability tests for online shard consolidation.
/// <para>
/// A fold that could strand a tree half-merged would be worse than no fold at
/// all, so the operation's contract is that an interruption at <em>any</em>
/// step boundary leaves a state a later attempt completes or safely abandons.
/// Each test here kills the coordinator at one boundary - modelled as a fresh
/// activation over the same persisted state, which is exactly what Orleans
/// gives a grain after a silo restart - and asserts the fold still lands, the
/// routing map still routes every virtual slot, and the coordinator is
/// consolidatable again afterwards.
/// </para>
/// <para>
/// The write-failure tests cover the other half: a persist that throws must
/// leave the activation's in-memory view agreeing with storage, or a retry
/// from that same activation would skip work the persisted state still owes.
/// </para>
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    private static IReadOnlyList<Dictionary<string, LwwValue<byte[]>>> DonorChain()
        => [Entries("a", "b"), Entries("c", "d"), Entries("e")];

    /// <summary>
    /// Models a silo restart: a brand-new coordinator activation over the same
    /// persisted state and an equivalent donor leaf chain.
    /// </summary>
    private static Harness Reactivate(Harness previous)
        => CreateGrain(existingState: previous.State, leafEntries: DonorChain());

    private static void AssertFoldLanded(Harness h)
    {
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.False, "The fold must reach a terminal state.");
            Assert.That(h.State.State.Complete, Is.True);
            Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.None));
            Assert.That(h.PersistedMap!.Slots.Length, Is.EqualTo(VirtualShardCount));
            Assert.That(h.PersistedMap!.Slots, Has.All.EqualTo(0),
                "Every virtual slot must route to the survivor once the fold lands.");
            Assert.That(h.PersistedMap!.GetPhysicalShardIndices(), Has.Count.EqualTo(1),
                "The physical shard count must actually come down - that is the point of a fold.");
        });
    }

    // --- Resume from each persisted phase boundary ---

    [Test]
    public async Task Interruption_after_intent_persist_resumes_and_completes()
    {
        var first = CreateGrain(leafEntries: DonorChain());
        await first.Grain.StartAsync(0);
        // Rewind to the boundary a crash between the intent persist and the
        // donor call would leave behind.
        first.State.State.Phase = ShardConsolidationPhase.BeginShadowWrite;

        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(resumed);
        await resumed.Donor.Received().BeginSplitAsync(0, Arg.Any<int[]>(), VirtualShardCount);
    }

    [Test]
    public async Task Interruption_mid_drain_resumes_from_the_persisted_cursor()
    {
        var options = new LatticeOptions { ConsolidationDrainLeavesPerPass = 1 };
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain(),
            options: options);

        await first.Grain.DrainAsync();
        Assert.That(first.State.State.DrainCursorLeafId, Is.Not.Null);
        Assert.That(first.State.State.LeavesScanned, Is.EqualTo(1));

        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(resumed);
        Assert.That(resumed.State.State.LeavesScanned, Is.GreaterThanOrEqualTo(3),
            "The resumed sweep must still visit the leaves the interrupted pass had not reached.");
    }

    [Test]
    public async Task Interruption_at_the_drain_to_swap_boundary_resumes_and_completes()
    {
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain());
        await first.Grain.DrainAsync();
        Assert.That(first.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Swap));

        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(resumed);
    }

    [Test]
    public async Task Interruption_inside_the_swap_re_runs_it_idempotently()
    {
        // A crash anywhere inside the freeze-and-flip step is recovered by
        // simply re-running it: every action in it is idempotent.
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Swap),
            leafEntries: DonorChain());
        await first.Grain.SwapAsync();

        // Rewind the persisted phase as though the post-swap phase write never
        // reached storage, and re-drive from a fresh activation.
        first.State.State.Phase = ShardConsolidationPhase.Swap;
        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(resumed);
        await resumed.Survivor.Received().ReclaimSlotsAsync(Arg.Any<int[]>(), VirtualShardCount);
    }

    [Test]
    public async Task Interruption_after_the_flip_still_retires_the_donor()
    {
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Reject),
            leafEntries: DonorChain());

        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        await resumed.Donor.Received(1).CompleteSplitAsync();
        Assert.That(resumed.State.State.Complete, Is.True,
            "A fold interrupted after the flip must finish; abandoning would strand the donor.");
    }

    [Test]
    public async Task Interruption_at_the_complete_boundary_resumes_and_completes()
    {
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Complete),
            leafEntries: DonorChain());

        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();

        Assert.That(resumed.State.State.Complete, Is.True);
        Assert.That(resumed.State.State.InProgress, Is.False);
        await resumed.Donor.Received().CompleteSplitAsync();
    }

    [Test]
    public async Task A_tree_is_consolidatable_again_after_an_interrupted_fold_completes()
    {
        var first = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain());
        var resumed = Reactivate(first);
        await resumed.Grain.RunConsolidationPassAsync();
        AssertFoldLanded(resumed);

        // Re-issuing the same fold on the healed tree must be a clean no-op
        // rather than a fault - the driver re-runs its whole plan every sweep.
        await resumed.Grain.StartAsync(0);

        Assert.That(resumed.State.State.InProgress, Is.False);
        Assert.That(await resumed.Grain.IsIdleAsync(), Is.True);
    }

    [Test]
    public async Task A_tree_is_consolidatable_again_after_an_abandoned_fold()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain());

        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();
        Assert.That(h.State.State.Cancelled, Is.True);
        Assert.That(await h.Grain.IsIdleAsync(), Is.True);

        // The abandoned fold left routing untouched, so a fresh fold must plan
        // the very same slot set and run to completion.
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(h);
        Assert.That(h.State.State.Cancelled, Is.False,
            "A completed fold must clear the cancelled marker of the abandoned one.");
    }

    [Test]
    public async Task Every_phase_boundary_leaves_a_routable_map()
    {
        // Walk the whole fold one phase at a time and assert the routing
        // invariant that matters most after each: no key is ever unreachable,
        // because every virtual slot always routes to some physical shard.
        var h = CreateGrain(leafEntries: DonorChain());
        await h.Grain.StartAsync(0);

        var guard = 0;
        while (h.State.State.InProgress && guard++ < 16)
        {
            await h.Grain.ProcessNextPhaseAsync();

            var map = h.PersistedMap!;
            Assert.That(map.Slots.Length, Is.EqualTo(VirtualShardCount));
            foreach (var target in map.Slots)
            {
                Assert.That(target, Is.GreaterThanOrEqualTo(0),
                    $"After phase {h.State.State.Phase} a virtual slot lost its owner.");
            }
        }

        AssertFoldLanded(h);
    }

    // --- Persist failures ---

    [Test]
    public async Task A_failed_intent_persist_leaves_the_coordinator_startable_again()
    {
        var h = CreateGrain(leafEntries: DonorChain());
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(0));
        Assert.That(h.State.State.InProgress, Is.False,
            "An in-memory 'in progress' over a storage row that says otherwise would "
            + "short-circuit every retry from this activation.");

        await h.Grain.StartAsync(0);

        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain));
    }

    [Test]
    public async Task A_failed_phase_persist_reverts_the_in_memory_phase()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain());
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.DrainAsync());
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Drain),
            "A phase the storage row never accepted must not be believed in memory.");

        await h.Grain.RunConsolidationPassAsync();
        AssertFoldLanded(h);
    }

    [Test]
    public async Task A_failed_finalise_persist_leaves_the_fold_retryable()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Complete),
            leafEntries: DonorChain());
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.FinaliseAsync());
        Assert.That(h.State.State.InProgress, Is.True,
            "A coordinator that believes it finished while storage says otherwise would never retry.");
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Complete));

        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Complete, Is.True);
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_failed_abandon_persist_leaves_the_cancel_retryable()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: DonorChain());
        await h.Grain.CancelAsync();
        h.State.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.AbandonAsync());
        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.CancelRequested, Is.True,
            "A cancel the storage row never accepted must survive in memory so the retry honours it.");

        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Cancelled, Is.True);
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task A_phase_that_throws_is_swallowed_by_the_timer_pump_and_retried()
    {
        // The background pump must not die on a transient cross-grain failure,
        // or a fold would stall forever after one blip.
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Swap),
            leafEntries: DonorChain());
        h.Donor.EnterRejectPhaseAsync().Returns(Task.FromException(new TimeoutException("transient")));

        await h.Grain.ProcessNextPhaseAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Swap),
            "A failed phase must not advance.");
        Assert.That(h.State.State.InProgress, Is.True);

        h.Donor.EnterRejectPhaseAsync().Returns(Task.CompletedTask);
        await h.Grain.RunConsolidationPassAsync();

        AssertFoldLanded(h);
    }
}
