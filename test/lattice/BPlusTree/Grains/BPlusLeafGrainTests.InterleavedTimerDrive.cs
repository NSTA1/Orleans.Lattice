using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The coverage-lag timer hands its starvation drive to a one-shot interleaving
/// grain timer instead of awaiting it on its own non-interleaved turn, where a
/// drive running for minutes blocked every non-interleaving call on the leaf
/// (the Layer 3 <c>set-point-mv</c> wedge: one tick held a leaf for 294 s).
/// <para>
/// These fixtures have no grain runtime, so a dispatch falls back to the inline
/// drive; the pending-dispatch state is arranged through a test hook and the
/// real tick is then driven.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_with_a_dispatched_drive_pending_does_not_drive_again()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, state) = await CreateStarvedLeafWithWalWorkAsync();
            grain.SetInterleavedTimerDrivePendingForTest(true);

            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                    "a tick that finds a dispatched drive outstanding must not start a second one: at most "
                    + "one timer drive per activation may be pending or running.");
                Assert.That(grain.InterleavedTimerDrivePendingForTest, Is.True,
                    "the tick must leave the outstanding dispatch to clear itself when its drive finishes.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_without_a_grain_runtime_drives_inline_and_leaves_nothing_pending()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, state) = await CreateStarvedLeafWithWalWorkAsync();

            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                    "precondition for the pending-dispatch fixture: with no dispatch outstanding the same "
                    + "tick must drive the starved leaf, or that fixture's unchanged checkpoint proves nothing.");
                Assert.That(grain.InterleavedTimerDrivePendingForTest, Is.False,
                    "a failed timer registration must not leave a dispatch marked pending, or every later "
                    + "tick would skip the drive for the life of the activation.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// A leaf with rows, no durable checkpoint and three WAL entries arriving
    /// after activation, so every coverage-lag tick routes it to the drive
    /// (issue #3300) and only a drive can advance its checkpoint.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State)>
        CreateStarvedLeafWithWalWorkAsync()
    {
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10),
            maxConcurrentReplays: TimerAdmissionCeiling);
        await ActivateAsync(grain);
        SeedRow(grain);
        wal.GrowTo(3);
        return (grain, state);
    }
}
