using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The tree registry's leaves replay outside the per-silo replay gate (#6166). Every gated
/// replay of a user tree reads its shard map from the registry with its permit lent, and the
/// registry reads that map through its own leaves. At 6M keys every permit holder of every
/// silo sat with its permit lent on that lookup while a registry leaf, gated behind them,
/// could not activate.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task A_registry_leaf_activates_while_every_replay_permit_is_taken()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var gate = (SemaphoreSlim?)null;
        var taken = 0;
        try
        {
            // A user-tree leaf sizes the gate, then every permit is taken.
            var (sizer, _, _, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                treeId: UniqueCoverageRepairTreeId("registry-gate-sizer"),
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: new GrowingWal().Coordinator);
            await LeafActivationHarness.ActivateAsync(sizer, CancellationToken.None);
            gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
            while (gate.Wait(0))
            {
                taken++;
            }

            var (registryLeaf, _, _, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                treeId: LatticeConstants.RegistryTreeId,
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: new GrowingWal().Coordinator);
            var (userLeaf, _, _, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                treeId: UniqueCoverageRepairTreeId("registry-gate-user"),
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: new GrowingWal().Coordinator);

            using var userCts = new CancellationTokenSource();
            var registryActivation = LeafActivationHarness.ActivateAsync(registryLeaf, CancellationToken.None);
            var userActivation = LeafActivationHarness.ActivateAsync(userLeaf, userCts.Token);
            var registryActivated = await Task.WhenAny(registryActivation, Task.Delay(TimeSpan.FromSeconds(10))) == registryActivation;
            var userWaited = !userActivation.IsCompleted;
            userCts.Cancel();
            try
            {
                await userActivation;
            }
            catch (OperationCanceledException)
            {
            }

            Assert.Multiple(() =>
            {
                Assert.That(taken, Is.EqualTo(TimerAdmissionCeiling), "precondition: every permit is taken");
                Assert.That(registryActivated, Is.True,
                    "a registry leaf waited on the replay gate, whose holders wait on the registry");
                Assert.That(userWaited, Is.True, "a user-tree leaf bypassed the replay gate");
            });
            await registryActivation;
        }
        finally
        {
            if (gate is not null && taken > 0)
            {
                gate.Release(taken);
            }

            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
