using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detector for #5999: the replay-demand withdrawal runs after the replay
/// permit is released, so even a withdrawal that throws can never strand the
/// permit.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task An_activation_whose_replay_demand_withdrawal_throws_still_returns_its_permit()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var held = false;
        SemaphoreSlim? gate = null;
        try
        {
            var (first, _, _, _) = CreateGrainWithMaterialiser(
                BuildCoordinator(head: 1, new CommitLogSliceEntry(0, BuildCommittedSet("a", [1]))),
                persistedCheckpoint: -1,
                maxConcurrentReplays: 1);
            await ActivateAsync(first);
            gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
            Assert.That(gate.CurrentCount, Is.EqualTo(1), "precondition: the gate holds one permit");

            // The second activation finds no free permit, so it declares replay
            // demand before it queues, and withdraws it once its replay ends.
            await gate.WaitAsync();
            held = true;
            var (second, _, _, _) = CreateGrainWithMaterialiser(
                BuildCoordinator(head: 1, new CommitLogSliceEntry(0, BuildCommittedSet("b", [1]))),
                persistedCheckpoint: -1,
                maxConcurrentReplays: 1);
            var activation = ActivateAsync(second);
            var deadline = Stopwatch.StartNew();
            while (BPlusLeafGrain.QueuedReplayPermitWaitersForTest == 0 && deadline.Elapsed < TimeSpan.FromSeconds(5))
                await Task.Delay(10);
            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(1), "precondition: the second activation is queued");

            BPlusLeafGrain.ReplayDemandWithdrawalFaultForTest = new InvalidOperationException("withdrawal failed");
            held = false;
            gate.Release();

            Assert.DoesNotThrowAsync(async () => await activation.WaitAsync(TimeSpan.FromSeconds(10)),
                "the withdrawal is advisory: its fault neither fails the activation nor escapes the replay");
            Assert.Multiple(() =>
            {
                Assert.That(gate.CurrentCount, Is.EqualTo(1), "the permit is back in the gate although the withdrawal threw");
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero, "nothing is left queued");
                Assert.That(BPlusLeafGrain.ReplayPermitCensusCountForTest, Is.Zero, "no holder is left in the census");
            });
        }
        finally
        {
            BPlusLeafGrain.ReplayDemandWithdrawalFaultForTest = null;
            if (held)
                gate?.Release();
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
