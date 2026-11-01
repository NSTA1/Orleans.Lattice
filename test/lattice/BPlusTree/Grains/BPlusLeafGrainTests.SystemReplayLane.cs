using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The reserved system replay lane (issue #5569), end to end through a real
/// activation.
/// <para>
/// <b>The regime.</b> Every shared permit is held and none is coming back, so
/// the shared gate's <c>no_progress</c> arm refuses arrivals. This is the state
/// a schema migration drove the journey estate into: replays of migrated and
/// derived trees held all four permits for minutes on a slow WAL store, and the
/// <c>sys-membership-groups</c> leaves auth reads on every request were refused
/// with them. Time is injected through the seeded progress stamp, so nothing
/// here sleeps or waits on a clock.
/// </para>
/// <para>
/// Every test perturbs process-wide admission statics, so each restores them
/// and is <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static string UniqueSystemReplayTree() => $"sys-replay-lane-{Guid.NewGuid():N}";

    /// <summary>
    /// Holds every shared permit and seeds a full, non-progressing queue: the
    /// shared pool is wedged. Returns the ceiling so the caller can restore it.
    /// </summary>
    private static async Task<int> WedgeSharedReplayPoolAsync()
    {
        var options = new LatticeOptions();
        var ceiling = await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        for (var i = 0; i < ceiling; i++)
            Assert.That(gate.Wait(0), Is.True, "instrument validation: the shared pool must start idle");
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.Zero, options.WalReplayPermitMaxQueueWait + TimeSpan.FromSeconds(1));
        return ceiling;
    }

    private static void UnwedgeSharedReplayPool(int ceiling)
    {
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        while (gate.CurrentCount < ceiling)
            gate.Release();
        ClearSeededAdmittedWaiters();
    }

    [Test]
    [NonParallelizable]
    public async Task A_system_tree_replay_is_admitted_through_the_reserved_lane_while_the_shared_pool_is_wedged()
    {
        // The defect, stated as a test. Before the lane this activation was
        // judged by the shared gate's arms and refused on no_progress, because
        // the shared permits were held by replays of unrelated trees.
        var ceiling = await WedgeSharedReplayPoolAsync();
        var treeId = UniqueSystemReplayTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                Assert.DoesNotThrowAsync(
                    async () => await LeafActivationHarness.ActivateAsync(
                        (IGrainBase)grain, CancellationToken.None),
                    "a system-tree replay must not be refused because unrelated replays hold the shared pool");
            }

            Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest!.CurrentCount, Is.Zero,
                "the system replay must not have taken a shared permit: the pool stays wedged");
        }
        finally
        {
            UnwedgeSharedReplayPool(ceiling);
        }

        Assert.That(refusals, Is.Empty, "no admission refusal may be recorded for the system tree");
    }
}
