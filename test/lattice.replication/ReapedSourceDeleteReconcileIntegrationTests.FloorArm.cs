using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4549: a bootstrap drop floor's install bumps the tree's floor epoch,
/// raises it in the tree registry and arms every shard root with it, so a write
/// admitted before the install cannot land after the reconcile scan. The two
/// halves cover different shard roots. One not yet activated, or not yet
/// stamped, reads the epoch from the registry before its first stamped write.
/// One that already handled a stamped write loaded the epoch then and never
/// re-reads it, so for it the arm is the only guard. This fixture covers that
/// second case: the straddler's own shard root handles a stamped write before
/// the install. The first case's own guard is the registry raise: a shard root
/// that reactivates after the install has lost the epoch the arm gave it and
/// reads it from the registry, which the second test exercises.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    [Test]
    public async Task A_shard_root_that_already_loaded_its_floor_epoch_refuses_a_straddler_because_the_install_arms_it()
    {
        const string tree = "rsdr-4549-armed-loaded";
        const string key = "third-straddler-loaded";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        // A stamped replicated write to the straddler's key routes to its shard
        // root, which loads the tree's floor epoch from the registry now and
        // keeps it from then on.
        Assert.That((await ApplyFromSiteCAsync(_siteB, tree, key, PastHlc(9))).Applied, Is.True,
            "precondition: a stamped write loads the shard root's floor epoch");

        // C's write reached the source, which deleted the key and reaped it. The
        // copy bound for the receiver is admitted before the floor exists.
        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        var admittedUnder = (await _siteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree)
            .GetAdmissionAsync(SiteCClusterId)).FloorEpoch;

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));

        Exception? refusal = null;
        RequestContext.Clear();
        ReplicationFloorAdmission.Stamp(admittedUnder);
        try
        {
            await _siteB.Client.GetGrain<IReplicationApplyGrain>(tree)
                .ApplySetAsync(key, ThirdValue, written, SiteCClusterId, null, 0);
        }
        catch (Exception ex)
        {
            refusal = ex;
        }
        finally
        {
            RequestContext.Clear();
        }

        Assert.Multiple(async () =>
        {
            Assert.That(refusal, Is.InstanceOf<ReplicationFloorAdmissionStaleException>(),
                "the shard root loaded its floor epoch before the install, so only the install's arm can make it refuse the straddler");
            Assert.That(await siteB.GetAsync(key), Is.Null, "the straddler must not resurrect the key after the scan");
        });
    }

    [Test]
    public async Task A_shard_root_reactivated_after_the_install_refuses_a_straddler_because_the_install_raised_the_registry_epoch()
    {
        const string tree = "rsdr-4549-armed-reactivated";
        const string key = "third-straddler-reactivated";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync("anchor", new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        var written = PastHlc(5);
        Assert.That((await ApplyFromSiteCAsync(_siteA, tree, key, written)).Applied, Is.True, "precondition");
        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        var admittedUnder = (await _siteB.Client.GetGrain<IReplicationHighWaterMarkGrain>(tree)
            .GetAdmissionAsync(SiteCClusterId)).FloorEpoch;

        await RebootstrapSiteBAsync(tree, SiteCFrontier(HybridLogicalClock.Tick(written)));

        // The straddler's shard root deactivates after the install, losing the
        // epoch the arm gave it in memory; its next activation reads the epoch
        // from the registry.
        var registry = _siteB.Client.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(tree);
        var shardMap = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        await _siteB.Client.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardMap.Resolve(key)}")
            .AsReference<IGrainManagementExtension>()
            .DeactivateOnIdle();
        await Task.Delay(500);

        Exception? refusal = null;
        RequestContext.Clear();
        ReplicationFloorAdmission.Stamp(admittedUnder);
        try
        {
            await _siteB.Client.GetGrain<IReplicationApplyGrain>(tree)
                .ApplySetAsync(key, ThirdValue, written, SiteCClusterId, null, 0);
        }
        catch (Exception ex)
        {
            refusal = ex;
        }
        finally
        {
            RequestContext.Clear();
        }

        Assert.Multiple(async () =>
        {
            Assert.That(refusal, Is.InstanceOf<ReplicationFloorAdmissionStaleException>(),
                "a shard root reactivated after the install no longer holds the arm's epoch, so only the registry raise can make it refuse the straddler");
            Assert.That(await siteB.GetAsync(key), Is.Null, "the straddler must not resurrect the key after the scan");
        });
    }
}