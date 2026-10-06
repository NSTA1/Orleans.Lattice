using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4742: decommissioning the source abandons its cross-tree barriers
/// without deciding them. An imported participant whose read fence one of those
/// barriers was holding must not lift its fence on the strength of the abandon:
/// its sibling's staged prepare is discarded by the same decommission, so a
/// lifted fence would serve the operation split - the imported tree post-saga,
/// its sibling pre-saga - permanently.
/// </summary>
public partial class CrossTreeImportBarrierIntegrationTests
{
    private IServiceProvider SiteBServices => ((InProcessSiloHandle)_siteB.Primary).SiloHost.Services;

    [Test]
    public async Task Decommissioning_the_source_keeps_an_imported_participant_fenced_until_it_is_re_added()
    {
        const string treeA = "xtib-decom-a";
        const string treeB = "xtib-decom-b";
        const string operationId = "xtib-decom-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        // Tree B's prepare reached site B and was staged; its terminal never will.
        var stamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        await _siteB.Client.GetGrain<IReplicationApplyGrain>(treeB).ApplyPreparedSetAsync(
            "k", [2], stamp, SiteAClusterId,
            sourceVectorClock: null, expiresAtTicks: 0, Guid.NewGuid(), atomicBatchSize: 0, atomicBatchIndex: 0);

        // Tree A is imported post-saga and arrives at the barrier, which holds its fence.
        await StartBootstrapAsync(treeA);
        var heldPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        Assert.That(heldPhase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff), "precondition: the import is held by the barrier");

        var registry = _siteB.Client.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
        try
        {
            await SiteBServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>()
                .DecommissionPeerAsync(SiteAClusterId, CancellationToken.None);

            var (samples, liftedSamples) = await SampleFenceAsync(treeA, HeldWindow);
            var b = await ReadAsync(treeB, "k");
            Assert.Multiple(() =>
            {
                Assert.That(samples, Is.GreaterThanOrEqualTo(MinimumHeldSamples), "precondition: the window was sampled throughout");
                Assert.That(b, Is.EqualTo((false, (byte[]?)null)), "tree B's discarded prepare leaves it pre-saga");
                Assert.That(liftedSamples, Is.Zero,
                    "an abandon is not a decision: tree A must stay fenced rather than be served post-saga beside tree B pre-saga");
            });

            // The re-add re-drives the held import from a fresh export, which
            // decides the fence; until then nothing else releases it.
            await registry.ClearDecommissionedAsync(SiteAClusterId);
            Assert.That(await Coordinator(treeA).RedriveForReAddedSourceAsync(SiteAClusterId), Is.True,
                "the re-add re-drives the import the decommission held");
            Assert.That(await Coordinator(treeA).RedriveForReAddedSourceAsync(SiteAClusterId), Is.False,
                "a second re-add has nothing left to re-drive");
        }
        finally
        {
            await registry.ClearDecommissionedAsync(SiteAClusterId);
        }
    }
}
