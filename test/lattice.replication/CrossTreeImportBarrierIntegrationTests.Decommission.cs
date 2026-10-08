using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4742: decommissioning the source abandons its cross-tree barriers
/// without deciding them. An imported participant whose shadow publication one
/// of those barriers was holding must not cut over on the strength of the
/// abandon: its sibling's staged prepare is discarded by the same decommission,
/// so a cutover would serve the operation split - the imported tree post-saga,
/// its sibling pre-saga - permanently.
/// </summary>
public partial class CrossTreeImportBarrierIntegrationTests
{
    private IServiceProvider SiteBServices => ((InProcessSiloHandle)_siteB.Primary).SiloHost.Services;

    [Test]
    public async Task Decommissioning_the_source_keeps_an_imported_participant_on_its_original_view_until_re_added()
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

            var (samples, unexpectedSamples) = await SampleOriginalViewAsync(treeA, "k", null, HeldWindow);
            var b = await ReadAsync(treeB, "k");
            Assert.Multiple(() =>
            {
                Assert.That(samples, Is.GreaterThanOrEqualTo(MinimumHeldSamples), "precondition: the window was sampled throughout");
                Assert.That(b, Is.EqualTo((false, (byte[]?)null)), "tree B's discarded prepare leaves it pre-saga");
                Assert.That(unexpectedSamples, Is.Zero,
                    "an abandon is not a decision: tree A stays on its original view beside tree B's pre-saga view");
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

    [Test]
    public async Task Re_adding_a_decommissioned_source_re_drives_its_held_import_and_publishes_after_the_barrier_decides()
    {
        // The re-add half of #4742's held-import latch, end to end: site A is added
        // back to site B's topology at runtime, and the driver activation
        // service - not a direct grain call - re-drives the import the
        // decommission held. Without the re-drive the latch keeps the shadow
        // unpublished, even once its sibling is imported and the operation's
        // barrier decides.
        const string treeA = "xtib-readd-a";
        const string treeB = "xtib-readd-b";
        const string operationId = "xtib-readd-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        await StartBootstrapAsync(treeA);
        var heldPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        Assert.That(heldPhase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff), "precondition: the import is held by the barrier");
        var drainedBefore = await Coordinator(treeA).GetDrainedExportEpochAsync(SiteAClusterId);
        Assert.That(drainedBefore, Is.Not.Null, "precondition: tree A was drained once");

        var registry = _siteB.Client.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
        try
        {
            await SiteBServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>()
                .DecommissionPeerAsync(SiteAClusterId, CancellationToken.None);
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)null)),
                "precondition: the decommission leaves tree A readable on its original view");

            SiteBTopology.EmitAdded(SiteAClusterId);

            long? drainedAfter = drainedBefore;
            var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
            while ((drainedAfter is null || drainedAfter <= drainedBefore) && Environment.TickCount64 < deadline)
            {
                await Task.Delay(250);
                drainedAfter = await Coordinator(treeA).GetDrainedExportEpochAsync(SiteAClusterId);
            }

            // The sibling is imported too, so the operation's barrier can decide.
            await StartBootstrapAsync(treeB);
            var bPhase = await AwaitPhaseAsync(treeB, LatticeBootstrapState.LiveIncremental);
            var aPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

            Assert.Multiple(async () =>
            {
                Assert.That(drainedAfter, Is.GreaterThan(drainedBefore),
                    "the runtime re-add re-drives tree A's held import from a fresh export");
                Assert.That(await registry.IsDecommissionedAsync(SiteAClusterId), Is.False, "the re-add clears the decommissioned mark");
                Assert.That(bPhase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
                Assert.That(aPhase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the fresh drain and sibling barrier allow the shadow to publish");
                Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 1 })), "tree A is served post-saga");
                Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 2 })), "with tree B");
            });
        }
        finally
        {
            SiteBTopology.EmitRemoved(SiteAClusterId);
            await registry.ClearDecommissionedAsync(SiteAClusterId);
        }
    }
}
