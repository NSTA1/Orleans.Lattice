using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    [Test]
    public async Task An_unstable_export_withholds_completion_until_its_owed_reconcile_finishes()
    {
        const string tree = "rsdr-5116-owed-completion";
        const string key = "deleted-and-reaped";
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(key, [1]);
        await BootstrapSiteBAsync(tree);
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        var previous = await coordinator.GetCompletedExportEpochAsync(SiteAClusterId)
            ?? throw new AssertionException("the initial stable bootstrap must record its export epoch");
        Assert.That(previous, Is.GreaterThan(0), "precondition: an earlier stable completion exists");

        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);
        _onDrainStarted = async () =>
        {
            await siteA.DeleteTreeAsync();
            await siteA.RecoverTreeAsync();
        };
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _onDrainStarted = null;
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await coordinator.GetCompletedExportEpochAsync(SiteAClusterId), Is.Null,
                "an unstable export must not let the source release its re-seed protection");
            Assert.That(await coordinator.GetDrainedExportEpochAsync(SiteAClusterId), Is.Null);
            Assert.That(await _siteB.Client.GetGrain<ILattice>(tree).GetAsync(key), Is.EqualTo(new byte[] { 1 }));
        });

        await DriveSiteBAsync(tree, c => c.RetryOwedReconcileAsync(SiteAClusterId));

        Assert.Multiple(async () =>
        {
            Assert.That(await coordinator.GetCompletedExportEpochAsync(SiteAClusterId), Is.GreaterThan(previous));
            Assert.That(await _siteB.Client.GetGrain<ILattice>(tree).GetAsync(key), Is.Null);
        });
    }
}
