using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A replicated write keeps one revision in the receiving region's history when the
/// shipper delivers it again and the receiving tree is then resized. The apply and
/// the resize copy are merge-channel writes that append the entry under the clock
/// its author stamped, and on a tree with no history view the history read serves
/// those log records (issue #4149).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class EntryHistoryReplicationReplayIntegrationTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ScanEntryHistoryAsync_reports_one_revision_for_a_write_applied_twice_and_resized()
    {
        var tree = $"history-replay-{Guid.NewGuid():N}";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };

        for (var delivery = 0; delivery < 2; delivery++)
        {
            await apply.ApplySetAsync("order-1003", new byte[29], hlc, TwoSiteClusterFixture.SiteAClusterId, sourceVectorClock: null, expiresAtTicks: 0);
        }

        var resize = _fixture.SiteB.Client.GetGrain<ITreeResizeGrain>(tree);
        await resize.ResizeAsync(16, 16);
        await resize.RunResizePassAsync();

        var page = await _fixture.SiteB.Client.GetGrain<ILattice>(tree)
            .ScanEntryHistoryAsync("order-1003", null, null, 100, null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Revisions.Select(r => r.Hlc).ToList(), Is.EqualTo(new[] { hlc }));
            Assert.That(page.Revisions.Select(r => r.OriginClusterId).ToList(), Is.EqualTo(new[] { TwoSiteClusterFixture.SiteAClusterId }));
        });
    }
}
