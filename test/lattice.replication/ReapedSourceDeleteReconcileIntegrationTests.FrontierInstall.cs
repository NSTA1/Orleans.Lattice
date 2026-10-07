using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b at the coordinator's call site: a completed bootstrap pins
/// the frontier the export carried in its close trailer onto the receiver's tree
/// frontier only when that frontier was read under the lineage the export
/// opened under (<see cref="BootstrapFrontierInstall.Decide"/>). The export's
/// per-origin low watermarks feed the receiver's reap gate and its causal
/// dependency checks, so a watermark from another lineage would vouch for writes
/// the imported contents never held. The direct-call test of <c>Decide</c> does
/// not see a coordinator that bypasses it; this one runs the real coordinator
/// and reads the real tree frontier.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static SnapshotSourceFrontier ClosingFrontier(HybridLogicalClock lowWatermark, Guid? lineage) => new()
    {
        Lineage = lineage,
        LowWatermarks = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [SiteCClusterId] = lowWatermark },
        Held = new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal) { [SiteCClusterId] = [] },
    };

    private async Task<ReplicationTreeFrontierSnapshot> BootstrapWithClosingFrontierAsync(
        string tree,
        Func<SnapshotSourceFrontier?, SnapshotSourceFrontier?> closing)
    {
        _closeFrontier = closing;
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _closeFrontier = null;
        }

        return await _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(tree).GetAsync();
    }

    [Test]
    public async Task A_completed_bootstrap_pins_the_exported_frontier_only_when_it_was_read_under_the_opening_lineage()
    {
        const string tree = "rsdr-4586-frontier-install";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("anchor", [1]);
        await BootstrapSiteBAsync(tree);
        var watermark = PastHlc(10);

        var foreign = await BootstrapWithClosingFrontierAsync(
            tree, _ => ClosingFrontier(watermark, Guid.NewGuid()));
        var matching = await BootstrapWithClosingFrontierAsync(
            tree, shipped => ClosingFrontier(watermark, shipped?.Lineage));

        Assert.Multiple(() =>
        {
            Assert.That(foreign.LowWatermarks.ContainsKey(SiteCClusterId), Is.False,
                "a frontier read under another lineage does not describe the imported contents, so nothing is pinned");
            Assert.That(matching.LowWatermarks.TryGetValue(SiteCClusterId, out var pinned), Is.True,
                "precondition: a frontier read under the opening lineage is pinned");
            Assert.That(pinned, Is.EqualTo(watermark));
        });
    }
}
