using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="ILatticeReplicationGc"/> public contract: the
/// silo's default registration is <see cref="LatticeReplicationGc"/>,
/// <see cref="ILatticeReplicationGc.RunOnceAsync"/> returns a
/// <see cref="ReplicationGcReport"/> populated with the requested tree
/// name, a non-negative <see cref="ReplicationGcReport.ShardsScanned"/>
/// count, and a non-negative
/// <see cref="ReplicationGcReport.EntriesTrimmed"/> total.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void ILatticeReplicationGc_default_registration_is_lattice_replication_gc()
    {
        var gc = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationGc>();

        Assert.That(gc, Is.InstanceOf<LatticeReplicationGc>());
    }

    [Test]
    public async Task ILatticeReplicationGc_run_once_returns_report_with_tree_name_and_non_negative_counters()
    {
        var treeId = NextTreeId("gc-run");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var gc = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationGc>();

        var report = await gc.RunOnceAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            Assert.That(report.ShardsScanned, Is.GreaterThanOrEqualTo(1));
            Assert.That(report.EntriesTrimmed, Is.GreaterThanOrEqualTo(0));
        });
    }

    [Test]
    public async Task ILatticeReplicationGc_run_once_min_cursor_is_non_null_after_consumer_reports()
    {
        var treeId = NextTreeId("gc-min-cursor");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var gc = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationGc>();

        // The shipper's cursor reports flow through the leaf cursor
        // reporter on a delay; poll until at least one report has
        // landed, then assert MinCursor is non-null.
        ReplicationGcReport report = default;
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                report = await gc.RunOnceAsync(treeId);
                return report.MinCursor is not null;
            },
            "GC run to observe a non-null MinCursor after replication");

        Assert.That(report.MinCursor, Is.Not.Null);
        Assert.That(report.MinCursor!.Value.CompareTo(HybridLogicalClock.Zero), Is.GreaterThanOrEqualTo(0));
    }
}
