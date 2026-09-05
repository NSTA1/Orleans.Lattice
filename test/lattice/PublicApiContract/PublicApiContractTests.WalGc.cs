using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Pins the core single-cluster public-API contract for the WAL
/// maintenance seams that now live in <c>Orleans.Lattice</c>:
/// the default DI registration is the in-memory
/// <see cref="InMemoryWalCursorRegistry"/> and <see cref="LatticeWalGc"/>
/// implementations and <see cref="ILatticeWalGc.RunOnceAsync"/> returns
/// a <see cref="LatticeWalGcReport"/> whose
/// <see cref="LatticeWalGcReport.TreeName"/> matches the request.
/// <para>
/// Replication-side overlays (<c>AddLatticeReplication</c> replacing
/// the in-memory defaults with replication-specific impls and per-tree
/// option mirrors) are covered by the corresponding partials under
/// <c>test/lattice.replication/PublicApiContract/</c>; this partial is
/// scoped to the single-cluster surface.
/// </para>
/// </summary>
public partial class PublicApiContractTests
{
    private static IServiceProvider RequireSiloServices()
    {
        var services = PublicApiContractClusterFixture.SiloServices;
        Assert.That(services, Is.Not.Null, "Silo IServiceProvider was not captured by the fixture.");
        return services!;
    }

    [Test]
    public void IWalCursorRegistry_default_registration_is_in_memory()
    {
        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        Assert.That(registry, Is.InstanceOf<InMemoryWalCursorRegistry>());
    }

    [Test]
    public void ILatticeWalGc_default_registration_is_lattice_wal_gc()
    {
        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        Assert.That(gc, Is.InstanceOf<LatticeWalGc>());
    }

    [Test]
    public void ILeafCursorReporter_is_registered_when_wal_cursor_registry_is_added()
    {
        var reporter = RequireSiloServices().GetService<ILeafCursorReporter>();
        Assert.That(reporter, Is.Not.Null,
            "AddWalCursorRegistry must register an ILeafCursorReporter so leaf grains pin the WAL GC.");
    }

    [Test]
    public async Task ILatticeWalGc_RunOnceAsync_returns_report_with_requested_tree_name_and_non_negative_counters()
    {
        var treeId = "pac-walgc-report-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            Assert.That(report.ShardsScanned, Is.GreaterThanOrEqualTo(1));
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "without a positive consumer cursor or retention TTL, the default GC pass must be a no-op");
        });
    }

    [Test]
    public async Task IWalCursorRegistry_SnapshotAsync_returns_empty_when_no_consumer_has_reported()
    {
        // Brand-new tree id that has not been touched: the registry
        // must return an empty snapshot rather than throwing.
        var treeId = "pac-cursor-empty-" + Guid.NewGuid().ToString("N")[..8];
        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        var snapshot = await registry.SnapshotAsync(treeId);
        Assert.That(snapshot, Is.Not.Null);
        Assert.That(snapshot, Is.Empty);
    }

    [Test]
    public async Task IWalCursorRegistry_GetMinCursorAsync_returns_null_when_no_consumer_has_reported()
    {
        var treeId = "pac-cursor-min-null-" + Guid.NewGuid().ToString("N")[..8];
        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        var min = await registry.GetMinCursorAsync(treeId);
        Assert.That(min, Is.Null);
    }
}
