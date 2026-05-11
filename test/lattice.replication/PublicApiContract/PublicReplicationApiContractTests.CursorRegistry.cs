using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="ILatticeReplicationCursorRegistry"/> public
/// contract: the default DI registration is
/// <see cref="InMemoryReplicationCursorRegistry"/>, the registry
/// surfaces <see cref="ReplicationCursorSnapshot"/> rows with a
/// non-default cursor for the per-peer shipper consumer id after
/// replication advances, and <c>GetMinCursorAsync</c> returns the
/// pointwise minimum across consumers.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void ILatticeReplicationCursorRegistry_default_registration_is_in_memory()
    {
        var registry = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationCursorRegistry>();

        Assert.That(registry, Is.InstanceOf<InMemoryReplicationCursorRegistry>());
    }

    [Test]
    public async Task ILatticeReplicationCursorRegistry_snapshot_contains_consumer_after_replication_advances()
    {
        var treeId = NextTreeId("cursor-registry");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var registry = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationCursorRegistry>();

        // Allow the leaf cursor reporter and shipper a brief window
        // to publish their cursor reports after the apply ack so the
        // SnapshotAsync call observes a non-empty list. The reporter
        // batches reports, so we poll until at least one consumer
        // shows up rather than asserting a specific count.
        IReadOnlyList<ReplicationCursorSnapshot>? snapshot = null;
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                snapshot = await registry.SnapshotAsync(treeId);
                return snapshot is { Count: > 0 };
            },
            "cursor-registry snapshot non-empty");

        Assert.That(snapshot, Is.Not.Null);
        Assert.That(snapshot!, Is.Not.Empty);
        Assert.That(snapshot!.Any(s => s.Cursor.CompareTo(HybridLogicalClock.Zero) >= 0), Is.True);
    }

    [Test]
    public async Task ILatticeReplicationCursorRegistry_get_min_cursor_returns_floor_or_null_pre_replication()
    {
        var treeId = NextTreeId("cursor-min");
        await CreateReplicatedTreeAsync(treeId);

        var registry = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<ILatticeReplicationCursorRegistry>();

        // Pre-replication, no consumer has reported, so the min
        // cursor is null. After we drive a replication cycle the
        // shipper consumer reports its cursor and the min flips to a
        // non-null value.
        var initialMin = await registry.GetMinCursorAsync(treeId);
        Assert.That(initialMin, Is.Null);
    }
}
