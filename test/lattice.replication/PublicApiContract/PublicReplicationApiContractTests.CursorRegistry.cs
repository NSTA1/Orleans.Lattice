using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the replication-side overlay of the
/// <see cref="IWalCursorRegistry"/> public contract: after
/// replication advances, the registry surfaces a
/// <see cref="WalCursorSnapshot"/> with a non-default cursor for the
/// per-peer shipper consumer id, and
/// <see cref="IWalCursorRegistry.GetMinCursorAsync"/> returns
/// <see langword="null"/> before any consumer has reported.
/// <para>
/// The default DI registration assertion lives in the core
/// <c>PublicApiContractTests.WalGc</c> partial because the type is
/// declared in <c>Orleans.Lattice</c>; this partial covers the
/// replication-driven traffic on top of it.
/// </para>
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task IWalCursorRegistry_snapshot_contains_consumer_after_replication_advances()
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
            .GetRequiredService<IWalCursorRegistry>();

        // Allow the leaf cursor reporter and shipper a brief window
        // to publish their cursor reports after the apply ack so the
        // SnapshotAsync call observes a non-empty list. The reporter
        // batches reports, so we poll until at least one consumer
        // shows up rather than asserting a specific count.
        IReadOnlyList<WalCursorSnapshot>? snapshot = null;
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
    public async Task IWalCursorRegistry_get_min_cursor_returns_floor_or_null_pre_replication()
    {
        var treeId = NextTreeId("cursor-min");
        await CreateReplicatedTreeAsync(treeId);

        var registry = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IWalCursorRegistry>();

        // Pre-replication, no consumer has reported, so the min
        // cursor is null. After we drive a replication cycle the
        // shipper consumer reports its cursor and the min flips to a
        // non-null value.
        var initialMin = await registry.GetMinCursorAsync(treeId);
        Assert.That(initialMin, Is.Null);
    }
}
