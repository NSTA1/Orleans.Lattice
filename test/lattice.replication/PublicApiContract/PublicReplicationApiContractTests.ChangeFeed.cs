using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="IChangeFeed"/> public contract: the silo-side
/// feed surfaces every locally-authored mutation in HLC ascending
/// order, the cursor-driven re-subscribe model advances past entries
/// already observed, the <c>includeLocalOrigin=false</c> filter
/// suppresses local-origin entries, and the feed never surfaces
/// entries installed by the remote-apply pipeline (the contract claim
/// from the type-level remarks on <see cref="IChangeFeed"/>).
/// </summary>
public partial class PublicReplicationApiContractTests
{
    private static async Task<List<WalRecord>> CollectFeedAsync(
        IAsyncEnumerable<WalRecord> source)
    {
        var result = new List<WalRecord>();
        await foreach (var record in source)
        {
            result.Add(record);
        }
        return result;
    }

    [Test]
    public async Task IChangeFeed_subscribe_yields_locally_authored_set_in_hlc_ascending_order()
    {
        var treeId = NextTreeId("feed-set");
        var lattice = await CreateReplicatedTreeAsync(treeId);
        var feed = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IChangeFeed>();

        await lattice.SetAsync("a", Bytes("1"));
        await lattice.SetAsync("b", Bytes("2"));
        await lattice.SetAsync("c", Bytes("3"));

        var entries = await CollectFeedAsync(feed.Subscribe(treeId, HybridLogicalClock.Zero));

        var sets = entries
            .Where(e => e.Op == MutationKind.Set)
            .Select(e => e.Key)
            .ToList();

        Assert.That(sets, Is.SupersetOf(new[] { "a", "b", "c" }));
        for (var i = 1; i < entries.Count; i++)
        {
            Assert.That(entries[i].Timestamp.CompareTo(entries[i - 1].Timestamp),
                Is.GreaterThanOrEqualTo(0),
                "Entries must be HLC-ascending.");
        }
    }

    [Test]
    public async Task IChangeFeed_subscribe_advances_when_cursor_is_set_to_last_observed_timestamp()
    {
        var treeId = NextTreeId("feed-cursor");
        var lattice = await CreateReplicatedTreeAsync(treeId);
        var feed = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IChangeFeed>();

        await lattice.SetAsync("a", Bytes("1"));
        var firstPass = await CollectFeedAsync(feed.Subscribe(treeId, HybridLogicalClock.Zero));
        Assert.That(firstPass, Is.Not.Empty);

        var cursor = firstPass[^1].Timestamp;

        var emptyPass = await CollectFeedAsync(feed.Subscribe(treeId, cursor));
        Assert.That(emptyPass, Is.Empty,
            "Re-subscribing at the last observed timestamp must yield no entries.");

        await lattice.SetAsync("b", Bytes("2"));
        var nextPass = await CollectFeedAsync(feed.Subscribe(treeId, cursor));
        Assert.That(nextPass.Select(e => e.Key), Contains.Item("b"));
    }

    [Test]
    public async Task IChangeFeed_subscribe_suppresses_local_origin_entries_when_filter_disabled()
    {
        var treeId = NextTreeId("feed-no-local");
        var lattice = await CreateReplicatedTreeAsync(treeId);
        var feed = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IChangeFeed>();

        await lattice.SetAsync("k", Bytes("v"));

        // The local-origin filter excludes entries whose
        // OriginClusterId matches the configured local ClusterId.
        // Durability-writer entries (empty origin) do not match the
        // local cluster id and therefore pass the filter; observer-
        // stamped entries do match and are suppressed.
        var filtered = await CollectFeedAsync(feed.Subscribe(treeId, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(
            filtered.Any(e => e.OriginClusterId == PublicReplicationApiClusterFixture.SiteAClusterId),
            Is.False,
            "includeLocalOrigin=false suppresses entries with OriginClusterId == local cluster id.");
    }

    [Test]
    public async Task IChangeFeed_subscribe_does_not_yield_remote_apply_installed_entries()
    {
        var treeId = NextTreeId("feed-apply-scope");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        // Author on Site A, observe convergence on Site B, then
        // subscribe to Site B's change feed. The contract claim is
        // that B's feed is empty for this tree because the apply
        // pipeline does not WAL-append on the destination.
        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var feedB = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<IChangeFeed>();

        var bSideEntries = await CollectFeedAsync(feedB.Subscribe(treeId, HybridLogicalClock.Zero));

        Assert.That(bSideEntries, Is.Empty,
            "The change feed must not surface entries installed by IReplicationApplier.");
    }
}
