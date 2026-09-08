using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins the catalog and shard-summary ordering shapes against the full-sort
/// behaviour they replaced.
/// </summary>
/// <remarks>
/// <para>
/// Two changes are covered. The catalog endpoints now serve a page by a
/// <see cref="CatalogTopSelector"/> bounded selection whenever nothing
/// downstream of the ordering can thin the candidate set, and fall back to
/// buffering plus a full sort when a per-entry filter (auth-backed visibility, a
/// source-tree filter) is in play. <see cref="GetShardSummariesAsync"/>'s
/// ordering is now an exact-width projection plus a stable insertion sort rather
/// than a LINQ ordering chain.
/// </para>
/// <para>
/// Every assertion is written as an equivalence against the reference answer -
/// what a full ordinal sort of the whole candidate set would have emitted - so a
/// bug in the bounded path cannot be masked by an expectation authored to match
/// it. The registry is deliberately seeded in a <b>shuffled</b> order, because
/// the real registry yields ordinal-sorted ids and an already-sorted source
/// would let a broken ordering pass.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeStateQueryOrderingTests
{
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private sealed class AllowNamedTrees(Func<string, bool> allow) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(allow(request.TreeId) ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("hidden"));
    }

    private sealed class FixedMembership(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);
    }

    private sealed class NotDeleted : ITreeDeletionGrain
    {
        public Task<bool> IsDeletedAsync() => Task.FromResult(false);

        public Task DeleteTreeAsync() => throw new NotSupportedException();

        public Task<TreeDeletionSnapshot> GetDeletionStatusAsync() => throw new NotSupportedException();

        public Task RecoverAsync() => throw new NotSupportedException();

        public Task PurgeNowAsync() => throw new NotSupportedException();
    }

    /// <summary>
    /// Builds a query over a registry seeded with <paramref name="allTreeIds"/>
    /// in exactly the order given, plus an optional per-tree diagnostic report
    /// so the shard-summary path can be driven.
    /// </summary>
    private static LatticeStateQuery CreateQuery(
        IReadOnlyList<string> allTreeIds,
        Func<string, bool>? allow = null,
        TreeDiagnosticReport? diagnose = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        registry.GetAllTreeIdsAsync().Returns(Task.FromResult(allTreeIds));
        registry.GetAllTreeIdsAsync(Arg.Any<string?>()).Returns(call =>
        {
            var prefix = call.Arg<string?>();
            return Task.FromResult<IReadOnlyList<string>>(
                string.IsNullOrEmpty(prefix)
                    ? allTreeIds
                    : [.. allTreeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal))]);
        });

        registry.GetEntriesAsync(Arg.Any<IReadOnlyList<string>>()).Returns(call =>
        {
            var ids = call.Arg<IReadOnlyList<string>>();
            var result = new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal);
            foreach (var id in ids)
            {
                result[id] = new TreeRegistryEntry { ShardCount = 4 };
            }

            return Task.FromResult(result);
        });

        grainFactory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>()).Returns(_ => new NotDeleted());

        if (diagnose is { } report)
        {
            var tree = Substitute.For<ILattice>();
            tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
            tree.DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(report));
            grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(_ => tree);
        }

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var serviceCollection = new ServiceCollection();
        if (allow is not null)
        {
            serviceCollection.AddSingleton<ILatticeAccessGate>(new AllowNamedTrees(allow));
            serviceCollection.AddSingleton<ILatticeMembershipContext>(
                new FixedMembership(new LatticeSubject("alice")));
        }

        return new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            serviceCollection.BuildServiceProvider(),
            new NullTenantContextResolver());
    }

    /// <summary>A shuffled catalog, so an unordered source is what reaches the query.</summary>
    private static string[] ShuffledCatalog(int size, int seed)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, size)
            .Select(i => $"catalog-tree-{i:D5}")
            .OrderBy(_ => random.Next())];
    }

    // ----- Bounded selection is page-equivalent to the full sort -----

    [Test]
    public async Task ListTreesAsync_pages_in_ordinal_order_from_a_shuffled_registry()
    {
        var ids = ShuffledCatalog(500, seed: 7);
        var query = CreateQuery(ids);

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 25 });

        var expected = ids.OrderBy(id => id, StringComparer.Ordinal).Take(25);
        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(expected));
            Assert.That(page.NextPageToken, Is.EqualTo(expected.Last()));
        });
    }

    [Test]
    public async Task ListTreesAsync_walks_every_id_exactly_once_across_pages()
    {
        var ids = ShuffledCatalog(233, seed: 11);
        var query = CreateQuery(ids);

        var walked = new List<string>();
        string? token = null;
        do
        {
            var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 20, PageToken = token });
            walked.AddRange(page.Entries.Select(e => e.TreeId));
            token = page.NextPageToken;
        }
        while (token is not null);

        Assert.That(walked, Is.EqualTo(ids.OrderBy(id => id, StringComparer.Ordinal)));
    }

    [Test]
    public async Task ListTreesAsync_clears_the_next_page_token_on_the_final_page()
    {
        // The bounded selection retains pageSize + 1 ids precisely so the token
        // is decided by the selection itself; the final page must not set one.
        var query = CreateQuery(ShuffledCatalog(20, seed: 3));

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 20 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(20));
            Assert.That(page.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task ListTreesAsync_sets_the_next_page_token_when_exactly_one_id_remains()
    {
        // The boundary the lookahead exists for: 21 ids and a page of 20.
        var query = CreateQuery(ShuffledCatalog(21, seed: 4));

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 20 });

        Assert.That(page.NextPageToken, Is.EqualTo("catalog-tree-00019"));
    }

    [Test]
    public async Task ListTreesAsync_with_visibility_on_still_fills_a_page_past_hidden_trees()
    {
        // The full-sort fallback exists for exactly this shape: the per-entry
        // probe drops most candidates, so a page's worth of selected ids would
        // not have been enough to fill a page.
        var ids = ShuffledCatalog(400, seed: 13);
        var query = CreateQuery(ids, allow: id => id.EndsWith('0'));

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 10 });

        var expected = ids
            .Where(id => id.EndsWith('0'))
            .OrderBy(id => id, StringComparer.Ordinal)
            .Take(10);
        Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(expected));
    }

    [Test]
    public async Task ListTagIndexesAsync_pages_in_ordinal_order_from_a_shuffled_registry()
    {
        var random = new Random(17);
        var ids = Enumerable.Range(0, 300)
            .Select(i => $"{LatticeConstants.TagIndexTreePrefix}index-{i:D4}")
            .OrderBy(_ => random.Next())
            .ToArray();
        var query = CreateQuery(ids);

        var page = await query.ListTagIndexesAsync(new CatalogRequest { PageSize = 15 });

        var expected = ids
            .OrderBy(id => id, StringComparer.Ordinal)
            .Take(15)
            .Select(id => id[LatticeConstants.TagIndexTreePrefix.Length..]);
        Assert.That(page.Entries.Select(e => e.IndexName), Is.EqualTo(expected));
    }

    [Test]
    public async Task ListTagIndexesAsync_walks_every_index_exactly_once_across_pages()
    {
        var random = new Random(19);
        var ids = Enumerable.Range(0, 97)
            .Select(i => $"{LatticeConstants.TagIndexTreePrefix}index-{i:D4}")
            .OrderBy(_ => random.Next())
            .ToArray();
        var query = CreateQuery(ids);

        var walked = new List<string>();
        string? token = null;
        do
        {
            var page = await query.ListTagIndexesAsync(new CatalogRequest { PageSize = 10, PageToken = token });
            walked.AddRange(page.Entries.Select(e => e.IndexName));
            token = page.NextPageToken;
        }
        while (token is not null);

        Assert.That(
            walked,
            Is.EqualTo(ids
                .OrderBy(id => id, StringComparer.Ordinal)
                .Select(id => id[LatticeConstants.TagIndexTreePrefix.Length..])));
    }

    [Test]
    public async Task ListTreesAsync_honours_a_page_token_that_lands_mid_catalog()
    {
        var ids = ShuffledCatalog(120, seed: 23);
        var query = CreateQuery(ids);

        var page = await query.ListTreesAsync(
            new CatalogRequest { PageSize = 10, PageToken = "catalog-tree-00050" });

        var expected = ids
            .Where(id => string.CompareOrdinal(id, "catalog-tree-00050") > 0)
            .OrderBy(id => id, StringComparer.Ordinal)
            .Take(10);
        Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(expected));
    }

    // ----- Shard-summary ordering -----

    private static TreeDiagnosticReport ReportWithShardIndices(params int[] indices) =>
        new()
        {
            Shards = [.. indices.Select(i => new ShardDiagnosticReport
            {
                ShardIndex = i,
                Depth = 1,
                RootIsLeaf = true,
                LiveKeys = 10 + i,
                Tombstones = i,
                OpsPerSecond = i,
                SplitInProgress = false,
            })],
        };

    [Test]
    public async Task GetShardSummariesAsync_returns_shards_in_index_order_when_the_report_is_ordered()
    {
        var query = CreateQuery(["tree"], diagnose: ReportWithShardIndices(0, 1, 2, 3));

        var result = await query.GetShardSummariesAsync("tree");

        Assert.That(result.Shards.Select(s => s.ShardIndex), Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public async Task GetShardSummariesAsync_orders_an_out_of_order_report_by_shard_index()
    {
        var query = CreateQuery(["tree"], diagnose: ReportWithShardIndices(5, 1, 4, 0, 3, 2));

        var result = await query.GetShardSummariesAsync("tree");

        Assert.That(result.Shards.Select(s => s.ShardIndex), Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task GetShardSummariesAsync_orders_a_reversed_report_by_shard_index()
    {
        var query = CreateQuery(["tree"], diagnose: ReportWithShardIndices(7, 6, 5, 4, 3, 2, 1, 0));

        var result = await query.GetShardSummariesAsync("tree");

        Assert.That(result.Shards.Select(s => s.ShardIndex), Is.EqualTo(Enumerable.Range(0, 8)));
    }

    [Test]
    public async Task GetShardSummariesAsync_keeps_rows_with_an_equal_index_in_source_order()
    {
        // The insertion sort is stable, so a report carrying a repeated index
        // emits its rows in the order the report listed them - exactly as the
        // prior LINQ ordering did.
        var query = CreateQuery(["tree"], diagnose: ReportWithShardIndices(2, 1, 1, 0));

        var result = await query.GetShardSummariesAsync("tree");

        Assert.Multiple(() =>
        {
            Assert.That(result.Shards.Select(s => s.ShardIndex), Is.EqualTo(new[] { 0, 1, 1, 2 }));

            // LiveKeys is derived from the source index, so an unstable swap of
            // the two equal-index rows would be visible here.
            Assert.That(result.Shards.Select(s => s.LiveKeys), Is.EqualTo(new long[] { 10, 11, 11, 12 }));
        });
    }

    [Test]
    public async Task GetShardSummariesAsync_projects_every_member_of_each_shard_row()
    {
        var query = CreateQuery(["tree"], diagnose: new TreeDiagnosticReport
        {
            Shards =
            [
                new ShardDiagnosticReport
                {
                    ShardIndex = 3,
                    Depth = 2,
                    RootIsLeaf = false,
                    LiveKeys = 41,
                    Tombstones = 7,
                    OpsPerSecond = 12.5,
                    SplitInProgress = true,
                },
            ],
        });

        var result = await query.GetShardSummariesAsync("tree");

        var shard = result.Shards.Single();
        Assert.Multiple(() =>
        {
            Assert.That(shard.ShardIndex, Is.EqualTo(3));
            Assert.That(shard.Depth, Is.EqualTo(2));
            Assert.That(shard.RootIsLeaf, Is.False);
            Assert.That(shard.LiveKeys, Is.EqualTo(41));
            Assert.That(shard.Tombstones, Is.EqualTo(7));
            Assert.That(shard.OpsPerSecond, Is.EqualTo(12.5));
            Assert.That(shard.SplitInProgress, Is.True);
        });
    }

    [Test]
    public async Task GetShardSummariesAsync_returns_no_shards_for_an_empty_report()
    {
        var query = CreateQuery(["tree"], diagnose: new TreeDiagnosticReport
        {
            Shards = ImmutableArray<ShardDiagnosticReport>.Empty,
        });

        var result = await query.GetShardSummariesAsync("tree");

        Assert.That(result.Shards, Is.Empty);
    }
}
