using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins the catalog paging call shape introduced by issue #1686: a page costs
/// <b>two batched waves</b> - one <see cref="ILatticeRegistry.GetEntriesAsync"/>
/// multi-get plus one bounded <see cref="ITreeDeletionGrain.IsDeletedAsync"/>
/// fan-out - instead of two sequential grain round-trips per emitted entry.
/// </summary>
/// <remarks>
/// <para>
/// The batching is a call-shape optimisation and must be caller-invisible, so
/// these tests assert both halves: the round-trip counts actually collapsed, and
/// the entries, their order, and the page token are unchanged across every
/// combination of visibility, tenancy, system-tree inclusion, and page token.
/// </para>
/// <para>
/// The load-bearing ordering property has its own test: the per-entry visibility
/// filter runs <em>first</em>, so an entry it drops is never batched, and the
/// batched read can never become a way to observe an entry the per-entry path
/// would have filtered.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeStateQueryCatalogBatchingTests
{
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    /// <summary>Records every registry and deletion round-trip the query made.</summary>
    private sealed class CatalogRecorder
    {
        /// <summary>One entry per batched multi-get, holding the ids it asked for.</summary>
        public List<string[]> RegistryBatches { get; } = [];

        /// <summary>Every id read through the unbatched per-entry registry member.</summary>
        public List<string> SingleEntryReads { get; } = [];

        /// <summary>Every id whose deletion state was probed, in probe order.</summary>
        public List<string> DeletionProbes { get; } = [];

        /// <summary>Total grain round-trips the catalog projection paid.</summary>
        public int TotalRoundTrips =>
            RegistryBatches.Count + SingleEntryReads.Count + DeletionProbes.Count;
    }

    /// <summary>
    /// Minimal <see cref="ITreeDeletionGrain"/> stand-in that records its probe
    /// and answers from a fixed soft-deleted set. Hand-rolled rather than mocked
    /// so probe attribution per tree id is exact.
    /// </summary>
    private sealed class FakeDeletionGrain(string treeId, CatalogRecorder recorder, bool deleted)
        : ITreeDeletionGrain
    {
        public Task<bool> IsDeletedAsync()
        {
            recorder.DeletionProbes.Add(treeId);
            return Task.FromResult(deleted);
        }

        public Task DeleteTreeAsync() => throw new NotSupportedException();

        public Task<TreeDeletionSnapshot> GetDeletionStatusAsync() => throw new NotSupportedException();

        public Task RecoverAsync() => throw new NotSupportedException();

        public Task PurgeNowAsync() => throw new NotSupportedException();
    }

    private sealed class FakeGate(Func<string, bool> allow) : ILatticeAccessGate
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

    /// <summary>
    /// Builds a query over a fake registry and deletion surface.
    /// <paramref name="allow"/> null leaves visibility disabled (no gate
    /// registered); non-null registers a gate plus the named subject, so the
    /// per-entry visibility probe runs.
    /// </summary>
    private static (LatticeStateQuery Query, CatalogRecorder Recorder) CreateQuery(
        IReadOnlyList<string> allTreeIds,
        Func<string, bool>? allow = null,
        LatticeSubject? subject = null,
        IReadOnlyDictionary<string, int>? shardCounts = null,
        IReadOnlySet<string>? softDeleted = null)
    {
        var recorder = new CatalogRecorder();
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
                    : allTreeIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList());
        });

        registry.GetEntryAsync(Arg.Any<string>()).Returns(call =>
        {
            recorder.SingleEntryReads.Add(call.Arg<string>());
            return Task.FromResult<TreeRegistryEntry?>(null);
        });

        registry.GetEntriesAsync(Arg.Any<IReadOnlyList<string>>()).Returns(call =>
        {
            var ids = call.Arg<IReadOnlyList<string>>();
            recorder.RegistryBatches.Add([.. ids]);

            var result = new Dictionary<string, TreeRegistryEntry>(StringComparer.Ordinal);
            foreach (var id in ids)
            {
                if (shardCounts is not null && shardCounts.TryGetValue(id, out var count))
                {
                    result[id] = new TreeRegistryEntry { ShardCount = count };
                }
            }

            return Task.FromResult(result);
        });

        grainFactory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>()).Returns(call =>
        {
            // GetGrain<T>(string, string) takes two strings, so index the key positionally.
            var id = call.ArgAt<string>(0);
            return new FakeDeletionGrain(id, recorder, softDeleted?.Contains(id) == true);
        });

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var serviceCollection = new ServiceCollection();
        if (allow is not null)
        {
            serviceCollection.AddSingleton<ILatticeAccessGate>(new FakeGate(allow));
            serviceCollection.AddSingleton<ILatticeMembershipContext>(
                new FixedMembership(subject ?? new LatticeSubject("alice")));
        }

        var query = new LatticeStateQuery(
            grainFactory,
            options,
            Options.Create(new LatticeApiStateOptions()),
            serviceCollection.BuildServiceProvider(),
            new NullTenantContextResolver());

        return (query, recorder);
    }

    private static readonly string[] Catalog =
    [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    ];

    // ----- Round-trip collapse -----

    [Test]
    public async Task A_catalog_page_reads_the_registry_in_exactly_one_batched_call()
    {
        var (query, recorder) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(Catalog.Length));
            Assert.That(recorder.RegistryBatches, Has.Count.EqualTo(1));
            Assert.That(recorder.SingleEntryReads, Is.Empty);
        });
    }

    [Test]
    public async Task A_catalog_page_costs_one_registry_call_plus_one_deletion_probe_per_entry()
    {
        // The measured figure the change targets: 2P sequential round-trips
        // become 1 + P, in two waves rather than 2P sequential awaits.
        var (query, recorder) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.That(
            recorder.TotalRoundTrips,
            Is.EqualTo(1 + page.Entries.Count),
            "a page must cost one batched registry read plus one deletion probe per emitted entry");
    }

    [Test]
    public async Task The_batched_read_asks_for_exactly_the_emitted_page_in_order()
    {
        var (query, recorder) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 3 });

        Assert.Multiple(() =>
        {
            Assert.That(recorder.RegistryBatches, Has.Count.EqualTo(1));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "alpha", "bravo", "charlie" }));
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "bravo", "charlie" }));
        });
    }

    [Test]
    public async Task Deletion_probes_are_bounded_by_the_page_size()
    {
        var (query, recorder) = CreateQuery(Catalog);

        await query.ListTreesAsync(new CatalogRequest { PageSize = 2 });

        Assert.That(recorder.DeletionProbes, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task An_empty_page_makes_no_registry_or_deletion_call()
    {
        var (query, recorder) = CreateQuery([]);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(recorder.RegistryBatches, Is.Empty);
            Assert.That(recorder.DeletionProbes, Is.Empty);
        });
    }

    // ----- The load-bearing ordering property -----

    [Test]
    public async Task An_entry_the_visibility_filter_drops_is_never_batched()
    {
        // Filter-first, then batch. Batching ahead of the visibility check would
        // read entries the per-entry path would have filtered - which is exactly
        // what the issue forbids.
        var (query, recorder) = CreateQuery(
            Catalog,
            allow: id => id is "alpha" or "delta");

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "delta" }));
            Assert.That(recorder.RegistryBatches, Has.Count.EqualTo(1));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "alpha", "delta" }));
            Assert.That(recorder.DeletionProbes, Is.EqualTo(new[] { "alpha", "delta" }));
        });
    }

    [Test]
    public async Task An_anonymous_subject_sees_an_empty_catalog_and_reads_nothing()
    {
        var (query, recorder) = CreateQuery(
            Catalog,
            allow: _ => true,
            subject: LatticeSubject.Anonymous);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(recorder.TotalRoundTrips, Is.Zero);
        });
    }

    [Test]
    public async Task A_fully_denying_gate_batches_nothing()
    {
        var (query, recorder) = CreateQuery(Catalog, allow: _ => false);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(recorder.RegistryBatches, Is.Empty);
            Assert.That(recorder.DeletionProbes, Is.Empty);
        });
    }

    [Test]
    public async Task Visibility_filtering_still_fills_a_whole_page_from_the_survivors()
    {
        // The filter thins the candidate set, so the loop must keep pulling
        // candidates until the page is full rather than batching the first
        // page-size candidates.
        var (query, recorder) = CreateQuery(
            Catalog,
            allow: id => id is not "bravo" and not "charlie");

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 3 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "delta", "echo" }));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "alpha", "delta", "echo" }));
        });
    }

    // ----- Caller-facing equivalence -----

    [Test]
    public async Task The_page_token_is_the_last_emitted_entry_when_more_candidates_remain()
    {
        var (query, _) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 2 });

        Assert.That(page.NextPageToken, Is.EqualTo("bravo"));
    }

    [Test]
    public async Task The_page_token_is_null_when_the_page_is_not_full()
    {
        var (query, _) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 100 });

        Assert.That(page.NextPageToken, Is.Null);
    }

    [Test]
    public async Task The_page_token_is_still_emitted_when_the_only_remaining_candidate_is_invisible()
    {
        // Pre-existing behaviour: the token is set as soon as one more candidate
        // exists, whether or not it would survive the visibility filter. The
        // batching must not change that - the next page is simply empty.
        var (query, _) = CreateQuery(
            ["alpha", "bravo", "charlie"],
            allow: id => id is not "charlie");

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "bravo" }));
            Assert.That(page.NextPageToken, Is.EqualTo("bravo"));
        });
    }

    [Test]
    public async Task A_supplied_page_token_resumes_strictly_after_it()
    {
        var (query, recorder) = CreateQuery(Catalog);

        var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 2, PageToken = "bravo" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "charlie", "delta" }));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "charlie", "delta" }));
        });
    }

    [Test]
    public async Task Full_pagination_visits_every_tree_exactly_once()
    {
        var seen = new List<string>();
        string? token = null;
        var pages = 0;

        do
        {
            var (query, _) = CreateQuery(Catalog);
            var page = await query.ListTreesAsync(new CatalogRequest { PageSize = 2, PageToken = token });
            seen.AddRange(page.Entries.Select(e => e.TreeId));
            token = page.NextPageToken;
            pages++;
        }
        while (token is not null && pages < 10);

        Assert.That(seen, Is.EqualTo(Catalog));
    }

    [Test]
    public async Task Registry_entries_are_projected_onto_the_matching_tree_id()
    {
        // Guards the batched projection against a positional misalignment: each
        // tree carries a distinct shard count, so a shifted mapping is visible.
        var (query, _) = CreateQuery(
            Catalog,
            shardCounts: new Dictionary<string, int>(StringComparer.Ordinal)
            {
                ["alpha"] = 1,
                ["bravo"] = 2,
                ["charlie"] = 3,
                ["delta"] = 4,
                ["echo"] = 5,
                ["foxtrot"] = 6,
            });

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Select(e => (e.TreeId, e.ShardCount)),
            Is.EqualTo(new[]
            {
                ("alpha", 1), ("bravo", 2), ("charlie", 3),
                ("delta", 4), ("echo", 5), ("foxtrot", 6),
            }));
    }

    [Test]
    public async Task An_unregistered_id_still_maps_to_a_default_shaped_entry()
    {
        // GetEntriesAsync omits absent ids, so the projection must fall back to
        // the same defaults the null single-entry read produced.
        var (query, _) = CreateQuery(["ghost"]);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(1));
            Assert.That(page.Entries[0].ShardCount, Is.EqualTo(LatticeConstants.DefaultShardCount));
            Assert.That(page.Entries[0].IsAlias, Is.False);
            Assert.That(page.Entries[0].Lifecycle, Is.EqualTo(TreeLifecycleState.Active));
        });
    }

    [Test]
    public async Task Deletion_state_is_projected_onto_the_matching_tree_id()
    {
        // Guards the bounded fan-out against a positional misalignment: only the
        // soft-deleted tree may report SoftDeleted.
        var (query, _) = CreateQuery(
            Catalog,
            softDeleted: new HashSet<string>(StringComparer.Ordinal) { "charlie" });

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Where(e => e.Lifecycle == TreeLifecycleState.SoftDeleted).Select(e => e.TreeId),
            Is.EqualTo(new[] { "charlie" }));
    }

    [Test]
    public async Task The_active_tenant_filter_still_scopes_the_batched_page()
    {
        var (query, recorder) = CreateQuery(
            ["t/acme/orders", "t/acme/users", "t/globex/secrets", "legacy"]);

        TreeCatalogPage page;
        using (LatticeActiveTenantContext.With(TenantId.Parse("acme")))
        {
            page = await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "t/acme/orders", "t/acme/users" }));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "t/acme/orders", "t/acme/users" }));
        });
    }

    [Test]
    public async Task The_default_tenant_batches_the_whole_unscoped_catalog()
    {
        var (query, recorder) = CreateQuery(Catalog);

        TreeCatalogPage page;
        using (LatticeActiveTenantContext.With(TenantId.Default))
        {
            page = await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(Catalog));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(Catalog));
        });
    }

    [Test]
    public async Task Requesting_system_trees_batches_them_too()
    {
        var ids = new[] { "alpha", LatticeConstants.RegistryTreeId, "bravo" };
        var (query, recorder) = CreateQuery(ids);

        var hidden = await query.ListTreesAsync(new CatalogRequest());

        var (shownQuery, shownRecorder) = CreateQuery(ids);
        var shown = await shownQuery.ListTreesAsync(new CatalogRequest { IncludeSystemTrees = true });

        Assert.Multiple(() =>
        {
            Assert.That(hidden.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "bravo" }));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "alpha", "bravo" }));
            Assert.That(shown.Entries.Select(e => e.TreeId), Does.Contain(LatticeConstants.RegistryTreeId));
            Assert.That(shownRecorder.RegistryBatches[0], Does.Contain(LatticeConstants.RegistryTreeId));
        });
    }

    [Test]
    public async Task Tag_index_trees_are_never_part_of_the_tree_catalog_batch()
    {
        var (query, recorder) = CreateQuery(
            ["alpha", LatticeConstants.TagIndexTreePrefix + "colour", "bravo"]);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "alpha", "bravo" }));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(new[] { "alpha", "bravo" }));
        });
    }

    // ----- Tag-index catalog -----

    [Test]
    public async Task The_tag_index_catalog_reads_the_registry_in_exactly_one_batched_call()
    {
        var ids = new[]
        {
            LatticeConstants.TagIndexTreePrefix + "colour",
            LatticeConstants.TagIndexTreePrefix + "size",
            LatticeConstants.TagIndexTreePrefix + "weight",
        };
        var (query, recorder) = CreateQuery(ids);

        var page = await query.ListTagIndexesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(ids));
            Assert.That(recorder.RegistryBatches, Has.Count.EqualTo(1));
            Assert.That(recorder.RegistryBatches[0], Is.EqualTo(ids));
            Assert.That(recorder.SingleEntryReads, Is.Empty);
        });
    }

    [Test]
    public async Task The_tag_index_catalog_projects_shard_counts_onto_the_matching_index()
    {
        var colour = LatticeConstants.TagIndexTreePrefix + "colour";
        var size = LatticeConstants.TagIndexTreePrefix + "size";
        var (query, _) = CreateQuery(
            [colour, size],
            shardCounts: new Dictionary<string, int>(StringComparer.Ordinal)
            {
                [colour] = 11,
                [size] = 22,
            });

        var page = await query.ListTagIndexesAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Select(e => (e.IndexName, e.TreeId, e.ShardCount)),
            Is.EqualTo(new[] { ("colour", colour, 11), ("size", size, 22) }));
    }

    [Test]
    public async Task The_tag_index_catalog_page_token_is_the_last_emitted_index()
    {
        var colour = LatticeConstants.TagIndexTreePrefix + "colour";
        var size = LatticeConstants.TagIndexTreePrefix + "size";
        var (query, _) = CreateQuery([colour, size, LatticeConstants.TagIndexTreePrefix + "weight"]);

        var page = await query.ListTagIndexesAsync(new CatalogRequest { PageSize = 2 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { colour, size }));
            Assert.That(page.NextPageToken, Is.EqualTo(size));
        });
    }

    [Test]
    public async Task An_empty_tag_index_catalog_makes_no_registry_call()
    {
        var (query, recorder) = CreateQuery(["alpha"]);

        var page = await query.ListTagIndexesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(recorder.RegistryBatches, Is.Empty);
        });
    }

    [Test]
    public async Task The_tag_index_catalog_is_fail_closed_and_batches_nothing_without_a_factory()
    {
        // Visibility on but no ILatticeTagIndexFactory registered: coverage cannot
        // be proven, every index is hidden, and nothing may be read.
        var (query, recorder) = CreateQuery(
            [LatticeConstants.TagIndexTreePrefix + "colour"],
            allow: _ => true);

        var page = await query.ListTagIndexesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(recorder.RegistryBatches, Is.Empty);
            Assert.That(recorder.SingleEntryReads, Is.Empty);
        });
    }
}
