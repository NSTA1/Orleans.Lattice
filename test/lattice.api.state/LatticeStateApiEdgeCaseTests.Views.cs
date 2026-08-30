using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Materialised-view mapping, listing, source resolution, and sampling edge cases.
/// </summary>
public sealed partial class LatticeStateApiEdgeCaseTests
{
    [Test]
    public void Map_tree_derives_depth_and_split_counts_from_shards()
    {
        var report = new TreeDiagnosticReport
        {
            TreeId = "tree",
            ShardCount = 3,
            VirtualShardCount = 4096,
            TotalLiveKeys = 12,
            TotalTombstones = 2,
            Shards = ImmutableArray.Create(
                new ShardDiagnosticReport { ShardIndex = 0, Depth = 3, LiveKeys = 5, Tombstones = 1, SplitInProgress = false },
                new ShardDiagnosticReport { ShardIndex = 1, Depth = 1, LiveKeys = 7, Tombstones = 1, SplitInProgress = true }),
            SampledAt = DateTimeOffset.UnixEpoch,
        };

        var summary = InvokeStatic<TreeStateSummary>("MapTree", "tree", report, new TreeConfigSummary());

        Assert.Multiple(() =>
        {
            Assert.That(summary.MinDepth, Is.EqualTo(1));
            Assert.That(summary.MaxDepth, Is.EqualTo(3));
            Assert.That(summary.ShardsSplitting, Is.EqualTo(1));
        });
    }

    [Test]
    public void View_crdt_shape_degrades_to_null_for_missing_catalog_empty_name_catalog_fault_or_non_member_view()
    {
        var noCatalog = CreateQuery();
        var emptyName = CreateQuery(services: ServicesWithCatalog(new FixedViewCatalog()));
        var throwing = CreateQuery(services: ServicesWithCatalog(new ThrowingViewCatalog()));
        var aggregation = CreateQuery(services: ServicesWithCatalog(new FixedViewCatalog(
            Registration("orders-view", "orders", aggregation: true))));

        Assert.Multiple(() =>
        {
            Assert.That(InvokeInstance<string?>(noCatalog, "ResolveViewCrdtShape", "view-orders-view"), Is.Null);
            Assert.That(InvokeInstance<string?>(emptyName, "ResolveViewCrdtShape", "view-"), Is.Null);
            Assert.That(InvokeInstance<string?>(throwing, "ResolveViewCrdtShape", "view-orders-view"), Is.Null);
            Assert.That(InvokeInstance<string?>(aggregation, "ResolveViewCrdtShape", "view-orders-view"), Is.Null);
        });
    }

    [Test]
    public void View_crdt_shape_uses_source_tree_mode_for_key_preserving_views()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve("orders").Returns(LatticeMergeMode.OrSet);
        var services = ServicesWithCatalog(new FixedViewCatalog(Registration("orders-view", "orders")), resolver);
        var query = CreateQuery(services: services);

        var shape = InvokeInstance<string?>(query, "ResolveViewCrdtShape", "view-orders-view");

        Assert.That(shape, Is.EqualTo(LatticeMergeMode.OrSet.ToString()));
    }

    [Test]
    public void Aggregation_view_detection_is_conservative_when_lookup_is_unavailable()
    {
        var notAView = CreateQuery(services: ServicesWithCatalog(new FixedViewCatalog()));
        var noCatalog = CreateQuery();
        var emptyName = CreateQuery(services: ServicesWithCatalog(new FixedViewCatalog()));
        var throwing = CreateQuery(services: ServicesWithCatalog(new ThrowingViewCatalog()));

        Assert.Multiple(() =>
        {
            Assert.That(InvokeInstance<bool>(notAView, "IsAggregationViewTree", "orders"), Is.False);
            Assert.That(InvokeInstance<bool>(noCatalog, "IsAggregationViewTree", "view-orders-view"), Is.False);
            Assert.That(InvokeInstance<bool>(emptyName, "IsAggregationViewTree", "view-"), Is.False);
            Assert.That(InvokeInstance<bool>(throwing, "IsAggregationViewTree", "view-orders-view"), Is.False);
        });
    }

    [Test]
    public void Aggregation_view_scan_start_is_floored_only_for_reserved_aggregation_ranges()
    {
        var query = CreateQuery(services: ServicesWithCatalog(new FixedViewCatalog(
            Registration("rollup", "orders", aggregation: true),
            Registration("copy", "orders"))));

        Assert.Multiple(() =>
        {
            Assert.That(InvokeInstance<string?>(query, "ClampViewScanStart", "orders", null), Is.Null);
            Assert.That(InvokeInstance<string?>(query, "ClampViewScanStart", "view-copy", "\0reserved"), Is.EqualTo("\0reserved"));
            Assert.That(InvokeInstance<string?>(query, "ClampViewScanStart", "view-rollup", null), Is.EqualTo(AggregationRowCodec.FirstNonReservedKey));
            Assert.That(InvokeInstance<string?>(query, "ClampViewScanStart", "view-rollup", "\0reserved"), Is.EqualTo(AggregationRowCodec.FirstNonReservedKey));
            Assert.That(InvokeInstance<string?>(query, "ClampViewScanStart", "view-rollup", "group-a"), Is.EqualTo("group-a"));
        });
    }

    [Test]
    public async Task View_read_tree_resolution_handles_empty_view_names_active_generations_and_maintainer_failures()
    {
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns("view-orders#g1");
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IViewMaintainerGrain>("orders").Returns(maintainer);
        var query = CreateQuery(grainFactory: grainFactory);

        var empty = await InvokeInstanceAsync<string?>(query, "ResolveReadTreeIdAsync", "view-", CancellationToken.None);
        var active = await InvokeInstanceAsync<string?>(query, "ResolveReadTreeIdAsync", "view-orders", CancellationToken.None);

        var brokenMaintainer = Substitute.For<IViewMaintainerGrain>();
        brokenMaintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns<Task<string>>(_ => throw new InvalidOperationException("missing"));
        var brokenFactory = Substitute.For<IGrainFactory>();
        brokenFactory.GetGrain<IViewMaintainerGrain>("orders").Returns(brokenMaintainer);
        var brokenQuery = CreateQuery(grainFactory: brokenFactory);
        var fallback = await InvokeInstanceAsync<string?>(brokenQuery, "ResolveReadTreeIdAsync", "view-orders", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(empty, Is.Null);
            Assert.That(active, Is.EqualTo("view-orders#g1"));
            Assert.That(fallback, Is.EqualTo("view-orders"));
        });
    }

    [Test]
    public async Task Sample_view_reports_null_stats_for_missing_views_and_transient_sampling_faults()
    {
        var missingFactory = Substitute.For<ILatticeViewFactory>();
        missingFactory.GetAsync("missing", Arg.Any<CancellationToken>()).Returns(Task.FromResult<ILatticeView?>(null));

        var failingView = Substitute.For<ILatticeView>();
        failingView.GetLagAsync(Arg.Any<CancellationToken>()).Returns<Task<long>>(_ => throw new InvalidOperationException("unavailable"));
        var failingFactory = Substitute.For<ILatticeViewFactory>();
        failingFactory.GetAsync("broken", Arg.Any<CancellationToken>()).Returns(Task.FromResult<ILatticeView?>(failingView));

        var missing = await InvokeStaticAsync<(long? Lag, long? EntryCount)>("SampleViewAsync", missingFactory, "missing", CancellationToken.None);
        var failing = await InvokeStaticAsync<(long? Lag, long? EntryCount)>("SampleViewAsync", failingFactory, "broken", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(missing.Lag, Is.Null);
            Assert.That(missing.EntryCount, Is.Null);
            Assert.That(failing.Lag, Is.Null);
            Assert.That(failing.EntryCount, Is.Null);
        });
    }

    [Test]
    public async Task ListViewsAsync_filters_runtime_views_by_source_tree_access_before_paging()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
        [
            RuntimeView("view-a", "allowed-a"),
            RuntimeView("view-b", "denied"),
            RuntimeView("view-c", "allowed-c"),
        ]));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var query = CreateQuery(
            services: VisibilityServices(treeId => treeId.StartsWith("allowed", StringComparison.Ordinal)),
            grainFactory: grainFactory);

        var page = await query.ListViewsAsync(new CatalogRequest { PageSize = 1 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(view => view.ViewName), Is.EqualTo(new[] { "view-a" }));
            Assert.That(page.NextPageToken, Is.EqualTo("view-a"));
        });
    }

    [Test]
    public async Task Resolve_view_key_filter_returns_null_for_anonymous_or_unresolved_views()
    {
        var anonymous = CreateQuery(services: VisibilityServicesWithoutMembership());
        var anonymousFilter = await InvokeInstanceAsync<Func<string, bool>?>(
            anonymous,
            "ResolveViewKeyFilterAsync",
            "view-orders",
            CancellationToken.None);

        var unresolved = CreateQuery(services: VisibilityServices(_ => true));
        var unresolvedFilter = await InvokeInstanceAsync<Func<string, bool>?>(
            unresolved,
            "ResolveViewKeyFilterAsync",
            "view-orders",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(anonymousFilter, Is.Null);
            Assert.That(unresolvedFilter, Is.Null);
        });
    }

    [Test]
    public async Task ListViewsAsync_falls_back_to_catalog_when_runtime_registry_fails()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns<Task<IReadOnlyList<RuntimeViewRegistration>>>(_ => throw new InvalidOperationException("registry down"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var services = ServicesWithCatalog(new FixedViewCatalog(Registration("startup-view", "source")));
        var query = CreateQuery(services: services, grainFactory: grainFactory);

        var page = await query.ListViewsAsync(new CatalogRequest());

        Assert.That(page.Entries.Select(entry => entry.ViewName), Is.EqualTo(new[] { "startup-view" }));
    }


    [Test]
    public async Task Resolve_view_source_tree_id_falls_back_to_runtime_registry_and_fails_closed_on_registry_faults()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
        [
            RuntimeView("runtime-view", "runtime-source"),
        ]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var query = CreateQuery(grainFactory: grainFactory);

        var resolved = await InvokeInstanceAsync<string?>(
            query,
            "ResolveViewSourceTreeIdAsync",
            "view-runtime-view",
            CancellationToken.None);

        var brokenRegistry = Substitute.For<IViewRegistryGrain>();
        brokenRegistry.ListAsync().Returns<Task<IReadOnlyList<RuntimeViewRegistration>>>(_ => throw new InvalidOperationException("registry unavailable"));
        var brokenFactory = Substitute.For<IGrainFactory>();
        brokenFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(brokenRegistry);
        var broken = CreateQuery(grainFactory: brokenFactory);
        var hidden = await InvokeInstanceAsync<string?>(
            broken,
            "ResolveViewSourceTreeIdAsync",
            "view-runtime-view",
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.EqualTo("runtime-source"));
            Assert.That(hidden, Is.Null);
        });
    }
}
