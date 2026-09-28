using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

public sealed partial class LatticeAppCatalogTests
{
    private static CatalogHarness MergeHarness(out TestCatalogSource alpha, out TestCatalogSource beta)
    {
        alpha = new TestCatalogSource("alpha");
        foreach (var slug in new[] { "crm", "notes", "zeta" })
            alpha.Publish(CatalogHarness.Manifest(slug));
        beta = new TestCatalogSource("beta");
        foreach (var slug in new[] { "ab", "crm", "zz" })
            beta.Publish(CatalogHarness.Manifest(slug));
        return new CatalogHarness().With(alpha, beta);
    }

    private static async Task<List<AvailableAppPage>> DrainAsync(ILatticeAppCatalog catalog, AvailableAppQuery query)
    {
        var pages = new List<AvailableAppPage>();
        string? continuation = null;
        do
        {
            var page = await catalog.ListAvailableAsync(query with { Continuation = continuation });
            pages.Add(page);
            continuation = page.Continuation;
            Assert.That(pages, Has.Count.LessThan(50), "the listing must terminate");
        }
        while (continuation is not null);

        return pages;
    }

    private static readonly string[] MergedOrder = ["ab@beta", "crm@alpha", "crm@beta", "notes@alpha", "zeta@alpha", "zz@beta"];

    [TestCase(1)]
    [TestCase(2)]
    [TestCase(4)]
    [TestCase(50)]
    public async Task ListAvailable_merges_every_source_by_slug_then_source_key_across_pages(int pageSize)
    {
        var harness = MergeHarness(out _, out _);

        var pages = await DrainAsync(harness.Catalog, new AvailableAppQuery { PageSize = pageSize });

        Assert.That(pages.SelectMany(p => p.Apps).Select(a => $"{a.Slug}@{a.SourceKey}"), Is.EqualTo(MergedOrder));
        Assert.That(pages.Take(pages.Count - 1).Select(p => p.Apps.Length), Is.All.EqualTo(pageSize));
        Assert.That(pages[^1].Continuation, Is.Null);
    }

    [Test]
    public async Task ListAvailable_returns_no_continuation_when_everything_fits()
    {
        var page = await MergeHarness(out _, out _).Catalog.ListAvailableAsync(new AvailableAppQuery());

        Assert.That(page.Apps, Has.Length.EqualTo(MergedOrder.Length));
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ListAvailable_lists_only_the_selected_source()
    {
        var harness = MergeHarness(out var alpha, out var beta);

        var page = await harness.Catalog.ListAvailableAsync(new AvailableAppQuery { SourceKey = "beta" });

        Assert.That(page.Apps.Select(a => a.Slug), Is.EqualTo(new[] { "ab", "crm", "zz" }));
        Assert.That(page.Apps.Select(a => a.SourceKey), Is.All.EqualTo("beta"));
        Assert.That(alpha.Listings, Is.Zero);
        Assert.That(beta.Listings, Is.EqualTo(1));
    }

    [Test]
    public async Task ListAvailable_of_an_unknown_source_is_an_empty_final_page()
    {
        var page = await MergeHarness(out _, out _).Catalog.ListAvailableAsync(new AvailableAppQuery { SourceKey = "gamma" });

        Assert.That(page.Apps, Is.Empty);
        Assert.That(page.Continuation, Is.Null);
    }

    [TestCase("not base64 at all!")]
    [TestCase("MQ")]
    [TestCase("eHh4")]
    public async Task ListAvailable_with_a_malformed_continuation_is_an_empty_final_page(string continuation)
    {
        var page = await MergeHarness(out _, out _).Catalog.ListAvailableAsync(new AvailableAppQuery { Continuation = continuation });

        Assert.That(page.Apps, Is.Empty);
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ListAvailable_refuses_a_continuation_issued_for_another_source_selection()
    {
        var catalog = MergeHarness(out _, out _).Catalog;
        var first = await catalog.ListAvailableAsync(new AvailableAppQuery { PageSize = 1 });

        var foreign = await catalog.ListAvailableAsync(new AvailableAppQuery { SourceKey = "alpha", Continuation = first.Continuation });

        Assert.That(first.Continuation, Is.Not.Null);
        Assert.That(foreign.Apps, Is.Empty);
        Assert.That(foreign.Continuation, Is.Null);
    }

    [Test]
    public void ListAvailable_rejects_invalid_queries_before_authorization()
    {
        var harness = MergeHarness(out _, out _);

        Assert.ThrowsAsync<ArgumentNullException>(() => harness.Catalog.ListAvailableAsync(null!));
        Assert.ThrowsAsync<ArgumentException>(() => harness.Catalog.ListAvailableAsync(new AvailableAppQuery { Filter = (AvailableAppFilter)99 }));
        Assert.ThrowsAsync<ArgumentException>(() => harness.Catalog.ListAvailableAsync(new AvailableAppQuery { SourceKey = "Not A Key" }));
        Assert.That(harness.Gate.Requests, Is.Empty);
    }

    [TestCase(0, 1)]
    [TestCase(-5, 1)]
    [TestCase(1000, AvailableAppQuery.MaxPageSize)]
    public async Task ListAvailable_clamps_the_page_size(int requested, int expected)
    {
        var feed = new FakeDynamicAppSource("fake-feed");
        for (var i = 0; i < AvailableAppQuery.MaxPageSize + 5; i++)
            feed.Add($"app-{i:D3}", "1.0.0");

        var page = await new CatalogHarness().With(feed).Catalog.ListAvailableAsync(new AvailableAppQuery { PageSize = requested });

        Assert.That(page.Apps, Has.Length.EqualTo(expected));
        Assert.That(page.Continuation, Is.Not.Null);
    }

    [Test]
    public async Task ListAvailable_joins_an_install_only_with_the_row_of_its_own_source()
    {
        var harness = MergeHarness(out _, out _);
        harness.Installs(CatalogHarness.Installed("crm", "1.0.0", "beta", AppRegistryLifecycleState.Enabled));

        var page = await harness.Catalog.ListAvailableAsync(new AvailableAppQuery());

        var crmAlpha = page.Apps.Single(a => a.Slug == "crm" && a.SourceKey == "alpha");
        var crmBeta = page.Apps.Single(a => a.Slug == "crm" && a.SourceKey == "beta");
        Assert.That(crmAlpha.InstalledVersion, Is.Null);
        Assert.That(crmAlpha.InstalledState, Is.Null);
        Assert.That(crmBeta.InstalledVersion, Is.EqualTo("1.0.0"));
        Assert.That(crmBeta.InstalledState, Is.EqualTo(AppLifecycleState.Enabled));
    }

    [Test]
    public async Task ListAvailable_reports_a_failed_activation_as_the_installed_state()
    {
        var harness = MergeHarness(out _, out _);
        harness.Installs(CatalogHarness.Installed("crm", "1.0.0", "beta", AppRegistryLifecycleState.Enabled));
        harness.Pipeline.GetStatusAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Status(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.TreeProvisioningFailed)));

        var page = await harness.Catalog.ListAvailableAsync(new AvailableAppQuery { Filter = AvailableAppFilter.Installed });

        Assert.That(page.Apps.Single().InstalledState, Is.EqualTo(AppLifecycleState.Failed));
    }

    [Test]
    public async Task ListAvailable_filters_installed_available_and_update_rows_against_the_tenant()
    {
        var local = new TestCatalogSource("in-image").Publish(CatalogHarness.Manifest("crm")).Publish(CatalogHarness.Manifest("notes"));
        var feed = new TestCatalogSource("feed")
            .Publish(CatalogHarness.Manifest("billing", "2.0.0")).Publish(CatalogHarness.Manifest("billing", "1.0.0"))
            .Publish(CatalogHarness.Manifest("crm", "3.0.0"))
            .Publish(CatalogHarness.Manifest("tasks", "1.0.0-rc.1"));
        var harness = new CatalogHarness().With(local, feed);
        harness.Installs(
            CatalogHarness.Installed("crm", "1.0.0", "in-image"),
            CatalogHarness.Installed("billing", "1.0.0", "feed"),
            CatalogHarness.Installed("tasks", "1.0.0", "feed"),
            CatalogHarness.Installed("notes", "1.0.0", "in-image", AppRegistryLifecycleState.Uninstalled));

        async Task<string[]> RowsAsync(AvailableAppFilter filter) =>
            (await harness.Catalog.ListAvailableAsync(new AvailableAppQuery { Filter = filter })).Apps.Select(a => $"{a.Slug}@{a.SourceKey}").ToArray();

        Assert.That(await RowsAsync(AvailableAppFilter.Installed), Is.EqualTo(new[] { "billing@feed", "crm@in-image", "tasks@feed" }));
        Assert.That(await RowsAsync(AvailableAppFilter.Available), Is.EqualTo(new[] { "notes@in-image" }));
        Assert.That(await RowsAsync(AvailableAppFilter.Updates), Is.EqualTo(new[] { "billing@feed" }), "a newer version from the same source only; a prerelease of an installed release is not newer");
        Assert.That(await RowsAsync(AvailableAppFilter.All), Has.Length.EqualTo(5));
    }

    [Test]
    public async Task ListAvailable_reports_versions_newest_first_with_presentation_and_ui()
    {
        var feed = new TestCatalogSource("feed").Publish(CatalogHarness.UiManifest("crm", "2.0.0")).Publish(CatalogHarness.Manifest("crm", "1.0.0"));

        var app = (await new CatalogHarness().With(feed).Catalog.ListAvailableAsync(new AvailableAppQuery())).Apps.Single();

        Assert.That(app.NewestVersion, Is.EqualTo("2.0.0"));
        Assert.That(app.AvailableVersions, Is.EqualTo(new[] { "2.0.0", "1.0.0" }));
        Assert.That(app.HasUi, Is.True);
        Assert.That(app.Presentation!.Categories, Is.EqualTo(new[] { "productivity" }));
    }

    [Test]
    public async Task ListAvailable_passes_the_text_filter_to_sources_that_search()
    {
        var feed = new TestCatalogSource("feed", AppSourceKind.Dynamic, AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search)
            .Publish(CatalogHarness.Manifest("crm")).Publish(CatalogHarness.Manifest("notes"));

        var page = await new CatalogHarness().With(feed).Catalog.ListAvailableAsync(new AvailableAppQuery { Text = "not" });

        Assert.That(page.Apps.Select(a => a.Slug), Is.EqualTo(new[] { "notes" }));
    }

    [Test]
    public async Task ListAvailable_treats_a_faulting_source_as_exhausted_and_still_lists_the_others()
    {
        var harness = MergeHarness(out var alpha, out _);
        alpha.ListFault = new InvalidOperationException("feed offline");

        var page = await harness.Catalog.ListAvailableAsync(new AvailableAppQuery());

        Assert.That(page.Apps.Select(a => a.SourceKey), Is.All.EqualTo("beta"));
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ListAvailable_skips_an_entry_its_source_cannot_describe()
    {
        var source = Substitute.For<IAppCatalogSource>();
        source.Descriptor.Returns(new AppSourceDescriptor("broken", "Broken", AppSourceKind.Static, AppSourceCapabilities.Enumerate));
        source.ListAsync(Arg.Any<AppSourceQuery>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<AppSourcePage>(AppSourcePage.Create(
            [
                AppSourceEntry.Unavailable(AppSlug.Parse("bad"), [new AppManifestError("bad", "$", "unreadable")]),
                AppSourceEntry.Available([AppVersion.Parse("1.0.0")], CatalogHarness.Manifest("crm"), new AppProvenance { Source = "broken" }),
            ],
            null)));

        var page = await new CatalogHarness().With(source).Catalog.ListAvailableAsync(new AvailableAppQuery());

        Assert.That(page.Apps.Select(a => a.Slug), Is.EqualTo(new[] { "crm" }));
    }

    [Test]
    public async Task ListAvailable_bounds_the_source_pages_one_call_fetches()
    {
        var source = Substitute.For<IAppCatalogSource>();
        source.Descriptor.Returns(new AppSourceDescriptor("empty-pages", "Empty", AppSourceKind.Dynamic, AppSourceCapabilities.Enumerate));
        source.ListAsync(Arg.Any<AppSourceQuery>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<AppSourcePage>(AppSourcePage.Create([], "more")));

        var page = await new CatalogHarness().With(source).Catalog.ListAvailableAsync(new AvailableAppQuery());

        Assert.That(page.Apps, Is.Empty);
        Assert.That(page.Continuation, Is.Not.Null, "a short page may still carry a continuation");
        await source.Received(LatticeAppCatalog.MaxFetchesPerCall).ListAsync(Arg.Any<AppSourceQuery>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ListAvailable_merges_the_in_image_source_with_a_dynamic_source()
    {
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("notes", "1.0.0"));
        var inImage = SourceTestManifests.Source(SourceTestManifests.Registration("notes", assembly));
        var feed = new FakeDynamicAppSource("fake-feed");
        feed.Add("notes", "2.0.0").Add("alpha", "1.0.0");

        var pages = await DrainAsync(new CatalogHarness().With(inImage, feed).Catalog, new AvailableAppQuery { PageSize = 1 });

        Assert.That(pages.SelectMany(p => p.Apps).Select(a => $"{a.Slug}@{a.SourceKey}"),
            Is.EqualTo(new[] { "alpha@fake-feed", "notes@fake-feed", "notes@in-image" }));
    }
}
