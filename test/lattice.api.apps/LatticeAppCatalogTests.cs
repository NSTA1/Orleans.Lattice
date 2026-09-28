using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAppCatalog"/>: the admin gate on every verb, the source projection,
/// pre-install description and verified icons.
/// </summary>
[TestFixture]
public sealed partial class LatticeAppCatalogTests
{
    private static CatalogHarness TwoSources(out TestCatalogSource inImage, out TestCatalogSource feed)
    {
        inImage = new TestCatalogSource("in-image").Publish(CatalogHarness.UiManifest("crm")).WithUiAssets();
        feed = new TestCatalogSource("feed", AppSourceKind.Dynamic, AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search)
            .Publish(CatalogHarness.UiManifest("crm", "2.0.0"))
            .Publish(CatalogHarness.UiManifest("crm", "1.0.0"));
        return new CatalogHarness().With(inImage, feed);
    }

    [Test]
    public async Task A_denied_caller_touches_no_source_or_registry_on_any_verb()
    {
        var harness = TwoSources(out _, out _);
        harness.Deny();
        var catalog = harness.Catalog;

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => catalog.ListSourcesAsync());
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => catalog.ListAvailableAsync(new AvailableAppQuery()));
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => catalog.DescribeFromSourceAsync("in-image", "crm"));
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => catalog.GetIconAsync("in-image", "crm"));
        Assert.That(await catalog.GetCapabilitiesAsync(), Is.EqualTo(new LatticeAppCatalogCapabilities()));

        harness.AssertNothingTouched();
        Assert.That(harness.Gate.Requests, Is.Not.Empty);
        Assert.That(harness.Gate.Requests.Select(r => r.Operation), Is.All.EqualTo(LatticeOperation.AppInstall));
        Assert.That(harness.Gate.Requests.Select(r => r.TreeId), Is.All.EqualTo(LatticeScope.ClusterWideTreeId));
    }

    [Test]
    public async Task Capabilities_grant_everything_to_an_installer()
    {
        var capabilities = await TwoSources(out _, out _).Catalog.GetCapabilitiesAsync();

        Assert.That(capabilities, Is.EqualTo(new LatticeAppCatalogCapabilities
        {
            CanListSources = true,
            CanListAvailable = true,
            CanDescribeFromSource = true,
            CanGetIcon = true,
        }));
    }

    [Test]
    public async Task ListSources_projects_every_source_in_registration_order()
    {
        var sources = await TwoSources(out _, out _).Catalog.ListSourcesAsync();

        Assert.That(sources.Select(s => s.Key), Is.EqualTo(new[] { "in-image", "feed" }));
        Assert.That(sources[0], Is.EqualTo(new AppSourceSummary
        {
            Key = "in-image",
            DisplayName = "Source in-image",
            Kind = AppSourceSummaryKind.Static,
            Capabilities = AppSourceSummaryCapabilities.Enumerate,
        }));
        Assert.That(sources[1].Kind, Is.EqualTo(AppSourceSummaryKind.Dynamic));
        Assert.That(sources[1].Capabilities, Is.EqualTo(AppSourceSummaryCapabilities.Enumerate | AppSourceSummaryCapabilities.Search));
    }

    [Test]
    public async Task ListSources_is_empty_when_no_source_is_configured() =>
        Assert.That(await new CatalogHarness().Catalog.ListSourcesAsync(), Is.Empty);

    [Test]
    public void ToSourceSet_adapts_every_registered_source_shape()
    {
        var set = new AppSourceSet([new TestCatalogSource("a-src")]);
        var lone = new TestCatalogSource("b-src");

        Assert.That(LatticeAppCatalog.ToSourceSet(set), Is.SameAs(set));
        Assert.That(LatticeAppCatalog.ToSourceSet(lone).Sources.Single(), Is.SameAs(lone));
        Assert.That(LatticeAppCatalog.ToSourceSet(Substitute.For<IAppSource>()).Sources, Is.Empty);
    }

    [Test]
    public async Task DescribeFromSource_describes_the_named_sources_version_with_presentation_and_ui()
    {
        var harness = TwoSources(out var inImage, out var feed);

        var descriptor = await harness.Catalog.DescribeFromSourceAsync("feed", "crm");

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Version, Is.EqualTo("2.0.0"));
        Assert.That(descriptor.SourceKey, Is.EqualTo("feed"));
        Assert.That(descriptor.Provenance.Source, Is.EqualTo("feed"));
        Assert.That(descriptor.State, Is.EqualTo(AppLifecycleState.NotInstalled));
        Assert.That(descriptor.Presentation!.DisplayName, Is.EqualTo("Notes <b>app</b>"));
        Assert.That(descriptor.Presentation.Icon!.Path, Is.EqualTo(UiTestManifests.IconPath));
        Assert.That(descriptor.Ui!.Entry, Is.EqualTo(UiTestManifests.EntryPath));
        Assert.That(inImage.Resolutions, Is.Zero, "only the named source is asked");
        Assert.That(feed.Resolutions, Is.EqualTo(1));
    }

    [Test]
    public async Task DescribeFromSource_selects_an_exact_version()
    {
        var descriptor = await TwoSources(out _, out _).Catalog.DescribeFromSourceAsync("feed", "crm", "1.0.0");

        Assert.That(descriptor!.Version, Is.EqualTo("1.0.0"));
    }

    [Test]
    public async Task DescribeFromSource_joins_the_install_only_when_it_came_from_that_source()
    {
        var harness = TwoSources(out _, out _);
        harness.Installs(CatalogHarness.Installed("crm", "1.0.0", "in-image", AppRegistryLifecycleState.Enabled));

        var fromInstallSource = await harness.Catalog.DescribeFromSourceAsync("in-image", "crm");
        var fromOtherSource = await harness.Catalog.DescribeFromSourceAsync("feed", "crm", "1.0.0");

        Assert.That(fromInstallSource!.State, Is.EqualTo(AppLifecycleState.Enabled));
        Assert.That(fromInstallSource.Ceiling, Is.Not.Null);
        Assert.That(fromOtherSource!.State, Is.EqualTo(AppLifecycleState.NotInstalled));
        Assert.That(fromOtherSource.Ceiling, Is.Null);
    }

    [TestCase("absent-source", "crm", null)]
    [TestCase("feed", "unknown", null)]
    [TestCase("feed", "crm", "9.9.9")]
    public async Task DescribeFromSource_returns_null_for_an_unknown_source_app_or_version(string source, string slug, string? version) =>
        Assert.That(await TwoSources(out _, out _).Catalog.DescribeFromSourceAsync(source, slug, version), Is.Null);

    [TestCase("", "crm", null)]
    [TestCase("Bad Key", "crm", null)]
    [TestCase("feed", "Not A Slug", null)]
    [TestCase("feed", "crm", "not-semver")]
    public void Malformed_input_is_rejected_before_authorization(string source, string slug, string? version)
    {
        var harness = TwoSources(out _, out _);

        Assert.ThrowsAsync<ArgumentException>(() => harness.Catalog.DescribeFromSourceAsync(source, slug, version));
        Assert.ThrowsAsync<ArgumentException>(() => harness.Catalog.GetIconAsync(source, slug, version));
        Assert.That(harness.Gate.Requests, Is.Empty);
        harness.AssertNothingTouched();
    }

    [Test]
    public async Task GetIcon_returns_the_verified_icon_bytes()
    {
        var icon = await TwoSources(out _, out _).Catalog.GetIconAsync("in-image", "crm");

        Assert.That(icon, Is.Not.Null);
        Assert.That(icon!.Bytes.ToArray(), Is.EqualTo(UiTestManifests.IconBytes));
        Assert.That(icon.MediaType, Is.EqualTo("image/svg+xml"));
        Assert.That(icon.Sha256, Is.EqualTo(UiTestManifests.Sha256(UiTestManifests.IconBytes)));
    }

    [Test]
    public async Task GetIcon_returns_null_rather_than_bytes_that_fail_digest_verification()
    {
        var source = new TestCatalogSource("in-image").Publish(CatalogHarness.UiManifest("crm"))
            .WithAsset(UiTestManifests.IconPath, "<svg>tampered</svg>"u8.ToArray());

        Assert.That(await new CatalogHarness().With(source).Catalog.GetIconAsync("in-image", "crm"), Is.Null);
        Assert.That(source.AssetOpens, Is.EqualTo(1));
    }

    [Test]
    public async Task GetIcon_returns_null_for_an_app_without_an_icon()
    {
        var source = new TestCatalogSource("in-image").Publish(CatalogHarness.Manifest("crm"));

        Assert.That(await new CatalogHarness().With(source).Catalog.GetIconAsync("in-image", "crm"), Is.Null);
        Assert.That(source.AssetOpens, Is.Zero);
    }

    [Test]
    public async Task DescribeFromSource_describes_an_app_a_dynamic_source_offers_in_several_versions()
    {
        var feed = new FakeDynamicAppSource("fake-feed");
        feed.Add("tasks", "2.0.0").Add("tasks", "1.0.0");

        var newest = await new CatalogHarness().With(feed).Catalog.DescribeFromSourceAsync("fake-feed", "tasks");
        var older = await new CatalogHarness().With(feed).Catalog.DescribeFromSourceAsync("fake-feed", "tasks", "1.0.0");

        Assert.That(newest!.Version, Is.EqualTo("2.0.0"));
        Assert.That(newest.SourceKey, Is.EqualTo("fake-feed"));
        Assert.That(newest.Presentation, Is.Null);
        Assert.That(newest.Ui, Is.Null);
        Assert.That(older!.Version, Is.EqualTo("1.0.0"));
    }

    [Test]
    public void A_source_that_cannot_describe_the_app_fails_with_the_facades_precondition_failure()
    {
        var broken = Substitute.For<IAppCatalogSource>();
        broken.Descriptor.Returns(new AppSourceDescriptor("broken", "Broken", AppSourceKind.Static, AppSourceCapabilities.Enumerate));
        broken.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<AppSourceResult>(AppSourceResult.InvalidManifest(AppSlug.Parse("crm"), [new AppManifestError("bad", "$", "broken")])));

        Assert.ThrowsAsync<InvalidOperationException>(() => new CatalogHarness().With(broken).Catalog.DescribeFromSourceAsync("broken", "crm"));
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var harness = new CatalogHarness();
        var set = new AppSourceSet([]);
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new LatticeAppCatalog(null!, harness.Registry, harness.Pipeline, harness.Gate, harness.Tenants));
            Assert.Throws<ArgumentNullException>(() => new LatticeAppCatalog(set, null!, harness.Pipeline, harness.Gate, harness.Tenants));
            Assert.Throws<ArgumentNullException>(() => new LatticeAppCatalog(set, harness.Registry, null!, harness.Gate, harness.Tenants));
            Assert.Throws<ArgumentNullException>(() => new LatticeAppCatalog(set, harness.Registry, harness.Pipeline, null!, harness.Tenants));
            Assert.Throws<ArgumentNullException>(() => new LatticeAppCatalog(set, harness.Registry, harness.Pipeline, harness.Gate, null!));
        });
    }
}
