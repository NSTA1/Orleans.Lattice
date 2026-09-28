using NSubstitute;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Regression coverage for resolving an installed app from the source its provenance names: with two sources
/// offering the same slug, activation, claim planning and resolution never report the installed app as
/// ambiguous, and never consult the other source.
/// </summary>
[TestFixture]
public sealed class AppSourceResolutionTests
{
    private static (AppSourceSet Set, TestCatalogSource A, TestCatalogSource B) TwoSourcesOffering(AppManifest manifest)
    {
        var a = new TestCatalogSource("feed-a").Publish(manifest);
        var b = new TestCatalogSource("feed-b").Publish(manifest);
        return (new AppSourceSet([a, b]), a, b);
    }

    [Test]
    public async Task ResolveFromAsync_with_a_key_asks_only_that_source()
    {
        var (set, a, b) = TwoSourcesOffering(ActivationHarness.Manifest());

        var result = await set.ResolveFromAsync(ActivationHarness.Slug, ActivationHarness.V1, "feed-b");

        Assert.That(result.IsResolved, Is.True);
        Assert.That(result.Provenance!.Source, Is.EqualTo("feed-b"));
        Assert.That(a.Resolutions, Is.Zero);
        Assert.That(b.Resolutions, Is.EqualTo(1));
    }

    [Test]
    public async Task ResolveFromAsync_without_a_key_reports_the_ambiguity()
    {
        var (set, _, _) = TwoSourcesOffering(ActivationHarness.Manifest());

        var result = await set.ResolveFromAsync(ActivationHarness.Slug, ActivationHarness.V1, sourceKey: null);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Ambiguous));
        Assert.That(result.SourceKeys, Is.EqualTo(new[] { "feed-a", "feed-b" }));
    }

    [Test]
    public async Task ResolveFromAsync_with_an_unknown_key_fails_closed_rather_than_asking_another_source()
    {
        var (set, a, b) = TwoSourcesOffering(ActivationHarness.Manifest());

        var result = await set.ResolveFromAsync(ActivationHarness.Slug, ActivationHarness.V1, "retired-feed");

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(a.Resolutions + b.Resolutions, Is.Zero);
    }

    [Test]
    public async Task ResolveFromAsync_on_a_source_that_is_not_a_set_ignores_the_key()
    {
        var source = Substitute.For<IAppSource>();
        var expected = AppSourceResult.NotFound(ActivationHarness.Slug);
        source.ResolveAsync(ActivationHarness.Slug, ActivationHarness.V1, Arg.Any<CancellationToken>()).Returns(new ValueTask<AppSourceResult>(expected));

        var result = await source.ResolveFromAsync(ActivationHarness.Slug, ActivationHarness.V1, "anything");

        Assert.That(result, Is.SameAs(expected));
    }

    [Test]
    public void ResolveFromAsync_rejects_a_null_source() =>
        Assert.Throws<ArgumentNullException>(() => AppSourceResolution.ResolveFromAsync(null!, ActivationHarness.Slug, null, null));

    [Test]
    public async Task ResolveInstalledAsync_uses_the_record_version_and_provenance_source()
    {
        var (set, a, b) = TwoSourcesOffering(ActivationHarness.Manifest());
        var record = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled) with { Provenance = new AppProvenance { Source = "feed-a" } };

        var result = await set.ResolveInstalledAsync(record);

        Assert.That(result.IsResolved, Is.True);
        Assert.That(result.Provenance!.Source, Is.EqualTo("feed-a"));
        Assert.That(b.Resolutions, Is.Zero);
    }

    [Test]
    public void ResolveInstalledAsync_rejects_a_null_record() =>
        Assert.Throws<ArgumentNullException>(() => new TestCatalogSource("feed-a").ResolveInstalledAsync(null!));

    [Test]
    public async Task Enable_activates_an_app_a_second_source_also_offers()
    {
        var manifest = ActivationHarness.Manifest();
        var (set, a, b) = TwoSourcesOffering(manifest);
        var harness = new ActivationHarness(source: set);
        var installed = await harness.Registry.InstallAsync(AppRegistryTestData.Request() with
        {
            Identity = manifest.Identity with { Provenance = new AppProvenance { Source = "feed-a" } },
        });
        Assert.That(installed.Succeeded, Is.True, installed.Message);
        var bBefore = b.Resolutions;

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.OwnedRuleIds(), Is.Not.Empty);
        Assert.That(a.Resolutions, Is.GreaterThan(0));
        Assert.That(b.Resolutions, Is.EqualTo(bBefore), "the other source offering the slug is never consulted");
    }

    [Test]
    public async Task Install_plans_its_tree_claims_from_the_source_the_provenance_names()
    {
        var manifest = ActivationHarness.Manifest();
        var (set, a, b) = TwoSourcesOffering(manifest);
        var harness = new ActivationHarness(source: set);

        var installed = await harness.Registry.InstallAsync(AppRegistryTestData.Request() with
        {
            Identity = manifest.Identity with { Provenance = new AppProvenance { Source = "feed-b" } },
        });

        Assert.That(installed.Succeeded, Is.True, installed.Message);
        Assert.That(a.Resolutions, Is.Zero);
        Assert.That(b.Resolutions, Is.EqualTo(1));
    }
}
