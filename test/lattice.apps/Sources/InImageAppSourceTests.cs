using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public partial class InImageAppSourceTests
{
    private static readonly AppSlug Slug = AppSlug.Parse("demo-app");
    private static readonly AppVersion Version = AppVersion.Parse("2.0.1");

    private static (InImageAppSource Source, FakeAppAssembly Assembly) CreateSource(string? json = null)
    {
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, json ?? SourceTestManifests.Minimal("demo-app", "2.0.1"));
        return (SourceTestManifests.Source(SourceTestManifests.Registration("demo-app", assembly)), assembly);
    }

    [Test]
    public async Task ResolveAsync_registered_slug_returns_manifest_provenance_and_handle()
    {
        var (source, assembly) = CreateSource();

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Resolved));
        Assert.That(result.IsResolved, Is.True);
        Assert.That(result.Slug, Is.EqualTo(Slug));
        Assert.That(result.Errors, Is.Empty);
        Assert.That(result.Manifest!.Identity.Slug, Is.EqualTo(Slug));
        Assert.That(result.Manifest.Identity.Version, Is.EqualTo(Version));
        Assert.That(result.Manifest.Trees.Select(t => t.Name), Is.EqualTo(new[] { "records" }));
        Assert.That(result.Provenance, Is.EqualTo(new AppProvenance
        {
            Source = InImageAppSource.SourceKey,
            Publisher = "first-party",
            Reference = "embedded:" + SourceTestManifests.ResourceName,
        }));
        Assert.That(result.Activation!.Identity, Is.SameAs(result.Manifest.Identity));
        Assert.That(result.RequestedVersion, Is.Null);
        Assert.That(result.AvailableVersion, Is.Null);
        Assert.That(assembly.ResourceReads, Is.EqualTo(1));
    }

    [Test]
    public async Task ResolveAsync_real_embedded_manifest_resolves_from_an_image_assembly()
    {
        var options = new InImageAppSourceOptions()
            .Register(AppSlug.Parse("tiny-app"), typeof(InImageAppSourceTests).Assembly, "test.tiny-app.json");
        var source = new InImageAppSource(Options.Create(options));

        var result = await source.ResolveAsync(AppSlug.Parse("tiny-app"), AppVersion.Parse("1.2.3-preview.1+test"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Resolved));
        Assert.That(result.Manifest!.Roles.Single().Name, Is.EqualTo("reader"));
        var activation = await result.Activation!.ActivateAsync();
        Assert.That(activation.Assembly, Is.SameAs(typeof(InImageAppSourceTests).Assembly));
    }

    [Test]
    public async Task ResolveAsync_matching_version_resolves_the_cached_result()
    {
        var (source, _) = CreateSource();

        var unversioned = await source.ResolveAsync(Slug);
        var versioned = await source.ResolveAsync(Slug, Version);

        Assert.That(versioned.Status, Is.EqualTo(AppSourceStatus.Resolved));
        Assert.That(versioned, Is.SameAs(unversioned));
    }

    [Test]
    public async Task ResolveAsync_unknown_slug_returns_not_found()
    {
        var (source, assembly) = CreateSource();

        var result = await source.ResolveAsync(AppSlug.Parse("other-app"), Version);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(result.IsResolved, Is.False);
        Assert.That(result.Slug, Is.EqualTo(AppSlug.Parse("other-app")));
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Activation, Is.Null);
        Assert.That(result.Provenance, Is.Null);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("not-found"));
        Assert.That(assembly.ResourceReads, Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_different_version_returns_version_mismatch_without_manifest()
    {
        var (source, _) = CreateSource();
        var requested = AppVersion.Parse("3.0.0");

        var result = await source.ResolveAsync(Slug, requested);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.VersionMismatch));
        Assert.That(result.RequestedVersion, Is.EqualTo(requested));
        Assert.That(result.AvailableVersion, Is.EqualTo(Version));
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Activation, Is.Null);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("version-mismatch"));
        Assert.That(result.Errors.Single().Path, Is.EqualTo("$.identity.version"));
    }

    [Test]
    public async Task ResolveAsync_version_comparison_is_exact_including_build_metadata()
    {
        var (source, _) = CreateSource();

        var result = await source.ResolveAsync(Slug, AppVersion.Parse("2.0.1+build.7"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.VersionMismatch));
    }

    [Test]
    public async Task ResolveAsync_retrieves_the_manifest_without_touching_app_code_or_activating()
    {
        // FakeAppAssembly throws on every type-system member, so success proves only the resource was read.
        var (source, assembly) = CreateSource();

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Manifest, Is.Not.Null);
        Assert.That(assembly.ResourceReads, Is.EqualTo(1));
        var activation = await result.Activation!.ActivateAsync();
        Assert.That(activation.IsActivated, Is.True);
        Assert.That(activation.Assembly, Is.SameAs(assembly));
        Assert.That(activation.Errors, Is.Empty);
    }

    [Test]
    public void Constructor_reads_no_resources()
    {
        var (_, assembly) = CreateSource();

        Assert.That(assembly.ResourceReads, Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_parses_each_manifest_once_and_returns_the_same_instance()
    {
        var (source, assembly) = CreateSource();

        var first = await source.ResolveAsync(Slug);
        for (var i = 0; i < 10; i++)
            Assert.That(await source.ResolveAsync(Slug), Is.SameAs(first));

        Assert.That(assembly.ResourceReads, Is.EqualTo(1));
    }

    [Test]
    public void ResolveAsync_concurrent_first_resolution_parses_once()
    {
        var (source, assembly) = CreateSource();
        var results = new AppSourceResult[64];

        Parallel.For(0, results.Length, i => results[i] = source.ResolveAsync(Slug).AsTask().GetAwaiter().GetResult());

        Assert.That(assembly.ResourceReads, Is.EqualTo(1));
        Assert.That(results.Distinct().Count(), Is.EqualTo(1));
    }

    [Test]
    public void ResolveAsync_completes_synchronously()
    {
        var (source, _) = CreateSource();

        Assert.That(source.ResolveAsync(Slug).IsCompletedSuccessfully, Is.True);
        Assert.That(source.ResolveAsync(AppSlug.Parse("other-app")).IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public async Task ResolveAsync_reports_the_registration_publisher_in_provenance()
    {
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("demo-app", "2.0.1"));
        var source = SourceTestManifests.Source(
            SourceTestManifests.Registration("demo-app", assembly) with { Publisher = "contoso" });

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Provenance!.Publisher, Is.EqualTo("contoso"));
        Assert.That(result.Provenance.Source, Is.EqualTo("in-image"));
    }

    [Test]
    public async Task ActivateAsync_is_idempotent()
    {
        var (source, _) = CreateSource();
        var handle = (await source.ResolveAsync(Slug)).Activation!;

        var first = await handle.ActivateAsync();
        var second = await handle.ActivateAsync();

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void Constructor_null_options_throws()
    {
        Assert.Throws<ArgumentNullException>(() => new InImageAppSource(null!));
        Assert.Throws<ArgumentNullException>(() => new InImageAppSource(Options.Create<InImageAppSourceOptions>(null!)));
    }

    [Test]
    public async Task Constructor_skips_null_registrations()
    {
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(null!);
        var source = new InImageAppSource(Options.Create(options));

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
    }
}
