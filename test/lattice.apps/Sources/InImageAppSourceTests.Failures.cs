namespace Orleans.Lattice.Apps.Tests;

public partial class InImageAppSourceTests
{
    [Test]
    public async Task ResolveAsync_malformed_json_returns_invalid_manifest_with_parser_errors()
    {
        var (source, _) = CreateSource("{ not json");

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Activation, Is.Null);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("json"));
    }

    [Test]
    public async Task ResolveAsync_manifest_failing_validation_carries_the_validator_errors()
    {
        var json = SourceTestManifests.Minimal("demo-app", "2.0.1").Replace("\"records\"", "\"Bad/Name\"");
        var (source, _) = CreateSource(json);

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(result.Errors.Select(e => e.Path), Does.Contain("$.trees[0].name"));
    }

    [Test]
    public async Task ResolveAsync_invalid_manifest_is_reported_even_when_a_version_is_requested()
    {
        var (source, _) = CreateSource("{ not json");

        var result = await source.ResolveAsync(Slug, AppVersion.Parse("9.9.9"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
    }

    [Test]
    public async Task ResolveAsync_missing_resource_returns_invalid_manifest()
    {
        var assembly = new FakeAppAssembly(_ => null);
        var source = SourceTestManifests.Source(SourceTestManifests.Registration("demo-app", assembly));

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("resource"));
    }

    [Test]
    public async Task ResolveAsync_throwing_resource_provider_returns_invalid_manifest_and_caches_it()
    {
        var assembly = new FakeAppAssembly(_ => throw new NotSupportedException("Injected provider failure."));
        var source = SourceTestManifests.Source(SourceTestManifests.Registration("demo-app", assembly));

        var first = await source.ResolveAsync(Slug);
        var second = await source.ResolveAsync(Slug);

        Assert.That(first.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(first.Errors.Single().Code, Is.EqualTo("resource"));
        Assert.That(first.Errors.Single().Message, Does.Contain("Injected provider failure."));
        Assert.That(second, Is.SameAs(first));
        Assert.That(assembly.ResourceReads, Is.EqualTo(1));
    }

    [Test]
    public async Task ResolveAsync_io_failure_returns_invalid_manifest()
    {
        var assembly = new FakeAppAssembly(_ => new FailingManifestStream());
        var source = SourceTestManifests.Source(SourceTestManifests.Registration("demo-app", assembly));

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("io"));
    }

    [Test]
    public async Task ResolveAsync_manifest_declaring_a_different_slug_returns_identity_mismatch()
    {
        var (source, _) = CreateSource(SourceTestManifests.Minimal("impostor", "2.0.1"));

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.IdentityMismatch));
        Assert.That(result.Slug, Is.EqualTo(Slug));
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Activation, Is.Null);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("identity-mismatch"));
        Assert.That(result.Errors.Single().Message, Does.Contain("impostor"));
    }

    [Test]
    public async Task ResolveAsync_duplicate_registration_returns_duplicate_without_reading_resources()
    {
        var first = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("demo-app", "2.0.1"));
        var second = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("demo-app", "2.0.2"));
        var source = SourceTestManifests.Source(
            SourceTestManifests.Registration("demo-app", first),
            SourceTestManifests.Registration("demo-app", second));

        var result = await source.ResolveAsync(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.DuplicateRegistration));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("duplicate"));
        Assert.That(first.ResourceReads + second.ResourceReads, Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_failure_of_one_registration_does_not_affect_another()
    {
        var broken = new FakeAppAssembly(SourceTestManifests.ResourceName, "{ not json");
        var healthy = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("demo-app", "2.0.1"));
        var source = SourceTestManifests.Source(
            SourceTestManifests.Registration("broken-app", broken),
            SourceTestManifests.Registration("demo-app", healthy));

        Assert.That((await source.ResolveAsync(AppSlug.Parse("broken-app"))).Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That((await source.ResolveAsync(Slug)).Status, Is.EqualTo(AppSourceStatus.Resolved));
    }
}
