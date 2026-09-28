namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppSourceResultTests
{
    private static readonly AppSlug Slug = AppSlug.Parse("demo-app");

    private static AppManifest Manifest() => new()
    {
        Identity = new() { Slug = Slug, Version = AppVersion.Parse("1.0.0") },
        Trees = [],
        Roles = [],
        Subscriptions = [],
        McpTools = [],
    };

    [Test]
    public void Resolved_exposes_manifest_provenance_and_handle()
    {
        var manifest = Manifest();
        var provenance = new AppProvenance();
        var handle = new InImageAppActivationHandle(manifest.Identity, typeof(AppSourceResultTests).Assembly);

        var result = AppSourceResult.Resolved(manifest, provenance, handle);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Resolved));
        Assert.That(result.IsResolved, Is.True);
        Assert.That(result.Slug, Is.EqualTo(Slug));
        Assert.That(result.Manifest, Is.SameAs(manifest));
        Assert.That(result.Provenance, Is.SameAs(provenance));
        Assert.That(result.Activation, Is.SameAs(handle));
        Assert.That(result.Errors, Is.Empty);
    }

    [Test]
    public void Resolved_null_arguments_throw()
    {
        var manifest = Manifest();
        var handle = new InImageAppActivationHandle(manifest.Identity, typeof(AppSourceResultTests).Assembly);

        Assert.Throws<ArgumentNullException>(() => AppSourceResult.Resolved(null!, new(), handle));
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.Resolved(manifest with { Identity = null! }, new(), handle));
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.Resolved(manifest, null!, handle));
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.Resolved(manifest, new(), null!));
    }

    [Test]
    public void NotFound_carries_a_not_found_error()
    {
        var result = AppSourceResult.NotFound(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(result.Slug, Is.EqualTo(Slug));
        Assert.That(result.Errors.Single(), Is.EqualTo(new AppManifestError(
            "not-found", "$.identity.slug", "No app 'demo-app' is available from this source.")));
    }

    [Test]
    public void VersionMismatch_carries_both_versions()
    {
        var requested = AppVersion.Parse("2.0.0");
        var available = AppVersion.Parse("1.0.0");

        var result = AppSourceResult.VersionMismatch(Slug, requested, available);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.VersionMismatch));
        Assert.That(result.RequestedVersion, Is.EqualTo(requested));
        Assert.That(result.AvailableVersion, Is.EqualTo(available));
        Assert.That(result.Errors.Single().Message, Does.Contain("'1.0.0'").And.Contain("'2.0.0'"));
    }

    [Test]
    public void InvalidManifest_copies_the_errors()
    {
        var errors = new List<AppManifestError> { new("json", "$", "Bad.") };

        var result = AppSourceResult.InvalidManifest(Slug, errors);
        errors.Clear();

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.InvalidManifest));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("json"));
        Assert.That(result.Manifest, Is.Null);
    }

    [Test]
    public void InvalidManifest_rejects_null_empty_or_null_containing_errors()
    {
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.InvalidManifest(Slug, null!));
        Assert.Throws<ArgumentException>(() => AppSourceResult.InvalidManifest(Slug, []));
        Assert.Throws<ArgumentException>(() => AppSourceResult.InvalidManifest(Slug, [null!]));
    }

    [Test]
    public void IdentityMismatch_names_both_slugs()
    {
        var result = AppSourceResult.IdentityMismatch(Slug, AppSlug.Parse("impostor"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.IdentityMismatch));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("identity-mismatch"));
        Assert.That(result.Errors.Single().Message, Does.Contain("impostor").And.Contain("demo-app"));
    }

    [Test]
    public void DuplicateRegistration_carries_a_duplicate_error()
    {
        var result = AppSourceResult.DuplicateRegistration(Slug);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.DuplicateRegistration));
        Assert.That(result.IsResolved, Is.False);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("duplicate"));
    }

    [Test]
    public void UnknownSource_is_not_found_and_names_the_key()
    {
        var result = AppSourceResult.UnknownSource(Slug, "nowhere");

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(result.Errors.Single(), Is.EqualTo(new AppManifestError(
            "unknown-source", "$.source", "No app source 'nowhere' is configured.")));
        Assert.That(result.SourceKeys, Is.Empty);
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.UnknownSource(Slug, null!));
    }

    [Test]
    public void Ambiguous_copies_and_lists_the_source_keys()
    {
        var keys = new List<string> { "in-image", "feed" };

        var result = AppSourceResult.Ambiguous(Slug, keys);
        keys.Clear();

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Ambiguous));
        Assert.That(result.IsResolved, Is.False);
        Assert.That(result.SourceKeys, Is.EqualTo(new[] { "in-image", "feed" }));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("ambiguous"));
        Assert.That(result.Errors.Single().Message, Does.Contain("in-image, feed"));
    }

    [Test]
    public void Ambiguous_rejects_null_fewer_than_two_or_null_containing_keys()
    {
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.Ambiguous(Slug, null!));
        Assert.Throws<ArgumentException>(() => AppSourceResult.Ambiguous(Slug, ["only"]));
        Assert.Throws<ArgumentException>(() => AppSourceResult.Ambiguous(Slug, ["a", null!]));
    }

    [Test]
    public void SourceMisconfigured_copies_the_errors()
    {
        var errors = new List<AppManifestError> { new("duplicate-source", "$.sources[1].key", "Twice.") };

        var result = AppSourceResult.SourceMisconfigured(Slug, errors);
        errors.Clear();

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("duplicate-source"));
        Assert.Throws<ArgumentNullException>(() => AppSourceResult.SourceMisconfigured(Slug, null!));
        Assert.Throws<ArgumentException>(() => AppSourceResult.SourceMisconfigured(Slug, []));
        Assert.Throws<ArgumentException>(() => AppSourceResult.SourceMisconfigured(Slug, [null!]));
    }

    [Test]
    public void Existing_outcomes_carry_no_source_keys()
    {
        Assert.That(AppSourceResult.NotFound(Slug).SourceKeys, Is.Empty);
        Assert.That(AppSourceResult.DuplicateRegistration(Slug).SourceKeys, Is.Empty);
    }

    [Test]
    public void AppSourceStatus_values_are_stable()
    {
        Assert.That((int)AppSourceStatus.Resolved, Is.EqualTo(0));
        Assert.That((int)AppSourceStatus.NotFound, Is.EqualTo(1));
        Assert.That((int)AppSourceStatus.VersionMismatch, Is.EqualTo(2));
        Assert.That((int)AppSourceStatus.InvalidManifest, Is.EqualTo(3));
        Assert.That((int)AppSourceStatus.IdentityMismatch, Is.EqualTo(4));
        Assert.That((int)AppSourceStatus.DuplicateRegistration, Is.EqualTo(5));
        Assert.That((int)AppSourceStatus.Ambiguous, Is.EqualTo(6));
        Assert.That((int)AppSourceStatus.SourceMisconfigured, Is.EqualTo(7));
    }
}
