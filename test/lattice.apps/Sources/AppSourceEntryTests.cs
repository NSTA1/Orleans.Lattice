using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppSourceEntryTests
{
    private static readonly AppSlug Slug = AppSlug.Parse("notes");
    private static readonly AppVersion Newest = AppVersion.Parse("2.0.0");
    private static readonly AppVersion Older = AppVersion.Parse("1.0.0");

    [Test]
    public void Available_carries_versions_manifest_and_provenance()
    {
        var manifest = SourceTestManifests.Manifest("notes", "2.0.0");
        var provenance = new AppProvenance { Source = "feed" };
        var versions = new List<AppVersion> { Newest, Older };

        var entry = AppSourceEntry.Available(versions, manifest, provenance);
        versions.Clear();

        Assert.That(entry.Slug, Is.EqualTo(Slug));
        Assert.That(entry.IsAvailable, Is.True);
        Assert.That(entry.Versions, Is.EqualTo(new[] { Newest, Older }));
        Assert.That(entry.Manifest, Is.SameAs(manifest));
        Assert.That(entry.Provenance, Is.SameAs(provenance));
        Assert.That(entry.Errors, Is.Empty);
    }

    [Test]
    public void Available_rejects_null_arguments()
    {
        var manifest = SourceTestManifests.Manifest("notes", "2.0.0");

        Assert.Throws<ArgumentNullException>(() => AppSourceEntry.Available(null!, manifest, new()));
        Assert.Throws<ArgumentNullException>(() => AppSourceEntry.Available([Newest], null!, new()));
        Assert.Throws<ArgumentNullException>(() => AppSourceEntry.Available([Newest], manifest with { Identity = null! }, new()));
        Assert.Throws<ArgumentNullException>(() => AppSourceEntry.Available([Newest], manifest, null!));
    }

    [Test]
    public void Available_rejects_empty_uninitialised_repeated_or_misordered_versions()
    {
        var manifest = SourceTestManifests.Manifest("notes", "2.0.0");

        Assert.Throws<ArgumentException>(() => AppSourceEntry.Available([], manifest, new()));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Available([Newest, default], manifest, new()));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Available([Newest, Newest], manifest, new()));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Available([Older, Newest], manifest, new()));
    }

    [Test]
    public void Unavailable_copies_the_errors_and_has_no_manifest()
    {
        var errors = new List<AppManifestError> { new("json", "$", "Bad.") };

        var entry = AppSourceEntry.Unavailable(Slug, errors);
        errors.Clear();

        Assert.That(entry.Slug, Is.EqualTo(Slug));
        Assert.That(entry.IsAvailable, Is.False);
        Assert.That(entry.Versions, Is.Empty);
        Assert.That(entry.Manifest, Is.Null);
        Assert.That(entry.Provenance, Is.Null);
        Assert.That(entry.Errors.Single().Code, Is.EqualTo("json"));
    }

    [Test]
    public void Unavailable_rejects_null_empty_or_null_containing_errors_and_the_default_slug()
    {
        Assert.Throws<ArgumentNullException>(() => AppSourceEntry.Unavailable(Slug, null!));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Unavailable(Slug, []));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Unavailable(Slug, [null!]));
        Assert.Throws<ArgumentException>(() => AppSourceEntry.Unavailable(default, [new("json", "$", "Bad.")]));
    }
}
