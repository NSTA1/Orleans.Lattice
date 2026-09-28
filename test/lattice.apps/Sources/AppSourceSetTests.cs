using NSubstitute;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public partial class AppSourceSetTests
{
    private static readonly AppSlug Notes = AppSlug.Parse("notes");
    private static readonly AppSlug Tasks = AppSlug.Parse("tasks");

    private static InImageAppSource InImage(params string[] slugs) =>
        SourceTestManifests.Source(slugs
            .Select(slug => SourceTestManifests.Registration(slug,
                new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal(slug, "1.0.0"))))
            .ToArray());

    private static IAppCatalogSource Stub(string key, AppSourceResult result)
    {
        var source = Substitute.For<IAppCatalogSource>();
        source.Descriptor.Returns(new AppSourceDescriptor(key, key, AppSourceKind.Static, AppSourceCapabilities.None));
        source.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<AppSourceResult>(result));
        return source;
    }

    [Test]
    public void Constructor_rejects_null_sources()
    {
        Assert.Throws<ArgumentNullException>(() => new AppSourceSet(null!));
    }

    [Test]
    public async Task An_empty_set_is_valid_and_resolves_nothing()
    {
        var set = new AppSourceSet([]);

        Assert.That(set.Sources, Is.Empty);
        Assert.That(set.IsValid, Is.True);
        Assert.That(set.CompositionErrors, Is.Empty);
        Assert.That((await set.ResolveAsync(Notes)).Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That((await ((IAppSource)set).ResolveAsync(Notes)).Status, Is.EqualTo(AppSourceStatus.NotFound));
    }

    [Test]
    public void Sources_keep_registration_order_and_TryGet_finds_each_by_key()
    {
        var inImage = InImage("notes");
        var feed = new FakeDynamicAppSource();

        var set = new AppSourceSet([feed, inImage]);

        Assert.That(set.Sources, Is.EqualTo(new IAppCatalogSource[] { feed, inImage }));
        Assert.That(set.TryGet("in-image", out var found), Is.True);
        Assert.That(found, Is.SameAs(inImage));
        Assert.That(set.TryGet(FakeDynamicAppSource.DefaultKey, out found), Is.True);
        Assert.That(found, Is.SameAs(feed));
        Assert.That(set.TryGet("missing", out found), Is.False);
        Assert.That(found, Is.Null);
        Assert.Throws<ArgumentNullException>(() => set.TryGet(null!, out _));
    }

    [Test]
    public async Task Resolution_through_the_seam_matches_the_in_image_source_alone()
    {
        var inImage = InImage("notes");
        IAppSource set = new AppSourceSet([inImage]);

        var viaSet = await set.ResolveAsync(Notes);
        var direct = await inImage.ResolveAsync(Notes);

        Assert.That(viaSet, Is.SameAs(direct), "the cached in-image result is passed through unchanged");
        Assert.That(viaSet.Provenance!.Source, Is.EqualTo(InImageAppSource.SourceKey));
        var mismatch = await set.ResolveAsync(Notes, AppVersion.Parse("9.9.9"));
        Assert.That(mismatch.Status, Is.EqualTo(AppSourceStatus.VersionMismatch));
        Assert.That(mismatch.AvailableVersion, Is.EqualTo(AppVersion.Parse("1.0.0")));
        Assert.That((await set.ResolveAsync(Tasks)).Status, Is.EqualTo(AppSourceStatus.NotFound));
    }

    [Test]
    public async Task Without_a_key_the_one_offering_source_answers()
    {
        var feed = new FakeDynamicAppSource().Add("tasks", "3.0.0");
        var set = new AppSourceSet([InImage("notes"), feed]);

        var notes = await set.ResolveAsync(Notes);
        var tasks = await set.ResolveAsync(Tasks);

        Assert.That(notes.Provenance!.Source, Is.EqualTo(InImageAppSource.SourceKey));
        Assert.That(tasks.IsResolved, Is.True);
        Assert.That(tasks.Provenance!.Source, Is.EqualTo(FakeDynamicAppSource.DefaultKey));
        Assert.That((await set.ResolveAsync(AppSlug.Parse("absent"))).Status, Is.EqualTo(AppSourceStatus.NotFound));
    }

    [Test]
    public async Task A_slug_offered_by_two_sources_is_ambiguous_without_a_key_and_never_picked()
    {
        var feed = new FakeDynamicAppSource().Add("notes", "2.0.0");
        var set = new AppSourceSet([InImage("notes"), feed]);

        var viaSet = await set.ResolveAsync(Notes);
        var viaSeam = await ((IAppSource)set).ResolveAsync(Notes);

        foreach (var result in new[] { viaSet, viaSeam })
        {
            Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Ambiguous));
            Assert.That(result.IsResolved, Is.False);
            Assert.That(result.Manifest, Is.Null);
            Assert.That(result.SourceKeys, Is.EqualTo(new[] { InImageAppSource.SourceKey, FakeDynamicAppSource.DefaultKey }));
            Assert.That(result.Errors.Single().Code, Is.EqualTo("ambiguous"));
        }
    }

    [Test]
    public async Task A_version_mismatch_still_counts_as_offering_the_slug()
    {
        var feed = new FakeDynamicAppSource().Add("notes", "2.0.0");
        var set = new AppSourceSet([InImage("notes"), feed]);

        var result = await set.ResolveAsync(Notes, AppVersion.Parse("2.0.0"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.Ambiguous));
    }

    [Test]
    public async Task A_single_offering_source_reports_its_own_failure()
    {
        var set = new AppSourceSet([InImage("notes"), new FakeDynamicAppSource()]);

        var result = await set.ResolveAsync(Notes, AppVersion.Parse("2.0.0"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.VersionMismatch));
    }

    [Test]
    public async Task With_a_key_only_the_named_source_answers()
    {
        var feed = new FakeDynamicAppSource().Add("notes", "2.0.0");
        var set = new AppSourceSet([InImage("notes"), feed]);

        var fromImage = await set.ResolveAsync(Notes, sourceKey: InImageAppSource.SourceKey);
        var fromFeed = await set.ResolveAsync(Notes, AppVersion.Parse("2.0.0"), FakeDynamicAppSource.DefaultKey);

        Assert.That(fromImage.Manifest!.Identity.Version, Is.EqualTo(AppVersion.Parse("1.0.0")));
        Assert.That(fromImage.Provenance!.Source, Is.EqualTo(InImageAppSource.SourceKey));
        Assert.That(fromFeed.Manifest!.Identity.Version, Is.EqualTo(AppVersion.Parse("2.0.0")));
        Assert.That(fromFeed.Provenance!.Source, Is.EqualTo(FakeDynamicAppSource.DefaultKey));
        Assert.That(feed.ResolveCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task An_unknown_key_is_not_found()
    {
        var set = new AppSourceSet([InImage("notes")]);

        var result = await set.ResolveAsync(Notes, sourceKey: "nowhere");

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("unknown-source"));
        Assert.That(result.Errors.Single().Message, Does.Contain("nowhere"));
    }
}
