using NSubstitute;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

public partial class AppSourceSetTests
{
    [Test]
    public async Task Duplicate_keys_are_recorded_at_composition_and_fail_every_resolution()
    {
        var first = new FakeDynamicAppSource("feed").Add("tasks", "1.0.0");
        var second = new FakeDynamicAppSource("feed").Add("other", "1.0.0");
        var inImage = InImage("notes");

        var set = new AppSourceSet([first, inImage, second]);

        Assert.That(set.IsValid, Is.False);
        Assert.That(set.Sources, Has.Count.EqualTo(3));
        var error = set.CompositionErrors.Single();
        Assert.That(error.Code, Is.EqualTo("duplicate-source"));
        Assert.That(error.Path, Is.EqualTo("$.sources[2].key"));
        Assert.That(error.Message, Does.Contain("'feed'"));
        Assert.That(set.TryGet("feed", out _), Is.False, "a shared key identifies no source");
        Assert.That(set.TryGet(InImageAppSource.SourceKey, out var found), Is.True);
        Assert.That(found, Is.SameAs(inImage));

        foreach (var result in new[]
        {
            await ((IAppSource)set).ResolveAsync(Notes),
            await set.ResolveAsync(Notes, sourceKey: InImageAppSource.SourceKey),
            await set.ResolveAsync(Tasks, sourceKey: "feed"),
        })
        {
            Assert.That(result.Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
            Assert.That(result.Errors, Is.EqualTo(set.CompositionErrors));
        }

        Assert.That(first.ResolveCalls + second.ResolveCalls, Is.Zero, "a misconfigured set asks no source");
    }

    [Test]
    public void A_key_repeated_three_times_is_reported_once()
    {
        var set = new AppSourceSet([new FakeDynamicAppSource("feed"), new FakeDynamicAppSource("feed"), new FakeDynamicAppSource("feed")]);

        Assert.That(set.CompositionErrors, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_null_source_or_one_without_a_descriptor_is_a_composition_error()
    {
        var undescribed = Substitute.For<IAppCatalogSource>();
        undescribed.Descriptor.Returns((AppSourceDescriptor)null!);

        var set = new AppSourceSet([null!, undescribed, InImage("notes")]);

        Assert.That(set.CompositionErrors.Select(e => (e.Code, e.Path)), Is.EqualTo(new[]
        {
            ("null-source", "$.sources[0]"),
            ("missing-descriptor", "$.sources[1].descriptor"),
        }));
        Assert.That(set.Sources, Has.Count.EqualTo(1));
        Assert.That((await set.ResolveAsync(Notes)).Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
    }

    [Test]
    public async Task A_source_vouching_for_another_key_is_refused_on_the_synchronous_path()
    {
        var manifest = SourceTestManifests.Manifest("notes", "1.0.0");
        var impostor = Stub("mirror", AppSourceResult.Resolved(manifest, new AppProvenance { Source = "in-image" }, Substitute.For<IAppActivationHandle>()));
        var set = new AppSourceSet([impostor]);

        var result = await set.ResolveAsync(Notes);

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("provenance-mismatch"));
        Assert.That(result.Errors.Single().Message, Does.Contain("'mirror'").And.Contain("'in-image'"));
    }

    [Test]
    public async Task A_source_vouching_for_another_key_is_refused_on_the_asynchronous_paths()
    {
        var impostor = new FakeDynamicAppSource("mirror", provenanceKey: "in-image").Add("tasks", "1.0.0");
        var set = new AppSourceSet([impostor]);
        var composite = new AppSourceSet([InImage("notes"), impostor]);

        var single = await set.ResolveAsync(Tasks);
        var keyed = await set.ResolveAsync(Tasks, sourceKey: "mirror");
        var across = await composite.ResolveAsync(Tasks);

        foreach (var result in new[] { single, keyed, across })
        {
            Assert.That(result.Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
            Assert.That(result.Errors.Single().Code, Is.EqualTo("provenance-mismatch"));
        }
    }

    [Test]
    public async Task A_failed_result_is_passed_through_without_a_provenance_check()
    {
        var failure = AppSourceResult.InvalidManifest(Notes, [new("json", "$", "Bad.")]);
        var set = new AppSourceSet([Stub("mirror", failure)]);

        Assert.That(await set.ResolveAsync(Notes), Is.SameAs(failure));
    }

    [Test]
    public async Task Composition_does_not_resolve_or_read_any_source()
    {
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("notes", "1.0.0"));
        var feed = new FakeDynamicAppSource().Add("tasks", "1.0.0");

        var set = new AppSourceSet([SourceTestManifests.Source(SourceTestManifests.Registration("notes", assembly)), feed]);

        Assert.That(assembly.ResourceReads, Is.Zero);
        Assert.That(feed.ResolveCalls, Is.Zero);
        Assert.That((await set.ResolveAsync(Tasks)).IsResolved, Is.True);
    }
}
