using System.Text;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Proves the catalogue contract, and <see cref="AppSourceSet"/> over it, against a dynamic source shape:
/// several versions per slug, text search, and acquisition before assets or code are available.
/// </summary>
[TestFixture]
public class FakeDynamicAppSourceTests
{
    private static readonly byte[] Script = Encoding.UTF8.GetBytes("export const v = 2;");
    private static readonly AppSlug Notes = AppSlug.Parse("notes");
    private static readonly AppVersion V2 = AppVersion.Parse("2.0.0");
    private static readonly AppVersion V1 = AppVersion.Parse("1.0.0");

    private static FakeDynamicAppSource Feed() => new FakeDynamicAppSource()
        .Add("notes", "2.0.0", ("ui/app.js", Script))
        .Add("notes", "1.0.0")
        .Add("notebook", "1.0.0")
        .Add("tasks", "1.0.0");

    [Test]
    public void Descriptor_is_dynamic_with_every_capability()
    {
        var descriptor = Feed().Descriptor;

        Assert.That(descriptor.Key, Is.EqualTo(FakeDynamicAppSource.DefaultKey));
        Assert.That(descriptor.Kind, Is.EqualTo(AppSourceKind.Dynamic));
        Assert.That(descriptor.Supports(FakeDynamicAppSource.AllCapabilities), Is.True);
    }

    [Test]
    public async Task Listing_carries_every_version_newest_first_with_the_newest_manifest()
    {
        var page = await Feed().ListAsync(new AppSourceQuery { Text = "notes" });

        var entry = page.Entries.Single();
        Assert.That(entry.Slug, Is.EqualTo(Notes));
        Assert.That(entry.Versions, Is.EqualTo(new[] { V2, V1 }));
        Assert.That(entry.Manifest!.Identity.Version, Is.EqualTo(V2));
        Assert.That(entry.Provenance!.Source, Is.EqualTo(FakeDynamicAppSource.DefaultKey));
        Assert.That(page.HasMore, Is.False);
    }

    [Test]
    public async Task Search_filters_and_pages_follow_the_continuation()
    {
        var feed = Feed();

        var first = await feed.ListAsync(new AppSourceQuery { Text = "note", PageSize = 1 });
        var second = await feed.ListAsync(new AppSourceQuery { Text = "note", PageSize = 1, Continuation = first.Continuation });
        var all = await feed.ListAsync(AppSourceQuery.Default);
        var malformed = await feed.ListAsync(new AppSourceQuery { Continuation = "not-a-number" });

        Assert.That(first.Entries.Single().Slug.Value, Is.EqualTo("notebook"));
        Assert.That(first.HasMore, Is.True);
        Assert.That(second.Entries.Single().Slug.Value, Is.EqualTo("notes"));
        Assert.That(second.HasMore, Is.False);
        Assert.That(all.Entries.Select(e => e.Slug.Value), Is.EqualTo(new[] { "notebook", "notes", "tasks" }));
        Assert.That(malformed, Is.SameAs(AppSourcePage.Empty));
    }

    [Test]
    public async Task Assets_are_not_available_until_acquired_then_verified_at_every_open()
    {
        var feed = Feed();
        var digest = SourceTestManifests.Sha256(Script);

        var before = await feed.OpenAssetAsync(Notes, V2, "ui/app.js", digest);
        feed.Acquire(Notes, V2);
        var opened = await feed.OpenAssetAsync(Notes, V2, "ui/app.js", digest);
        var tampered = await feed.OpenAssetAsync(Notes, V2, "ui/app.js", SourceTestManifests.Sha256([1, 2, 3]));
        var traversal = await feed.OpenAssetAsync(Notes, V2, "../ui/app.js", digest);
        var otherVersion = await feed.OpenAssetAsync(Notes, V1, "ui/app.js", digest);

        Assert.That(before.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
        Assert.That(opened.Status, Is.EqualTo(AppAssetStatus.Opened));
        Assert.That(opened.Content.ToArray(), Is.EqualTo(Script));
        Assert.That(opened.MediaType, Is.EqualTo("text/javascript"));
        Assert.That(tampered.Status, Is.EqualTo(AppAssetStatus.DigestMismatch));
        Assert.That(tampered.Content.IsEmpty, Is.True);
        Assert.That(traversal.Status, Is.EqualTo(AppAssetStatus.NotFound));
        Assert.That(otherVersion.Status, Is.EqualTo(AppAssetStatus.NotAvailable));
    }

    [Test]
    public async Task Resolution_through_the_set_offers_the_manifest_before_acquisition_and_code_only_after()
    {
        var feed = Feed();
        var set = new AppSourceSet([SourceTestManifests.Source(), feed]);

        var newest = await set.ResolveAsync(Notes);
        var older = await set.ResolveAsync(Notes, V1, FakeDynamicAppSource.DefaultKey);
        var beforeAcquire = await newest.Activation!.ActivateAsync();
        feed.Acquire(Notes, V2);
        var afterAcquire = await newest.Activation.ActivateAsync();

        Assert.That(newest.Manifest!.Identity.Version, Is.EqualTo(V2));
        Assert.That(newest.Provenance!.Reference, Is.EqualTo("feed:notes@2.0.0"));
        Assert.That(older.Manifest!.Identity.Version, Is.EqualTo(V1));
        Assert.That(beforeAcquire.IsActivated, Is.False);
        Assert.That(afterAcquire.IsActivated, Is.True);
        Assert.That(set.TryGet(FakeDynamicAppSource.DefaultKey, out var found), Is.True);
        Assert.That(found!.Descriptor.Supports(AppSourceCapabilities.RequiresAcquisition), Is.True);
    }
}
