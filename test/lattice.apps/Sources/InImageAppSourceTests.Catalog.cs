using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

public partial class InImageAppSourceTests
{
    private static (InImageAppSource Source, FakeAppAssembly[] Assemblies) CreateCatalog(params string[] slugs)
    {
        var assemblies = slugs
            .Select(slug => new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal(slug, "1.0.0")))
            .ToArray();
        var registrations = slugs.Select((slug, i) => SourceTestManifests.Registration(slug, assemblies[i])).ToArray();
        return (SourceTestManifests.Source(registrations), assemblies);
    }

    [Test]
    public void Descriptor_is_the_static_enumerable_in_image_source()
    {
        var (source, _) = CreateSource();

        Assert.That(source.Descriptor.Key, Is.EqualTo("in-image"));
        Assert.That(source.Descriptor.Key, Is.EqualTo(InImageAppSource.SourceKey));
        Assert.That(source.Descriptor.DisplayName, Is.Not.Empty);
        Assert.That(source.Descriptor.Kind, Is.EqualTo(AppSourceKind.Static));
        Assert.That(source.Descriptor.Capabilities, Is.EqualTo(AppSourceCapabilities.Enumerate));
        Assert.That(source, Is.InstanceOf<IAppCatalogSource>());
    }

    [Test]
    public async Task ListAsync_lists_every_registration_once_in_slug_order()
    {
        var (source, _) = CreateCatalog("tasks", "alpha", "notes");

        var page = await source.ListAsync(AppSourceQuery.Default);

        Assert.That(page.Entries.Select(e => e.Slug.Value), Is.EqualTo(new[] { "alpha", "notes", "tasks" }));
        Assert.That(page.HasMore, Is.False);
        var entry = page.Entries[1];
        Assert.That(entry.IsAvailable, Is.True);
        Assert.That(entry.Versions, Is.EqualTo(new[] { AppVersion.Parse("1.0.0") }));
        Assert.That(entry.Manifest!.Identity.Slug.Value, Is.EqualTo("notes"));
        Assert.That(entry.Provenance!.Source, Is.EqualTo(InImageAppSource.SourceKey));
    }

    [Test]
    public async Task ListAsync_pages_through_the_continuation()
    {
        var (source, _) = CreateCatalog("e-app", "a-app", "d-app", "b-app", "c-app");
        var slugs = new List<string>();
        var pages = 0;
        string? continuation = null;

        do
        {
            var page = await source.ListAsync(new AppSourceQuery { PageSize = 2, Continuation = continuation });
            slugs.AddRange(page.Entries.Select(e => e.Slug.Value));
            continuation = page.Continuation;
            pages++;
        }
        while (continuation is not null);

        Assert.That(slugs, Is.EqualTo(new[] { "a-app", "b-app", "c-app", "d-app", "e-app" }));
        Assert.That(pages, Is.EqualTo(3));
    }

    [Test]
    public async Task ListAsync_a_page_that_ends_exactly_at_the_last_entry_has_no_continuation()
    {
        var (source, _) = CreateCatalog("a-app", "b-app");

        var page = await source.ListAsync(new AppSourceQuery { PageSize = 2 });

        Assert.That(page.Entries, Has.Count.EqualTo(2));
        Assert.That(page.Continuation, Is.Null);
    }

    [Test]
    public async Task ListAsync_a_continuation_past_the_end_or_empty_source_yields_the_empty_page()
    {
        var (source, _) = CreateCatalog("a-app");

        Assert.That(await source.ListAsync(new AppSourceQuery { Continuation = "zz" }), Is.SameAs(AppSourcePage.Empty));
        Assert.That(await SourceTestManifests.Source().ListAsync(AppSourceQuery.Default), Is.SameAs(AppSourcePage.Empty));
    }

    [Test]
    public async Task ListAsync_any_continuation_text_resumes_after_it_in_ordinal_order()
    {
        var (source, _) = CreateCatalog("a-app", "b-app", "c-app");

        var page = await source.ListAsync(new AppSourceQuery { Continuation = "b" });

        Assert.That(page.Entries.Select(e => e.Slug.Value), Is.EqualTo(new[] { "b-app", "c-app" }));
    }

    [Test]
    public async Task ListAsync_ignores_the_text_filter_without_search()
    {
        var (source, _) = CreateCatalog("a-app", "b-app");

        var page = await source.ListAsync(new AppSourceQuery { Text = "zzz" });

        Assert.That(page.Entries, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task ListAsync_lists_broken_and_duplicate_registrations_as_unavailable()
    {
        var broken = new FakeAppAssembly(SourceTestManifests.ResourceName, "{ not json");
        var good = new FakeAppAssembly(SourceTestManifests.ResourceName, SourceTestManifests.Minimal("dup-app", "1.0.0"));
        var source = SourceTestManifests.Source(
            SourceTestManifests.Registration("broken", broken),
            SourceTestManifests.Registration("dup-app", good),
            SourceTestManifests.Registration("dup-app", good));

        var page = await source.ListAsync(AppSourceQuery.Default);

        Assert.That(page.Entries.Select(e => (e.Slug.Value, e.IsAvailable)), Is.EqualTo(new[] { ("broken", false), ("dup-app", false) }));
        Assert.That(page.Entries[0].Errors.Single().Code, Is.EqualTo("json"));
        Assert.That(page.Entries[1].Errors.Single().Code, Is.EqualTo("duplicate"));
    }

    [Test]
    public async Task ListAsync_reads_each_manifest_once_and_touches_no_app_code()
    {
        var (source, assemblies) = CreateCatalog("a-app", "b-app");

        var first = await source.ListAsync(AppSourceQuery.Default);
        var second = await source.ListAsync(AppSourceQuery.Default);
        _ = await source.ResolveAsync(AppSlug.Parse("a-app"));

        Assert.That(assemblies.Select(a => a.ResourceReads), Is.EqualTo(new[] { 1, 1 }));
        Assert.That(second.Entries[0], Is.SameAs(first.Entries[0]), "entries are cached per registration");
    }

    [Test]
    public void ListAsync_rejects_a_null_query()
    {
        var (source, _) = CreateSource();

        Assert.ThrowsAsync<ArgumentNullException>(async () => await source.ListAsync(null!));
    }
}
