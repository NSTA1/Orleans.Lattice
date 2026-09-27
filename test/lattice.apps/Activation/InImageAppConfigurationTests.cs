using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for the configuration-time integrations of in-image apps:
/// <see cref="InImageAppManifestCatalog"/>, <see cref="AppReplicationIntentPostConfigure"/>, and
/// <see cref="AppTreeOptionsConfigurator"/>.
/// </summary>
[TestFixture]
public sealed class InImageAppConfigurationTests
{
    private const string Replicated = """
        {
          "identity": { "slug": "notes", "version": "1.0.0" },
          "trees": [
            { "name": "records", "softDeleteDuration": "1.00:00:00" },
            { "name": "legacy", "adoptedTreeId": "legacy-tree" }
          ],
          "roles": [],
          "replication": [
            { "tree": "records", "mergeMode": "LwwRegister" },
            { "tree": "legacy", "mergeMode": "OrSet" }
          ],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private static InImageAppManifestCatalog Catalog(params (string Slug, string Json)[] apps)
    {
        var options = new InImageAppSourceOptions();
        foreach (var (slug, json) in apps)
            options.Register(AppSlug.Parse(slug), new FakeAppAssembly(SourceTestManifests.ResourceName, json), SourceTestManifests.ResourceName);
        var wrapped = Options.Create(options);
        return new InImageAppManifestCatalog(wrapped, new InImageAppSource(wrapped));
    }

    [Test]
    public void Catalog_holds_only_valid_manifests_and_never_throws()
    {
        var catalog = Catalog(
            ("notes", Replicated),
            ("broken", "{ not json"),
            ("other", SourceTestManifests.Minimal("mismatched", "1.0.0")));

        Assert.That(catalog.Manifests.Select(m => m.Identity.Slug.Value), Is.EqualTo(new[] { "notes" }));
    }

    [Test]
    public void Catalog_skips_a_source_that_throws()
    {
        var options = new InImageAppSourceOptions();
        options.Register(AppSlug.Parse("notes"), new FakeAppAssembly(SourceTestManifests.ResourceName, Replicated), SourceTestManifests.ResourceName);
        var source = Substitute.For<IAppSource>();
        source.ResolveAsync(default, default, default)
            .ReturnsForAnyArgs<ValueTask<AppSourceResult>>(_ => throw new InvalidOperationException("boom"));

        var catalog = new InImageAppManifestCatalog(Options.Create(options), source);

        Assert.That(catalog.Manifests, Is.Empty);
    }

    [Test]
    public void Replication_intent_is_merged_additively()
    {
        var postConfigure = new AppReplicationIntentPostConfigure(Catalog(("notes", Replicated)));
        var options = new LatticeReplicationOptions
        {
            ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["operator-tree"] = LatticeMergeMode.LwwRegister,
                ["legacy-tree"] = LatticeMergeMode.LwwRegister,
            },
        };

        postConfigure.PostConfigure(Options.DefaultName, options);

        Assert.That(options.ReplicatedTrees, Is.EquivalentTo(new Dictionary<string, LatticeMergeMode>
        {
            ["operator-tree"] = LatticeMergeMode.LwwRegister,
            // An operator's entry is never overwritten, even with the app's declared mode.
            ["legacy-tree"] = LatticeMergeMode.LwwRegister,
            ["a/notes/records"] = LatticeMergeMode.LwwRegister,
        }));
    }

    [Test]
    public void Replication_intent_populates_an_empty_map()
    {
        var postConfigure = new AppReplicationIntentPostConfigure(Catalog(("notes", Replicated)));
        var options = new LatticeReplicationOptions();

        postConfigure.PostConfigure(Options.DefaultName, options);

        Assert.That(options.ReplicatedTrees, Is.EquivalentTo(new Dictionary<string, LatticeMergeMode>
        {
            ["a/notes/records"] = LatticeMergeMode.LwwRegister,
            ["legacy-tree"] = LatticeMergeMode.OrSet,
        }));
    }

    [Test]
    public void No_replication_intent_leaves_the_map_untouched()
    {
        var postConfigure = new AppReplicationIntentPostConfigure(Catalog(("notes", SourceTestManifests.Minimal("notes", "1.0.0"))));
        var original = new Dictionary<string, LatticeMergeMode> { ["x"] = LatticeMergeMode.LwwRegister };
        var options = new LatticeReplicationOptions { ReplicatedTrees = original };

        postConfigure.PostConfigure(Options.DefaultName, options);

        Assert.That(options.ReplicatedTrees, Is.SameAs(original));
    }

    [TestCase("a/notes/records")]
    [TestCase("t/acme/a/notes/records")]
    public void Declared_soft_delete_duration_applies_to_the_structural_tree_in_every_tenant(string treeId)
    {
        var configurator = new AppTreeOptionsConfigurator(Catalog(("notes", Replicated)));
        var options = new LatticeOptions();

        configurator.Configure(treeId, options);

        Assert.That(options.SoftDeleteDuration, Is.EqualTo(TimeSpan.FromDays(1)));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("legacy-tree")]
    [TestCase("a/notes/legacy")]
    [TestCase("a/other/records")]
    [TestCase("a/notes")]
    [TestCase("plain")]
    public void Other_trees_keep_the_default_soft_delete_duration(string? treeId)
    {
        var configurator = new AppTreeOptionsConfigurator(Catalog(("notes", Replicated)));
        var options = new LatticeOptions();

        configurator.Configure(treeId, options);
        configurator.Configure(options);

        Assert.That(options.SoftDeleteDuration, Is.EqualTo(LatticeOptions.DefaultSoftDeleteDuration));
    }

    [Test]
    public void Structural_tree_names_split_and_compose()
    {
        Assert.That(AppActivationTreeNames.TrySplitStructuralTree("t/acme/a/notes/records", out var slug, out var tree), Is.True);
        Assert.That(slug.ToString(), Is.EqualTo("notes"));
        Assert.That(tree.ToString(), Is.EqualTo("records"));
        Assert.That(AppActivationTreeNames.StructuralTree(AppRegistryTestData.Acme, ActivationHarness.Slug, "records"), Is.EqualTo("t/acme/a/notes/records"));
        Assert.That(AppActivationTreeNames.StructuralTree(TenantId.Default, ActivationHarness.Slug, "records"), Is.EqualTo("a/notes/records"));
        Assert.That(AppActivationTreeNames.BelongsToTenant("t/acme/x", AppRegistryTestData.Acme), Is.True);
        Assert.That(AppActivationTreeNames.BelongsToTenant("t/acme/x", TenantId.Default), Is.False);
        Assert.That(AppActivationTreeNames.BelongsToTenant("x", TenantId.Default), Is.True);
        Assert.That(AppActivationTreeNames.StatusTree, Is.EqualTo("sys-app-activation"));
    }
}
