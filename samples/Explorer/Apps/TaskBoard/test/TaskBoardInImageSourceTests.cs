using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard.Tests;

/// <summary>
/// The task board registers through the same <c>AddLatticeApp</c> line the Explorer sample uses,
/// and the in-image source then enumerates, resolves and serves it.
/// </summary>
[TestFixture]
public sealed class TaskBoardInImageSourceTests
{
    private static InImageAppSource Source()
    {
        var services = new ServiceCollection();
        services.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);
        using var provider = services.BuildServiceProvider();
        return new InImageAppSource(provider.GetRequiredService<IOptions<InImageAppSourceOptions>>());
    }

    [Test]
    public void The_default_asset_prefix_is_the_one_the_app_embeds_under()
    {
        var registration = new InImageAppRegistration(AppSlug.Parse(TaskBoardApp.Slug), TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName);

        Assert.That(registration.AssetResourcePrefix, Is.EqualTo(TaskBoardApp.AssetResourcePrefix));
    }

    [Test]
    public async Task The_app_enumerates_from_the_in_image_source()
    {
        var source = Source();

        var page = await source.ListAsync(AppSourceQuery.Default);

        Assert.Multiple(() =>
        {
            Assert.That(source.Descriptor.Key, Is.EqualTo("in-image"));
            Assert.That(page.Entries, Has.Count.EqualTo(1));
            var entry = page.Entries[0];
            Assert.That(entry.Slug.Value, Is.EqualTo(TaskBoardApp.Slug));
            Assert.That(entry.IsAvailable, Is.True, string.Join("; ", entry.Errors.Select(e => e.Path + ": " + e.Message)));
            Assert.That(entry.Versions, Is.EqualTo(new[] { AppVersion.Parse("1.0.0") }));
            Assert.That(entry.Provenance!.Source, Is.EqualTo(InImageAppSource.SourceKey));
            Assert.That(entry.Manifest!.Presentation!.DisplayName, Is.EqualTo("Task board"));
        });
    }

    [Test]
    public async Task The_app_resolves_from_the_composed_source_set()
    {
        var set = new AppSourceSet([Source()]);

        var result = await set.ResolveAsync(AppSlug.Parse(TaskBoardApp.Slug), sourceKey: InImageAppSource.SourceKey);

        Assert.That(result.IsResolved, Is.True, string.Join("; ", result.Errors.Select(e => e.Path + ": " + e.Message)));
        Assert.That(result.Manifest!.Ui, Is.Not.Null);
    }

    [Test]
    public async Task Every_bundle_asset_opens_verified_from_the_in_image_source()
    {
        var source = Source();
        var manifest = TaskBoardFiles.Manifest();

        foreach (var asset in manifest.Ui!.Assets)
        {
            var opened = await source.OpenAssetAsync(manifest.Identity.Slug, manifest.Identity.Version, asset.Path, asset.Digest);

            Assert.That(opened.IsOpened, Is.True, asset.Path + ": " + opened.Status);
            Assert.That(opened.MediaType, Is.EqualTo(asset.MediaType), asset.Path);
            Assert.That(opened.Content.ToArray(), Is.EqualTo(TaskBoardFiles.ReadAsset(asset.Path)), asset.Path);
        }
    }
}
