namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Every static web asset the Explorer's UI and AppKit publish must be loadable at
/// runtime: its file lies under the content root the runtime resolves it against,
/// and it is not empty (issue #3831).
/// </summary>
/// <remarks>
/// The static web asset runtime does not serve an asset from where the build
/// found it. It serves <c>content root + relative path</c>. A file linked in from
/// elsewhere, or staged into <c>obj/</c>, is listed with the right length and then
/// served as an empty 200, which left every design token undefined and the whole
/// console unstyled while every build-time check passed. This gate reads the
/// build manifest and applies the runtime's own resolution rule to each asset.
/// </remarks>
[TestFixture]
public sealed class StaticWebAssetRuntimeResolutionTests
{
    private static readonly string[] Projects =
    [
        "src/lattice.explorer/UI",
        "src/lattice.explorer/AppKit",
    ];

    [TestCaseSource(nameof(Projects))]
    public void Every_asset_resolves_under_its_content_root_and_is_not_empty(string project)
    {
        var assets = StaticWebAssetManifest.Assets(project).Where(asset => !asset.IsCompressed).ToArray();
        var offenders = new List<string>();

        foreach (var asset in assets)
        {
            var resolved = Path.GetFullPath(Path.Combine(asset.ContentRoot, asset.RelativePath.Replace('/', Path.DirectorySeparatorChar)));
            if (!string.Equals(resolved, Path.GetFullPath(asset.Identity), StringComparison.OrdinalIgnoreCase))
            {
                offenders.Add($"{asset.BasePath}/{asset.RelativePath}: built from {asset.Identity}, but the runtime reads {resolved}");
            }
            else if (new FileInfo(resolved).Length == 0)
            {
                offenders.Add($"{asset.BasePath}/{asset.RelativePath}: empty file");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(assets, Has.Length.GreaterThan(5), "the scan must reach the project's assets");
            Assert.That(offenders, Is.Empty,
                "a static web asset whose file is not under its content root is served empty at runtime. Keep the file in the project's wwwroot."
                + Environment.NewLine + string.Join(Environment.NewLine, offenders));
        });
    }

    [Test]
    public void The_rule_rejects_an_asset_built_from_outside_its_content_root()
    {
        // Battery test: the shape of the defect this gate exists for.
        var asset = new StaticWebAssetManifest.Asset(
            "design/tokens.css",
            Path.Combine(Path.GetTempPath(), "docs-site", "tokens.css"),
            "_content/Orleans.Lattice.Explorer.UI",
            IsCompressed: false,
            ContentRoot: Path.Combine(Path.GetTempPath(), "UI", "wwwroot"));

        var resolved = Path.GetFullPath(Path.Combine(asset.ContentRoot, asset.RelativePath));

        Assert.That(resolved, Is.Not.EqualTo(Path.GetFullPath(asset.Identity)).IgnoreCase);
    }
}
