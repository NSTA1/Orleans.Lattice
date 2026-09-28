using Bunit;
using Orleans.Lattice.Explorer.Shell.Design;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>
/// The design system's head links: every stylesheet in cascade order and the
/// favicon, each naming an asset the Shell actually serves - a broken
/// <c>_content/</c> link fails as a silent 404, so it is checked here.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtDesignHeadTests : ShellDesignTestContext
{
    [Test]
    public void It_links_every_stylesheet_in_cascade_order_then_the_favicon()
    {
        var cut = Render<LtDesignHead>();

        Assert.Multiple(() =>
        {
            Assert.That(
                cut.FindAll("link[rel=stylesheet]").Select(link => link.GetAttribute("href")),
                Is.EqualTo(ShellDesignAssets.Stylesheets));
            Assert.That(cut.Find("link[rel=icon]").GetAttribute("href"), Is.EqualTo(ShellDesignAssets.Favicon));
            Assert.That(cut.Find("link[rel=icon]").GetAttribute("type"), Is.EqualTo("image/svg+xml"));
        });
    }

    [Test]
    public void The_tokens_load_before_everything_that_reads_them()
    {
        var order = ShellDesignAssets.Stylesheets.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(order.IndexOf(ShellDesignAssets.TokensStylesheet), Is.LessThan(order.IndexOf(ShellDesignAssets.OperateStylesheet)));
            Assert.That(order.IndexOf(ShellDesignAssets.OperateStylesheet), Is.LessThan(order.IndexOf(ShellDesignAssets.PrimitivesStylesheet)));
        });
    }

    [Test]
    public void Every_linked_asset_is_one_the_shell_serves()
    {
        var served = StaticWebAssetManifest.Assets("src/lattice.explorer/Shell")
            .Where(asset => !asset.IsCompressed)
            .Select(asset => asset.BasePath + "/" + asset.RelativePath)
            .ToHashSet(StringComparer.Ordinal);

        var linked = ShellDesignAssets.Stylesheets.Append(ShellDesignAssets.Favicon).ToArray();

        Assert.That(linked.Where(path => !served.Contains(path)), Is.Empty,
            "every design link must resolve to a static web asset the Shell publishes");
    }
}
