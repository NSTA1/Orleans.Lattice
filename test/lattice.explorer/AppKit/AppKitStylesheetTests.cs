using System.IO.Compression;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Tests.UI.Design;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The kit stylesheet is built on the documentation site's own tokens and
/// fonts, carried byte-identically under the kit's own web root (real files,
/// so the runtime can serve them; the docs-site files stay the source of
/// truth), and keys every
/// theme, contrast, density and motion choice off the attributes the bootstrap
/// sets. Its high-contrast overlay is the Explorer's own, value for value.
/// </summary>
[TestFixture]
public sealed class AppKitStylesheetTests
{
    private const string PaperSelector = ":root,[data-theme=\"paper\"]";
    private const string BoardSelector = "[data-theme=\"board\"]";
    private const string PaperMoreSelector = "[data-theme=\"paper\"][data-contrast=\"more\"]";
    private const string BoardMoreSelector = "[data-theme=\"board\"][data-contrast=\"more\"]";

    /// <summary>Every docs-site file the kit links, and the path it is served at.</summary>
    private static readonly (string Served, string Source)[] LinkedFiles =
    [
        ("appkit/v1/tokens.css", "docs-site/template/public/tokens.css"),
        ("appkit/v1/fonts/recursive-sans-linear.woff2", "docs-site/template/public/fonts/recursive-sans-linear.woff2"),
        ("appkit/v1/fonts/cascadia-mono.woff2", "docs-site/template/public/fonts/cascadia-mono.woff2"),
        ("appkit/v1/fonts/OFL-Recursive.txt", "docs-site/template/public/fonts/OFL-Recursive.txt"),
        ("appkit/v1/fonts/OFL-CascadiaMono.txt", "docs-site/template/public/fonts/OFL-CascadiaMono.txt"),
    ];

    [Test]
    [TestCaseSource(nameof(LinkedFileCases))]
    public void Every_docs_site_asset_is_served_byte_identical_from_the_kit_path(string served, string source)
    {
        var assets = StaticWebAssetManifest.Assets(AppKitPaths.Project)
            .Where(asset => !asset.IsCompressed && asset.RelativePath == served)
            .ToArray();

        Assert.That(assets, Has.Length.EqualTo(1), $"the kit must serve exactly one {served}");
        Assert.Multiple(() =>
        {
            Assert.That(assets[0].BasePath, Is.EqualTo(AppKitPaths.BasePath));
            Assert.That(Path.GetFullPath(assets[0].Identity), Is.EqualTo(Path.GetFullPath(AppKitPaths.Absolute(AppKitPaths.Project + "/wwwroot/" + served))).IgnoreCase,
                $"{served} must be served from the kit's own web root, where the runtime resolves it");
            Assert.That(File.ReadAllBytes(assets[0].Identity), Is.EqualTo(File.ReadAllBytes(AppKitPaths.Absolute(source))),
                $"{served} must be byte-identical to {source}: copy {source} over {AppKitPaths.Project}/wwwroot/{served}");
        });
    }

    [Test]
    public void The_kit_serves_the_same_tokens_bytes_as_the_shell()
    {
        // One source of truth: the frame and the Explorer around it draw from identical tokens.
        var kit = StaticWebAssetManifest.Assets(AppKitPaths.Project).Single(a => !a.IsCompressed && a.RelativePath == "appkit/v1/tokens.css");
        var shell = StaticWebAssetManifest.Assets("src/lattice.explorer/UI").Single(a => !a.IsCompressed && a.RelativePath == "design/tokens.css");

        Assert.That(File.ReadAllBytes(kit.Identity), Is.EqualTo(File.ReadAllBytes(shell.Identity)));
    }

    [Test]
    public void The_pre_compressed_tokens_variant_decompresses_to_the_docs_site_bytes()
    {
        var compressed = StaticWebAssetManifest.Assets(AppKitPaths.Project)
            .Where(asset => asset.IsCompressed && asset.RelativePath == "appkit/v1/tokens.css")
            .ToArray();

        Assert.That(compressed, Is.Not.Empty, "the build compresses tokens.css");
        var expected = File.ReadAllBytes(AppKitPaths.Absolute(ShellStylesheets.DocsSiteTokens));
        foreach (var asset in compressed)
        {
            using var input = new GZipStream(File.OpenRead(asset.Identity), CompressionMode.Decompress);
            using var output = new MemoryStream();
            input.CopyTo(output);
            Assert.That(output.ToArray(), Is.EqualTo(expected), asset.Identity);
        }
    }

    [Test]
    public void The_kit_project_neither_links_nor_stages_a_design_asset()
    {
        // A linked or staged file sits outside the project's content root, and the
        // static web asset runtime then serves it as an empty 200 (issue #3831).
        var project = AppKitPaths.Read(AppKitPaths.Project + "/Orleans.Lattice.Explorer.AppKit.csproj");

        Assert.Multiple(() =>
        {
            Assert.That(project, Does.Not.Contain("Link=\"wwwroot"), "no kit asset may be linked into the web root");
            Assert.That(Regex.IsMatch(project, @"<Copy\b"), Is.False, "no kit asset may be staged at build time");
        });
    }

    [Test]
    public void The_kit_stylesheet_imports_the_tokens_first()
    {
        var css = ShellStylesheets.WithoutComments(AppKitPaths.Stylesheet).Trim();

        Assert.Multiple(() =>
        {
            Assert.That(css, Does.StartWith("@import url(\"tokens.css\");"), "@import must precede every rule");
            Assert.That(Regex.Matches(css, "@import"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void The_kit_fonts_are_self_hosted_from_the_kit_path()
    {
        var css = ShellStylesheets.WithoutComments(AppKitPaths.Stylesheet);
        var urls = Regex.Matches(css, @"url\(""(?<u>[^""]+)""\)").Select(m => m.Groups["u"].Value).ToArray();
        var published = StaticWebAssetManifest.Assets(AppKitPaths.Project).Select(a => a.RelativePath).ToHashSet(StringComparer.Ordinal);

        Assert.Multiple(() =>
        {
            Assert.That(urls, Is.EquivalentTo(new[] { "tokens.css", "fonts/recursive-sans-linear.woff2", "fonts/cascadia-mono.woff2" }));
            foreach (var url in urls)
            {
                Assert.That(published, Does.Contain("appkit/v1/" + url), url + " resolves beside the stylesheet");
            }

            Assert.That(css, Does.Contain("font-family: \"Recursive Sans Linear\";"));
            Assert.That(css, Does.Contain("font-family: \"Lattice Mono\";"), "the family name tokens.css asks for");
            Assert.That(Regex.IsMatch(css, @"(?:https?:)?//"), Is.False, "nothing is fetched from a third party");
        });
    }

    [Test]
    public void The_kit_stylesheet_keys_off_the_bootstrap_attributes()
    {
        var selectors = ShellStylesheets.Rules(AppKitPaths.Stylesheet).Select(rule => rule.Selector).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(selectors, Does.Contain(PaperSelector));
            Assert.That(selectors, Does.Contain(BoardSelector));
            Assert.That(selectors, Does.Contain(PaperMoreSelector));
            Assert.That(selectors, Does.Contain(BoardMoreSelector));
            Assert.That(selectors, Does.Contain("[data-density=\"compact\"]"));
            Assert.That(selectors.Any(s => s.Contains("[data-reduced-motion=\"true\"]", StringComparison.Ordinal)), Is.True);
        });
    }

    [Test]
    public void The_bootstrap_sets_every_attribute_the_stylesheet_keys_off()
    {
        var boot = AppKitPaths.Read(AppKitPaths.Boot);

        Assert.Multiple(() =>
        {
            foreach (var attribute in new[] { "data-theme", "data-bs-theme", "data-contrast", "data-density", "data-reduced-motion" })
            {
                Assert.That(boot, Does.Contain("root.setAttribute(\"" + attribute + "\""), attribute);
            }

            Assert.That(boot, Does.Contain("theme === \"board\" ? \"dark\" : \"light\""), "board is the tokens' dark material");
        });
    }

    [Test]
    public void The_high_contrast_overlay_is_the_explorers_own()
    {
        Assert.Multiple(() =>
        {
            AssertSameOverlay(ShellStylesheets.PaperMoreSelector, PaperMoreSelector);
            AssertSameOverlay(ShellStylesheets.BoardMoreSelector, BoardMoreSelector);
        });
    }

    [Test]
    public void The_kit_control_tokens_match_the_explorer_operate_register()
    {
        var shellPaper = ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.PaperSelector);
        var shellBoard = ShellStylesheets.Block(ShellStylesheets.Operate, ShellStylesheets.BoardSelector);
        var kitPaper = ShellStylesheets.Block(AppKitPaths.Stylesheet, PaperSelector);
        var kitBoard = ShellStylesheets.Block(AppKitPaths.Stylesheet, BoardSelector);

        Assert.Multiple(() =>
        {
            Assert.That(kitPaper["--lt-app-control-border"], Is.EqualTo(shellPaper["--lt-op-control-border"]));
            Assert.That(kitBoard["--lt-app-control-border"], Is.EqualTo(shellBoard["--lt-op-control-border"]));
            Assert.That(kitPaper["--lt-app-focus-ring-width"], Is.EqualTo(shellPaper["--lt-op-focus-ring-width"]));
            Assert.That(kitPaper["--lt-app-focus-ring-offset"], Is.EqualTo(shellPaper["--lt-op-focus-ring-offset"]));
            Assert.That(kitPaper["--lt-app-row-height"], Is.EqualTo(shellPaper["--lt-op-row-height-comfortable"]));
        });
    }

    [Test]
    public void The_kit_controls_are_touch_targets_in_every_density()
    {
        // The Explorer's responsive contract: 44px targets in comfortable density, never below 24px in compact.
        var comfortable = ShellStylesheets.Block(AppKitPaths.Stylesheet, PaperSelector)["--lt-app-control-height"];
        var compact = ShellStylesheets.Block(AppKitPaths.Stylesheet, "[data-density=\"compact\"]")["--lt-app-control-height"];
        var css = ShellStylesheets.WithoutComments(AppKitPaths.Stylesheet);

        Assert.Multiple(() =>
        {
            Assert.That(ShellStylesheets.Pixels(comfortable), Is.GreaterThanOrEqualTo(44));
            Assert.That(ShellStylesheets.Pixels(compact), Is.GreaterThanOrEqualTo(24));
            Assert.That(css, Does.Contain("min-height: var(--lt-app-control-height);"));
            Assert.That(css, Does.Contain("min-width: var(--lt-app-control-height);"), "a square target for an icon-only button");
        });
    }

    [Test]
    public void The_kit_gives_fluid_small_width_defaults_without_width_queries()
    {
        var css = ShellStylesheets.WithoutComments(AppKitPaths.Stylesheet);

        Assert.Multiple(() =>
        {
            Assert.That(Regex.IsMatch(css, @"@media[^{]*\b(?:min|max)-width|@container|\bwidth\s*[<>]"), Is.False,
                "the frame is fluid; an app owns any breakpoints of its own");
            Assert.That(Regex.IsMatch(css, @"(?<![-\w])width:\s*\d+(?:\.\d+)?(?:px|rem)"), Is.False, "no fixed widths");
            Assert.That(css, Does.Contain("max-width: 100%;"), "media never overflows the frame");
            Assert.That(css, Does.Contain("overflow-wrap: anywhere;"), "long keys wrap instead of scrolling the page");
            Assert.That(ShellStylesheets.Rules(AppKitPaths.Stylesheet).Select(r => r.Selector), Does.Contain(".lt-app-scroll"),
                "a wide table scrolls inside its own wrapper");
        });
    }

    [Test]
    public void The_kit_stylesheet_is_plain_ascii_without_imports_from_elsewhere()
    {
        var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Stylesheet));

        Assert.That(bytes.All(b => b is 0x09 or 0x0a or 0x0d or (>= 0x20 and < 0x7f)), Is.True);
    }

    private static void AssertSameOverlay(string shellSelector, string kitSelector)
    {
        var shell = ShellStylesheets.Block(ShellStylesheets.Operate, shellSelector)
            .ToDictionary(pair => pair.Key.Replace("--lt-op-", "--lt-app-", StringComparison.Ordinal), pair => pair.Value);
        var kit = ShellStylesheets.Block(AppKitPaths.Stylesheet, kitSelector);

        Assert.That(kit, Is.EquivalentTo(shell), kitSelector);
    }

    private static IEnumerable<TestCaseData> LinkedFileCases() =>
        LinkedFiles.Select(file => new TestCaseData(file.Served, file.Source).SetArgDisplayNames(file.Served));
}
