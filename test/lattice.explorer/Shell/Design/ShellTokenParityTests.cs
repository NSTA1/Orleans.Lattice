using System.IO.Compression;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design;

/// <summary>
/// The Shell serves the documentation site's own <c>tokens.css</c>, fonts,
/// licences and favicon, byte for byte, by linking them into its static web
/// assets at build time (epic #3807, E10). A hand-maintained copy would drift
/// from the site the first time either changed; this is the gate that says it
/// cannot.
/// </summary>
/// <remarks>
/// It reads the static web asset manifest the Razor SDK writes for the Shell -
/// the list a head serves at runtime - rather than the project file, so it
/// measures what is served: each asset's source must be the docs-site file
/// itself, and the pre-compressed variant the head may serve instead must
/// decompress to the same bytes.
/// </remarks>
[TestFixture]
public sealed class ShellTokenParityTests
{
    private const string ShellProject = "src/lattice.explorer/Shell";
    private const string ShellBasePath = "_content/Orleans.Lattice.Explorer.Shell";

    /// <summary>Every docs-site file the Shell links, and the path it is served at.</summary>
    private static readonly (string Served, string Source)[] LinkedFiles =
    [
        ("design/tokens.css", "docs-site/template/public/tokens.css"),
        ("design/favicon.svg", "docs-site/template/public/favicon.svg"),
        ("design/fonts/recursive-sans-linear.woff2", "docs-site/template/public/fonts/recursive-sans-linear.woff2"),
        ("design/fonts/cascadia-mono.woff2", "docs-site/template/public/fonts/cascadia-mono.woff2"),
        ("design/fonts/OFL-Recursive.txt", "docs-site/template/public/fonts/OFL-Recursive.txt"),
        ("design/fonts/OFL-CascadiaMono.txt", "docs-site/template/public/fonts/OFL-CascadiaMono.txt"),
    ];

    [Test]
    public void The_shell_serves_the_docs_site_tokens_byte_identical()
    {
        AssertServedByteIdentical("design/tokens.css", "docs-site/template/public/tokens.css");
    }

    [Test]
    [TestCaseSource(nameof(LinkedFileCases))]
    public void Every_linked_docs_site_asset_is_served_byte_identical(string served, string source)
    {
        AssertServedByteIdentical(served, source);
    }

    [Test]
    public void The_pre_compressed_tokens_variant_decompresses_to_the_docs_site_bytes()
    {
        // A head may serve the .gz variant instead of the file; it has to be the
        // same stylesheet, not a stale compression of an older one.
        var compressed = StaticWebAssetManifest.Assets(ShellProject)
            .Where(asset => asset.IsCompressed && asset.RelativePath == "design/tokens.css")
            .ToArray();

        Assert.That(compressed, Is.Not.Empty, "the build compresses the linked tokens.css");

        var expected = File.ReadAllBytes(ShellStylesheets.Absolute(ShellStylesheets.DocsSiteTokens));
        foreach (var asset in compressed)
        {
            using var input = new GZipStream(File.OpenRead(asset.Identity), CompressionMode.Decompress);
            using var output = new MemoryStream();
            input.CopyTo(output);
            Assert.That(output.ToArray(), Is.EqualTo(expected), asset.Identity + " must decompress to tokens.css");
        }
    }

    [Test]
    public void No_linked_file_is_copied_into_the_shell_wwwroot()
    {
        // The link is what keeps one source of truth. A copy dropped beside it
        // would win the static-web-asset conflict or fail the build, and either
        // way would stop tracking the documentation site.
        Assert.Multiple(() =>
        {
            foreach (var (served, _) in LinkedFiles)
            {
                var copy = ShellStylesheets.Absolute(ShellProject + "/wwwroot/" + served);
                Assert.That(File.Exists(copy), Is.False, served + " must be linked from docs-site, never copied");
            }
        });
    }

    [Test]
    public void The_shell_project_links_each_file_as_content_rather_than_copying_it()
    {
        var project = File.ReadAllText(ShellStylesheets.Absolute(ShellProject + "/Orleans.Lattice.Explorer.Shell.csproj"));

        Assert.Multiple(() =>
        {
            foreach (var (served, _) in LinkedFiles)
            {
                var link = "Link=\"wwwroot\\" + served.Replace('/', '\\') + "\"";
                Assert.That(project, Does.Contain(link), $"the Shell project must link {served}");
            }

            Assert.That(Regex.IsMatch(project, @"<Copy\b"), Is.False, "the Shell must not copy design assets at build time");
        });
    }

    [Test]
    public void The_manifest_reader_strips_the_fingerprint_pattern()
    {
        // Battery test: without this the gates above would compare nothing.
        Assert.Multiple(() =>
        {
            Assert.That(StaticWebAssetManifest.StripFingerprint("design/tokens#[.{fingerprint}]?.css"),
                Is.EqualTo("design/tokens.css"));
            Assert.That(StaticWebAssetManifest.StripFingerprint("appkit/v1/frame#[.{fingerprint=abc}]?.html"),
                Is.EqualTo("appkit/v1/frame.html"));
            Assert.That(StaticWebAssetManifest.StripFingerprint("design/plain.css"), Is.EqualTo("design/plain.css"));
        });
    }

    private static IEnumerable<TestCaseData> LinkedFileCases() =>
        LinkedFiles.Select(file => new TestCaseData(file.Served, file.Source).SetArgDisplayNames(file.Served));

    private static void AssertServedByteIdentical(string served, string source)
    {
        var assets = StaticWebAssetManifest.Assets(ShellProject)
            .Where(asset => !asset.IsCompressed && asset.RelativePath == served)
            .ToArray();

        Assert.That(assets, Has.Length.EqualTo(1), $"the Shell must serve exactly one {served}");

        var asset = assets[0];
        var sourcePath = ShellStylesheets.Absolute(source);

        Assert.Multiple(() =>
        {
            Assert.That(asset.BasePath, Is.EqualTo(ShellBasePath));
            Assert.That(
                Path.GetFullPath(asset.Identity),
                Is.EqualTo(Path.GetFullPath(sourcePath)).IgnoreCase,
                $"{served} must be served from {source} itself, not from a copy");
            Assert.That(File.ReadAllBytes(asset.Identity), Is.EqualTo(File.ReadAllBytes(sourcePath)),
                $"{served} must be byte-identical to {source}");
        });
    }
}
