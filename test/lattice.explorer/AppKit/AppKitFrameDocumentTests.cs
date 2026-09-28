using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The bootstrap document every app frame loads: published at the versioned
/// path, static and app-agnostic, and compatible with the exact frame content
/// security policy (epic #3807, E4) - no inline script or style, and nothing
/// loaded from anywhere but beside it.
/// </summary>
[TestFixture]
public sealed class AppKitFrameDocumentTests
{
    [Test]
    public void The_bootstrap_is_published_at_the_versioned_path()
    {
        var frames = StaticWebAssetManifest.Assets("src/lattice.explorer/Shell")
            .Where(asset => !asset.IsCompressed && asset.RelativePath == "appkit/v1/frame.html")
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(frames, Has.Length.EqualTo(1), "a head that references the Shell serves the AppKit frame");
            Assert.That(frames[0].BasePath, Is.EqualTo(AppKitPaths.BasePath));
            Assert.That(Path.GetFullPath(frames[0].Identity), Is.EqualTo(Path.GetFullPath(AppKitPaths.Absolute(AppKitPaths.Frame))).IgnoreCase);
        });
    }

    [Test]
    public void Every_kit_file_the_frame_needs_is_published_beside_it()
    {
        var published = StaticWebAssetManifest.Assets(AppKitPaths.Project)
            .Where(asset => !asset.IsCompressed)
            .Select(asset => asset.RelativePath)
            .ToHashSet(StringComparer.Ordinal);

        Assert.That(published, Is.SupersetOf(new[]
        {
            "appkit/v1/frame.html", "appkit/v1/boot.js", "appkit/v1/lattice-app.css", "appkit/v1/tokens.css",
            "appkit/v1/protocol.schema.json", "appkit/v1/fonts/recursive-sans-linear.woff2",
            "appkit/v1/fonts/cascadia-mono.woff2", "appkit/v1/fonts/OFL-Recursive.txt", "appkit/v1/fonts/OFL-CascadiaMono.txt",
        }));
    }

    [Test]
    public void The_example_bundle_is_not_published()
    {
        // The example is a starting point for app authors, not part of the kit a frame loads.
        var published = StaticWebAssetManifest.Assets(AppKitPaths.Project).Select(asset => asset.RelativePath);

        Assert.That(published.Where(path => !path.StartsWith("appkit/v1/", StringComparison.Ordinal)), Is.Empty);
    }

    [Test]
    public void The_bootstrap_is_a_utf8_document_that_declares_its_protocol()
    {
        var html = AppKitPaths.Read(AppKitPaths.Frame);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.StartWith("<!doctype html>"));
            Assert.That(html, Does.Contain("<meta charset=\"utf-8\">"));
            Assert.That(html, Does.Contain("<meta name=\"referrer\" content=\"no-referrer\">"));
            Assert.That(html, Does.Contain("data-appkit-protocol=\"1\""));
            Assert.That(html, Does.Contain("data-theme=\"paper\""));
        });
    }

    [Test]
    public void The_bootstrap_loads_exactly_the_loader_and_the_kit_stylesheet()
    {
        var html = WithoutComments();
        var scripts = Regex.Matches(html, "<script\\b[^>]*>", RegexOptions.IgnoreCase).Select(m => m.Value).ToArray();
        var links = Regex.Matches(html, "<link\\b[^>]*>", RegexOptions.IgnoreCase).Select(m => m.Value).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(scripts, Is.EqualTo(new[] { "<script src=\"boot.js\" defer>" }), "one classic, deferred loader from beside the document");
            Assert.That(links, Is.EqualTo(new[] { "<link rel=\"stylesheet\" href=\"lattice-app.css\">" }));
            Assert.That(Regex.Matches(html, "</script>", RegexOptions.IgnoreCase), Has.Count.EqualTo(1), "the script element is empty");
        });
    }

    [Test]
    public void The_bootstrap_fits_the_frame_content_security_policy()
    {
        // "default-src 'none'; script-src 'self' blob:; style-src 'self' blob:; form-action 'none';
        // base-uri 'none'": inline script, inline style, absolute URLs, forms and <base> would be blocked or leak.
        var html = WithoutComments();

        Assert.Multiple(() =>
        {
            Assert.That(Regex.IsMatch(html, @"<script(?![^>]*\bsrc=)[^>]*>", RegexOptions.IgnoreCase), Is.False, "no inline script");
            Assert.That(Regex.IsMatch(html, @"<style\b", RegexOptions.IgnoreCase), Is.False, "no inline style element");
            Assert.That(Regex.IsMatch(html, @"\sstyle\s*=", RegexOptions.IgnoreCase), Is.False, "no inline style attribute");
            Assert.That(Regex.IsMatch(html, @"\son[a-z]+\s*=", RegexOptions.IgnoreCase), Is.False, "no inline event handler");
            Assert.That(Regex.IsMatch(html, @"(?:https?:)?//", RegexOptions.IgnoreCase), Is.False, "no absolute or third-party URL");
            Assert.That(Regex.IsMatch(html, @"javascript:|data:", RegexOptions.IgnoreCase), Is.False, "no script or data URL");
            Assert.That(Regex.IsMatch(html, @"<(?:base|form|iframe|object|embed|img)\b", RegexOptions.IgnoreCase), Is.False, "nothing the policy forbids");
            Assert.That(Regex.IsMatch(html, @"http-equiv", RegexOptions.IgnoreCase), Is.False, "the policy is a response header, never a meta tag the frame could weaken");
        });
    }

    [Test]
    public void The_bootstrap_body_is_empty_and_app_agnostic()
    {
        var html = WithoutComments();
        var body = Regex.Match(html, "<body>(.*)</body>", RegexOptions.Singleline).Groups[1].Value;

        Assert.Multiple(() =>
        {
            Assert.That(body.Trim(), Is.Empty, "the entry fragment is the only body content");
            Assert.That(html, Does.Not.Contain("slug").And.Not.Contain("tenant"), "nothing app- or user-specific is baked in");
        });
    }

    [Test]
    public void The_bootstrap_is_plain_ascii()
    {
        var bytes = File.ReadAllBytes(AppKitPaths.Absolute(AppKitPaths.Frame));

        Assert.That(bytes.All(b => b is >= 0x09 and < 0x7f), Is.True);
    }

    [Test]
    public void The_appkit_project_references_nothing()
    {
        var project = AppKitPaths.Read(AppKitPaths.Project + "/Orleans.Lattice.Explorer.AppKit.csproj");

        Assert.Multiple(() =>
        {
            Assert.That(project, Does.Not.Contain("<ProjectReference"), "everything in AppKit runs inside an untrusted frame");
            Assert.That(project, Does.Not.Contain("<PackageReference"));
        });
    }

    private static string WithoutComments() =>
        Regex.Replace(AppKitPaths.Read(AppKitPaths.Frame), "<!--.*?-->", string.Empty, RegexOptions.Singleline);
}
