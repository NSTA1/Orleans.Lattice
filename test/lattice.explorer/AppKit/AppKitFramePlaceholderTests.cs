using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.Tests.Shell.Design;

namespace Orleans.Lattice.Explorer.Tests.AppKit;

/// <summary>
/// The AppKit skeleton: the placeholder frame bootstrap exists at the versioned
/// path the frame host maps, is published as a static web asset of the AppKit
/// package, and already obeys the constraints the real bootstrap will be served
/// under - no inline script or style, and no third-party origin.
/// </summary>
[TestFixture]
public sealed class AppKitFramePlaceholderTests
{
    private const string AppKitProject = "src/lattice.explorer/AppKit";
    private const string FramePath = AppKitProject + "/wwwroot/appkit/v1/frame.html";

    [Test]
    public void The_placeholder_bootstrap_is_published_at_the_versioned_path()
    {
        var frames = StaticWebAssetManifest.Assets("src/lattice.explorer/Shell")
            .Where(asset => !asset.IsCompressed && asset.RelativePath == "appkit/v1/frame.html")
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(frames, Has.Length.EqualTo(1), "a head that references the Shell serves the AppKit frame");
            Assert.That(frames[0].BasePath, Is.EqualTo("_content/Orleans.Lattice.Explorer.AppKit"));
            Assert.That(Path.GetFullPath(frames[0].Identity), Is.EqualTo(Path.GetFullPath(ShellStylesheets.Absolute(FramePath))).IgnoreCase);
        });
    }

    [Test]
    public void The_placeholder_declares_the_protocol_version_it_stands_in_for()
    {
        var html = File.ReadAllText(ShellStylesheets.Absolute(FramePath));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.StartWith("<!doctype html>"));
            Assert.That(html, Does.Contain("<meta charset=\"utf-8\">"));
            Assert.That(html, Does.Contain("data-appkit-protocol=\"1\""));
        });
    }

    [Test]
    public void The_placeholder_already_fits_the_frame_content_security_policy()
    {
        // The bootstrap is served with "default-src 'none'; script-src 'self'
        // blob:; style-src 'self' blob:" (epic #3807, E4), so inline script,
        // inline style and any absolute URL would be blocked or would leak.
        var html = Regex.Replace(File.ReadAllText(ShellStylesheets.Absolute(FramePath)), "<!--.*?-->", string.Empty, RegexOptions.Singleline);

        Assert.Multiple(() =>
        {
            Assert.That(Regex.IsMatch(html, @"<script(?![^>]*\bsrc=)[^>]*>", RegexOptions.IgnoreCase), Is.False, "no inline script");
            Assert.That(Regex.IsMatch(html, @"<style\b", RegexOptions.IgnoreCase), Is.False, "no inline style element");
            Assert.That(Regex.IsMatch(html, @"\sstyle\s*=", RegexOptions.IgnoreCase), Is.False, "no inline style attribute");
            Assert.That(Regex.IsMatch(html, @"\son[a-z]+\s*=", RegexOptions.IgnoreCase), Is.False, "no inline event handler");
            Assert.That(Regex.IsMatch(html, @"(?:https?:)?//", RegexOptions.IgnoreCase), Is.False, "no absolute or third-party URL");
        });
    }

    [Test]
    public void The_appkit_project_references_nothing()
    {
        var project = File.ReadAllText(ShellStylesheets.Absolute(AppKitProject + "/Orleans.Lattice.Explorer.AppKit.csproj"));

        Assert.Multiple(() =>
        {
            Assert.That(project, Does.Not.Contain("<ProjectReference"), "everything in AppKit runs inside an untrusted frame");
            Assert.That(project, Does.Not.Contain("<PackageReference"));
        });
    }
}
