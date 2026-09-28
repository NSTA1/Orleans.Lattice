using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppAssetPathTests
{
    [TestCase("app.js")]
    [TestCase("js/app.js")]
    [TestCase("assets/fonts/recursive-sans_linear.woff2")]
    [TestCase("a")]
    [TestCase("a/b/c/d.e.f")]
    [TestCase(".well-known/x.json")]
    [TestCase("..x/y.js")]
    public void IsValid_accepts_normalised_relative_paths(string path)
    {
        Assert.That(AppAssetPath.IsValid(path), Is.True);
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("/app.js")]
    [TestCase("../app.js")]
    [TestCase("js/../app.js")]
    [TestCase("js/..")]
    [TestCase("./app.js")]
    [TestCase("js/./app.js")]
    [TestCase("js//app.js")]
    [TestCase("js/")]
    [TestCase("js\\app.js")]
    [TestCase("..\\app.js")]
    [TestCase("c:/app.js")]
    [TestCase("App.js")]
    [TestCase("app.js?v=1")]
    [TestCase("app.js#top")]
    [TestCase("app%2ejs")]
    [TestCase("app .js")]
    [TestCase("https://example.com/app.js")]
    [TestCase("js/app.js\u0000")]
    [TestCase("\u00e9.js")]
    public void IsValid_rejects_traversal_absolute_and_non_normalised_paths(string? path)
    {
        Assert.That(AppAssetPath.IsValid(path), Is.False);
    }

    [Test]
    public void IsValid_bounds_the_length()
    {
        Assert.That(AppAssetPath.IsValid(new string('a', AppAssetPath.MaxLength)), Is.True);
        Assert.That(AppAssetPath.IsValid(new string('a', AppAssetPath.MaxLength + 1)), Is.False);
    }

    [TestCase("index.html", "text/html")]
    [TestCase("a/index.htm", "text/html")]
    [TestCase("site.css", "text/css")]
    [TestCase("app.js", "text/javascript")]
    [TestCase("app.mjs", "text/javascript")]
    [TestCase("icon.svg", "image/svg+xml")]
    [TestCase("icon.png", "image/png")]
    [TestCase("icon.webp", "image/webp")]
    [TestCase("font.woff2", "font/woff2")]
    [TestCase("data.json", "application/json")]
    public void MediaTypeOf_maps_the_bundle_media_types(string path, string expected)
    {
        Assert.That(AppAssetPath.MediaTypeOf(path), Is.EqualTo(expected));
    }

    [TestCase("tool.exe")]
    [TestCase("readme")]
    [TestCase("dir.js/readme")]
    [TestCase("archive.tar.gz")]
    [TestCase("font.woff")]
    [TestCase(".js")]
    public void MediaTypeOf_returns_null_for_any_other_extension(string path)
    {
        Assert.That(AppAssetPath.MediaTypeOf(path), Is.Null);
    }
}
