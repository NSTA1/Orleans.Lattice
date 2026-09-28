using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppUiBundleTests
{
    private static readonly string Zero = new('0', 64);
    private static readonly string One = new('1', 64);

    private static AppUiAsset Asset(string path, string digest, string mediaType = "text/html") =>
        new() { Path = path, MediaType = mediaType, Digest = digest };

    [TestCase("index.html")]
    [TestCase("a/b/c.js")]
    [TestCase("fonts/recursive.woff2")]
    [TestCase("a-b_c.d")]
    [TestCase(".well-known")]
    [TestCase("a/.b")]
    [TestCase("a..b")]
    [TestCase("...")]
    [TestCase("0")]
    public void IsValidPath_accepts_normalised_relative_lower_case_paths(string path)
    {
        Assert.That(AppUiBundle.IsValidPath(path), Is.True);
        Assert.That(SchemaPathPattern.IsMatch(path), Is.True, "schema and validator must agree");
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("/index.html")]
    [TestCase("a//b")]
    [TestCase("a/")]
    [TestCase(".")]
    [TestCase("..")]
    [TestCase("./a")]
    [TestCase("a/./b")]
    [TestCase("../a")]
    [TestCase("a/../b")]
    [TestCase("a/..")]
    [TestCase("a\\b")]
    [TestCase("..\\a")]
    [TestCase("c:/a")]
    [TestCase("A.html")]
    [TestCase("a/B.js")]
    [TestCase("a.html?x=1")]
    [TestCase("a.html#top")]
    [TestCase("%2e%2e/a")]
    [TestCase("a b")]
    [TestCase("a\u0000b")]
    [TestCase("caf\u00e9.html")]
    [TestCase("https://example.com/a.js")]
    [TestCase("blob:a")]
    public void IsValidPath_rejects_traversal_absolute_and_unnormalised_paths(string? path)
    {
        Assert.That(AppUiBundle.IsValidPath(path), Is.False);
        if (path is not null)
            Assert.That(SchemaPathPattern.IsMatch(path) && path.Length <= AppUiBundle.MaxPathLength, Is.False, "schema and validator must agree");
    }

    [Test]
    public void IsValidPath_bounds_the_length()
    {
        Assert.That(AppUiBundle.MaxPathLength, Is.EqualTo(256));
        Assert.That(AppUiBundle.IsValidPath(new string('a', AppUiBundle.MaxPathLength)), Is.True);
        Assert.That(AppUiBundle.IsValidPath(new string('a', AppUiBundle.MaxPathLength + 1)), Is.False);
    }

    [Test]
    public void IsValidDigest_accepts_only_64_lower_case_hex_characters()
    {
        Assert.That(AppUiBundle.IsValidDigest(Zero), Is.True);
        Assert.That(AppUiBundle.IsValidDigest("0123456789abcdef".PadRight(64, 'f')), Is.True);
        foreach (var digest in new[] { null, "", new string('0', 63), new string('0', 65), new string('A', 64), new string('g', 64), " " + new string('0', 63) })
            Assert.That(AppUiBundle.IsValidDigest(digest), Is.False, digest ?? "<null>");
    }

    [Test]
    public void Caps_and_media_type_allow_list_match_bundle_format_v1()
    {
        Assert.That(AppUiBundle.MaxAssets, Is.EqualTo(256));
        Assert.That(AppUiBundle.MaxAssetBytes, Is.EqualTo(2 * 1024 * 1024));
        Assert.That(AppUiBundle.MaxBundleBytes, Is.EqualTo(16 * 1024 * 1024));
        Assert.That(AppUiBundle.AllowedMediaTypes, Is.EquivalentTo(new[]
        {
            "text/html", "text/css", "text/javascript", "image/svg+xml", "image/png", "image/webp", "font/woff2", "application/json",
        }));
        Assert.That(AppUiBundle.AllowedMediaTypes.Contains("TEXT/HTML"), Is.False);
        Assert.That(AppUiBundle.AllowedMediaTypes.Contains("text/plain"), Is.False);
    }

    [Test]
    public void ComputeBundleDigest_matches_the_published_formula_for_known_vectors()
    {
        Assert.That(AppUiBundle.ComputeBundleDigest([]),
            Is.EqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
        Assert.That(AppUiBundle.ComputeBundleDigest([Asset("b.css", One, "text/css"), Asset("a.html", Zero)]),
            Is.EqualTo("c595a04213c0685bd57d47c1c075100d4dc7f9347a0203f911343586840da922"));
    }

    [Test]
    public void ComputeBundleDigest_is_order_independent_and_ignores_media_type()
    {
        AppUiAsset[] assets = [Asset("z/last.js", One, "text/javascript"), Asset("index.html", Zero), Asset("a.css", Zero, "text/css")];
        var expected = Expected(assets);
        Assert.That(AppUiBundle.ComputeBundleDigest(assets), Is.EqualTo(expected));
        Assert.That(AppUiBundle.ComputeBundleDigest(assets.Reverse().ToArray()), Is.EqualTo(expected));
        Assert.That(AppUiBundle.ComputeBundleDigest(assets.Select(a => a with { MediaType = "application/json" }).ToList()), Is.EqualTo(expected));
        Assert.That(AppUiBundle.ComputeBundleDigest([assets[0] with { Digest = Zero }, assets[1], assets[2]]), Is.Not.EqualTo(expected));
    }

    [Test]
    public void ComputeBundleDigest_sorts_ordinally_not_culturally()
    {
        AppUiAsset[] assets = [Asset("b.js", Zero), Asset("B.js", One), Asset("_.js", Zero), Asset("a-b.js", One)];
        Assert.That(AppUiBundle.ComputeBundleDigest(assets), Is.EqualTo(Expected(assets)));
    }

    [Test]
    public void ComputeBundleDigest_hashes_duplicate_paths_deterministically()
    {
        AppUiAsset[] assets = [Asset("a.js", One), Asset("a.js", Zero)];
        Assert.That(AppUiBundle.ComputeBundleDigest(assets), Is.EqualTo(AppUiBundle.ComputeBundleDigest(assets.Reverse().ToArray())));
    }

    [Test]
    public void ComputeBundleDigest_handles_paths_longer_than_the_pooled_buffer()
    {
        AppUiAsset[] assets = [Asset(new string('p', 5000), Zero), Asset("a", One)];
        Assert.That(AppUiBundle.ComputeBundleDigest(assets), Is.EqualTo(Expected(assets)));
    }

    [Test]
    public void ComputeBundleDigest_guards_its_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => AppUiBundle.ComputeBundleDigest(null!));
        Assert.Throws<ArgumentException>(() => AppUiBundle.ComputeBundleDigest([null!]));
        Assert.Throws<ArgumentException>(() => AppUiBundle.ComputeBundleDigest([Asset(null!, Zero)]));
        Assert.Throws<ArgumentException>(() => AppUiBundle.ComputeBundleDigest([Asset("a", null!)]));
        Assert.Throws<ArgumentException>(() => AppUiBundle.ComputeBundleDigest(new MiscountedCollection([Asset("a", Zero)], 2)));
        Assert.Throws<ArgumentException>(() => AppUiBundle.ComputeBundleDigest(new MiscountedCollection([Asset("a", Zero), Asset("b", Zero)], 1)));
    }

    private sealed class MiscountedCollection(AppUiAsset[] items, int count) : IReadOnlyCollection<AppUiAsset>
    {
        public int Count => count;
        public IEnumerator<AppUiAsset> GetEnumerator() => ((IEnumerable<AppUiAsset>)items).GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private static string Expected(IEnumerable<AppUiAsset> assets) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Concat(
            assets.OrderBy(a => a.Path, StringComparer.Ordinal).Select(a => $"{a.Path}\0{a.Digest}\n")))));

    private static Regex SchemaPathPattern { get; } = new(
        JsonNode.Parse(AppManifestResources.GetJsonSchema())!["$defs"]!["bundlePath"]!["pattern"]!.GetValue<string>());
}
