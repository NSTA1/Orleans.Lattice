using System.Text;
using Orleans.Lattice.Explorer.UI.Framing;
using F1 = Orleans.Lattice.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing;

/// <summary>
/// Pins the Shell's port of F1's bundle rules (<see cref="AppFrameBundleRules"/>) byte for byte
/// to the authoritative implementation in <c>Orleans.Lattice.Apps</c>, over a corpus that
/// includes hostile inputs. The Shell does not reference <c>lattice.apps</c>, so this test is
/// the permanent guard: change either side and it fails until the other follows.
/// </summary>
[TestFixture]
public sealed class AppFrameBundleRulesParityTests
{
    private static readonly string?[] Paths =
    [
        null, string.Empty, "a", "index.html", "app/main.js", "fonts/mono.woff2", "a-b_c.d/e", "__proto__", "constructor",
        "..", ".", "../x", "a/../b", "a/./b", "./a", "/a", "a/", "a//b", "a\\b", "C:/a", "c:", "a?b", "a#b", "a%2fb", "%2e%2e/x",
        "A", "Index.html", "a b", "a\tb", "a\0b", "\u00e9", "a/\u212a", "\u0130", "\uff0e\uff0e/x", "a\u200bb", "\ud800",
        "a.", ".a", "...", "a..b", "-", "_", new string('a', 256), new string('a', 257), "a/" + new string('b', 254),
    ];

    private static readonly string?[] Digests =
    [
        null, string.Empty, new string('a', 64), new string('0', 64), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        new string('A', 64), new string('a', 63), new string('a', 65), new string('g', 64), new string('a', 63) + " ",
        new string('a', 63) + "\u0661", new string('a', 63) + "\uff41",
    ];

    [Test]
    public void IsValidPath_matches_F1_on_every_corpus_path()
    {
        Assert.Multiple(() =>
        {
            foreach (var path in Paths)
            {
                Assert.That(AppFrameBundleRules.IsValidPath(path), Is.EqualTo(F1.AppUiBundle.IsValidPath(path)), Describe(path));
            }
        });
    }

    [Test]
    public void IsValidDigest_matches_F1_on_every_corpus_digest()
    {
        Assert.Multiple(() =>
        {
            foreach (var digest in Digests)
            {
                Assert.That(AppFrameBundleRules.IsValidDigest(digest), Is.EqualTo(F1.AppUiBundle.IsValidDigest(digest)), Describe(digest));
            }
        });
    }

    [Test]
    public void ComputeBundleDigest_matches_F1_on_every_corpus_bundle()
    {
        List<(string Path, string Digest)[]> bundles =
        [
            [],
            [("index.html", new string('a', 64))],
            [("b.js", new string('b', 64)), ("a.css", new string('c', 64)), ("index.html", new string('d', 64))],
            [("a", "x"), ("a", "w"), ("A", "y")],
            [("\u00e9/\u0000", "\ud800"), ("z", string.Empty)],
            [(new string('p', 600), new string('d', 600))],
        ];

        var random = new Random(3817);
        for (var i = 0; i < 25; i++)
        {
            var count = random.Next(0, 40);
            var assets = new (string, string)[count];
            for (var j = 0; j < count; j++)
            {
                assets[j] = (RandomText(random, 1, 30), RandomText(random, 0, 70));
            }

            bundles.Add(assets);
        }

        Assert.Multiple(() =>
        {
            foreach (var bundle in bundles)
            {
                var f1 = F1.AppUiBundle.ComputeBundleDigest(bundle
                    .Select(asset => new F1.AppUiAsset { Path = asset.Path, MediaType = "text/html", Digest = asset.Digest })
                    .ToArray());
                Assert.That(AppFrameBundleRules.ComputeBundleDigest(bundle), Is.EqualTo(f1));
            }
        });
    }

    [Test]
    public void ComputeBundleDigest_rejects_null_input_as_F1_does()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => AppFrameBundleRules.ComputeBundleDigest(null!), Throws.ArgumentNullException);
            Assert.That(() => AppFrameBundleRules.ComputeBundleDigest([(null!, "a")]), Throws.ArgumentException);
            Assert.That(() => AppFrameBundleRules.ComputeBundleDigest([("a", null!)]), Throws.ArgumentException);
        });
    }

    [Test]
    public void IsValidEntryFragment_matches_F1_on_every_corpus_fragment()
    {
        List<byte[]> fragments =
        [
            [],
            Bytes("<main>ok</main>"),
            Bytes("<script>alert(1)</script>"),
            Bytes("<SCRIPT src=x>"),
            Bytes("<ScRiPt"),
            Bytes("<scripts>"),
            Bytes("<script\n>"),
            Bytes("< script>"),
            Bytes("<!-- <script> -->"),
            Bytes("<svg><script>1</script></svg>"),
            Bytes("<noscript>x</noscript>"),
            Bytes("&lt;script&gt;"),
            Bytes("<html>"),
            Bytes("<HTML lang=en>"),
            Bytes("<html"),
            Bytes("<htmlx>"),
            Bytes("<head>"),
            Bytes("<head/>"),
            Bytes("<header>ok</header>"),
            Bytes("<headx"),
            Bytes("<body>ok</body>"),
            Bytes("<<script"),
            Bytes("<"),
            Bytes("<s"),
            Bytes("<\u0131script>"),
            Bytes("<\u017fcript>"),
            [0xC3, 0x28],
            [0xED, 0xA0, 0x80],
            [0xF8, 0x88, 0x80, 0x80, 0x80],
            [0x3C, 0x00, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74],
            Bytes("<main>" + new string('x', F1.AppUiBundle.MaxAssetBytes - 13) + "</main>"),
            Bytes("<main>" + new string('x', F1.AppUiBundle.MaxAssetBytes - 12) + "</main>"),
        ];

        Assert.Multiple(() =>
        {
            foreach (var fragment in fragments)
            {
                var f1 = F1.AppManifestValidator.ValidateUiEntryFragment(fragment).Count == 0;
                Assert.That(AppFrameBundleRules.IsValidEntryFragment(fragment), Is.EqualTo(f1), Describe(Encoding.UTF8.GetString(fragment[..Math.Min(40, fragment.Length)])));
            }
        });
    }

    [Test]
    public void The_limits_and_media_types_match_F1()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppFrameBundleRules.MaxAssets, Is.EqualTo(F1.AppUiBundle.MaxAssets));
            Assert.That(AppFrameBundleRules.MaxAssetBytes, Is.EqualTo(F1.AppUiBundle.MaxAssetBytes));
            Assert.That(AppFrameBundleRules.MaxBundleBytes, Is.EqualTo(F1.AppUiBundle.MaxBundleBytes));
            Assert.That(AppFrameBundleRules.MaxPathLength, Is.EqualTo(F1.AppUiBundle.MaxPathLength));
            Assert.That(AppFrameBundleRules.AllowedMediaTypes, Is.EquivalentTo(F1.AppUiBundle.AllowedMediaTypes));
        });
    }

    [Test]
    public void ComputeDigest_is_lower_case_sha256()
    {
        Assert.That(
            AppFrameBundleRules.ComputeDigest("abc"u8),
            Is.EqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Describe(string? value) =>
        value is null ? "(null)" : "\"" + string.Concat(value.Take(60).Select(c => c < 0x20 || c > 0x7e ? $"\\u{(int)c:x4}" : c.ToString())) + "\"";

    private static string RandomText(Random random, int min, int max)
    {
        const string Alphabet = "abcxyz019./_-AZ\u00e9\u4e2d";
        var length = random.Next(min, max + 1);
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = Alphabet[random.Next(Alphabet.Length)];
        }

        return new string(chars);
    }
}
