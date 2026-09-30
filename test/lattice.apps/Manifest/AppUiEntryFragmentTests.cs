using System.Text;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppUiEntryFragmentTests
{
    private static IReadOnlyList<AppManifestError> Validate(string html) =>
        AppManifestValidator.ValidateUiEntryFragment(Encoding.UTF8.GetBytes(html));

    [TestCase("")]
    [TestCase("<main id=\"app\"><h1>Tasks</h1></main>")]
    [TestCase("<header>Top</header><section class=\"heading\">x</section>")]
    [TestCase("<htmlish-widget></htmlish-widget><head-count>3</head-count>")]
    [TestCase("<p>a &lt;script&gt; is escaped text</p>")]
    [TestCase("</script>")]
    [TestCase("<p>caf\u00e9 \u65e5\u672c</p>")]
    [TestCase("< script>not a tag</p>")]
    public void ValidateUiEntryFragment_accepts_script_free_fragments(string html)
    {
        Assert.That(Validate(html), Is.Empty);
    }

    [TestCase("<script>alert(1)</script>")]
    [TestCase("<SCRIPT src=\"x.js\"></SCRIPT>")]
    [TestCase("<ScRiPt>")]
    [TestCase("<svg><script>alert(1)</script></svg>")]
    [TestCase("<!-- <script> -->")]
    [TestCase("<div><script")]
    [TestCase("<scriptx>")]
    [TestCase("<p>ok</p>\n<script type=\"module\">import x from './a.js'</script>")]
    public void ValidateUiEntryFragment_rejects_any_script_element(string html)
    {
        var errors = Validate(html);
        Assert.That(errors.Select(e => (e.Code, e.Path)), Does.Contain(("fragment", "$.ui.entry")));
    }

    [TestCase("<html><body>x</body></html>")]
    [TestCase("<HEAD><title>x</title></HEAD>")]
    [TestCase("<head/>")]
    [TestCase("<html\tlang=\"en\">")]
    [TestCase("<p>x</p><html")]
    public void ValidateUiEntryFragment_rejects_documents(string html)
    {
        var errors = Validate(html);
        Assert.That(errors, Has.Count.EqualTo(1));
        Assert.That(errors[0].Code, Is.EqualTo("fragment"));
        Assert.That(errors[0].Path, Is.EqualTo("$.ui.entry"));
    }

    [Test]
    public void ValidateUiEntryFragment_reports_every_distinct_defect()
    {
        var errors = AppManifestValidator.ValidateUiEntryFragment([.. "<html><script>"u8, 0xC3, 0x28]);
        Assert.That(errors.Select(e => e.Code), Is.EqualTo(new[] { "encoding", "fragment", "fragment" }));
    }

    [Test]
    public void ValidateUiEntryFragment_rejects_malformed_utf8()
    {
        Assert.That(AppManifestValidator.ValidateUiEntryFragment([0x3C, 0x70, 0x3E, 0xFF]).Single().Code, Is.EqualTo("encoding"));
    }

    [Test]
    public void ValidateUiEntryFragment_is_bounded_by_the_asset_cap()
    {
        var atCap = new byte[AppUiBundle.MaxAssetBytes];
        atCap.AsSpan().Fill((byte)'a');
        Assert.That(AppManifestValidator.ValidateUiEntryFragment(atCap), Is.Empty);
        var over = new byte[AppUiBundle.MaxAssetBytes + 1];
        Assert.That(AppManifestValidator.ValidateUiEntryFragment(over).Single().Code, Is.EqualTo("limit"));
    }
}
