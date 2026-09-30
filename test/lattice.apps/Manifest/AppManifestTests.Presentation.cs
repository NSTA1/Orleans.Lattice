namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    private static AppManifest WithPresentation(Func<AppPresentation, AppPresentation> change)
    {
        var manifest = UiManifest;
        return manifest with { Presentation = change(manifest.Presentation!) };
    }

    [TestCase("displayName", 60)]
    [TestCase("summary", 160)]
    [TestCase("description", 4000)]
    [TestCase("publisherDisplayName", 80)]
    public void Presentation_text_is_accepted_at_its_bound_and_rejected_past_it(string member, int max)
    {
        AppManifest Set(string value) => WithPresentation(p => member switch
        {
            "displayName" => p with { DisplayName = value },
            "summary" => p with { Summary = value },
            "description" => p with { Description = value },
            _ => p with { PublisherDisplayName = value },
        });

        AssertValid(Set(new string('x', max)));
        AssertError(Set(new string('x', max + 1)), "limit", "$.presentation." + member);
    }

    [Test]
    public void Presentation_display_name_is_required_and_optional_text_may_be_omitted_but_not_blank()
    {
        AssertError(WithPresentation(p => p with { DisplayName = null! }), "required", "$.presentation.displayName");
        AssertError(WithPresentation(p => p with { DisplayName = "" }), "required", "$.presentation.displayName");
        AssertError(WithPresentation(p => p with { DisplayName = " \t " }), "required", "$.presentation.displayName");
        AssertError(WithPresentation(p => p with { Summary = "" }), "required", "$.presentation.summary");
        AssertError(WithPresentation(p => p with { Description = "\n" }), "required", "$.presentation.description");
        AssertError(WithPresentation(p => p with { PublisherDisplayName = " " }), "required", "$.presentation.publisherDisplayName");
        AssertValid(WithPresentation(_ => new AppPresentation { DisplayName = "X" }));
    }

    [TestCase("\n")]
    [TestCase("\r")]
    [TestCase("\t")]
    [TestCase("\u0000")]
    [TestCase("\u0007")]
    [TestCase("\u001b")]
    [TestCase("\u007f")]
    [TestCase("\u0085")]
    [TestCase("\u2028")]
    [TestCase("\u2029")]
    [TestCase("\u202a")]
    [TestCase("\u202e")]
    [TestCase("\u2066")]
    [TestCase("\u2069")]
    public void Presentation_single_line_text_rejects_breaks_controls_and_bidirectional_overrides(string character)
    {
        var text = "a" + character + "b";
        AssertError(WithPresentation(p => p with { DisplayName = text }), "text", "$.presentation.displayName");
        AssertError(WithPresentation(p => p with { Summary = text }), "text", "$.presentation.summary");
        AssertError(WithPresentation(p => p with { PublisherDisplayName = text }), "text", "$.presentation.publisherDisplayName");
    }

    [Test]
    public void Presentation_description_allows_line_breaks_and_tabs_but_no_other_controls()
    {
        AssertValid(WithPresentation(p => p with { Description = "One\nTwo\r\nThree\tfour\u2028five\u2029six <b>not html</b> **not markdown**" }));
        foreach (var character in new[] { "\u0000", "\u0007", "\u000b", "\u000c", "\u0085", "\u202e", "\u2067" })
            AssertError(WithPresentation(p => p with { Description = "a" + character + "b" }), "text", "$.presentation.description");
    }

    [Test]
    public void Presentation_text_accepts_ordinary_unicode()
    {
        AssertValid(WithPresentation(p => p with { DisplayName = "Caf\u00e9 \u65e5\u672c \u0645\u0631\u062d\u0628\u0627", Summary = "\u200f right-to-left mark is fine" }));
    }

    [TestCase("ab")]
    [TestCase("data")]
    [TestCase("developer-tools")]
    [TestCase("a1")]
    [TestCase("a234567890123456789012345678901")]
    public void Presentation_category_accepts_the_published_pattern(string category)
    {
        Assert.That(category.Length, Is.LessThanOrEqualTo(31));
        AssertValid(WithPresentation(p => p with { Categories = [category] }));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("a")]
    [TestCase("1ab")]
    [TestCase("-ab")]
    [TestCase("Data")]
    [TestCase("ab_c")]
    [TestCase("ab c")]
    [TestCase("a2345678901234567890123456789012")]
    public void Presentation_category_rejects_values_outside_the_pattern(string? category)
    {
        AssertError(WithPresentation(p => p with { Categories = [category!] }), "category", "$.presentation.categories[0]");
    }

    [Test]
    public void Presentation_categories_are_bounded_and_unique()
    {
        AssertValid(WithPresentation(p => p with { Categories = [] }));
        AssertValid(WithPresentation(p => p with { Categories = null }));
        AssertValid(WithPresentation(p => p with { Categories = ["aa", "bb", "cc", "dd", "ee"] }));
        AssertError(WithPresentation(p => p with { Categories = ["aa", "bb", "cc", "dd", "ee", "ff"] }), "limit", "$.presentation.categories");
        AssertError(WithPresentation(p => p with { Categories = ["aa", "bb", "aa"] }), "duplicate", "$.presentation.categories[2]");
    }

    [TestCase("https://example.com")]
    [TestCase("https://example.com/docs/a?b=c#d")]
    [TestCase("HTTPS://EXAMPLE.COM/")]
    [TestCase("https://example.com:8443/x")]
    [TestCase("https://[::1]/docs")]
    public void Presentation_documentation_url_accepts_absolute_https(string url)
    {
        AssertValid(WithPresentation(p => p with { DocumentationUrl = url }));
    }

    [TestCase("")]
    [TestCase("http://example.com")]
    [TestCase("ftp://example.com")]
    [TestCase("javascript:alert(1)")]
    [TestCase("data:text/html,x")]
    [TestCase("//example.com/docs")]
    [TestCase("/docs")]
    [TestCase("docs/index.html")]
    [TestCase("https://user:pass@example.com")]
    [TestCase("https://user@example.com")]
    [TestCase(" https://example.com")]
    [TestCase("https://example.com/a b")]
    [TestCase("https://example.com/\n")]
    [TestCase("https:///path")]
    public void Presentation_documentation_url_rejects_anything_but_absolute_https(string url)
    {
        AssertError(WithPresentation(p => p with { DocumentationUrl = url }), "url", "$.presentation.documentationUrl");
    }

    [Test]
    public void Presentation_documentation_url_is_length_bounded()
    {
        const string prefix = "https://example.com/";
        AssertValid(WithPresentation(p => p with { DocumentationUrl = prefix + new string('a', 2048 - prefix.Length) }));
        AssertError(WithPresentation(p => p with { DocumentationUrl = prefix + new string('a', 2049 - prefix.Length) }), "url", "$.presentation.documentationUrl");
    }

    [Test]
    public void Presentation_icon_must_be_a_listed_image_asset_with_a_matching_digest()
    {
        AppManifest Icon(string path, string digest) => WithPresentation(p => p with { Icon = new() { Path = path, Digest = digest } });

        AssertError(Icon("../icon.svg", Sha("svg")), "path", "$.presentation.icon.path");
        AssertError(Icon("img/icon.gif", Sha("svg")), "media-type", "$.presentation.icon.path");
        AssertError(Icon("img/icon.svg", Sha("svg").ToUpperInvariant()), "digest", "$.presentation.icon.digest");
        AssertError(Icon("img/other.svg", Sha("svg")), "reference", "$.presentation.icon.path");
        AssertError(Icon("img/icon.svg", Sha("other")), "digest", "$.presentation.icon.digest");
        AssertError(Icon(null!, Sha("svg")), "path", "$.presentation.icon.path");

        var manifest = UiManifest;
        var mislabelled = manifest.Ui!.Assets.Select(a => a.Path == "img/icon.svg" ? a with { MediaType = "text/css" } : a).ToArray();
        AssertError(manifest with { Ui = WithAssets(manifest.Ui, mislabelled) }, "media-type", "$.presentation.icon.path");
    }

    [TestCase("icon.png", "image/png")]
    [TestCase("a/b/icon.webp", "image/webp")]
    public void Presentation_icon_accepts_png_and_webp(string path, string mediaType)
    {
        var manifest = UiManifest;
        var assets = manifest.Ui!.Assets.Append(new AppUiAsset { Path = path, MediaType = mediaType, Digest = Sha(path) }).ToArray();
        AssertValid(manifest with
        {
            Ui = WithAssets(manifest.Ui, assets),
            Presentation = manifest.Presentation! with { Icon = new() { Path = path, Digest = Sha(path) } },
        });
    }

    [Test]
    public void Presentation_icon_without_a_ui_section_is_checked_for_shape_only()
    {
        var manifest = UiManifest with { Ui = null };
        AssertValid(manifest);
        AssertValid(manifest with { Presentation = manifest.Presentation! with { Icon = new() { Path = "anything.png", Digest = Sha("x") } } });
        AssertError(manifest with { Presentation = manifest.Presentation! with { Icon = new() { Path = "icon.jpg", Digest = Sha("x") } } },
            "media-type", "$.presentation.icon.path");
    }
}
