namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    private static AppManifest WithUi(Func<AppUiDeclaration, AppUiDeclaration> change)
    {
        var manifest = UiManifest;
        return manifest with { Ui = change(manifest.Ui!) };
    }

    private static AppUiAsset UiAsset(string path, string mediaType = "application/json") =>
        new() { Path = path, MediaType = mediaType, Digest = Sha(path) };

    [Test]
    public void Ui_assets_are_required_and_non_empty()
    {
        AssertError(WithUi(u => u with { Assets = null! }), "required", "$.ui.assets");
        AssertError(WithUi(u => WithAssets(u, [])), "required", "$.ui.assets");
        AssertError(WithUi(u => u with { Assets = [.. u.Assets, null!] }), "required", $"$.ui.assets[{UiManifest.Ui!.Assets.Length}]");
    }

    [Test]
    public void Ui_assets_are_capped_at_256()
    {
        AppUiDeclaration Fill(AppUiDeclaration ui, int total) =>
            WithAssets(ui, [.. ui.Assets, .. Enumerable.Range(0, total - ui.Assets.Length).Select(i => UiAsset($"extra/{i}.json"))]);

        AssertValid(WithUi(u => Fill(u, AppUiBundle.MaxAssets)));
        var over = WithUi(u => Fill(u, AppUiBundle.MaxAssets + 1));
        AssertError(over, "limit", "$.ui.assets");
        Assert.That(AppManifestValidator.Validate(over).Errors.Any(e => e.Path.StartsWith("$.ui.assets[", StringComparison.Ordinal)), Is.False,
            "an oversized asset list is rejected without per-entry work");
    }

    [TestCase("/abs.json")]
    [TestCase("../up.json")]
    [TestCase("a/../b.json")]
    [TestCase("a\\b.json")]
    [TestCase("Upper.json")]
    [TestCase("q.json?x")]
    [TestCase("f.json#x")]
    [TestCase("a//b.json")]
    public void Ui_asset_paths_must_be_normalised_and_relative(string path)
    {
        AssertError(WithUi(u => WithAssets(u, [.. u.Assets, UiAsset(path)])), "path", $"$.ui.assets[{UiManifest.Ui!.Assets.Length}].path");
    }

    [Test]
    public void Ui_asset_paths_are_unique()
    {
        AssertError(WithUi(u => WithAssets(u, [.. u.Assets, UiAsset("index.html", "text/html")])), "duplicate", $"$.ui.assets[{UiManifest.Ui!.Assets.Length}].path");
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("text/plain")]
    [TestCase("TEXT/HTML")]
    [TestCase("text/html; charset=utf-8")]
    [TestCase("image/gif")]
    [TestCase("application/javascript")]
    [TestCase("application/octet-stream")]
    public void Ui_asset_media_types_come_from_the_allow_list(string? mediaType)
    {
        AssertError(WithUi(u => WithAssets(u, [.. u.Assets, UiAsset("x.bin", mediaType!)])), "media-type", $"$.ui.assets[{UiManifest.Ui!.Assets.Length}].mediaType");
    }

    [Test]
    public void Ui_asset_every_allowed_media_type_is_accepted()
    {
        AssertValid(WithUi(u => WithAssets(u, [.. u.Assets, .. AppUiBundle.AllowedMediaTypes.Select((m, i) => UiAsset($"all/{i}", m))])));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("abc")]
    [TestCase("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855")]
    [TestCase("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85")]
    [TestCase("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855a")]
    [TestCase("g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
    public void Ui_digests_must_be_lower_case_sha256_hex(string? digest)
    {
        var manifest = UiManifest;
        var index = manifest.Ui!.Assets.Length;
        var asset = UiAsset("bad.json") with { Digest = digest! };
        var withBadAsset = manifest with { Ui = manifest.Ui with { Assets = [.. manifest.Ui.Assets, asset] } };
        AssertError(withBadAsset, "digest", $"$.ui.assets[{index}].digest");
        Assert.That(AppManifestValidator.Validate(withBadAsset).Errors.Any(e => e.Code == "bundle-digest"), Is.False,
            "the bundle digest is only recomputed over well-formed assets");
        AssertError(manifest with { Ui = manifest.Ui with { BundleDigest = digest! } }, "digest", "$.ui.bundleDigest");
    }

    [Test]
    public void Ui_bundle_digest_is_recomputed_and_a_mismatch_is_rejected()
    {
        var manifest = UiManifest;
        AssertError(manifest with { Ui = manifest.Ui! with { BundleDigest = Sha("wrong") } }, "bundle-digest", "$.ui.bundleDigest");

        var tampered = manifest.Ui!.Assets.Select(a => a.Path == "app.js" ? a with { Digest = Sha("evil") } : a).ToArray();
        AssertError(manifest with { Ui = manifest.Ui with { Assets = tampered } }, "bundle-digest", "$.ui.bundleDigest");

        var renamed = manifest.Ui.Assets.Select(a => a.Path == "data/seed.json" ? a with { Path = "data/seed2.json" } : a).ToArray();
        AssertError(manifest with { Ui = manifest.Ui with { Assets = renamed } }, "bundle-digest", "$.ui.bundleDigest");

        AssertValid(manifest with { Ui = manifest.Ui with { Assets = manifest.Ui.Assets.Reverse().ToArray() } });
        AssertValid(manifest with
        {
            Ui = manifest.Ui with { Assets = manifest.Ui.Assets.Select(a => a.Path == "data/seed.json" ? a with { MediaType = "text/css" } : a).ToArray() },
        });
    }

    [Test]
    public void Ui_entry_must_be_a_listed_html_asset()
    {
        AssertError(WithUi(u => u with { Entry = null! }), "required", "$.ui.entry");
        AssertError(WithUi(u => u with { Entry = "" }), "required", "$.ui.entry");
        AssertError(WithUi(u => u with { Entry = "/index.html" }), "path", "$.ui.entry");
        AssertError(WithUi(u => u with { Entry = "missing.html" }), "reference", "$.ui.entry");
        AssertError(WithUi(u => u with { Entry = "app.css" }), "media-type", "$.ui.entry");
    }

    [Test]
    public void Ui_styles_must_be_unique_listed_css_assets()
    {
        AssertValid(WithUi(u => u with { Styles = null }));
        AssertValid(WithUi(u => u with { Styles = [] }));
        AssertError(WithUi(u => u with { Styles = ["app.css", "app.css"] }), "duplicate", "$.ui.styles[1]");
        AssertError(WithUi(u => u with { Styles = ["missing.css"] }), "reference", "$.ui.styles[0]");
        AssertError(WithUi(u => u with { Styles = ["app.js"] }), "media-type", "$.ui.styles[0]");
        AssertError(WithUi(u => u with { Styles = ["..\\app.css"] }), "path", "$.ui.styles[0]");
        AssertError(WithUi(u => u with { Styles = [null!] }), "required", "$.ui.styles[0]");
        AssertError(WithUi(u => u with { Styles = Enumerable.Repeat("app.css", AppUiBundle.MaxAssets + 1).ToArray() }), "limit", "$.ui.styles");
    }

    [Test]
    public void Ui_scripts_must_be_unique_listed_javascript_assets()
    {
        AssertValid(WithUi(u => u with { Scripts = null }));
        AssertValid(WithUi(u => u with { Scripts = [] }));
        AssertError(WithUi(u => u with { Scripts = [new() { Path = "app.js" }, new() { Path = "app.js", Module = true }] }), "duplicate", "$.ui.scripts[1].path");
        AssertError(WithUi(u => u with { Scripts = [new() { Path = "missing.js" }] }), "reference", "$.ui.scripts[0].path");
        AssertError(WithUi(u => u with { Scripts = [new() { Path = "index.html" }] }), "media-type", "$.ui.scripts[0].path");
        AssertError(WithUi(u => u with { Scripts = [new() { Path = "https://cdn.example.com/x.js" }] }), "path", "$.ui.scripts[0].path");
        AssertError(WithUi(u => u with { Scripts = [null!] }), "required", "$.ui.scripts[0]");
        AssertError(WithUi(u => u with { Scripts = [new() { Path = null! }] }), "required", "$.ui.scripts[0].path");
        AssertError(WithUi(u => u with { Scripts = Enumerable.Repeat(new AppUiScript { Path = "app.js" }, AppUiBundle.MaxAssets + 1).ToArray() }), "limit", "$.ui.scripts");
    }

    [Test]
    public void Ui_references_are_not_resolved_when_the_asset_list_is_unusable()
    {
        var errors = AppManifestValidator.Validate(WithUi(u => u with { Assets = null! })).Errors;
        Assert.That(errors.Any(e => e.Code == "reference"), Is.False);
    }

    [TestCase(0)]
    [TestCase(-1)]
    [TestCase(AppUiProtocol.Current + 1)]
    [TestCase(int.MaxValue)]
    public void Ui_min_protocol_cannot_exceed_the_current_protocol(int minProtocol)
    {
        AssertError(WithUi(u => u with { MinProtocol = minProtocol }), "protocol", "$.ui.minProtocol");
    }

    [Test]
    public void Ui_min_protocol_defaults_to_one()
    {
        var root = UiJson();
        root["ui"]!.AsObject().Remove("minProtocol");
        Assert.That(AppManifestParser.Parse(root.ToJsonString()).Manifest!.Ui!.MinProtocol, Is.EqualTo(1));
        Assert.That(new AppUiDeclaration { Entry = "a", Assets = [], BundleDigest = "" }.MinProtocol, Is.EqualTo(1));
    }

    [Test]
    public void Ui_bridge_may_be_omitted_or_empty()
    {
        AssertValid(WithUi(u => u with { Bridge = null }));
        AssertValid(WithUi(u => u with { Bridge = [] }));
        AssertValid(WithUi(u => u with { Bridge = AppUiBridgeOperations.All.Select(o => new AppUiBridgeDeclaration { Operation = o }).ToArray() }));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("data.admin")]
    [TestCase("Data.Read")]
    [TestCase("data.read ")]
    [TestCase("app.install")]
    [TestCase("lifecycle.uninstall")]
    public void Ui_bridge_rejects_unknown_operations(string? operation)
    {
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = operation! }] }), "bridge", "$.ui.bridge[0].operation");
    }

    [Test]
    public void Ui_bridge_rejects_duplicates_null_entries_and_oversized_sections()
    {
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "nav.sync" }, new() { Operation = "nav.sync" }] }), "duplicate", "$.ui.bridge[1].operation");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = ["records"] }, new() { Operation = "data.read" }] }), "duplicate", "$.ui.bridge[1].operation");
        AssertError(WithUi(u => u with { Bridge = [null!] }), "required", "$.ui.bridge[0]");
        AssertError(WithUi(u => u with { Bridge = Enumerable.Repeat(new AppUiBridgeDeclaration { Operation = "nav.sync" }, AppManifestLimits.MaxSectionItems + 1).ToArray() }),
            "limit", "$.ui.bridge");
    }

    [Test]
    public void Ui_bridge_trees_are_only_for_data_operations_and_must_be_declared()
    {
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "context.read", Trees = ["records"] }] }), "bridge", "$.ui.bridge[0].trees");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "ui.notify", Trees = [] }] }), "bridge", "$.ui.bridge[0].trees");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = [] }] }), "bridge", "$.ui.bridge[0].trees");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.write", Trees = ["undeclared"] }] }), "reference", "$.ui.bridge[0].trees[0]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.delete", Trees = ["events"] }] }), "reference", "$.ui.bridge[0].trees[0]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = ["a/records"] }] }), "name", "$.ui.bridge[0].trees[0]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = ["*"] }] }), "name", "$.ui.bridge[0].trees[0]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = [null!] }] }), "name", "$.ui.bridge[0].trees[0]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = ["records", "records"] }] }), "duplicate", "$.ui.bridge[0].trees[1]");
        AssertError(WithUi(u => u with { Bridge = [new() { Operation = "data.read", Trees = Enumerable.Repeat("records", AppManifestLimits.MaxSectionItems + 1).ToArray() }] }),
            "limit", "$.ui.bridge[0].trees");
        foreach (var operation in new[] { "data.read", "data.write", "data.delete" })
            AssertValid(WithUi(u => u with { Bridge = [new() { Operation = operation, Trees = ["records"] }] }));
    }

    [Test]
    public void Ui_and_presentation_validation_never_throws_on_null_members()
    {
        var manifest = UiManifest with
        {
            Presentation = new AppPresentation { DisplayName = null!, Icon = new() { Path = null!, Digest = null! } },
            Ui = new AppUiDeclaration
            {
                Entry = null!, Assets = [null!, new() { Path = null!, MediaType = null!, Digest = null! }], BundleDigest = null!,
                Styles = [null!], Scripts = [null!, new() { Path = null! }], Bridge = [null!, new() { Operation = null!, Trees = [null!] }],
            },
        };
        var result = AppManifestValidator.Validate(manifest);
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors, Is.Not.Empty);
    }
}
