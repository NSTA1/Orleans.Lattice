using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Json.Schema;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    internal static string Sha(string content) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(content)));

    // Computed independently of AppUiBundle so the validator's recomputation is checked against the published formula.
    internal static string BundleDigestOf(IEnumerable<(string Path, string Digest)> assets) =>
        Sha(string.Concat(assets.OrderBy(a => a.Path, StringComparer.Ordinal).Select(a => $"{a.Path}\0{a.Digest}\n")));

    private static string BundleDigestOf(IEnumerable<AppUiAsset> assets) => BundleDigestOf(assets.Select(a => (a.Path, a.Digest)));

    internal static JsonObject UiJson()
    {
        var root = JsonNode.Parse(Json)!.AsObject();
        root["presentation"] = JsonNode.Parse($$"""
            {
              "displayName": "Tiny App",
              "summary": "A tiny app for tests.",
              "description": "Line one.\nLine two.",
              "icon": { "path": "img/icon.svg", "digest": "{{Sha("svg")}}" },
              "categories": ["data", "developer-tools"],
              "documentationUrl": "https://example.com/docs/tiny?x=1#top",
              "publisherDisplayName": "Example Publisher"
            }
            """);
        root["ui"] = JsonNode.Parse($$"""
            {
              "entry": "index.html",
              "styles": ["app.css"],
              "scripts": [{ "path": "app.js", "module": true }, { "path": "legacy.js" }],
              "assets": [
                { "path": "index.html", "mediaType": "text/html", "digest": "{{Sha("html")}}" },
                { "path": "app.css", "mediaType": "text/css", "digest": "{{Sha("css")}}" },
                { "path": "app.js", "mediaType": "text/javascript", "digest": "{{Sha("js")}}" },
                { "path": "legacy.js", "mediaType": "text/javascript", "digest": "{{Sha("legacy")}}" },
                { "path": "img/icon.svg", "mediaType": "image/svg+xml", "digest": "{{Sha("svg")}}" },
                { "path": "fonts/recursive.woff2", "mediaType": "font/woff2", "digest": "{{Sha("font")}}" },
                { "path": "data/seed.json", "mediaType": "application/json", "digest": "{{Sha("json")}}" }
              ],
              "bridge": [
                { "operation": "context.read" },
                { "operation": "data.read" },
                { "operation": "data.write", "trees": ["records"] }
              ],
              "minProtocol": 1
            }
            """);
        return Seal(root);
    }

    internal static JsonObject Seal(JsonObject root)
    {
        var assets = root["ui"]!["assets"]!.AsArray()
            .Select(a => (a!["path"]!.GetValue<string>(), a["digest"]!.GetValue<string>()));
        root["ui"]!["bundleDigest"] = BundleDigestOf(assets);
        return root;
    }

    internal static AppManifest UiManifest
    {
        get
        {
            var result = AppManifestParser.Parse(UiJson().ToJsonString());
            Assert.That(result.IsValid, Is.True, () => string.Join("; ", result.Errors));
            return result.Manifest!;
        }
    }

    private static AppUiDeclaration WithAssets(AppUiDeclaration ui, AppUiAsset[] assets) =>
        ui with { Assets = assets, BundleDigest = BundleDigestOf(assets) };

    private static void AssertError(AppManifest manifest, string code, string path)
    {
        var errors = AppManifestValidator.Validate(manifest).Errors;
        Assert.That(errors.Any(e => e.Code == code && e.Path == path), Is.True,
            () => $"Expected {code} at {path}; got: {string.Join("; ", errors)}");
    }

    private static void AssertValid(AppManifest manifest)
    {
        var result = AppManifestValidator.Validate(manifest);
        Assert.That(result.IsValid, Is.True, () => string.Join("; ", result.Errors));
    }

    [Test]
    public void Parse_manifest_with_presentation_and_ui_reads_every_member_and_satisfies_the_schema()
    {
        var json = UiJson();
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(json).IsValid, Is.True);
        var manifest = UiManifest;

        var presentation = manifest.Presentation!;
        Assert.That(presentation.DisplayName, Is.EqualTo("Tiny App"));
        Assert.That(presentation.Summary, Is.EqualTo("A tiny app for tests."));
        Assert.That(presentation.Description, Is.EqualTo("Line one.\nLine two."));
        Assert.That(presentation.Icon, Is.EqualTo(new AppIconReference { Path = "img/icon.svg", Digest = Sha("svg") }));
        Assert.That(presentation.Categories, Is.EqualTo(new[] { "data", "developer-tools" }));
        Assert.That(presentation.DocumentationUrl, Is.EqualTo("https://example.com/docs/tiny?x=1#top"));
        Assert.That(presentation.PublisherDisplayName, Is.EqualTo("Example Publisher"));

        var ui = manifest.Ui!;
        Assert.That(ui.Entry, Is.EqualTo("index.html"));
        Assert.That(ui.Styles, Is.EqualTo(new[] { "app.css" }));
        Assert.That(ui.Scripts, Is.EqualTo(new[] { new AppUiScript { Path = "app.js", Module = true }, new AppUiScript { Path = "legacy.js" } }));
        Assert.That(ui.Scripts![1].Module, Is.False, "module defaults to a classic script");
        Assert.That(ui.Assets, Has.Length.EqualTo(7));
        Assert.That(ui.Assets[0], Is.EqualTo(new AppUiAsset { Path = "index.html", MediaType = "text/html", Digest = Sha("html") }));
        Assert.That(ui.BundleDigest, Is.EqualTo(AppUiBundle.ComputeBundleDigest(ui.Assets)));
        Assert.That(ui.Bridge!.Select(b => b.Operation), Is.EqualTo(new[] { "context.read", "data.read", "data.write" }));
        Assert.That(ui.Bridge![0].Trees, Is.Null);
        Assert.That(ui.Bridge![2].Trees, Is.EqualTo(new[] { "records" }));
        Assert.That(ui.MinProtocol, Is.EqualTo(1));
    }

    [Test]
    public void Every_tracked_manifest_in_the_repository_stays_valid_and_declares_no_ui_or_presentation()
    {
        var root = HygieneRepository.FindRepoRoot();
        var manifests = HygieneRepository.TrackedFiles(root)
            .Where(f => f.EndsWith(".app.json", StringComparison.OrdinalIgnoreCase) || f.EndsWith("tiny-app.json", StringComparison.OrdinalIgnoreCase))
            .ToArray();
        Assert.That(manifests.Select(Path.GetFileName), Does.Contain("repo-context.app.json").And.Contain("tiny-app.json"), "the scan must not go vacuous");
        foreach (var file in manifests)
        {
            var result = AppManifestParser.Parse(File.ReadAllText(file));
            Assert.That(result.IsValid, Is.True, () => $"{file}: {string.Join("; ", result.Errors)}");
            var node = JsonNode.Parse(File.ReadAllText(file))!.AsObject();
            if (!node.ContainsKey("ui") && !node.ContainsKey("presentation"))
            {
                Assert.That(result.Manifest!.Presentation, Is.Null, file);
                Assert.That(result.Manifest.Ui, Is.Null, file);
            }
            Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(node).IsValid, Is.True, file);
        }
    }

    [Test]
    public void Parse_null_presentation_and_ui_are_absent()
    {
        var root = UiJson();
        root["presentation"] = null;
        root["ui"] = null;
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.True, () => string.Join("; ", result.Errors));
        Assert.That(result.Manifest!.Presentation, Is.Null);
        Assert.That(result.Manifest.Ui, Is.Null);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.True);
    }

    [TestCase("presentation")]
    [TestCase("presentation.icon")]
    [TestCase("ui")]
    [TestCase("ui.assets.0")]
    [TestCase("ui.scripts.0")]
    [TestCase("ui.bridge.0")]
    public void Parse_rejects_unknown_members_in_the_new_sections(string at)
    {
        var root = UiJson();
        JsonNode node = root;
        foreach (var segment in at.Split('.'))
            node = int.TryParse(segment, out var index) ? node[index]! : node[segment]!;
        node["html"] = "<b>x</b>";
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors[0].Code, Is.EqualTo("json"));
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [TestCase("presentation", "displayName")]
    [TestCase("ui", "entry")]
    [TestCase("ui", "assets")]
    [TestCase("ui", "bundleDigest")]
    [TestCase("ui.assets.0", "mediaType")]
    [TestCase("ui.assets.0", "digest")]
    [TestCase("ui.bridge.0", "operation")]
    [TestCase("presentation.icon", "digest")]
    public void Parse_rejects_missing_required_members_in_the_new_sections(string at, string member)
    {
        var root = UiJson();
        JsonNode node = root;
        foreach (var segment in at.Split('.'))
            node = int.TryParse(segment, out var index) ? node[index]! : node[segment]!;
        node.AsObject().Remove(member);
        Assert.That(AppManifestParser.Parse(root.ToJsonString()).IsValid, Is.False);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [Test]
    public void Manifest_with_ui_Orleans_roundtrip_preserves_the_new_sections()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppManifest).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var source = UiManifest;
        var copy = serializer.Deserialize<AppManifest>(serializer.SerializeToArray(source));
        AssertValid(copy);
        Assert.That(copy.Presentation!.DisplayName, Is.EqualTo(source.Presentation!.DisplayName));
        Assert.That(copy.Presentation.Icon, Is.EqualTo(source.Presentation.Icon));
        Assert.That(copy.Presentation.Categories, Is.EqualTo(source.Presentation.Categories));
        Assert.That(copy.Ui!.Assets, Is.EqualTo(source.Ui!.Assets));
        Assert.That(copy.Ui.Scripts, Is.EqualTo(source.Ui.Scripts));
        Assert.That(copy.Ui.Styles, Is.EqualTo(source.Ui.Styles));
        Assert.That(copy.Ui.BundleDigest, Is.EqualTo(source.Ui.BundleDigest));
        Assert.That(AppUiBridgeRequest.FromManifest(copy), Is.EqualTo(AppUiBridgeRequest.FromManifest(source)));
    }

    public static IEnumerable<TestCaseData> SchemaAndParserRejections()
    {
        static TestCaseData Case(string name, Action<JsonObject> mutate) => new TestCaseData(mutate).SetName($"Schema_and_parser_reject_{name}");
        yield return Case("an_unknown_bridge_operation", r => r["ui"]!["bridge"]![0]!["operation"] = "data.admin");
        yield return Case("trees_on_a_non_data_operation", r => r["ui"]!["bridge"]![0]!["trees"] = new JsonArray("records"));
        yield return Case("an_empty_tree_list", r => r["ui"]!["bridge"]![2]!["trees"] = new JsonArray());
        yield return Case("a_disallowed_media_type", r => { r["ui"]!["assets"]![6]!["mediaType"] = "text/plain"; Seal(r); });
        yield return Case("an_upper_case_digest", r => { r["ui"]!["assets"]![6]!["digest"] = Sha("json").ToUpperInvariant(); Seal(r); });
        yield return Case("a_traversal_path", r => { r["ui"]!["assets"]![6]!["path"] = "../seed.json"; Seal(r); });
        yield return Case("an_absolute_entry", r => r["ui"]!["entry"] = "/index.html");
        yield return Case("a_backslash_path", r => r["ui"]!["styles"]![0] = "css\\app.css");
        yield return Case("a_future_protocol", r => r["ui"]!["minProtocol"] = AppUiProtocol.Current + 1);
        yield return Case("a_zero_protocol", r => r["ui"]!["minProtocol"] = 0);
        yield return Case("a_long_display_name", r => r["presentation"]!["displayName"] = new string('x', 61));
        yield return Case("a_multi_line_summary", r => r["presentation"]!["summary"] = "a\nb");
        yield return Case("an_http_documentation_url", r => r["presentation"]!["documentationUrl"] = "http://example.com");
        yield return Case("six_categories", r => r["presentation"]!["categories"] = new JsonArray("aa", "bb", "cc", "dd", "ee", "ff"));
        yield return Case("an_upper_case_category", r => r["presentation"]!["categories"]![0] = "Data");
        yield return Case("a_gif_icon", r => r["presentation"]!["icon"]!["path"] = "img/icon.gif");
        yield return Case("a_blank_display_name", r => r["presentation"]!["displayName"] = "  ");
    }

    [TestCaseSource(nameof(SchemaAndParserRejections))]
    public void Schema_and_parser_reject(Action<JsonObject> mutate)
    {
        var root = UiJson();
        mutate(root);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False, "schema");
        Assert.That(AppManifestParser.Parse(root.ToJsonString()).IsValid, Is.False, "parser");
    }
}
