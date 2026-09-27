using System.Text;
using System.Text.Json.Nodes;
using Json.Schema;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed partial class AppManifestTests
{
    private static string Json
    {
        get
        {
            using var stream = typeof(AppManifestTests).Assembly.GetManifestResourceStream("test.tiny-app.json")!;
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }
    }

    private static AppManifest Manifest => AppManifestParser.Parse(Json).Manifest!;

    [Test]
    public void Load_embedded_manifest_reads_all_sections_without_filesystem()
    {
        var result = AppManifestResources.Load(typeof(AppManifestTests).Assembly, "test.tiny-app.json");
        Assert.That(result.IsValid, Is.True, string.Join("; ", result.Errors));
        Assert.That(result.Errors, Is.Empty);
        var manifest = result.Manifest!;
        Assert.That(manifest.Identity.Slug.Value, Is.EqualTo("tiny-app"));
        Assert.That(manifest.Identity.Version.Value, Is.EqualTo("1.2.3-preview.1+test"));
        Assert.That(manifest.Identity.Provenance.Source, Is.EqualTo("in-image"));
        Assert.That(manifest.Identity.Provenance.Publisher, Is.EqualTo("first-party"));
        Assert.That(manifest.Identity.Provenance.Reference, Is.EqualTo("embedded:test.tiny-app.json"));
        var tree = manifest.Trees.Single();
        Assert.That(tree.Name, Is.EqualTo("records"));
        Assert.That(tree.ShardCount, Is.EqualTo(2));
        Assert.That(tree.VirtualShardCount, Is.EqualTo(16));
        Assert.That(tree.MaxLeafKeys, Is.EqualTo(128));
        Assert.That(tree.MaxInternalChildren, Is.EqualTo(32));
        Assert.That(tree.WalPartitions, Is.EqualTo(4));
        Assert.That(tree.SoftDeleteDuration, Is.EqualTo(TimeSpan.FromDays(3)));
        Assert.That(tree.Rebuildable, Is.True);
        var role = manifest.Roles.Single();
        Assert.That(role.Name, Is.EqualTo("reader"));
        Assert.That(role.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead));
        Assert.That(role.Scopes[0].Tree, Is.EqualTo("records"));
        Assert.That(role.Scopes[0].App, Is.Null);
        Assert.That(role.Scopes[0].Kind, Is.EqualTo(LatticeScopeKind.Tree));
        Assert.That(role.Scopes[0].KeyOrPrefix, Is.Null);
        Assert.That(role.Scopes[1].App, Is.EqualTo(AppSlug.Parse("other-app")));
        Assert.That(role.Scopes[1].Kind, Is.EqualTo(LatticeScopeKind.Prefix));
        Assert.That(role.Scopes[1].KeyOrPrefix, Is.EqualTo("public/"));
        Assert.That(manifest.Replication![0].Tree, Is.EqualTo("records"));
        Assert.That(manifest.Replication[0].MergeMode, Is.EqualTo(LatticeMergeMode.LwwRegister));
        Assert.That(manifest.Schema![0].Tree, Is.EqualTo("records"));
        Assert.That(manifest.Schema[0].Family, Is.EqualTo("tiny"));
        Assert.That(manifest.Schema[0].Version, Is.EqualTo(1));
        Assert.That(manifest.Schema[0].StrictIngest, Is.True);
        Assert.That(manifest.Subscriptions[0].Name, Is.EqualTo("observe"));
        Assert.That(manifest.Subscriptions[0].Tree, Is.EqualTo("events"));
        Assert.That(manifest.Subscriptions[0].App, Is.EqualTo(AppSlug.Parse("other-app")));
        Assert.That(manifest.Subscriptions[0].KeyPrefix, Is.EqualTo("public/"));
        Assert.That(manifest.McpTools[0].Name, Is.EqualTo("read_records"));
        Assert.That(manifest.McpTools[0].Description, Is.EqualTo("Reads records."));
        Assert.That(manifest.McpTools[0].Role, Is.EqualTo("reader"));
    }

    [Test]
    public void Parse_stream_leaves_caller_stream_open()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(Json));
        Assert.That(AppManifestParser.Parse(stream).IsValid, Is.True);
        Assert.That(stream.CanRead, Is.True);
        stream.Position = 0;
        Assert.That(AppManifestParser.Parse(stream).Manifest!.Identity, Is.EqualTo(Manifest.Identity));
        Assert.Throws<ArgumentNullException>(() => AppManifestParser.Parse((Stream)null!));
    }

    [Test]
    public void Load_missing_resource_returns_diagnostic_and_guards_programmer_arguments()
    {
        foreach (var name in new[] { "", "absent", "test.tiny-app.JSON" })
        {
            var result = AppManifestResources.Load(typeof(AppManifestTests).Assembly, name);
            Assert.That(result.IsValid, Is.False);
            Assert.That(result.Manifest, Is.Null);
            Assert.That(result.Errors.Single().Code, Is.EqualTo("resource"));
        }
        Assert.Throws<ArgumentNullException>(() => AppManifestResources.Load(null!, "x"));
        Assert.Throws<ArgumentNullException>(() => AppManifestResources.Load(typeof(AppManifestTests).Assembly, null!));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("null")]
    [TestCase("[]")]
    [TestCase("{}")]
    [TestCase("{")]
    [TestCase("{\"identity\": null}")]
    public void Parse_malformed_content_returns_structured_failure(string? json)
    {
        var result = AppManifestParser.Parse(json);
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Manifest, Is.Null);
        Assert.That(result.Errors, Is.Not.Empty);
        Assert.That(result.Errors[0].Code, Is.Not.Empty);
        Assert.That(result.Errors[0].Path, Does.StartWith("$"));
        Assert.That(result.Errors[0].Message, Is.Not.Empty);
    }

    [TestCase("\"slug\": \"tiny-app\"", "\"slug\": \"bad_slug\"")]
    [TestCase("\"slug\": \"tiny-app\"", "\"slug\": null")]
    [TestCase("\"slug\": \"tiny-app\"", "\"slug\": 4")]
    [TestCase("\"version\": \"1.2.3-preview.1+test\"", "\"version\": null")]
    [TestCase("\"version\": \"1.2.3-preview.1+test\"", "\"version\": \"01.2.3\"")]
    [TestCase("\"version\": \"1.2.3-preview.1+test\"", "\"version\": 5")]
    [TestCase("\"operations\": [\"Read\", \"RangeRead\"]", "\"operations\": 9")]
    [TestCase("\"kind\": \"Prefix\"", "\"kind\": \"prefix\"")]
    [TestCase("\"kind\": \"Prefix\"", "\"kind\": 2")]
    [TestCase("\"mergeMode\": \"LwwRegister\"", "\"mergeMode\": \"Unknown\"")]
    [TestCase("\"mergeMode\": \"LwwRegister\"", "\"mergeMode\": \"0\"")]
    [TestCase("\"mergeMode\": \"LwwRegister\"", "\"mergeMode\": 0")]
    [TestCase("\"rebuildable\": true", "\"rebuildable\": \"true\"")]
    [TestCase("\"shardCount\": 2", "\"shardCount\": \"2\"")]
    [TestCase("\"shardCount\": 2", "\"shardCount\": 2.5")]
    [TestCase("\"shardCount\": 2", "\"shardCount\": 2147483648")]
    [TestCase("\"softDeleteDuration\": \"3.00:00:00\"", "\"softDeleteDuration\": \"bad\"")]
    [TestCase("\"operations\": [\"Read\", \"RangeRead\"]", "\"operations\": [\"Read\"], \"inherits\": [\"admin\"]")]
    [TestCase("\"operations\": [\"Read\", \"RangeRead\"]", "\"operations\": [\"Read\"], \"operations\": [\"Write\"]")]
    [TestCase("\"trees\":", "\"Trees\":")]
    public void Parse_wrong_shapes_unknown_and_duplicate_members_fail(string oldText, string replacement)
    {
        Assert.That(Json, Does.Contain(oldText));
        var result = AppManifestParser.Parse(Json.Replace(oldText, replacement, StringComparison.Ordinal));
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors[0].Code, Is.EqualTo("json"));
    }

    [TestCase("identity")]
    [TestCase("trees")]
    [TestCase("roles")]
    [TestCase("subscriptions")]
    [TestCase("mcpTools")]
    public void Parse_required_sections_cannot_be_missing_or_null(string section)
    {
        var json = JsonNode.Parse(Json)!.AsObject();
        json.Remove(section);
        Assert.That(AppManifestParser.Parse(json.ToJsonString()).IsValid, Is.False);
        json[section] = null;
        Assert.That(AppManifestParser.Parse(json.ToJsonString()).IsValid, Is.False);
    }

    [Test]
    public void Parse_minimal_manifest_inherits_optional_defaults()
    {
        const string json = """{"identity":{"slug":"ab","version":"1.0.0"},"trees":[{"name":"data"}],"roles":[],"subscriptions":[],"mcpTools":[]}""";
        var result = AppManifestParser.Parse(json);
        Assert.That(result.IsValid, Is.True);
        Assert.That(result.Manifest!.Replication, Is.Null);
        Assert.That(result.Manifest.Schema, Is.Null);
        Assert.That(result.Manifest.Identity.Provenance, Is.EqualTo(new AppProvenance()));
        Assert.That(result.Manifest.Trees[0], Is.EqualTo(new AppTreeDeclaration { Name = "data" }));
    }

    [Test]
    public void GetJsonSchema_is_valid_draft202012_and_accepts_embedded_manifest()
    {
        var schemaText = AppManifestResources.GetJsonSchema();
        var schema = JsonSchema.FromText(schemaText);
        Assert.That(schema.Evaluate(JsonNode.Parse(Json)).IsValid, Is.True);
        Assert.That(JsonNode.Parse(schemaText)!["$schema"]!.GetValue<string>(),
            Is.EqualTo("https://json-schema.org/draft/2020-12/schema"));
        var defs = JsonNode.Parse(schemaText)!["$defs"]!;
        Assert.That(defs["replication"]!["properties"]!["mergeMode"]!["enum"]!.AsArray()
            .Select(x => x!.GetValue<string>()), Is.EquivalentTo(Enum.GetNames<LatticeMergeMode>()));
        Assert.That(defs["scope"]!["properties"]!["kind"]!["enum"]!.AsArray()
            .Select(x => x!.GetValue<string>()), Is.EquivalentTo(Enum.GetNames<LatticeScopeKind>()));
    }

    [TestCase("\"slug\": \"tiny-app\"", "\"slug\": \"bad_slug\"")]
    [TestCase("\"version\": \"1.2.3-preview.1+test\"", "\"version\": \"01.2.3\"")]
    [TestCase("\"operations\": [\"Read\", \"RangeRead\"]", "\"operations\": []")]
    [TestCase("\"operations\": [\"Read\", \"RangeRead\"]", "\"operations\": [\"Read\"], \"inherits\": []")]
    [TestCase("\"maxLeafKeys\": 128", "\"maxLeafKeys\": 1")]
    [TestCase("\"maxInternalChildren\": 32", "\"maxInternalChildren\": 2")]
    [TestCase("\"walPartitions\": 4", "\"walPartitions\": 0")]
    [TestCase("\"virtualShardCount\": 16", "\"virtualShardCount\": 0")]
    [TestCase("\"shardCount\": 2", "\"shardCount\": 4097")]
    [TestCase("\"keyOrPrefix\": \"public/\"", "\"keyOrPrefix\": null")]
    [TestCase("\"description\": \"Reads records.\"", "\"description\": \"  \"")]
    [TestCase("\"mergeMode\": \"LwwRegister\"", "\"mergeMode\": 0")]
    public void JsonSchema_and_parser_reject_invalid_structural_content(string oldText, string replacement)
    {
        var json = Json.Replace(oldText, replacement, StringComparison.Ordinal);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(JsonNode.Parse(json)).IsValid, Is.False);
        Assert.That(AppManifestParser.Parse(json).IsValid, Is.False);
    }
}
