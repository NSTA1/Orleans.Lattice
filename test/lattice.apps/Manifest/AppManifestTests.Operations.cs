using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppManifestTests
{
    [TestCase("null")]
    [TestCase("0")]
    [TestCase("9")]
    [TestCase("\"Read\"")]
    [TestCase("[]")]
    [TestCase("[null]")]
    [TestCase("[9]")]
    [TestCase("[\"None\"]")]
    [TestCase("[\"Unknown\"]")]
    [TestCase("[\"Read, Write\"]")]
    [TestCase("[\"read\"]")]
    [TestCase("[\"Read\", \"Read\"]")]
    [TestCase("[\"Telemetry\"]")]
    [TestCase("[\"AppInstall\"]")]
    public void Parse_operations_reject_noncanonical_or_scopeless_requests(string operations)
    {
        var root = JsonNode.Parse(Json)!;
        root["roles"]![0]!["operations"] = JsonNode.Parse(operations);
        Assert.That(AppManifestParser.Parse(root.ToJsonString()).IsValid, Is.False);
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
    }

    [Test]
    public void Parse_operations_accepts_every_explicit_tree_scoped_bit_and_preserves_mask()
    {
        var operations = Enum.GetValues<LatticeOperation>()
            .Where(o => o != LatticeOperation.None && o != LatticeOperation.Telemetry && Enum.GetName(o) != "AppInstall")
            .ToArray();
        var root = JsonNode.Parse(Json)!;
        root["roles"]![0]!["operations"] = JsonSerializer.SerializeToNode(operations.Select(o => Enum.GetName(o)));
        var result = AppManifestParser.Parse(root.ToJsonString());
        Assert.That(result.IsValid, Is.True);
        Assert.That(result.Manifest!.Roles[0].Operations, Is.EqualTo(operations.Aggregate((mask, op) => mask | op)));
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.True);
        var documented = JsonNode.Parse(AppManifestResources.GetJsonSchema())!["$defs"]!["role"]!["properties"]!["operations"]!["items"]!["enum"]!;
        Assert.That(documented.AsArray().Select(n => n!.GetValue<string>()), Is.EquivalentTo(operations.Select(o => Enum.GetName(o))));
        Assert.That(AppManifestValidator.Validate(Manifest with
        {
            Roles = [Manifest.Roles[0] with { Operations = LatticeOperation.Telemetry }],
        }).IsValid, Is.False);
    }

    [Test]
    public void Json_converters_write_the_same_canonical_identity_and_operation_shapes()
    {
        var options = new JsonSerializerOptions();
        options.Converters.Add(new AppSlugJsonConverter());
        options.Converters.Add(new AppVersionJsonConverter());
        options.Converters.Add(new AppOperationsJsonConverter());
        options.Converters.Add(new AppEnumJsonConverter<LatticeMergeMode>());
        Assert.That(JsonSerializer.Serialize(AppSlug.Parse("ab"), options), Is.EqualTo("\"ab\""));
        Assert.That(JsonSerializer.Serialize(AppVersion.Parse("1.2.3"), options), Is.EqualTo("\"1.2.3\""));
        Assert.That(JsonSerializer.Serialize(LatticeOperation.Read | LatticeOperation.Write, options), Is.EqualTo("[\"Read\",\"Write\"]"));
        Assert.That(JsonSerializer.Serialize(LatticeMergeMode.LwwRegister, options), Is.EqualTo("\"LwwRegister\""));
        Assert.Throws<JsonException>(() => JsonSerializer.Serialize(LatticeOperation.None, options));
        Assert.Throws<JsonException>(() => JsonSerializer.Serialize(LatticeOperation.Telemetry, options));
    }

    [Test]
    public void Parse_invalid_stream_and_embedded_content_return_errors()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("{"));
        Assert.That(AppManifestParser.Parse(stream).Errors.Single().Code, Is.EqualTo("json"));
        using var failed = new FailingManifestStream();
        Assert.That(AppManifestParser.Parse(failed).Errors.Single().Code, Is.EqualTo("io"));
        var result = AppManifestResources.Load(typeof(AppManifestResources).Assembly,
            "Orleans.Lattice.Apps.Manifest.app-manifest.schema.json");
        Assert.That(result.IsValid, Is.False);
        Assert.That(result.Errors.Single().Code, Is.EqualTo("json"));
    }

    [TestCase("slug", "tiny-app\n")]
    [TestCase("version", "1.2.3\n")]
    public void JsonSchema_rejects_terminal_newlines_like_runtime_identity_parsing(string field, string value)
    {
        var root = JsonNode.Parse(Json)!;
        root["identity"]![field] = value;
        Assert.That(JsonSchema.FromText(AppManifestResources.GetJsonSchema()).Evaluate(root).IsValid, Is.False);
        Assert.That(AppManifestParser.Parse(root.ToJsonString()).IsValid, Is.False);
    }

    [Test]
    public void TryParse_slug_validation_allocates_nothing()
    {
        for (var i = 0; i < 100; i++)
            AppSlug.TryParse("valid-app", out _);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            AppSlug.TryParse("valid-app", out _);
            AppSlug.TryParse("invalid_app", out _);
        }
        Assert.That(GC.GetAllocatedBytesForCurrentThread() - before, Is.Zero);
    }
}
