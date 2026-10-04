using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Json.Schema;
using YamlDotNet.RepresentationModel;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Format checks over <c>docs/agents</c>: every artifact kind has a JSON Schema
/// (draft 2020-12) under <c>schemas/</c>, every schema is itself a valid draft
/// 2020-12 document whose <c>schema</c> constant matches the kind it defines, and
/// every artifact - JSON and YAML alike - validates against the schema of its
/// kind. The schemas are what an agent validates a specification with, so a
/// specification that drifted from its published shape fails here.
/// </summary>
public sealed partial class AgentDocsConsistencyTests
{
    private const string SchemaArtifactKind = "json-schema";

    private const string SchemaArtifactSchema = "lattice.agents/json-schema/v1";

    private static readonly Regex YamlInteger = new(@"^-?[0-9]+$", RegexOptions.Compiled);

    private static readonly Regex YamlFloat = new(@"^-?[0-9]+\.[0-9]+([eE][-+]?[0-9]+)?$", RegexOptions.Compiled);

    [Test]
    public void Every_artifact_kind_has_a_format_schema()
    {
        var manifest = ReadManifest();
        var kinds = manifest.Where(a => a.Kind != SchemaArtifactKind).Select(a => a.Kind).Append("index").Distinct().ToList();

        Assert.Multiple(() =>
        {
            foreach (var kind in kinds)
            {
                var relative = $"schemas/{kind}.schema.json";
                Assert.That(manifest.Any(a => a.Path == relative && a.Kind == SchemaArtifactKind && a.Schema == SchemaArtifactSchema), Is.True,
                    $"kind '{kind}' has no {relative} listed in index.json with kind '{SchemaArtifactKind}'.");
            }

            foreach (var artifact in manifest.Where(a => a.Kind == SchemaArtifactKind))
            {
                Assert.That(artifact.Path, Does.Match(@"^schemas/[a-z0-9-]+\.schema\.json$"), $"{artifact.Path}: schemas live at schemas/<kind>.schema.json.");
                var kind = Path.GetFileName(artifact.Path)[..^".schema.json".Length];
                Assert.That(kinds, Does.Contain(kind), $"{artifact.Path} defines a kind no artifact uses.");
            }
        });
    }

    [Test]
    public void Every_format_schema_is_valid_draft_2020_12_for_its_kind()
    {
        var schemas = ReadManifest().Where(a => a.Kind == SchemaArtifactKind).ToList();
        Assert.That(schemas, Is.Not.Empty, "index.json lists no format schemas, so nothing was examined.");

        Assert.Multiple(() =>
        {
            foreach (var artifact in schemas)
            {
                var node = JsonNode.Parse(ReadAgentText(artifact.Path))!;
                var kind = Path.GetFileName(artifact.Path)[..^".schema.json".Length];
                Assert.That(MetaSchemas.Draft202012.Evaluate(node).IsValid, Is.True, $"{artifact.Path} is not a valid draft 2020-12 schema.");
                Assert.That(node["$schema"]?.GetValue<string>(), Is.EqualTo("https://json-schema.org/draft/2020-12/schema"), $"{artifact.Path}: $schema.");
                Assert.That(node["$id"]?.GetValue<string>(), Does.EndWith($"/docs/agents/{artifact.Path}"), $"{artifact.Path}: $id must name its own published path.");
                Assert.That(node["properties"]?["schema"]?["const"]?.GetValue<string>(), Is.EqualTo($"lattice.agents/{kind}/v1"),
                    $"{artifact.Path}: properties.schema.const must be the schema id of the kind it defines.");
            }
        });
    }

    [Test]
    public void Every_artifact_validates_against_the_format_schema_of_its_kind()
    {
        var manifest = ReadManifest();
        var targets = manifest.Where(a => a.Kind != SchemaArtifactKind).Select(a => (a.Path, a.Kind)).Append(("index.json", "index")).ToList();
        var schemas = new Dictionary<string, JsonSchema>(StringComparer.Ordinal);
        var failures = new List<string>();

        foreach (var (relative, kind) in targets)
        {
            var schemaPath = $"schemas/{kind}.schema.json";
            if (!File.Exists(Path.Combine(AgentsRoot, schemaPath)))
            {
                failures.Add($"{relative}: no {schemaPath}.");
                continue;
            }

            if (!schemas.TryGetValue(kind, out var schema))
            {
                schema = JsonSchema.FromText(ReadAgentText(schemaPath));
                schemas[kind] = schema;
            }

            failures.AddRange(Validate(schema, relative, ReadArtifactNode(relative)));
        }

        Assert.That(targets.Count, Is.GreaterThan(1), "No artifacts were validated.");
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures.Take(50)));
    }

    [Test]
    public void Format_schemas_reject_a_malformed_artifact()
    {
        // Proves the validation above can fail: a rollback step written as a bare
        // string instead of {action, api, source} must be refused.
        var schema = JsonSchema.FromText(ReadAgentText("schemas/procedure.schema.json"));
        var procedure = ReadArtifactNode("procedures/reshard.yaml")!.AsObject();
        Assert.That(Validate(schema, "procedures/reshard.yaml", procedure), Is.Empty, "The unmodified procedure must validate.");

        procedure["rollback"]!["steps"] = new JsonArray(JsonValue.Create("ILatticeTreeAdmin.ReshardTreeAsync"));
        Assert.That(Validate(schema, "probe", procedure), Is.Not.Empty, "A bare-string rollback step validated, so the schema checks nothing.");
    }

    private static IEnumerable<string> Validate(JsonSchema schema, string relative, JsonNode? node)
    {
        var results = schema.Evaluate(node, new EvaluationOptions { OutputFormat = OutputFormat.List });
        if (results.IsValid)
        {
            return Array.Empty<string>();
        }

        var messages = (results.Details ?? Array.Empty<EvaluationResults>())
            .Where(d => d.HasErrors && d.Errors is not null)
            .SelectMany(d => d.Errors!.Select(e => $"{relative} {d.InstanceLocation}: {e.Value}"))
            .Distinct()
            .Take(10)
            .ToList();
        return messages.Count > 0 ? messages : new[] { $"{relative}: does not validate against its format schema." };
    }

    private static JsonNode? ReadArtifactNode(string relative)
    {
        var text = ReadAgentText(relative);
        if (relative.EndsWith(".json", StringComparison.Ordinal))
        {
            return JsonNode.Parse(text);
        }

        var stream = new YamlStream();
        stream.Load(new StringReader(text));
        Assert.That(stream.Documents, Has.Count.EqualTo(1), $"{relative}: expected exactly one YAML document.");
        return ToJson(stream.Documents[0].RootNode);
    }

    // YAML 1.2 core-schema typing for plain scalars; a quoted scalar is always a string.
    private static JsonNode? ToJson(YamlNode node)
    {
        switch (node)
        {
            case YamlMappingNode mapping:
                var obj = new JsonObject();
                foreach (var (key, value) in mapping.Children)
                {
                    obj[((YamlScalarNode)key).Value!] = ToJson(value);
                }

                return obj;
            case YamlSequenceNode sequence:
                var array = new JsonArray();
                foreach (var item in sequence.Children)
                {
                    array.Add(ToJson(item));
                }

                return array;
            case YamlScalarNode scalar:
                var text = scalar.Value ?? string.Empty;
                if (scalar.Style != YamlDotNet.Core.ScalarStyle.Plain)
                {
                    return JsonValue.Create(text);
                }

                return text switch
                {
                    "" or "~" or "null" => null,
                    "true" => JsonValue.Create(true),
                    "false" => JsonValue.Create(false),
                    _ when YamlInteger.IsMatch(text) => JsonValue.Create(long.Parse(text, System.Globalization.CultureInfo.InvariantCulture)),
                    _ when YamlFloat.IsMatch(text) => JsonValue.Create(double.Parse(text, System.Globalization.CultureInfo.InvariantCulture)),
                    _ => JsonValue.Create(text),
                };
            default:
                Assert.Fail($"Unsupported YAML node {node.NodeType}.");
                return null;
        }
    }
}
