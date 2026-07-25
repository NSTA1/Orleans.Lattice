using System.Text.Json;
using ModelContextProtocol.Protocol;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeApiMcpRegionToolSchema"/>: the helper that
/// clones a tool's protocol definition with the optional <c>region</c> property
/// added to its input schema. Proves the property is advertised, the original tool
/// is left unchanged, existing properties are preserved, a missing or non-object
/// schema is handled, and a pre-existing <c>region</c> property is not clobbered.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpRegionToolSchemaTests
{
    private static Tool ToolWithSchema(string schemaJson)
        => new()
        {
            Name = "sample",
            InputSchema = JsonSerializer.Deserialize<JsonElement>(schemaJson),
        };

    private static JsonElement PropertyOf(Tool tool, string name)
        => tool.InputSchema.GetProperty("properties").GetProperty(name);

    [Test]
    public void Adds_an_optional_region_string_property()
    {
        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(
            ToolWithSchema("""{"type":"object","properties":{}}"""));

        var region = PropertyOf(augmented, "region");
        Assert.That(region.GetProperty("type").GetString(), Is.EqualTo("string"));
    }

    [Test]
    public void Preserves_existing_properties()
    {
        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(
            ToolWithSchema("""{"type":"object","properties":{"treeId":{"type":"string"}}}"""));

        Assert.Multiple(() =>
        {
            Assert.That(PropertyOf(augmented, "treeId").GetProperty("type").GetString(), Is.EqualTo("string"));
            Assert.That(PropertyOf(augmented, "region").GetProperty("type").GetString(), Is.EqualTo("string"));
        });
    }

    [Test]
    public void Leaves_the_original_tool_unchanged()
    {
        var original = ToolWithSchema("""{"type":"object","properties":{}}""");

        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(original);

        Assert.Multiple(() =>
        {
            Assert.That(
                original.InputSchema.GetProperty("properties").TryGetProperty("region", out _),
                Is.False,
                "The source tool's schema must not be mutated.");
            Assert.That(ReferenceEquals(original, augmented), Is.False);
        });
    }

    [Test]
    public void Preserves_the_tool_name()
    {
        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(
            ToolWithSchema("""{"type":"object","properties":{}}"""));

        Assert.That(augmented.Name, Is.EqualTo("sample"));
    }

    [Test]
    public void Synthesises_an_object_schema_when_the_input_schema_has_no_properties()
    {
        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(
            ToolWithSchema("""{"type":"object"}"""));

        Assert.Multiple(() =>
        {
            Assert.That(augmented.InputSchema.GetProperty("type").GetString(), Is.EqualTo("object"));
            Assert.That(PropertyOf(augmented, "region").GetProperty("type").GetString(), Is.EqualTo("string"));
        });
    }

    [Test]
    public void Does_not_clobber_a_pre_existing_region_property()
    {
        var augmented = LatticeApiMcpRegionToolSchema.WithRegionProperty(
            ToolWithSchema("""{"type":"object","properties":{"region":{"type":"string","description":"custom"}}}"""));

        Assert.That(
            PropertyOf(augmented, "region").GetProperty("description").GetString(),
            Is.EqualTo("custom"),
            "An existing region property must be preserved, not overwritten.");
    }

    [Test]
    public void Null_tool_throws()
        => Assert.That(() => LatticeApiMcpRegionToolSchema.WithRegionProperty(null!), Throws.ArgumentNullException);
}
