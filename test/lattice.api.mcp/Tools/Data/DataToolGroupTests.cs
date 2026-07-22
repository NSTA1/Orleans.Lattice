using System.Text.Json;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="DataToolGroup"/>, the data tool module. Proves the
/// write opt-in (writes disabled offers only the two read tools; writes enabled
/// adds the four mutating tools), that the module serves the data group, and that
/// the read tools carry <c>readOnlyHint</c> while the write tools carry
/// <c>destructiveHint</c> and are non-<c>readOnlyHint</c>. Deterministic - the
/// tools are inspected, never invoked.
/// </summary>
[TestFixture]
public sealed class DataToolGroupTests
{
    private static readonly string[] ReadToolNames = { "lattice_data_get", "lattice_data_read_range" };

    private static readonly string[] WriteToolNames =
    {
        "lattice_data_set", "lattice_data_delete", "lattice_data_set_many_atomic", "lattice_data_set_many_atomic_cross_tree",
    };

    private static HashSet<string> ToolNames(DataToolGroup group)
        => group.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal);

    private static McpServerTool Tool(DataToolGroup group, string name)
        => group.Tools.Single(t => t.ProtocolTool.Name == name);

    [Test]
    public void Group_serves_the_data_facade()
    {
        var group = new DataToolGroup(enableWrites: false);

        Assert.That(group.Group, Is.EqualTo(LatticeApiMcpGroup.Data));
    }

    [Test]
    public void Writes_disabled_offers_only_the_two_read_tools()
    {
        var group = new DataToolGroup(enableWrites: false);

        Assert.Multiple(() =>
        {
            Assert.That(group.Tools, Has.Count.EqualTo(2));
            Assert.That(ToolNames(group), Is.EquivalentTo(ReadToolNames));
        });
    }

    [Test]
    public void Writes_enabled_offers_the_read_and_write_tools()
    {
        var group = new DataToolGroup(enableWrites: true);

        Assert.Multiple(() =>
        {
            Assert.That(group.Tools, Has.Count.EqualTo(6));
            Assert.That(ToolNames(group), Is.EquivalentTo(ReadToolNames.Concat(WriteToolNames)));
        });
    }

    [Test]
    public void Tool_names_are_unique()
    {
        var group = new DataToolGroup(enableWrites: true);

        Assert.That(ToolNames(group), Has.Count.EqualTo(group.Tools.Count));
    }

    [Test]
    public void Read_tools_carry_the_read_only_annotation()
    {
        var group = new DataToolGroup(enableWrites: true);

        Assert.Multiple(() =>
        {
            foreach (var name in ReadToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.ReadOnlyHint, Is.True, $"{name} must be read-only.");
                Assert.That(annotations?.DestructiveHint, Is.False, $"{name} must not be destructive.");
            }
        });
    }

    [Test]
    public void Write_tools_carry_the_destructive_annotation_and_are_not_read_only()
    {
        var group = new DataToolGroup(enableWrites: true);

        Assert.Multiple(() =>
        {
            foreach (var name in WriteToolNames)
            {
                var annotations = Tool(group, name).ProtocolTool.Annotations;
                Assert.That(annotations?.DestructiveHint, Is.True, $"{name} must be destructive.");
                Assert.That(annotations?.ReadOnlyHint, Is.False, $"{name} must not be read-only.");
            }
        });
    }

    [Test]
    public void Read_range_marks_only_the_tree_id_required()
    {
        var group = new DataToolGroup(enableWrites: false);

        var required = RequiredPropertyNames(Tool(group, "lattice_data_read_range"));

        Assert.That(required, Is.EquivalentTo(new[] { "treeId" }),
            "The optional range bounds, page size, and continuation token must not be schema-required.");
    }

    [Test]
    public void Read_range_still_advertises_all_input_properties()
    {
        var group = new DataToolGroup(enableWrites: false);

        var properties = PropertyNames(Tool(group, "lattice_data_read_range"));

        Assert.That(
            properties,
            Is.SupersetOf(new[] { "treeId", "startInclusive", "endExclusive", "pageSize", "continuationToken" }),
            "Making the bounds optional must not drop them from the advertised schema.");
    }

    [Test]
    public void Data_get_still_requires_its_key_inputs()
    {
        var group = new DataToolGroup(enableWrites: false);

        var required = RequiredPropertyNames(Tool(group, "lattice_data_get"));

        Assert.That(required, Is.EquivalentTo(new[] { "treeId", "key" }),
            "The genuinely-required point-read inputs must stay required.");
    }

    private static HashSet<string> RequiredPropertyNames(McpServerTool tool)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var schema = tool.ProtocolTool.InputSchema;
        if (schema.ValueKind == JsonValueKind.Object
            && schema.TryGetProperty("required", out var required)
            && required.ValueKind == JsonValueKind.Array)
        {
            foreach (var entry in required.EnumerateArray())
            {
                if (entry.ValueKind == JsonValueKind.String)
                {
                    names.Add(entry.GetString()!);
                }
            }
        }

        return names;
    }

    private static HashSet<string> PropertyNames(McpServerTool tool)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var schema = tool.ProtocolTool.InputSchema;
        if (schema.ValueKind == JsonValueKind.Object
            && schema.TryGetProperty("properties", out var props)
            && props.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in props.EnumerateObject())
            {
                names.Add(property.Name);
            }
        }

        return names;
    }
}
