using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Protocol;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Builds the region-aware advertised form of a facade-backed tool: a clone of the
/// tool's protocol definition whose input schema carries the optional
/// <c>region</c> property, so every wrapped group tool advertises the same
/// region selector without any per-handler change. Computed once when the tool is
/// wrapped (session setup), never on the invocation hot path.
/// </summary>
internal static class LatticeApiMcpRegionToolSchema
{
    /// <summary>The name of the optional per-call region selector property.</summary>
    public const string RegionPropertyName = "region";

    private const string RegionDescription =
        "Optional. Target a specific region by its id (from lattice_list_regions). "
        + "Omit to use the current region, which behaves exactly as before. "
        + "Targeting an unknown or unreachable region returns a clean fault.";

    /// <summary>
    /// Returns a clone of <paramref name="tool"/> whose input schema includes the
    /// optional <c>region</c> string property. The original tool is unchanged.
    /// </summary>
    /// <param name="tool">The protocol tool to augment. Must not be <c>null</c>.</param>
    /// <returns>A region-aware clone of the tool.</returns>
    public static Tool WithRegionProperty(Tool tool)
    {
        ArgumentNullException.ThrowIfNull(tool);

        var schema = BuildSchema(tool.InputSchema);
        return new Tool
        {
            Name = tool.Name,
            Title = tool.Title,
            Description = tool.Description,
            InputSchema = schema,
            OutputSchema = tool.OutputSchema,
            Annotations = tool.Annotations,
            Icons = tool.Icons,
            Meta = tool.Meta,
        };
    }

    private static JsonElement BuildSchema(JsonElement inputSchema)
    {
        var root = inputSchema.ValueKind == JsonValueKind.Object
            ? JsonObject.Create(inputSchema)!
            : new JsonObject { ["type"] = "object" };

        root["type"] ??= "object";

        if (root["properties"] is not JsonObject properties)
        {
            properties = new JsonObject();
            root["properties"] = properties;
        }

        if (!properties.ContainsKey(RegionPropertyName))
        {
            properties[RegionPropertyName] = new JsonObject
            {
                ["type"] = "string",
                ["description"] = RegionDescription,
            };
        }

        return JsonSerializer.SerializeToElement(root);
    }
}
