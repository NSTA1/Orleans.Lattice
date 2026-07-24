using System.Text.Json;
using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The shared <see cref="JsonSerializerOptions"/> every Lattice MCP tool group
/// uses to serialize its result DTOs, so the whole tool surface emits a single,
/// self-consistent representation.
/// </summary>
/// <remarks>
/// <para>
/// The MCP SDK serializes a tool result twice - a text block and a
/// structured-content block - from the <b>same</b> options instance. The base
/// <see cref="McpJsonUtilities.DefaultOptions"/> render <c>int64</c> fields as
/// JSON numbers, which lose precision when a host re-parses the structured block
/// through an IEEE-754 double (issue #1339). Deriving from the SDK defaults keeps
/// the SDK's type-info resolver and enum-as-string behaviour intact while adding
/// <see cref="Int64JsonStringConverter"/> so every <c>int64</c> is emitted as a
/// string and both copies round-trip byte-exact.
/// </para>
/// <para>
/// The instance is built once, marked read-only, and shared by every tool
/// group's <c>McpServerToolCreateOptions.SerializerOptions</c>, so it is the
/// single narrow seam that governs MCP tool result serialization.
/// </para>
/// </remarks>
internal static class LatticeApiMcpToolSerialization
{
    /// <summary>
    /// The shared, immutable tool-result serializer options: the MCP SDK defaults
    /// plus <see cref="Int64JsonStringConverter"/>.
    /// </summary>
    public static JsonSerializerOptions Options { get; } = BuildOptions();

    private static JsonSerializerOptions BuildOptions()
    {
        var options = new JsonSerializerOptions(McpJsonUtilities.DefaultOptions);
        options.Converters.Add(new Int64JsonStringConverter());
        options.MakeReadOnly();
        return options;
    }
}
