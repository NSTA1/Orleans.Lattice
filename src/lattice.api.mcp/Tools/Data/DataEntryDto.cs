namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A plain key / value entry used as both a returned entry of a bounded range
/// read and an upsert leg of a write batch on the MCP data tools. The value is
/// the full opaque byte payload, carried as a base64 string in the tool's JSON
/// structured content. Deliberately free of Orleans serialization attributes:
/// the MCP SDK serializes it with <c>System.Text.Json</c>, not the Orleans
/// wire format.
/// </summary>
public sealed record DataEntryDto
{
    /// <summary>The entry key.</summary>
    public required string Key { get; init; }

    /// <summary>The full value bytes (base64-encoded in JSON structured content).</summary>
    public byte[] Value { get; init; } = Array.Empty<byte>();
}
