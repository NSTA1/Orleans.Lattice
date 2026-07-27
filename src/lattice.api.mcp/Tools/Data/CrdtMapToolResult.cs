namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the OR-Map read tool: each live field mapped to its
/// current concurrent value bytes (one normally, more than one only while a
/// field's concurrent writes are unresolved), each value base64-encoded in the
/// tool's JSON structured content. Tombstoned and absent fields are omitted; an
/// absent or unreadable key yields an empty map.
/// </summary>
public sealed record CrdtMapToolResult
{
    /// <summary>Logical tree the map lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The map key.</summary>
    public required string Key { get; init; }

    /// <summary>Each live field mapped to its current concurrent value bytes.</summary>
    public required IReadOnlyDictionary<string, IReadOnlyList<byte[]>> Fields { get; init; }
}
