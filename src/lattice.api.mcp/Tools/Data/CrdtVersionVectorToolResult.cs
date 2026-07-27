namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the version-vector read tool: each writer's current
/// clock, keyed by replica id. A clock is formatted <c>"wallClockTicks:counter"</c>.
/// An absent or unreadable key yields an empty map.
/// </summary>
public sealed record CrdtVersionVectorToolResult
{
    /// <summary>Logical tree the version vector lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The version-vector key.</summary>
    public required string Key { get; init; }

    /// <summary>Per-replica clocks, each formatted <c>"wallClockTicks:counter"</c>.</summary>
    public required IReadOnlyDictionary<string, string> Entries { get; init; }
}
