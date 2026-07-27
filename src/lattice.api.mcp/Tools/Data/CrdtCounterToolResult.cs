namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the PN-counter read tool: the converged total, summed
/// per replica across every increment and decrement the key has observed. An
/// absent or unreadable key reads as zero.
/// </summary>
public sealed record CrdtCounterToolResult
{
    /// <summary>Logical tree the counter lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The counter key.</summary>
    public required string Key { get; init; }

    /// <summary>The converged counter total.</summary>
    public required long Value { get; init; }
}
