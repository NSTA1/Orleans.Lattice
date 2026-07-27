namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of an OR-Flag or RW-Flag read tool: the converged boolean
/// presence bit. An absent or unreadable key reads as <see langword="false"/>.
/// </summary>
public sealed record CrdtFlagToolResult
{
    /// <summary>Logical tree the flag lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The flag key.</summary>
    public required string Key { get; init; }

    /// <summary>The converged flag state.</summary>
    public required bool Enabled { get; init; }
}
