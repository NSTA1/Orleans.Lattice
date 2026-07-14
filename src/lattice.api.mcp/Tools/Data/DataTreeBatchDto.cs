namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// One participating tree's slice of a cross-tree atomic write on the MCP data
/// tools: the <see cref="TreeId"/> to write into, the key / value
/// <see cref="Upserts"/>, and the <see cref="DeleteKeys"/> to commit atomically
/// on that tree. A plain input DTO for the cross-tree atomic write tool; every
/// <see cref="TreeId"/> in a batch must be distinct and non-empty.
/// </summary>
public sealed record DataTreeBatchDto
{
    /// <summary>The logical tree this slice writes into. Must be non-empty and distinct within a batch.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key / value pairs to write atomically on this tree. May be empty when the slice is delete-only.</summary>
    public IReadOnlyList<DataEntryDto> Upserts { get; init; } = Array.Empty<DataEntryDto>();

    /// <summary>The keys to delete atomically on this tree. May be empty when the slice is upsert-only.</summary>
    public IReadOnlyList<string> DeleteKeys { get; init; } = Array.Empty<string>();
}
