namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_delete_range</c> bounded range-delete tool.
/// <see cref="DeletedCount"/> is the total number of keys tombstoned across the
/// half-open range. A caller who may not delete the whole range is rejected
/// before this result is produced.
/// </summary>
public sealed record DataRangeDeleteToolResult
{
    /// <summary>Logical tree the range was deleted from.</summary>
    public required string TreeId { get; init; }

    /// <summary>Total number of keys tombstoned across the range.</summary>
    public required int DeletedCount { get; init; }
}
