namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_read_range</c> bounded range-read tool: one
/// page of key-ordered <see cref="Entries"/> pruned to the caller's authorized
/// subset, and a <see cref="ContinuationToken"/> to resume paging on the next
/// call. A <see langword="null"/> token signals the range is fully drained.
/// </summary>
public sealed record DataRangePageToolResult
{
    /// <summary>Logical tree the page was read from.</summary>
    public required string TreeId { get; init; }

    /// <summary>The entries on this page, in ascending key order.</summary>
    public IReadOnlyList<DataEntryDto> Entries { get; init; } = Array.Empty<DataEntryDto>();

    /// <summary>
    /// The token to resume paging on the next request, or <see langword="null"/>
    /// when the range is fully drained.
    /// </summary>
    public string? ContinuationToken { get; init; }
}
