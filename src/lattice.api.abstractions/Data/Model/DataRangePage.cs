namespace Orleans.Lattice.Api.Data;

/// <summary>
/// One page of a bounded range read: the key-ordered <see cref="Entries"/> and,
/// when more entries remain beyond this page, a
/// <see cref="ContinuationToken"/> to pass back on the next
/// <see cref="DataRangeRequest"/> to resume. A <see langword="null"/> token
/// signals the range is fully drained.
/// </summary>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataRangePage)]
public sealed record DataRangePage
{
    /// <summary>Logical tree the page was read from.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The entries on this page, in ascending key order, pruned to the caller's
    /// authorized key subset.
    /// </summary>
    [Id(1)] public IReadOnlyList<DataEntry> Entries { get; init; } = Array.Empty<DataEntry>();

    /// <summary>
    /// The token to resume paging on the next request, or <see langword="null"/>
    /// when the range is fully drained.
    /// </summary>
    [Id(2)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// How much of the requested range the caller's access admitted, reported
    /// only on the first page of a scan opened with
    /// <see cref="DataRangeRequest.IncludeGateCoverage"/> set over a tree the
    /// caller can observe; otherwise <see langword="null"/>. It is a
    /// classification and never a count or a list of the withheld keys, because
    /// either would disclose data the caller may not read.
    /// </summary>
    [Id(3)] public LatticeRangeReadGateCoverage? GateCoverage { get; init; }
}
