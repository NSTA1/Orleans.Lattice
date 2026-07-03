namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Request for a single-page, bounded range read over a tree's entries in
/// ascending key order. Reads one page of up to
/// <see cref="PageSize"/> entries for the half-open key range
/// <c>[<see cref="StartInclusive"/>, <see cref="EndExclusive"/>)</c> and, when
/// more entries remain, returns a <see cref="DataRangePage.ContinuationToken"/>
/// the caller passes back on the next request to resume paging.
/// </summary>
/// <remarks>
/// A live streaming scan / change feed is intentionally out of scope for v1;
/// this is a discrete, resumable paged read. Range reads are pruned to the
/// caller's authorized key subset by the gated <see cref="ILattice"/> surface.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataRangeRequest)]
[Immutable]
public sealed record DataRangeRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Inclusive lower key bound, or <see langword="null"/> to start from the
    /// first key. Ignored when <see cref="ContinuationToken"/> is set.
    /// </summary>
    [Id(1)] public string? StartInclusive { get; init; }

    /// <summary>
    /// Exclusive upper key bound, or <see langword="null"/> to read to the last
    /// key. Ignored when <see cref="ContinuationToken"/> is set.
    /// </summary>
    [Id(2)] public string? EndExclusive { get; init; }

    /// <summary>
    /// Maximum entries to return on this page. Non-positive values fall back to
    /// the configured default; larger values are clamped to the configured
    /// maximum.
    /// </summary>
    [Id(3)] public int PageSize { get; init; }

    /// <summary>
    /// The continuation token returned by a prior page, or
    /// <see langword="null"/> / empty to open a fresh scan. When set, the range
    /// bounds are ignored and paging resumes from where the prior page ended.
    /// </summary>
    [Id(4)] public string? ContinuationToken { get; init; }
}
