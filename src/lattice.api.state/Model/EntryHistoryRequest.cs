namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for the per-key change-history endpoint
/// (<see cref="ILatticeStateQuery.GetEntryHistoryAsync"/>). Selects a tree and a
/// single key, optionally bounds the revision window by hybrid-logical-clock,
/// bounds the page size and per-revision value-preview budget, and chooses the
/// in-page revision order. Paging is continuation-based: the first call (no
/// <see cref="ContinuationToken"/>) starts at the oldest in-range revision and
/// every continuation resumes immediately after the prior page.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryHistoryRequest)]
[Immutable]
public sealed record EntryHistoryRequest
{
    /// <summary>Logical tree identifier the key lives on.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key whose revision timeline to read.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>
    /// Inclusive lower hybrid-logical-clock bound, or <see langword="null"/> to
    /// start at the oldest available revision.
    /// </summary>
    [Id(2)] public HybridLogicalClock? FromHlc { get; init; }

    /// <summary>
    /// Inclusive upper hybrid-logical-clock bound, or <see langword="null"/> to
    /// run to the newest available revision.
    /// </summary>
    [Id(3)] public HybridLogicalClock? ToHlc { get; init; }

    /// <summary>
    /// Maximum number of revisions to return in this page. Values of <c>0</c> or
    /// below fall back to the configured default; larger values are clamped to
    /// the configured maximum.
    /// </summary>
    [Id(4)] public int Limit { get; init; }

    /// <summary>
    /// Opaque continuation token returned by a prior page, or
    /// <see langword="null"/> / empty to start a fresh read. The token resumes
    /// the timeline immediately after the last revision of the prior page.
    /// </summary>
    [Id(5)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// Per-revision value / delta preview byte budget. Values of <c>0</c> or
    /// below fall back to the configured default; larger values are clamped to
    /// the configured maximum. The full value length is always reported on each
    /// <see cref="EntryRevisionRecord"/> even when the preview is truncated.
    /// </summary>
    [Id(6)] public int ValuePreviewBudget { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the revisions in the returned page are
    /// ordered newest-first; when <see langword="false"/> (the default) they are
    /// ordered oldest-first. Paging always advances through the timeline from
    /// oldest to newest regardless of this flag - it controls only the order of
    /// the revisions within each returned page.
    /// </summary>
    [Id(7)] public bool Reverse { get; init; }
}
