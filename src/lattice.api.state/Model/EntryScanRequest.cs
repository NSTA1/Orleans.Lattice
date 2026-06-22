namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for the entry / key-range inspection endpoint
/// (<see cref="ILatticeStateQuery.ScanEntriesAsync"/>). Selects a tree and an
/// optional key range, bounds the page size and per-entry value-preview
/// budget, and optionally carries a server-side predicate so non-matching
/// values never cross the wire. Paging is snapshot-isolated: the first call
/// (no <see cref="ContinuationToken"/>) opens a point-in-time cursor and every
/// continuation pages against that same frozen view, so a multi-page scan is
/// resilient to concurrent writes, splits, and reshards.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.EntryScanRequest)]
[Immutable]
public sealed record EntryScanRequest
{
    /// <summary>Logical tree identifier to scan.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Inclusive lower key bound, or <see langword="null"/> to start at the
    /// first key.
    /// </summary>
    [Id(1)] public string? StartInclusive { get; init; }

    /// <summary>
    /// Exclusive upper key bound, or <see langword="null"/> to run to the last
    /// key.
    /// </summary>
    [Id(2)] public string? EndExclusive { get; init; }

    /// <summary>When <see langword="true"/>, scans in descending key order.</summary>
    [Id(3)] public bool Reverse { get; init; }

    /// <summary>
    /// Maximum number of entries to return in this page. Values of <c>0</c> or
    /// below fall back to the configured default; larger values are clamped to
    /// the configured maximum.
    /// </summary>
    [Id(4)] public int PageSize { get; init; }

    /// <summary>
    /// Opaque continuation token returned by a prior page. <see langword="null"/>
    /// or empty opens a fresh snapshot scan; otherwise the scan resumes the
    /// same point-in-time cursor.
    /// </summary>
    [Id(5)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// Per-entry value-preview byte budget. Values of <c>0</c> or below fall
    /// back to the configured default; larger values are clamped to the
    /// configured maximum. The full value length is always reported on each
    /// <see cref="EntryRecord"/> even when the preview is truncated.
    /// </summary>
    [Id(6)] public int ValuePreviewBudget { get; init; }

    /// <summary>
    /// Optional server-side predicate (the wire-stable lowering of a client
    /// <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c>). When set, the scan pushes
    /// the filter down so only matching entries are materialised and
    /// non-matching values are never transferred.
    /// </summary>
    [Id(7)] public LatticePredicateNode? Predicate { get; init; }

    /// <summary>
    /// Optional tag-index name. When set together with <see cref="Tag"/>, the
    /// scan is restricted to the keys of <see cref="TreeId"/> that carry
    /// <see cref="Tag"/> in this index (the index tree is resolved server-side,
    /// so the internal <c>tag-</c> naming convention never crosses the wire).
    /// The index name is the clean name surfaced by
    /// <see cref="ILatticeStateQuery.ListTagIndexesAsync"/>.
    /// </summary>
    [Id(8)] public string? IndexName { get; init; }

    /// <summary>
    /// Optional tag value to filter by. Honoured only when
    /// <see cref="IndexName"/> is also set; the scan then returns the entries of
    /// <see cref="TreeId"/> tagged with this value. The snapshot-cursor key
    /// range (<see cref="StartInclusive"/> / <see cref="EndExclusive"/> /
    /// <see cref="Reverse"/> / <see cref="Predicate"/>) does not apply to a
    /// tag-filtered scan.
    /// </summary>
    [Id(9)] public string? Tag { get; init; }
}
