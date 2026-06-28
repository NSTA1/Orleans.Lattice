namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the per-key change-history RPC. A serializable mirror of
/// <see cref="EntryHistoryResult"/> carrying a continuation-paged page of a
/// key's revision timeline plus the history metadata (how the timeline is
/// bounded and, when truncated, the oldest still-readable revision).
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.EntryHistoryResponse)]
[Immutable]
public sealed record EntryHistoryResponse
{
    /// <summary>Lookup outcome mapped from the facade result.</summary>
    [Id(0)] public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The key whose history was queried.</summary>
    [Id(2)] public required string Key { get; init; }

    /// <summary>The revisions in this page, ordered per the request's reverse flag (oldest-first by default).</summary>
    [Id(3)] public IReadOnlyList<EntryRevisionRecord> Revisions { get; init; } = Array.Empty<EntryRevisionRecord>();

    /// <summary>
    /// Opaque continuation token to fetch the next page, or
    /// <see langword="null"/> when the timeline is fully drained.
    /// </summary>
    [Id(4)] public string? ContinuationToken { get; init; }

    /// <summary>How the returned timeline is bounded below.</summary>
    [Id(5)] public EntryHistoryBound Bound { get; init; }

    /// <summary>
    /// On a <see cref="EntryHistoryBound.Truncated"/> page, the
    /// hybrid-logical-clock timestamp of the oldest still-readable revision;
    /// <see cref="HybridLogicalClock.Zero"/> otherwise.
    /// </summary>
    [Id(6)] public HybridLogicalClock EarliestAvailable { get; init; }
}
