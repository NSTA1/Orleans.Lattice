namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for the change-observation endpoint
/// (<see cref="ILatticeStateObserver.ObserveAsync"/>). Scopes a live
/// subscription to a single tree and an optional key range, optionally resumes
/// from a prior cursor, and selects whether library-maintenance changes are
/// included.
/// </summary>
/// <remarks>
/// A subscription observes one tree (v1 is one-tree-per-subscription). The
/// stream is open-ended: it yields notifications as changes commit and only
/// completes when the caller cancels. Resuming with a
/// <see cref="ContinuationToken"/> older than the WAL retention window raises a
/// <see cref="LatticeStateCursorExpiredException"/> rather than silently
/// skipping the missed changes.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.StateObserveRequest)]
[Immutable]
public sealed record StateObserveRequest
{
    /// <summary>Logical tree identifier to observe.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Inclusive lower key bound of the observed range, or <see langword="null"/>
    /// to observe from the first key.
    /// </summary>
    [Id(1)] public string? StartInclusive { get; init; }

    /// <summary>
    /// Exclusive upper key bound of the observed range, or <see langword="null"/>
    /// to observe to the last key.
    /// </summary>
    [Id(2)] public string? EndExclusive { get; init; }

    /// <summary>
    /// Opaque resume cursor from a prior <see cref="StateChangeNotification.Position"/>.
    /// <see langword="null"/> or empty starts from the live tail (only changes
    /// committed after the subscription opens). A non-empty token resumes
    /// immediately after the referenced notification.
    /// </summary>
    [Id(3)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// When <see langword="true"/>, includes library-maintenance changes
    /// (compaction, rebalance, rewrite). Defaults to <see langword="false"/>,
    /// surfacing only user-driven writes.
    /// </summary>
    [Id(4)] public bool IncludeMaintenance { get; init; }
}
