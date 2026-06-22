using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// A single live state-change notification yielded by
/// <see cref="ILatticeStateObserver.ObserveAsync"/> (and the gRPC change
/// subscription built over it). Carries the tree, the affected key (and, for a
/// range delete, the exclusive upper bound), the change kind, the mutation's
/// hybrid-logical-clock timestamp, the mutation category, and a monotonic
/// <see cref="Position"/> the client can supply on (re)subscribe to resume
/// without gaps.
/// </summary>
/// <remarks>
/// Delivery is at-least-once: a client that resumes from a stored
/// <see cref="Position"/> may observe a notification it already processed
/// after a reconnect, so client application must be idempotent. The
/// <see cref="Position"/> token is opaque; do not parse it.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.StateChangeNotification)]
[Immutable]
public sealed record StateChangeNotification
{
    /// <summary>The logical tree the change occurred on.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// The affected key. For a <see cref="StateChangeKind.DeleteRange"/> this
    /// is the inclusive lower bound of the deleted range.
    /// </summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>
    /// The exclusive upper bound for a <see cref="StateChangeKind.DeleteRange"/>,
    /// or <see langword="null"/> for a single-key change.
    /// </summary>
    [Id(2)] public string? EndExclusiveKey { get; init; }

    /// <summary>The kind of change.</summary>
    [Id(3)] public StateChangeKind Kind { get; init; }

    /// <summary>The mutation's hybrid-logical-clock timestamp.</summary>
    [Id(4)] public HybridLogicalClock Hlc { get; init; }

    /// <summary>Whether the change was a user-driven write or library maintenance.</summary>
    [Id(5)] public MutationCategory Category { get; init; }

    /// <summary>
    /// Opaque, monotonic resume cursor. Supply the last successfully-processed
    /// value as <see cref="StateObserveRequest.ContinuationToken"/> on
    /// (re)subscribe to resume immediately after this notification.
    /// </summary>
    [Id(6)] public required string Position { get; init; }
}
