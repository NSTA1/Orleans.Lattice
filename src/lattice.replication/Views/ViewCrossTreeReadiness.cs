using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// One participating view's cross-tree readiness registration, handed to
/// <see cref="IViewCrossTreeCoordinatorGrain.RegisterReadyAsync"/> once that
/// view's maintainer has staged the cross-tree batch's slice and resolved its
/// own active view tree id.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ViewCrossTreeReadiness)]
internal sealed record ViewCrossTreeReadiness
{
    /// <summary>The cross-tree operation id (the coordinator's key).</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>The registering view's logical name.</summary>
    [Id(1)] public required string ViewName { get; init; }

    /// <summary>
    /// The view wait set: the names of every view whose source tree is one of
    /// the cross-tree batch's participants and that has a configured view.
    /// Frozen on the first registration and validated for exact match on later
    /// registrations.
    /// </summary>
    [Id(2)] public required IReadOnlyList<string> WaitSet { get; init; }

    /// <summary>
    /// The registering view's active-generation tree id, resolved locally by the
    /// maintainer at staging time so the coordinator can target the joint flip
    /// without a call back into the maintainer.
    /// </summary>
    [Id(3)] public required string ViewTreeId { get; init; }

    /// <summary>
    /// This view's slice: the coalesced upsert entries the cross-tree batch
    /// would flip into the view tree. Retraction deletes are not carried here -
    /// the cross-tree atomic write only sets (mirroring the single-tree atomic
    /// path), so the maintainer applies its own deletes after the joint flip.
    /// </summary>
    [Id(4)] public required List<KeyValuePair<string, byte[]>> Upserts { get; init; }
}
