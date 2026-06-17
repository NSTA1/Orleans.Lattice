using Orleans.Concurrency;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// The result of <see cref="IViewCrossTreeCoordinatorGrain.RegisterReadyAsync"/>
/// (and <see cref="IViewCrossTreeCoordinatorGrain.RegisterDegradedAsync"/>):
/// whether the joint cross-tree flip has durably committed across every
/// participant view tree, is still pending, or has been terminally degraded.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ViewCrossTreeDecision)]
internal sealed record ViewCrossTreeDecision
{
    /// <summary>
    /// <c>true</c> once every wait-set view has registered and the joint
    /// cross-tree flip has durably committed across every participant view tree.
    /// While <c>false</c> the wait set is still incomplete: the registering
    /// maintainer keeps its batch staged and re-registers on a later drain (or
    /// degrades to per-tree-slice atomicity once its readiness timeout elapses).
    /// </summary>
    [Id(0)] public required bool Applied { get; init; }

    /// <summary>
    /// <c>true</c> once the coordinator's decision has been terminally degraded:
    /// a participant timed out waiting for the joint flip and the coordinator
    /// will never issue it. Every participant (the one that degraded and any that
    /// register afterwards) flips its own slice per-tree-atomically instead, so a
    /// late joint flip can never clobber a degraded participant's local flip.
    /// Mutually exclusive with <see cref="Applied"/>.
    /// </summary>
    [Id(1)] public bool Degraded { get; init; }

    /// <summary>A not-yet-applied result: the wait set is still incomplete.</summary>
    public static ViewCrossTreeDecision NotReady { get; } = new() { Applied = false };

    /// <summary>An applied result: the joint flip has durably committed.</summary>
    public static ViewCrossTreeDecision Committed { get; } = new() { Applied = true };

    /// <summary>
    /// A terminally-degraded result: no joint flip will be issued; the caller
    /// flips its own slice per-tree-atomically.
    /// </summary>
    public static ViewCrossTreeDecision DegradedResult { get; } = new() { Applied = false, Degraded = true };
}
