using Orleans.Concurrency;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// The result of <see cref="IViewCrossTreeCoordinatorGrain.RegisterReadyAsync"/>:
/// whether the joint cross-tree flip has durably committed across every
/// participant view tree.
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

    /// <summary>A not-yet-applied result: the wait set is still incomplete.</summary>
    public static ViewCrossTreeDecision NotReady { get; } = new() { Applied = false };

    /// <summary>An applied result: the joint flip has durably committed.</summary>
    public static ViewCrossTreeDecision Committed { get; } = new() { Applied = true };
}
