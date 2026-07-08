namespace Orleans.Lattice.Replication;

/// <summary>
/// Engage request for the durable per-tree write-fence and shipping-pause
/// primitive (<see cref="Grains.ISagaWriteFenceGrain"/>). Carries the saga
/// identity, the group of trees the local cluster hosts for the saga (fenced
/// and lifted as one atomic group), the coordinator cluster whose terminal
/// completion gates the shipping resume, and the bounded cutover window that
/// sizes the self-lifting fence deadline.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaWriteFenceRequest)]
[Immutable]
internal readonly record struct SagaWriteFenceRequest
{
    /// <summary>Identifier of the cross-cluster saga engaging the fence.</summary>
    [Id(0)]
    public string SagaId { get; init; }

    /// <summary>
    /// Physical tree ids the local cluster hosts for the saga's target
    /// (a single tree, or every tree in a backup set present on this cluster).
    /// The whole set is fenced and lifted together so no tree in the set is
    /// writable while its siblings are fenced.
    /// </summary>
    [Id(1)]
    public List<string> Trees { get; init; }

    /// <summary>
    /// Cluster id of the saga coordinator. Global completion is observed by
    /// dialling that cluster's coordinator grain; shipping resume is gated on
    /// it.
    /// </summary>
    [Id(2)]
    public string CoordinatorClusterId { get; init; }

    /// <summary>
    /// Bounded cutover window, in seconds, that sizes the self-lifting fence
    /// deadline. A non-positive value selects the primitive's default window.
    /// The write fence self-lifts once this window elapses so a coordinator
    /// crash never strands the tree write-fenced; it deliberately covers only
    /// the short cutover, not the (potentially long) shadow build.
    /// </summary>
    [Id(3)]
    public int FenceWindowSeconds { get; init; }
}
