namespace Orleans.Lattice.Scaling;

/// <summary>
/// An in-process, cluster-wide runtime-statistics snapshot: the count of active
/// silos (the current replica count) plus a per-silo resource sample. Produced
/// by <see cref="IClusterRuntimeStatisticsSource"/> from a single cluster
/// management round-trip and consumed by the compute collector and the replica
/// count provider. Not an Orleans wire type - it never leaves the silo.
/// </summary>
internal readonly record struct ClusterRuntimeSnapshot
{
    /// <summary>
    /// The number of active silos in the cluster (the current replica count).
    /// </summary>
    public int ActiveSiloCount { get; init; }

    /// <summary>
    /// Per-silo resource samples for the active silos. Never <see langword="null"/>
    /// once produced by the source; a default-constructed value exposes an empty
    /// list via <see cref="Silos"/>.
    /// </summary>
    private readonly IReadOnlyList<SiloResourceSample>? _silos;

    /// <summary>
    /// Per-silo resource samples. Never <see langword="null"/>: defaults to an
    /// empty list so callers can index without a null check.
    /// </summary>
    public IReadOnlyList<SiloResourceSample> Silos
    {
        get => _silos ?? Array.Empty<SiloResourceSample>();
        init => _silos = value;
    }
}
