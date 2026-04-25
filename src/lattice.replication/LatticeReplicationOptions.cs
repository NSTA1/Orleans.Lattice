namespace Orleans.Lattice.Replication;

/// <summary>
/// Configuration options for <c>Orleans.Lattice.Replication</c>. Register a named
/// instance to override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeReplicationOptions&gt;("my-tree", o => o.ReplicatedTrees = new[] { "my-tree" });
/// </code>
/// The unnamed (default) instance applies cluster-wide. Per-tree overrides
/// follow the same named-options pattern as <c>LatticeOptions</c>.
/// </summary>
public class LatticeReplicationOptions
{
    /// <summary>
    /// Stable identifier for the local Orleans cluster. Stamped on every
    /// replicated mutation so receivers can attribute the origin and break
    /// replication cycles. Must be globally unique across every cluster that
    /// participates in replication, and must be set to a non-empty value -
    /// the registered <c>IValidateOptions&lt;LatticeReplicationOptions&gt;</c>
    /// rejects an empty or whitespace cluster id at first-resolve time.
    /// </summary>
    public string ClusterId { get; set; } = DefaultClusterId;

    /// <summary>
    /// Names of the trees that participate in replication. An empty
    /// collection means "no trees are replicated"; the default is empty so
    /// replication is opt-in per tree.
    /// </summary>
    public IReadOnlyCollection<string> ReplicatedTrees { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Number of write-ahead-log partitions per replicated tree. Each
    /// captured <see cref="ReplogEntry"/> is routed to a single
    /// <c>IReplogShardGrain</c> activation keyed by
    /// <c>{treeId}/{partition}</c>, where <c>partition</c> is a stable hash
    /// of the entry's key modulo this value. Defaults to <see cref="DefaultReplogPartitions"/>
    /// (a single per-tree WAL, sufficient for low-fan-in workloads); raise
    /// to fan WAL writes across multiple grain activations on hot trees.
    /// Must be at least <c>1</c>; the registered options validator rejects
    /// non-positive values at first-resolve time.
    /// </summary>
    public int ReplogPartitions { get; set; } = DefaultReplogPartitions;

    /// <summary>
    /// Default value for <see cref="ClusterId"/>: an empty sentinel that
    /// represents "unset". This default is rejected by
    /// <c>LatticeReplicationOptionsValidator</c> so a host that calls
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
    /// without supplying a cluster id fails fast on first options resolution.
    /// </summary>
    public const string DefaultClusterId = "";

    /// <summary>
    /// Default value for <see cref="ReplogPartitions"/>: a single WAL
    /// partition per replicated tree. Adequate for low-fan-in workloads;
    /// raise for hot trees that benefit from parallel WAL append paths.
    /// </summary>
    public const int DefaultReplogPartitions = 1;
}
