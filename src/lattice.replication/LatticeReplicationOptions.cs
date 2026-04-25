namespace Orleans.Lattice.Replication;

/// <summary>
/// Configuration options for <c>Orleans.Lattice.Replication</c>. Register a named
/// instance to override settings for a specific tree:
/// <code>
/// siloBuilder.Services.Configure&lt;LatticeReplicationOptions&gt;("my-tree", o => o.KeyFilter = k => k.StartsWith("repl/"));
/// </code>
/// The unnamed (default) instance applies cluster-wide. Per-tree overrides
/// follow the same named-options pattern as <c>LatticeOptions</c>; the
/// commit-time observer resolves the per-tree instance via
/// <c>IOptionsMonitor&lt;LatticeReplicationOptions&gt;.Get(treeId)</c>.
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
    /// Per-tree opt-in allowlist. <c>null</c> (the default) means
    /// "every tree is replicated"; an empty collection means "no trees are
    /// replicated"; a non-empty collection restricts replication to the
    /// named trees only. Membership is checked at commit time on the
    /// producer side, so a mutation against a tree outside the allowlist
    /// never reaches the WAL.
    /// </summary>
    public IReadOnlyCollection<string>? ReplicatedTrees { get; set; }

    /// <summary>
    /// Optional per-key filter evaluated on the producer side at commit
    /// time. When non-<c>null</c>, only mutations whose key satisfies the
    /// predicate are forwarded to the WAL; rejected mutations never touch
    /// replication state. Combines with <see cref="KeyPrefixes"/> as a
    /// logical AND - both filters must accept the key for it to replicate.
    /// <para>
    /// For <see cref="MutationKind.DeleteRange"/> mutations the predicate
    /// is evaluated against the inclusive start key only; replicating a
    /// range with mixed-prefix keys is the responsibility of the caller.
    /// </para>
    /// </summary>
    public Func<string, bool>? KeyFilter { get; set; }

    /// <summary>
    /// Optional declarative prefix allowlist evaluated on the producer
    /// side at commit time. <c>null</c> or empty means "no prefix
    /// restriction"; a non-empty collection restricts replication to keys
    /// that start with at least one of the listed prefixes. Combines with
    /// <see cref="KeyFilter"/> as a logical AND - both filters must
    /// accept the key for it to replicate.
    /// </summary>
    public IReadOnlyCollection<string>? KeyPrefixes { get; set; }

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
