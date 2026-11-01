using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The cluster-wide record of how far the runtime replication-configuration tree
/// has moved (#5462). A single activation, addressed by <see cref="GrainKey"/>:
/// the silo that observes a committed config write advances it, and every silo
/// reads it to learn that a write it did not observe itself has committed. The
/// core change feed fires only on the silo hosting the leaf that committed the
/// write, so without it every other silo would keep deciding enrollment from its
/// pre-write snapshot - refusing a peer's re-seed export and declining to
/// coordinate a restore of a tree the cluster replicates.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationConfigEpochGrain)]
internal interface IReplicationConfigEpochGrain : IGrainWithStringKey
{
    /// <summary>The fixed key of the single cluster-wide activation.</summary>
    const string GrainKey = "sys-replication-config-epoch";

    /// <summary>Returns the current epoch.</summary>
    /// <returns>The current epoch.</returns>
    [ReadOnly]
    Task<ReplicationConfigEpoch> GetAsync();

    /// <summary>Advances the epoch after a committed config write.</summary>
    /// <returns>The advanced epoch.</returns>
    Task<ReplicationConfigEpoch> AdvanceAsync();
}
