namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Commit-log adapter abstraction over the per-shard write-ahead log.
/// The core library consumes the WAL through this interface so the
/// durability commit point lives entirely in the replication package
/// without the core taking a hard reference on
/// <c>Orleans.Lattice.Replication</c>.
/// <para>
/// Resolved via <see cref="System.IServiceProvider"/> as a nullable
/// service: when the replication package's
/// <c>AddLatticeReplication</c> extension has registered a concrete
/// adapter the seam yields it, otherwise the resolution returns
/// <see langword="null"/> and the leaf grain operates from its
/// in-memory projection alone (suitable for unit tests and
/// non-replicated single-cluster deployments). Internal by design -
/// the seam is the contract between the core library and a single
/// library-internal adapter, not a public extensibility surface for
/// third-party producers.
/// </para>
/// </summary>
internal interface ICommitLogWriter
{
    /// <summary>
    /// Persists <paramref name="mutation"/> to the per-shard WAL and
    /// returns the per-shard offset assigned to the appended entry.
    /// Offsets start at <c>0</c> for the first entry of a given
    /// <c>(treeId, shardIndex)</c> pair and increase monotonically by
    /// one per successful append; gaps never appear in a successfully
    /// persisted WAL.
    /// <para>
    /// The shard index the entry lands on is determined by the adapter
    /// from <see cref="LatticeMutation.Key"/> via the same partition
    /// hash the existing replication sink uses, so a foreground caller
    /// does not need to compute it.
    /// </para>
    /// </summary>
    /// <param name="mutation">The mutation to append. Must not be the default value.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the originating call.</param>
    /// <returns>The per-shard offset assigned to the appended entry.</returns>
    Task<long> AppendAsync(LatticeMutation mutation, CancellationToken cancellationToken = default);
}
