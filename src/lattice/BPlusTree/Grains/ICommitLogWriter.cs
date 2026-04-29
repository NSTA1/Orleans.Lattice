namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Dormant seam over the per-shard write-ahead log. the dormant seam''s commit-log
/// adapter abstraction: the core library consumes the WAL through this
/// interface so the durability commit point can be promoted from
/// <c>state.WriteStateAsync()</c> on <c>BPlusLeafGrain</c> to a WAL append
/// without taking a hard reference on the
/// <c>Orleans.Lattice.Replication</c> package.
/// <para>
/// Resolved via <see cref="System.IServiceProvider"/> as a nullable
/// service: when the replication package''s
/// <c>AddLatticeReplication</c> extension has registered a concrete
/// adapter the seam yields it, otherwise the resolution returns
/// <see langword="null"/> and pre-the WAL-as-commit-point promotion commit behaviour applies
/// unchanged. Internal by design — the seam is the contract between the
/// core library and a single library-internal adapter, not a public
/// extensibility surface for third-party producers.
/// </para>
/// <para>
/// <b>Dormancy.</b> the dormant seam ships this interface and its concrete adapter
/// without any foreground call site. the future foreground caller wires
/// <see cref="AppendAsync"/> into the leaf write path under the
/// <c>LatticeOptions.LeafShadowWrites</c> toggle.
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
