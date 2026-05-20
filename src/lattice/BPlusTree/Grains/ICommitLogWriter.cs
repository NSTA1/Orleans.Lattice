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

    /// <summary>
    /// Persists every mutation in <paramref name="mutations"/> to the
    /// per-shard WAL as a single grain-dispatch envelope per touched
    /// WAL partition and returns the per-shard offset assigned to each
    /// mutation, in the same order as the input.
    /// <para>
    /// This is the batched counterpart to <see cref="AppendAsync"/>;
    /// the adapter groups the input by the partition hash the
    /// single-mutation overload applies, dispatches one
    /// <see cref="IWalShardGrain.AppendBatchAsync"/> call per
    /// partition, and stitches the returned offsets back into the
    /// caller's input order. A 64-key bulk write that hashes to a
    /// single WAL partition therefore pays one grain RPC instead of
    /// 64, which is the throughput win the batched leaf write path targets.
    /// </para>
    /// <para>
    /// Atomicity is per-partition: the per-partition batch is
    /// all-or-nothing (every mutation that landed on the same WAL
    /// partition is durably persisted before its offsets are
    /// returned, or none of them are), but mutations that hash to
    /// different WAL partitions commit independently. Callers that
    /// require cross-partition atomic semantics drive a saga via
    /// the shard-root coordinator's prepare/commit protocol; this
    /// adapter offers no cross-partition transaction.
    /// </para>
    /// </summary>
    /// <param name="mutations">The mutations to append, in caller-defined order. The returned offsets are parallel to this list.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the originating call.</param>
    /// <returns>An immutable list of per-shard offsets, one per input mutation, in input order.</returns>
    Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<LatticeMutation> mutations, CancellationToken cancellationToken = default);
}
