namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A leaf activation's side of the WAL append fence (epic #5605), as the
/// commit-log writer sees it. The writer asks it for the stamp to append under at
/// each partition, and reports back every outcome, so the leaf claims its epoch
/// eagerly, re-presents a claim a restarted partition lost, records the unstamped
/// appends reported to it, and stops when it has been superseded.
/// </summary>
internal interface ILeafAppendFence
{
    /// <summary>
    /// The stamp to append under at <paramref name="partition"/>, claiming the
    /// activation's epoch there first if it has not been raised yet. Returns
    /// <see langword="null"/> when the activation's view of the fence gate is
    /// closed, so the append goes through the existing, unstamped method. Completes
    /// synchronously once the partition is claimed.
    /// </summary>
    /// <exception cref="WalAppendEpochStaleException">The activation has been superseded.</exception>
    ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken);

    /// <summary>A stamped append at <paramref name="partition"/> was admitted; records the reported unstamped watermark.</summary>
    void OnStampedAppended(int partition, long unstampedWatermark);

    /// <summary>
    /// The activation appended unstamped at <paramref name="partition"/>, up to
    /// <paramref name="highestOffset"/>; it reports its own unstamped appends to
    /// itself, as if the shard had (<c>ReadPositionHonestNoSelfReport</c>).
    /// </summary>
    void OnUnstampedAppended(int partition, long highestOffset);

    /// <summary>
    /// The partition had not loaded the activation's entry: the next
    /// <see cref="StampForAsync"/> re-presents the claim, which loads it.
    /// </summary>
    void OnUnloaded(int partition, WalAppendStamp stamp);

    /// <summary>
    /// A stamped append was rejected as stale: the activation stops and
    /// deactivates. Returns the exception the writer throws.
    /// </summary>
    WalAppendEpochStaleException OnStale(WalAppendStamp stamp);

    /// <summary>
    /// A stamped method failed with the definite "method unknown" failure of a
    /// silo running an older build: the activation closes its own view of the
    /// gate, and the writer re-issues the append unstamped.
    /// </summary>
    void OnStampedMethodUnknown();
}
