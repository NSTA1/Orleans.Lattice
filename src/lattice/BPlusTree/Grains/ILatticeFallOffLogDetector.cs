namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Decision returned by <see cref="ILatticeFallOffLogDetector"/> at
/// leaf activation time. The leaf consults the detector before
/// driving <c>ILeafProjection.Apply</c> over the WAL slice; the
/// returned decision selects between a tail replay (the WAL still
/// covers the gap), a snapshot-then-WAL recovery, a full WAL rebuild,
/// or surfacing <see cref="LeafProjectionStaleException"/>.
/// </summary>
internal enum FallOffLogDecision
{
    /// <summary>
    /// The persisted projection checkpoint is within the readable
    /// portion of the WAL and the gap is within
    /// <see cref="LatticeOptions.MaxLeafReplayEntries"/>. The leaf
    /// drives <c>ILeafProjection.Apply</c> over the slice
    /// <c>(checkpoint, head]</c> directly.
    /// </summary>
    TailReplay = 0,

    /// <summary>
    /// A fall-off-log trigger fired (WAL trimmed past checkpoint,
    /// replay budget exceeded, or projection older than
    /// <see cref="LatticeOptions.LeafProjectionRetention"/>) and the
    /// configured policy selects the snapshot-then-WAL recovery path.
    /// </summary>
    SnapshotThenWal = 1,

    /// <summary>
    /// A fall-off-log trigger fired and the configured policy is
    /// <see cref="ProjectionRebuildPolicy.FullRebuildFromWal"/>. The
    /// leaf rebuilds from the absolute WAL tail, failing fast with
    /// <see cref="LeafProjectionStaleException"/> if the WAL has been
    /// trimmed.
    /// </summary>
    FullRebuildFromWal = 2,

    /// <summary>
    /// A fall-off-log trigger fired and the configured policy is
    /// <see cref="ProjectionRebuildPolicy.Fail"/>. The leaf surfaces
    /// <see cref="LeafProjectionStaleException"/> immediately and
    /// requires an operator-driven rebuild.
    /// </summary>
    Fail = 3,
}

/// <summary>
/// Silo-scoped seam that classifies a leaf grain''s replay path at
/// activation time. Pure decision logic - the detector consults the
/// commit-log reader for head/tail offsets and the resolved options
/// for the configured triggers, then returns a
/// <see cref="FallOffLogDecision"/> that the leaf grain''s activation
/// hook acts on.
/// </summary>
internal interface ILatticeFallOffLogDetector
{
    /// <summary>
    /// Classifies the activation-time replay path for the supplied
    /// <paramref name="treeId"/> / <paramref name="shardIndex"/>.
    /// </summary>
    /// <param name="treeId">Logical tree id.</param>
    /// <param name="shardIndex">WAL shard index.</param>
    /// <param name="checkpointOffset">
    /// The leaf''s persisted projection checkpoint offset under
    /// "applied through offset N inclusive" semantics. The next entry
    /// the materialiser will read is at <c>checkpointOffset + 1</c>.
    /// Pass <c>-1</c> as the "nothing applied" sentinel - a freshly
    /// activated leaf with no persisted state, or a leaf whose
    /// projection was reset via the operator rebuild seam - so the
    /// next replay starts at WAL offset <c>0</c> inclusive. Pass a
    /// real WAL offset (<c>0</c> or greater) for a leaf that has
    /// applied entries up to and including that offset.
    /// </param>
    /// <param name="checkpointAge">The wall-clock age of the persisted projection checkpoint, or <see cref="TimeSpan.Zero"/> when not tracked.</param>
    /// <param name="options">The resolved options for the tree.</param>
    /// <param name="cancellationToken">Cancellation token propagated to the underlying WAL grain calls.</param>
    Task<FallOffLogDecision> ClassifyAsync(
        string treeId,
        int shardIndex,
        long checkpointOffset,
        TimeSpan checkpointAge,
        ResolvedLatticeOptions options,
        CancellationToken cancellationToken);
}
