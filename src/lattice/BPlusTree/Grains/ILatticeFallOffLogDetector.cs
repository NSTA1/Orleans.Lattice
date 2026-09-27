namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Decision returned by <see cref="ILatticeFallOffLogDetector"/> at
/// leaf activation time. The leaf consults the detector before
/// driving <c>ILeafProjection.Apply</c> over the WAL slice. Every
/// non-loss outcome (<see cref="TailReplay"/>, <see cref="SnapshotPending"/>,
/// <see cref="TailReplayOverBudget"/>) tail-replays; genuine loss (the WAL
/// trimmed past the checkpoint) maps the configured
/// <see cref="ProjectionRebuildPolicy"/> to <see cref="SnapshotThenWal"/>,
/// <see cref="FullRebuildFromWal"/> or <see cref="Fail"/>, each of which the
/// leaf currently surfaces as <see cref="LeafProjectionStaleException"/>.
/// </summary>
internal enum FallOffLogDecision
{
    /// <summary>
    /// The persisted projection checkpoint is within the readable
    /// portion of the WAL and the gap is within
    /// <see cref="LatticeOptions.MaxLeafReplayEntries"/>. The leaf
    /// drives <c>ILeafProjection.Apply</c> over the slice
    /// <c>(checkpoint, head)</c> directly, the head being the next offset
    /// the WAL will assign.
    /// </summary>
    TailReplay = 0,

    /// <summary>
    /// Genuine loss - the WAL has been trimmed past the leaf's persisted
    /// checkpoint (<c>tail &gt; checkpoint + 1</c>) - and the configured policy
    /// is the snapshot-then-WAL recovery path. The leaf currently surfaces
    /// <see cref="LeafProjectionStaleException"/>. The cost triggers
    /// (replay budget, projection age) never produce this value.
    /// </summary>
    SnapshotThenWal = 1,

    /// <summary>
    /// Genuine loss and the configured policy is
    /// <see cref="ProjectionRebuildPolicy.FullRebuildFromWal"/>. The WAL has
    /// been trimmed, so a complete history is unavailable and the leaf
    /// surfaces <see cref="LeafProjectionStaleException"/>.
    /// </summary>
    FullRebuildFromWal = 2,

    /// <summary>
    /// Genuine loss and the configured policy is
    /// <see cref="ProjectionRebuildPolicy.Fail"/>. The leaf surfaces
    /// <see cref="LeafProjectionStaleException"/> immediately and
    /// requires an operator-driven rebuild.
    /// </summary>
    Fail = 3,

    /// <summary>
    /// Non-fatal advisory: no hard trigger fired, but the leaf's
    /// persisted checkpoint is within
    /// <see cref="LatticeOptions.LeafSnapshotMargin"/> of the WAL tail.
    /// The leaf treats this exactly as <see cref="TailReplay"/> at
    /// activation time (no behaviour change); the maintenance grain
    /// interprets the advisory as "schedule a snapshot capture" so
    /// the leaf-cache projection is durably copied into the snapshot
    /// storage grain before the WAL trims past the checkpoint.
    /// </summary>
    SnapshotPending = 4,

    /// <summary>
    /// Non-fatal advisory: a <b>cost</b> trigger fired - the replay gap
    /// exceeds <see cref="LatticeOptions.MaxLeafReplayEntries"/>, or the
    /// projection is older than
    /// <see cref="LatticeOptions.LeafProjectionRetention"/> - but the WAL
    /// still covers every offset the leaf needs, so a tail replay
    /// converges to exactly the same projection.
    /// <para>
    /// The leaf replays as it would for <see cref="TailReplay"/>; nothing
    /// warns or meters on this value (issue #2149). The over-budget warning
    /// and counter are raised during the replay itself, off the entries the
    /// leaf actually applies. Its one incidental effect is that it is
    /// returned before the <see cref="SnapshotPending"/> check, so an
    /// over-budget or over-age leaf never yields that advisory.
    /// </para>
    /// <para>
    /// This decision must never be fatal. A cost signal is not data loss:
    /// the genuine-loss condition is the WAL having been trimmed past the
    /// leaf's checkpoint, which is the only trigger that routes to the
    /// configured <see cref="ProjectionRebuildPolicy"/>. Treating a budget
    /// overrun as unrecoverable corruption permanently bricks a tree whose
    /// data is entirely intact (issue #1738).
    /// </para>
    /// </summary>
    TailReplayOverBudget = 5,
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
    /// The leaf's persisted projection checkpoint offset, under
    /// "SCANNED through offset N inclusive" semantics rather than
    /// "applied through" (issue #2270). Replay advances it over every
    /// entry it READS, including entries it deliberately skips as
    /// belonging to another leaf's key range or shard, so it is not a
    /// count of entries this leaf applied. That is load-bearing rather
    /// than an oversight, and
    /// <c>BPlusLeafGrain.RebuildProjectionFromWalAsync</c> carries the
    /// reason: <c>LatticeWalGc.ComputeMaterialiserOffsetFloorAsync</c>
    /// takes the MINIMUM of these offsets as the WAL retention floor,
    /// so advancing only over applied entries would let one leaf that
    /// owns no key in a partition pin WAL truncation for the whole
    /// tree. The distinction does not change this classifier's
    /// arithmetic - it compares the offset against the readable WAL
    /// window either way - but reading it as "applied through" is what
    /// makes that advance look like a defect worth removing.
    /// The next entry the materialiser will read is at
    /// <c>checkpointOffset + 1</c>. Pass <c>-1</c> as the "nothing
    /// scanned" sentinel - a freshly activated leaf with no persisted
    /// state, or a leaf whose projection was reset via the operator
    /// rebuild seam - so the next replay starts at WAL offset <c>0</c>
    /// inclusive. Pass a real WAL offset (<c>0</c> or greater) for a
    /// leaf whose replay has scanned up to and including that offset.
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
