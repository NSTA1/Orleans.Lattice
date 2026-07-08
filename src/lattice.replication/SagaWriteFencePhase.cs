namespace Orleans.Lattice.Replication;

/// <summary>
/// Lifecycle phase of the durable per-tree write-fence and shipping-pause
/// primitive engaged for a cross-cluster saga cutover, persisted in
/// <see cref="Grains.SagaWriteFenceState"/> so it survives an activation crash.
/// <para>
/// The primitive has <b>two distinct release points that must not be
/// conflated</b>: the local write unblock (per-cluster, on the local flip) and
/// the cross-cluster shipping resume (gated on global saga completion). The
/// phase records which release points have been reached.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SagaWriteFencePhase)]
internal enum SagaWriteFencePhase
{
    /// <summary>No fence is engaged - the steady state.</summary>
    None = 0,

    /// <summary>
    /// The write fence, shipping pause, and inbound receive fence are all
    /// engaged on every tree in the local group. Writes are refused, and
    /// neither post-cut entries leave nor peer entries are applied.
    /// </summary>
    Engaged = 1,

    /// <summary>
    /// The local write fence has been lifted (the local flip completed, or the
    /// bounded cutover deadline self-lifted it) but shipping and receiving stay
    /// paused because the saga has not yet globally completed. Writes are
    /// admitted again; cross-cluster propagation is still gated.
    /// </summary>
    WritesUnblocked = 2,

    /// <summary>
    /// Terminal - the saga globally completed, so the write fence, shipping
    /// pause, and receive fence have all been lifted and shipping has resumed
    /// with its cursor intact.
    /// </summary>
    Lifted = 3,
}
