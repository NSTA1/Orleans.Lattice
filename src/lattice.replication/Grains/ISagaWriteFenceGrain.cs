namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable, group-atomic write-fence and shipping-pause primitive for a
/// cross-cluster restore saga, keyed by saga id. One activation per saga owns
/// the fence for every tree the local cluster hosts in the saga's target group.
/// <para>
/// The primitive is the mechanism a restore participant engages during prepare
/// and releases on the terminal decision; it is not the restore itself. It
/// enforces the two-release-point rule:
/// </para>
/// <list type="number">
///   <item><description><b>Local write unblock</b> (<see cref="UnblockWritesAsync"/>)
///     may happen per-cluster once that cluster has flipped, admitting local
///     writes again while cross-cluster propagation stays paused.</description></item>
///   <item><description><b>Cross-cluster shipping/receiving resume</b> is gated on
///     <b>global</b> saga completion and only happens when
///     <see cref="PollResumeAsync"/> observes every participant has flipped.
///     Resuming on a mere local flip is unsafe: an early-flipping cluster could
///     receive a laggard's still-advanced post-cut entries and re-advance.</description></item>
/// </list>
/// <para>
/// The fence is durable and self-lifting: the write fence lifts on the bounded
/// cutover deadline even if the coordinator never signals, so a crash mid-saga
/// never strands a tree write-fenced. Shipping stays paused past the write-fence
/// self-lift and resumes strictly on observed global completion.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ISagaWriteFenceGrain)]
internal interface ISagaWriteFenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Engages the write fence, shipping pause, and inbound receive fence on
    /// every tree in <paramref name="request"/> as one atomic group. Idempotent
    /// for the same saga id; re-engaging refreshes the fanned-out fences.
    /// </summary>
    /// <param name="request">The engage request. <see cref="SagaWriteFenceRequest.SagaId"/>
    /// must match this grain's key; <see cref="SagaWriteFenceRequest.Trees"/> must be non-null.</param>
    Task EngageAsync(SagaWriteFenceRequest request);

    /// <summary>
    /// Lifts the <b>write fence only</b> for the group (the local flip release
    /// point), admitting local writes again. Shipping and receiving stay paused
    /// until global completion. No-op if the fence is not engaged.
    /// </summary>
    Task UnblockWritesAsync();

    /// <summary>
    /// Fully lifts the fence for the group - write fence, shipping pause, and
    /// receive fence - immediately and unconditionally. Called on an
    /// abort/compensation terminal decision, where no post-cut entries exist to
    /// re-propagate so global gating is unnecessary. Idempotent.
    /// </summary>
    Task LiftAsync();

    /// <summary>
    /// Re-evaluates the release gates now and returns the resulting snapshot.
    /// Self-lifts the write fence if the cutover deadline has passed, and
    /// resumes shipping and receiving if - and only if - the saga has globally
    /// completed. This is the reminder-driven crash-recovery entrypoint; it is
    /// also safe to call on demand.
    /// </summary>
    Task<SagaWriteFenceSnapshot> PollResumeAsync();

    /// <summary>Returns the current fence snapshot without mutating state.</summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<SagaWriteFenceSnapshot> GetSnapshotAsync();
}
