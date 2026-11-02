using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Coordinator for a cross-cluster saga. One activation per saga id (this
/// grain's key), living in the initiating cluster. Generalises the
/// intra-cluster two-level saga to span clusters: each participant is a cluster
/// reached over <see cref="ISagaControlChannel"/>. The coordinator drives
/// <see cref="CrossClusterSagaPhase.Preparing"/> -&gt; single global decision
/// -&gt; <see cref="CrossClusterSagaPhase.Completed"/>, persisting every phase
/// transition so it is crash-resumable and re-attach is idempotent.
/// <para>
/// <b>Decision authority.</b> Readers dial <see cref="GetDecisionAsync"/> for
/// the single source of truth on whether a participant commits or compensates.
/// The <see cref="CrossClusterSagaPhase.Preparing"/> -&gt;
/// <see cref="CrossClusterSagaPhase.Committed"/> transition is the one atomic
/// moment at which the saga becomes visible.
/// </para>
/// <para>
/// The interface is <c>internal</c>: callers reach the coordinator through the
/// package's own composition, never the grain interface directly.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.ICrossClusterSagaCoordinatorGrain)]
internal interface ICrossClusterSagaCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts (or resumes / re-attaches to) the saga over
    /// <paramref name="participantClusterIds"/>. Dispatches prepare to every
    /// participant cluster, commits only on a unanimous
    /// <see cref="SagaVote.Commit"/> vote, and otherwise aborts and compensates
    /// every prepared participant. Returns the terminal
    /// <see cref="CrossClusterSagaOutcome"/>. A duplicate call after the saga
    /// has completed returns the memoized outcome without re-running.
    /// </summary>
    /// <param name="participantClusterIds">
    /// The participant cluster ids. De-duplicated and canonicalised on first
    /// submit; must be non-null. An empty set is a vacuous commit.
    /// </param>
    /// <param name="targetTree">Logical tree id the mutation targets. Must be non-null.</param>
    /// <param name="manifestId">Content-manifest id describing the mutation. Must be non-null.</param>
    /// <param name="coordinatorClusterId">
    /// The initiating (coordinator) cluster id, stamped onto every control
    /// request. Must be non-null and non-empty.
    /// </param>
    /// <param name="setId">
    /// When set, the id of the captured backup <b>set</b> this saga restores as one
    /// atomic unit; it is stamped onto every outgoing
    /// <see cref="SagaControlRequest.SetId"/> so each participant expands and flips
    /// the set's member trees as one group. <c>null</c> for an ordinary single-tree
    /// restore. Additive: an existing single-tree caller omits it and the saga
    /// behaves exactly as before. Participates in the saga's re-submit stability
    /// fingerprint alongside the target tree and manifest id.
    /// </param>
    /// <exception cref="System.InvalidOperationException">
    /// The same saga id was previously submitted with a different participant
    /// set, target tree, manifest id, or set id.
    /// </exception>
    Task<CrossClusterSagaOutcome> RunAsync(
        List<string> participantClusterIds,
        string targetTree,
        string manifestId,
        string coordinatorClusterId,
        string? setId = null);

    /// <summary>
    /// Records (or re-attaches to) the saga exactly as <see cref="RunAsync"/>
    /// does, and returns at once without preparing anyone: the saga is advanced
    /// by <see cref="DriveAsync"/> and followed through <see cref="GetStatusAsync"/>.
    /// A caller that must not hold one grain call open for the whole saga - which
    /// can outlast the response timeout and report a committed saga as failed
    /// (issue #5506) - uses this instead of <see cref="RunAsync"/>.
    /// <para>
    /// Interleaved so a re-attach is answered even while a drive holds the
    /// coordinator. It mutates state only on first submit, before its first
    /// await; a re-attach only checks the persisted fingerprint.
    /// </para>
    /// </summary>
    /// <param name="participantClusterIds">As for <see cref="RunAsync"/>.</param>
    /// <param name="targetTree">As for <see cref="RunAsync"/>.</param>
    /// <param name="manifestId">As for <see cref="RunAsync"/>.</param>
    /// <param name="coordinatorClusterId">As for <see cref="RunAsync"/>.</param>
    /// <param name="setId">As for <see cref="RunAsync"/>.</param>
    /// <returns>The saga's status after the submit.</returns>
    /// <exception cref="System.InvalidOperationException">
    /// The same saga id was previously submitted with a different participant
    /// set, target tree, manifest id, or set id.
    /// </exception>
    [AlwaysInterleave]
    Task<CrossClusterSagaStatus> BeginAsync(
        List<string> participantClusterIds,
        string targetTree,
        string manifestId,
        string coordinatorClusterId,
        string? setId = null);

    /// <summary>
    /// Advances the saga as far as it will go in one turn of the coordinator's
    /// own: prepare, decide, and finalize. One-way, so the caller never waits on
    /// it and no response timeout applies to the saga's length. Idempotent: a
    /// completed or not-started saga is left as it is, and a fault leaves the
    /// saga where it stopped, visible through <see cref="GetStatusAsync"/>, for
    /// the next drive or the keepalive reminder to resume.
    /// </summary>
    [OneWay]
    Task DriveAsync();

    /// <summary>
    /// The saga's current status: its phase, its outcome once decided, whether a
    /// drive is running, and whether a participant refused the decision. Pure
    /// read, interleaved, so it answers while a drive holds the coordinator.
    /// </summary>
    [AlwaysInterleave]
    Task<CrossClusterSagaStatus> GetStatusAsync();

    /// <summary>
    /// The single global decision for this saga. Returns
    /// <see cref="CrossClusterSagaDecision.InFlight"/> while the coordinator is
    /// still preparing (so dialled reads see the pre-saga view), then the
    /// recorded <see cref="CrossClusterSagaDecision.Committed"/> /
    /// <see cref="CrossClusterSagaDecision.Aborted"/> verdict the instant the
    /// global decision is made. Pure read, safe to interleave.
    /// </summary>
    [AlwaysInterleave]
    Task<CrossClusterSagaDecision> GetDecisionAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator has reached a terminal state (or
    /// was never started). Used by tests and idempotent re-attach.
    /// </summary>
    Task<bool> IsCompleteAsync();

    /// <summary>
    /// The saga's durable decision, answered to a prepared participant whose
    /// cutover-fence timer expired (issue #4637), so it commits or compensates
    /// on the coordinator's decision and never on its timer alone.
    /// <see cref="CrossClusterSagaDecision.Committed"/> or
    /// <see cref="CrossClusterSagaDecision.Aborted"/> once decided;
    /// <see cref="CrossClusterSagaDecision.InFlight"/> while the coordinator is
    /// still collecting votes, which it bounds with its own prepare deadline and
    /// records as an abort past it. A coordinator that never started this saga
    /// (or no longer holds it) answers <see cref="CrossClusterSagaDecision.Aborted"/>:
    /// it prepared nobody, and a later run re-prepares every participant, which
    /// the compensated one refuses.
    /// <para>
    /// Read-only and interleaved, so a participant's query is answered even while
    /// a long prepare pass holds the coordinator. A <paramref name="participantClusterId"/>
    /// that is not one of the saga's recorded participants is refused with
    /// <see cref="UnauthorizedAccessException"/>.
    /// </para>
    /// </summary>
    /// <param name="participantClusterId">The authenticated cluster asking.</param>
    [AlwaysInterleave]
    Task<CrossClusterSagaDecision> ResolveDecisionForParticipantAsync(string participantClusterId);
}
