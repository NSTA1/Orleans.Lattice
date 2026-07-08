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
    /// <exception cref="System.InvalidOperationException">
    /// The same saga id was previously submitted with a different participant
    /// set, target tree, or manifest id.
    /// </exception>
    Task<CrossClusterSagaOutcome> RunAsync(
        List<string> participantClusterIds,
        string targetTree,
        string manifestId,
        string coordinatorClusterId);

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
}
