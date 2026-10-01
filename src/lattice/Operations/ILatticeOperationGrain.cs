namespace Orleans.Lattice.Operations;

/// <summary>
/// The durable tracking record of one coordinated long-running operation, keyed
/// by <see cref="LatticeOperationKey.For"/> (tenant and operation id). The work
/// itself runs in the <see cref="LatticeOperationRunner"/> of the silo that
/// accepted it; this grain holds the status every silo reads, the cancellation
/// request every silo can raise, and the liveness checks that turn a lost runner
/// into a <see cref="LatticeOperationState.Failed"/> operation rather than a stuck
/// running one.
/// </summary>
[Alias(TypeAliases.ILatticeOperationGrain)]
internal interface ILatticeOperationGrain : IGrainWithStringKey
{
    /// <summary>
    /// Creates the operation as <see cref="LatticeOperationState.Queued"/>, or
    /// returns the existing record unchanged when one already exists (an
    /// idempotent start).
    /// </summary>
    /// <param name="request">The begin request.</param>
    /// <returns>Whether this call created the operation, and its record.</returns>
    /// <exception cref="InvalidOperationException">An operation with the same id but a different kind exists.</exception>
    Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request);

    /// <summary>Records progress (moving a queued operation to running) and renews the heartbeat.</summary>
    /// <param name="report">The progress report.</param>
    /// <returns><see langword="true"/> when the runner must stop: cancellation was requested, or the operation is no longer running.</returns>
    Task<bool> ReportAsync(LatticeOperationProgressReport report);

    /// <summary>Renews the heartbeat without changing progress.</summary>
    /// <returns><see langword="true"/> when the runner must stop.</returns>
    Task<bool> HeartbeatAsync();

    /// <summary>Records the terminal outcome. A no-op once the operation is terminal.</summary>
    /// <param name="completion">The outcome.</param>
    /// <returns>The record after the call, or <see langword="null"/> when no operation exists.</returns>
    Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion);

    /// <summary>
    /// Reads the record, or <see langword="null"/> when no operation exists or it
    /// was pruned after its retention window. A non-terminal operation whose
    /// runner silo is dead, or whose heartbeat lease has lapsed, is failed first.
    /// </summary>
    /// <returns>The record, or <see langword="null"/>.</returns>
    Task<LatticeOperationRecord?> GetAsync();

    /// <summary>Requests cancellation. A terminal operation is returned unchanged.</summary>
    /// <returns>The record, or <see langword="null"/> when no operation exists.</returns>
    Task<LatticeOperationRecord?> RequestCancelAsync();
}
