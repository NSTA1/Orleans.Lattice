namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-repository self-index coordinator, keyed by repository id. It is the
/// single owner of one repository's "reach and stay fully indexed" guarantee: it
/// drives the initial index when the repository is onboarded, and then runs a
/// continuous, paged, low-cost background scan that detects a file which was
/// indexed structurally but whose embedding never fully landed - a run interrupted
/// between the structural commit and the vectorise pass, or a completed run whose
/// reconcile skipped an unembedded-but-unchanged file - and re-drives that
/// repository's idempotent index so the embedding back-fill closes the gap without
/// a client call. It also re-drives a repository whose last run outright failed.
/// One grain per registered repository is armed on repository add and torn down on
/// removal; a durable keep-alive reminder keeps each grain activated across host
/// restarts, and a jittered grain-local timer spreads the scans so many
/// repositories never all scan at once.
/// </summary>
internal interface IRepoContextSelfIndexGrain : IGrainWithStringKey
{
    /// <summary>
    /// Onboards (or re-onboards) this repository: registers the keep-alive reminder
    /// that keeps the grain activated across restarts, arms the scan timer if it is
    /// not already armed, and drives the initial indexing pass for the supplied
    /// request - returning the run's initial progress snapshot. Arming the durable
    /// reminder before the run starts is the onboarding commit point, so an
    /// interrupted first pass is still healed by this grain's own scan. Idempotent:
    /// calling it again re-drives the (idempotent, single-flight) index and re-arms
    /// anything not already armed.
    /// </summary>
    /// <param name="request">The indexing request describing the repository root and the ingest filters. Must not be <see langword="null"/>.</param>
    /// <returns>The initial progress snapshot of the driven indexing pass.</returns>
    Task<RepoIndexProgress> EnsureRunningAsync(RepoIndexJobRequest request);

    /// <summary>
    /// Stops this repository's self-index scan: disposes the scan timer, unregisters
    /// the keep-alive reminder, clears the durable checkpoint, and deactivates the
    /// grain, so a removed repository leaves no reminder firing and no state behind.
    /// Idempotent - stopping a grain that was never running is a harmless no-op.
    /// Called when a repository is removed.
    /// </summary>
    Task StopAsync();
}
