namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The seam a bootstrap run reports incremental progress through. The default is
/// no sink (a direct, synchronous bootstrap call reports nothing); the
/// asynchronous indexing path supplies a sink that forwards each delta to the
/// durable job grain, so a run's phase and counters can be observed while it is
/// still in flight.
/// </summary>
internal interface IRepoIndexProgressSink
{
    /// <summary>
    /// Records a partial progress delta. Implementations must be safe to call from
    /// the bootstrap run's thread and should not throw the run down on a transient
    /// reporting failure - progress is advisory, not part of the durable write.
    /// </summary>
    /// <param name="update">The fields that changed since the last report.</param>
    /// <param name="cancellationToken">Cancels the report.</param>
    /// <returns>A task that completes when the delta has been accepted.</returns>
    ValueTask ReportAsync(RepoIndexProgressUpdate update, CancellationToken cancellationToken);
}
