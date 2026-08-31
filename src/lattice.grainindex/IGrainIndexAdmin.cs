namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The operator surface over this silo's grain indexes: what they are, how far
/// their background backfills have got, and the controls to pause, resume, and
/// rebuild them.
/// </summary>
/// <remarks>
/// <para>
/// Resolve it from the silo's container -
/// <c>services.GetRequiredService&lt;IGrainIndexAdmin&gt;()</c> - anywhere a
/// silo-side service can be resolved: a hosted service, a minimal API endpoint,
/// a management grain. <c>AddGrainIndex</c> registers it, so no extra wiring is
/// needed.
/// <para>Example:</para>
/// <code>
/// var admin = services.GetRequiredService&lt;IGrainIndexAdmin&gt;();
/// foreach (var name in admin.DeclaredIndexes)
/// {
///     var status = await admin.GetStatusAsync(name);
///     Console.WriteLine($"{name}: {status.Backfill.State} {status.Progress.PercentComplete}%");
/// }
/// </code>
/// </para>
/// <para>
/// Every figure it reports is read from the index-registry system tree - the
/// stored declaration record and the crawl's durable checkpoint - rather than
/// from whatever a live activation happens to remember, so two silos asked the
/// same question give the same answer and a silo that has never run a pass
/// still reports the truth.
/// </para>
/// <para>
/// The control methods delegate to the one backfill activation that owns the
/// index cluster-wide, so calling them from any silo is equivalent, and calling
/// them repeatedly is safe: pausing a paused crawl, resuming a completed one, or
/// rebuilding twice all settle to the same state.
/// </para>
/// <para>
/// The surface is deliberately shaped for later remoting - names in, plain
/// serializable reports out, no live object graphs - so an HTTP or dashboard
/// exposure can be layered on it without changing it.
/// </para>
/// </remarks>
public interface IGrainIndexAdmin
{
    /// <summary>
    /// The logical names of every index this silo declares, in declaration
    /// order. It is the declaration set, not the registry's: an index appears
    /// here from the moment the silo declares it, before its first
    /// reconciliation has written a record.
    /// </summary>
    IReadOnlyList<string> DeclaredIndexes { get; }

    /// <summary>
    /// Reports one index's effective declaration, registry record, drift
    /// status, backfill state, progress, and entry count.
    /// </summary>
    /// <param name="indexName">The index to report on. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The index's status.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexNotDeclaredException">This silo declares no index by that name.</exception>
    Task<GrainIndexStatus> GetStatusAsync(string indexName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports the status of every declared index, in declaration order.
    /// </summary>
    /// <remarks>
    /// Each index costs the same reads
    /// <see cref="GetStatusAsync(string, CancellationToken)"/> does, including
    /// the entry count over its tree, so this is an operator call rather than
    /// something to poll on a tight loop.
    /// </remarks>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>One status per declared index.</returns>
    Task<IReadOnlyList<GrainIndexStatus>> ListStatusAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Holds an index's backfill at its checkpoint. Position and totals are
    /// preserved, so a resume continues rather than restarts. A no-op on a crawl
    /// that has completed or has never started.
    /// </summary>
    /// <param name="indexName">The index whose crawl to pause. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The crawl's state after the call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexNotDeclaredException">This silo declares no index by that name.</exception>
    Task<GrainIndexBackfillStatus> PauseBackfillAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a held or failed backfill to running, from its checkpoint rather
    /// than from the beginning. A no-op on a crawl that has completed or has
    /// never started.
    /// </summary>
    /// <param name="indexName">The index whose crawl to resume. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The crawl's state after the call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexNotDeclaredException">This silo declares no index by that name.</exception>
    Task<GrainIndexBackfillStatus> ResumeBackfillAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Rebuilds an index: discards the crawl's checkpoint and crawls the whole
    /// population again, re-visiting grains the index already records so their
    /// entries are rewritten under the declaration in force now.
    /// </summary>
    /// <remarks>
    /// This is the same restart the drift gate schedules when a breaking
    /// declaration change is accepted under
    /// <see cref="GrainIndexDriftPolicy.Rebuild"/>, invoked deliberately rather
    /// than in response to drift. It re-visits enrolled grains, so restarting a
    /// completed crawl does real work rather than finishing instantly.
    /// </remarks>
    /// <param name="indexName">The index to rebuild. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The crawl's state after the call.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexNotDeclaredException">This silo declares no index by that name.</exception>
    Task<GrainIndexBackfillStatus> RebuildAsync(string indexName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs exactly one backfill pass now, instead of waiting for the crawl's
    /// schedule. A pass on a crawl that is not running does nothing and reports
    /// the current state.
    /// </summary>
    /// <remarks>
    /// The operator's "step it along" button, and the seam a deployment that
    /// drives its crawls deliberately (with
    /// <see cref="GrainIndexOptions.BackfillEnabled"/> switched off) uses to
    /// pace them itself.
    /// </remarks>
    /// <param name="indexName">The index whose crawl to step. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>What the pass did.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    /// <exception cref="GrainIndexNotDeclaredException">This silo declares no index by that name.</exception>
    Task<GrainIndexBackfillBatchResult> RunBackfillPassAsync(
        string indexName,
        CancellationToken cancellationToken = default);
}
