using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Reports the derived indexing cadence once at startup, so the conversion from the
/// operator-facing wall-clock knobs to the pass counts the reconcile actually enforces
/// is visible in the log rather than having to be reconstructed from source.
/// <para>
/// <see cref="RepoContextIndexingOptions.FullWalkInterval"/> and
/// <see cref="RepoContextIndexingOptions.EmbeddingGapScanInterval"/> are converted to pass
/// counts against <see cref="RepoContextIndexingOptions.MaximumReconcileSpacing"/>, so the
/// three knobs are a matched set: raising the reconcile interval silently re-denominates
/// both of the others. Because the conversion clamps to a minimum of one pass rather than
/// erroring, an out-of-range result degenerates quietly - and when
/// <see cref="RepoContextIndexingOptions.PassesPerFullWalk"/> floors to one,
/// <see cref="RepoContextIndexingOptions.PruningCanEngage"/> goes false and
/// directory-modification-time pruning stops happening at all. See issue #2075. Likewise,
/// when <see cref="RepoContextIndexingOptions.PassesPerEmbeddingGapScan"/> floors to one,
/// the embedding gap scan runs on every pass and its cadence has no effect; both
/// degenerate cases are logged as warnings. See issue #3350.
/// </para>
/// <para>
/// The approximate-index build sweep cadence is reported beside them precisely because it
/// is <i>not</i> a member of that matched set. It used to be - it was derived from the
/// reconcile interval, so raising that variable throttled arming as well (issue #2459) -
/// and stating the two cadences together, with the independence spelled out, is what makes
/// the separation visible to the operator who previously had no way to see the coupling.
/// The line reports configuration only: whether the sweep runs is decided elsewhere and
/// announced by the sweep service's own entry line, and re-deriving that decision here
/// would put a second copy of a guard in a second place.
/// </para>
/// <para>
/// This is observability only: it reads the options and logs. It changes no behaviour and
/// never fails startup, because a cadence this service disagrees with is still a cadence
/// the host is entitled to run.
/// </para>
/// </summary>
/// <param name="options">The resolved indexing cadence.</param>
/// <param name="logger">The log sink for the startup report.</param>
internal sealed class RepoContextIndexingCadenceReporter(
    RepoContextIndexingOptions options,
    ILogger<RepoContextIndexingCadenceReporter> logger) : IHostedService
{
    /// <summary>
    /// Logs the configured wall-clock intervals alongside the pass counts they derive,
    /// and warns when the arithmetic has disabled pruning or collapsed the embedding gap
    /// scan cadence to every pass.
    /// </summary>
    /// <param name="cancellationToken">Unused; the report is synchronous.</param>
    /// <returns>A completed task.</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        logger.LogInformation(
            "Repository-context indexing cadence: reconcile every {ReconcileSeconds:0.###} s "
            + "(spacing {SpacingSeconds:0.###} s including jitter); full walk "
            + "{FullWalkSeconds:0.###} s = {FullWalkPasses} pass(es); embedding gap scan "
            + "{GapScanSeconds:0.###} s = {GapScanPasses} pass(es); "
            + "directory-modification-time pruning can engage: {PruningCanEngage}. "
            + "The approximate-index build sweep is configured at {AnnSweepSeconds:0.###} s, "
            + "which is independent of the reconcile cadence above (issue #2459); whether that "
            + "sweep runs at all is a separate decision this line does not report.",
            options.ReconcileInterval.TotalSeconds,
            options.MaximumReconcileSpacing.TotalSeconds,
            options.FullWalkInterval.TotalSeconds,
            options.PassesPerFullWalk,
            options.EmbeddingGapScanInterval.TotalSeconds,
            options.PassesPerEmbeddingGapScan,
            options.PruningCanEngage,
            options.EffectiveAnnSweepInterval.TotalSeconds);

        if (!options.PruningCanEngage)
        {
            // Deliberately unconditional rather than trying to infer intent. Pruning
            // being off means RepoWalkPruning is inert - its snapshot is written on
            // every run and never read for the benefit it exists to provide - and that
            // is worth one startup line whether or not the operator meant it. The line
            // carries the arithmetic that produced it and the threshold that undoes it,
            // because the whole failure mode is that neither is discoverable.
            logger.LogWarning(
                "Directory-modification-time pruning is DISABLED by the cadence arithmetic: a "
                + "full walk interval of {FullWalkSeconds:0.###} s floors to one pass at a "
                + "reconcile spacing of {SpacingSeconds:0.###} s, so every reconcile forces a "
                + "full sweep and the prune cache is written on every run but never read. The "
                + "wall-clock interval knobs are a matched set - they are converted to pass "
                + "counts against the reconcile spacing, so raising the reconcile interval "
                + "re-denominates both of the others. Set the full walk interval above the "
                + "reconcile spacing (for example {SuggestedFullWalkSeconds:0.###} s) to "
                + "re-enable pruning, or lower the reconcile interval.",
                options.FullWalkInterval.TotalSeconds,
                options.MaximumReconcileSpacing.TotalSeconds,
                options.MaximumReconcileSpacing.TotalSeconds * 2);
        }

        if (options.PassesPerEmbeddingGapScan <= 1)
        {
            // Issue #3350. The same degeneracy as the pruning warning above, on the
            // other pass-counted knob: an interval at or below the reconcile spacing
            // rounds up to one pass, so the periodic term of the gap-scan gate is true
            // on every pass and the convergence back-off behind it can never change
            // whether the scan runs. The shipped 20-minute default sits exactly on that
            // boundary at the default 15 + 5 minute spacing, so this fires at defaults
            // by design - it is the only place the collapse is otherwise visible, as a
            // bare "1 pass(es)" on the line above.
            logger.LogWarning(
                "The embedding gap scan cadence has NO EFFECT: an interval of "
                + "{GapScanSeconds:0.###} s is at or below the reconcile spacing of "
                + "{SpacingSeconds:0.###} s, so it resolves to one pass and the whole-repository "
                + "gap scan runs on every reconcile pass whatever the coverage verdict says. The "
                + "wall-clock interval knobs are a matched set - they are converted to pass "
                + "counts against the reconcile spacing, which is the reconcile interval plus "
                + "its jitter. Set the embedding gap scan interval above the reconcile spacing "
                + "(for example {SuggestedGapScanSeconds:0.###} s) for the cadence to engage, or "
                + "lower the reconcile interval.",
                options.EmbeddingGapScanInterval.TotalSeconds,
                options.MaximumReconcileSpacing.TotalSeconds,
                options.MaximumReconcileSpacing.TotalSeconds * 2);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// No-op: the reporter holds no resources.
    /// </summary>
    /// <param name="cancellationToken">Unused.</param>
    /// <returns>A completed task.</returns>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
