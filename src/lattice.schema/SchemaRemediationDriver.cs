using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Drives an accepted remediation to a terminal report one bounded
/// <see cref="ILatticeSchemaRemediationGrain.RunSliceAsync"/> at a time, from
/// outside the grain, so no single grain call runs for the whole remediation and
/// none is cut off by the response timeout (issue #4123). Shared by the in-process
/// blocking admin verbs and the tracked schema operations.
/// </summary>
internal static class SchemaRemediationDriver
{
    /// <summary>
    /// Drives <paramref name="accepted"/> to a terminal report.
    /// </summary>
    /// <param name="grain">The tree's remediation coordinator.</param>
    /// <param name="accepted">The report the accept call returned.</param>
    /// <param name="progress">Where each slice's progress is reported, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">
    /// Requests cancellation. Once cancelled, the driver asks the coordinator to
    /// cancel the remediation it follows before every further slice; a remediation
    /// already at cutover declines and is driven on to completion.
    /// </param>
    /// <returns>The terminal report.</returns>
    internal static async Task<LatticeSchemaRemediationReport> DriveAsync(
        ILatticeSchemaRemediationGrain grain,
        LatticeSchemaRemediationReport accepted,
        ILatticeOperationProgress? progress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grain);
        var report = accepted;
        var followed = report.OperationId;
        if (progress is not null && report.InProgress)
        {
            await ReportAsync(progress, report, phaseTotal: null, cancellationToken).ConfigureAwait(false);
        }

        while (report.InProgress)
        {
            if (cancellationToken.IsCancellationRequested && followed is not null)
            {
                report = await grain.CancelAsync(followed).ConfigureAwait(false);
                if (!report.InProgress)
                {
                    break;
                }
            }

            var slice = await grain.RunSliceAsync().ConfigureAwait(false);
            report = slice.Report;
            if (progress is not null && report.InProgress)
            {
                await ReportAsync(progress, report, slice.PhaseTotal, cancellationToken).ConfigureAwait(false);
            }
        }

        return report;
    }

    /// <summary>The <see cref="SchemaOperationPhases"/> name of a remediation phase.</summary>
    /// <param name="phase">The phase.</param>
    /// <returns>The phase name.</returns>
    internal static string PhaseName(LatticeSchemaRemediationPhase phase) => phase switch
    {
        LatticeSchemaRemediationPhase.DryRun => SchemaOperationPhases.DryRun,
        LatticeSchemaRemediationPhase.Build => SchemaOperationPhases.Build,
        LatticeSchemaRemediationPhase.Cutover => SchemaOperationPhases.Cutover,
        _ => phase.ToString(),
    };

    private static async Task ReportAsync(
        ILatticeOperationProgress progress,
        LatticeSchemaRemediationReport report,
        int? phaseTotal,
        CancellationToken cancellationToken)
    {
        var counted = report.Phase is LatticeSchemaRemediationPhase.DryRun or LatticeSchemaRemediationPhase.Build;
        try
        {
            await progress.ReportAsync(
                PhaseName(report.Phase),
                counted ? report.ScannedCount : 0,
                counted ? phaseTotal : null,
                counted ? SchemaOperationPhases.ValuesUnit : null).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The sink refuses reports once cancellation is requested; the loop turns
            // the request into a cancel of the remediation itself before the next
            // slice, so the remediation is never abandoned mid-phase.
        }
    }
}
