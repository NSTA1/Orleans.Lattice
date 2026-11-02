using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Drives an accepted remediation to a terminal report one bounded
/// <see cref="ILatticeSchemaRemediationGrain.RunSliceAsync(string)"/> at a time, from
/// outside the grain, so no single grain call runs for the whole remediation and
/// none is cut off by the response timeout (issue #4123). Shared by the in-process
/// blocking admin verbs and the tracked schema operations.
/// </summary>
/// <remarks>
/// A driver follows exactly the remediation its accept call returned, identified by
/// that report's <see cref="LatticeSchemaRemediationReport.OperationId"/>, and never
/// slices, cancels or reports another (issue #5267). A start that joined a
/// remediation already in flight follows that remediation; when it ends under the
/// operation that started it, the follower reports its terminal outcome while the
/// coordinator still holds it. Once a later remediation has replaced that outcome,
/// the driver stops and returns the last report it saw of the followed remediation,
/// which is still <see cref="LatticeSchemaRemediationReport.InProgress"/>: the
/// followed remediation was superseded (<see cref="IsSuperseded"/>).
/// <para>
/// A slice call that outruns the grain call timeout is retried rather than failing
/// the run (issue #5440): each slice persists its cursor, so the retry resumes where
/// the slow slice left off. Only <see cref="MaxConsecutiveSliceTimeouts"/> timeouts in
/// a row end the run, rethrowing the cause.
/// </para>
/// </remarks>
internal static class SchemaRemediationDriver
{
    /// <summary>
    /// How many slices in a row may time out before the driver gives up and fails
    /// the run with the cause (issue #5440). A slice that completes resets the count.
    /// </summary>
    internal const int MaxConsecutiveSliceTimeouts = 5;

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
    /// <returns>
    /// The followed remediation's terminal report, or, when it ended under another
    /// operation and the coordinator no longer holds its outcome, the last in-flight
    /// report the driver saw of it (<see cref="IsSuperseded"/>).
    /// </returns>
    /// <exception cref="TimeoutException">
    /// <see cref="MaxConsecutiveSliceTimeouts"/> slices in a row timed out; the inner
    /// exception is the last timeout.
    /// </exception>
    internal static async Task<LatticeSchemaRemediationReport> DriveAsync(
        ILatticeSchemaRemediationGrain grain,
        LatticeSchemaRemediationReport accepted,
        ILatticeOperationProgress? progress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grain);
        var report = accepted;
        var followed = report.OperationId;
        var consecutiveTimeouts = 0;
        if (progress is not null && report.InProgress)
        {
            await ReportAsync(progress, report, phaseTotal: null, cancellationToken).ConfigureAwait(false);
        }

        while (report.InProgress)
        {
            if (cancellationToken.IsCancellationRequested && followed is not null)
            {
                var afterCancel = await grain.CancelAsync(followed).ConfigureAwait(false);
                if (!Follows(afterCancel, followed))
                {
                    return report;
                }

                report = afterCancel;
                if (!report.InProgress)
                {
                    break;
                }
            }

            SchemaRemediationSlice slice;
            try
            {
                slice = followed is null
                    ? await grain.RunSliceAsync().ConfigureAwait(false)
                    : await grain.RunSliceAsync(followed).ConfigureAwait(false);
                consecutiveTimeouts = 0;
            }
            catch (TimeoutException ex)
            {
                // The call outran the grain call timeout (issue #5440). The slice
                // itself may still be running, and it persists its cursor before it
                // returns or banks what it wrote before it faults, so the next slice
                // queues behind it on the non-reentrant coordinator and resumes from
                // that cursor; a destination write is idempotent, so a repeated value
                // is written again, never twice. The operation stays Running while the
                // retries last.
                if (++consecutiveTimeouts >= MaxConsecutiveSliceTimeouts)
                {
                    throw new TimeoutException(
                        $"The schema remediation slice timed out {consecutiveTimeouts} consecutive times; the last "
                        + $"attempt failed with: {ex.Message}",
                        ex);
                }

                continue;
            }

            if (followed is not null && !Follows(slice.Report, followed))
            {
                return report;
            }

            report = slice.Report;
            if (progress is not null && report.InProgress)
            {
                await ReportAsync(progress, report, slice.PhaseTotal, cancellationToken).ConfigureAwait(false);
            }
        }

        return report;
    }

    /// <summary>
    /// Whether a report <see cref="DriveAsync"/> returned means the followed
    /// remediation ended under another operation, with its outcome no longer held by
    /// the coordinator: the driver returns a report still in flight only then.
    /// </summary>
    /// <param name="report">The report <see cref="DriveAsync"/> returned.</param>
    /// <returns><see langword="true"/> when the followed remediation was superseded.</returns>
    internal static bool IsSuperseded(LatticeSchemaRemediationReport report) => report.InProgress;

    /// <summary>
    /// Why an operation that followed <paramref name="followedOperationId"/> ended
    /// without its outcome.
    /// </summary>
    /// <param name="followedOperationId">The followed remediation's operation id.</param>
    /// <returns>The reason.</returns>
    internal static string SupersededReason(string? followedOperationId) =>
        $"The remediation this operation followed ('{followedOperationId}') ended under another operation, and "
        + "the tree's remediation coordinator no longer holds its outcome because a later remediation replaced it. "
        + $"Its outcome is on the record of operation '{followedOperationId}', which started it, when that operation "
        + "was tracked. This operation applied nothing further.";

    /// <summary>
    /// Returns <paramref name="report"/> when it is terminal, for a caller that must
    /// return a terminal report, and throws when the followed remediation was
    /// superseded.
    /// </summary>
    /// <param name="report">The report <see cref="DriveAsync"/> returned.</param>
    /// <returns><paramref name="report"/>.</returns>
    /// <exception cref="InvalidOperationException">The followed remediation was superseded.</exception>
    internal static LatticeSchemaRemediationReport RequireTerminal(LatticeSchemaRemediationReport report) =>
        IsSuperseded(report) ? throw new InvalidOperationException(SupersededReason(report.OperationId)) : report;

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

    private static bool Follows(LatticeSchemaRemediationReport report, string followed) =>
        string.Equals(report.OperationId, followed, StringComparison.Ordinal);

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
