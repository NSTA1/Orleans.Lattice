using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The log shape of a deferred replay that was <b>refused admission</b> to the
/// per-silo WAL replay permit queue (issue #3906).
/// <para>
/// <b>Why a refusal is not logged like a fault.</b> A refusal is the admission
/// gate of issue #3284 working as designed: the queue is past its bound and not
/// draining, so the activation is turned away with a retryable
/// <see cref="LatticeSaturatedException"/> instead of joining a queue it cannot
/// reach the head of. Each refusal is already counted exactly on
/// <c>orleans.lattice.saturation.refusals</c> under the
/// <c>replay_permit_admission</c> source. The replay barrier used to report it
/// through the generic "deferred WAL replay failed" warning, with the exception
/// attached, at up to one line per second per silo. Under a contended cold open
/// that was one full stack trace and roughly 450 characters of remediation prose
/// per second, measured at 52,220 log lines in 50 minutes, and it buried the
/// warnings that mattered.
/// </para>
/// <para>
/// A refusal is therefore <b>aggregated</b>: every refusal is counted into a
/// per-silo tally, and at most one summary line per
/// <see cref="ReplayAdmissionRefusalLogInterval"/> reports how many refusals the
/// silo made since the previous summary. The line carries no exception, because
/// the stack is always the same frames inside the replay barrier and names
/// nothing a reader can act on. A genuine replay fault keeps the fault warning,
/// with its exception.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The minimum interval between two replay-admission refusal summary lines on
    /// one silo. The per-refusal count is exact on the saturation-refusal
    /// counter, so the log only has to say that refusals are happening and at
    /// roughly what rate.
    /// </summary>
    internal static readonly TimeSpan ReplayAdmissionRefusalLogInterval = TimeSpan.FromMinutes(1);

    private static readonly long ReplayAdmissionRefusalLogIntervalTimestampTicks =
        (long)(ReplayAdmissionRefusalLogInterval.TotalSeconds * Stopwatch.Frequency);

    /// <summary>
    /// Refusals counted since the last summary line. Silo-wide, because the
    /// admission gate it describes is silo-wide.
    /// </summary>
    private static long _replayAdmissionRefusalsSinceLastLine;

    /// <summary>
    /// <see cref="Stopwatch"/> timestamp of the last summary line, or zero when
    /// none has been written since the process started (or since a test reset).
    /// </summary>
    private static long _lastReplayAdmissionRefusalLineTimestamp;

    /// <summary>
    /// Counts one replay-admission refusal and decides whether this refusal
    /// writes the summary line. Allocation-free: two interlocked operations on
    /// the common path, where the line is not due.
    /// </summary>
    /// <param name="now">The current <see cref="Stopwatch"/> timestamp.</param>
    /// <param name="refusals">
    /// When this call returns <see langword="true"/>, the number of refusals
    /// since the previous summary line, including this one. Every refusal is
    /// reported by exactly one line: a refusal counted while another caller is
    /// taking the line is reported by the next one.
    /// </param>
    /// <param name="sincePreviousLine">
    /// When this call returns <see langword="true"/>, the time since the previous
    /// summary line, or <see cref="TimeSpan.Zero"/> for the first line.
    /// </param>
    /// <returns><see langword="true"/> when the caller must write the summary line.</returns>
    internal static bool TryTakeReplayAdmissionRefusalLine(
        long now, out long refusals, out TimeSpan sincePreviousLine)
    {
        Interlocked.Increment(ref _replayAdmissionRefusalsSinceLastLine);

        var last = Volatile.Read(ref _lastReplayAdmissionRefusalLineTimestamp);
        if ((last != 0 && now - last < ReplayAdmissionRefusalLogIntervalTimestampTicks)
            || Interlocked.CompareExchange(ref _lastReplayAdmissionRefusalLineTimestamp, now, last) != last)
        {
            refusals = 0;
            sincePreviousLine = TimeSpan.Zero;
            return false;
        }

        refusals = Interlocked.Exchange(ref _replayAdmissionRefusalsSinceLastLine, 0);
        sincePreviousLine = last == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(last, now);
        return true;
    }

    /// <summary>
    /// Clears the refusal tally and the line stamp. Test-only: both are
    /// process-wide statics, so a fixture asserting on the first line of an
    /// episode needs them empty regardless of what ran before it.
    /// </summary>
    internal static void ResetReplayAdmissionRefusalLogForTest()
    {
        Volatile.Write(ref _replayAdmissionRefusalsSinceLastLine, 0);
        Volatile.Write(ref _lastReplayAdmissionRefusalLineTimestamp, 0);
    }

    /// <summary>
    /// Whether <paramref name="ex"/> is this activation's own replay-admission
    /// refusal, as opposed to a replay fault. Keyed on the recorded admission
    /// phase as well as the type, exactly as the activation-failure counter's
    /// reason split is, so a saturation raised by some other seam the replay
    /// reaches is still reported as the fault it is.
    /// </summary>
    private bool IsOwnReplayAdmissionRefusal(Exception ex)
        => ex is LatticeSaturatedException { SaturationSource: LatticeSaturationSource.ReplayPermitAdmission }
            && _replayAdmissionPhase == ReplayAdmissionPhase.RefusedAdmission;

    /// <summary>
    /// The <c>arm</c> tag value of this activation's most recent replay-admission
    /// refusal (issue #3921): <c>wait_exceeded</c> or <c>no_progress</c>, or
    /// <see langword="null"/> before any. Always one of the frozen tag strings on
    /// <see cref="LatticeMetrics"/>, so recording it allocates nothing.
    /// </summary>
    private string? _replayAdmissionRefusalArm;

    /// <summary>
    /// Counts a replay-admission refusal into the silo tally and, at most once per
    /// <see cref="ReplayAdmissionRefusalLogInterval"/>, writes the summary line.
    /// Never throws: it runs on the terminal path of the replay barrier, where an
    /// incidental failure would replace the refusal the caller needs to see.
    /// </summary>
    private void ReportReplayAdmissionRefusal(string? treeId)
    {
        try
        {
            if (!TryTakeReplayAdmissionRefusalLine(Stopwatch.GetTimestamp(), out var refusals, out var since))
                return;

            var logger = ResolveLogger();
            if (logger is null || !logger.IsEnabled(LogLevel.Warning))
                return;

            logger.LogWarning(
                "WAL replay admission refused {Refusals} leaf replay(s) on this silo since the previous line "
                + "({SincePreviousSeconds}s ago; 0 means the first), most recently leaf {GrainId} on tree {TreeId} "
                + "on the {Arm} arm. Gate: {Ceiling} permit(s), {QueuedWaiters} admitted waiter(s). Expected "
                + "backpressure, not a fault: the request fails with a retryable LatticeSaturatedException and "
                + "the next data operation re-arms the replay. The arms have opposite remedies: wait_exceeded "
                + "means queued waits are completing slowly, so too much is queued; no_progress means no permit "
                + "has been released to the queue for WalReplayPermitMaxQueueWait, so the replays holding the "
                + "permits are slow (read orleans.lattice.wal.replay.permit_hold against "
                + "orleans.lattice.wal.replay.permits_served), and raising WalMaterialiserMaxConcurrentReplays "
                + "on a store-bound silo makes it worse. Exact count per arm: "
                + "orleans.lattice.saturation.refusals (source replay_permit_admission, tag arm). At most one "
                + "line per {IntervalSeconds}s. See docs/lattice/configuration.md#walreplaypermitqueuedepthperpermit "
                + "and #walmaterialisermaxconcurrentreplays.",
                refusals,
                (long)since.TotalSeconds,
                context.GrainId,
                treeId,
                _replayAdmissionRefusalArm ?? "unknown",
                Volatile.Read(ref _replayConcurrencyCeiling),
                Volatile.Read(ref _queuedReplayPermitWaiters),
                (long)ReplayAdmissionRefusalLogInterval.TotalSeconds);
        }
        catch
        {
            // Intentionally swallowed. Losing the diagnostic is a bounded loss;
            // losing the refusal it describes is not.
        }
    }
}
