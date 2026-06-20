/// <summary>
/// Classifies exceptions thrown by the silo's proactive shard warm-up loop
/// so a transient, self-healing failure can be retried instead of aborting
/// the whole host. Extracted as a <c>public static</c> helper (mirroring
/// <see cref="BenchWorkloadDispatcher"/>) so the classification is
/// independently unit-testable without exposing <c>Program</c>'s private
/// helpers via <c>[InternalsVisibleTo]</c>.
/// </summary>
public static class WarmUpRetryClassifier
{
    /// <summary>
    /// True when <paramref name="ex"/> is, or wraps anywhere in its inner
    /// chain, an <see cref="OperationCanceledException"/> - which includes
    /// <see cref="TaskCanceledException"/>.
    /// <para>
    /// A shard warm-up probe activates the shard root, which can cascade into
    /// a grain-state read against a transiently-throttled storage account. When
    /// that read exceeds the activation deadline Orleans surfaces a bare
    /// <see cref="TaskCanceledException"/> out of <c>ActivationData.ActivateAsync</c>.
    /// Because the silo's warm-up runs inside a <c>BackgroundService</c> with
    /// <c>BackgroundServiceExceptionBehavior.StopHost</c>, letting that
    /// cancellation escape kills the entire silo and produces a spurious WEDGE
    /// (no FINAL emitted) on re-runs - even though retrying a few hundred
    /// milliseconds later succeeds once the throttle window clears (issue #821).
    /// </para>
    /// <para>
    /// This predicate intentionally does NOT distinguish a genuine
    /// host-shutdown cancellation from a transient activation timeout: both
    /// surface as <see cref="OperationCanceledException"/>. The caller is
    /// responsible for excluding the shutdown case first (typically via a
    /// <c>!stoppingToken.IsCancellationRequested</c> guard) so that a real
    /// shutdown is still honoured immediately rather than retried.
    /// </para>
    /// </summary>
    public static bool IsTransientActivationCancellation(Exception? ex)
    {
        for (var cur = ex; cur is not null; cur = cur.InnerException)
        {
            if (cur is OperationCanceledException)
            {
                return true;
            }
        }

        return false;
    }
}
