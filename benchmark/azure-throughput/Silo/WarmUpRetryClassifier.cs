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

    /// <summary>
    /// True when <paramref name="ex"/> is, or wraps anywhere in its inner
    /// chain, an Orleans placement failure raised because no silo is yet
    /// compatible with the target grain - the "no active nodes are compatible
    /// with grain" condition thrown by <c>PlacementService.GetCompatibleSilos</c>
    /// (via <c>StatelessWorkerDirector.OnAddActivation</c> for the
    /// local-placed <c>LatticeGrain</c>).
    /// <para>
    /// This is a startup-ordering race, not a genuine misconfiguration: the
    /// silo's proactive warm-up (and, when a shard override is set, its reshard
    /// submit) is the very first grain call, and it can fire in the brief window
    /// after the host has begun executing hosted <c>BackgroundService</c>s but
    /// before local membership has marked this silo Active and published its
    /// grain manifest - so placement transiently sees zero compatible silos
    /// ("Known nodes with grain type: none"). Retrying a few hundred
    /// milliseconds later succeeds once membership converges.
    /// </para>
    /// <para>
    /// Before Orleans 10.2.2 the early call was tolerated and this exception did
    /// not surface; on 10.2.2 it does, so without classifying it the warm-up
    /// loop drops to its fatal branch and - because the silo runs with
    /// <c>BackgroundServiceExceptionBehavior.StopHost</c> - the whole host
    /// aborts, producing a spurious WEDGE (no FINAL emitted) on every cohort.
    /// </para>
    /// <para>
    /// Matching is by the stable message fragment rather than the concrete
    /// exception type (which is a bare <c>OrleansException</c>) so it is robust
    /// to the grain/interface names embedded in the message. A genuinely
    /// unregistered grain would carry the same message, but retrying it is safe:
    /// it simply exhausts the bounded attempt budget and still aborts loudly,
    /// merely delayed - strictly better than an instant host core-dump on a
    /// self-healing race. The shutdown-vs-transient distinction remains the
    /// caller's responsibility (a <c>stopping-token</c> guard).
    /// </para>
    /// </summary>
    public static bool IsTransientPlacementConvergence(Exception? ex)
    {
        for (var cur = ex; cur is not null; cur = cur.InnerException)
        {
            var message = cur.Message;
            if (!string.IsNullOrEmpty(message)
                && message.Contains("No active nodes are compatible with grain", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
