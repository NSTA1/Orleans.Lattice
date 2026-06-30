namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Classifies the transient exception Orleans' reminder service raises while it
/// is still initializing, and offers a bounded wait-out retry for callers that
/// register an <em>essential</em> reminder. Best-effort first-write bootstraps
/// use the classifier to defer registration instead of failing the user's write;
/// essential reminders (the atomic-write saga's crash-recovery keepalive) use
/// <see cref="RetryWhileInitializingAsync(Func{Task}, CancellationToken)"/> to
/// wait the startup window out rather than fail.
/// <para>
/// Orleans' <c>LocalReminderService</c> initializes asynchronously <em>after</em>
/// the silo reaches <c>Active</c> in the membership oracle: it reads the reminder
/// table and establishes range responsibility on a background path. A grain that
/// calls <c>RegisterOrUpdateReminder</c> inside that startup window blocks on the
/// service's internal <c>WaitForInitCompletion</c> and, if that wait elapses,
/// throws <c>OrleansException: "Reminder Service is still initializing and it is
/// taking a long time. Please retry again later."</c> (with an inner
/// <see cref="TimeoutException"/>). Orleans deliberately decouples reminder-service
/// init from silo readiness and exposes no public "reminders ready" signal, so the
/// service's own contract is to retry the transient - which is what the lazy
/// compaction / hot-shard-monitor bootstraps on the first write to a tree do.
/// </para>
/// </summary>
internal static class ReminderServiceReadiness
{
    /// <summary>
    /// Stable substring of the Orleans reminder-service "still initializing"
    /// exception message. Matched by substring (rather than exception type) for
    /// the same resilience-to-Orleans-internals reason the message-rejection
    /// classifier matches on a type-name string.
    /// </summary>
    internal const string StillInitializingMarker = "Reminder Service is still initializing";

    /// <summary>
    /// True when <paramref name="ex"/> is, or wraps anywhere in its inner chain,
    /// the transient "Reminder Service is still initializing" condition. A caller
    /// that treats a true result as retriable must be registering a non-essential
    /// (best-effort) reminder whose failure should not propagate to a user write.
    /// </summary>
    internal static bool IsStillInitializing(Exception? ex)
    {
        for (var cur = ex; cur is not null; cur = cur.InnerException)
        {
            if (cur.Message is { Length: > 0 } message
                && message.Contains(StillInitializingMarker, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Default inter-attempt backoff for
    /// <see cref="RetryWhileInitializingAsync(Func{Task}, CancellationToken)"/>.
    /// One entry per retry, so the attempt count is
    /// <c>Length + 1</c> (here, five total attempts). The cumulative wall-clock
    /// spent in backoff is ~11 s, comfortably under the Orleans response
    /// deadline; the reminder service typically finishes its async init within
    /// a few seconds of the silo reaching <c>Active</c>, so the first retry
    /// usually lands once the service is up.
    /// </summary>
    internal static readonly TimeSpan[] DefaultRegistrationBackoff =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(3),
        TimeSpan.FromSeconds(5),
    ];

    /// <summary>
    /// Invokes <paramref name="operation"/> (an essential reminder registration)
    /// and, when it fails with the transient "still initializing" condition,
    /// waits the corresponding <see cref="DefaultRegistrationBackoff"/> delay and
    /// retries. Any other exception propagates immediately, on the first attempt,
    /// without consuming a retry slot. When every attempt sees the transient, the
    /// most-recent exception is rethrown so a genuinely stuck reminder service
    /// still surfaces with its original shape.
    /// <para>
    /// Unlike the best-effort first-write bootstraps that
    /// <see cref="IsStillInitializing(Exception?)"/> lets defer (they re-attempt
    /// on a later write), the atomic-write saga's keepalive reminder is its
    /// crash-recovery anchor and has no natural re-attempt seam, so it waits the
    /// startup window out here rather than failing the user's atomic write.
    /// </para>
    /// </summary>
    /// <param name="operation">The reminder registration to invoke. Idempotent on
    /// retry (Orleans' <c>RegisterOrUpdateReminder</c> is an upsert).</param>
    /// <param name="cancellationToken">Honoured during the inter-attempt backoff;
    /// a cancellation observed inside <paramref name="operation"/> surfaces
    /// directly without consuming a retry attempt.</param>
    internal static Task RetryWhileInitializingAsync(
        Func<Task> operation,
        CancellationToken cancellationToken = default)
        => RetryWhileInitializingAsync(operation, DefaultRegistrationBackoff, cancellationToken);

    /// <summary>
    /// Backoff-injectable core of
    /// <see cref="RetryWhileInitializingAsync(Func{Task}, CancellationToken)"/>.
    /// The total attempt count is <c>backoffBetweenAttempts.Count + 1</c>; an
    /// empty list means a single attempt with no retry. Exposed with an explicit
    /// backoff so unit tests can drive the retry budget without real delays.
    /// </summary>
    internal static async Task RetryWhileInitializingAsync(
        Func<Task> operation,
        IReadOnlyList<TimeSpan> backoffBetweenAttempts,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(backoffBetweenAttempts);

        var maxAttempts = backoffBetweenAttempts.Count + 1;
        Exception? last = null;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
                return;
            }
            catch (Exception ex) when (IsStillInitializing(ex))
            {
                last = ex;
                if (attempt == maxAttempts) break;
                await Task.Delay(backoffBetweenAttempts[attempt - 1], cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        // Budget exhausted: rethrow the most-recent transient so a genuinely
        // stuck reminder service surfaces with the same shape it would have
        // without the envelope.
        throw last!;
    }
}
