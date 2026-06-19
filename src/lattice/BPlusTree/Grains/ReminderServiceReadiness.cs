namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Classifies the transient exception Orleans' reminder service raises while it
/// is still initializing, so a best-effort first-write reminder bootstrap can
/// defer registration instead of failing the user's write.
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
}
