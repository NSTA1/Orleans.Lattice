using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Tracking wrapper around <c>ForwardShadowAsync</c> that guarantees every
/// dispatched shadow-forward task is observed - either synchronously by the
/// calling mutation (happy path) or asynchronously by a fault-logging
/// continuation (retry-loop abandonment).
/// <para>
/// Closes a hygiene gap in the single-key mutation paths (<c>SetAsync</c>,
/// <c>SetAsync(ttl)</c>, <c>GetOrSetAsync</c>, <c>DeleteAsync</c>): each
/// retry iteration dispatches a fresh forward task, and if the local write
/// fails transiently before the caller awaits <c>forwardTask</c>, the prior
/// iteration's forward is abandoned. Without observation, a fault in that
/// abandoned task would surface only via
/// <see cref="TaskScheduler.UnobservedTaskException"/>, bypassing the
/// grain's normal error-handling path. LWW guarantees the destination
/// still converges; this type exists so faults are never silent.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Dispatches a shadow-forward task via <see cref="ForwardShadowAsync"/>
    /// and attaches a fault-logging continuation so the task is observed even
    /// if the caller abandons it (e.g. during a transient-exception retry).
    /// Returns the underlying task so happy-path callers may <c>await</c> it
    /// and surface forward failures alongside local ones.
    /// </summary>
    private Task TrackShadowForward(Func<IShardRootGrain, Task> forwardAction)
    {
        var task = ForwardShadowAsync(forwardAction);
        // Fast path: forwarding inactive (target is null) or already completed
        // synchronously - no observation needed.
        if (task.IsCompleted) { _ = task.Exception; return task; }

        // Attach an OnlyOnFaulted continuation that accesses t.Exception,
        // marking any fault as observed. When the caller also awaits the
        // task (happy / retry-succeeded paths), they observe the exception
        // first; the continuation still fires but is redundant. When the
        // caller abandons the task (retry-loop prior iteration), the
        // continuation is the sole observer - the fault is logged once.
        _ = task.ContinueWith(
            static (t, state) =>
            {
                var logger = (ILogger)state!;
                var ex = t.Exception?.GetBaseException();
                logger.LogWarning(ex,
                    "Shadow-forward task faulted outside of its caller''s await path; " +
                    "LWW on the destination will still converge, but the fault is reported here for diagnostics.");
            },
            state: logger,
            cancellationToken: default,
            continuationOptions: TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            scheduler: TaskScheduler.Default);

        return task;
    }
}

