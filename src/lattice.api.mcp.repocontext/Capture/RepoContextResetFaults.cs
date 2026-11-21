using System.Data.Common;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Classifies and retries the transient faults a <c>repocontext_reset_index</c>
/// sweep meets while background maintenance runs on the trees it is draining
/// (#5182).
/// <para>
/// Two fault classes used to abort the whole reset although neither is a
/// property of the request. A <see cref="LeafRetiredException"/> surfaces when the
/// range delete reaches a leaf that empty-leaf reclaim retired between the route
/// and the mutation; a fresh range delete re-resolves the route against the
/// current topology and makes progress. A storage provider's transient fault (for
/// example SQLite <c>SQLITE_BUSY</c> / <c>SQLITE_LOCKED</c>, "database is
/// locked") surfaces when background compaction or snapshot capture holds the
/// store; it clears once that work yields.
/// </para>
/// <para>
/// The range delete resumes by key, so re-running it after either fault is
/// idempotent: entries already tombstoned are not revisited.
/// </para>
/// </summary>
internal static class RepoContextResetFaults
{
    /// <summary>How many times a sweep step is attempted before the fault is surfaced.</summary>
    internal const int MaxAttempts = 6;

    /// <summary>The back-off before the first retry; doubled on each subsequent retry.</summary>
    internal static readonly TimeSpan InitialBackoff = TimeSpan.FromMilliseconds(200);

    /// <summary>The ceiling on a single back-off.</summary>
    internal static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Whether <paramref name="ex"/>, or any exception it wraps, is a transient
    /// fault a reset sweep should retry rather than surface.
    /// </summary>
    /// <param name="ex">The fault to classify.</param>
    /// <returns><see langword="true"/> for a retired leaf or a transient storage fault.</returns>
    internal static bool IsTransient(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is LeafRetiredException or DbException { IsTransient: true } || IsStoreLocked(current))
            {
                return true;
            }

            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsTransient(inner))
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        return false;
    }

    // A provider that does not mark busy/locked as transient (or a fault that
    // crossed a grain boundary and lost its concrete type) still carries the
    // SQLite message text, so match it as a fallback.
    private static bool IsStoreLocked(Exception ex) =>
        ex.Message.Contains("database is locked", StringComparison.OrdinalIgnoreCase)
        || ex.Message.Contains("database table is locked", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The back-off before retry number <paramref name="retry"/> (1-based):
    /// exponential from <see cref="InitialBackoff"/>, capped at <see cref="MaxBackoff"/>.
    /// </summary>
    /// <param name="retry">The 1-based retry number.</param>
    /// <returns>The delay to wait before that retry.</returns>
    internal static TimeSpan BackoffFor(int retry)
    {
        var ticks = InitialBackoff.Ticks << Math.Min(Math.Max(retry - 1, 0), 16);
        return ticks >= MaxBackoff.Ticks ? MaxBackoff : TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// Runs <paramref name="step"/>, retrying it with back-off while it fails with
    /// a transient fault, and returning what the successful attempt reports.
    /// Any other fault, cancellation included, propagates immediately; a transient
    /// fault that outlasts <paramref name="maxAttempts"/> propagates unchanged.
    /// </summary>
    /// <param name="step">The idempotent sweep step to run.</param>
    /// <param name="delay">Waits the given back-off; injected so tests need no wall clock.</param>
    /// <param name="maxAttempts">The total number of attempts, at least one.</param>
    /// <param name="cancellationToken">Cancels between attempts.</param>
    /// <typeparam name="T">The result of a step.</typeparam>
    /// <returns>The result of the successful attempt.</returns>
    internal static async Task<T> RetryTransientAsync<T>(
        Func<CancellationToken, Task<T>> step,
        Func<TimeSpan, CancellationToken, Task> delay,
        int maxAttempts,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(step);
        ArgumentNullException.ThrowIfNull(delay);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await step(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < maxAttempts && ex is not OperationCanceledException && IsTransient(ex))
            {
                await delay(BackoffFor(attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
