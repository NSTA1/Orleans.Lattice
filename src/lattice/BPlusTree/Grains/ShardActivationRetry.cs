namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounded internal-retry envelope around a public <see cref="ILattice"/>
/// operator whose first turn drives the shard-root activation-readiness seed
/// (<c>ShardRootGrain.EnsureRootSlowWithDeadlineAsync</c>) and may therefore
/// surface <see cref="ShardActivationTimeoutException"/> during cold-start
/// races where the registry / root-leaf RPC is not yet visible.
/// <para>
/// The seed exception is by design retriable - every cross-grain step inside
/// the seed is idempotent on retry. Operator-facing code should not have to
/// learn that contract; this helper absorbs the typed exception for a small
/// bounded number of attempts before propagating, with a short backoff
/// between attempts so the next attempt lands against refreshed routing
/// rather than re-hitting the same parked activation immediately.
/// </para>
/// <para>
/// Retry shape: at most <see cref="MaxAttempts"/> (3) total attempts. The
/// <i>per-attempt</i> ceiling is the seed's own
/// <see cref="LatticeOptions.ActivationReadyTimeout"/> (default 15 s),
/// applied inside <c>EnsureRootSlowWithDeadlineAsync</c>; this helper does
/// not add a wall-clock timeout of its own. Backoff between attempts is
/// linear at 1 s, 2 s (after attempts 1 and 2). Worst-case wall on defaults
/// is approximately <c>3 x 15 s + 3 s = ~48 s</c>, comfortably under the
/// 3-minute Orleans response deadline that originally motivated the seed
/// bound.
/// </para>
/// <para>
/// <b>Scoping for the wider audit.</b> This helper is presently consumed by
/// <see cref="LatticeGrain.ReshardAsync"/> only - the observably-broken path
/// under the bench-startup pattern that motivated the fix. The wider audit
/// of operator entry points that should adopt the same envelope is tracked
/// separately on the issue tracker; this helper is the seam that audit work
/// will reuse rather than reinventing per-call-site.
/// </para>
/// </summary>
internal static class ShardActivationRetry
{
    /// <summary>
    /// Maximum number of attempts performed by <see cref="RunAsync"/>
    /// before the original <see cref="ShardActivationTimeoutException"/> is
    /// rethrown. Includes the first attempt, so the operator pays at most
    /// <c>MaxAttempts - 1</c> retries.
    /// </summary>
    internal const int MaxAttempts = 3;

    /// <summary>
    /// Backoff delays applied between attempts, in seconds. Index 0 is the
    /// wait after attempt 1 has thrown (before attempt 2); index 1 is the
    /// wait after attempt 2 has thrown (before attempt 3). The array length
    /// must be at least <c>MaxAttempts - 1</c>.
    /// </summary>
    private static readonly TimeSpan[] BackoffBetweenAttempts =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    /// <summary>
    /// Invokes <paramref name="operation"/> up to <see cref="MaxAttempts"/>
    /// times, absorbing <see cref="ShardActivationTimeoutException"/> on the
    /// first two failures and waiting the corresponding backoff before the
    /// next attempt. On exhausted budget the most-recent exception is
    /// rethrown so the caller sees the same exception type and message they
    /// would have seen without the envelope.
    /// </summary>
    /// <param name="operation">The grain RPC to invoke. Idempotent on retry
    /// by construction (the seed steps themselves are idempotent, and the
    /// operator's own steady-state path is retry-safe).</param>
    /// <param name="cancellationToken">Caller-supplied cancellation. Honoured
    /// between attempts (during the backoff <see cref="Task.Delay(TimeSpan, CancellationToken)"/>);
    /// a cancellation observed inside <paramref name="operation"/> itself
    /// surfaces directly to the caller without consuming a retry attempt.</param>
    internal static async Task RunAsync(Func<Task> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        ShardActivationTimeoutException? last = null;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
                return;
            }
            catch (ShardActivationTimeoutException ex)
            {
                last = ex;
                if (attempt == MaxAttempts) break;
                var backoff = BackoffBetweenAttempts[attempt - 1];
                await Task.Delay(backoff, cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        // Budget exhausted: rethrow the most-recent typed exception so
        // operators see the same shape they would have seen pre-envelope.
        throw last!;
    }

    /// <summary>
    /// Generic overload of <see cref="RunAsync(Func{Task}, CancellationToken)"/>
    /// for operations that produce a value. Same retry semantics; preserved as
    /// a distinct overload rather than wrapping the void path in a sentinel
    /// closure so the value path takes no extra closure allocation.
    /// </summary>
    internal static async Task<T> RunAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        ShardActivationTimeoutException? last = null;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                return await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
            catch (ShardActivationTimeoutException ex)
            {
                last = ex;
                if (attempt == MaxAttempts) break;
                var backoff = BackoffBetweenAttempts[attempt - 1];
                await Task.Delay(backoff, cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        throw last!;
    }
}
