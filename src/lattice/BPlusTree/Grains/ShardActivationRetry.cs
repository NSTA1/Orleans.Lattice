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
/// is approximately <c>3 x 15 s + 3 s = ~48 s</c>. That is under the
/// 3-minute response timeout the test cluster fixture configures, which is
/// what originally motivated the seed bound, but it exceeds the Orleans
/// default response timeout of 30 seconds, so on a host that keeps the
/// Orleans default a caller's await can time out before this envelope is
/// exhausted.
/// </para>
/// <para>
/// <b>Transient silo-membership churn.</b> In addition to the cold-start
/// seed timeout, this envelope also absorbs the transient faults a grain
/// RPC observes when its target activation's host is restarting, draining,
/// or has just left the cluster - Orleans' <c>SiloUnavailableException</c>
/// (the call landed on an activation whose silo is gone) and
/// <c>OrleansMessageRejectionException</c> (the runtime rejected a forward
/// to a deactivating grain). Both clear once the Orleans directory
/// re-places the activation on a live silo, so retrying under the same
/// idempotency contract turns a membership-convergence artifact into a
/// transparent reissue instead of surfacing it to the operator. Detection
/// is by type name (see <see cref="IsTransientSiloChurn"/>) to avoid a
/// compile-time dependency on the Orleans.Runtime types.
/// </para>
/// <para>
/// <b>Replay-permit back-pressure (issue #3294).</b> The envelope also
/// absorbs a <see cref="LatticeSaturatedException"/> whose
/// <see cref="LatticeSaturatedException.SaturationSource"/> is
/// <see cref="LatticeSaturationSource.ReplayPermitAdmission"/> - the per-silo
/// WAL replay permit gate refusing a leaf <em>activation</em> admission. That
/// refusal has no other retry anywhere: it aborts an activation, and an
/// activation that fails has no queue to park on and no policy of its own, so
/// without this arm the bound does not shed the request, it fails it.
/// </para>
/// <para>
/// It is deliberately the <b>only</b> saturation source retried here, and the
/// filter is on <see cref="LatticeSaturatedException.SaturationSource"/> rather than on
/// the exception type. Every other source refuses only after its wait budget
/// has already elapsed against a tree that has just reported it is full, so
/// retrying re-offers the same work into the regime that refused it. Issue
/// #3348 was exactly that: a generic handler treating a WAL <em>append</em>
/// refusal as retryable and re-fanning a whole batch across every shard. Both
/// paths run through this envelope, so a type-only filter here would
/// reintroduce #3348 one layer below where it was fixed.
/// </para>
/// <para>
/// Its backoff is a separate, longer, jittered ladder (2 s, 4 s +/- 25%)
/// rather than the seed ladder above. Longer because the documented recovery
/// for a saturation regime is 1-10 s, which the seed ladder's 3 s total does
/// not span; jittered because refusals at this seam are correlated by
/// construction - the callers were refused by one gate at one moment - so an
/// unjittered retry re-converges them into the thundering herd that the
/// admission bound of issue #3284 exists to remove.
/// </para>
/// <para>
/// <b>Saga state-write conflicts (issue #3572).</b> The envelope also
/// absorbs a <see cref="LatticeStateWriteFailedException"/> whose
/// <see cref="LatticeStateWriteFailedException.Conflict"/> is set: a saga
/// grain's state write lost an optimistic-concurrency (ETag) check, typically
/// because a storage-SDK transport retry landed the first attempt and the
/// retry then saw the changed ETag. The grain has already marked itself
/// conflicted and requested deactivation, so the retry, after the seed
/// ladder's backoff, lands on a fresh activation that reloads the row and
/// resumes idempotently from what is durable. A non-conflict state-write fault
/// is not retried here.
/// </para>
/// <para>
/// <b>Scoping.</b> This helper is consumed throughout the
/// <see cref="LatticeGrain"/> partials - bulk load, cursors, digests, entry
/// and key enumeration, orphan repair, projection administration, warm-up,
/// resharding, and the <c>SetManyAsync</c> write fan-out - rather than by a
/// single entry point. Because <see cref="RunAsync"/> is the one envelope
/// they all share, every arm described above applies to all of them,
/// including the replay-permit arm added for issue #3294. That breadth is
/// wanted here: the refusal is raised by the per-silo permit gate while a
/// leaf activation is being admitted, so it is reachable from any call that
/// must activate a leaf, and an arm confined to one call site would leave
/// the rest of that surface failing load it could have shed.
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
    /// Backoff delays applied between attempts that failed with a retryable
    /// replay-permit saturation refusal, before jitter. Indexed as
    /// <see cref="BackoffBetweenAttempts"/> is.
    /// <para>
    /// Longer than the seed ladder because the two are waiting for different
    /// things. The seed ladder waits for an activation to finish appearing,
    /// which is fast; this one waits for a saturated permit queue to drain,
    /// whose documented recovery is 1-10 seconds. At 1 s + 2 s the seed ladder
    /// would exhaust its whole budget inside the fastest recovery the regime
    /// admits, turning a retry that was supposed to shed load into three
    /// refusals in quick succession. 2 s + 4 s spans the lower half of that
    /// window and still leaves the worst case (about 6 s of backoff plus the
    /// attempts themselves) far inside the Orleans response deadline.
    /// </para>
    /// </summary>
    private static readonly TimeSpan[] SaturationBackoffBetweenAttempts =
    [
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(4),
    ];

    /// <summary>
    /// Proportional jitter applied to <see cref="SaturationBackoffBetweenAttempts"/>,
    /// as a fraction either side of the nominal delay (0.25 = +/-25%).
    /// </summary>
    private const double SaturationBackoffJitter = 0.25;

    /// <summary>
    /// True when <paramref name="ex"/> - or any exception in its inner chain -
    /// is a <see cref="LatticeSaturatedException"/> raised by the WAL replay
    /// permit admission gate, which is the one saturation refusal this
    /// envelope may safely retry (issue #3294).
    /// <para>
    /// The check is on <see cref="LatticeSaturatedException.SaturationSource"/>, never on
    /// the type alone. Every other source refuses after its wait budget has
    /// already elapsed, so retrying re-offers work into the regime that
    /// refused it; the WAL append refusal in particular re-fans an entire
    /// batch across every shard, which is issue #3348. Both reach this
    /// envelope, so the discriminator is what keeps the two fixes from
    /// undoing each other.
    /// </para>
    /// <para>
    /// The walk stops at the first <see cref="LatticeSaturatedException"/> it
    /// finds rather than searching the chain for a retryable one: the
    /// outermost saturation is the refusal that actually describes what
    /// happened, and treating a nested one as authoritative would let an
    /// inner, already-handled refusal license a retry of an outer refusal that
    /// forbids it.
    /// </para>
    /// </summary>
    internal static bool IsRetryableSaturation(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is LatticeSaturatedException saturated)
                return saturated.SaturationSource == LatticeSaturationSource.ReplayPermitAdmission;
        }

        return false;
    }

    /// <summary>
    /// Selects the backoff preceding the next attempt: the jittered saturation
    /// ladder when <paramref name="ex"/> was a retryable replay-permit
    /// refusal, otherwise the plain seed ladder.
    /// </summary>
    private static TimeSpan NextBackoff(Exception ex, int attempt)
    {
        if (!IsRetryableSaturation(ex))
            return BackoffBetweenAttempts[attempt - 1];

        var nominal = SaturationBackoffBetweenAttempts[attempt - 1];

        // Random.Shared is thread-safe and allocation-free here. The jitter is
        // symmetric about the nominal delay, so the ladder's expected total is
        // unchanged and only the correlation between refused callers is broken.
        var factor = 1.0 + ((Random.Shared.NextDouble() * 2.0 - 1.0) * SaturationBackoffJitter);
        return nominal * factor;
    }

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

        Exception? last = null;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
                return;
            }
            catch (Exception ex) when (
                ex is ShardActivationTimeoutException
                || IsTransientSiloChurn(ex)
                || IsRetryableSaturation(ex)
                || GrainStateWriteFaults.IsTranslatedConflict(ex))
            {
                last = ex;
                if (attempt == MaxAttempts) break;
                var backoff = NextBackoff(ex, attempt);
                await Task.Delay(backoff, cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        // Budget exhausted: rethrow the most-recent exception so operators
        // see the same shape they would have seen pre-envelope.
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

        Exception? last = null;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                return await operation().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
            catch (Exception ex) when (
                ex is ShardActivationTimeoutException
                || IsTransientSiloChurn(ex)
                || IsRetryableSaturation(ex)
                || GrainStateWriteFaults.IsTranslatedConflict(ex))
            {
                last = ex;
                if (attempt == MaxAttempts) break;
                var backoff = NextBackoff(ex, attempt);
                await Task.Delay(backoff, cancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            }
        }

        throw last!;
    }

    /// <summary>
    /// True when <paramref name="ex"/> - or any exception in its inner
    /// chain - is one of the transient silo-membership-churn faults a grain
    /// RPC can observe when its target activation's host is restarting,
    /// draining, or has just left the cluster: Orleans'
    /// <c>SiloUnavailableException</c> (the call landed on an activation
    /// whose silo is gone) and <c>OrleansMessageRejectionException</c> (the
    /// runtime rejected a forward to a deactivating grain). Both clear once
    /// the Orleans directory re-places the activation on a live silo, so the
    /// operation is safe to retry under the same idempotency contract as the
    /// cold-start seed timeout. Matched by type name - one of the Orleans
    /// types is internal - mirroring the detection the atomic-write saga
    /// coordinator already uses for the deactivation-race rejection shape.
    /// Lattice's own <see cref="ShardRootDeactivatingException"/> is the same
    /// condition raised one hop earlier, by a shard root that refused a point
    /// write because it had already requested its own deactivation (#812).
    /// </summary>
    internal static bool IsTransientSiloChurn(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is ShardRootDeactivatingException)
            {
                return true;
            }

            var typeName = e.GetType().Name;
            if (typeName.Contains("SiloUnavailableException", StringComparison.Ordinal)
                || typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
