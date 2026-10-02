namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Idempotency-key + retry-policy plumbing for the public
/// <see cref="ILattice"/> mutating entry-points. Both layers are
/// strictly additive: callers gate entry into these helpers on
/// <see cref="LatticeIdempotencyContext.IsActive"/> so the no-scope
/// cold path bypasses the closure / state-machine cost entirely.
/// Origin stamping is intentionally NOT derived from the
/// idempotency key - it is resolved exclusively by
/// <see cref="LatticeOriginContext"/> /
/// <see cref="ILatticeOriginClusterIdResolver"/> at the silo, so
/// callers cannot misroute loop-suppression or per-origin merge
/// resolution.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Runs <paramref name="operation"/> under the ambient idempotency
    /// scope and the configured retry policy (if any). Establishes
    /// <see cref="LatticeHlcOverrideContext"/> from
    /// <see cref="LatticeIdempotencyContext.Current"/>.<see cref="LatticeIdempotencyKey.Timestamp"/>
    /// so the leaf grain's existing stamping path picks up the key's
    /// HLC via the standard ambient mechanism. Callers MUST check
    /// <see cref="LatticeIdempotencyContext.IsActive"/> before calling
    /// this helper - the no-scope fast path is the caller's
    /// responsibility so it can avoid the closure allocation.
    /// </summary>
    private async Task RunMutationAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
    {
        var key = LatticeIdempotencyContext.Current;
        if (key is null)
        {
            // Defensive: callers should gate on IsActive, but if the
            // scope was cleared between the check and this call we
            // still degrade to a direct await.
            await operation(cancellationToken);
            return;
        }

        var policy = Options.RetryPolicy;
        using var hlcScope = LatticeHlcOverrideContext.Current is null
            ? LatticeHlcOverrideContext.With(key.Value.Timestamp)
            : NullScope.Instance;

        if (policy is null)
        {
            await operation(cancellationToken);
            return;
        }

        var turn = TaskScheduler.Current;
        await policy.ExecuteAsync(ct => OnTurn(turn, operation, ct), cancellationToken);
    }

    /// <summary>
    /// Typed sibling of <see cref="RunMutationAsync(Func{CancellationToken, Task}, CancellationToken)"/>
    /// for entry-points that return a value (e.g. <c>DeleteRangeAsync</c>'s
    /// deleted count, <c>SetIfVersionAsync</c>'s applied bit,
    /// <c>GetOrSetAsync</c>'s prior value). Callers must gate entry on
    /// <see cref="LatticeIdempotencyContext.IsActive"/> for the same
    /// reason.
    /// </summary>
    private async Task<T> RunMutationAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        var key = LatticeIdempotencyContext.Current;
        if (key is null)
        {
            return await operation(cancellationToken);
        }

        var policy = Options.RetryPolicy;
        using var hlcScope = LatticeHlcOverrideContext.Current is null
            ? LatticeHlcOverrideContext.With(key.Value.Timestamp)
            : NullScope.Instance;

        if (policy is null)
        {
            return await operation(cancellationToken);
        }

        var turn = TaskScheduler.Current;
        return await policy.ExecuteAsync(ct => OnTurn(turn, operation, ct), cancellationToken);
    }

    /// <summary>
    /// Runs one attempt the retry policy drives on <paramref name="turn"/>, the
    /// activation scheduler captured when the mutation began. A policy is free to
    /// resume with <c>ConfigureAwait(false)</c> - the shipped
    /// <see cref="BoundedExponentialRetryPolicy"/> does, after its back-off delay -
    /// so a retry would otherwise run the grain's own mutation code on a thread-pool
    /// thread, outside the activation's turn and in parallel with any interleaved
    /// request the activation admits meanwhile. Re-entering the captured scheduler
    /// keeps every attempt inside the turn whatever the policy does; an attempt
    /// already on it (the first, in the common case) runs inline.
    /// </summary>
    private static Task OnTurn(TaskScheduler turn, Func<CancellationToken, Task> operation, CancellationToken cancellationToken) =>
        TaskScheduler.Current == turn
            ? operation(cancellationToken)
            : Task.Factory.StartNew(
                () => operation(cancellationToken),
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                turn).Unwrap();

    /// <summary>
    /// Typed sibling of <see cref="OnTurn(TaskScheduler, Func{CancellationToken, Task}, CancellationToken)"/>.
    /// </summary>
    private static Task<T> OnTurn<T>(TaskScheduler turn, Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken) =>
        TaskScheduler.Current == turn
            ? operation(cancellationToken)
            : Task.Factory.StartNew(
                () => operation(cancellationToken),
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                turn).Unwrap();

    private sealed class NullScope : IDisposable
    {
        internal static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}
