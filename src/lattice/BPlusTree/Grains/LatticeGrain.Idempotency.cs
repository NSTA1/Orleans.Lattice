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
        var ownsStamp = LatticeHlcOverrideContext.Current is null;
        using var hlcScope = ownsStamp
            ? LatticeHlcOverrideContext.With(key.Value.Timestamp)
            : NoOpDisposable.Instance;
        // Issue #4586: the key's stamp is minted for this operation, so a
        // replicated tree's WAL clock floor governs it, and a refusal - which
        // cannot be re-stamped without breaking the key's contract - surfaces as
        // a typed expiry.
        using var freshScope = ownsStamp ? LatticeFreshStampContext.Begin() : NoOpDisposable.Instance;
        var keyStamp = key.Value.Timestamp;
        Func<CancellationToken, Task> guarded = ownsStamp
            ? ct => GuardIdempotencyKeyExpiryAsync(operation, keyStamp, ct)
            : operation;

        if (policy is null)
        {
            await guarded(cancellationToken);
            return;
        }

        var turn = TaskScheduler.Current;
        await policy.ExecuteAsync(ct => OnTurn(turn, guarded, ct), cancellationToken);
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
        var ownsStamp = LatticeHlcOverrideContext.Current is null;
        using var hlcScope = ownsStamp
            ? LatticeHlcOverrideContext.With(key.Value.Timestamp)
            : NoOpDisposable.Instance;
        using var freshScope = ownsStamp ? LatticeFreshStampContext.Begin() : NoOpDisposable.Instance;
        var keyStamp = key.Value.Timestamp;
        Func<CancellationToken, Task<T>> guarded = ownsStamp
            ? ct => GuardIdempotencyKeyExpiryAsync(operation, keyStamp, ct)
            : operation;

        if (policy is null)
        {
            return await guarded(cancellationToken);
        }

        var turn = TaskScheduler.Current;
        return await policy.ExecuteAsync(ct => OnTurn(turn, guarded, ct), cancellationToken);
    }

    /// <summary>
    /// Runs one attempt of an idempotency-keyed mutation, turning a replicated
    /// tree's WAL clock-floor refusal of the key's stamp into
    /// <see cref="LatticeIdempotencyKeyExpiredException"/> (issue #4586). The
    /// stamp is the key's by contract and cannot be renewed, so the refusal is
    /// deterministic for this key.
    /// </summary>
    private static async Task GuardIdempotencyKeyExpiryAsync(
        Func<CancellationToken, Task> operation, HybridLogicalClock keyStamp, CancellationToken cancellationToken)
    {
        try
        {
            await operation(cancellationToken);
        }
        catch (WalStampBelowFloorException refusal)
        {
            throw new LatticeIdempotencyKeyExpiredException(keyStamp, refusal.Floor, refusal);
        }
    }

    /// <summary>Typed sibling of <see cref="GuardIdempotencyKeyExpiryAsync(Func{CancellationToken, Task}, HybridLogicalClock, CancellationToken)"/>.</summary>
    private static async Task<T> GuardIdempotencyKeyExpiryAsync<T>(
        Func<CancellationToken, Task<T>> operation, HybridLogicalClock keyStamp, CancellationToken cancellationToken)
    {
        try
        {
            return await operation(cancellationToken);
        }
        catch (WalStampBelowFloorException refusal)
        {
            throw new LatticeIdempotencyKeyExpiredException(keyStamp, refusal.Floor, refusal);
        }
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
}
