namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bounded retry for saga decision registry mutators (issue #3501). A registry
/// write that fails rolls every mutation it carried back and surfaces a
/// <see cref="TxRegistryWriteFailedException"/>. After an optimistic-concurrency
/// conflict the registry has also deactivated, so the retry lands on a fresh
/// activation that reloads its row. Every registry mutator is idempotent (the
/// decision write-once guard, the participant set, forget), so re-issuing the
/// same call is safe. Any other exception, and the last attempt's failure,
/// propagate unchanged.
/// </summary>
internal static class TxRegistryWriteRetry
{
    /// <summary>The total number of attempts, the first included.</summary>
    internal const int MaxAttempts = 4;

    /// <summary>The backoff before the second attempt; it doubles each retry.</summary>
    internal static readonly TimeSpan BaseBackoff = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Runs <paramref name="operation"/>, retrying it on
    /// <see cref="TxRegistryWriteFailedException"/> up to
    /// <see cref="MaxAttempts"/> attempts in total. The state-passing shape keeps
    /// the hot path free of a per-call closure.
    /// </summary>
    /// <typeparam name="TState">The operation's argument bundle.</typeparam>
    /// <param name="state">The argument bundle handed to every attempt.</param>
    /// <param name="operation">The registry call to issue.</param>
    /// <returns>A task that completes when an attempt succeeds.</returns>
    internal static Task RunAsync<TState>(TState state, Func<TState, Task> operation)
    {
        var first = operation(state);
        return first.IsCompletedSuccessfully ? first : RetryAsync(first, state, operation);
    }

    /// <summary>
    /// Records <paramref name="txid"/>'s terminal decision on
    /// <paramref name="registry"/> under <see cref="RunAsync{TState}"/>.
    /// </summary>
    /// <param name="registry">The registry that owns the transaction.</param>
    /// <param name="txid">The transaction id.</param>
    /// <param name="committed">Whether the decision is commit (otherwise abort).</param>
    /// <returns>A task that completes once the decision is durable.</returns>
    internal static Task MarkDecisionAsync(ITxRegistryGrain registry, Guid txid, bool committed) =>
        RunAsync(
            (registry, txid, committed),
            static s => s.committed ? s.registry.MarkCommittedAsync(s.txid) : s.registry.MarkAbortedAsync(s.txid));

    private static async Task RetryAsync<TState>(Task first, TState state, Func<TState, Task> operation)
    {
        var attempt = 1;
        var pending = first;
        while (true)
        {
            try
            {
                await pending;
                return;
            }
            catch (TxRegistryWriteFailedException) when (attempt < MaxAttempts)
            {
                await Task.Delay(BaseBackoff * (1 << (attempt - 1)));
                attempt++;
                pending = operation(state);
            }
        }
    }
}
