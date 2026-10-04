using System.Diagnostics;

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
/// <para>
/// A decision refused by a snapshot capture's decision gate
/// (<see cref="TxDecisionGateRefusal.DecisionGated"/>, issue #4485) is waited
/// out and re-issued, bounded by <see cref="MaxGatedWait"/>. A registration
/// refused by a backup set's fence
/// (<see cref="TxDecisionGateRefusal.RegistrationFenced"/>) is NOT retried: the
/// cross-tree sub-saga fails its prepare so its coordinator aborts, which is
/// what lets the set's drain terminate.
/// </para>
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
    internal static Task RunAsync<TState>(TState state, Func<TState, Task> operation) =>
        RunAsync(state, operation, waitOutDecisionGate: true);

    /// <summary>
    /// <see cref="RunAsync{TState}(TState, Func{TState, Task})"/>, choosing
    /// whether a decision-gate refusal is waited out (the saga grains) or
    /// propagated to the caller (the replication apply path, which defers the
    /// replicated entry instead of parking a stateless worker, issue #4485).
    /// </summary>
    /// <typeparam name="TState">The operation's argument bundle.</typeparam>
    /// <param name="state">The argument bundle handed to every attempt.</param>
    /// <param name="operation">The registry call to issue.</param>
    /// <param name="waitOutDecisionGate">Whether to wait out a decision-gate refusal.</param>
    /// <returns>A task that completes when an attempt succeeds.</returns>
    internal static Task RunAsync<TState>(TState state, Func<TState, Task> operation, bool waitOutDecisionGate)
    {
        var first = operation(state);
        return first.IsCompletedSuccessfully ? first : RetryAsync(first, state, operation, waitOutDecisionGate);
    }

    /// <summary>
    /// Records <paramref name="txid"/>'s terminal decision on
    /// <paramref name="registry"/> under <see cref="RunAsync{TState}"/>.
    /// </summary>
    /// <param name="registry">The registry that owns the transaction.</param>
    /// <param name="txid">The transaction id.</param>
    /// <param name="committed">Whether the decision is commit (otherwise abort).</param>
    /// <returns>A task that completes once the decision is durable.</returns>
    /// <param name="waitOutDecisionGate">
    /// Whether to wait out a snapshot capture's decision gate (the default), or
    /// propagate its <see cref="TxDecisionGateRefusedException"/> so a
    /// replication apply defers the entry (issue #4485).
    /// </param>
    internal static Task MarkDecisionAsync(ITxRegistryGrain registry, Guid txid, bool committed, bool waitOutDecisionGate = true) =>
        RunAsync(
            (registry, txid, committed),
            static s => s.committed ? s.registry.MarkCommittedAsync(s.txid) : s.registry.MarkAbortedAsync(s.txid),
            waitOutDecisionGate);

    private static async Task RetryAsync<TState>(Task first, TState state, Func<TState, Task> operation, bool waitOutDecisionGate)
    {
        var attempt = 1;
        var pending = first;
        var gatedSince = 0L;
        var gatedDelay = GatedBaseDelay;
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
            catch (TxDecisionGateRefusedException ex) when (
                waitOutDecisionGate
                && ex.Refusal == TxDecisionGateRefusal.DecisionGated
                && (gatedSince == 0 || Stopwatch.GetElapsedTime(gatedSince) < MaxGatedWait))
            {
                // Issue #4485: a snapshot capture holds the decision gate. The
                // decision is deferred, not failed: wait for the capture to
                // release the gate (or for its lease to lapse) and re-issue the
                // same idempotent call. Bounded so a capture that never lets go
                // surfaces as a failure rather than an indefinite stall.
                if (gatedSince == 0) gatedSince = Stopwatch.GetTimestamp();
                var hint = TimeSpan.FromMilliseconds(Math.Max(1, ex.RetryAfterMilliseconds));
                await Task.Delay(hint < gatedDelay ? hint : gatedDelay);
                if (gatedDelay < GatedMaxDelay) gatedDelay *= 2;
                pending = operation(state);
            }
        }
    }

    /// <summary>The first wait after a decision-gate refusal; it doubles each retry.</summary>
    internal static readonly TimeSpan GatedBaseDelay = TimeSpan.FromMilliseconds(20);

    /// <summary>The longest single wait between decision-gate retries.</summary>
    internal static readonly TimeSpan GatedMaxDelay = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// How long a decision may wait on capture gates in total before the refusal
    /// propagates. A capture renews its gate only while it is alive, so a
    /// crashed capture's gate lapses well within this.
    /// </summary>
    internal static readonly TimeSpan MaxGatedWait = TimeSpan.FromMinutes(5);
}
