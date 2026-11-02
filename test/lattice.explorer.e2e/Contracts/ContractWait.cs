using Grpc.Core;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// Bounded waits for the estate's asynchronous state: a background operation reaching a
/// terminal state, a projection catching up, a peer region converging. Every wait has a
/// deadline and names what it waited for, so a seeder never hangs and never passes silently.
/// </summary>
public static class ContractWait
{
    /// <summary>The default deadline for a wait.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(3);

    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(500);

    /// <summary>Waits until <paramref name="condition"/> holds, retrying a transient gRPC failure as not yet.</summary>
    /// <param name="what">What is awaited, for the timeout's message.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="timeout">The deadline; <see cref="DefaultTimeout"/> by default.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <exception cref="TimeoutException">The condition did not hold in time.</exception>
    public static async Task UntilAsync(string what, Func<Task<bool>> condition, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(condition);
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        Exception? last = null;
        while (true)
        {
            var attempt = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                if (await condition())
                {
                    return;
                }
            }
            catch (RpcException ex) when (IsTransient(ex.StatusCode))
            {
                last = ex;
                ContractRetries.Record(what, attempt.Elapsed, $"{ex.StatusCode}: {ex.Status.Detail}");
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(
                    $"Timed out after {(timeout ?? DefaultTimeout).TotalSeconds:0} seconds waiting for {what}." +
                    (last is null ? string.Empty : $" Last transient failure: {last.Message}"));
            }

            await Task.Delay(Poll, cancellationToken);
        }
    }

    /// <summary>
    /// Waits until <paramref name="condition"/> holds while the awaited work keeps advancing: the
    /// wait fails when <paramref name="progress"/> has not changed for <paramref name="stall"/>,
    /// or when <paramref name="ceiling"/> has elapsed in all, so work whose length scales with its
    /// size is judged by whether it moves, never by one fixed deadline. A transient gRPC failure
    /// counts as not yet.
    /// </summary>
    /// <param name="what">What is awaited, for the timeout's message.</param>
    /// <param name="condition">The condition; it also refreshes what <paramref name="progress"/> reads.</param>
    /// <param name="progress">The work's progress as last observed; any change counts as an advance.</param>
    /// <param name="stall">How long the work may go without advancing.</param>
    /// <param name="ceiling">The absolute deadline, however steadily the work advances.</param>
    /// <param name="describe">The progress so far, for the timeout's message.</param>
    /// <exception cref="TimeoutException">The work stalled, or ran past the ceiling.</exception>
    public static async Task UntilAdvancingAsync(
        string what,
        Func<Task<bool>> condition,
        Func<long> progress,
        TimeSpan stall,
        TimeSpan ceiling,
        Func<string> describe)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentNullException.ThrowIfNull(progress);
        ArgumentNullException.ThrowIfNull(describe);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var advancedAt = TimeSpan.Zero;
        var observed = progress();
        Exception? last = null;
        while (true)
        {
            var attempt = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                if (await condition())
                {
                    return;
                }
            }
            catch (RpcException ex) when (IsTransient(ex.StatusCode))
            {
                last = ex;
                ContractRetries.Record(what, attempt.Elapsed, $"{ex.StatusCode}: {ex.Status.Detail}");
            }

            var now = clock.Elapsed;
            if (progress() is var current && current != observed)
            {
                observed = current;
                advancedAt = now;
            }

            var failure = now - advancedAt > stall
                ? $"{what} made no progress for {stall.TotalSeconds:0} seconds"
                : now > ceiling
                    ? $"{what} was still advancing after the {ceiling.TotalSeconds:0}-second ceiling"
                    : null;
            if (failure is not null)
            {
                throw new TimeoutException(
                    $"Timed out after {now.TotalSeconds:0} seconds: {failure}. Progress: {describe()}." +
                    (last is null ? string.Empty : $" Last transient failure: {last.Message}"));
            }

            await Task.Delay(Poll);
        }
    }

    /// <summary>Polls a tracked operation until it is terminal, and requires it to have succeeded.</summary>
    /// <param name="what">The operation, for the failure message.</param>
    /// <param name="status">Reads the operation's status.</param>
    /// <param name="timeout">The deadline; <see cref="DefaultTimeout"/> by default.</param>
    /// <returns>The succeeded operation's status.</returns>
    /// <exception cref="InvalidOperationException">The operation failed or was cancelled.</exception>
    public static async Task<LatticeOperationStatus> SucceededAsync(string what, Func<Task<LatticeOperationStatus?>> status, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(status);
        LatticeOperationStatus? last = null;
        await UntilAsync(what, async () =>
        {
            last = await status();
            return last is { IsTerminal: true };
        }, timeout);

        return last!.State == LatticeOperationState.Succeeded
            ? last
            : throw new InvalidOperationException($"{what} ended {last.State} in phase '{last.Phase}': {last.FailureReason}");
    }

    /// <summary>Runs <paramref name="call"/>, retrying a transient gRPC failure until the deadline.</summary>
    /// <typeparam name="T">The call's result.</typeparam>
    /// <param name="what">The call, for the timeout's message.</param>
    /// <param name="call">The call.</param>
    /// <param name="timeout">The deadline; <see cref="DefaultTimeout"/> by default.</param>
    public static async Task<T> RetryingAsync<T>(string what, Func<Task<T>> call, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(call);
        T result = default!;
        await UntilAsync(what, async () =>
        {
            result = await call();
            return true;
        }, timeout);
        return result;
    }

    /// <summary>Whether a gRPC status is one a retry may clear: the estate settling, not a refusal.</summary>
    /// <param name="code">The status code.</param>
    public static bool IsTransient(StatusCode code) =>
        code is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Internal or StatusCode.Aborted or StatusCode.ResourceExhausted;
}
