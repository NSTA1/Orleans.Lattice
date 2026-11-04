using System.Runtime.CompilerServices;
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
    /// <exception cref="TimeoutException">
    /// The operation was not terminal in time; the message carries the last status read, so a
    /// stuck operation and a slow one can be told apart.
    /// </exception>
    public static async Task<LatticeOperationStatus> SucceededAsync(string what, Func<Task<LatticeOperationStatus?>> status, TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(status);
        LatticeOperationStatus? last = null;
        try
        {
            await UntilAsync(what, async () =>
            {
                last = await status();
                return last is { IsTerminal: true };
            }, timeout);
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"{ex.Message} Last status: {DescribeStatus(last)}.", ex);
        }

        return last!.State == LatticeOperationState.Succeeded
            ? last
            : throw new InvalidOperationException($"{what} ended {last.State} in phase '{last.Phase}': {last.FailureReason}");
    }

    /// <summary>
    /// Polls a tracked operation until it is terminal while it keeps advancing, and requires it to
    /// have succeeded: the wait fails only when the operation's phase and completed units have not
    /// moved for <paramref name="stall"/>, or when <paramref name="ceiling"/> has elapsed, and
    /// reports the trajectory it observed.
    /// </summary>
    /// <param name="what">The operation, for the failure message.</param>
    /// <param name="status">Reads the operation's status.</param>
    /// <param name="stall">How long the operation may go without advancing.</param>
    /// <param name="ceiling">The absolute deadline.</param>
    /// <param name="last">Receives the last status read, also when the wait times out.</param>
    /// <returns>The succeeded operation's status.</returns>
    /// <exception cref="InvalidOperationException">The operation failed or was cancelled.</exception>
    /// <exception cref="TimeoutException">The operation stalled, or ran past the ceiling.</exception>
    public static async Task<LatticeOperationStatus> SucceededWhileAdvancingAsync(
        string what,
        Func<Task<LatticeOperationStatus?>> status,
        TimeSpan stall,
        TimeSpan ceiling,
        StrongBox<LatticeOperationStatus?>? last = null)
    {
        ArgumentNullException.ThrowIfNull(status);
        last ??= new StrongBox<LatticeOperationStatus?>();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var trajectory = new List<string>();
        (LatticeOperationState, string, int?, long)? seen = null;
        var advances = 0L;
        await UntilAdvancingAsync(
            what,
            async () =>
            {
                var current = await status();
                last.Value = current;
                if (current is not null && (current.State, current.Phase, current.PhaseIndex, current.CompletedUnits) is var key && key != seen)
                {
                    seen = key;
                    advances++;
                    trajectory.Add($"{current.State}/{current.Phase} {current.CompletedUnits}{(current.TotalUnits is { } total ? $"/{total}" : string.Empty)} at {clock.Elapsed.TotalSeconds:0} s");
                }

                return current is { IsTerminal: true };
            },
            () => advances,
            stall,
            ceiling,
            () => trajectory.Count == 0 ? "no status read" : string.Join(", ", trajectory));

        var done = last.Value!;
        return done.State == LatticeOperationState.Succeeded
            ? done
            : throw new InvalidOperationException($"{what} ended {done.State} in phase '{done.Phase}': {done.FailureReason}");
    }

    /// <summary>An operation status as state, phase and units, for a timeout's message.</summary>
    /// <param name="status">The status, or <see langword="null"/> when none was read.</param>
    /// <returns>The description.</returns>
    public static string DescribeStatus(LatticeOperationStatus? status) => status is null
        ? "none read"
        : $"{status.State}/{status.Phase} {status.CompletedUnits}{(status.TotalUnits is { } total ? $"/{total}" : string.Empty)}";

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
