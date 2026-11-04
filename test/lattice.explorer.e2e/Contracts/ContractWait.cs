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
    /// <param name="holding">
    /// Whether the work is waiting on other work it has named, read after each poll; while it
    /// is, the stall check does not run. <see langword="null"/> when the work never waits.
    /// </param>
    /// <exception cref="TimeoutException">The work stalled, or ran past the ceiling.</exception>
    public static async Task UntilAdvancingAsync(
        string what,
        Func<Task<bool>> condition,
        Func<long> progress,
        TimeSpan stall,
        TimeSpan ceiling,
        Func<string> describe,
        Func<bool>? holding = null)
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

            if (holding?.Invoke() == true)
            {
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
    /// moved for <paramref name="stall"/>, when, given <paramref name="convergence"/>, its backlog
    /// has set no new low for that long, or when <paramref name="ceiling"/> has elapsed, and
    /// reports the trajectory it observed.
    /// </summary>
    /// <param name="what">The operation, for the failure message.</param>
    /// <param name="status">Reads the operation's status.</param>
    /// <param name="stall">How long the operation may go without advancing.</param>
    /// <param name="ceiling">The absolute deadline.</param>
    /// <param name="last">Receives the last status read, also when the wait times out.</param>
    /// <param name="convergence">
    /// How long the operation's backlog - its phase total less its completed units, while the
    /// phase reports a total - may go without reaching a new low within one phase; unbounded
    /// when <see langword="null"/>. An operation whose total grows as fast as it completes
    /// units keeps advancing but never finishes, and only this catches it.
    /// </param>
    /// <param name="externalWait">
    /// A phase in which the operation is waiting on other work rather than stalled; see
    /// <see cref="ExternalWait"/>. <see langword="null"/> when every phase is judged by the
    /// stall check alone.
    /// </param>
    /// <returns>The succeeded operation's status.</returns>
    /// <exception cref="InvalidOperationException">The operation failed or was cancelled.</exception>
    /// <exception cref="TimeoutException">The operation stalled, stopped converging, or ran past the ceiling.</exception>
    public static async Task<LatticeOperationStatus> SucceededWhileAdvancingAsync(
        string what,
        Func<Task<LatticeOperationStatus?>> status,
        TimeSpan stall,
        TimeSpan ceiling,
        StrongBox<LatticeOperationStatus?>? last = null,
        TimeSpan? convergence = null,
        ExternalWait? externalWait = null)
    {
        ArgumentNullException.ThrowIfNull(status);
        last ??= new StrongBox<LatticeOperationStatus?>();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var trajectory = new List<string>();
        (LatticeOperationState, string, int?, long)? seen = null;
        var advances = 0L;
        var backlog = new Convergence();
        var waiting = new ExternalWaitTracker(externalWait);
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

                if (current is { IsTerminal: false } && convergence is { } window
                    && backlog.Observe(current, clock.Elapsed) is var sinceLow && sinceLow > window)
                {
                    throw new TimeoutException(
                        $"Timed out after {clock.Elapsed.TotalSeconds:0} seconds: {what} kept advancing, but its backlog set no new "
                        + $"low for {window.TotalSeconds:0} seconds, so it is not converging. Progress: {string.Join(", ", trajectory)}.");
                }

                await waiting.ObserveAsync(current, clock.Elapsed, what, trajectory);
                return current is { IsTerminal: true };
            },
            () => advances,
            stall,
            ceiling,
            () => (trajectory.Count == 0 ? "no status read" : string.Join(", ", trajectory)) + waiting.Describe(),
            () => waiting.Holding);

        var done = last.Value!;
        return done.State == LatticeOperationState.Succeeded
            ? done
            : throw new InvalidOperationException($"{what} ended {done.State} in phase '{done.Phase}': {done.FailureReason}");
    }

    /// <summary>
    /// A phase in which a tracked operation is waiting on other work - a schema remediation in
    /// <c>WaitingForAliasOperation</c> while another operation holds the tree's alias - rather
    /// than making no progress. While the operation is in <paramref name="Phase"/> and
    /// <paramref name="Holder"/> names the same holder as when the wait began, the stall check
    /// does not run; the wait has its own <paramref name="Ceiling"/> instead, and every timeout
    /// names the holder. A wait whose holder the harness cannot name is judged by the stall
    /// check as before.
    /// </summary>
    /// <param name="Phase">The operation's waiting phase.</param>
    /// <param name="Ceiling">How long one continuous wait in <paramref name="Phase"/> may last.</param>
    /// <param name="Holder">
    /// Names the work being waited on, or returns <see langword="null"/> when it cannot be
    /// identified; a transient gRPC failure counts as unidentified.
    /// </param>
    public sealed record ExternalWait(string Phase, TimeSpan Ceiling, Func<Task<string?>> Holder);

    /// <summary>Tracks one continuous <see cref="ExternalWait"/> and its holder.</summary>
    private sealed class ExternalWaitTracker(ExternalWait? wait)
    {
        private TimeSpan? _since;
        private string? _holder;
        private string? _lastHolder;

        /// <summary>Whether the stall check is paused: the operation waits on a holder the harness has named.</summary>
        public bool Holding { get; private set; }

        public async Task ObserveAsync(LatticeOperationStatus? status, TimeSpan now, string what, List<string> trajectory)
        {
            if (wait is null || status is not { IsTerminal: false } || status.Phase != wait.Phase)
            {
                _since = null;
                _holder = null;
                Holding = false;
                return;
            }

            string? holder;
            try
            {
                holder = await wait.Holder();
            }
            catch (RpcException ex) when (IsTransient(ex.StatusCode))
            {
                holder = null;
            }

            _lastHolder = holder ?? _lastHolder;
            if (_since is null || holder != _holder)
            {
                // A new wait, or one now on a different (or no longer named) holder, starts over.
                _since = now;
                _holder = holder;
            }

            Holding = holder is not null;
            if (now - _since > wait.Ceiling)
            {
                throw new TimeoutException(
                    $"Timed out after {now.TotalSeconds:0} seconds: {what} waited in {wait.Phase} for "
                    + $"{wait.Ceiling.TotalSeconds:0} seconds on {holder ?? "a holder the harness cannot name"}. "
                    + $"Progress: {string.Join(", ", trajectory)}.");
            }
        }

        public string Describe() => _lastHolder is null ? string.Empty : $"; last {wait!.Phase} holder: {_lastHolder}";
    }

    /// <summary>
    /// Tracks the lowest backlog (total less completed units) an operation has reported in its
    /// current phase, and when it was set; a new phase starts a new low.
    /// </summary>
    private sealed class Convergence
    {
        private (string Phase, int? Index)? _phase;
        private long _low;
        private TimeSpan _lowAt;

        /// <summary>
        /// Records <paramref name="status"/> observed at <paramref name="now"/>, and returns how
        /// long ago the phase's backlog last reached a new low; zero while the phase reports no
        /// total.
        /// </summary>
        public TimeSpan Observe(LatticeOperationStatus status, TimeSpan now)
        {
            if (status.TotalUnits is not { } total)
            {
                _phase = null;
                return TimeSpan.Zero;
            }

            var backlog = total - status.CompletedUnits;
            if (_phase != (status.Phase, status.PhaseIndex) || backlog < _low)
            {
                _phase = (status.Phase, status.PhaseIndex);
                _low = backlog;
                _lowAt = now;
            }

            return now - _lowAt;
        }
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
