namespace Orleans.Lattice;

/// <summary>
/// Extension methods for <see cref="IWalSaturationSignal"/> that
/// package the canonical back-pressure response pattern as a single
/// call. Consumers that drive offered load into <see cref="ILattice"/>
/// (TCP listener fronting a single tree, a saga coordinator
/// dispatching per-entry writes, an HTTP handler accepting client
/// requests) should call
/// <see cref="ApplyBackPressureAsync(IWalSaturationSignal, string, TimeSpan, System.Threading.CancellationToken)"/>
/// once per offered unit (TCP line, saga entry, HTTP request) to
/// translate the three-state signal into the right local action -
/// no-op on Healthy, an honest per-call delay on Throttled, a full
/// park-until-Healthy on Saturated.
/// <para>
/// Centralising the pattern here avoids each consumer rolling its
/// own Throttled response (a recurring source of "the signal fires
/// but back-pressure is too soft to prevent the failure tail")
/// because the per-call response is too gentle. The default per-call
/// delay on Throttled is small enough (1 ms) that a transient
/// regime does not produce a perceptible latency penalty, but the
/// cumulative slowdown across many calls applies meaningful
/// back-pressure before the regime escalates to Saturated. Callers
/// that want a different trade-off pass an explicit
/// <see cref="System.TimeSpan"/>.
/// </para>
/// </summary>
public static class WalSaturationSignalExtensions
{
    /// <summary>
    /// Default per-call delay applied on
    /// <see cref="WalSaturationState.Throttled"/> when the caller does
    /// not pass an explicit duration to
    /// <see cref="ApplyBackPressureAsync(IWalSaturationSignal, string, TimeSpan, System.Threading.CancellationToken)"/>.
    /// Sized so a 10 k events/sec offered stream slows to ~1 k
    /// events/sec during Throttled (10x cumulative slowdown), giving
    /// the writer's admission gate time to drain before the regime
    /// escalates to Saturated without producing a perceptible
    /// per-call latency penalty when the regime is transient.
    /// </summary>
    public static readonly TimeSpan DefaultThrottledDelay = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Translates the per-tree saturation signal into the canonical
    /// per-call back-pressure response:
    /// <list type="bullet">
    /// <item><description><see cref="WalSaturationState.Healthy"/> -
    /// returns immediately (no allocation, synchronous fast path).
    /// The signal cache lookup is the entire cost.</description></item>
    /// <item><description><see cref="WalSaturationState.Throttled"/> -
    /// awaits <c>Task.Delay(throttledDelay, ct)</c>. The delay is
    /// short by default (1 ms) so a transient flap does not produce
    /// a perceptible latency penalty, but cumulative across many
    /// calls it applies meaningful back-pressure on the offered
    /// stream (a TCP reader slows from 10 k events/sec to
    /// ~1 k events/sec at the default).</description></item>
    /// <item><description><see cref="WalSaturationState.Saturated"/> -
    /// awaits <see cref="IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>.
    /// Parks the caller until the sampler observes the tree return
    /// to Healthy. The recovery latency is bounded by one
    /// <see cref="LatticeOptions.WalSaturationSampleInterval"/>
    /// beyond the underlying signal clearing.</description></item>
    /// </list>
    /// <para>
    /// <b>Caller contract.</b> Call this once per offered unit (TCP
    /// line, saga entry, HTTP request) before dispatching the unit
    /// to <see cref="ILattice"/>. The call must be awaited so the
    /// Throttled delay actually applies; a fire-and-forget
    /// invocation defeats the back-pressure semantic.
    /// </para>
    /// <para>
    /// <b>Cancellation.</b> Cancellation through
    /// <paramref name="cancellationToken"/> throws
    /// <see cref="System.OperationCanceledException"/> exactly as the
    /// underlying <see cref="System.Threading.Tasks.Task.Delay(System.TimeSpan, System.Threading.CancellationToken)"/>
    /// /
    /// <see cref="IWalSaturationSignal.WaitForHealthyAsync(string, System.Threading.CancellationToken)"/>
    /// do; the Healthy fast-path observes the token only by
    /// returning a completed task (no synchronous throw).
    /// </para>
    /// </summary>
    /// <param name="signal">The per-tree saturation signal.</param>
    /// <param name="treeId">The tree to apply back-pressure for.</param>
    /// <param name="throttledDelay">Per-call delay applied on
    /// <see cref="WalSaturationState.Throttled"/>. Pass
    /// <see cref="System.TimeSpan.Zero"/> to disable the Throttled
    /// response (the signal is still observed but the Throttled
    /// branch becomes a no-op, equivalent to the historical
    /// scheduler-yield pattern that produced too-soft back-pressure
    /// under bursty regimes).</param>
    /// <param name="cancellationToken">Cancels a parked
    /// <see cref="WalSaturationState.Saturated"/> wait or a
    /// <see cref="WalSaturationState.Throttled"/> delay.</param>
    /// <returns>A task that completes when the per-call back-pressure
    /// action is finished (immediate on Healthy, after the delay on
    /// Throttled, after the recovery on Saturated).</returns>
    /// <exception cref="ArgumentNullException">Thrown when
    /// <paramref name="signal"/> or <paramref name="treeId"/> is
    /// <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when
    /// <paramref name="throttledDelay"/> is negative (and not
    /// <see cref="System.TimeSpan.Zero"/>).</exception>
    /// <exception cref="System.OperationCanceledException">Thrown
    /// when <paramref name="cancellationToken"/> is cancelled during
    /// the delay or the parked wait.</exception>
    public static Task ApplyBackPressureAsync(
        this IWalSaturationSignal signal,
        string treeId,
        TimeSpan throttledDelay,
        CancellationToken cancellationToken = default)
        => ApplyBackPressureAsync(signal, treeId, throttledDelay, TimeProvider.System, cancellationToken);

    /// <summary>
    /// Time-provider seam behind the public
    /// <see cref="ApplyBackPressureAsync(IWalSaturationSignal, string, TimeSpan, System.Threading.CancellationToken)"/>
    /// overload. The public overload forwards <see cref="TimeProvider.System"/>,
    /// so production behaviour is identical to <c>Task.Delay(throttledDelay, ct)</c>;
    /// tests inject a controllable <see cref="TimeProvider"/> to assert the
    /// Throttled delay deterministically instead of measuring wall-clock elapsed
    /// time (which is timing-sensitive under parallel load).
    /// </summary>
    internal static Task ApplyBackPressureAsync(
        this IWalSaturationSignal signal,
        string treeId,
        TimeSpan throttledDelay,
        TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(signal);
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(timeProvider);
        if (throttledDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(throttledDelay),
                throttledDelay,
                "throttledDelay must be non-negative (TimeSpan.Zero disables the Throttled branch).");
        }
        var state = signal.GetCurrentState(treeId);
        return state switch
        {
            WalSaturationState.Saturated => signal.WaitForHealthyAsync(treeId, cancellationToken),
            WalSaturationState.Throttled when throttledDelay > TimeSpan.Zero => Task.Delay(throttledDelay, timeProvider, cancellationToken),
            _ => Task.CompletedTask,
        };
    }

    /// <summary>
    /// Convenience overload that uses
    /// <see cref="DefaultThrottledDelay"/> (1 ms) for the Throttled
    /// per-call delay. The default is sized for the canonical TCP
    /// reader pattern - see the
    /// <see cref="ApplyBackPressureAsync(IWalSaturationSignal, string, TimeSpan, System.Threading.CancellationToken)"/>
    /// overload for the full semantics.
    /// </summary>
    /// <param name="signal">The per-tree saturation signal.</param>
    /// <param name="treeId">The tree to apply back-pressure for.</param>
    /// <param name="cancellationToken">Cancels a parked
    /// <see cref="WalSaturationState.Saturated"/> wait or a
    /// <see cref="WalSaturationState.Throttled"/> delay.</param>
    /// <returns>A task that completes when the per-call back-pressure
    /// action is finished.</returns>
    public static Task ApplyBackPressureAsync(
        this IWalSaturationSignal signal,
        string treeId,
        CancellationToken cancellationToken = default)
        => ApplyBackPressureAsync(signal, treeId, TimeProvider.System, cancellationToken);

    /// <summary>
    /// Time-provider seam behind the public convenience
    /// <see cref="ApplyBackPressureAsync(IWalSaturationSignal, string, System.Threading.CancellationToken)"/>
    /// overload. Forwards <see cref="DefaultThrottledDelay"/> so tests can assert,
    /// deterministically through an injected <see cref="TimeProvider"/>, that the
    /// convenience overload applies the default Throttled delay.
    /// </summary>
    internal static Task ApplyBackPressureAsync(
        this IWalSaturationSignal signal,
        string treeId,
        TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
        => ApplyBackPressureAsync(signal, treeId, DefaultThrottledDelay, timeProvider, cancellationToken);
}