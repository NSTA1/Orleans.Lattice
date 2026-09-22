namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// <see cref="IBenchSaturationGate"/> for a host that cannot observe WAL
/// saturation - in practice the Orleans-client producer used by the
/// multi-silo rig.
/// </summary>
/// <remarks>
/// <para>
/// This declines to answer rather than guessing. <c>IWalSaturationSignal</c>
/// is silo-scoped and in-process, and in a multi-silo cluster the measured
/// tree spans every silo, so no single client-observable value exists. A
/// client that polled one replica's signal would produce a confident,
/// plausible, wrong reading - strictly worse than reporting nothing,
/// because a wrong reading silently changes the FINAL accounting while
/// looking correct.
/// </para>
/// <para>
/// The practical consequence is narrow and is documented in
/// <c>docs/lattice/performance-multi-silo.md</c>: with this gate
/// installed the engine runs without the FX-029 residual-batch
/// abandonment and the FX-038 in-flight-tail quiesce. In an
/// <b>unsaturated</b> cohort that is a no-op and the numbers are directly
/// comparable to the single-silo rig. In a <b>saturated</b> cohort
/// <c>discarded=</c> reads 0 and <c>failed=</c> may read higher than the
/// single-silo equivalent, because the residual batch is dispatched
/// instead of abandoned and the in-flight tail is released without
/// waiting for recovery.
/// </para>
/// <para>
/// Saturation is still <i>detected</i> in that topology - it is simply
/// detected somewhere else. The observer that records it runs in-silo and
/// keeps emitting <c>[silo:saturation]</c> lines on every replica, so the
/// harness harvests saturation from the silo logs and annotates the
/// affected cohort rather than losing the signal.
/// </para>
/// </remarks>
internal sealed class NoOpBenchSaturationGate : IBenchSaturationGate
{
    /// <summary>Always <c>null</c>: this host observes no saturation.</summary>
    public DateTimeOffset? LastSaturatedUtc(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return null;
    }

    /// <summary>
    /// Completes immediately. Honours an already-cancelled token so the
    /// engine's bounded-wait call sites behave identically to the
    /// in-silo implementation.
    /// </summary>
    public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return cancellationToken.IsCancellationRequested
            ? Task.FromCanceled(cancellationToken)
            : Task.CompletedTask;
    }
}
