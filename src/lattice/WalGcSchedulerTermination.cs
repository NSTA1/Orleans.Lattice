namespace Orleans.Lattice;

/// <summary>
/// Why the WAL GC scheduler's <c>ExecuteAsync</c> loop stopped running.
/// <para>
/// A <see cref="Microsoft.Extensions.Hosting.BackgroundService"/> that returns
/// from <c>ExecuteAsync</c> is finished for the lifetime of the process: nothing
/// restarts it, and the host neither logs nor reports the return. Every exit
/// below therefore ends silo-wide WAL garbage collection permanently, and before
/// issue #3060 not one of them emitted anything a scrape could see.
/// </para>
/// <para>
/// This taxonomy is the <i>terminal</i> half of the liveness set. The
/// <see cref="LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName"/> gauge answers
/// "where is the loop parked right now"; this counter answers "has the loop
/// stopped, and why". A reader needs both, because a loop wedged inside a phase
/// and a loop that has returned are indistinguishable on a counter alone.
/// </para>
/// <para>
/// Diagnostic only. Recording a termination never alters what the scheduler does
/// on its way out.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(LatticeWalGcScheduler),
    LatticeMetrics.WalGcSchedulerTerminationsName,
    LatticeMetrics.TagReason)]
internal enum WalGcSchedulerTermination
{
    /// <summary>
    /// <c>WalGcInterval</c> was zero or negative, so collection is switched off
    /// by configuration and the loop returned before its first pass.
    /// <para>
    /// A deliberate, correct exit - and the one most likely to be mistaken for a
    /// defect, because on every other instrument a disabled scheduler and a
    /// wedged one are the same absence. Recording it is what lets a reader
    /// dismiss the possibility in one scrape instead of reading configuration on
    /// a running silo.
    /// </para>
    /// </summary>
    Disabled = 0,

    /// <summary>
    /// The host asked the service to stop and the loop observed the cancellation,
    /// either at the loop condition or out of a cadence delay. The ordinary
    /// shutdown exit.
    /// </summary>
    Cancelled = 1,

    /// <summary>
    /// The loop body threw an exception that escaped <c>ExecuteAsync</c>. This is
    /// candidate (C) of issue #3060.
    /// <para>
    /// The arm is kept even though the evidence argues against it: the host runs
    /// under the default <c>BackgroundServiceExceptionBehavior.StopHost</c>, the
    /// container's restart count stayed at zero, and hundreds of unrelated series
    /// kept advancing across the frozen window, so the process plainly did not
    /// stop. That argument rests on the deployed image matching the tracked tree
    /// and on no host-level handler intercepting the stop, and neither was
    /// verified - a candidate argued down on an unverified premise still gets an
    /// instrument, because the cost of the instrument is one counter arm and the
    /// cost of being wrong is another blind window.
    /// </para>
    /// <para>
    /// The exception is recorded and then <b>rethrown</b>. Swallowing it here
    /// would convert a fault the host is configured to act on into exactly the
    /// silent stop this issue is about.
    /// </para>
    /// </summary>
    Faulted = 2,
}
