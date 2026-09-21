using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo record of where the WAL GC scheduler's <c>ExecuteAsync</c> loop is
/// currently parked, and the source of the
/// <see cref="LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName"/> observable gauge.
/// <para>
/// <b>Why a gauge, when the sibling instruments are counters.</b> Issue #3060 is
/// a loop that stops advancing while its process stays healthy, and that is a
/// <i>location</i> problem rather than an <i>event</i> problem. A
/// <see cref="Counter{T}"/> can only be incremented by code that runs, so any
/// counter sited on the stall is structurally dependent on the very thing that
/// stopped: the moment the loop wedges, the counter that would have reported it
/// goes quiet along with everything else, and the scrape is an absence. An
/// observable gauge is polled on the collector's thread, so it keeps answering
/// while the loop is wedged - and the answer names the await that is not
/// returning.
/// </para>
/// <para>
/// <b>Why the series is never absent.</b>
/// <see cref="Observe"/> always emits at least one measurement: with no
/// scheduler having entered a phase it reports
/// <see cref="WalGcSchedulerPhase.Unstarted"/>. An observable instrument emits
/// from its declaration site rather than from an argument list, so the gauge is
/// published as soon as this class is touched, whether or not the scheduler ever
/// ran. That is deliberate and load-bearing: it makes the presence of the series
/// a statement about the <b>build</b> rather than about the scheduler, so a
/// scrape with no <c>phase_age</c> at all is a build that predates this change,
/// and a scrape carrying one licenses reading every sibling instrument's zero as
/// a measurement rather than as a missing wire. Every instrument in this set
/// ships in one commit, so this gauge is the build witness for all of them.
/// </para>
/// <para>
/// <b>Single writer.</b> <c>LatticeWalGcScheduler</c> documents its one
/// <c>ExecuteAsync</c> loop as the sole mutator of its cadence state, and the
/// same confinement holds here: exactly one loop per silo calls
/// <see cref="Enter"/>, so the state needs no lock and a scrape needs never
/// block a pass. Tests that construct several schedulers in one process share
/// this state by design - the instrument is silo-scoped, and a fixture that
/// scrapes it must be non-parallel for the same reason a fixture that scrapes
/// any process-global instrument must be.
/// </para>
/// <para>
/// <b>Platform-scoped, not tenant-scoped.</b> The scheduler loop is one per
/// silo and sweeps every tenant's trees in turn, so its liveness is a property
/// of the silo and cannot vary by tenant. The measurement still carries the
/// tenant dimension - as the constant platform sentinel - so that one query
/// shape works across instruments, which is the contract the tenant-dimension
/// hygiene gate enforces.
/// </para>
/// </summary>
internal static class WalGcSchedulerPhaseCensus
{
    /// <summary>
    /// The live phase. A reference so that a reader sees a whole, self-consistent
    /// tuple: the phase, its tree, and the instant it was entered have to change
    /// together, and three separate volatile fields would let a scrape observe a
    /// phase from one transition beside a timestamp from the next.
    /// <para>
    /// Written with <see cref="Volatile.Write{T}"/> and read with
    /// <see cref="Volatile.Read{T}"/> so the collector's thread sees the
    /// publication rather than a cached reference.
    /// </para>
    /// </summary>
    private static PhaseState _state = new(
        WalGcSchedulerPhase.Unstarted,
        Tree: null,
        EnteredAtUtcTicks: DateTimeOffset.UtcNow.UtcTicks,
        Time: TimeProvider.System);

    /// <summary>
    /// The observable gauge itself. Registered here rather than beside the other
    /// instruments in <see cref="LatticeMetrics"/> for the reason
    /// <c>CoordinatorPhaseTickCensus</c> gives: an observable instrument's
    /// measurements come from its callback rather than from an argument list, so
    /// its registration and the tenant-dimension emission have to be readable
    /// together.
    /// <para>
    /// Declared <b>below</b> the state <see cref="Observe"/> reads. A listener
    /// may observe the gauge during instrument publication, which runs this
    /// initialiser re-entrantly, and static initialisers run in declaration
    /// order - a callback that ran before <see cref="_state"/> was assigned would
    /// dereference null.
    /// </para>
    /// </summary>
    internal static readonly ObservableGauge<double> Gauge =
        LatticeMetrics.Meter.CreateObservableGauge(
            LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName,
            Observe,
            unit: "s",
            description: "How long the silo's WAL GC scheduler loop has been in its current phase, tagged by phase and, while collecting, by tree. Always reports, so the series is present from process start and its absence means the build predates the instrument rather than that the scheduler is idle. A phase whose age exceeds the cadence the scheduler chose is a stalled sweep.");

    /// <summary>
    /// Records that the loop has entered <paramref name="phase"/>, resetting the
    /// age the gauge reports.
    /// </summary>
    /// <param name="phase">The phase now being entered.</param>
    /// <param name="tree">
    /// The tree being collected, for the four collecting phases; <c>null</c>
    /// otherwise. Carried only while collecting, because that is the only stage
    /// whose stall is attributable to one tree - and because the loop sweeps
    /// trees sequentially, so at most one tree tag is live at a time and the
    /// series cannot fan out across an unbounded leaf population.
    /// </param>
    /// <param name="time">
    /// The scheduler's own time source, so that the age the gauge reports is
    /// measured on the same clock the scheduler schedules on. A fixture driving
    /// virtual time would otherwise see a real-time age against virtual-time
    /// waits.
    /// </param>
    internal static void Enter(WalGcSchedulerPhase phase, string? tree, TimeProvider time) =>
        Volatile.Write(
            ref _state,
            new PhaseState(phase, tree, time.GetUtcNow().UtcTicks, time));

    /// <summary>
    /// Resets the census to <see cref="WalGcSchedulerPhase.Unstarted"/> on the
    /// system clock, as it stood before any scheduler ran.
    /// <para>
    /// Exists for fixtures. The state is process-global by design, so a test that
    /// asserts on a phase has to be able to establish a known starting point;
    /// production never calls this, because a silo has exactly one scheduler and
    /// its phases only ever move forward.
    /// </para>
    /// </summary>
    internal static void ResetForTests() =>
        Volatile.Write(
            ref _state,
            new PhaseState(
                WalGcSchedulerPhase.Unstarted,
                Tree: null,
                EnteredAtUtcTicks: DateTimeOffset.UtcNow.UtcTicks,
                Time: TimeProvider.System));

    /// <summary>
    /// The tag every measurement of this gauge carries for
    /// <paramref name="phase"/>.
    /// <para>
    /// Total over the enum with no discard arm: a member added without an arm
    /// here fails the arming gate rather than silently joining another member's
    /// series, which is the shape this whole instrument exists to make
    /// impossible.
    /// </para>
    /// </summary>
    /// <param name="phase">The phase to name.</param>
    /// <returns>The <see cref="LatticeMetrics.TagPhase"/> tag for it.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The phase has no arm.</exception>
    internal static KeyValuePair<string, object?> PhaseTag(WalGcSchedulerPhase phase) => phase switch
    {
        WalGcSchedulerPhase.Unstarted => new(LatticeMetrics.TagPhase, "unstarted"),
        WalGcSchedulerPhase.Disabled => new(LatticeMetrics.TagPhase, "disabled"),
        WalGcSchedulerPhase.Starting => new(LatticeMetrics.TagPhase, "starting"),
        WalGcSchedulerPhase.Enumerating => new(LatticeMetrics.TagPhase, "enumerating"),
        WalGcSchedulerPhase.CollectingPriming => new(LatticeMetrics.TagPhase, "collecting.priming"),
        WalGcSchedulerPhase.CollectingReconciling => new(LatticeMetrics.TagPhase, "collecting.reconciling"),
        WalGcSchedulerPhase.CollectingGcRun => new(LatticeMetrics.TagPhase, "collecting.gc_run"),
        WalGcSchedulerPhase.CollectingHealing => new(LatticeMetrics.TagPhase, "collecting.healing"),
        WalGcSchedulerPhase.Pruning => new(LatticeMetrics.TagPhase, "pruning"),
        WalGcSchedulerPhase.Waiting => new(LatticeMetrics.TagPhase, "waiting"),
        WalGcSchedulerPhase.Stopped => new(LatticeMetrics.TagPhase, "stopped"),
        _ => throw new ArgumentOutOfRangeException(nameof(phase), phase, "Unmapped WAL GC scheduler phase."),
    };

    /// <summary>
    /// Emits exactly one measurement: the age, in seconds, of the phase the loop
    /// is currently in.
    /// <para>
    /// One series rather than one per phase. The question is "where is the loop
    /// now, and for how long", and a loop is in one phase - emitting a stale age
    /// for the ten phases it is <i>not</i> in would leave a reader to work out
    /// which of eleven climbing numbers is the live one, and a dashboard to chart
    /// ten fictions.
    /// </para>
    /// <para>
    /// The age is clamped at zero. A fixture that rewinds virtual time, or a
    /// system clock stepped backwards, must not publish a negative duration.
    /// </para>
    /// </summary>
    /// <returns>The single live phase measurement.</returns>
    internal static IEnumerable<Measurement<double>> Observe()
    {
        var state = Volatile.Read(ref _state);
        var elapsedTicks = state.Time.GetUtcNow().UtcTicks - state.EnteredAtUtcTicks;
        var seconds = elapsedTicks <= 0 ? 0d : (double)elapsedTicks / TimeSpan.TicksPerSecond;

        return state.Tree is null
            ? [new Measurement<double>(seconds, PhaseTag(state.Phase), LatticeTenantLabel.Platform)]
            :
            [
                new Measurement<double>(
                    seconds,
                    PhaseTag(state.Phase),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.Tree),
                    LatticeTenantLabel.Platform),
            ];
    }

    /// <summary>One phase, the tree it belongs to if any, and when it began.</summary>
    /// <param name="Phase">The phase the loop is in.</param>
    /// <param name="Tree">The tree being collected, or <c>null</c> outside the collecting stage.</param>
    /// <param name="EnteredAtUtcTicks">When the phase was entered, on <paramref name="Time"/>'s clock.</param>
    /// <param name="Time">The clock the age is measured on.</param>
    private sealed record PhaseState(
        WalGcSchedulerPhase Phase,
        string? Tree,
        long EnteredAtUtcTicks,
        TimeProvider Time);
}
