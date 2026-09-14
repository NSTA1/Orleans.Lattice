namespace Orleans.Lattice;

/// <summary>
/// Where the WAL GC scheduler's single <c>ExecuteAsync</c> loop is currently
/// parked, as reported by
/// <see cref="LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName"/>.
/// <para>
/// <b>Why the sweep's liveness needs a location and not an event.</b> Issue
/// #3060 is a loop that stops advancing while its process stays healthy, and a
/// <see cref="System.Diagnostics.Metrics.Counter{T}"/> cannot report that,
/// because a counter is only ever incremented by code that runs: any counter
/// sited on the failure is structurally dependent on the very thing that
/// stopped. The phase gauge is observed on the collector's thread, so it keeps
/// answering while the loop is wedged, and the answer names the await that is
/// not returning.
/// </para>
/// <para>
/// <b>Why the collecting stage is four members rather than one.</b> A single
/// <c>collecting</c> phase would reproduce this defect one level down: four
/// different awaits, with four different remedies, would again share one
/// observable. Priming a retention series, reconciling snapshot pins, running
/// the collector, and healing a blocked tree fail for unrelated reasons and are
/// fixed in unrelated places, so a reader has to be able to tell them apart from
/// one scrape.
/// </para>
/// <para>
/// Diagnostic only. The phase a loop records never changes what it does next.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(WalGcSchedulerPhaseCensus),
    LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName,
    LatticeMetrics.TagPhase)]
internal enum WalGcSchedulerPhase
{
    /// <summary>
    /// No scheduler has entered a phase in this process. The gauge reports this
    /// from the moment the instrument is published, which is what makes its
    /// presence a statement about the <i>build</i> rather than about the
    /// scheduler: a scrape carrying no <c>phase_age</c> series at all is a build
    /// that predates issue #3060, and a scrape carrying one is a build that has
    /// the instrument regardless of whether the scheduler ever started.
    /// </summary>
    Unstarted = 0,

    /// <summary>
    /// <c>WalGcInterval</c> is zero or negative, so collection is switched off by
    /// configuration and the loop returned before its first pass.
    /// </summary>
    Disabled = 1,

    /// <summary>
    /// Inside the randomised startup stagger, before the first pass. Bounded by
    /// the configured interval, so an age here that exceeds it is itself the
    /// finding.
    /// </summary>
    Starting = 2,

    /// <summary>
    /// Awaiting the registry enumeration that opens every pass - the first await,
    /// and the one candidate (A) of issue #3060 names.
    /// </summary>
    Enumerating = 3,

    /// <summary>
    /// Priming one tree's retention series, before any collection work for it.
    /// </summary>
    CollectingPriming = 4,

    /// <summary>
    /// Reconciling one tree's snapshot pins. An optional stage that is skipped
    /// when no snapshot pin registry is configured.
    /// </summary>
    CollectingReconciling = 5,

    /// <summary>
    /// Inside one tree's collector run - the stage that does the storage work,
    /// and the likeliest place for a pass to stall for a long time legitimately
    /// before it stalls for good.
    /// </summary>
    CollectingGcRun = 6,

    /// <summary>
    /// Observing or healing one tree that a blocked materialiser pin is holding
    /// back, after its collector run returned.
    /// </summary>
    CollectingHealing = 7,

    /// <summary>
    /// Pruning the scheduler's per-tree bookkeeping for trees the registry no
    /// longer reports, after every due tree has been swept.
    /// </summary>
    Pruning = 8,

    /// <summary>
    /// Parked on the cadence delay between passes. The phase a healthy scheduler
    /// spends nearly all of its time in, so an age here is only a finding when it
    /// exceeds the wait the previous pass actually chose - which is why
    /// <see cref="LatticeMetrics.WalGcSchedulerWaitName"/> is part of the same
    /// set.
    /// </summary>
    Waiting = 9,

    /// <summary>
    /// The loop has returned and will not run again for the lifetime of the
    /// process. The reason is on
    /// <see cref="LatticeMetrics.WalGcSchedulerTerminationsName"/>; this arm is
    /// what makes the <i>state</i> readable from a single scrape, with an age
    /// that says how long the silo has been without a sweep.
    /// </summary>
    Stopped = 10,
}
