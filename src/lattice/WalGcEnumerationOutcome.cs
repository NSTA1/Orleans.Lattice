namespace Orleans.Lattice;

/// <summary>
/// What the WAL GC scheduler's registry enumeration produced on one pass.
/// <para>
/// The enumeration is the first await of every pass and the single point every
/// later stage depends on, so it is also the pass's most consequential silent
/// failure. Before issue #3060 a faulted enumeration was swallowed into a
/// <c>LogDebug</c> and answered with a relaxing quiet wait, which produced no
/// counter, no warning, and a scrape in which every <c>wal_gc</c> series simply
/// stopped advancing - byte-identical to a silo with nothing to collect.
/// </para>
/// <para>
/// <b>Five arms rather than two, because <c>_quietWait</c> already documents
/// three conditions behind one observable.</b> Its own summary names "an empty
/// registry, a registry that faulted, or a registry reporting only blank ids",
/// and all three take the same silent path to the same relaxing wait. They have
/// different causes and different remedies - a faulted registry is a fault to
/// chase, an empty one is a silo with no trees yet, and one reporting only blank
/// ids is a registry returning corrupt content while appearing healthy - so they
/// are separate arms here. Collapsing them is what made the condition
/// undiagnosable in the first place.
/// </para>
/// <para>
/// Diagnostic only. Which arm a pass records never changes what the pass is
/// allowed to trim, nor how long it waits afterwards.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(LatticeWalGcScheduler),
    LatticeMetrics.WalGcSchedulerEnumerationsName,
    LatticeMetrics.TagOutcome)]
internal enum WalGcEnumerationOutcome
{
    /// <summary>
    /// The registry answered with at least one non-blank tree id. The ordinary
    /// healthy arm, and the one whose <i>absence</i> is the finding: a scheduler
    /// that has stopped sweeping stops advancing this arm while the process
    /// carries on.
    /// </summary>
    Succeeded = 0,

    /// <summary>
    /// The registry call threw. This is candidate (A) of issue #3060, and the
    /// arm the issue was filed to make visible.
    /// </summary>
    Faulted = 1,

    /// <summary>
    /// The registry call was cancelled because the silo is shutting down. Kept
    /// distinct from <see cref="Faulted"/> because a cancelled enumeration during
    /// host shutdown is correct behaviour rather than a fault, and rendering an
    /// orderly shutdown as a fault would train a reader to ignore the arm that
    /// matters.
    /// </summary>
    Cancelled = 2,

    /// <summary>
    /// The registry answered with no tree ids at all. A silo that has not yet
    /// registered a tree, which is the expected reading during early startup and
    /// on an idle host.
    /// </summary>
    Empty = 3,

    /// <summary>
    /// The registry answered with ids, but every one of them was null or empty,
    /// so the pass had nothing collectable despite a non-empty answer.
    /// <para>
    /// This is the arm that cannot be inferred from any other: a registry in this
    /// state reports success, returns content, and collects nothing, so it
    /// presents exactly as a healthy idle silo on every other instrument.
    /// </para>
    /// </summary>
    AllBlank = 4,

    /// <summary>
    /// The enumeration did not answer inside the scheduler's own bound, so the
    /// pass abandoned it rather than waiting indefinitely.
    /// <para>
    /// Deliberately <b>not</b> folded into <see cref="Faulted"/>. A fault is a
    /// property of the registry; a timeout is a property of the bound this
    /// scheduler applies to it, and reporting the second as the first would
    /// render a decision of ours as a finding about the system - the same
    /// conflation <see cref="WalGcBlockingPinState.Unreadable"/> exists to
    /// prevent one subsystem over.
    /// </para>
    /// </summary>
    TimedOut = 5,
}
