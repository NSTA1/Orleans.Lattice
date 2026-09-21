using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo census of how many leaf-snapshot captures are executing at once,
/// and the source of the
/// <see cref="LatticeMetrics.LeafSnapshotCaptureConcurrencyPeakGaugeName"/>
/// observable gauge. This is the measurement issue #2696 asks for and that
/// PR #2723 deliberately deferred: cross-leaf capture concurrency, which no
/// existing instrument reports.
/// <para>
/// <b>Cross-leaf, because the per-leaf guard is not the quantity in question.</b>
/// <c>BPlusLeafGrain</c> already holds a single-flight <c>bool</c> that stops one
/// leaf capturing twice concurrently, and a capture turned away by it is counted
/// as an <c>already_in_flight</c> decline. That guard is per activation, so it
/// says nothing about how many <i>different</i> leaves are capturing against the
/// one shared snapshot storage provider at the same instant. A thousand leaves
/// each passing their own guard is exactly the fan-out issue #2696 describes, and
/// it is invisible to every per-leaf instrument by construction.
/// </para>
/// <para>
/// <b>Why a monotone peak rather than the obvious alternatives.</b> Both of the
/// natural choices are unreadable on this deployment, and each fails in a way
/// that is silent:
/// </para>
/// <list type="bullet">
///   <item><description>An <i>instantaneous</i> gauge of the current depth is
///   sampled only when the endpoint is scraped, so the transient spike that makes
///   the metric worth having is precisely what falls between two scrapes. This is
///   the objection PR #2723 raised when it deferred the metric, and it is
///   correct.</description></item>
///   <item><description>A <see cref="Histogram{T}"/> recorded at each capture
///   entry - the idiom used by <c>orleans.lattice.leaf.commit.in_flight</c> and
///   its siblings - is event-sampled and so cannot miss the spike, but this
///   container's exposition renders every histogram as <c>_sum</c> and
///   <c>_count</c> with no buckets and no quantiles. Only the mean survives, and
///   a mean dilutes a spike to nothing: one capture at depth 64 among ten
///   thousand at depth 0 reads as 0.0064. The same limit is recorded on
///   <see cref="LatticeMetrics.LeafSnapshotCaptureDuration"/>.</description></item>
/// </list>
/// <para>
/// A <b>monotone non-decreasing</b> peak is immune to both. Because the value
/// never falls, a spike that raises it is reported by that scrape and by every
/// scrape afterwards, so scrape timing stops being a risk to be managed and
/// becomes irrelevant. The cost is that the peak is a high-water mark for the
/// life of the process and does not decay; that is the intended reading, and a
/// restart is what resets it.
/// </para>
/// <para>
/// <b>Zero-primed by construction.</b> The callback always yields a measurement,
/// so the series exists from the first scrape of a silo that has never captured
/// anything. A reported <c>0</c> therefore means "measured none", not "no
/// detector" - the distinction that makes an absence-based argument admissible at
/// all, and one this epic has had to relearn from instruments that were declared
/// but never primed.
/// </para>
/// <para>
/// <b>Unattributable to a tenant, deliberately.</b> The peak is a maximum taken
/// across every leaf on the silo, so it spans trees and therefore tenants; the
/// contended resource - one storage provider per silo - is itself silo-wide.
/// Splitting the series per tree would report several smaller numbers, none of
/// which is the depth the provider actually saw, so the measurement is emitted
/// with the constant platform sentinel. The per-tree half of the question is
/// answered by <see cref="LatticeMetrics.LeafSnapshotCaptureConcurrentEntries"/>,
/// which is tree-tagged.
/// </para>
/// </summary>
internal sealed class LeafSnapshotCaptureConcurrencyCensus
{
    private static readonly object RegistrationLock = new();

    /// <summary>
    /// The one census for this process, and deliberately not a DI singleton.
    /// <para>
    /// The measured quantity is a maximum taken across every leaf on the silo,
    /// so it is process-scoped by definition. Two instances would each hold a
    /// <i>partial</i> maximum while the gauge could report only one of them, and
    /// a high-water mark that under-reports is worse than absent: it looks like
    /// a measurement and licenses the conclusion that concurrency stayed low.
    /// A single static instance is the only shape that cannot split that way.
    /// </para>
    /// </summary>
    private static readonly LeafSnapshotCaptureConcurrencyCensus Instance = new();

    /// <summary>
    /// The registered peak gauge, or <see langword="null"/> before
    /// <see cref="EnsureGaugeRegistered"/> has run. Held as a field rather than
    /// a bare flag so the registration state is the instrument itself.
    /// <para>
    /// Declared below <see cref="Instance"/>, which its callback reads. The field
    /// has no initialiser, so no ordering hazard exists either way, but the
    /// placement keeps the class consistent with the rule that an observable
    /// instrument is declared below the state it observes.
    /// </para>
    /// </summary>
    private static ObservableGauge<int>? _peakGauge;

    /// <summary>
    /// Captures currently between the attempt boundary and the completion of the
    /// capture, across every leaf activation on this silo.
    /// </summary>
    private int _inFlight;

    /// <summary>
    /// The greatest value <see cref="_inFlight"/> has ever reached on this
    /// instance. Monotone non-decreasing, which is the property that makes the
    /// gauge immune to scrape timing.
    /// </summary>
    private int _peak;

    /// <summary>
    /// Private so the process-wide <see cref="Shared"/> instance cannot be
    /// bypassed by constructing a second census whose peak nothing exports.
    /// </summary>
    private LeafSnapshotCaptureConcurrencyCensus()
    {
    }

    /// <summary>
    /// The census every leaf activation on this silo contributes to.
    /// </summary>
    internal static LeafSnapshotCaptureConcurrencyCensus Shared => Instance;

    /// <summary>
    /// Marks one capture as entering the attempt boundary and returns the number
    /// of captures that were <b>already</b> in flight when it entered - so zero
    /// means it ran alone. The caller must dispose the returned scope exactly
    /// once, on every path including a throwing one, or the depth leaks upward
    /// and the peak ratchets on a count that is no longer real.
    /// </summary>
    public CaptureConcurrencyScope Enter(out int depthBefore)
    {
        var depthAfter = Interlocked.Increment(ref _inFlight);
        depthBefore = depthAfter - 1;

        // Compare-and-swap rather than a plain compare-and-assign: two captures
        // entering concurrently can both observe a stale peak, and a plain write
        // would let the loser's smaller value overwrite the winner's larger one -
        // silently lowering a high-water mark, which is the one thing this
        // instrument must never do. The loop re-reads and retries until the
        // published peak is at least this observation.
        var observed = Volatile.Read(ref _peak);
        while (depthAfter > observed)
        {
            var seen = Interlocked.CompareExchange(ref _peak, depthAfter, observed);
            if (seen == observed)
            {
                break;
            }

            observed = seen;
        }

        return new CaptureConcurrencyScope(this);
    }

    /// <summary>
    /// Current in-flight capture depth. Test and diagnostic surface only; the
    /// exported instrument is the peak, for the reasons on the type.
    /// </summary>
    internal int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>
    /// Greatest concurrent capture depth observed on this census.
    /// </summary>
    internal int Peak => Volatile.Read(ref _peak);

    /// <summary>
    /// Test-only reset. Clears both the live depth and the high-water mark so a
    /// successor fixture does not inherit a peak from an earlier one.
    /// Intentionally <c>internal</c> so production code cannot reset a monotone
    /// series.
    /// </summary>
    internal void ResetForTesting()
    {
        Interlocked.Exchange(ref _inFlight, 0);
        Interlocked.Exchange(ref _peak, 0);
    }

    /// <summary>
    /// Yields the single peak measurement. Always yields exactly one value, which
    /// is what keeps the series zero-primed on a silo that has never captured.
    /// </summary>
    private IEnumerable<Measurement<int>> ObservePeak()
    {
        yield return new Measurement<int>(Volatile.Read(ref _peak), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Registers the peak gauge on the shared meter. Idempotent, and called from
    /// <c>AddLattice</c> so the series exists from host build rather than from
    /// the first capture.
    /// <para>
    /// Eager registration is the half of zero-priming that is easy to lose. A
    /// gauge registered lazily on first capture reports nothing at all on a silo
    /// that has never captured, so its absence would mean "no detector" exactly
    /// when the operator needs it to mean "measured none" - the reading this
    /// instrument exists to supply. Registering at host build makes the zero
    /// evidential.
    /// </para>
    /// <para>
    /// The gauge is created here rather than held in a static field, so the rule
    /// that an observable instrument must be declared below every piece of state
    /// its callback reads is satisfied trivially: there is no field whose
    /// initialiser could run before <see cref="Instance"/>.
    /// </para>
    /// </summary>
    internal static void EnsureGaugeRegistered()
    {
        lock (RegistrationLock)
        {
            if (_peakGauge is not null)
            {
                return;
            }

            _peakGauge = LatticeMetrics.Meter.CreateObservableGauge(
                LatticeMetrics.LeafSnapshotCaptureConcurrencyPeakGaugeName,
                static () => Instance.ObservePeak(),
                unit: "{capture}",
                description: "Greatest number of leaf-snapshot captures observed executing concurrently on this silo since process start. Monotone, so a transient spike cannot fall between two scrapes.");
        }
    }

    /// <summary>
    /// The lifetime of one capture's contribution to the in-flight depth. A
    /// struct so the hot path allocates nothing, and idempotent on repeat
    /// disposal so a defensive double-dispose cannot drive the depth negative.
    /// </summary>
    internal struct CaptureConcurrencyScope : IDisposable
    {
        private LeafSnapshotCaptureConcurrencyCensus? _owner;

        internal CaptureConcurrencyScope(LeafSnapshotCaptureConcurrencyCensus owner) => _owner = owner;

        /// <summary>
        /// Releases this capture's contribution to the in-flight depth. Never
        /// lowers the peak, which is a high-water mark rather than a live value.
        /// </summary>
        public void Dispose()
        {
            var owner = _owner;
            if (owner is null)
            {
                return;
            }

            _owner = null;
            Interlocked.Decrement(ref owner._inFlight);
        }
    }
}
