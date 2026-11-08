using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Silo-scoped bound on concurrent retained-byte WAL usage scans
/// (<see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>), applied by
/// <see cref="WalShardGrain"/> where the scan runs so every usage path - the
/// WAL-only poll, the deep storage-usage roll-up and the on-demand report - is
/// covered (issue #6017).
/// <para>
/// <b>Non-queuing.</b> A scan waits at most <see cref="AdmissionWait"/> for a
/// slot and is then refused rather than queued. A queue would only defer the
/// overload: the usage poll re-asks on a fixed cadence, so a backlog of queued
/// scans is a backlog of answers nobody is still waiting for. A refused scan
/// reports its partition as unavailable, which the usage aggregators already
/// cover with the partition's last-known figure.
/// </para>
/// <para>
/// <b>One instance per silo, not a static.</b> Registered as a DI singleton by
/// <c>AddLattice</c>, so the several silos of an in-process test cluster each
/// hold their own bound, exactly as separate processes do. The bound is read
/// from the default (unnamed) <see cref="LatticeOptions"/> once, when the silo
/// builds the singleton; a later options change applies on the next start.
/// </para>
/// <para>
/// The in-flight count is exported on
/// <see cref="LatticeMetrics.StorageUsageScansInFlightGaugeName"/>. That gauge
/// sums every live limiter in the process, which is the silo's own count in
/// any one-silo-per-process deployment.
/// </para>
/// </summary>
internal sealed class WalUsageScanLimiter
{
    /// <summary>
    /// How long a scan waits for a free slot before it is refused. Fixed rather
    /// than configurable: it only has to absorb the jitter between one scan
    /// finishing and the next arriving, and anything long enough to matter
    /// would turn the bound back into a queue. One second is far below the
    /// usage fan-out budget, so a refusal still leaves the aggregator time to
    /// answer.
    /// </summary>
    internal static readonly TimeSpan AdmissionWait = TimeSpan.FromSeconds(1);

    private static readonly object RegistrationLock = new();

    /// <summary>
    /// Scans in flight across every limiter in this process. Read by the
    /// in-flight gauge, and declared above it.
    /// </summary>
    private static int _processInFlight;

    private readonly SemaphoreSlim _slots;
    private readonly TimeSpan _admissionWait;
    private int _inFlight;

    /// <summary>
    /// Builds the silo's limiter from the default (unnamed) options.
    /// </summary>
    /// <param name="options">The Lattice options monitor.</param>
    public WalUsageScanLimiter(IOptionsMonitor<LatticeOptions> options)
        : this(ReadLimit(options), AdmissionWait)
    {
    }

    /// <summary>
    /// Builds a limiter with an explicit bound and admission wait. Test seam.
    /// </summary>
    /// <param name="limit">Maximum concurrent scans; values below 1 are treated as 1.</param>
    /// <param name="admissionWait">How long a scan waits for a slot before it is refused.</param>
    internal WalUsageScanLimiter(int limit, TimeSpan admissionWait)
    {
        Limit = Math.Max(1, limit);
        _admissionWait = admissionWait;
        _slots = new SemaphoreSlim(Limit, Limit);
        EnsureGaugeRegistered();
    }

    /// <summary>The configured bound on concurrent scans.</summary>
    internal int Limit { get; }

    /// <summary>Scans currently holding a slot on this limiter.</summary>
    internal int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>
    /// Waits up to the admission wait for a slot. Returns <see langword="true"/>
    /// when a slot was taken, in which case the caller must call
    /// <see cref="Release"/> exactly once; <see langword="false"/> when the scan
    /// is refused.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    internal async Task<bool> TryEnterAsync(CancellationToken cancellationToken)
    {
        if (!await _slots.WaitAsync(_admissionWait, cancellationToken).ConfigureAwait(true))
        {
            return false;
        }

        Interlocked.Increment(ref _inFlight);
        Interlocked.Increment(ref _processInFlight);
        return true;
    }

    /// <summary>Returns a slot taken by a successful <see cref="TryEnterAsync"/>.</summary>
    internal void Release()
    {
        Interlocked.Decrement(ref _inFlight);
        Interlocked.Decrement(ref _processInFlight);
        _slots.Release();
    }

    private static int ReadLimit(IOptionsMonitor<LatticeOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.Get(Options.DefaultName).MaxConcurrentWalUsageScansPerSilo;
    }

    private static IEnumerable<Measurement<int>> ObserveInFlight()
    {
        yield return new Measurement<int>(Volatile.Read(ref _processInFlight), LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Registers the in-flight gauge on the shared meter. Idempotent; called
    /// from <c>AddLattice</c> so the series exists from silo build, and from
    /// every constructor as a backstop.
    /// </summary>
    internal static void EnsureGaugeRegistered()
    {
        lock (RegistrationLock)
        {
            if (_inFlightGauge is not null)
            {
                return;
            }

            _inFlightGauge = LatticeMetrics.Meter.CreateObservableGauge(
                LatticeMetrics.StorageUsageScansInFlightGaugeName,
                static () => ObserveInFlight(),
                unit: "{scan}",
                description: "Retained-byte WAL usage scans running on this silo now, against LatticeOptions.MaxConcurrentWalUsageScansPerSilo.");
        }
    }

    /// <summary>
    /// The registered in-flight gauge, or <see langword="null"/> before
    /// <see cref="EnsureGaugeRegistered"/> has run. Declared last, below every
    /// field its callback reads.
    /// </summary>
    private static ObservableGauge<int>? _inFlightGauge;
}
