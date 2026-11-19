using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Samples the silo's resident activation count from the instrument Orleans already
/// publishes, so the size of the set a drain has to get through is readable while
/// the container is running.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why an instrument and not a grain call.</b> The resident set is also reachable
/// through <c>IManagementGrain</c>, and every other way of asking costs a grain call.
/// The two moments this count is wanted are the worst two moments to make one: during
/// a periodic health poll, where it would put avoidable load on the silo, and at the
/// start of a drain, where a call would compete with the very teardown it is trying
/// to measure and could itself hang past the budget. Orleans publishes the number as
/// an observable gauge that costs a callback to read, so this reads that.
/// </para>
/// <para>
/// <b>Selection is by instrument name and the name is not ours.</b>
/// <see cref="InstrumentName"/> belongs to Orleans, so a future release may rename or
/// withdraw it. That is why every consumer of this class takes
/// <see langword="null"/> for an answer and says "unavailable" rather than
/// substituting a zero: a residency of zero and a residency nobody could read are
/// different facts, and reporting the second as the first would turn a lost signal
/// into a confident wrong one. <c>RepoContextActivationCensusTests</c> pins the
/// listening behaviour against a probe instrument of the same name, and
/// <c>RepoContextActivationCensusInstrumentNameTests</c> pins the assumption that
/// Orleans still publishes it.
/// </para>
/// <para>
/// <b>A sample is the sum of one observation.</b> On the pinned Orleans runtime the gauge reports
/// one measurement per grain type (tagged <c>grain_type</c>) rather than a single
/// silo-wide value, so each sample sums every measurement a single observation
/// yields. A published instrument that yields no measurements - a silo with no grain
/// type activated yet - is a residency of zero, not an unreadable one.
/// </para>
/// </remarks>
public sealed class RepoContextActivationCensus : IDisposable
{
    /// <summary>
    /// The Orleans catalog instrument reporting the activation working set: the
    /// activations resident on this silo.
    /// </summary>
    public const string InstrumentName = "orleans-catalog-activation-working-set";

    private readonly MeterListener _listener;
    private readonly Lock _gate = new();
    private long _sum;
    private bool _instrumentEnabled;
    private bool _disposed;

    /// <summary>Initializes the census and begins listening.</summary>
    /// <param name="instrumentName">
    /// The instrument to observe, defaulting to <see cref="InstrumentName"/>.
    /// Overridable so a test can drive the listener from a probe instrument it owns
    /// rather than having to stand up a silo.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="instrumentName"/> is null.</exception>
    public RepoContextActivationCensus(string? instrumentName = null)
    {
        var name = instrumentName ?? InstrumentName;
        ArgumentNullException.ThrowIfNull(name);

        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (string.Equals(instrument.Name, name, StringComparison.Ordinal))
                {
                    listener.EnableMeasurementEvents(instrument);
                    Volatile.Write(ref _instrumentEnabled, true);
                }
            },
        };

        _listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Record(measurement));
        _listener.SetMeasurementEventCallback<int>((_, measurement, _, _) => Record(measurement));
        _listener.SetMeasurementEventCallback<double>((_, measurement, _, _) => Record((long)measurement));
        _listener.Start();
    }

    /// <summary>
    /// Samples the resident activation count, or returns <see langword="null"/> when
    /// no reading is available.
    /// </summary>
    /// <remarks>
    /// A fault raised by the observing callback is swallowed and reported as
    /// unavailable. The callback belongs to Orleans and runs on this thread, and this
    /// is a diagnostic: on the drain path in particular, a throw here would cost the
    /// stop sequence to save a number.
    /// </remarks>
    /// <returns>The resident activation count, or <see langword="null"/>.</returns>
    public int? TrySample()
    {
        if (Volatile.Read(ref _disposed))
        {
            return null;
        }

        // Held across the observation so concurrent samples cannot interleave their
        // sums. The observable callbacks run synchronously on this thread and Lock
        // is re-entrant, so Record taking the same gate does not deadlock.
        lock (_gate)
        {
            if (!Volatile.Read(ref _instrumentEnabled))
            {
                return null;
            }

            _sum = 0;
            try
            {
                _listener.RecordObservableInstruments();
            }
            catch (Exception)
            {
                // Deliberately broad: the callback is another component's code and the
                // caller is either shutting down or polling for diagnostics.
                return null;
            }

            return _sum is >= 0 and <= int.MaxValue ? (int)_sum : null;
        }
    }

    /// <summary>Stops listening. Safe to call more than once.</summary>
    public void Dispose()
    {
        if (Volatile.Read(ref _disposed))
        {
            return;
        }

        Volatile.Write(ref _disposed, true);
        _listener.Dispose();
    }

    private void Record(long measurement)
    {
        lock (_gate)
        {
            _sum += measurement;
        }
    }
}
