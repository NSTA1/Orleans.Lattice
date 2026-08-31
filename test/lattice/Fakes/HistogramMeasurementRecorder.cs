using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// A single captured histogram measurement: the recorded value together with
/// the tag set it carried.
/// </summary>
internal sealed record CapturedMeasurement(double Value, IReadOnlyList<KeyValuePair<string, object?>> Tags)
{
    /// <summary>
    /// Returns the value of the tag named <paramref name="key"/>, or
    /// <c>null</c> when the measurement carried no such tag.
    /// </summary>
    public object? Tag(string key)
    {
        for (var i = 0; i < Tags.Count; i++)
        {
            if (string.Equals(Tags[i].Key, key, StringComparison.Ordinal))
            {
                return Tags[i].Value;
            }
        }

        return null;
    }
}

/// <summary>
/// Captures <see cref="double"/> measurements recorded on one specific
/// histogram instrument, for tests that assert on emitted telemetry.
/// <para>
/// The Lattice meter is a process-wide static, so a listener started by one
/// fixture also sees measurements recorded by any other fixture running
/// concurrently. The recorder therefore matches the instrument by reference
/// and, when <c>treeTagValue</c> is supplied, keeps only the measurements
/// whose <see cref="LatticeMetrics.TagTree"/> tag equals it. Passing a tree id
/// unique to the test makes the assertions immune to parallel fixtures.
/// </para>
/// </summary>
internal sealed class HistogramMeasurementRecorder : IDisposable
{
    private readonly List<CapturedMeasurement> _measurements = [];
    private readonly object _lock = new();
    private readonly MeterListener _listener;
    private readonly string? _treeTagValue;

    /// <summary>
    /// Starts listening to <paramref name="instrument"/>, optionally keeping
    /// only the measurements tagged with <paramref name="treeTagValue"/>.
    /// </summary>
    public HistogramMeasurementRecorder(Histogram<double> instrument, string? treeTagValue = null)
    {
        ArgumentNullException.ThrowIfNull(instrument);

        _treeTagValue = treeTagValue;
        _listener = new MeterListener
        {
            InstrumentPublished = (published, listener) =>
            {
                if (ReferenceEquals(published, instrument))
                {
                    listener.EnableMeasurementEvents(published);
                }
            },
        };

        _listener.SetMeasurementEventCallback<double>(OnMeasurement);
        _listener.Start();
    }

    /// <summary>
    /// The measurements captured so far, in record order.
    /// </summary>
    public IReadOnlyList<CapturedMeasurement> Measurements
    {
        get { lock (_lock) return _measurements.ToArray(); }
    }

    /// <summary>Stops the underlying <see cref="MeterListener"/>.</summary>
    public void Dispose() => _listener.Dispose();

    private void OnMeasurement(
        Instrument instrument,
        double measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        var captured = tags.ToArray();

        if (_treeTagValue is not null && !CarriesTree(captured, _treeTagValue))
        {
            return;
        }

        lock (_lock) _measurements.Add(new CapturedMeasurement(measurement, captured));
    }

    private static bool CarriesTree(KeyValuePair<string, object?>[] tags, string treeTagValue)
    {
        for (var i = 0; i < tags.Length; i++)
        {
            if (string.Equals(tags[i].Key, LatticeMetrics.TagTree, StringComparison.Ordinal)
                && string.Equals(tags[i].Value as string, treeTagValue, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
