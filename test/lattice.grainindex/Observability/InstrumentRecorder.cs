using System.Diagnostics.Metrics;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// A <see cref="MeterListener"/> over the grain-index instruments, used to
/// assert what a code path actually published rather than what it was told to
/// publish.
/// </summary>
/// <remarks>
/// It listens to the real instruments on the real meter, so a test proves the
/// observable behaviour an OpenTelemetry subscriber would see. Nothing about a
/// recording site's internals is inspected.
/// </remarks>
internal sealed class InstrumentRecorder : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<Recorded> _measurements = [];
    private readonly Lock _gate = new();

    /// <summary>
    /// Starts listening to every instrument on the grain-index meter whose name
    /// begins with the grain-index prefix.
    /// </summary>
    internal InstrumentRecorder()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (ReferenceEquals(instrument.Meter, GrainIndexMetrics.Meter)
                && instrument.Name.StartsWith("orleans.lattice.grainindex.", StringComparison.Ordinal))
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        _listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => Add(instrument.Name, value, tags));
        _listener.SetMeasurementEventCallback<int>(
            (instrument, value, tags, _) => Add(instrument.Name, value, tags));
        _listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => Add(instrument.Name, value, tags));

        _listener.Start();
    }

    /// <summary>Polls every observable gauge, so its measurements are recorded.</summary>
    internal void Collect() => _listener.RecordObservableInstruments();

    /// <summary>Every measurement recorded so far, oldest first.</summary>
    /// <returns>The recorded measurements.</returns>
    internal IReadOnlyList<Recorded> Measurements()
    {
        lock (_gate)
        {
            return [.. _measurements];
        }
    }

    /// <summary>The measurements published by one instrument.</summary>
    /// <param name="instrumentName">The instrument's canonical dotted name.</param>
    /// <returns>The instrument's measurements, oldest first.</returns>
    internal IReadOnlyList<Recorded> For(string instrumentName)
    {
        lock (_gate)
        {
            return [.. _measurements.Where(m => m.Instrument == instrumentName)];
        }
    }

    /// <summary>
    /// The sum of one instrument's measurements, optionally restricted to the
    /// measurements carrying a given tag value.
    /// </summary>
    /// <param name="instrumentName">The instrument's canonical dotted name.</param>
    /// <param name="tagKey">A tag key to filter on, or <c>null</c> for no filter.</param>
    /// <param name="tagValue">The tag value to require when <paramref name="tagKey"/> is supplied.</param>
    /// <returns>The summed value.</returns>
    internal double Sum(string instrumentName, string? tagKey = null, string? tagValue = null)
    {
        var total = 0d;
        foreach (var measurement in For(instrumentName))
        {
            if (tagKey is not null && !measurement.HasTag(tagKey, tagValue))
                continue;

            total += measurement.Value;
        }

        return total;
    }

    /// <summary>The most recent measurement of one instrument, or <c>null</c>.</summary>
    /// <param name="instrumentName">The instrument's canonical dotted name.</param>
    /// <param name="index">The index name the measurement must be tagged with.</param>
    /// <returns>The latest matching measurement, or <c>null</c>.</returns>
    internal Recorded? Latest(string instrumentName, string index)
    {
        Recorded? latest = null;
        foreach (var measurement in For(instrumentName))
        {
            if (measurement.HasTag(GrainIndexMetrics.TagIndex, index))
                latest = measurement;
        }

        return latest;
    }

    /// <summary>Forgets every measurement recorded so far.</summary>
    internal void Reset()
    {
        lock (_gate)
        {
            _measurements.Clear();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _listener.Dispose();

    private void Add(string instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var copied = new KeyValuePair<string, object?>[tags.Length];
        tags.CopyTo(copied);

        lock (_gate)
        {
            _measurements.Add(new Recorded(instrument, value, copied));
        }
    }

    /// <summary>One published measurement.</summary>
    /// <param name="Instrument">The instrument's canonical dotted name.</param>
    /// <param name="Value">The measured value, widened to a double.</param>
    /// <param name="Tags">The measurement's tags.</param>
    internal sealed record Recorded(
        string Instrument,
        double Value,
        KeyValuePair<string, object?>[] Tags)
    {
        /// <summary>Whether the measurement carries a tag with the given key and value.</summary>
        /// <param name="key">The tag key.</param>
        /// <param name="value">The required value, or <c>null</c> to accept any.</param>
        /// <returns><c>true</c> when a matching tag is present.</returns>
        internal bool HasTag(string key, string? value)
        {
            foreach (var tag in Tags)
            {
                if (string.Equals(tag.Key, key, StringComparison.Ordinal)
                    && (value is null || Equals(tag.Value, value)))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
