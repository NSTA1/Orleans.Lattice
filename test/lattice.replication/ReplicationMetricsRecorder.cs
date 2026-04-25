using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Captures every measurement recorded on a single meter for the lifetime
/// of the recorder. Used by <see cref="TwoSiteClusterFixture"/> to give
/// convergence tests a single point of access to replication telemetry
/// without having to wire a <see cref="MeterListener"/> per test.
/// </summary>
internal sealed class ReplicationMetricsRecorder : IDisposable
{
    private readonly MeterListener listener;
    private readonly ConcurrentQueue<MeterRecord> records = new();

    public ReplicationMetricsRecorder(string meterName)
    {
        listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == meterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>(Record);
        listener.SetMeasurementEventCallback<double>(Record);
        listener.Start();
    }

    public IReadOnlyCollection<MeterRecord> Records => records.ToArray();

    public IEnumerable<MeterRecord> ForInstrument(string name) =>
        records.Where(r => r.Name == name);

    public void RecordObservableInstruments() => listener.RecordObservableInstruments();

    public void Dispose() => listener.Dispose();

    private void Record<T>(Instrument instrument, T value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? _) where T : struct
    {
        var copy = new KeyValuePair<string, object?>[tags.Length];
        tags.CopyTo(copy);
        records.Enqueue(new MeterRecord(instrument.Name, Convert.ToDouble(value), copy));
    }
}

internal readonly record struct MeterRecord(
    string Name,
    double Value,
    IReadOnlyList<KeyValuePair<string, object?>> Tags);
