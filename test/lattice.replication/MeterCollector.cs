using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Test helper that subscribes a <see cref="MeterListener"/> to a single
/// instrument by name and records every measurement. Use to assert that an
/// instrument fires with the expected value/tags during a test.
/// </summary>
internal sealed class MeterCollector<T> : IDisposable where T : struct
{
    private readonly MeterListener listener;
    private readonly ConcurrentQueue<RecordedMeasurement<T>> measurements = new();

    public MeterCollector(string meterName, string instrumentName)
    {
        listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == meterName && instrument.Name == instrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<T>((instrument, value, tags, _) =>
        {
            var copy = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(copy);
            measurements.Enqueue(new RecordedMeasurement<T>(value, copy));
        });
        listener.Start();
    }

    public IReadOnlyCollection<RecordedMeasurement<T>> Measurements => measurements.ToArray();

    public void RecordObservableInstruments() => listener.RecordObservableInstruments();

    public void Dispose() => listener.Dispose();
}

internal readonly record struct RecordedMeasurement<T>(
    T Value,
    IReadOnlyList<KeyValuePair<string, object?>> Tags) where T : struct;
