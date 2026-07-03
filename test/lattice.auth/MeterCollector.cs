using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Test helper that subscribes a <see cref="MeterListener"/> to a single
/// instrument by name and records every measurement. Use to assert that an
/// instrument fires with the expected value / tags during a test.
/// </summary>
internal sealed class MeterCollector<T> : IDisposable where T : struct
{
    private readonly MeterListener _listener;
    private readonly ConcurrentQueue<RecordedMeasurement<T>> _measurements = new();

    public MeterCollector(string meterName, string instrumentName)
    {
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == meterName && instrument.Name == instrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };
        _listener.SetMeasurementEventCallback<T>((_, value, tags, _) =>
        {
            var copy = new KeyValuePair<string, object?>[tags.Length];
            tags.CopyTo(copy);
            _measurements.Enqueue(new RecordedMeasurement<T>(value, copy));
        });
        _listener.Start();
    }

    public IReadOnlyCollection<RecordedMeasurement<T>> Measurements => _measurements.ToArray();

    public void RecordObservableInstruments() => _listener.RecordObservableInstruments();

    public void Dispose() => _listener.Dispose();
}

internal readonly record struct RecordedMeasurement<T>(
    T Value,
    IReadOnlyList<KeyValuePair<string, object?>> Tags) where T : struct;
