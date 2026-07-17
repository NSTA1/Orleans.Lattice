using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Test helper that subscribes a <see cref="MeterListener"/> to a single
/// instrument by name and records every measurement, so a test can assert that
/// an instrument fires with the expected value during a facade call.
/// </summary>
internal sealed class MeterCollector<T> : IDisposable where T : struct
{
    private readonly MeterListener _listener;
    private readonly ConcurrentQueue<T> _measurements = new();

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
        _listener.SetMeasurementEventCallback<T>((_, value, _, _) => _measurements.Enqueue(value));
        _listener.Start();
    }

    public IReadOnlyCollection<T> Values => _measurements.ToArray();

    public long Count => _measurements.Count;

    public long Sum() => _measurements.Sum(v => Convert.ToInt64(v));

    public void Dispose() => _listener.Dispose();
}
