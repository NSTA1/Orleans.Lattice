using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Captures every measurement one <see cref="RepoContextIngestReporter"/> publishes,
/// listening to that reporter's own meter instance by reference so a parallel
/// fixture's reporter can never leak into the capture.
/// </summary>
internal sealed class IngestMeasurementCapture : IDisposable
{
    private readonly MeterListener _listener;
    private readonly ConcurrentQueue<IngestMeasurement> _measurements = new();

    /// <summary>Starts capturing <paramref name="reporter"/>'s measurements.</summary>
    /// <param name="reporter">The reporter to listen to.</param>
    internal IngestMeasurementCapture(RepoContextIngestReporter reporter)
    {
        _listener = MeterListening.StartForMeter(reporter.Meter, listener =>
        {
            listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Capture(instrument, value, tags));
            listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Capture(instrument, value, tags));
        });
    }

    /// <summary>Every measurement captured so far, in arrival order.</summary>
    internal IReadOnlyList<IngestMeasurement> Measurements => [.. _measurements];

    /// <summary>Collects every observable instrument once, capturing its readings.</summary>
    internal void CollectObservables() => _listener.RecordObservableInstruments();

    /// <summary>
    /// Sums every captured measurement on <paramref name="instrument"/> whose tags match
    /// <paramref name="repository"/> and, when supplied, <paramref name="outcome"/>.
    /// </summary>
    internal double Sum(string instrument, string repository = RepoIndexRunnerHarness.RepoId, string? outcome = null) =>
        Measurements
            .Where(m => m.Instrument == instrument && m.Matches(repository, outcome))
            .Sum(m => m.Value);

    /// <summary>Counts the captured measurements on <paramref name="instrument"/> matching the tags.</summary>
    internal int Count(string instrument, string repository = RepoIndexRunnerHarness.RepoId, string? outcome = null) =>
        Measurements.Count(m => m.Instrument == instrument && m.Matches(repository, outcome));

    /// <inheritdoc />
    public void Dispose() => _listener.Dispose();

    private void Capture(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var copy = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var tag in tags)
        {
            copy[tag.Key] = tag.Value?.ToString();
        }

        _measurements.Enqueue(new IngestMeasurement(instrument.Name, value, copy));
    }
}
