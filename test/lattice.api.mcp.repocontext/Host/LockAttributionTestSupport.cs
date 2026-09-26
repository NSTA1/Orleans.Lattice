using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Test doubles shared by the lock-attribution fixtures (issue #2431): a logger that
/// keeps each entry's structured arguments, a hand-stepped clock, a scripted inner
/// storage, and a listener bound to one meter instance.
/// </summary>
internal static class LockAttributionTestSupport
{
    /// <summary>One structured log entry: its level, event id and named arguments.</summary>
    internal sealed record Entry(LogLevel Level, EventId EventId, IReadOnlyDictionary<string, object?> Arguments, string Message)
    {
        public object? this[string name] => Arguments[name];
    }

    /// <summary>A logger that records every entry with its structured arguments.</summary>
    internal sealed class RecordingLogger : ILogger<RepoContextLockAttributingGrainStorage>
    {
        private readonly List<Entry> _entries = [];

        public IReadOnlyList<Entry> Entries
        {
            get
            {
                lock (_entries)
                {
                    return _entries.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var arguments = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                foreach (var pair in pairs)
                {
                    arguments[pair.Key] = pair.Value;
                }
            }

            lock (_entries)
            {
                _entries.Add(new Entry(logLevel, eventId, arguments, formatter(state, exception)));
            }
        }
    }

    /// <summary>A clock that only moves when told to.</summary>
    internal sealed class SteppedTimeProvider : TimeProvider
    {
        private long _ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Interlocked.Read(ref _ticks);

        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
    }

    /// <summary>An inner storage whose every call runs a scripted behaviour.</summary>
    internal sealed class ScriptedStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        public Func<RepoContextGrainStorageOperation, GrainId, Task> Behaviour { get; set; } = (_, _) => Task.CompletedTask;

        public int Participations { get; private set; }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
            => Behaviour(RepoContextGrainStorageOperation.Read, grainId);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
            => Behaviour(RepoContextGrainStorageOperation.Write, grainId);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
            => Behaviour(RepoContextGrainStorageOperation.Clear, grainId);

        public void Participate(ISiloLifecycle lifecycle) => Participations++;
    }

    /// <summary>
    /// Records every measurement on one <see cref="RepoContextGrainStorageLockMeter"/>
    /// instance, matched by reference so a sibling fixture's meter cannot leak in.
    /// Started after the meter is constructed, so it sees what the meter records from
    /// then on and not the zero-priming, which the exposition fixture covers.
    /// </summary>
    internal sealed class MeterRecorder : IDisposable
    {
        private readonly List<(string Name, double Value, Dictionary<string, object?> Tags)> _measurements = [];
        private readonly MeterListener _listener;

        public MeterRecorder(RepoContextGrainStorageLockMeter meter)
        {
            _listener = MeterListening.StartForMeter(meter.Meter, listener =>
            {
                listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument.Name, value, tags));
            });
        }

        public void Sample() => _listener.RecordObservableInstruments();

        /// <summary>The sum of every measurement on a counter or histogram arm matching the tags.</summary>
        public double Sum(string instrument, params (string Key, string Value)[] tags)
        {
            lock (_measurements)
            {
                return _measurements
                    .Where(m => m.Name == instrument && tags.All(t => Equals(m.Tags.GetValueOrDefault(t.Key), t.Value)))
                    .Sum(m => m.Value);
            }
        }

        /// <summary>Every value recorded on one instrument, in order.</summary>
        public IReadOnlyList<double> Values(string instrument)
        {
            lock (_measurements)
            {
                return _measurements.Where(m => m.Name == instrument).Select(m => m.Value).ToArray();
            }
        }

        /// <summary>The most recent value observed on one instrument.</summary>
        public double? Last(string instrument) => Values(instrument) is { Count: > 0 } values ? values[^1] : null;

        public void Dispose() => _listener.Dispose();

        private void Add(string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                copy[tag.Key] = tag.Value;
            }

            lock (_measurements)
            {
                _measurements.Add((name, value, copy));
            }
        }
    }
}
