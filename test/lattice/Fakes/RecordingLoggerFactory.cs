using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// One captured log call: its level, its rendered message, and the structured
/// state the template was given, flattened to a name/value lookup.
/// </summary>
/// <param name="Level">The level the entry was written at.</param>
/// <param name="Message">The rendered message.</param>
/// <param name="State">
/// The structured state by template placeholder name. Empty when the state was
/// not an <see cref="IReadOnlyList{T}"/> of key/value pairs, which is the shape
/// the templated <c>LogWarning</c> overloads produce.
/// </param>
public sealed record RecordedLogEntry(
    LogLevel Level,
    string Message,
    IReadOnlyDictionary<string, object?> State)
{
    /// <summary>
    /// Reads a structured value by its template placeholder name.
    /// </summary>
    /// <param name="name">The placeholder name, without braces.</param>
    /// <returns>The captured value, or <see langword="null"/> when absent.</returns>
    public object? Value(string name) => State.TryGetValue(name, out var value) ? value : null;

    /// <summary>
    /// Reads a structured value by its template placeholder name as an
    /// <see cref="long"/>.
    /// </summary>
    /// <param name="name">The placeholder name, without braces.</param>
    /// <returns>The captured value converted to <see cref="long"/>.</returns>
    public long Int64(string name) => Convert.ToInt64(Value(name), System.Globalization.CultureInfo.InvariantCulture);
}

/// <summary>
/// An <see cref="ILoggerFactory"/> that records every entry written through it,
/// so a test can assert on what a grain actually logged - both the message and
/// the structured values behind it.
/// <para>
/// Grains resolve their logger through
/// <see cref="IServiceProvider"/>.<c>GetService&lt;ILoggerFactory&gt;()</c> on
/// the activation, so registering one of these in a test's
/// <c>ActivationServices</c> is enough to capture their output.
/// </para>
/// </summary>
public sealed class RecordingLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentQueue<RecordedLogEntry> _entries = new();

    /// <summary>
    /// Every entry captured so far, oldest first.
    /// </summary>
    public IReadOnlyList<RecordedLogEntry> Entries => _entries.ToArray();

    /// <summary>
    /// The captured entries at <see cref="LogLevel.Warning"/> or above.
    /// </summary>
    public IReadOnlyList<RecordedLogEntry> Warnings =>
        _entries.Where(e => e.Level >= LogLevel.Warning).ToArray();

    /// <summary>
    /// Discards every captured entry.
    /// </summary>
    public void Clear() => _entries.Clear();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new RecordingLogger(_entries);

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider)
    {
        // No-op: this factory is the sink.
    }

    /// <inheritdoc />
    public void Dispose()
    {
        // Nothing to release.
    }

    private sealed class RecordingLogger(ConcurrentQueue<RecordedLogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            var values = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (state is IReadOnlyList<KeyValuePair<string, object?>> pairs)
            {
                foreach (var pair in pairs)
                {
                    values[pair.Key] = pair.Value;
                }
            }

            entries.Enqueue(new RecordedLogEntry(logLevel, formatter(state, exception), values));
        }

        private sealed class NullScope : IDisposable
        {
            internal static readonly NullScope Instance = new();

            public void Dispose()
            {
                // Nothing to release.
            }
        }
    }
}
