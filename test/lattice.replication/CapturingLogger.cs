using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// An <see cref="ILogger{TCategoryName}"/> that keeps the records written to it,
/// so a fault a component deliberately swallows stays assertable. Several
/// replication components are explicitly best-effort - a failed background pass
/// must never reach the caller - and the log line is the only observable
/// evidence that the fault happened at all.
/// </summary>
/// <typeparam name="T">The logger category type.</typeparam>
internal sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly List<(LogLevel Level, string Message, Exception? Exception)> _records = [];

    /// <summary>A snapshot of every record captured so far.</summary>
    public IReadOnlyList<(LogLevel Level, string Message, Exception? Exception)> Records
    {
        get
        {
            lock (_records)
            {
                return _records.ToArray();
            }
        }
    }

    /// <summary>A snapshot of the captured records at <see cref="LogLevel.Warning"/> or above.</summary>
    public IReadOnlyList<(LogLevel Level, string Message, Exception? Exception)> Warnings
    {
        get
        {
            lock (_records)
            {
                return _records.Where(r => r.Level >= LogLevel.Warning).ToArray();
            }
        }
    }

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => true;

    /// <inheritdoc />
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        lock (_records)
        {
            _records.Add((logLevel, formatter(state, exception), exception));
        }
    }
}
